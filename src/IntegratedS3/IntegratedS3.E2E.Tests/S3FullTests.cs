using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using Xunit;

namespace IntegratedS3.E2E.Tests;

/// <summary>
/// Full end-to-end suite (slower): multipart upload/complete/abort, list v1/v2 with prefix + pagination,
/// versioning + delete markers, conditional GETs (304 / 412), and the 403 bad-signature error path.
/// All driven by the AWS SDK against the real loopback host.
/// </summary>
[Collection(E2ECollection.Name)]
[Trait("Suite", "Full")]
public sealed class S3FullTests(E2EHostFixture host)
{
    private static string NewBucket() => $"e2e-full-{Guid.NewGuid():N}"[..24];

    private static byte[] DeterministicBytes(int length, byte seed)
    {
        var buffer = new byte[length];
        for (var i = 0; i < length; i++)
        {
            buffer[i] = (byte)((i * 31) + seed);
        }

        return buffer;
    }

    [Fact]
    public async Task Multipart_UploadCompleteAndDownload_AssemblesParts()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();
        const string key = "multipart/assembled.bin";
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });

        var part1 = DeterministicBytes(5 * 1024 * 1024, 1); // 5 MiB (>= S3 min part size)
        var part2 = DeterministicBytes(1024, 2);

        var initiate = await s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucket,
            Key = key,
            ContentType = "application/octet-stream",
        });
        Assert.False(string.IsNullOrWhiteSpace(initiate.UploadId));

        var etags = new List<PartETag>();
        var partNumber = 1;
        foreach (var part in new[] { part1, part2 })
        {
            using var stream = new MemoryStream(part);
            var uploaded = await s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket,
                Key = key,
                UploadId = initiate.UploadId,
                PartNumber = partNumber,
                InputStream = stream,
                PartSize = part.Length,
            });
            etags.Add(new PartETag(partNumber, uploaded.ETag));
            partNumber++;
        }

        var complete = await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = bucket,
            Key = key,
            UploadId = initiate.UploadId,
            PartETags = etags,
        });
        Assert.Equal(HttpStatusCode.OK, complete.HttpStatusCode);

        var get = await s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key });
        Assert.Equal(part1.Length + part2.Length, get.ContentLength);
        using var memory = new MemoryStream();
        await get.ResponseStream.CopyToAsync(memory);
        var downloaded = memory.ToArray();
        Assert.Equal(part1, downloaded[..part1.Length]);
        Assert.Equal(part2, downloaded[part1.Length..]);
    }

    [Fact]
    public async Task Multipart_Abort_RemovesUploadAndBlocksCompletion()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();
        const string key = "multipart/aborted.bin";
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });

        var initiate = await s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucket,
            Key = key,
        });

        using (var stream = new MemoryStream(DeterministicBytes(5 * 1024 * 1024, 3)))
        {
            await s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket,
                Key = key,
                UploadId = initiate.UploadId,
                PartNumber = 1,
                InputStream = stream,
                PartSize = 5 * 1024 * 1024,
            });
        }

        var abort = await s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = bucket,
            Key = key,
            UploadId = initiate.UploadId,
        });
        Assert.Equal(HttpStatusCode.NoContent, abort.HttpStatusCode);

        await Assert.ThrowsAnyAsync<AmazonS3Exception>(() => s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = bucket,
            Key = key,
            UploadId = initiate.UploadId,
            PartETags = [new PartETag(1, "\"whatever\"")],
        }));
    }

    [Fact]
    public async Task Listing_V1AndV2_HonorPrefixDelimiterAndPagination()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });

        for (var i = 0; i < 5; i++)
        {
            await s3.PutObjectAsync(new PutObjectRequest { BucketName = bucket, Key = $"logs/2026/{i:D2}.txt", ContentBody = "x" });
        }

        await s3.PutObjectAsync(new PutObjectRequest { BucketName = bucket, Key = "images/pic.png", ContentBody = "y" });

        // Prefix + delimiter groups the single non-prefixed folder as a common prefix.
        var delimited = await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket, Delimiter = "/" });
        Assert.Contains("logs/", delimited.CommonPrefixes);
        Assert.Contains("images/", delimited.CommonPrefixes);

        // Prefix filter.
        var prefixed = await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket, Prefix = "logs/2026/" });
        Assert.Equal(5, prefixed.S3Objects.Count);

        // V2 pagination with continuation token.
        var firstPage = await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket, Prefix = "logs/2026/", MaxKeys = 2 });
        Assert.Equal(2, firstPage.S3Objects.Count);
        Assert.True(firstPage.IsTruncated);
        var secondPage = await s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = "logs/2026/",
            MaxKeys = 2,
            ContinuationToken = firstPage.NextContinuationToken,
        });
        Assert.Equal(2, secondPage.S3Objects.Count);

        // V1 listing with marker pagination.
        var v1 = await s3.ListObjectsAsync(new ListObjectsRequest { BucketName = bucket, Prefix = "logs/2026/", MaxKeys = 2 });
        Assert.Equal(2, v1.S3Objects.Count);
        Assert.True(v1.IsTruncated);
    }

    [Fact]
    public async Task Versioning_PreservesHistoryAndCreatesDeleteMarkers()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();
        const string key = "versioned/object.txt";
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });
        await s3.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucket,
            VersioningConfig = new S3BucketVersioningConfig { Status = VersionStatus.Enabled },
        });

        var v1 = await s3.PutObjectAsync(new PutObjectRequest { BucketName = bucket, Key = key, ContentBody = "version-one" });
        var v2 = await s3.PutObjectAsync(new PutObjectRequest { BucketName = bucket, Key = key, ContentBody = "version-two" });
        Assert.False(string.IsNullOrWhiteSpace(v1.VersionId));
        Assert.NotEqual(v1.VersionId, v2.VersionId);

        var versions = await s3.ListVersionsAsync(new ListVersionsRequest { BucketName = bucket, Prefix = key });
        Assert.Equal(2, versions.Versions.Count);

        // Soft delete creates a delete marker; the current GET then 404s.
        var delete = await s3.DeleteObjectAsync(new DeleteObjectRequest { BucketName = bucket, Key = key });
        Assert.False(string.IsNullOrWhiteSpace(delete.VersionId));

        var missing = await Assert.ThrowsAnyAsync<AmazonS3Exception>(() =>
            s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key }));
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);

        // Historical version is still readable by version id.
        var historical = await s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key, VersionId = v1.VersionId });
        using var reader = new StreamReader(historical.ResponseStream);
        Assert.Equal("version-one", await reader.ReadToEndAsync());

        var afterDelete = await s3.ListVersionsAsync(new ListVersionsRequest { BucketName = bucket, Prefix = key });
        Assert.Contains(afterDelete.Versions, v => v.IsDeleteMarker == true);
    }

    [Fact]
    public async Task ConditionalGet_IfNoneMatch_Returns304_AndIfMatch_Returns412()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();
        const string key = "conditional/object.txt";
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });
        var put = await s3.PutObjectAsync(new PutObjectRequest { BucketName = bucket, Key = key, ContentBody = "conditional" });
        var etag = put.ETag;

        // If-None-Match with the current ETag => 304 Not Modified.
        var notModified = await Assert.ThrowsAnyAsync<AmazonS3Exception>(() =>
            s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key, EtagToNotMatch = etag }));
        Assert.Equal(HttpStatusCode.NotModified, notModified.StatusCode);

        // If-Match with a wrong ETag => 412 Precondition Failed.
        var precondition = await Assert.ThrowsAnyAsync<AmazonS3Exception>(() =>
            s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key, EtagToMatch = "\"does-not-match\"" }));
        Assert.Equal(HttpStatusCode.PreconditionFailed, precondition.StatusCode);
    }

    [Fact]
    public async Task BadSignature_Returns403()
    {
        using var s3 = host.CreateClient(E2EHostFixture.AccessKeyId, "totally-wrong-secret");
        var bucket = NewBucket();

        var ex = await Assert.ThrowsAnyAsync<AmazonS3Exception>(() =>
            s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket }));
        Assert.Equal(HttpStatusCode.Forbidden, ex.StatusCode);
    }
}
