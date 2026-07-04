using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using Xunit;

namespace IntegratedS3.E2E.Tests;

/// <summary>
/// Fast, deterministic, offline smoke subset (&lt; ~30s): bucket + object CRUD, listing, a 404 error path,
/// and a presigned-URL round trip. Runs the whole pre-push / free-CI regression gate.
/// </summary>
[Collection(E2ECollection.Name)]
[Trait("Suite", "Smoke")]
public sealed class S3SmokeTests(E2EHostFixture host)
{
    private static string NewBucket() => $"e2e-smoke-{Guid.NewGuid():N}"[..24];

    [Fact]
    public async Task BucketLifecycle_CreateListHeadDelete()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();

        var put = await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });
        Assert.Equal(HttpStatusCode.OK, put.HttpStatusCode);

        var list = await s3.ListBucketsAsync();
        Assert.Contains(list.Buckets, b => b.BucketName == bucket);

        var delete = await s3.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket });
        Assert.Equal(HttpStatusCode.NoContent, delete.HttpStatusCode);
    }

    [Fact]
    public async Task ObjectRoundTrip_PutGetHeadDelete()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();
        const string key = "docs/smoke.txt";
        const string payload = "hello e2e smoke";

        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });

        var put = await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            ContentBody = payload,
            ContentType = "text/plain",
            UseChunkEncoding = false,
        });
        Assert.Equal(HttpStatusCode.OK, put.HttpStatusCode);
        Assert.False(string.IsNullOrWhiteSpace(put.ETag));

        var head = await s3.GetObjectMetadataAsync(new GetObjectMetadataRequest { BucketName = bucket, Key = key });
        Assert.Equal(HttpStatusCode.OK, head.HttpStatusCode);
        Assert.Equal("text/plain", head.Headers.ContentType);
        Assert.Equal(payload.Length, head.Headers.ContentLength);

        var get = await s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key });
        using (var reader = new StreamReader(get.ResponseStream))
        {
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }

        var listed = await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket });
        Assert.Equal(key, Assert.Single(listed.S3Objects).Key);

        var delete = await s3.DeleteObjectAsync(new DeleteObjectRequest { BucketName = bucket, Key = key });
        Assert.Equal(HttpStatusCode.NoContent, delete.HttpStatusCode);
    }

    [Fact]
    public async Task GetMissingObject_Returns404NoSuchKey()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });

        var ex = await Assert.ThrowsAnyAsync<AmazonS3Exception>(() =>
            s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = "missing.txt" }));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
        Assert.Equal("NoSuchKey", ex.ErrorCode);
    }

    [Fact]
    public async Task PresignedUrl_PutThenGet_RoundTrips()
    {
        using var s3 = host.CreateClient();
        var bucket = NewBucket();
        const string key = "presigned/object.bin";
        var payload = "presigned-payload"u8.ToArray();
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });

        var putUrl = s3.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            BucketName = bucket,
            Key = key,
            Verb = HttpVerb.PUT,
            Protocol = Amazon.S3.Protocol.HTTP,
            Expires = DateTime.UtcNow.AddMinutes(5),
        });
        var getUrl = s3.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            BucketName = bucket,
            Key = key,
            Verb = HttpVerb.GET,
            Protocol = Amazon.S3.Protocol.HTTP,
            Expires = DateTime.UtcNow.AddMinutes(5),
        });

        using var http = host.CreatePlainHttpClient();
        using var putContent = new ByteArrayContent(payload);
        var putResponse = await http.PutAsync(putUrl, putContent);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        var getResponse = await http.GetAsync(getUrl);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(payload, await getResponse.Content.ReadAsByteArrayAsync());
    }
}
