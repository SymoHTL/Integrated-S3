using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using IntegratedS3.AspNetCore;
using IntegratedS3.Core.Services;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3AwsSdkCompatibilityTests : IClassFixture<WebUiApplicationFactory>
{
    private readonly WebUiApplicationFactory _factory;

    public IntegratedS3AwsSdkCompatibilityTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task AmazonS3Client_PathStyleCrudAndListObjectsV2_WorkAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-access";
        const string secretAccessKey = "aws-sdk-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-bucket";
        const string objectKey = "docs/aws-sdk.txt";
        const string payload = "hello from amazon sdk";

        var putBucketResponse = await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        });
        Assert.Equal(System.Net.HttpStatusCode.OK, putBucketResponse.HttpStatusCode);

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = payload,
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(System.Net.HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(System.Net.HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal("text/plain", metadataResponse.Headers.ContentType);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(System.Net.HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        using (var reader = new StreamReader(getObjectResponse.ResponseStream)) {
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }

        var listObjectsResponse = await s3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucketName,
            MaxKeys = 1000
        });
        Assert.Equal(System.Net.HttpStatusCode.OK, listObjectsResponse.HttpStatusCode);
        var listedObject = Assert.Single(listObjectsResponse.S3Objects);
        Assert.Equal(objectKey, listedObject.Key);

        var deleteObjectResponse = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(System.Net.HttpStatusCode.NoContent, deleteObjectResponse.HttpStatusCode);
    }

    [Fact]
    public async Task AmazonS3Client_CopyObjectAndConditionalRequests_WorkAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-copy-access";
        const string secretAccessKey = "aws-sdk-copy-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string sourceBucketName = "aws-sdk-copy-source";
        const string targetBucketName = "aws-sdk-copy-target";
        const string sourceKey = "docs/source.txt";
        const string targetKey = "docs/copied.txt";
        const string payload = "copied by amazon sdk";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = sourceBucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = targetBucketName
        })).HttpStatusCode);

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            ContentBody = payload,
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.False(string.IsNullOrWhiteSpace(metadataResponse.ChecksumCRC32));
        Assert.False(string.IsNullOrWhiteSpace(metadataResponse.ChecksumCRC32C));
        Assert.False(string.IsNullOrWhiteSpace(metadataResponse.ChecksumSHA256));

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            DestinationBucket = targetBucketName,
            DestinationKey = targetKey,
            ETagToMatch = metadataResponse.ETag,
            UnmodifiedSinceDate = metadataResponse.LastModified!.Value.ToUniversalTime().AddMinutes(-5)
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
        Assert.Equal(metadataResponse.ChecksumCRC32, copyResponse.ChecksumCRC32);
        Assert.Equal(metadataResponse.ChecksumCRC32C, copyResponse.ChecksumCRC32C);
        Assert.Equal(metadataResponse.ChecksumSHA256, copyResponse.ChecksumSHA256);

        var failedCopyException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            DestinationBucket = targetBucketName,
            DestinationKey = "docs/not-copied.txt",
            ETagToNotMatch = metadataResponse.ETag,
            ModifiedSinceDate = metadataResponse.LastModified!.Value.ToUniversalTime().AddMinutes(-5)
        }));
        Assert.Equal(HttpStatusCode.PreconditionFailed, failedCopyException.StatusCode);

        var missingCopiedObjectException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = targetBucketName,
            Key = "docs/not-copied.txt"
        }));
        Assert.Equal(HttpStatusCode.NotFound, missingCopiedObjectException.StatusCode);

        var copiedObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = targetBucketName,
            Key = targetKey
        });
        Assert.Equal(HttpStatusCode.OK, copiedObjectResponse.HttpStatusCode);
        using (var copiedReader = new StreamReader(copiedObjectResponse.ResponseStream)) {
            Assert.Equal(payload, await copiedReader.ReadToEndAsync());
        }

        var conditionalHeadResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            EtagToMatch = metadataResponse.ETag,
            ModifiedSinceDate = metadataResponse.LastModified!.Value.ToUniversalTime().AddMinutes(-5)
        });
        Assert.Equal(HttpStatusCode.OK, conditionalHeadResponse.HttpStatusCode);

        var conditionalGetResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            EtagToMatch = metadataResponse.ETag,
            ModifiedSinceDate = metadataResponse.LastModified!.Value.ToUniversalTime().AddMinutes(-5)
        });
        Assert.Equal(HttpStatusCode.OK, conditionalGetResponse.HttpStatusCode);
        using (var conditionalReader = new StreamReader(conditionalGetResponse.ResponseStream)) {
            Assert.Equal(payload, await conditionalReader.ReadToEndAsync());
        }

        var notModifiedHeadException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            EtagToNotMatch = metadataResponse.ETag
        }));
        Assert.Equal(HttpStatusCode.NotModified, notModifiedHeadException.StatusCode);

        var notModifiedGetException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            EtagToNotMatch = metadataResponse.ETag
        }));
        Assert.Equal(HttpStatusCode.NotModified, notModifiedGetException.StatusCode);
    }

    [Fact]
    public async Task AmazonS3Client_DeleteMissingObjects_IsIdempotentAndCreatesVersionedDeleteMarkers()
    {
        const string accessKeyId = "aws-sdk-delete-missing-access";
        const string secretAccessKey = "aws-sdk-delete-missing-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string unversionedBucket = "aws-sdk-delete-missing";
        const string versionedBucket = "aws-sdk-delete-missing-versioned";
        const string objectKey = "docs/missing.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = unversionedBucket
        })).HttpStatusCode);

        var deleteMissing = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = unversionedBucket,
            Key = objectKey
        });

        Assert.Equal(HttpStatusCode.NoContent, deleteMissing.HttpStatusCode);
        Assert.Null(deleteMissing.VersionId);
        Assert.True(string.IsNullOrEmpty(deleteMissing.DeleteMarker));

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = versionedBucket
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = versionedBucket,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Enabled
            }
        })).HttpStatusCode);

        var deleteVersionedMissing = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = versionedBucket,
            Key = objectKey
        });

        Assert.Equal(HttpStatusCode.NoContent, deleteVersionedMissing.HttpStatusCode);
        Assert.Equal("true", deleteVersionedMissing.DeleteMarker);
        Assert.False(string.IsNullOrWhiteSpace(deleteVersionedMissing.VersionId));

        var versionsResponse = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = versionedBucket,
            Prefix = objectKey
        });

        Assert.Equal(HttpStatusCode.OK, versionsResponse.HttpStatusCode);
        var deleteMarker = Assert.Single(versionsResponse.Versions);
        Assert.Equal(objectKey, deleteMarker.Key);
        Assert.Equal(deleteVersionedMissing.VersionId, deleteMarker.VersionId);
        Assert.True(deleteMarker.IsDeleteMarker);
        Assert.True(deleteMarker.IsLatest);
    }

    [Fact]
    public async Task AmazonS3Client_PutObject_WithSha1Checksum_ExposesSdkChecksumFieldsAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-sha1-access";
        const string secretAccessKey = "aws-sdk-sha1-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-sha1-bucket";
        const string objectKey = "docs/sha1.txt";
        const string copiedObjectKey = "docs/sha1-copy.txt";
        const string payload = "hello sha1 from amazon sdk";
        var checksum = ComputeSha1Base64(payload);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = payload,
            ContentType = "text/plain",
            UseChunkEncoding = false,
            ChecksumAlgorithm = ChecksumAlgorithm.SHA1,
            ChecksumSHA1 = checksum
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);
        Assert.Equal(checksum, putObjectResponse.ChecksumSHA1);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(checksum, metadataResponse.ChecksumSHA1);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        Assert.Equal(checksum, getObjectResponse.ChecksumSHA1);
        using (var reader = new StreamReader(getObjectResponse.ResponseStream)) {
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = objectKey,
            DestinationBucket = bucketName,
            DestinationKey = copiedObjectKey
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
        Assert.Equal(checksum, copyResponse.ChecksumSHA1);

        var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = copiedObjectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
        Assert.Equal(checksum, copiedMetadataResponse.ChecksumSHA1);
    }

    [Fact]
    public async Task AmazonS3Client_PutObject_WithCrc32cChecksum_ExposesSdkChecksumFieldsAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-crc32c-access";
        const string secretAccessKey = "aws-sdk-crc32c-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-crc32c-bucket";
        const string objectKey = "docs/crc32c.txt";
        const string copiedObjectKey = "docs/crc32c-copy.txt";
        const string payload = "hello crc32c from amazon sdk";
        var checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(payload);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = payload,
            ContentType = "text/plain",
            UseChunkEncoding = false,
            ChecksumAlgorithm = ChecksumAlgorithm.CRC32C,
            ChecksumCRC32C = checksum
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);
        Assert.Equal(checksum, putObjectResponse.ChecksumCRC32C);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(checksum, metadataResponse.ChecksumCRC32C);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        Assert.Equal(checksum, getObjectResponse.ChecksumCRC32C);
        using (var reader = new StreamReader(getObjectResponse.ResponseStream)) {
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = objectKey,
            DestinationBucket = bucketName,
            DestinationKey = copiedObjectKey
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
        Assert.Equal(checksum, copyResponse.ChecksumCRC32C);

        var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = copiedObjectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
        Assert.Equal(checksum, copiedMetadataResponse.ChecksumCRC32C);
    }

    [Fact]
    public async Task AmazonS3Client_SdkGeneratedPresignedUrls_CanUploadAndDownloadObjects()
    {
        const string accessKeyId = "aws-sdk-presign-access";
        const string secretAccessKey = "aws-sdk-presign-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-presign-bucket";
        const string objectKey = "docs/presigned.txt";
        const string payload = "uploaded via sdk presign";

        var createBucketResponse = await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        });
        Assert.Equal(HttpStatusCode.OK, createBucketResponse.HttpStatusCode);

        var presignedPutUrl = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            Verb = HttpVerb.PUT,
            Protocol = Amazon.S3.Protocol.HTTP,
            Expires = DateTime.UtcNow.AddMinutes(5)
        });

        using (var presignedPutRequest = new HttpRequestMessage(HttpMethod.Put, presignedPutUrl)
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        }) {
            var presignedPutResponse = await isolatedClient.Client.SendAsync(presignedPutRequest);
            Assert.Equal(HttpStatusCode.OK, presignedPutResponse.StatusCode);
        }

        var presignedGetUrl = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            Verb = HttpVerb.GET,
            Protocol = Amazon.S3.Protocol.HTTP,
            Expires = DateTime.UtcNow.AddMinutes(5)
        });

        using var presignedGetRequest = new HttpRequestMessage(HttpMethod.Get, presignedGetUrl);
        var presignedGetResponse = await isolatedClient.Client.SendAsync(presignedGetRequest);
        Assert.Equal(HttpStatusCode.OK, presignedGetResponse.StatusCode);
        Assert.Equal(payload, await presignedGetResponse.Content.ReadAsStringAsync());

        var sdkGetResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, sdkGetResponse.HttpStatusCode);
        using (var sdkReader = new StreamReader(sdkGetResponse.ResponseStream)) {
            Assert.Equal(payload, await sdkReader.ReadToEndAsync());
        }
    }

    [Fact]
    public async Task AmazonS3Client_VirtualHostedStyleSdkGeneratedPresignedUrls_CanUploadAndDownloadObjects()
    {
        const string accessKeyId = "aws-sdk-virtual-presign-access";
        const string secretAccessKey = "aws-sdk-virtual-presign-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(
            accessKeyId,
            secretAccessKey,
            options => {
                options.EnableVirtualHostedStyleAddressing = true;
                options.VirtualHostedStyleHostSuffixes = ["localhost"];
            });

        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey, forcePathStyle: false, hostOverride: "localhost");

        const string bucketName = "aws-sdk-virtual-presign-bucket";
        const string objectKey = "docs/virtual-presigned.txt";
        const string payload = "uploaded via virtual hosted sdk presign";

        var createBucketResponse = await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        });
        Assert.Equal(HttpStatusCode.OK, createBucketResponse.HttpStatusCode);

        var presignedPutUrl = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            Verb = HttpVerb.PUT,
            Protocol = Amazon.S3.Protocol.HTTP,
            Expires = DateTime.UtcNow.AddMinutes(5)
        });

        using (var presignedPutRequest = new HttpRequestMessage(HttpMethod.Put, presignedPutUrl)
        {
            Content = new StringContent(payload, Encoding.UTF8, "text/plain")
        }) {
            var presignedPutResponse = await isolatedClient.Client.SendAsync(presignedPutRequest);
            Assert.Equal(HttpStatusCode.OK, presignedPutResponse.StatusCode);
        }

        var presignedGetUrl = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            Verb = HttpVerb.GET,
            Protocol = Amazon.S3.Protocol.HTTP,
            Expires = DateTime.UtcNow.AddMinutes(5)
        });

        using var presignedGetRequest = new HttpRequestMessage(HttpMethod.Get, presignedGetUrl);
        var presignedGetResponse = await isolatedClient.Client.SendAsync(presignedGetRequest);
        Assert.Equal(HttpStatusCode.OK, presignedGetResponse.StatusCode);
        Assert.Equal(payload, await presignedGetResponse.Content.ReadAsStringAsync());

        var sdkGetResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, sdkGetResponse.HttpStatusCode);
        using (var sdkReader = new StreamReader(sdkGetResponse.ResponseStream)) {
            Assert.Equal(payload, await sdkReader.ReadToEndAsync());
        }
    }

    [Fact]
    public async Task AmazonS3Client_GetObjectAndMetadata_WithVersionId_ReadHistoricalVersions()
    {
        const string accessKeyId = "aws-sdk-version-read-access";
        const string secretAccessKey = "aws-sdk-version-read-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-version-read-bucket";
        const string objectKey = "docs/versioned-read.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Enabled
            }
        })).HttpStatusCode);

        var v1Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "historical version",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        var v1VersionId = Assert.IsType<string>(v1Put.VersionId);

        var v2Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "current version",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.NotEqual(v1VersionId, v2Put.VersionId);

        var historicalMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = v1VersionId
        });
        Assert.Equal(HttpStatusCode.OK, historicalMetadataResponse.HttpStatusCode);
        Assert.Equal(v1Put.ETag, historicalMetadataResponse.ETag);

        var historicalGetResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = v1VersionId
        });
        Assert.Equal(HttpStatusCode.OK, historicalGetResponse.HttpStatusCode);
        using (var historicalReader = new StreamReader(historicalGetResponse.ResponseStream)) {
            Assert.Equal("historical version", await historicalReader.ReadToEndAsync());
        }

        var currentGetResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, currentGetResponse.HttpStatusCode);
        using (var currentReader = new StreamReader(currentGetResponse.ResponseStream)) {
            Assert.Equal("current version", await currentReader.ReadToEndAsync());
        }
    }

    [Fact]
    public async Task AmazonS3Client_GetBucketLocation_WorksAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-location-access";
        const string secretAccessKey = "aws-sdk-location-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-location-bucket";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var locationResponse = await s3Client.GetBucketLocationAsync(new GetBucketLocationRequest
        {
            BucketName = bucketName
        });

        Assert.Equal(HttpStatusCode.OK, locationResponse.HttpStatusCode);
        Assert.Equal(string.Empty, locationResponse.Location?.ToString());
    }

    [Fact]
    public async Task AmazonS3Client_ListBuckets_WorksAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-root-access";
        const string secretAccessKey = "aws-sdk-root-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = "aws-sdk-root-bucket"
        });

        var listBucketsResponse = await s3Client.ListBucketsAsync();

        Assert.Equal(HttpStatusCode.OK, listBucketsResponse.HttpStatusCode);
        var bucket = Assert.Single(listBucketsResponse.Buckets);
        Assert.Equal("aws-sdk-root-bucket", bucket.BucketName);
    }

    [Fact]
    public async Task AmazonS3Client_VirtualHostedStyleCrudAndListObjectsV2_WorkAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-virtual-access";
        const string secretAccessKey = "aws-sdk-virtual-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(
            accessKeyId,
            secretAccessKey,
            options => {
                options.EnableVirtualHostedStyleAddressing = true;
                options.VirtualHostedStyleHostSuffixes = ["localhost"];
            });

        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey, forcePathStyle: false, hostOverride: "localhost");

        const string bucketName = "aws-sdk-virtual-bucket";
        const string objectKey = "docs/virtual-sdk.txt";
        const string payload = "hello from virtual host style";

        var putBucketResponse = await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        });
        Assert.Equal(HttpStatusCode.OK, putBucketResponse.HttpStatusCode);

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = payload,
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal("text/plain", metadataResponse.Headers.ContentType);

        var listObjectsResponse = await s3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucketName,
            MaxKeys = 1000
        });
        Assert.Equal(HttpStatusCode.OK, listObjectsResponse.HttpStatusCode);
        var listedObject = Assert.Single(listObjectsResponse.S3Objects);
        Assert.Equal(objectKey, listedObject.Key);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        using (var reader = new StreamReader(getObjectResponse.ResponseStream)) {
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }
    }

    [Fact]
    public async Task AmazonS3Client_VirtualHostedStyleCopyObjectAndConditionalRequests_WorkAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-virtual-copy-access";
        const string secretAccessKey = "aws-sdk-virtual-copy-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(
            accessKeyId,
            secretAccessKey,
            options => {
                options.EnableVirtualHostedStyleAddressing = true;
                options.VirtualHostedStyleHostSuffixes = ["localhost"];
            });

        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey, forcePathStyle: false, hostOverride: "localhost");

        const string sourceBucketName = "aws-sdk-virtual-copy-source";
        const string targetBucketName = "aws-sdk-virtual-copy-target";
        const string sourceKey = "docs/source.txt";
        const string targetKey = "docs/copied.txt";
        const string payload = "copied by amazon sdk via virtual host style";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = sourceBucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = targetBucketName
        })).HttpStatusCode);

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            ContentBody = payload,
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            DestinationBucket = targetBucketName,
            DestinationKey = targetKey,
            ETagToMatch = metadataResponse.ETag
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);

        var copiedObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = targetBucketName,
            Key = targetKey
        });
        Assert.Equal(HttpStatusCode.OK, copiedObjectResponse.HttpStatusCode);
        using (var copiedReader = new StreamReader(copiedObjectResponse.ResponseStream)) {
            Assert.Equal(payload, await copiedReader.ReadToEndAsync());
        }

        var conditionalHeadResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            EtagToMatch = metadataResponse.ETag,
            ModifiedSinceDate = metadataResponse.LastModified!.Value.ToUniversalTime().AddMinutes(-5)
        });
        Assert.Equal(HttpStatusCode.OK, conditionalHeadResponse.HttpStatusCode);

        var conditionalGetResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            EtagToMatch = metadataResponse.ETag,
            ModifiedSinceDate = metadataResponse.LastModified!.Value.ToUniversalTime().AddMinutes(-5)
        });
        Assert.Equal(HttpStatusCode.OK, conditionalGetResponse.HttpStatusCode);
        using (var conditionalReader = new StreamReader(conditionalGetResponse.ResponseStream)) {
            Assert.Equal(payload, await conditionalReader.ReadToEndAsync());
        }

        var notModifiedHeadException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            EtagToNotMatch = metadataResponse.ETag
        }));
        Assert.Equal(HttpStatusCode.NotModified, notModifiedHeadException.StatusCode);

        var notModifiedGetException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            EtagToNotMatch = metadataResponse.ETag
        }));
        Assert.Equal(HttpStatusCode.NotModified, notModifiedGetException.StatusCode);
    }

    [Fact]
    public async Task AmazonS3Client_ListObjectsV2_WithDelimiterAndStartAfter_ReturnsExpectedPrefixesAndObjects()
    {
        const string accessKeyId = "aws-sdk-list-access";
        const string secretAccessKey = "aws-sdk-list-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-list-bucket";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        foreach (var (key, payload) in new[]
                 {
                     ("docs/2024/a.txt", "A"),
                     ("docs/2024/b.txt", "B"),
                     ("docs/2025/c.txt", "C"),
                     ("docs/readme.txt", "R")
                 }) {
            var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                ContentBody = payload,
                ContentType = "text/plain",
                UseChunkEncoding = false
            });

            Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);
        }

        var listResponse = await s3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = "docs/",
            Delimiter = "/",
            StartAfter = "docs/2024/b.txt"
        });

        Assert.Equal(HttpStatusCode.OK, listResponse.HttpStatusCode);
        Assert.Equal("docs/2025/", Assert.Single(listResponse.CommonPrefixes));
        Assert.Equal("docs/readme.txt", Assert.Single(listResponse.S3Objects).Key);
    }

    [Fact]
    public async Task AmazonS3Client_MultipartUpload_WorksAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-multipart-access";
        const string secretAccessKey = "aws-sdk-multipart-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-multipart-bucket";
        const string objectKey = "docs/multipart.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentType = "text/plain"
        });
        Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);

        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes("hello "));
        var part1Response = await s3Client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 1,
            InputStream = part1Stream,
            PartSize = part1Stream.Length,
            IsLastPart = false
        });
        Assert.Equal(HttpStatusCode.OK, part1Response.HttpStatusCode);

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes("world"));
        var part2Response = await s3Client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 2,
            InputStream = part2Stream,
            PartSize = part2Stream.Length,
            IsLastPart = true
        });
        Assert.Equal(HttpStatusCode.OK, part2Response.HttpStatusCode);

        var completeResponse = await s3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartETags =
            [
                new PartETag(1, part1Response.ETag),
                new PartETag(2, part2Response.ETag)
            ]
        });
        Assert.Equal(HttpStatusCode.OK, completeResponse.HttpStatusCode);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        using var reader = new StreamReader(getObjectResponse.ResponseStream);
        Assert.Equal("hello world", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_ListMultipartUploads_RespectsMarkersAndCommonPrefixes()
    {
        const string accessKeyId = "aws-sdk-multipart-list-access";
        const string secretAccessKey = "aws-sdk-multipart-list-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-multipart-list-bucket";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        async Task<string> InitiateAsync(string key)
        {
            var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = key,
                ContentType = "text/plain"
            });

            Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);
            return initiateResponse.UploadId;
        }

        var firstUploadId = await InitiateAsync("docs/alpha.txt");
        await Task.Delay(2);
        var secondUploadId = await InitiateAsync("docs/alpha.txt");
        await Task.Delay(2);
        await InitiateAsync("docs/nested/beta.txt");

        var firstPage = await s3Client.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = bucketName,
            Prefix = "docs/",
            Delimiter = "/",
            MaxUploads = 2
        });

        Assert.Equal(HttpStatusCode.OK, firstPage.HttpStatusCode);
        Assert.True(firstPage.IsTruncated);
        Assert.Equal("docs/alpha.txt", firstPage.NextKeyMarker);
        Assert.Equal(secondUploadId, firstPage.NextUploadIdMarker);
        Assert.Equal(2, firstPage.MultipartUploads.Count);
        Assert.Equal("docs/alpha.txt", firstPage.MultipartUploads[0].Key);
        Assert.Equal(firstUploadId, firstPage.MultipartUploads[0].UploadId);
        Assert.Equal("docs/alpha.txt", firstPage.MultipartUploads[1].Key);
        Assert.Equal(secondUploadId, firstPage.MultipartUploads[1].UploadId);
        Assert.True(firstPage.CommonPrefixes is null || firstPage.CommonPrefixes.Count == 0);

        var secondPage = await s3Client.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = bucketName,
            Prefix = "docs/",
            Delimiter = "/",
            KeyMarker = "docs/alpha.txt",
            UploadIdMarker = secondUploadId
        });

        Assert.Equal(HttpStatusCode.OK, secondPage.HttpStatusCode);
        Assert.False(secondPage.IsTruncated);
        Assert.True(secondPage.MultipartUploads is null || secondPage.MultipartUploads.Count == 0);
        Assert.Equal("docs/nested/", Assert.Single(secondPage.CommonPrefixes ?? []));
    }

    [Fact]
    public async Task AmazonS3Client_MultipartUploadAndCopy_WithChecksumAlgorithm_ExposesCompositeChecksumMetadataAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-multipart-checksum-access";
        const string secretAccessKey = "aws-sdk-multipart-checksum-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-multipart-checksum-bucket";
        const string objectKey = "docs/multipart-checksum.txt";
        const string copiedObjectKey = "docs/multipart-checksum-copy.txt";
        const string part1Payload = "hello ";
        const string part2Payload = "world";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var part1Checksum = ComputeSha256Base64(part1Payload);
        var part2Checksum = ComputeSha256Base64(part2Payload);
        var compositeChecksum = ComputeMultipartSha256Base64(part1Checksum, part2Checksum);

        var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentType = "text/plain",
            ChecksumAlgorithm = ChecksumAlgorithm.SHA256
        });
        Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);
        Assert.Equal(ChecksumAlgorithm.SHA256, initiateResponse.ChecksumAlgorithm);

        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes(part1Payload));
        var part1Response = await s3Client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 1,
            InputStream = part1Stream,
            PartSize = part1Stream.Length,
            IsLastPart = false,
            ChecksumAlgorithm = ChecksumAlgorithm.SHA256,
            ChecksumSHA256 = part1Checksum
        });
        Assert.Equal(HttpStatusCode.OK, part1Response.HttpStatusCode);
        Assert.Equal(part1Checksum, part1Response.ChecksumSHA256);

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes(part2Payload));
        var part2Response = await s3Client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 2,
            InputStream = part2Stream,
            PartSize = part2Stream.Length,
            IsLastPart = true,
            ChecksumAlgorithm = ChecksumAlgorithm.SHA256,
            ChecksumSHA256 = part2Checksum
        });
        Assert.Equal(HttpStatusCode.OK, part2Response.HttpStatusCode);
        Assert.Equal(part2Checksum, part2Response.ChecksumSHA256);

        var completeResponse = await s3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartETags =
            [
                new PartETag(1, part1Response.ETag),
                new PartETag(2, part2Response.ETag)
            ]
        });
        Assert.Equal(HttpStatusCode.OK, completeResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, completeResponse.ChecksumSHA256);
        Assert.Equal(ChecksumType.COMPOSITE, completeResponse.ChecksumType);

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = objectKey,
            DestinationBucket = bucketName,
            DestinationKey = copiedObjectKey
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, copyResponse.ChecksumSHA256);
        Assert.Equal(ChecksumType.COMPOSITE, copyResponse.ChecksumType);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, metadataResponse.ChecksumSHA256);
        Assert.Equal(ChecksumType.COMPOSITE, metadataResponse.ChecksumType);

        var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = copiedObjectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, copiedMetadataResponse.ChecksumSHA256);
        Assert.Equal(ChecksumType.COMPOSITE, copiedMetadataResponse.ChecksumType);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        using var reader = new StreamReader(getObjectResponse.ResponseStream);
        Assert.Equal(part1Payload + part2Payload, await reader.ReadToEndAsync());

        var copiedObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = copiedObjectKey
        });
        Assert.Equal(HttpStatusCode.OK, copiedObjectResponse.HttpStatusCode);
        using var copiedReader = new StreamReader(copiedObjectResponse.ResponseStream);
        Assert.Equal(part1Payload + part2Payload, await copiedReader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_MultipartUpload_WithSha1ChecksumAlgorithm_ExposesCompositeChecksumMetadataAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-multipart-sha1-access";
        const string secretAccessKey = "aws-sdk-multipart-sha1-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-multipart-sha1-bucket";
        const string objectKey = "docs/multipart-sha1.txt";
        const string copiedObjectKey = "docs/multipart-sha1-copy.txt";
        const string part1Payload = "hello ";
        const string part2Payload = "world";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var part1Checksum = ComputeSha1Base64(part1Payload);
        var part2Checksum = ComputeSha1Base64(part2Payload);
        var compositeChecksum = ComputeMultipartSha1Base64(part1Checksum, part2Checksum);

        var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentType = "text/plain",
            ChecksumAlgorithm = ChecksumAlgorithm.SHA1
        });
        Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);
        Assert.Equal(ChecksumAlgorithm.SHA1, initiateResponse.ChecksumAlgorithm);

        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes(part1Payload));
        var part1Response = await s3Client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 1,
            InputStream = part1Stream,
            PartSize = part1Stream.Length,
            IsLastPart = false,
            ChecksumAlgorithm = ChecksumAlgorithm.SHA1,
            ChecksumSHA1 = part1Checksum
        });
        Assert.Equal(HttpStatusCode.OK, part1Response.HttpStatusCode);
        Assert.Equal(part1Checksum, part1Response.ChecksumSHA1);

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes(part2Payload));
        var part2Response = await s3Client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 2,
            InputStream = part2Stream,
            PartSize = part2Stream.Length,
            IsLastPart = true,
            ChecksumAlgorithm = ChecksumAlgorithm.SHA1,
            ChecksumSHA1 = part2Checksum
        });
        Assert.Equal(HttpStatusCode.OK, part2Response.HttpStatusCode);
        Assert.Equal(part2Checksum, part2Response.ChecksumSHA1);

        var completeResponse = await s3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartETags =
            [
                new PartETag(1, part1Response.ETag),
                new PartETag(2, part2Response.ETag)
            ]
        });
        Assert.Equal(HttpStatusCode.OK, completeResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, completeResponse.ChecksumSHA1);
        Assert.Equal(ChecksumType.COMPOSITE, completeResponse.ChecksumType);

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = objectKey,
            DestinationBucket = bucketName,
            DestinationKey = copiedObjectKey
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, copyResponse.ChecksumSHA1);
        Assert.Equal(ChecksumType.COMPOSITE, copyResponse.ChecksumType);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, metadataResponse.ChecksumSHA1);
        Assert.Equal(ChecksumType.COMPOSITE, metadataResponse.ChecksumType);

        var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = copiedObjectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, copiedMetadataResponse.ChecksumSHA1);
        Assert.Equal(ChecksumType.COMPOSITE, copiedMetadataResponse.ChecksumType);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, getObjectResponse.ChecksumSHA1);
        Assert.Equal(ChecksumType.COMPOSITE, getObjectResponse.ChecksumType);
        using var reader = new StreamReader(getObjectResponse.ResponseStream);
        Assert.Equal(part1Payload + part2Payload, await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_MultipartUpload_WithCrc32cChecksumAlgorithm_ExposesCompositeChecksumMetadataAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-multipart-crc32c-access";
        const string secretAccessKey = "aws-sdk-multipart-crc32c-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-multipart-crc32c-bucket";
        const string objectKey = "docs/multipart-crc32c.txt";
        const string copiedObjectKey = "docs/multipart-crc32c-copy.txt";
        const string part1Payload = "hello ";
        const string part2Payload = "world";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var part1Checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(part1Payload);
        var part2Checksum = ChecksumTestAlgorithms.ComputeCrc32cBase64(part2Payload);
        var compositeChecksum = ChecksumTestAlgorithms.ComputeMultipartCrc32cBase64(part1Checksum, part2Checksum);

        var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentType = "text/plain",
            ChecksumAlgorithm = ChecksumAlgorithm.CRC32C
        });
        Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);
        Assert.Equal(ChecksumAlgorithm.CRC32C, initiateResponse.ChecksumAlgorithm);

        await using var part1Stream = new MemoryStream(Encoding.UTF8.GetBytes(part1Payload));
        var part1Response = await s3Client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 1,
            InputStream = part1Stream,
            PartSize = part1Stream.Length,
            IsLastPart = false,
            ChecksumAlgorithm = ChecksumAlgorithm.CRC32C,
            ChecksumCRC32C = part1Checksum
        });
        Assert.Equal(HttpStatusCode.OK, part1Response.HttpStatusCode);
        Assert.Equal(part1Checksum, part1Response.ChecksumCRC32C);

        await using var part2Stream = new MemoryStream(Encoding.UTF8.GetBytes(part2Payload));
        var part2Response = await s3Client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 2,
            InputStream = part2Stream,
            PartSize = part2Stream.Length,
            IsLastPart = true,
            ChecksumAlgorithm = ChecksumAlgorithm.CRC32C,
            ChecksumCRC32C = part2Checksum
        });
        Assert.Equal(HttpStatusCode.OK, part2Response.HttpStatusCode);
        Assert.Equal(part2Checksum, part2Response.ChecksumCRC32C);

        var completeResponse = await s3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartETags =
            [
                new PartETag(1, part1Response.ETag),
                new PartETag(2, part2Response.ETag)
            ]
        });
        Assert.Equal(HttpStatusCode.OK, completeResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, completeResponse.ChecksumCRC32C);
        Assert.Equal(ChecksumType.COMPOSITE, completeResponse.ChecksumType);

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = objectKey,
            DestinationBucket = bucketName,
            DestinationKey = copiedObjectKey
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, copyResponse.ChecksumCRC32C);
        Assert.Equal(ChecksumType.COMPOSITE, copyResponse.ChecksumType);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, metadataResponse.ChecksumCRC32C);
        Assert.Equal(ChecksumType.COMPOSITE, metadataResponse.ChecksumType);

        var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = copiedObjectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, copiedMetadataResponse.ChecksumCRC32C);
        Assert.Equal(ChecksumType.COMPOSITE, copiedMetadataResponse.ChecksumType);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        Assert.Equal(compositeChecksum, getObjectResponse.ChecksumCRC32C);
        Assert.Equal(ChecksumType.COMPOSITE, getObjectResponse.ChecksumType);
        using var reader = new StreamReader(getObjectResponse.ResponseStream);
        Assert.Equal(part1Payload + part2Payload, await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_ListVersionsAndDeleteMarkers_WorkAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-versions-access";
        const string secretAccessKey = "aws-sdk-versions-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-versions-bucket";
        const string objectKey = "docs/history.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Enabled
            }
        })).HttpStatusCode);

        var v1Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "version one",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        var v2Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "version two",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        Assert.NotEqual(v1Put.VersionId, v2Put.VersionId);

        var deleteCurrent = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });

        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.HttpStatusCode);
        Assert.Equal("true", deleteCurrent.DeleteMarker);
        Assert.False(string.IsNullOrWhiteSpace(deleteCurrent.VersionId));

        var currentGetException = await Assert.ThrowsAsync<NoSuchKeyException>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        }));
        Assert.Equal(HttpStatusCode.NotFound, currentGetException.StatusCode);

        var versionsResponse = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = objectKey
        });

        Assert.Equal(HttpStatusCode.OK, versionsResponse.HttpStatusCode);
        Assert.Contains(versionsResponse.Versions, version => version.Key == objectKey && version.VersionId == v1Put.VersionId);
        Assert.Contains(versionsResponse.Versions, version => version.Key == objectKey && version.VersionId == v2Put.VersionId);

        var deleteDeleteMarker = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = deleteCurrent.VersionId
        });

        Assert.Equal(HttpStatusCode.NoContent, deleteDeleteMarker.HttpStatusCode);
        Assert.Equal("true", deleteDeleteMarker.DeleteMarker);
        Assert.Equal(deleteCurrent.VersionId, deleteDeleteMarker.VersionId);

        var restoredGet = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, restoredGet.HttpStatusCode);
        using var reader = new StreamReader(restoredGet.ResponseStream);
        Assert.Equal("version two", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_DeleteMarkerGetAndHeadBehavior_MatchesS3Semantics()
    {
        const string accessKeyId = "aws-sdk-delete-marker-read-access";
        const string secretAccessKey = "aws-sdk-delete-marker-read-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-delete-marker-read-bucket";
        const string objectKey = "docs/deleted.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Enabled
            }
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "delete marker reads",
            ContentType = "text/plain",
            UseChunkEncoding = false
        })).HttpStatusCode);

        var deleteCurrent = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });

        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.HttpStatusCode);
        Assert.Equal("true", deleteCurrent.DeleteMarker);
        var deleteMarkerVersionId = Assert.IsType<string>(deleteCurrent.VersionId);

        var currentGetException = await Assert.ThrowsAsync<NoSuchKeyException>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        }));
        Assert.Equal(HttpStatusCode.NotFound, currentGetException.StatusCode);
        Assert.Equal("NoSuchKey", currentGetException.ErrorCode);

        var currentHeadException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey
        }));
        Assert.Equal(HttpStatusCode.NotFound, currentHeadException.StatusCode);

        var explicitGetException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = deleteMarkerVersionId
        }));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, explicitGetException.StatusCode);
        Assert.Equal("MethodNotAllowed", explicitGetException.ErrorCode);

        var explicitHeadException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = deleteMarkerVersionId
        }));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, explicitHeadException.StatusCode);
    }

    [Fact]
    public async Task AmazonS3Client_CopyObject_SourceVersionIdUsesHistoricalVersionPreconditionsAndRejectsDeleteMarkers()
    {
        const string accessKeyId = "aws-sdk-copy-version-access";
        const string secretAccessKey = "aws-sdk-copy-version-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string sourceBucketName = "aws-sdk-copy-version-source";
        const string targetBucketName = "aws-sdk-copy-version-target";
        const string sourceKey = "docs/source.txt";
        const string historicalCopyKey = "docs/historical-copy.txt";
        const string currentCopyKey = "docs/current-copy.txt";
        const string deleteMarkerCopyKey = "docs/delete-marker-copy.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = sourceBucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = targetBucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = sourceBucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Enabled
            }
        })).HttpStatusCode);

        var v1Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            ContentBody = "version one",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        var v2Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            ContentBody = "version two",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        Assert.NotEqual(v1Put.VersionId, v2Put.VersionId);

        var historicalMetadata = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            VersionId = v1Put.VersionId
        });
        Assert.Equal(HttpStatusCode.OK, historicalMetadata.HttpStatusCode);
        Assert.Equal(v1Put.VersionId, historicalMetadata.VersionId);

        var deleteCurrent = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey
        });

        Assert.Equal(HttpStatusCode.NoContent, deleteCurrent.HttpStatusCode);
        Assert.Equal("true", deleteCurrent.DeleteMarker);
        var deleteMarkerVersionId = Assert.IsType<string>(deleteCurrent.VersionId);

        var historicalCopy = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            SourceVersionId = v1Put.VersionId,
            DestinationBucket = targetBucketName,
            DestinationKey = historicalCopyKey,
            ETagToMatch = historicalMetadata.ETag,
            UnmodifiedSinceDate = historicalMetadata.LastModified!.Value.ToUniversalTime().AddMinutes(5)
        });

        Assert.Equal(HttpStatusCode.OK, historicalCopy.HttpStatusCode);
        Assert.Equal(v1Put.VersionId, historicalCopy.SourceVersionId);

        var copiedHistoricalObject = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = targetBucketName,
            Key = historicalCopyKey
        });
        Assert.Equal(HttpStatusCode.OK, copiedHistoricalObject.HttpStatusCode);
        using (var reader = new StreamReader(copiedHistoricalObject.ResponseStream)) {
            Assert.Equal("version one", await reader.ReadToEndAsync());
        }

        var currentCopyException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            DestinationBucket = targetBucketName,
            DestinationKey = currentCopyKey
        }));
        Assert.Equal(HttpStatusCode.NotFound, currentCopyException.StatusCode);
        Assert.Equal("NoSuchKey", currentCopyException.ErrorCode);

        var deleteMarkerCopyException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            SourceVersionId = deleteMarkerVersionId,
            DestinationBucket = targetBucketName,
            DestinationKey = deleteMarkerCopyKey
        }));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, deleteMarkerCopyException.StatusCode);
        Assert.Equal("MethodNotAllowed", deleteMarkerCopyException.ErrorCode);
    }

    [Fact]
    public async Task AmazonS3Client_ListVersions_WithMarkers_ContinuesWithinSameKey()
    {
        const string accessKeyId = "aws-sdk-version-markers-access";
        const string secretAccessKey = "aws-sdk-version-markers-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-version-markers-bucket";
        const string primaryKey = "docs/history.txt";
        const string secondaryKey = "docs/zeta.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Enabled
            }
        })).HttpStatusCode);

        var v1Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = primaryKey,
            ContentBody = "version one",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        var v2Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = primaryKey,
            ContentBody = "version two",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        var deleteCurrent = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = primaryKey
        });

        var secondaryPut = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = secondaryKey,
            ContentBody = "version zeta",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        var firstPage = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = "docs/",
            MaxKeys = 1
        });

        Assert.Equal(HttpStatusCode.OK, firstPage.HttpStatusCode);
        Assert.True(firstPage.IsTruncated);
        var firstDeleteMarker = Assert.Single(firstPage.Versions);
        Assert.Equal(primaryKey, firstDeleteMarker.Key);
        Assert.Equal(deleteCurrent.VersionId, firstDeleteMarker.VersionId);
        Assert.True(firstDeleteMarker.IsDeleteMarker);
        Assert.True(firstDeleteMarker.IsLatest);
        Assert.Equal(primaryKey, firstPage.NextKeyMarker);
        Assert.Equal(deleteCurrent.VersionId, firstPage.NextVersionIdMarker);

        var secondPage = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = "docs/",
            MaxKeys = 10,
            KeyMarker = firstPage.NextKeyMarker,
            VersionIdMarker = firstPage.NextVersionIdMarker
        });

        Assert.Equal(HttpStatusCode.OK, secondPage.HttpStatusCode);
        Assert.Equal(firstPage.NextKeyMarker, secondPage.KeyMarker);
        Assert.Equal(firstPage.NextVersionIdMarker, secondPage.VersionIdMarker);
        Assert.False(secondPage.IsTruncated);
        Assert.Collection(
            secondPage.Versions,
            version => {
                Assert.Equal(primaryKey, version.Key);
                Assert.Equal(v2Put.VersionId, version.VersionId);
                Assert.False(version.IsLatest);
            },
            version => {
                Assert.Equal(primaryKey, version.Key);
                Assert.Equal(v1Put.VersionId, version.VersionId);
                Assert.False(version.IsLatest);
            },
            version => {
                Assert.Equal(secondaryKey, version.Key);
                Assert.Equal(secondaryPut.VersionId, version.VersionId);
                Assert.True(version.IsLatest);
            });
    }

    [Fact]
    public async Task AmazonS3Client_DeleteObjectTagging_WorksAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-tagging-access";
        const string secretAccessKey = "aws-sdk-tagging-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-tagging-bucket";
        const string objectKey = "docs/tagged.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Enabled
            }
        })).HttpStatusCode);

        var v1Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "version one",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        var putHistoricalTags = await s3Client.PutObjectTaggingAsync(new PutObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = v1Put.VersionId,
            Tagging = new Tagging
            {
                TagSet =
                [
                    new Tag
                    {
                        Key = "generation",
                        Value = "one"
                    }
                ]
            }
        });
        Assert.Equal(HttpStatusCode.OK, putHistoricalTags.HttpStatusCode);
        Assert.Equal(v1Put.VersionId, putHistoricalTags.VersionId);

        var v2Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "version two",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        var putCurrentTags = await s3Client.PutObjectTaggingAsync(new PutObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            Tagging = new Tagging
            {
                TagSet =
                [
                    new Tag
                    {
                        Key = "generation",
                        Value = "two"
                    }
                ]
            }
        });
        Assert.Equal(HttpStatusCode.OK, putCurrentTags.HttpStatusCode);
        Assert.Equal(v2Put.VersionId, putCurrentTags.VersionId);

        var deleteCurrentTags = await s3Client.DeleteObjectTaggingAsync(new DeleteObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.NoContent, deleteCurrentTags.HttpStatusCode);
        Assert.Equal(v2Put.VersionId, deleteCurrentTags.VersionId);

        var currentTags = await s3Client.GetObjectTaggingAsync(new GetObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, currentTags.HttpStatusCode);
        Assert.Equal(v2Put.VersionId, currentTags.VersionId);
        Assert.True(currentTags.Tagging is null || currentTags.Tagging.Count == 0);

        var historicalTags = await s3Client.GetObjectTaggingAsync(new GetObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = v1Put.VersionId
        });
        Assert.Equal(HttpStatusCode.OK, historicalTags.HttpStatusCode);
        Assert.Equal(v1Put.VersionId, historicalTags.VersionId);
        var historicalTag = Assert.Single(historicalTags.Tagging);
        Assert.Equal("generation", historicalTag.Key);
        Assert.Equal("one", historicalTag.Value);

        var deleteHistoricalTags = await s3Client.DeleteObjectTaggingAsync(new DeleteObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = v1Put.VersionId
        });
        Assert.Equal(HttpStatusCode.NoContent, deleteHistoricalTags.HttpStatusCode);
        Assert.Equal(v1Put.VersionId, deleteHistoricalTags.VersionId);

        var clearedHistoricalTags = await s3Client.GetObjectTaggingAsync(new GetObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = v1Put.VersionId
        });
        Assert.Equal(HttpStatusCode.OK, clearedHistoricalTags.HttpStatusCode);
        Assert.Equal(v1Put.VersionId, clearedHistoricalTags.VersionId);
        Assert.True(clearedHistoricalTags.Tagging is null || clearedHistoricalTags.Tagging.Count == 0);
    }

    [Fact]
    public async Task AmazonS3Client_PutObjectTagging_RejectsInvalidTagSets()
    {
        const string accessKeyId = "aws-sdk-invalid-tagging-access";
        const string secretAccessKey = "aws-sdk-invalid-tagging-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-invalid-tagging-bucket";
        const string objectKey = "docs/tagged.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "tag me",
            ContentType = "text/plain",
            UseChunkEncoding = false
        })).HttpStatusCode);

        var exception = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.PutObjectTaggingAsync(new PutObjectTaggingRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            Tagging = new Tagging
            {
                TagSet = Enumerable.Range(0, 11).Select(index => new Tag
                {
                    Key = $"tag-{index}",
                    Value = $"value-{index}"
                }).ToList()
            }
        }));

        Assert.Equal(HttpStatusCode.BadRequest, exception.StatusCode);
        Assert.Equal("InvalidTag", exception.ErrorCode);
    }

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateAuthenticatedLoopbackClientAsync(
        string accessKeyId,
        string secretAccessKey,
        Action<IntegratedS3Options>? configureOptions = null)
    {
        return _factory.CreateLoopbackIsolatedClientAsync(builder => {
            builder.Services.Configure<IntegratedS3Options>(options => {
                options.EnableAwsSignatureV4Authentication = true;
                options.AccessKeyCredentials =
                [
                    new IntegratedS3AccessKeyCredential
                    {
                        AccessKeyId = accessKeyId,
                        SecretAccessKey = secretAccessKey,
                        DisplayName = "aws-sdk-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
                configureOptions?.Invoke(options);
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });
    }

    private static string ComputeSha1Base64(string content)
    {
        return Convert.ToBase64String(SHA1.HashData(Encoding.UTF8.GetBytes(content)));
    }

    private static string ComputeMultipartSha1Base64(params string[] partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        foreach (var partChecksum in partChecksums) {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Length}";
    }

    private static string ComputeSha256Base64(string content)
    {
        return Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
    }

    private static string ComputeMultipartSha256Base64(params string[] partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var partChecksum in partChecksums) {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Length}";
    }

    private static AmazonS3Client CreateS3Client(Uri baseAddress, string accessKeyId, string secretAccessKey, bool forcePathStyle = true, string? hostOverride = null)
    {
        var endpointBaseAddress = hostOverride is null
            ? baseAddress
            : new UriBuilder(baseAddress)
            {
                Host = hostOverride
            }.Uri;

        var serviceUrl = new Uri(endpointBaseAddress, "/integrated-s3").ToString().TrimEnd('/');
        return new AmazonS3Client(
            new BasicAWSCredentials(accessKeyId, secretAccessKey),
            new AmazonS3Config
            {
                ServiceURL = serviceUrl,
                ForcePathStyle = forcePathStyle,
                UseHttp = true,
                AuthenticationRegion = "us-east-1"
            });
    }

    private sealed class ScopeBasedIntegratedS3AuthorizationService : IIntegratedS3AuthorizationService
    {
        public ValueTask<Abstractions.Results.StorageResult> AuthorizeAsync(System.Security.Claims.ClaimsPrincipal principal, Core.Models.StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var requiredScope = request.Operation switch
            {
                Core.Models.StorageOperationType.ListBuckets => "storage.read",
                Core.Models.StorageOperationType.HeadBucket => "storage.read",
                Core.Models.StorageOperationType.ListObjects => "storage.read",
                Core.Models.StorageOperationType.GetObject => "storage.read",
                Core.Models.StorageOperationType.GetBucketLocation => "storage.read",
                Core.Models.StorageOperationType.GetBucketCors => "storage.read",
                Core.Models.StorageOperationType.GetObjectTags => "storage.read",
                Core.Models.StorageOperationType.HeadObject => "storage.read",
                _ => "storage.write"
            };

            if (principal.HasClaim("scope", requiredScope)) {
                return ValueTask.FromResult(Abstractions.Results.StorageResult.Success());
            }

            return ValueTask.FromResult(Abstractions.Results.StorageResult.Failure(new Abstractions.Errors.StorageError
            {
                Code = Abstractions.Errors.StorageErrorCode.AccessDenied,
                Message = $"Missing required scope '{requiredScope}'.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                SuggestedHttpStatusCode = 403
            }));
        }
    }
}
