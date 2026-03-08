using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System.Net;
using System.Net.Http.Headers;
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
