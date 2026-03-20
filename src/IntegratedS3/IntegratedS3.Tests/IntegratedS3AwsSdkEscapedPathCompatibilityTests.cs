using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System.Net;
using System.Security.Claims;
using System.Text;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.AspNetCore;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Services;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3AwsSdkEscapedPathCompatibilityTests : IClassFixture<WebUiApplicationFactory>
{
    private readonly WebUiApplicationFactory _factory;

    public IntegratedS3AwsSdkEscapedPathCompatibilityTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task AmazonS3Client_PathStyleCrud_WithSpaceContainingKey_WorksAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-space-key-access";
        const string secretAccessKey = "aws-sdk-space-key-secret";
        const string bucketName = "aws-sdk-space-key-bucket";
        const string objectKey = "docs/folder with spaces/hello world.txt";
        const string payload = "hello from amazon sdk space key";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

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

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        using (var reader = new StreamReader(getObjectResponse.ResponseStream)) {
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }

        var listObjectsResponse = await s3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = "docs/folder with spaces/",
            MaxKeys = 1000
        });
        Assert.Equal(HttpStatusCode.OK, listObjectsResponse.HttpStatusCode);
        var listedObject = Assert.Single(listObjectsResponse.S3Objects);
        Assert.Equal(objectKey, listedObject.Key);

        var deleteObjectResponse = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.NoContent, deleteObjectResponse.HttpStatusCode);
    }

    [Fact]
    public async Task AmazonS3Client_SdkGeneratedPresignedUrls_WithSpaceContainingKey_CanUploadAndDownloadObjects()
    {
        const string accessKeyId = "aws-sdk-presign-space-access";
        const string secretAccessKey = "aws-sdk-presign-space-secret";
        const string bucketName = "aws-sdk-presign-space-bucket";
        const string objectKey = "docs/presigned folder/hello world.txt";
        const string payload = "uploaded via sdk presign with spaces";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var presignedPutUrl = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            Verb = HttpVerb.PUT,
            Protocol = Amazon.S3.Protocol.HTTP,
            Expires = DateTime.UtcNow.AddMinutes(5)
        });
        AssertEscapedSpaceSegments(presignedPutUrl);

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
        AssertEscapedSpaceSegments(presignedGetUrl);

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
                        DisplayName = "aws-sdk-space-key-user",
                        Scopes = ["storage.read", "storage.write"]
                    }
                ];
                configureOptions?.Invoke(options);
            });
            builder.Services.AddSingleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>();
        });
    }

    private static AmazonS3Client CreateS3Client(Uri baseAddress, string accessKeyId, string secretAccessKey)
    {
        var serviceUrl = new Uri(baseAddress, "/integrated-s3").ToString().TrimEnd('/');
        return new AmazonS3Client(
            new BasicAWSCredentials(accessKeyId, secretAccessKey),
            new AmazonS3Config
            {
                ServiceURL = serviceUrl,
                ForcePathStyle = true,
                UseHttp = true,
                AuthenticationRegion = "us-east-1"
            });
    }

    private static void AssertEscapedSpaceSegments(string requestUrl)
    {
        var requestUri = new Uri(requestUrl, UriKind.Absolute);
        Assert.Contains("presigned%20folder", requestUri.AbsoluteUri, StringComparison.Ordinal);
        Assert.Contains("hello%20world.txt", requestUri.AbsoluteUri, StringComparison.Ordinal);
    }

    private sealed class ScopeBasedIntegratedS3AuthorizationService : IIntegratedS3AuthorizationService
    {
        public ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var requiredScope = request.Operation switch
            {
                StorageOperationType.ListBuckets => "storage.read",
                StorageOperationType.HeadBucket => "storage.read",
                StorageOperationType.ListObjects => "storage.read",
                StorageOperationType.ListObjectVersions => "storage.read",
                StorageOperationType.ListMultipartUploads => "storage.read",
                StorageOperationType.ListMultipartParts => "storage.read",
                StorageOperationType.GetObject => "storage.read",
                StorageOperationType.GetBucketLocation => "storage.read",
                StorageOperationType.GetBucketCors => "storage.read",
                StorageOperationType.GetBucketDefaultEncryption => "storage.read",
                StorageOperationType.GetObjectTags => "storage.read",
                StorageOperationType.HeadObject => "storage.read",
                _ => "storage.write"
            };

            if (principal.HasClaim("scope", requiredScope)) {
                return ValueTask.FromResult(StorageResult.Success());
            }

            return ValueTask.FromResult(StorageResult.Failure(new StorageError
            {
                Code = StorageErrorCode.AccessDenied,
                Message = $"Missing required scope '{requiredScope}'.",
                BucketName = request.BucketName,
                ObjectKey = request.Key,
                SuggestedHttpStatusCode = 403
            }));
        }
    }
}
