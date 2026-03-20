using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using IntegratedS3.AspNetCore;
using IntegratedS3.Core.Services;
using IntegratedS3.Provider.S3.Internal;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;
using StorageBucketDefaultEncryptionConfiguration = IntegratedS3.Abstractions.Models.BucketDefaultEncryptionConfiguration;
using StorageBucketDefaultEncryptionRule = IntegratedS3.Abstractions.Models.BucketDefaultEncryptionRule;
using StorageBucketVersioningStatus = IntegratedS3.Abstractions.Models.BucketVersioningStatus;
using StorageCopyObjectMetadataDirective = IntegratedS3.Abstractions.Models.CopyObjectMetadataDirective;
using StorageMultipartUploadInfo = IntegratedS3.Abstractions.Models.MultipartUploadInfo;
using StorageMultipartUploadPart = IntegratedS3.Abstractions.Models.MultipartUploadPart;
using StorageObjectLegalHoldInfo = IntegratedS3.Abstractions.Models.ObjectLegalHoldInfo;
using StorageObjectRange = IntegratedS3.Abstractions.Models.ObjectRange;
using StorageObjectRetentionInfo = IntegratedS3.Abstractions.Models.ObjectRetentionInfo;
using StorageObjectServerSideEncryptionAlgorithm = IntegratedS3.Abstractions.Models.ObjectServerSideEncryptionAlgorithm;
using StorageObjectServerSideEncryptionInfo = IntegratedS3.Abstractions.Models.ObjectServerSideEncryptionInfo;
using StorageObjectServerSideEncryptionSettings = IntegratedS3.Abstractions.Models.ObjectServerSideEncryptionSettings;
using StorageObjectCustomerEncryptionSettings = IntegratedS3.Abstractions.Models.ObjectCustomerEncryptionSettings;
using StorageObjectTaggingDirective = IntegratedS3.Abstractions.Models.ObjectTaggingDirective;
using StoragePutBucketDefaultEncryptionRequest = IntegratedS3.Abstractions.Requests.PutBucketDefaultEncryptionRequest;
using StorageUploadPartCopyRequest = IntegratedS3.Abstractions.Requests.UploadPartCopyRequest;

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
        Assert.Null(listedObject.Owner);

        var deleteObjectResponse = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(System.Net.HttpStatusCode.NoContent, deleteObjectResponse.HttpStatusCode);
    }

    [Fact]
    public async Task AmazonS3Client_PathStyleAwsChunkedPutObject_WorksAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-chunked-access";
        const string secretAccessKey = "aws-sdk-chunked-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-chunked-bucket";
        const string objectKey = "docs/chunked.txt";
        const string payload = "hello from chunked amazon sdk";

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
            UseChunkEncoding = true
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        using var reader = new StreamReader(getObjectResponse.ResponseStream);
        Assert.Equal(payload, await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_LegacyAndFetchOwnerListApis_WorkAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-legacy-list-access";
        const string secretAccessKey = "aws-sdk-legacy-list-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-legacy-list-bucket";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        foreach (var key in new[]
                 {
                     "docs/a.txt",
                     "docs/b.txt",
                     "docs/c.txt"
                 }) {
            Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                ContentBody = key,
                ContentType = "text/plain",
                UseChunkEncoding = false
            })).HttpStatusCode);
        }

        var legacyResponse = await s3Client.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = bucketName,
            Prefix = "docs/",
            Marker = "docs/a.txt",
            MaxKeys = 1
        });

        Assert.Equal(HttpStatusCode.OK, legacyResponse.HttpStatusCode);
        Assert.True(legacyResponse.IsTruncated);
        var legacyObject = Assert.Single(legacyResponse.S3Objects);
        Assert.Equal("docs/b.txt", legacyObject.Key);
        Assert.NotNull(legacyObject.Owner);

        var v2Response = await s3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = "docs/",
            FetchOwner = true,
            MaxKeys = 1
        });

        Assert.Equal(HttpStatusCode.OK, v2Response.HttpStatusCode);
        var v2Object = Assert.Single(v2Response.S3Objects);
        Assert.Equal("docs/a.txt", v2Object.Key);
        Assert.NotNull(v2Object.Owner);
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
        var expectedChecksumCrc32 = putObjectResponse.ChecksumCRC32;
        Assert.False(string.IsNullOrWhiteSpace(expectedChecksumCrc32));
        Assert.True(string.IsNullOrWhiteSpace(putObjectResponse.ChecksumCRC32C));
        Assert.True(string.IsNullOrWhiteSpace(putObjectResponse.ChecksumSHA256));

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(expectedChecksumCrc32, metadataResponse.ChecksumCRC32);
        Assert.True(string.IsNullOrWhiteSpace(metadataResponse.ChecksumCRC32C));
        Assert.True(string.IsNullOrWhiteSpace(metadataResponse.ChecksumSHA256));

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
        Assert.Equal(expectedChecksumCrc32, copyResponse.ChecksumCRC32);
        Assert.True(string.IsNullOrWhiteSpace(copyResponse.ChecksumCRC32C));
        Assert.True(string.IsNullOrWhiteSpace(copyResponse.ChecksumSHA256));

        var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = targetBucketName,
            Key = targetKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
        Assert.Equal(expectedChecksumCrc32, copiedMetadataResponse.ChecksumCRC32);
        Assert.True(string.IsNullOrWhiteSpace(copiedMetadataResponse.ChecksumCRC32C));
        Assert.True(string.IsNullOrWhiteSpace(copiedMetadataResponse.ChecksumSHA256));

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
    public async Task AmazonS3Client_BucketDefaultEncryption_ControlPlaneCrud_RoundTripsAgainstS3Provider()
    {
        const string accessKeyId = "aws-sdk-bucket-encryption-access";
        const string secretAccessKey = "aws-sdk-bucket-encryption-secret";
        var storageClient = new AwsSdkCompatibilityS3Client();

        await using var isolatedClient = await CreateAuthenticatedLoopbackS3ProviderClientAsync(accessKeyId, secretAccessKey, storageClient);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-bucket-encryption-bucket";
        const string kmsKeyId = "alias/aws-sdk-default";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var putEncryptionResponse = await s3Client.PutBucketEncryptionAsync(CreateBucketEncryptionRequest(
            bucketName,
            ServerSideEncryptionMethod.AWSKMS,
            kmsKeyId));
        Assert.Equal(HttpStatusCode.OK, putEncryptionResponse.HttpStatusCode);

        var getEncryptionResponse = await s3Client.GetBucketEncryptionAsync(new GetBucketEncryptionRequest
        {
            BucketName = bucketName
        });
        Assert.Equal(HttpStatusCode.OK, getEncryptionResponse.HttpStatusCode);
        var encryptionRule = Assert.Single(getEncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);
        Assert.Equal(ServerSideEncryptionMethod.AWSKMS, encryptionRule.ServerSideEncryptionByDefault.ServerSideEncryptionAlgorithm);
        Assert.Equal(kmsKeyId, encryptionRule.ServerSideEncryptionByDefault.ServerSideEncryptionKeyManagementServiceKeyId);

        var deleteEncryptionResponse = await s3Client.DeleteBucketEncryptionAsync(new DeleteBucketEncryptionRequest
        {
            BucketName = bucketName
        });
        Assert.Equal(HttpStatusCode.NoContent, deleteEncryptionResponse.HttpStatusCode);

        var missingConfigurationException = await Assert.ThrowsAsync<AmazonS3Exception>(() => storageClient.GetBucketDefaultEncryptionAsync(bucketName));
        Assert.Equal("ServerSideEncryptionConfigurationNotFoundError", missingConfigurationException.ErrorCode);
        Assert.Equal(HttpStatusCode.NotFound, missingConfigurationException.StatusCode);
    }

    [Fact]
    public async Task AmazonS3Client_PutObject_WithoutExplicitServerSideEncryption_InheritsBucketDefaultAgainstS3Provider()
    {
        const string accessKeyId = "aws-sdk-put-default-encryption-access";
        const string secretAccessKey = "aws-sdk-put-default-encryption-secret";
        var storageClient = new AwsSdkCompatibilityS3Client();

        await using var isolatedClient = await CreateAuthenticatedLoopbackS3ProviderClientAsync(accessKeyId, secretAccessKey, storageClient);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-put-default-encryption-bucket";
        const string objectKey = "docs/default-encrypted.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketEncryptionAsync(CreateBucketEncryptionRequest(
            bucketName,
            ServerSideEncryptionMethod.AES256))).HttpStatusCode);

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "default encrypted payload",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);
        Assert.Equal(ServerSideEncryptionMethod.AES256, putObjectResponse.ServerSideEncryptionMethod);
        Assert.NotNull(storageClient.LastPutObjectServerSideEncryption);
        Assert.Equal(StorageObjectServerSideEncryptionAlgorithm.Aes256, storageClient.LastPutObjectServerSideEncryption!.Algorithm);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(ServerSideEncryptionMethod.AES256, metadataResponse.ServerSideEncryptionMethod);
    }

    [Fact]
    public async Task AmazonS3Client_CopyObject_WithoutExplicitDestinationEncryption_InheritsBucketDefaultAgainstS3Provider()
    {
        const string accessKeyId = "aws-sdk-copy-default-encryption-access";
        const string secretAccessKey = "aws-sdk-copy-default-encryption-secret";
        var storageClient = new AwsSdkCompatibilityS3Client();

        await using var isolatedClient = await CreateAuthenticatedLoopbackS3ProviderClientAsync(accessKeyId, secretAccessKey, storageClient);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string sourceBucketName = "aws-sdk-copy-default-encryption-source";
        const string destinationBucketName = "aws-sdk-copy-default-encryption-destination";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";
        const string kmsKeyId = "alias/aws-sdk-copy-default";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = sourceBucketName
        })).HttpStatusCode);
        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = destinationBucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = sourceBucketName,
            Key = sourceKey,
            ContentBody = "copy me",
            ContentType = "text/plain",
            UseChunkEncoding = false
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketEncryptionAsync(CreateBucketEncryptionRequest(
            destinationBucketName,
            ServerSideEncryptionMethod.AWSKMS,
            kmsKeyId))).HttpStatusCode);

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            DestinationBucket = destinationBucketName,
            DestinationKey = destinationKey
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
        Assert.Equal(ServerSideEncryptionMethod.AWSKMS, copyResponse.ServerSideEncryptionMethod);
        Assert.Equal(kmsKeyId, copyResponse.ServerSideEncryptionKeyManagementServiceKeyId);
        Assert.NotNull(storageClient.LastCopyObjectServerSideEncryption);
        Assert.Equal(StorageObjectServerSideEncryptionAlgorithm.Kms, storageClient.LastCopyObjectServerSideEncryption!.Algorithm);
        Assert.Equal(kmsKeyId, storageClient.LastCopyObjectServerSideEncryption.KeyId);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = destinationBucketName,
            Key = destinationKey
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(ServerSideEncryptionMethod.AWSKMS, metadataResponse.ServerSideEncryptionMethod);
        Assert.Equal(kmsKeyId, metadataResponse.ServerSideEncryptionKeyManagementServiceKeyId);
    }

    [Fact]
    public async Task AmazonS3Client_PutObject_ExplicitServerSideEncryption_OverridesBucketDefaultAgainstS3Provider()
    {
        const string accessKeyId = "aws-sdk-explicit-encryption-access";
        const string secretAccessKey = "aws-sdk-explicit-encryption-secret";
        var storageClient = new AwsSdkCompatibilityS3Client();

        await using var isolatedClient = await CreateAuthenticatedLoopbackS3ProviderClientAsync(accessKeyId, secretAccessKey, storageClient);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-explicit-encryption-bucket";
        const string objectKey = "docs/explicit-encrypted.txt";
        const string explicitKeyId = "alias/aws-sdk-explicit";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketEncryptionAsync(CreateBucketEncryptionRequest(
            bucketName,
            ServerSideEncryptionMethod.AES256))).HttpStatusCode);

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "override default encryption",
            ContentType = "text/plain",
            ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS,
            ServerSideEncryptionKeyManagementServiceKeyId = explicitKeyId,
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);
        Assert.Equal(ServerSideEncryptionMethod.AWSKMS, putObjectResponse.ServerSideEncryptionMethod);
        Assert.Equal(explicitKeyId, putObjectResponse.ServerSideEncryptionKeyManagementServiceKeyId);
        Assert.NotNull(storageClient.LastPutObjectServerSideEncryption);
        Assert.Equal(StorageObjectServerSideEncryptionAlgorithm.Kms, storageClient.LastPutObjectServerSideEncryption!.Algorithm);
        Assert.Equal(explicitKeyId, storageClient.LastPutObjectServerSideEncryption.KeyId);

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(ServerSideEncryptionMethod.AWSKMS, metadataResponse.ServerSideEncryptionMethod);
        Assert.Equal(explicitKeyId, metadataResponse.ServerSideEncryptionKeyManagementServiceKeyId);
    }

    [Fact]
    public async Task AmazonS3Client_CopyObject_WithChecksumAlgorithm_ExposesRequestedChecksumFieldsAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-copy-checksum-access";
        const string secretAccessKey = "aws-sdk-copy-checksum-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);
        Dictionary<string, string>? copyRequestHeaders = null;
        Dictionary<string, string>? copyResponseHeaders = null;

        void CaptureCopyRequestHeaders(object? _, RequestEventArgs eventArgs)
        {
            if (eventArgs is WebServiceRequestEventArgs { Request: CopyObjectRequest } requestEventArgs) {
                copyRequestHeaders = new Dictionary<string, string>(requestEventArgs.Headers, StringComparer.OrdinalIgnoreCase);
            }
        }

        void CaptureCopyResponseHeaders(object? _, ResponseEventArgs eventArgs)
        {
            if (eventArgs is WebServiceResponseEventArgs { Request: CopyObjectRequest, Response: CopyObjectResponse } responseEventArgs) {
                copyResponseHeaders = new Dictionary<string, string>(responseEventArgs.ResponseHeaders, StringComparer.OrdinalIgnoreCase);
            }
        }

        s3Client.BeforeRequestEvent += CaptureCopyRequestHeaders;
        s3Client.AfterResponseEvent += CaptureCopyResponseHeaders;

        const string bucketName = "aws-sdk-copy-checksum-bucket";
        const string sourceKey = "docs/source.txt";
        const string copiedObjectKey = "docs/copied.txt";
        const string payload = "copied with requested checksum algorithm";
        var expectedChecksumSha1 = ComputeSha1Base64(payload);

        try {
            Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
            {
                BucketName = bucketName
            })).HttpStatusCode);

            var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                ContentBody = payload,
                ContentType = "text/plain",
                UseChunkEncoding = false
            });
            Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

            var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = bucketName,
                SourceKey = sourceKey,
                DestinationBucket = bucketName,
                DestinationKey = copiedObjectKey,
                ChecksumAlgorithm = ChecksumAlgorithm.SHA1
            });
            Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
            Assert.NotNull(copyRequestHeaders);
            Assert.Equal("SHA1", copyRequestHeaders["x-amz-checksum-algorithm"]);
            Assert.NotNull(copyResponseHeaders);
            Assert.Equal("SHA1", copyResponseHeaders["x-amz-checksum-algorithm"]);
            Assert.Equal(expectedChecksumSha1, copyResponse.ChecksumSHA1);

            var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = copiedObjectKey,
                ChecksumMode = ChecksumMode.ENABLED
            });
            Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
            Assert.Equal(expectedChecksumSha1, copiedMetadataResponse.ChecksumSHA1);

            var copiedObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = copiedObjectKey,
                ChecksumMode = ChecksumMode.ENABLED
            });
            Assert.Equal(HttpStatusCode.OK, copiedObjectResponse.HttpStatusCode);
            Assert.Equal(expectedChecksumSha1, copiedObjectResponse.ChecksumSHA1);
            using var copiedReader = new StreamReader(copiedObjectResponse.ResponseStream);
            Assert.Equal(payload, await copiedReader.ReadToEndAsync());
        }
        finally {
            s3Client.BeforeRequestEvent -= CaptureCopyRequestHeaders;
            s3Client.AfterResponseEvent -= CaptureCopyResponseHeaders;
        }
    }

    [Fact]
    public async Task AmazonS3Client_CopyObjectOntoSelf_WithMetadataDirectiveReplace_UpdatesMtimeMetadataAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-copy-self-access";
        const string secretAccessKey = "aws-sdk-copy-self-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);
        Dictionary<string, string>? copyRequestHeaders = null;

        void CaptureCopyRequestHeaders(object? _, RequestEventArgs eventArgs)
        {
            if (eventArgs is WebServiceRequestEventArgs { Request: CopyObjectRequest } requestEventArgs) {
                copyRequestHeaders = new Dictionary<string, string>(requestEventArgs.Headers, StringComparer.OrdinalIgnoreCase);
            }
        }

        s3Client.BeforeRequestEvent += CaptureCopyRequestHeaders;

        const string bucketName = "aws-sdk-copy-self-bucket";
        const string objectKey = "docs/rclone.txt";
        const string payload = "same-key copy keeps the payload";
        const string originalMtime = "2024-10-11T12:13:14.123456789Z";
        const string replacementMtime = "2025-01-02T03:04:05.987654321Z";

        try {
            Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
            {
                BucketName = bucketName
            })).HttpStatusCode);

            var putObjectRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                ContentBody = payload,
                ContentType = "text/plain",
                UseChunkEncoding = false
            };
            putObjectRequest.Metadata["mtime"] = originalMtime;
            putObjectRequest.Metadata["source-only"] = "remove-me";

            var putObjectResponse = await s3Client.PutObjectAsync(putObjectRequest);
            Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

            var initialMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.Equal(HttpStatusCode.OK, initialMetadataResponse.HttpStatusCode);
            Assert.Equal(originalMtime, initialMetadataResponse.Metadata["mtime"]);
            Assert.Equal("remove-me", initialMetadataResponse.Metadata["source-only"]);

            var copyObjectRequest = new CopyObjectRequest
            {
                SourceBucket = bucketName,
                SourceKey = objectKey,
                DestinationBucket = bucketName,
                DestinationKey = objectKey,
                MetadataDirective = S3MetadataDirective.REPLACE,
                ContentType = "text/plain"
            };
            copyObjectRequest.Metadata["mtime"] = replacementMtime;
            copyObjectRequest.Metadata["updated-by"] = "rclone";

            var copyResponse = await s3Client.CopyObjectAsync(copyObjectRequest);
            Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
            Assert.NotNull(copyRequestHeaders);
            Assert.Equal("REPLACE", copyRequestHeaders["x-amz-metadata-directive"]);
            Assert.Equal(replacementMtime, copyRequestHeaders["x-amz-meta-mtime"]);
            Assert.False(copyRequestHeaders.ContainsKey("x-amz-meta-source-only"));

            var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
            Assert.Equal(replacementMtime, copiedMetadataResponse.Metadata["mtime"]);
            Assert.Equal("rclone", copiedMetadataResponse.Metadata["updated-by"]);
            Assert.Null(copiedMetadataResponse.Metadata["source-only"]);

            var copiedObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.Equal(HttpStatusCode.OK, copiedObjectResponse.HttpStatusCode);
            using var copiedReader = new StreamReader(copiedObjectResponse.ResponseStream);
            Assert.Equal(payload, await copiedReader.ReadToEndAsync());
        }
        finally {
            s3Client.BeforeRequestEvent -= CaptureCopyRequestHeaders;
        }
    }

    [Fact]
    public async Task AmazonS3Client_CopyObjectOntoSelf_WithSourceVersionIdAndMetadataDirectiveReplace_UsesHistoricalVersionAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-copy-self-version-access";
        const string secretAccessKey = "aws-sdk-copy-self-version-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-copy-self-version-bucket";
        const string objectKey = "docs/rclone-versioned.txt";
        const string historicalPayload = "version one payload";
        const string currentPayload = "version two payload";
        const string historicalMtime = "2024-10-11T12:13:14.123456789Z";
        const string currentMtime = "2024-12-24T01:02:03.123456789Z";
        const string replacementMtime = "2025-01-02T03:04:05.987654321Z";

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

        var historicalPutRequest = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = historicalPayload,
            ContentType = "text/plain",
            UseChunkEncoding = false
        };
        historicalPutRequest.Metadata["mtime"] = historicalMtime;
        historicalPutRequest.Metadata["source-only"] = "remove-me";

        var historicalPut = await s3Client.PutObjectAsync(historicalPutRequest);
        Assert.Equal(HttpStatusCode.OK, historicalPut.HttpStatusCode);

        var currentPutRequest = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = currentPayload,
            ContentType = "text/plain",
            UseChunkEncoding = false
        };
        currentPutRequest.Metadata["mtime"] = currentMtime;
        currentPutRequest.Metadata["current-only"] = "keep-current";

        var currentPut = await s3Client.PutObjectAsync(currentPutRequest);
        Assert.Equal(HttpStatusCode.OK, currentPut.HttpStatusCode);
        Assert.NotEqual(historicalPut.VersionId, currentPut.VersionId);

        var copyObjectRequest = new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = objectKey,
            SourceVersionId = historicalPut.VersionId,
            DestinationBucket = bucketName,
            DestinationKey = objectKey,
            MetadataDirective = S3MetadataDirective.REPLACE,
            ContentType = "text/plain"
        };
        copyObjectRequest.Metadata["mtime"] = replacementMtime;
        copyObjectRequest.Metadata["updated-by"] = "rclone";

        var copyResponse = await s3Client.CopyObjectAsync(copyObjectRequest);
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);
        Assert.Equal(historicalPut.VersionId, copyResponse.SourceVersionId);

        var historicalMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = historicalPut.VersionId
        });
        Assert.Equal(HttpStatusCode.OK, historicalMetadataResponse.HttpStatusCode);
        Assert.Equal(historicalMtime, historicalMetadataResponse.Metadata["mtime"]);
        Assert.Equal("remove-me", historicalMetadataResponse.Metadata["source-only"]);
        Assert.Null(historicalMetadataResponse.Metadata["updated-by"]);

        var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
        Assert.Equal(replacementMtime, copiedMetadataResponse.Metadata["mtime"]);
        Assert.Equal("rclone", copiedMetadataResponse.Metadata["updated-by"]);
        Assert.Null(copiedMetadataResponse.Metadata["source-only"]);
        Assert.Null(copiedMetadataResponse.Metadata["current-only"]);

        var copiedObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, copiedObjectResponse.HttpStatusCode);
        using var copiedReader = new StreamReader(copiedObjectResponse.ResponseStream);
        Assert.Equal(historicalPayload, await copiedReader.ReadToEndAsync());
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
    public async Task AmazonS3Client_DeleteObjects_HonorsQuietModeAndDeleteMarkerVersionFidelity()
    {
        const string accessKeyId = "aws-sdk-batch-delete-access";
        const string secretAccessKey = "aws-sdk-batch-delete-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-batch-delete-bucket";
        const string objectKey = "docs/history.txt";
        const string missingVersionId = "missing-version";

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

        var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "version one",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

        var quietDeleteException = await Assert.ThrowsAsync<DeleteObjectsException>(() => s3Client.DeleteObjectsAsync(new DeleteObjectsRequest
        {
            BucketName = bucketName,
            Quiet = true,
            Objects =
            [
                new KeyVersion
                {
                    Key = objectKey
                },
                new KeyVersion
                {
                    Key = objectKey,
                    VersionId = missingVersionId
                }
            ]
        }));

        var quietDeleteResponse = quietDeleteException.Response;
        Assert.Equal(HttpStatusCode.OK, quietDeleteResponse.HttpStatusCode);
        Assert.True(quietDeleteResponse.DeletedObjects is null || quietDeleteResponse.DeletedObjects.Count == 0);

        var quietDeleteError = Assert.Single(quietDeleteResponse.DeleteErrors);
        Assert.Equal(objectKey, quietDeleteError.Key);
        Assert.Equal(missingVersionId, quietDeleteError.VersionId);
        Assert.Equal("NoSuchVersion", quietDeleteError.Code);

        var currentGetException = await Assert.ThrowsAsync<NoSuchKeyException>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        }));
        Assert.Equal(HttpStatusCode.NotFound, currentGetException.StatusCode);

        var restoredPutResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "version two",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, restoredPutResponse.HttpStatusCode);

        var createDeleteMarkerResponse = await s3Client.DeleteObjectsAsync(new DeleteObjectsRequest
        {
            BucketName = bucketName,
            Objects =
            [
                new KeyVersion
                {
                    Key = objectKey
                }
            ]
        });
        Assert.Equal(HttpStatusCode.OK, createDeleteMarkerResponse.HttpStatusCode);

        var createdDeleteMarker = Assert.Single(createDeleteMarkerResponse.DeletedObjects);
        Assert.Equal(objectKey, createdDeleteMarker.Key);
        Assert.Null(createdDeleteMarker.VersionId);
        Assert.True(createdDeleteMarker.DeleteMarker is true);
        var deleteMarkerVersionId = Assert.IsType<string>(createdDeleteMarker.DeleteMarkerVersionId);

        var deleteDeleteMarkerResponse = await s3Client.DeleteObjectsAsync(new DeleteObjectsRequest
        {
            BucketName = bucketName,
            Objects =
            [
                new KeyVersion
                {
                    Key = objectKey,
                    VersionId = deleteMarkerVersionId
                }
            ]
        });

        Assert.Equal(HttpStatusCode.OK, deleteDeleteMarkerResponse.HttpStatusCode);
        var deletedDeleteMarker = Assert.Single(deleteDeleteMarkerResponse.DeletedObjects);
        Assert.Equal(objectKey, deletedDeleteMarker.Key);
        Assert.Equal(deleteMarkerVersionId, deletedDeleteMarker.VersionId);
        Assert.True(deletedDeleteMarker.DeleteMarker is true);
        Assert.Equal(deleteMarkerVersionId, deletedDeleteMarker.DeleteMarkerVersionId);
        Assert.True(deleteDeleteMarkerResponse.DeleteErrors is null || deleteDeleteMarkerResponse.DeleteErrors.Count == 0);

        var restoredObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, restoredObjectResponse.HttpStatusCode);
        using var reader = new StreamReader(restoredObjectResponse.ResponseStream);
        Assert.Equal("version two", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_PutObject_WithSha1Checksum_ExposesSdkChecksumFieldsAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-sha1-access";
        const string secretAccessKey = "aws-sdk-sha1-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);
        Dictionary<string, string>? putResponseHeaders = null;

        void CapturePutResponseHeaders(object? _, ResponseEventArgs eventArgs)
        {
            if (eventArgs is WebServiceResponseEventArgs { Request: PutObjectRequest, Response: PutObjectResponse } responseEventArgs) {
                putResponseHeaders = new Dictionary<string, string>(responseEventArgs.ResponseHeaders, StringComparer.OrdinalIgnoreCase);
            }
        }

        s3Client.AfterResponseEvent += CapturePutResponseHeaders;

        const string bucketName = "aws-sdk-sha1-bucket";
        const string objectKey = "docs/sha1.txt";
        const string copiedObjectKey = "docs/sha1-copy.txt";
        const string payload = "hello sha1 from amazon sdk";
        var checksum = ComputeSha1Base64(payload);

        try {
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
            var recordedPutResponseHeaders = putResponseHeaders
                ?? throw new Xunit.Sdk.XunitException("Expected PUT response headers.");
            Assert.Equal("SHA1", recordedPutResponseHeaders["x-amz-checksum-algorithm"]);
            Assert.False(recordedPutResponseHeaders.ContainsKey("x-amz-checksum-sha256"));
            Assert.False(recordedPutResponseHeaders.ContainsKey("x-amz-checksum-crc32"));
            Assert.False(recordedPutResponseHeaders.ContainsKey("x-amz-checksum-crc32c"));
            Assert.Equal(checksum, putObjectResponse.ChecksumSHA1);

            var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                ChecksumMode = ChecksumMode.ENABLED
            });
            Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
            Assert.Equal(checksum, metadataResponse.ChecksumSHA1);
            Assert.True(string.IsNullOrWhiteSpace(metadataResponse.ChecksumSHA256));
            Assert.True(string.IsNullOrWhiteSpace(metadataResponse.ChecksumCRC32));
            Assert.True(string.IsNullOrWhiteSpace(metadataResponse.ChecksumCRC32C));

            var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                ChecksumMode = ChecksumMode.ENABLED
            });
            Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
            Assert.Equal(checksum, getObjectResponse.ChecksumSHA1);
            Assert.True(string.IsNullOrWhiteSpace(getObjectResponse.ChecksumSHA256));
            Assert.True(string.IsNullOrWhiteSpace(getObjectResponse.ChecksumCRC32));
            Assert.True(string.IsNullOrWhiteSpace(getObjectResponse.ChecksumCRC32C));
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
            Assert.True(string.IsNullOrWhiteSpace(copyResponse.ChecksumSHA256));
            Assert.True(string.IsNullOrWhiteSpace(copyResponse.ChecksumCRC32));
            Assert.True(string.IsNullOrWhiteSpace(copyResponse.ChecksumCRC32C));

            var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = copiedObjectKey,
                ChecksumMode = ChecksumMode.ENABLED
            });
            Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
            Assert.Equal(checksum, copiedMetadataResponse.ChecksumSHA1);
            Assert.True(string.IsNullOrWhiteSpace(copiedMetadataResponse.ChecksumSHA256));
            Assert.True(string.IsNullOrWhiteSpace(copiedMetadataResponse.ChecksumCRC32));
            Assert.True(string.IsNullOrWhiteSpace(copiedMetadataResponse.ChecksumCRC32C));
        }
        finally {
            s3Client.AfterResponseEvent -= CapturePutResponseHeaders;
        }
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
        Assert.True(string.IsNullOrWhiteSpace(putObjectResponse.ChecksumSHA256));
        Assert.True(string.IsNullOrWhiteSpace(putObjectResponse.ChecksumSHA1));
        Assert.True(string.IsNullOrWhiteSpace(putObjectResponse.ChecksumCRC32));

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(checksum, metadataResponse.ChecksumCRC32C);
        Assert.True(string.IsNullOrWhiteSpace(metadataResponse.ChecksumSHA256));
        Assert.True(string.IsNullOrWhiteSpace(metadataResponse.ChecksumSHA1));
        Assert.True(string.IsNullOrWhiteSpace(metadataResponse.ChecksumCRC32));

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        Assert.Equal(checksum, getObjectResponse.ChecksumCRC32C);
        Assert.True(string.IsNullOrWhiteSpace(getObjectResponse.ChecksumSHA256));
        Assert.True(string.IsNullOrWhiteSpace(getObjectResponse.ChecksumSHA1));
        Assert.True(string.IsNullOrWhiteSpace(getObjectResponse.ChecksumCRC32));
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
        Assert.True(string.IsNullOrWhiteSpace(copyResponse.ChecksumSHA256));
        Assert.True(string.IsNullOrWhiteSpace(copyResponse.ChecksumSHA1));
        Assert.True(string.IsNullOrWhiteSpace(copyResponse.ChecksumCRC32));

        var copiedMetadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = copiedObjectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, copiedMetadataResponse.HttpStatusCode);
        Assert.Equal(checksum, copiedMetadataResponse.ChecksumCRC32C);
        Assert.True(string.IsNullOrWhiteSpace(copiedMetadataResponse.ChecksumSHA256));
        Assert.True(string.IsNullOrWhiteSpace(copiedMetadataResponse.ChecksumSHA1));
        Assert.True(string.IsNullOrWhiteSpace(copiedMetadataResponse.ChecksumCRC32));
    }

    [Fact]
    public async Task AmazonS3Client_PutObject_WithStreamingSha1ChecksumAlgorithm_UsesSignedTrailerChecksumAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-signed-trailer-put-access";
        const string secretAccessKey = "aws-sdk-signed-trailer-put-secret";
        const string checksumAlgorithmName = "SHA1";
        const string checksumTrailerHeaderName = "x-amz-checksum-sha1";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);
        Dictionary<string, string>? putRequestHeaders = null;

        void CapturePutRequestHeaders(object? _, ResponseEventArgs eventArgs)
        {
            if (eventArgs is WebServiceResponseEventArgs { Request: PutObjectRequest, Response: PutObjectResponse } responseEventArgs) {
                putRequestHeaders = new Dictionary<string, string>(responseEventArgs.RequestHeaders, StringComparer.OrdinalIgnoreCase);
            }
        }

        s3Client.AfterResponseEvent += CapturePutRequestHeaders;

        const string bucketName = "aws-sdk-signed-trailer-put-sha1";
        const string objectKey = "docs/signed-trailer-sha1.txt";
        const string payload = "hello signed trailer SHA1 from amazon sdk";
        var expectedChecksum = ComputeSha1Base64(payload);

        try {
            Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
            {
                BucketName = bucketName
            })).HttpStatusCode);

            // Keep the stream non-seekable so the SDK must emit the checksum via a signed trailer.
            await using var payloadStream = new NonSeekableMemoryStream(Encoding.UTF8.GetBytes(payload));
            var putObjectResponse = await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                InputStream = payloadStream,
                ContentType = "text/plain",
                UseChunkEncoding = true,
                DisablePayloadSigning = false,
                ChecksumAlgorithm = ChecksumAlgorithm.SHA1
            });
            Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);
            Assert.Equal(expectedChecksum, putObjectResponse.ChecksumSHA1);

            var recordedPutRequestHeaders = putRequestHeaders
                ?? throw new Xunit.Sdk.XunitException("Expected PUT request headers.");
            AssertSignedTrailerChecksumRequest(recordedPutRequestHeaders, checksumAlgorithmName, checksumTrailerHeaderName);

            var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                ChecksumMode = ChecksumMode.ENABLED
            });
            Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
            Assert.Equal(expectedChecksum, metadataResponse.ChecksumSHA1);

            var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                ChecksumMode = ChecksumMode.ENABLED
            });
            Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
            Assert.Equal(expectedChecksum, getObjectResponse.ChecksumSHA1);
            using var reader = new StreamReader(getObjectResponse.ResponseStream);
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }
        finally {
            s3Client.AfterResponseEvent -= CapturePutRequestHeaders;
        }
    }

    [Fact]
    public async Task AmazonS3Client_UploadPart_WithStreamingSha1ChecksumAlgorithm_UsesSignedTrailerChecksumAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-signed-trailer-part-access";
        const string secretAccessKey = "aws-sdk-signed-trailer-part-secret";
        const string checksumAlgorithmName = "SHA1";
        const string checksumTrailerHeaderName = "x-amz-checksum-sha1";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);
        Dictionary<string, string>? uploadPartRequestHeaders = null;

        void CaptureUploadPartRequestHeaders(object? _, ResponseEventArgs eventArgs)
        {
            if (eventArgs is WebServiceResponseEventArgs { Request: UploadPartRequest, Response: UploadPartResponse } responseEventArgs) {
                uploadPartRequestHeaders = new Dictionary<string, string>(responseEventArgs.RequestHeaders, StringComparer.OrdinalIgnoreCase);
            }
        }

        s3Client.AfterResponseEvent += CaptureUploadPartRequestHeaders;

        const string bucketName = "aws-sdk-signed-trailer-part-sha1";
        const string objectKey = "docs/signed-trailer-part-sha1.txt";
        const string payload = "hello signed trailer multipart SHA1 from amazon sdk";
        var expectedChecksum = ComputeSha1Base64(payload);
        string? uploadId = null;

        try {
            Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
            {
                BucketName = bucketName
            })).HttpStatusCode);

            var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                ContentType = "text/plain",
                ChecksumAlgorithm = ChecksumAlgorithm.SHA1
            });
            Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);
            uploadId = initiateResponse.UploadId;

            var payloadBytes = Encoding.UTF8.GetBytes(payload);
            // UploadPart still wraps the request body in a seekability-dependent PartialWrapperStream.
            await using var payloadStream = new MemoryStream(payloadBytes);
            var uploadPartResponse = await s3Client.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = uploadId,
                PartNumber = 1,
                InputStream = payloadStream,
                PartSize = payloadBytes.Length,
                IsLastPart = true,
                UseChunkEncoding = true,
                DisablePayloadSigning = false,
                ChecksumAlgorithm = ChecksumAlgorithm.SHA1
            });
            Assert.Equal(HttpStatusCode.OK, uploadPartResponse.HttpStatusCode);
            Assert.Equal(expectedChecksum, uploadPartResponse.ChecksumSHA1);

            var recordedUploadPartRequestHeaders = uploadPartRequestHeaders
                ?? throw new Xunit.Sdk.XunitException("Expected UploadPart request headers.");
            AssertSignedTrailerChecksumRequest(recordedUploadPartRequestHeaders, checksumAlgorithmName, checksumTrailerHeaderName);

            var listPartsResponse = await s3Client.ListPartsAsync(new ListPartsRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = uploadId
            });
            Assert.Equal(HttpStatusCode.OK, listPartsResponse.HttpStatusCode);
            var uploadedPart = Assert.Single(listPartsResponse.Parts);
            Assert.Equal(expectedChecksum, uploadedPart.ChecksumSHA1);
        }
        finally {
            s3Client.AfterResponseEvent -= CaptureUploadPartRequestHeaders;

            if (!string.IsNullOrWhiteSpace(uploadId)) {
                await s3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = objectKey,
                    UploadId = uploadId
                });
            }
        }
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
    public async Task AmazonS3Client_VirtualHostedStyleSdkGeneratedPresignedUrls_WithSignedContentType_CanUploadAndDownloadObjects()
    {
        const string accessKeyId = "aws-sdk-virtual-presign-content-type-access";
        const string secretAccessKey = "aws-sdk-virtual-presign-content-type-secret";
        const string contentType = "text/plain";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(
            accessKeyId,
            secretAccessKey,
            options => {
                options.EnableVirtualHostedStyleAddressing = true;
                options.VirtualHostedStyleHostSuffixes = ["localhost"];
            });

        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey, forcePathStyle: false, hostOverride: "localhost");

        const string bucketName = "aws-sdk-virtual-presign-content-type-bucket";
        const string objectKey = "docs/virtual-presigned-content-type.txt";
        const string payload = "uploaded via virtual hosted sdk presign with content type";

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
            Expires = DateTime.UtcNow.AddMinutes(5),
            ContentType = contentType
        });

        using (var presignedPutRequest = new HttpRequestMessage(HttpMethod.Put, presignedPutUrl)
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes(payload))
        }) {
            presignedPutRequest.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
            var presignedPutResponse = await isolatedClient.Client.SendAsync(presignedPutRequest);
            Assert.Equal(HttpStatusCode.OK, presignedPutResponse.StatusCode);
        }

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(contentType, metadataResponse.Headers.ContentType);

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
    public async Task AmazonS3Client_VirtualHostedStyleAwsChunkedPutObject_WorksAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-virtual-chunked-access";
        const string secretAccessKey = "aws-sdk-virtual-chunked-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(
            accessKeyId,
            secretAccessKey,
            options => {
                options.EnableVirtualHostedStyleAddressing = true;
                options.VirtualHostedStyleHostSuffixes = ["localhost"];
            });

        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey, forcePathStyle: false, hostOverride: "localhost");

        const string bucketName = "aws-sdk-virtual-chunked-bucket";
        const string objectKey = "docs/virtual-chunked.txt";
        const string payload = "hello from virtual hosted chunked sdk";

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
            UseChunkEncoding = true
        });
        Assert.Equal(HttpStatusCode.OK, putObjectResponse.HttpStatusCode);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        using var reader = new StreamReader(getObjectResponse.ResponseStream);
        Assert.Equal(payload, await reader.ReadToEndAsync());
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
    public async Task AmazonS3Client_CopyObject_CopySourceConditionalPrecedence_WorkAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-copy-precedence-access";
        const string secretAccessKey = "aws-sdk-copy-precedence-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string sourceBucketName = "aws-sdk-copy-precedence-source";
        const string targetBucketName = "aws-sdk-copy-precedence-target";
        const string sourceKey = "docs/source.txt";
        const string payload = "copied by amazon sdk precedence";

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

        var precedenceCopyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            DestinationBucket = targetBucketName,
            DestinationKey = "docs/precedence-copy.txt",
            ETagToMatch = metadataResponse.ETag,
            UnmodifiedSinceDate = metadataResponse.LastModified!.Value.ToUniversalTime().AddMinutes(-5)
        });
        Assert.Equal(HttpStatusCode.OK, precedenceCopyResponse.HttpStatusCode);

        var copiedObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = targetBucketName,
            Key = "docs/precedence-copy.txt"
        });
        Assert.Equal(HttpStatusCode.OK, copiedObjectResponse.HttpStatusCode);
        using (var copiedReader = new StreamReader(copiedObjectResponse.ResponseStream)) {
            Assert.Equal(payload, await copiedReader.ReadToEndAsync());
        }

        var failedPrecedenceCopyException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            DestinationBucket = targetBucketName,
            DestinationKey = "docs/blocked-precedence-copy.txt",
            ETagToNotMatch = metadataResponse.ETag,
            ModifiedSinceDate = metadataResponse.LastModified!.Value.ToUniversalTime().AddMinutes(-5)
        }));
        Assert.Equal(HttpStatusCode.PreconditionFailed, failedPrecedenceCopyException.StatusCode);

        var missingCopiedObjectException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = targetBucketName,
            Key = "docs/blocked-precedence-copy.txt"
        }));
        Assert.Equal(HttpStatusCode.NotFound, missingCopiedObjectException.StatusCode);
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
        const string completedPayload = "hello world";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-multipart-bucket";
        const string objectKey = "docs/multipart.txt";
        var expectedChecksumCrc32c = ChecksumTestAlgorithms.ComputeCrc32cBase64(completedPayload);

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

        var metadataResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, metadataResponse.HttpStatusCode);
        Assert.Equal(expectedChecksumCrc32c, metadataResponse.ChecksumCRC32C);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ChecksumMode = ChecksumMode.ENABLED
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        Assert.Equal(expectedChecksumCrc32c, getObjectResponse.ChecksumCRC32C);
        using var reader = new StreamReader(getObjectResponse.ResponseStream);
        Assert.Equal(completedPayload, await reader.ReadToEndAsync());
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
        Assert.NotNull(firstPage.MultipartUploads[0].Owner);
        Assert.NotNull(firstPage.MultipartUploads[0].Initiator);
        Assert.Equal("docs/alpha.txt", firstPage.MultipartUploads[1].Key);
        Assert.Equal(secondUploadId, firstPage.MultipartUploads[1].UploadId);
        Assert.NotNull(firstPage.MultipartUploads[1].Owner);
        Assert.NotNull(firstPage.MultipartUploads[1].Initiator);
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
    public async Task AmazonS3Client_ListParts_RespectsPartMarkersAndPerPartChecksums()
    {
        const string accessKeyId = "aws-sdk-list-parts-access";
        const string secretAccessKey = "aws-sdk-list-parts-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-list-parts-bucket";
        const string objectKey = "docs/list-parts.txt";
        const string part1Payload = "hello ";
        const string part2Payload = "world";
        const string part3Payload = "!";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentType = "text/plain",
            ChecksumAlgorithm = ChecksumAlgorithm.SHA256
        });
        Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);

        async Task<(UploadPartResponse Response, string Checksum)> UploadPartAsync(int partNumber, string payload, bool isLastPart)
        {
            var checksum = ComputeSha256Base64(payload);
            await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
            var response = await s3Client.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = initiateResponse.UploadId,
                PartNumber = partNumber,
                InputStream = stream,
                PartSize = stream.Length,
                IsLastPart = isLastPart,
                ChecksumAlgorithm = ChecksumAlgorithm.SHA256,
                ChecksumSHA256 = checksum
            });
            Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
            return (response, checksum);
        }

        var part1 = await UploadPartAsync(1, part1Payload, isLastPart: false);
        var part2 = await UploadPartAsync(2, part2Payload, isLastPart: false);
        var part3 = await UploadPartAsync(3, part3Payload, isLastPart: true);

        var firstPage = await s3Client.ListPartsAsync(new ListPartsRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            MaxParts = 2
        });

        Assert.Equal(HttpStatusCode.OK, firstPage.HttpStatusCode);
        Assert.True(firstPage.IsTruncated);
        Assert.Equal(2, firstPage.NextPartNumberMarker);
        Assert.Equal(2, firstPage.Parts.Count);
        Assert.Equal(1, firstPage.Parts[0].PartNumber);
        Assert.Equal(part1.Response.ETag, firstPage.Parts[0].ETag);
        Assert.Equal(part1.Checksum, firstPage.Parts[0].ChecksumSHA256);
        Assert.Equal(2, firstPage.Parts[1].PartNumber);
        Assert.Equal(part2.Response.ETag, firstPage.Parts[1].ETag);
        Assert.Equal(part2.Checksum, firstPage.Parts[1].ChecksumSHA256);

        var secondPage = await s3Client.ListPartsAsync(new ListPartsRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            UploadId = initiateResponse.UploadId,
            PartNumberMarker = "2"
        });

        Assert.Equal(HttpStatusCode.OK, secondPage.HttpStatusCode);
        Assert.False(secondPage.IsTruncated);
        var remainingPart = Assert.Single(secondPage.Parts);
        Assert.Equal(3, remainingPart.PartNumber);
        Assert.Equal(part3.Response.ETag, remainingPart.ETag);
        Assert.Equal(part3.Checksum, remainingPart.ChecksumSHA256);
    }

    [Fact]
    public async Task AmazonS3Client_UploadPartCopy_WorksAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-copy-part-access";
        const string secretAccessKey = "aws-sdk-copy-part-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-copy-part-bucket";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";

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
            Key = sourceKey,
            ContentBody = "hello world",
            ContentType = "text/plain"
        });
        Assert.Equal(HttpStatusCode.OK, v1Put.HttpStatusCode);

        var v1Metadata = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = sourceKey,
            VersionId = v1Put.VersionId
        });
        Assert.Equal(HttpStatusCode.OK, v1Metadata.HttpStatusCode);

        var v2Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = sourceKey,
            ContentBody = "goodbye world",
            ContentType = "text/plain"
        });
        Assert.Equal(HttpStatusCode.OK, v2Put.HttpStatusCode);
        Assert.NotEqual(v1Put.VersionId, v2Put.VersionId);

        var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = destinationKey,
            ContentType = "text/plain",
            ChecksumAlgorithm = ChecksumAlgorithm.SHA256
        });
        Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);

        var copiedPartResponse = await s3Client.CopyPartAsync(new CopyPartRequest
        {
            SourceBucket = bucketName,
            SourceKey = sourceKey,
            SourceVersionId = v1Put.VersionId,
            DestinationBucket = bucketName,
            DestinationKey = destinationKey,
            UploadId = initiateResponse.UploadId,
            PartNumber = 1,
            FirstByte = 6,
            LastByte = 10,
            ETagToMatch =
            [
                v1Metadata.ETag
            ]
        });

        Assert.Equal(HttpStatusCode.OK, copiedPartResponse.HttpStatusCode);
        Assert.Equal(v1Put.VersionId, copiedPartResponse.CopySourceVersionId);
        Assert.Equal(ComputeSha256Base64("world"), copiedPartResponse.ChecksumSHA256);

        var completeResponse = await s3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = destinationKey,
            UploadId = initiateResponse.UploadId,
            PartETags =
            [
                new PartETag(1, copiedPartResponse.ETag)
            ]
        });
        Assert.Equal(HttpStatusCode.OK, completeResponse.HttpStatusCode);

        var getObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = destinationKey
        });
        Assert.Equal(HttpStatusCode.OK, getObjectResponse.HttpStatusCode);
        using var reader = new StreamReader(getObjectResponse.ResponseStream);
        Assert.Equal("world", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_UploadPartCopy_WithInjectedChecksumAlgorithmHeader_ExposesRequestedChecksumFieldsAgainstIntegratedS3()
    {
        const string accessKeyId = "aws-sdk-copy-part-checksum-access";
        const string secretAccessKey = "aws-sdk-copy-part-checksum-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);
        Dictionary<string, string>? copyPartRequestHeaders = null;
        Dictionary<string, string>? copyPartResponseHeaders = null;

        void CaptureAndInjectCopyPartRequestHeaders(object? _, RequestEventArgs eventArgs)
        {
            if (eventArgs is WebServiceRequestEventArgs { Request: CopyPartRequest } requestEventArgs) {
                requestEventArgs.Headers["x-amz-checksum-algorithm"] = "SHA1";
                copyPartRequestHeaders = new Dictionary<string, string>(requestEventArgs.Headers, StringComparer.OrdinalIgnoreCase);
            }
        }

        void CaptureCopyPartResponseHeaders(object? _, ResponseEventArgs eventArgs)
        {
            if (eventArgs is WebServiceResponseEventArgs { Request: CopyPartRequest, Response: CopyPartResponse } responseEventArgs) {
                copyPartResponseHeaders = new Dictionary<string, string>(responseEventArgs.ResponseHeaders, StringComparer.OrdinalIgnoreCase);
            }
        }

        const string bucketName = "aws-sdk-copy-part-checksum-bucket";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";
        const string sourcePayload = "hello world";
        const string copiedPayload = "world";
        var expectedChecksumSha1 = ComputeSha1Base64(copiedPayload);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = sourceKey,
            ContentBody = sourcePayload,
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putResponse.HttpStatusCode);

        var sourceMetadata = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = sourceKey
        });
        Assert.Equal(HttpStatusCode.OK, sourceMetadata.HttpStatusCode);

        var initiateResponse = await s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = destinationKey,
            ContentType = "text/plain"
        });
        Assert.Equal(HttpStatusCode.OK, initiateResponse.HttpStatusCode);

        s3Client.BeforeRequestEvent += CaptureAndInjectCopyPartRequestHeaders;
        s3Client.AfterResponseEvent += CaptureCopyPartResponseHeaders;

        try {
            // CopyPartRequest does not currently expose a checksum algorithm property, so inject the wire header directly.
            var copiedPartResponse = await s3Client.CopyPartAsync(new CopyPartRequest
            {
                SourceBucket = bucketName,
                SourceKey = sourceKey,
                DestinationBucket = bucketName,
                DestinationKey = destinationKey,
                UploadId = initiateResponse.UploadId,
                PartNumber = 1,
                FirstByte = 6,
                LastByte = 10,
                ETagToMatch =
                [
                    sourceMetadata.ETag
                ]
            });

            Assert.Equal(HttpStatusCode.OK, copiedPartResponse.HttpStatusCode);
            var recordedCopyPartRequestHeaders = copyPartRequestHeaders
                ?? throw new Xunit.Sdk.XunitException("Expected CopyPart request headers.");
            Assert.Equal("SHA1", recordedCopyPartRequestHeaders["x-amz-checksum-algorithm"]);

            var recordedCopyPartResponseHeaders = copyPartResponseHeaders
                ?? throw new Xunit.Sdk.XunitException("Expected CopyPart response headers.");
            Assert.Equal(expectedChecksumSha1, recordedCopyPartResponseHeaders["x-amz-checksum-sha1"]);
            Assert.False(recordedCopyPartResponseHeaders.ContainsKey("x-amz-checksum-sha256"));

            Assert.Equal(expectedChecksumSha1, copiedPartResponse.ChecksumSHA1);
            Assert.True(string.IsNullOrWhiteSpace(copiedPartResponse.ChecksumSHA256));
        }
        finally {
            s3Client.BeforeRequestEvent -= CaptureAndInjectCopyPartRequestHeaders;
            s3Client.AfterResponseEvent -= CaptureCopyPartResponseHeaders;
        }
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

        var missingVersionGetException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = "missing-version"
        }));
        Assert.Equal(HttpStatusCode.NotFound, missingVersionGetException.StatusCode);
        Assert.Equal("NoSuchVersion", missingVersionGetException.ErrorCode);

        var missingVersionHeadException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = "missing-version"
        }));
        Assert.Equal(HttpStatusCode.NotFound, missingVersionHeadException.StatusCode);

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

        var missingVersionCopyException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = sourceBucketName,
            SourceKey = sourceKey,
            SourceVersionId = "missing-version",
            DestinationBucket = targetBucketName,
            DestinationKey = "docs/missing-version-copy.txt"
        }));
        Assert.Equal(HttpStatusCode.NotFound, missingVersionCopyException.StatusCode);
        Assert.Equal("NoSuchVersion", missingVersionCopyException.ErrorCode);

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

    [Fact]
    public async Task AmazonS3Client_PutBucketVersioning_EnablesThenSuspendsVersioning()
    {
        const string accessKeyId = "aws-sdk-versioning-control-access";
        const string secretAccessKey = "aws-sdk-versioning-control-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-versioning-control-bucket";
        const string objectKey1 = "docs/versioned1.txt";
        const string objectKey2 = "docs/versioned2.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        // Enable versioning
        var enableVersioningResponse = await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Enabled
            }
        });
        Assert.Equal(HttpStatusCode.OK, enableVersioningResponse.HttpStatusCode);

        // Put first object with versioning enabled
        var v1Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey1,
            ContentBody = "version one",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, v1Put.HttpStatusCode);
        var v1VersionId = v1Put.VersionId!;
        Assert.False(string.IsNullOrWhiteSpace(v1VersionId));

        // Put second object, should have different version ID
        var v2Put = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey1,
            ContentBody = "version two",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, v2Put.HttpStatusCode);
        var v2VersionId = v2Put.VersionId!;
        Assert.NotEqual(v1VersionId, v2VersionId);

        // Suspend versioning
        var suspendVersioningResponse = await s3Client.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            VersioningConfig = new S3BucketVersioningConfig
            {
                Status = VersionStatus.Suspended
            }
        });
        Assert.Equal(HttpStatusCode.OK, suspendVersioningResponse.HttpStatusCode);

        // Put object on a new key with versioning suspended
        var suspendedPut = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey2,
            ContentBody = "suspended version",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, suspendedPut.HttpStatusCode);

        // Get the versioning status back to verify it's suspended
        var getVersioningResponse = await s3Client.GetBucketVersioningAsync(new GetBucketVersioningRequest
        {
            BucketName = bucketName
        });
        Assert.Equal(VersionStatus.Suspended, getVersioningResponse.VersioningConfig.Status);
    }

    [Fact]
    public async Task AmazonS3Client_DeleteMarkerVersionId_AvailableInResponse()
    {
        const string accessKeyId = "aws-sdk-delete-marker-version-id-access";
        const string secretAccessKey = "aws-sdk-delete-marker-version-id-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-delete-marker-version-id-bucket";
        const string objectKey = "docs/deletable.txt";

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

        // Put an object
        var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "content to delete",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putResponse.HttpStatusCode);

        // Delete the object to create delete marker
        var deleteResponse = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });

        // Verify response contains delete marker indicators
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.HttpStatusCode);
        Assert.Equal("true", deleteResponse.DeleteMarker);
        var deleteMarkerVersionId = Assert.IsType<string>(deleteResponse.VersionId);
        Assert.False(string.IsNullOrWhiteSpace(deleteMarkerVersionId));

        // Verify that getting the object now fails (delete marker is current)
        var getCurrentException = await Assert.ThrowsAsync<NoSuchKeyException>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        }));
        Assert.Equal(HttpStatusCode.NotFound, getCurrentException.StatusCode);
        Assert.Equal("NoSuchKey", getCurrentException.ErrorCode);

        // Verify we can retrieve the object before the delete marker
        var getHistoricalResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = putResponse.VersionId
        });
        Assert.Equal(HttpStatusCode.OK, getHistoricalResponse.HttpStatusCode);
        Assert.Equal(putResponse.VersionId, getHistoricalResponse.VersionId);
        using var reader = new StreamReader(getHistoricalResponse.ResponseStream);
        Assert.Equal("content to delete", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task AmazonS3Client_DeleteObject_WithVersionId_DeletesHistoricalVersionWithoutCreatingDeleteMarker()
    {
        const string accessKeyId = "aws-sdk-delete-version-id-access";
        const string secretAccessKey = "aws-sdk-delete-version-id-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        var bucketName = $"aws-sdk-delete-version-id-{Guid.NewGuid():N}";
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

        var firstPut = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "version one",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, firstPut.HttpStatusCode);
        var firstVersionId = Assert.IsType<string>(firstPut.VersionId);

        var secondPut = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "version two",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, secondPut.HttpStatusCode);
        var secondVersionId = Assert.IsType<string>(secondPut.VersionId);
        Assert.NotEqual(firstVersionId, secondVersionId);

        var deleteHistoricalVersion = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = firstVersionId
        });

        Assert.Equal(HttpStatusCode.NoContent, deleteHistoricalVersion.HttpStatusCode);
        Assert.Equal(firstVersionId, deleteHistoricalVersion.VersionId);
        Assert.True(string.IsNullOrEmpty(deleteHistoricalVersion.DeleteMarker));

        var currentObjectResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });
        Assert.Equal(HttpStatusCode.OK, currentObjectResponse.HttpStatusCode);
        Assert.Equal(secondVersionId, currentObjectResponse.VersionId);
        using (var reader = new StreamReader(currentObjectResponse.ResponseStream)) {
            Assert.Equal("version two", await reader.ReadToEndAsync());
        }

        var deletedVersionException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = firstVersionId
        }));
        Assert.Equal(HttpStatusCode.NotFound, deletedVersionException.StatusCode);
        Assert.Equal("NoSuchVersion", deletedVersionException.ErrorCode);

        var versionsResponse = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = objectKey
        });

        Assert.Equal(HttpStatusCode.OK, versionsResponse.HttpStatusCode);
        var remainingVersion = Assert.Single(versionsResponse.Versions);
        Assert.Equal(objectKey, remainingVersion.Key);
        Assert.Equal(secondVersionId, remainingVersion.VersionId);
        Assert.True(remainingVersion.IsLatest ?? false);
        Assert.False(remainingVersion.IsDeleteMarker ?? false);
    }

    [Fact]
    public async Task AmazonS3Client_GetObjectWithVersionId_RetrievesSpecificVersion()
    {
        const string accessKeyId = "aws-sdk-get-version-id-access";
        const string secretAccessKey = "aws-sdk-get-version-id-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-get-version-id-bucket";
        const string objectKey = "docs/multi-version.txt";

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

        // Create multiple versions
        var versionIds = new List<string>();
        var contents = new[] { "first content", "second content", "third content" };

        foreach (var content in contents)
        {
            var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                ContentBody = content,
                ContentType = "text/plain",
                UseChunkEncoding = false
            });
            Assert.Equal(HttpStatusCode.OK, putResponse.HttpStatusCode);
            versionIds.Add(putResponse.VersionId!);
        }

        // Verify all versions have different IDs
        Assert.NotEqual(versionIds[0], versionIds[1]);
        Assert.NotEqual(versionIds[1], versionIds[2]);

        // Retrieve each specific version and verify content
        for (int i = 0; i < versionIds.Count; i++)
        {
            var getResponse = await s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = versionIds[i]
            });
            Assert.Equal(HttpStatusCode.OK, getResponse.HttpStatusCode);
            Assert.Equal(versionIds[i], getResponse.VersionId);
            using var reader = new StreamReader(getResponse.ResponseStream);
            Assert.Equal(contents[i], await reader.ReadToEndAsync());
        }

        // Test invalid version ID
        var invalidVersionException = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            VersionId = "invalid-version-id"
        }));
        Assert.Equal(HttpStatusCode.NotFound, invalidVersionException.StatusCode);
        Assert.Equal("NoSuchVersion", invalidVersionException.ErrorCode);
    }

    [Fact]
    public async Task AmazonS3Client_ListVersions_ResponseContainsVersionsAndDeleteMarkers()
    {
        const string accessKeyId = "aws-sdk-list-versions-response-access";
        const string secretAccessKey = "aws-sdk-list-versions-response-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-list-versions-response-bucket";
        const string objectKey = "docs/complex.txt";

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

        // Create multiple versions
        var v1 = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "v1",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        var v2 = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "v2",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        // Create delete marker
        var deleteMarker = await s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey
        });

        // Create another version after delete marker
        var v3 = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "v3",
            ContentType = "text/plain",
            UseChunkEncoding = false
        });

        // List versions
        var listResponse = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = objectKey
        });

        Assert.Equal(HttpStatusCode.OK, listResponse.HttpStatusCode);
        Assert.NotNull(listResponse.Versions);

        // Verify versions are in response
        Assert.Contains(listResponse.Versions, v => v.VersionId == v1.VersionId && v.Key == objectKey);
        Assert.Contains(listResponse.Versions, v => v.VersionId == v2.VersionId && v.Key == objectKey);
        Assert.Contains(listResponse.Versions, v => v.VersionId == v3.VersionId && v.Key == objectKey);

        // Verify delete marker is in response (delete markers are in Versions collection with IsDeleteMarker = true)
        var deleteMarkerEntry = Assert.Single(listResponse.Versions, dm => dm.VersionId == deleteMarker.VersionId && (dm.IsDeleteMarker ?? false));
        Assert.Equal(objectKey, deleteMarkerEntry.Key);
        Assert.True(deleteMarkerEntry.IsDeleteMarker);
        Assert.False(deleteMarkerEntry.IsLatest ?? false); // Delete marker is not latest since v3 is latest

        // Verify latest version is v3
        var latestVersion = listResponse.Versions.Single(v => v.IsLatest == true);
        Assert.Equal(v3.VersionId, latestVersion.VersionId);
    }

    [Fact]
    public async Task AmazonS3Client_ListVersions_PaginationWithMarkers()
    {
        const string accessKeyId = "aws-sdk-list-versions-pagination-access";
        const string secretAccessKey = "aws-sdk-list-versions-pagination-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-list-versions-pagination-bucket";

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

        // Create multiple objects with multiple versions each
        var allVersions = new Dictionary<string, List<string>>();
        foreach (var key in new[] { "a.txt", "b.txt", "c.txt" })
        {
            allVersions[key] = new List<string>();
            for (int i = 0; i < 3; i++)
            {
                var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentBody = $"{key} v{i}",
                    ContentType = "text/plain",
                    UseChunkEncoding = false
                });
                allVersions[key].Add(putResponse.VersionId!);
            }
        }

        // Get first page with small MaxKeys
        var page1 = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            MaxKeys = 2
        });

        Assert.Equal(HttpStatusCode.OK, page1.HttpStatusCode);
        Assert.True(page1.IsTruncated);
        Assert.NotNull(page1.NextKeyMarker);
        Assert.NotNull(page1.NextVersionIdMarker);
        Assert.Equal(2, page1.Versions.Count);

        // Get next page using markers
        var page2 = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            MaxKeys = 2,
            KeyMarker = page1.NextKeyMarker,
            VersionIdMarker = page1.NextVersionIdMarker
        });

        Assert.Equal(HttpStatusCode.OK, page2.HttpStatusCode);
        Assert.NotEqual(page1.Versions.First().VersionId, page2.Versions.First().VersionId);

        // Verify pagination provides different content
        var allPaginatedVersions = new HashSet<string>(page1.Versions.Select(v => v.VersionId));
        foreach (var version in page2.Versions)
        {
            Assert.DoesNotContain(version.VersionId, allPaginatedVersions);
            allPaginatedVersions.Add(version.VersionId);
        }
    }

    [Fact]
    public async Task AmazonS3Client_ListVersions_PrefixFiltering()
    {
        const string accessKeyId = "aws-sdk-list-versions-prefix-access";
        const string secretAccessKey = "aws-sdk-list-versions-prefix-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-list-versions-prefix-bucket";

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

        // Create objects with different prefixes
        var keys = new[] { "docs/file1.txt", "docs/file2.txt", "logs/app.log", "logs/error.log", "data/info.json" };
        foreach (var key in keys)
        {
            var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                ContentBody = key,
                ContentType = "text/plain",
                UseChunkEncoding = false
            });
            Assert.Equal(HttpStatusCode.OK, putResponse.HttpStatusCode);
        }

        // List versions with "docs/" prefix
        var docsResponse = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = "docs/"
        });

        Assert.Equal(HttpStatusCode.OK, docsResponse.HttpStatusCode);
        Assert.All(docsResponse.Versions, v => Assert.StartsWith("docs/", v.Key));
        Assert.Equal(2, docsResponse.Versions.Count);

        // List versions with "logs/" prefix
        var logsResponse = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = "logs/"
        });

        Assert.Equal(HttpStatusCode.OK, logsResponse.HttpStatusCode);
        Assert.All(logsResponse.Versions, v => Assert.StartsWith("logs/", v.Key));
        Assert.Equal(2, logsResponse.Versions.Count);

        // List versions with "data/" prefix
        var dataResponse = await s3Client.ListVersionsAsync(new ListVersionsRequest
        {
            BucketName = bucketName,
            Prefix = "data/"
        });

        Assert.Equal(HttpStatusCode.OK, dataResponse.HttpStatusCode);
        Assert.Single(dataResponse.Versions);
        Assert.Equal("data/info.json", dataResponse.Versions[0].Key);
    }

    [Fact]
    public async Task AmazonS3Client_PutObject_WithSseC_DiskProviderRejectsGracefully()
    {
        const string accessKeyId = "aws-sdk-ssec-put-reject-access";
        const string secretAccessKey = "aws-sdk-ssec-put-reject-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-put-reject-bucket";
        const string objectKey = "docs/ssec-put.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        // The disk provider rejects SSE-C with a non-500 error
        var exception = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "ssec payload",
            ContentType = "text/plain",
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5,
            UseChunkEncoding = false
        }));
        Assert.NotEqual(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.True(((int)exception.StatusCode >= 400 && (int)exception.StatusCode < 500) || exception.StatusCode == (HttpStatusCode)501,
            $"Expected 4xx/501 status code but got {(int)exception.StatusCode}");
    }

    [Fact]
    public async Task AmazonS3Client_GetObject_WithSseC_DiskProviderRejectsGracefully()
    {
        const string accessKeyId = "aws-sdk-ssec-get-reject-access";
        const string secretAccessKey = "aws-sdk-ssec-get-reject-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-get-reject-bucket";
        const string objectKey = "docs/ssec-get.txt";
        const string payload = "normal payload for ssec get test";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = payload,
            ContentType = "text/plain",
            UseChunkEncoding = false
        })).HttpStatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        // The disk provider rejects SSE-C headers; verify graceful rejection
        var exception = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5
        }));
        Assert.NotEqual(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.True(((int)exception.StatusCode >= 400 && (int)exception.StatusCode < 500) || exception.StatusCode == (HttpStatusCode)501,
            $"Expected 4xx/501 status code but got {(int)exception.StatusCode}");
    }

    [Fact]
    public async Task AmazonS3Client_HeadObject_WithSseC_DiskProviderRejectsGracefully()
    {
        const string accessKeyId = "aws-sdk-ssec-head-reject-access";
        const string secretAccessKey = "aws-sdk-ssec-head-reject-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-head-reject-bucket";
        const string objectKey = "docs/ssec-head.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "normal payload for ssec head test",
            ContentType = "text/plain",
            UseChunkEncoding = false
        })).HttpStatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        // The disk provider rejects SSE-C headers; verify graceful rejection
        var exception = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5
        }));
        Assert.NotEqual(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.True(((int)exception.StatusCode >= 400 && (int)exception.StatusCode < 500) || exception.StatusCode == (HttpStatusCode)501,
            $"Expected 4xx/501 status code but got {(int)exception.StatusCode}");
    }

    [Fact]
    public async Task AmazonS3Client_CopyObject_WithDestinationSseC_DiskProviderRejectsGracefully()
    {
        const string accessKeyId = "aws-sdk-ssec-copy-dest-reject-access";
        const string secretAccessKey = "aws-sdk-ssec-copy-dest-reject-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-copy-dest-reject-bucket";
        const string sourceKey = "docs/ssec-copy-source.txt";
        const string destKey = "docs/ssec-copy-dest.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = sourceKey,
            ContentBody = "source payload for ssec copy dest test",
            ContentType = "text/plain",
            UseChunkEncoding = false
        })).HttpStatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        // The disk provider rejects SSE-C headers on copy; verify graceful rejection
        var exception = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = sourceKey,
            DestinationBucket = bucketName,
            DestinationKey = destKey,
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5
        }));
        Assert.NotEqual(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.True(((int)exception.StatusCode >= 400 && (int)exception.StatusCode < 500) || exception.StatusCode == (HttpStatusCode)501,
            $"Expected 4xx/501 status code but got {(int)exception.StatusCode}");
    }

    [Fact]
    public async Task AmazonS3Client_CopyObject_WithSourceSseC_DiskProviderRejectsGracefully()
    {
        const string accessKeyId = "aws-sdk-ssec-copy-src-reject-access";
        const string secretAccessKey = "aws-sdk-ssec-copy-src-reject-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-copy-src-reject-bucket";
        const string sourceKey = "docs/ssec-copy-source-enc.txt";
        const string destKey = "docs/ssec-copy-dest-from-enc.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = sourceKey,
            ContentBody = "source payload for ssec copy source test",
            ContentType = "text/plain",
            UseChunkEncoding = false
        })).HttpStatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        // The disk provider rejects copy-source SSE-C headers; verify graceful rejection
        var exception = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = sourceKey,
            DestinationBucket = bucketName,
            DestinationKey = destKey,
            CopySourceServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            CopySourceServerSideEncryptionCustomerProvidedKey = customerKey,
            CopySourceServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5
        }));
        Assert.NotEqual(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.True(((int)exception.StatusCode >= 400 && (int)exception.StatusCode < 500) || exception.StatusCode == (HttpStatusCode)501,
            $"Expected 4xx/501 status code but got {(int)exception.StatusCode}");
    }

    [Fact]
    public async Task AmazonS3Client_InitiateMultipartUpload_WithSseC_DiskProviderRejectsGracefully()
    {
        const string accessKeyId = "aws-sdk-ssec-multipart-reject-access";
        const string secretAccessKey = "aws-sdk-ssec-multipart-reject-secret";

        await using var isolatedClient = await CreateAuthenticatedLoopbackClientAsync(accessKeyId, secretAccessKey);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-multipart-reject-bucket";
        const string objectKey = "docs/ssec-multipart.bin";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        // The disk provider rejects SSE-C headers on multipart initiation; verify graceful rejection
        var exception = await Assert.ThrowsAsync<AmazonS3Exception>(() => s3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5
        }));
        Assert.NotEqual(HttpStatusCode.InternalServerError, exception.StatusCode);
        Assert.True(((int)exception.StatusCode >= 400 && (int)exception.StatusCode < 500) || exception.StatusCode == (HttpStatusCode)501,
            $"Expected 4xx/501 status code but got {(int)exception.StatusCode}");
    }

    [Fact]
    public async Task AmazonS3Client_PutGetObject_WithSseC_RoundTripsAgainstS3Provider()
    {
        const string accessKeyId = "aws-sdk-ssec-roundtrip-access";
        const string secretAccessKey = "aws-sdk-ssec-roundtrip-secret";
        var storageClient = new AwsSdkCompatibilityS3Client();

        await using var isolatedClient = await CreateAuthenticatedLoopbackS3ProviderClientAsync(accessKeyId, secretAccessKey, storageClient);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-roundtrip-bucket";
        const string objectKey = "docs/ssec-roundtrip.txt";
        const string payload = "ssec round-trip payload";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = payload,
            ContentType = "text/plain",
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5,
            UseChunkEncoding = false
        });
        Assert.Equal(HttpStatusCode.OK, putResponse.HttpStatusCode);

        var getResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5
        });
        Assert.Equal(HttpStatusCode.OK, getResponse.HttpStatusCode);
        using (var reader = new StreamReader(getResponse.ResponseStream)) {
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }
    }

    [Fact]
    public async Task AmazonS3Client_HeadObject_WithSseC_ReturnsMetadataAgainstS3Provider()
    {
        const string accessKeyId = "aws-sdk-ssec-head-s3-access";
        const string secretAccessKey = "aws-sdk-ssec-head-s3-secret";
        var storageClient = new AwsSdkCompatibilityS3Client();

        await using var isolatedClient = await CreateAuthenticatedLoopbackS3ProviderClientAsync(accessKeyId, secretAccessKey, storageClient);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-head-s3-bucket";
        const string objectKey = "docs/ssec-head-s3.txt";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        var customerKey = Convert.ToBase64String(keyBytes);
        var customerKeyMd5 = Convert.ToBase64String(MD5.HashData(keyBytes));

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentBody = "ssec head metadata payload",
            ContentType = "text/plain",
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5,
            UseChunkEncoding = false
        })).HttpStatusCode);

        var headResponse = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKey,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyMd5
        });
        Assert.Equal(HttpStatusCode.OK, headResponse.HttpStatusCode);
        Assert.Equal("text/plain", headResponse.Headers.ContentType);
    }

    [Fact]
    public async Task AmazonS3Client_CopyObject_SseCToSseC_AgainstS3Provider()
    {
        const string accessKeyId = "aws-sdk-ssec-copy-s3-access";
        const string secretAccessKey = "aws-sdk-ssec-copy-s3-secret";
        var storageClient = new AwsSdkCompatibilityS3Client();

        await using var isolatedClient = await CreateAuthenticatedLoopbackS3ProviderClientAsync(accessKeyId, secretAccessKey, storageClient);
        using var s3Client = CreateS3Client(isolatedClient.BaseAddress!, accessKeyId, secretAccessKey);

        const string bucketName = "aws-sdk-ssec-copy-s3-bucket";
        const string sourceKey = "docs/ssec-copy-source.txt";
        const string destKey = "docs/ssec-copy-dest.txt";
        const string payload = "ssec copy source payload";

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName
        })).HttpStatusCode);

        var keyABytes = new byte[32];
        RandomNumberGenerator.Fill(keyABytes);
        var customerKeyA = Convert.ToBase64String(keyABytes);
        var customerKeyAMd5 = Convert.ToBase64String(MD5.HashData(keyABytes));

        var keyBBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBBytes);
        var customerKeyB = Convert.ToBase64String(keyBBytes);
        var customerKeyBMd5 = Convert.ToBase64String(MD5.HashData(keyBBytes));

        Assert.Equal(HttpStatusCode.OK, (await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = sourceKey,
            ContentBody = payload,
            ContentType = "text/plain",
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKeyA,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyAMd5,
            UseChunkEncoding = false
        })).HttpStatusCode);

        var copyResponse = await s3Client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = bucketName,
            SourceKey = sourceKey,
            DestinationBucket = bucketName,
            DestinationKey = destKey,
            CopySourceServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            CopySourceServerSideEncryptionCustomerProvidedKey = customerKeyA,
            CopySourceServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyAMd5,
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKeyB,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyBMd5
        });
        Assert.Equal(HttpStatusCode.OK, copyResponse.HttpStatusCode);

        var getResponse = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = destKey,
            ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
            ServerSideEncryptionCustomerProvidedKey = customerKeyB,
            ServerSideEncryptionCustomerProvidedKeyMD5 = customerKeyBMd5
        });
        Assert.Equal(HttpStatusCode.OK, getResponse.HttpStatusCode);
        using (var reader = new StreamReader(getResponse.ResponseStream)) {
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }
    }

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateAuthenticatedLoopbackClientAsync(
        string accessKeyId,
        string secretAccessKey,
        Action<IntegratedS3Options>? configureOptions = null,
        Action<WebApplicationBuilder>? configureBuilder = null,
        Action<ConfigurationManager>? configureConfiguration = null)
    {
        return _factory.CreateLoopbackIsolatedClientAsync(
            builder => {
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
                configureBuilder?.Invoke(builder);
            },
            configureConfiguration: configureConfiguration);
    }

    private Task<WebUiApplicationFactory.IsolatedWebUiClient> CreateAuthenticatedLoopbackS3ProviderClientAsync(
        string accessKeyId,
        string secretAccessKey,
        IS3StorageClient storageClient,
        Action<IntegratedS3Options>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(storageClient);

        return CreateAuthenticatedLoopbackClientAsync(
            accessKeyId,
            secretAccessKey,
            configureOptions,
            builder => builder.Services.Replace(ServiceDescriptor.Singleton(storageClient)),
            configuration => {
                configuration["IntegratedS3:ReferenceHost:StorageProvider"] = "S3";
                configuration["IntegratedS3:S3:ProviderName"] = "aws-sdk-compatibility-s3";
                configuration["IntegratedS3:S3:Region"] = "us-east-1";
            });
    }

    private static PutBucketEncryptionRequest CreateBucketEncryptionRequest(
        string bucketName,
        ServerSideEncryptionMethod algorithm,
        string? keyId = null)
    {
        return new PutBucketEncryptionRequest
        {
            BucketName = bucketName,
            ServerSideEncryptionConfiguration = new ServerSideEncryptionConfiguration
            {
                ServerSideEncryptionRules =
                [
                    new ServerSideEncryptionRule
                    {
                        ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault
                        {
                            ServerSideEncryptionAlgorithm = algorithm,
                            ServerSideEncryptionKeyManagementServiceKeyId = keyId
                        }
                    }
                ]
            }
        };
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

    private static void AssertSignedTrailerChecksumRequest(
        IReadOnlyDictionary<string, string> requestHeaders,
        string checksumAlgorithmName,
        string checksumTrailerHeaderName)
    {
        Assert.Equal("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", requestHeaders["x-amz-content-sha256"]);
        Assert.Equal(checksumTrailerHeaderName, requestHeaders["x-amz-trailer"]);
        Assert.False(requestHeaders.ContainsKey(checksumTrailerHeaderName));

        var declaredChecksumAlgorithm = requestHeaders.TryGetValue("x-amz-sdk-checksum-algorithm", out var sdkChecksumAlgorithm)
            ? sdkChecksumAlgorithm
            : requestHeaders["x-amz-checksum-algorithm"];
        Assert.Equal(checksumAlgorithmName, declaredChecksumAlgorithm);
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

    private sealed class AwsSdkCompatibilityS3Client : IS3StorageClient
    {
        private readonly Dictionary<string, S3BucketEntry> _buckets = new(StringComparer.Ordinal);
        private readonly Dictionary<string, StorageBucketDefaultEncryptionConfiguration> _bucketDefaultEncryptions = new(StringComparer.Ordinal);
        private readonly Dictionary<(string BucketName, string Key), StoredObject> _objects = new();
        private readonly Dictionary<string, StorageBucketVersioningStatus> _bucketVersioning = new(StringComparer.Ordinal);

        public StorageObjectServerSideEncryptionSettings? LastPutObjectServerSideEncryption { get; private set; }

        public StorageObjectServerSideEncryptionSettings? LastCopyObjectServerSideEncryption { get; private set; }

        public Task<IReadOnlyList<S3BucketEntry>> ListBucketsAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult<IReadOnlyList<S3BucketEntry>>(_buckets.Values.OrderBy(static bucket => bucket.Name).ToArray());
        }

        public Task<S3BucketEntry> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_buckets.TryGetValue(bucketName, out var existingBucket)) {
                throw CreateS3Exception("BucketAlreadyOwnedByYou", HttpStatusCode.Conflict, $"Bucket '{bucketName}' already exists.");
            }

            var bucket = new S3BucketEntry(bucketName, DateTimeOffset.UtcNow);
            _buckets[bucketName] = bucket;
            _bucketVersioning[bucketName] = StorageBucketVersioningStatus.Disabled;
            return Task.FromResult(bucket);
        }

        public Task<S3BucketEntry?> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult<S3BucketEntry?>(_buckets.TryGetValue(bucketName, out var bucket) ? bucket : null);
        }

        public Task DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            _buckets.Remove(bucketName);
            _bucketVersioning.Remove(bucketName);
            _bucketDefaultEncryptions.Remove(bucketName);

            foreach (var objectKey in _objects.Keys.Where(key => string.Equals(key.BucketName, bucketName, StringComparison.Ordinal)).ToArray()) {
                _objects.Remove(objectKey);
            }

            return Task.CompletedTask;
        }

        public Task<S3BucketLocationEntry> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            return Task.FromResult(new S3BucketLocationEntry(null));
        }

        public Task<S3VersioningEntry> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            return Task.FromResult(new S3VersioningEntry(_bucketVersioning[bucketName]));
        }

        public Task<S3VersioningEntry> SetBucketVersioningAsync(string bucketName, StorageBucketVersioningStatus status, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            _bucketVersioning[bucketName] = status;
            return Task.FromResult(new S3VersioningEntry(status));
        }

        public Task<StorageBucketDefaultEncryptionConfiguration> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            if (_bucketDefaultEncryptions.TryGetValue(bucketName, out var configuration)) {
                return Task.FromResult(CloneBucketDefaultEncryptionConfiguration(configuration));
            }

            throw CreateS3Exception(
                "ServerSideEncryptionConfigurationNotFoundError",
                HttpStatusCode.NotFound,
                $"Bucket '{bucketName}' does not have a default encryption configuration.");
        }

        public Task<StorageBucketDefaultEncryptionConfiguration> SetBucketDefaultEncryptionAsync(StoragePutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(request.BucketName);

            var configuration = new StorageBucketDefaultEncryptionConfiguration
            {
                BucketName = request.BucketName,
                Rule = new StorageBucketDefaultEncryptionRule
                {
                    Algorithm = request.Rule.Algorithm,
                    KeyId = request.Rule.KeyId
                }
            };

            _bucketDefaultEncryptions[request.BucketName] = configuration;
            return Task.FromResult(CloneBucketDefaultEncryptionConfiguration(configuration));
        }

        public Task DeleteBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            _bucketDefaultEncryptions.Remove(bucketName);
            return Task.CompletedTask;
        }

        public Task<S3ObjectListPage> ListObjectsAsync(string bucketName, string? prefix, string? continuationToken, int? maxKeys, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            var filteredEntries = _objects
                .Where(entry => string.Equals(entry.Key.BucketName, bucketName, StringComparison.Ordinal)
                    && (prefix is null || entry.Key.Key.StartsWith(prefix, StringComparison.Ordinal)))
                .OrderBy(entry => entry.Key.Key, StringComparer.Ordinal)
                .Select(entry => entry.Value.ToEntry(entry.Key.Key))
                .ToArray();

            return Task.FromResult(new S3ObjectListPage(
                maxKeys is > 0 ? filteredEntries.Take(maxKeys.Value).ToArray() : filteredEntries,
                NextContinuationToken: null));
        }

        public Task<S3ObjectVersionListPage> ListObjectVersionsAsync(
            string bucketName,
            string? prefix,
            string? delimiter,
            string? keyMarker,
            string? versionIdMarker,
            int? maxKeys,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            return Task.FromResult(new S3ObjectVersionListPage([], null, null));
        }

        public Task<S3ObjectEntry?> HeadObjectAsync(string bucketName, string key, string? versionId, StorageObjectCustomerEncryptionSettings? customerEncryption, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            return Task.FromResult<S3ObjectEntry?>(_objects.TryGetValue((bucketName, key), out var storedObject)
                ? storedObject.ToEntry(key)
                : null);
        }

        public Task<Uri> CreatePresignedGetObjectUrlAsync(string bucketName, string key, string? versionId, DateTimeOffset expiresAtUtc, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(new Uri($"http://example.invalid/{bucketName}/{Uri.EscapeDataString(key)}", UriKind.Absolute));
        }

        public Task<Uri> CreatePresignedPutObjectUrlAsync(string bucketName, string key, string? contentType, DateTimeOffset expiresAtUtc, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(new Uri($"http://example.invalid/{bucketName}/{Uri.EscapeDataString(key)}", UriKind.Absolute));
        }

        public Task<S3GetObjectResult> GetObjectAsync(
            string bucketName,
            string key,
            string? versionId,
            StorageObjectRange? range,
            string? ifMatchETag,
            string? ifNoneMatchETag,
            DateTimeOffset? ifModifiedSinceUtc,
            DateTimeOffset? ifUnmodifiedSinceUtc,
            StorageObjectCustomerEncryptionSettings? customerEncryption,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            if (!_objects.TryGetValue((bucketName, key), out var storedObject)) {
                throw CreateS3Exception("NoSuchKey", HttpStatusCode.NotFound, $"Object '{key}' does not exist in bucket '{bucketName}'.");
            }

            var content = new MemoryStream(storedObject.Content, writable: false);
            return Task.FromResult(new S3GetObjectResult(storedObject.ToEntry(key), content, storedObject.Content.LongLength));
        }

        public Task<StorageObjectRetentionInfo> GetObjectRetentionAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            return Task.FromResult(new StorageObjectRetentionInfo
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId
            });
        }

        public Task<StorageObjectLegalHoldInfo> GetObjectLegalHoldAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            return Task.FromResult(new StorageObjectLegalHoldInfo
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId
            });
        }

        public async Task<S3ObjectEntry> PutObjectAsync(
            string bucketName,
            string key,
            Stream content,
            long? contentLength,
            string? contentType,
            string? cacheControl,
            string? contentDisposition,
            string? contentEncoding,
            string? contentLanguage,
            DateTimeOffset? expiresUtc,
            IReadOnlyDictionary<string, string>? metadata,
            IReadOnlyDictionary<string, string>? tags,
            IReadOnlyDictionary<string, string>? checksums,
            StorageObjectServerSideEncryptionSettings? serverSideEncryption,
            StorageObjectCustomerEncryptionSettings? customerEncryption,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            LastPutObjectServerSideEncryption = CloneServerSideEncryptionSettings(serverSideEncryption);

            using var buffer = new MemoryStream();
            await content.CopyToAsync(buffer, cancellationToken).ConfigureAwait(false);
            var payload = buffer.ToArray();
            var storedObject = new StoredObject(
                Content: payload,
                ContentType: contentType,
                ETag: ComputeETag(payload),
                LastModifiedUtc: DateTimeOffset.UtcNow,
                Metadata: CloneDictionary(metadata),
                Checksums: CloneDictionary(checksums, StringComparer.OrdinalIgnoreCase),
                ServerSideEncryption: ToServerSideEncryptionInfo(serverSideEncryption),
                CacheControl: cacheControl,
                ContentDisposition: contentDisposition,
                ContentEncoding: contentEncoding,
                ContentLanguage: contentLanguage,
                ExpiresUtc: expiresUtc,
                Tags: CloneDictionary(tags));

            _objects[(bucketName, key)] = storedObject;
            return storedObject.ToEntry(key);
        }

        public Task<S3DeleteObjectResult> DeleteObjectAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            _objects.Remove((bucketName, key));
            return Task.FromResult(new S3DeleteObjectResult(key, null, false));
        }

        public Task<S3ObjectEntry> CopyObjectAsync(
            string sourceBucketName,
            string sourceKey,
            string destinationBucketName,
            string destinationKey,
            string? sourceVersionId,
            string? sourceIfMatchETag,
            string? sourceIfNoneMatchETag,
            DateTimeOffset? sourceIfModifiedSinceUtc,
            DateTimeOffset? sourceIfUnmodifiedSinceUtc,
            StorageCopyObjectMetadataDirective metadataDirective,
            string? contentType,
            string? cacheControl,
            string? contentDisposition,
            string? contentEncoding,
            string? contentLanguage,
            DateTimeOffset? expiresUtc,
            IReadOnlyDictionary<string, string>? metadata,
            bool overwriteIfExists,
            StorageObjectTaggingDirective taggingDirective,
            IReadOnlyDictionary<string, string>? tags,
            string? checksumAlgorithm,
            IReadOnlyDictionary<string, string>? checksums,
            StorageObjectServerSideEncryptionSettings? destinationServerSideEncryption,
            StorageObjectCustomerEncryptionSettings? sourceCustomerEncryption,
            StorageObjectCustomerEncryptionSettings? destinationCustomerEncryption,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(sourceBucketName);
            EnsureBucketExists(destinationBucketName);

            if (!_objects.TryGetValue((sourceBucketName, sourceKey), out var sourceObject)) {
                throw CreateS3Exception("NoSuchKey", HttpStatusCode.NotFound, $"Object '{sourceKey}' does not exist in bucket '{sourceBucketName}'.");
            }

            LastCopyObjectServerSideEncryption = CloneServerSideEncryptionSettings(destinationServerSideEncryption);

            var copiedObject = new StoredObject(
                Content: sourceObject.Content.ToArray(),
                ContentType: metadataDirective == StorageCopyObjectMetadataDirective.Replace ? contentType : sourceObject.ContentType,
                ETag: sourceObject.ETag,
                LastModifiedUtc: DateTimeOffset.UtcNow,
                Metadata: metadataDirective == StorageCopyObjectMetadataDirective.Replace ? CloneDictionary(metadata) : CloneDictionary(sourceObject.Metadata),
                Checksums: checksums is not null
                    ? CloneDictionary(checksums, StringComparer.OrdinalIgnoreCase)
                    : CloneDictionary(sourceObject.Checksums, StringComparer.OrdinalIgnoreCase),
                ServerSideEncryption: ToServerSideEncryptionInfo(destinationServerSideEncryption),
                CacheControl: metadataDirective == StorageCopyObjectMetadataDirective.Replace ? cacheControl : sourceObject.CacheControl,
                ContentDisposition: metadataDirective == StorageCopyObjectMetadataDirective.Replace ? contentDisposition : sourceObject.ContentDisposition,
                ContentEncoding: metadataDirective == StorageCopyObjectMetadataDirective.Replace ? contentEncoding : sourceObject.ContentEncoding,
                ContentLanguage: metadataDirective == StorageCopyObjectMetadataDirective.Replace ? contentLanguage : sourceObject.ContentLanguage,
                ExpiresUtc: metadataDirective == StorageCopyObjectMetadataDirective.Replace ? expiresUtc : sourceObject.ExpiresUtc,
                Tags: taggingDirective == StorageObjectTaggingDirective.Replace ? CloneDictionary(tags) : CloneDictionary(sourceObject.Tags));

            _objects[(destinationBucketName, destinationKey)] = copiedObject;
            return Task.FromResult(copiedObject.ToEntry(destinationKey));
        }

        public Task<StorageMultipartUploadInfo> InitiateMultipartUploadAsync(
            string bucketName,
            string key,
            string? contentType,
            string? cacheControl,
            string? contentDisposition,
            string? contentEncoding,
            string? contentLanguage,
            DateTimeOffset? expiresUtc,
            IReadOnlyDictionary<string, string>? metadata,
            IReadOnlyDictionary<string, string>? tags,
            string? checksumAlgorithm,
            StorageObjectServerSideEncryptionSettings? serverSideEncryption,
            StorageObjectCustomerEncryptionSettings? customerEncryption,
            CancellationToken cancellationToken = default)
            => throw new NotSupportedException("Multipart compatibility coverage is not exercised by this harness.");

        public Task<StorageMultipartUploadPart> UploadMultipartPartAsync(
            string bucketName,
            string key,
            string uploadId,
            int partNumber,
            Stream content,
            long? contentLength,
            string? checksumAlgorithm,
            IReadOnlyDictionary<string, string>? checksums,
            StorageObjectCustomerEncryptionSettings? customerEncryption,
            CancellationToken cancellationToken = default)
            => throw new NotSupportedException("Multipart compatibility coverage is not exercised by this harness.");

        public Task<StorageMultipartUploadPart> UploadPartCopyAsync(StorageUploadPartCopyRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException("Multipart compatibility coverage is not exercised by this harness.");

        public Task<S3ObjectEntry> CompleteMultipartUploadAsync(
            string bucketName,
            string key,
            string uploadId,
            IReadOnlyList<StorageMultipartUploadPart> parts,
            CancellationToken cancellationToken = default)
            => throw new NotSupportedException("Multipart compatibility coverage is not exercised by this harness.");

        public Task AbortMultipartUploadAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException("Multipart compatibility coverage is not exercised by this harness.");

        public Task<S3MultipartUploadListPage> ListMultipartUploadsAsync(
            string bucketName,
            string? prefix,
            string? keyMarker,
            string? uploadIdMarker,
            int? maxUploads,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            return Task.FromResult(new S3MultipartUploadListPage([], null, null));
        }

        public Task<S3MultipartPartListPage> ListMultipartPartsAsync(
            string bucketName,
            string key,
            string uploadId,
            int? partNumberMarker,
            int? maxParts,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);
            return Task.FromResult(new S3MultipartPartListPage([], null));
        }

        public Task<IReadOnlyDictionary<string, string>> GetObjectTagsAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            IReadOnlyDictionary<string, string> tags = _objects.TryGetValue((bucketName, key), out var storedObject) && storedObject.Tags is not null
                ? CloneDictionary(storedObject.Tags)!
                : new Dictionary<string, string>(StringComparer.Ordinal);

            return Task.FromResult(tags);
        }

        public Task PutObjectTagsAsync(string bucketName, string key, string? versionId, IReadOnlyDictionary<string, string> tags, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            if (!_objects.TryGetValue((bucketName, key), out var storedObject)) {
                throw CreateS3Exception("NoSuchKey", HttpStatusCode.NotFound, $"Object '{key}' does not exist in bucket '{bucketName}'.");
            }

            _objects[(bucketName, key)] = storedObject with { Tags = CloneDictionary(tags) };
            return Task.CompletedTask;
        }

        public Task DeleteObjectTagsAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            EnsureBucketExists(bucketName);

            if (_objects.TryGetValue((bucketName, key), out var storedObject)) {
                _objects[(bucketName, key)] = storedObject with { Tags = null };
            }

            return Task.CompletedTask;
        }

        public void Dispose() { }

        private void EnsureBucketExists(string bucketName)
        {
            if (!_buckets.ContainsKey(bucketName)) {
                throw CreateS3Exception("NoSuchBucket", HttpStatusCode.NotFound, $"Bucket '{bucketName}' does not exist.");
            }
        }

        private static AmazonS3Exception CreateS3Exception(string errorCode, HttpStatusCode statusCode, string message)
        {
            return new AmazonS3Exception(message)
            {
                ErrorCode = errorCode,
                StatusCode = statusCode
            };
        }

        private static StorageBucketDefaultEncryptionConfiguration CloneBucketDefaultEncryptionConfiguration(StorageBucketDefaultEncryptionConfiguration configuration)
        {
            return new StorageBucketDefaultEncryptionConfiguration
            {
                BucketName = configuration.BucketName,
                Rule = new StorageBucketDefaultEncryptionRule
                {
                    Algorithm = configuration.Rule.Algorithm,
                    KeyId = configuration.Rule.KeyId
                }
            };
        }

        private static StorageObjectServerSideEncryptionSettings? CloneServerSideEncryptionSettings(StorageObjectServerSideEncryptionSettings? settings)
        {
            if (settings is null) {
                return null;
            }

            return new StorageObjectServerSideEncryptionSettings
            {
                Algorithm = settings.Algorithm,
                KeyId = settings.KeyId,
                Context = CloneDictionary(settings.Context)
            };
        }

        private static StorageObjectServerSideEncryptionInfo? ToServerSideEncryptionInfo(StorageObjectServerSideEncryptionSettings? settings)
        {
            if (settings is null) {
                return null;
            }

            return new StorageObjectServerSideEncryptionInfo
            {
                Algorithm = settings.Algorithm,
                KeyId = settings.KeyId
            };
        }

        private static Dictionary<string, string>? CloneDictionary(IReadOnlyDictionary<string, string>? source, IEqualityComparer<string>? comparer = null)
        {
            if (source is null) {
                return null;
            }

            return new Dictionary<string, string>(source, comparer ?? StringComparer.Ordinal);
        }

        private static string ComputeETag(byte[] content)
        {
            return Convert.ToHexString(MD5.HashData(content)).ToLowerInvariant();
        }

        private sealed record StoredObject(
            byte[] Content,
            string? ContentType,
            string ETag,
            DateTimeOffset LastModifiedUtc,
            IReadOnlyDictionary<string, string>? Metadata,
            IReadOnlyDictionary<string, string>? Checksums,
            StorageObjectServerSideEncryptionInfo? ServerSideEncryption,
            string? CacheControl,
            string? ContentDisposition,
            string? ContentEncoding,
            string? ContentLanguage,
            DateTimeOffset? ExpiresUtc,
            IReadOnlyDictionary<string, string>? Tags)
        {
            public S3ObjectEntry ToEntry(string key)
            {
                return new S3ObjectEntry(
                    Key: key,
                    ContentLength: Content.LongLength,
                    ContentType: ContentType,
                    ETag: ETag,
                    LastModifiedUtc: LastModifiedUtc,
                    Metadata: Metadata,
                    VersionId: null,
                    Checksums: Checksums,
                    ServerSideEncryption: ServerSideEncryption,
                    CacheControl: CacheControl,
                    ContentDisposition: ContentDisposition,
                    ContentEncoding: ContentEncoding,
                    ContentLanguage: ContentLanguage,
                    ExpiresUtc: ExpiresUtc);
            }
        }
    }

    private sealed class NonSeekableMemoryStream(byte[] payload) : MemoryStream(payload)
    {
        public override bool CanSeek => false;
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
                Core.Models.StorageOperationType.ListObjectVersions => "storage.read",
                Core.Models.StorageOperationType.ListMultipartUploads => "storage.read",
                Core.Models.StorageOperationType.ListMultipartParts => "storage.read",
                Core.Models.StorageOperationType.GetObject => "storage.read",
                Core.Models.StorageOperationType.GetBucketLocation => "storage.read",
                Core.Models.StorageOperationType.GetBucketCors => "storage.read",
                Core.Models.StorageOperationType.GetBucketDefaultEncryption => "storage.read",
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
