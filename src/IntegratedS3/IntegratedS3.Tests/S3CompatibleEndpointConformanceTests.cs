using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Provider.S3;
using IntegratedS3.Provider.S3.Internal;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class S3CompatibleEndpointConformanceTests
{
    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_ExercisesCopyMultipartChecksumSseVersioning_AndDelegatedRead()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var resolver = new S3StorageObjectLocationResolver(options, client);
        var bucketName = $"compat-{Guid.NewGuid():N}";
        string? pendingMultipartUploadId = null;

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(createBucket.IsSuccess, createBucket.Error?.Message);

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(enableVersioning.IsSuccess, enableVersioning.Error?.Message);
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            const string sourceKey = "docs/source.txt";
            const string copiedKey = "docs/copied.txt";
            const string multipartKey = "docs/multipart.bin";
            const string firstPayload = "local-compatible payload v1";
            const string secondPayload = "local-compatible payload v2";

            var firstPut = await PutTextObjectAsync(storage, bucketName, sourceKey, firstPayload);
            Assert.True(firstPut.IsSuccess, firstPut.Error?.Message);

            var secondPut = await PutTextObjectAsync(storage, bucketName, sourceKey, secondPayload);
            Assert.True(secondPut.IsSuccess, secondPut.Error?.Message);
            var firstVersionId = Assert.IsType<string>(firstPut.Value?.VersionId);
            var secondVersionId = Assert.IsType<string>(secondPut.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);

            var currentVersioning = await storage.GetBucketVersioningAsync(bucketName);
            Assert.True(currentVersioning.IsSuccess, currentVersioning.Error?.Message);
            Assert.Equal(BucketVersioningStatus.Enabled, currentVersioning.Value!.Status);

            var copyResult = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = secondVersionId,
                DestinationBucketName = bucketName,
                DestinationKey = copiedKey,
                DestinationServerSideEncryption = CreateAes256Settings()
            });
            Assert.True(copyResult.IsSuccess, copyResult.Error?.Message);

            var copiedHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = copiedKey
            });
            Assert.True(copiedHead.IsSuccess, copiedHead.Error?.Message);

            var initiateMultipart = await storage.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = multipartKey,
                ContentType = "application/octet-stream",
                ChecksumAlgorithm = "SHA256",
                ServerSideEncryption = CreateAes256Settings()
            });
            Assert.True(initiateMultipart.IsSuccess, initiateMultipart.Error?.Message);
            pendingMultipartUploadId = initiateMultipart.Value!.UploadId;

            var part1 = await UploadPartAsync(storage, bucketName, multipartKey, pendingMultipartUploadId, 1, CreateFilledBuffer(5 * 1024 * 1024, (byte)'a'));
            var part2 = await UploadPartAsync(storage, bucketName, multipartKey, pendingMultipartUploadId, 2, CreateFilledBuffer(1024 * 1024, (byte)'b'));
            Assert.True(part1.IsSuccess, part1.Error?.Message);
            Assert.True(part2.IsSuccess, part2.Error?.Message);

            var completeMultipart = await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = multipartKey,
                UploadId = pendingMultipartUploadId,
                Parts =
                [
                    part1.Value!,
                    part2.Value!
                ]
            });
            Assert.True(completeMultipart.IsSuccess, completeMultipart.Error?.Message);
            pendingMultipartUploadId = null;

            var versions = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
            {
                BucketName = bucketName,
                Prefix = sourceKey
            }).ToListAsync();
            Assert.True(versions.Count(static entry => !entry.IsDeleteMarker) >= 2);
            Assert.Contains(versions, entry => entry.VersionId == firstVersionId);
            Assert.Contains(versions, entry => entry.VersionId == secondVersionId);

            var resolvedLocation = await resolver.ResolveReadLocationAsync(new ResolveObjectLocationRequest
            {
                ProviderName = options.ProviderName,
                BucketName = bucketName,
                Key = sourceKey,
                VersionId = secondVersionId,
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5)
            });

            var delegatedLocation = Assert.IsType<StorageResolvedObjectLocation>(resolvedLocation);
            var delegatedUri = Assert.IsType<Uri>(delegatedLocation.Location);
            Assert.Equal(StorageObjectAccessMode.Delegated, delegatedLocation.AccessMode);
            Assert.Equal(settings.ServiceUri.Scheme, delegatedUri.Scheme);
            Assert.Contains(
                $"versionId={Uri.EscapeDataString(secondVersionId)}",
                delegatedUri.Query,
                StringComparison.Ordinal);

            using var httpClient = CreatePresignedHttpClient(settings.ServiceUri);
            var delegatedReadBody = await httpClient.GetStringAsync(delegatedUri);
            Assert.Equal(secondPayload, delegatedReadBody);
        }
        finally
        {
            if (pendingMultipartUploadId is not null)
            {
                try
                {
                    await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = "docs/multipart.bin",
                        UploadId = pendingMultipartUploadId
                    });
                }
                catch
                {
                }
            }

            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_GetObject_HonorsHistoricalVersionConditionalHeaders()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-read-version-{Guid.NewGuid():N}";
        const string objectKey = "docs/history.txt";
        const string firstPayload = "version one";
        const string secondPayload = "version two";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for historical conditional reads", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for historical conditional reads", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutPlainTextObjectAsync(storage, bucketName, objectKey, firstPayload);
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put historical read version one", firstVersion.Error?.Message));

            var secondVersion = await PutPlainTextObjectAsync(storage, bucketName, objectKey, secondPayload);
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put historical read version two", secondVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            var secondVersionId = Assert.IsType<string>(secondVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);

            var firstHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId
            });
            Assert.True(
                firstHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head historical read version one", firstHead.Error?.Message));
            var firstHeadInfo = Assert.IsType<ObjectInfo>(firstHead.Value);
            var firstHeadEtag = Assert.IsType<string>(firstHeadInfo.ETag);

            var secondHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = secondVersionId
            });
            Assert.True(
                secondHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head historical read version two", secondHead.Error?.Message));
            var secondHeadInfo = Assert.IsType<ObjectInfo>(secondHead.Value);
            var secondHeadEtag = Assert.IsType<string>(secondHeadInfo.ETag);
            Assert.NotEqual(firstHeadEtag, secondHeadEtag);

            var historicalRead = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                IfNoneMatchETag = secondHeadEtag,
                IfModifiedSinceUtc = firstHeadInfo.LastModifiedUtc.AddMinutes(5)
            });
            Assert.True(
                historicalRead.IsSuccess,
                CreateConformanceFailureMessage(settings, "read historical version with mismatched If-None-Match and future If-Modified-Since", historicalRead.Error?.Message));

            await using (var historicalReadResponse = historicalRead.Value!)
            {
                Assert.False(historicalReadResponse.IsNotModified);
                Assert.Equal(firstVersionId, historicalReadResponse.Object.VersionId);
                Assert.Equal(firstHeadEtag, historicalReadResponse.Object.ETag);
                using var reader = new StreamReader(historicalReadResponse.Content, Encoding.UTF8);
                Assert.Equal(firstPayload, await reader.ReadToEndAsync());
            }

            var notModifiedRead = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                IfNoneMatchETag = firstHeadEtag,
                IfModifiedSinceUtc = firstHeadInfo.LastModifiedUtc.AddMinutes(-5)
            });
            Assert.True(
                notModifiedRead.IsSuccess,
                CreateConformanceFailureMessage(settings, "surface not-modified response for historical version", notModifiedRead.Error?.Message));

            await using (var notModifiedResponse = notModifiedRead.Value!)
            {
                Assert.True(notModifiedResponse.IsNotModified);
                Assert.Equal(firstVersionId, notModifiedResponse.Object.VersionId);
                Assert.Equal(firstHeadEtag, notModifiedResponse.Object.ETag);
                Assert.Equal(firstHeadInfo.ContentLength, notModifiedResponse.TotalContentLength);
                using var content = new MemoryStream();
                await notModifiedResponse.Content.CopyToAsync(content);
                Assert.Empty(content.ToArray());
            }

            var preconditionFailed = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                IfMatchETag = secondHeadEtag
            });
            Assert.False(
                preconditionFailed.IsSuccess,
                CreateConformanceFailureMessage(settings, "reject historical version read when If-Match targets the current version"));
            Assert.NotNull(preconditionFailed.Error);
            Assert.Equal(StorageErrorCode.PreconditionFailed, preconditionFailed.Error.Code);
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_CopyObjectOntoSelfWithHistoricalVersionAndMetadataReplace_CreatesRestoredCurrentVersion()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-copy-self-{Guid.NewGuid():N}";
        const string objectKey = "docs/source.txt";
        const string firstPayload = "version one";
        const string secondPayload = "version two";
        const string replacementMtime = "1712345678";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for same-key copy", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for same-key copy", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutPlainTextObjectAsync(
                storage,
                bucketName,
                objectKey,
                firstPayload,
                metadata: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["mtime"] = "1700000000",
                    ["source-only"] = "remove-me"
                },
                cacheControl: "max-age=60",
                contentDisposition: "attachment; filename=\"original.txt\"");
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put historical same-key source version", firstVersion.Error?.Message));

            var secondVersion = await PutPlainTextObjectAsync(
                storage,
                bucketName,
                objectKey,
                secondPayload,
                metadata: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["mtime"] = "1700000001",
                    ["current-only"] = "keep-current"
                });
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put current same-key source version", secondVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            var secondVersionId = Assert.IsType<string>(secondVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);

            var copyResult = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = objectKey,
                SourceVersionId = firstVersionId,
                DestinationBucketName = bucketName,
                DestinationKey = objectKey,
                MetadataDirective = CopyObjectMetadataDirective.Replace,
                ContentType = "text/plain",
                CacheControl = "no-cache",
                ContentDisposition = "inline; filename=\"restored.txt\"",
                Metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["mtime"] = replacementMtime,
                    ["updated-by"] = "track-h"
                }
            });
            Assert.True(
                copyResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "copy historical version onto same key with metadata replace", copyResult.Error?.Message));

            var restoredVersionId = Assert.IsType<string>(copyResult.Value?.VersionId);
            Assert.NotEqual(firstVersionId, restoredVersionId);
            Assert.NotEqual(secondVersionId, restoredVersionId);
            Assert.Equal("text/plain", copyResult.Value.ContentType);
            Assert.Equal("no-cache", copyResult.Value.CacheControl);
            Assert.Equal("inline; filename=\"restored.txt\"", copyResult.Value.ContentDisposition);
            AssertMetadataValue(
                copyResult.Value.Metadata,
                "mtime",
                replacementMtime,
                settings,
                "same-key copy result metadata");
            AssertMetadataValue(
                copyResult.Value.Metadata,
                "updated-by",
                "track-h",
                settings,
                "same-key copy result metadata");
            AssertMetadataMissing(
                copyResult.Value.Metadata,
                "source-only",
                settings,
                "same-key copy result metadata");

            var currentHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                currentHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head restored same-key copy", currentHead.Error?.Message));
            Assert.Equal(restoredVersionId, currentHead.Value!.VersionId);
            Assert.Equal("text/plain", currentHead.Value.ContentType);
            Assert.Equal("no-cache", currentHead.Value.CacheControl);
            Assert.Equal("inline; filename=\"restored.txt\"", currentHead.Value.ContentDisposition);
            AssertMetadataValue(
                currentHead.Value.Metadata,
                "mtime",
                replacementMtime,
                settings,
                "same-key head metadata");
            AssertMetadataValue(
                currentHead.Value.Metadata,
                "updated-by",
                "track-h",
                settings,
                "same-key head metadata");
            AssertMetadataMissing(
                currentHead.Value.Metadata,
                "source-only",
                settings,
                "same-key head metadata");
            AssertMetadataMissing(
                currentHead.Value.Metadata,
                "current-only",
                settings,
                "same-key head metadata");

            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read restored same-key current object"));
            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read historical source version after same-key copy",
                    firstVersionId));
            Assert.Equal(
                secondPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read previous current version after same-key copy",
                    secondVersionId));
            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read restored version by id",
                    restoredVersionId));

            var versions = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
            {
                BucketName = bucketName,
                Prefix = objectKey
            }).ToListAsync();

            Assert.Equal(3, versions.Count(static entry => !entry.IsDeleteMarker));
            Assert.Contains(versions, entry => entry.VersionId == firstVersionId);
            Assert.Contains(versions, entry => entry.VersionId == secondVersionId);
            Assert.Contains(versions, entry => entry.VersionId == restoredVersionId && entry.IsLatest);
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_CopyObject_HonorsHistoricalSourceVersionPreconditions()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-copy-version-cond-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string copiedKey = "docs/copied.txt";
        const string failedCopyKey = "docs/rejected.txt";
        const string firstPayload = "version one";
        const string secondPayload = "version two";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for historical copy preconditions", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for historical copy preconditions", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutPlainTextObjectAsync(storage, bucketName, sourceKey, firstPayload);
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put historical copy source version one", firstVersion.Error?.Message));

            var secondVersion = await PutPlainTextObjectAsync(storage, bucketName, sourceKey, secondPayload);
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put historical copy source version two", secondVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            var secondVersionId = Assert.IsType<string>(secondVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);

            var firstHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                VersionId = firstVersionId
            });
            Assert.True(
                firstHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head historical copy source version one", firstHead.Error?.Message));
            var firstHeadInfo = Assert.IsType<ObjectInfo>(firstHead.Value);
            var firstHeadEtag = Assert.IsType<string>(firstHeadInfo.ETag);

            var secondHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                VersionId = secondVersionId
            });
            Assert.True(
                secondHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head historical copy source version two", secondHead.Error?.Message));
            var secondHeadEtag = Assert.IsType<string>(secondHead.Value?.ETag);

            var historicalCopy = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = firstVersionId,
                SourceIfMatchETag = firstHeadEtag,
                SourceIfUnmodifiedSinceUtc = firstHeadInfo.LastModifiedUtc.AddMinutes(5),
                DestinationBucketName = bucketName,
                DestinationKey = copiedKey
            });
            Assert.True(
                historicalCopy.IsSuccess,
                CreateConformanceFailureMessage(settings, "copy historical source version when current version differs", historicalCopy.Error?.Message));
            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    copiedKey,
                    settings,
                    "read copied historical source version"));

            var rejectedCopy = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = firstVersionId,
                SourceIfMatchETag = secondHeadEtag,
                DestinationBucketName = bucketName,
                DestinationKey = failedCopyKey
            });
            Assert.False(
                rejectedCopy.IsSuccess,
                CreateConformanceFailureMessage(settings, "reject copy when historical source If-Match targets the current version"));
            Assert.NotNull(rejectedCopy.Error);
            Assert.Equal(StorageErrorCode.PreconditionFailed, rejectedCopy.Error.Code);

            var rejectedCopyHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = failedCopyKey
            });
            Assert.False(
                rejectedCopyHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "leave rejected historical copy target absent"));
            Assert.Equal(StorageErrorCode.ObjectNotFound, rejectedCopyHead.Error!.Code);
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_CopyObject_SourceVersionIdStillWorksWhenDeleteMarkerIsCurrent()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-copy-delete-marker-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string historicalCopyKey = "docs/historical-copy.txt";
        const string currentCopyKey = "docs/current-copy.txt";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for delete-marker copy", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for delete-marker copy", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutPlainTextObjectAsync(storage, bucketName, sourceKey, "version one");
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put historical delete-marker source version", firstVersion.Error?.Message));

            var secondVersion = await PutPlainTextObjectAsync(storage, bucketName, sourceKey, "version two");
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put current delete-marker source version", secondVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            var secondVersionId = Assert.IsType<string>(secondVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create current delete marker before copy", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);
            Assert.NotNull(deleteCurrent.Value.VersionId);

            var historicalCopy = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = firstVersionId,
                DestinationBucketName = bucketName,
                DestinationKey = historicalCopyKey
            });
            Assert.True(
                historicalCopy.IsSuccess,
                CreateConformanceFailureMessage(settings, "copy historical version while delete marker is current", historicalCopy.Error?.Message));

            Assert.Equal(
                "version one",
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    historicalCopyKey,
                    settings,
                    "read historical copy created while delete marker is current"));

            var currentCopy = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                DestinationBucketName = bucketName,
                DestinationKey = currentCopyKey
            });
            Assert.False(
                currentCopy.IsSuccess,
                CreateConformanceFailureMessage(settings, "reject copy from current delete marker"));
            Assert.NotNull(currentCopy.Error);

            var currentCopyHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = currentCopyKey
            });
            Assert.False(
                currentCopyHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "leave rejected current delete-marker copy target absent"));
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_CopyObject_HistoricalVersionWithChecksumAndSseSurvivesCurrentDeleteMarker()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-copy-checksum-sse-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/historical-copy.txt";
        const string historicalPayload = "checksum and sse historical payload";
        const string currentPayload = "checksum and sse current payload";
        var expectedHistoricalChecksum = ComputeSha256Base64(Encoding.UTF8.GetBytes(historicalPayload));

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for checksum+sse historical copy", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for checksum+sse historical copy", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutTextObjectAsync(storage, bucketName, sourceKey, historicalPayload);
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put checksum+sse historical copy source version", firstVersion.Error?.Message));

            var secondVersion = await PutTextObjectAsync(storage, bucketName, sourceKey, currentPayload);
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put checksum+sse current copy source version", secondVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersion.Value!.VersionId);

            var firstHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                VersionId = firstVersionId
            });
            Assert.True(
                firstHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head checksum+sse historical copy source version", firstHead.Error?.Message));
            AssertChecksumValue(
                firstHead.Value!.Checksums,
                "sha256",
                expectedHistoricalChecksum,
                settings,
                "head checksum+sse historical copy source checksum");
            AssertServerSideEncryptionAlgorithm(
                firstHead.Value.ServerSideEncryption,
                ObjectServerSideEncryptionAlgorithm.Aes256,
                settings,
                "head checksum+sse historical copy source encryption");
            var firstVersionEtag = Assert.IsType<string>(firstHead.Value.ETag);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create current delete marker before checksum+sse historical copy", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);

            var historicalCopy = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = firstVersionId,
                SourceIfMatchETag = firstVersionEtag,
                SourceIfUnmodifiedSinceUtc = firstHead.Value.LastModifiedUtc.AddMinutes(5),
                DestinationBucketName = bucketName,
                DestinationKey = destinationKey,
                ChecksumAlgorithm = "SHA256",
                DestinationServerSideEncryption = CreateAes256Settings()
            });
            Assert.True(
                historicalCopy.IsSuccess,
                CreateConformanceFailureMessage(settings, "copy checksum+sse historical version while delete marker is current", historicalCopy.Error?.Message));
            AssertChecksumValue(
                historicalCopy.Value!.Checksums,
                "sha256",
                expectedHistoricalChecksum,
                settings,
                "checksum+sse historical copy result checksum");
            AssertServerSideEncryptionAlgorithm(
                historicalCopy.Value.ServerSideEncryption,
                ObjectServerSideEncryptionAlgorithm.Aes256,
                settings,
                "checksum+sse historical copy result encryption");

            var copiedHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = destinationKey
            });
            Assert.True(
                copiedHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head checksum+sse historical copy destination", copiedHead.Error?.Message));
            Assert.Equal(historicalCopy.Value.VersionId, copiedHead.Value!.VersionId);
            AssertChecksumValue(
                copiedHead.Value.Checksums,
                "sha256",
                expectedHistoricalChecksum,
                settings,
                "head checksum+sse historical copy destination checksum");
            AssertServerSideEncryptionAlgorithm(
                copiedHead.Value.ServerSideEncryption,
                ObjectServerSideEncryptionAlgorithm.Aes256,
                settings,
                "head checksum+sse historical copy destination encryption");

            Assert.Equal(
                historicalPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    destinationKey,
                    settings,
                    "read checksum+sse historical copy after current delete marker"));
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_ResolvedHistoricalReadLocation_RemainsPinnedAfterCurrentDeleteMarker()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var resolver = new S3StorageObjectLocationResolver(options, client);
        var bucketName = $"compat-presigned-version-{Guid.NewGuid():N}";
        const string objectKey = "docs/source.txt";
        const string firstPayload = "version one";
        const string secondPayload = "version two";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for historical delegated read", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for historical delegated read", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutPlainTextObjectAsync(storage, bucketName, objectKey, firstPayload);
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put delegated read version one", firstVersion.Error?.Message));

            var secondVersion = await PutPlainTextObjectAsync(storage, bucketName, objectKey, secondPayload);
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put delegated read version two", secondVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            var secondVersionId = Assert.IsType<string>(secondVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);

            var resolvedLocation = await resolver.ResolveReadLocationAsync(new ResolveObjectLocationRequest
            {
                ProviderName = options.ProviderName,
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5)
            });

            var delegatedLocation = Assert.IsType<StorageResolvedObjectLocation>(resolvedLocation);
            var delegatedUri = Assert.IsType<Uri>(delegatedLocation.Location);
            Assert.Equal(StorageObjectAccessMode.Delegated, delegatedLocation.AccessMode);
            Assert.Contains(
                $"versionId={Uri.EscapeDataString(firstVersionId)}",
                delegatedUri.Query,
                StringComparison.Ordinal);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create current delete marker after resolving delegated historical read", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);
            Assert.NotNull(deleteCurrent.Value.VersionId);

            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read historical version after current delete marker",
                    firstVersionId));

            using var httpClient = CreatePresignedHttpClient(settings.ServiceUri);
            var delegatedReadBody = await httpClient.GetStringAsync(delegatedUri);
            Assert.Equal(firstPayload, delegatedReadBody);
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_PresignObjectDirect_GetObject_HistoricalVersionRemainsPinnedAfterCurrentDeleteMarker()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-direct-get-version-{Guid.NewGuid():N}";
        const string objectKey = "docs/source.txt";
        const string firstPayload = "direct historical version one";
        const string secondPayload = "direct historical version two";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for direct historical presigned get", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for direct historical presigned get", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutTextObjectAsync(storage, bucketName, objectKey, firstPayload);
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put direct historical presigned get version one", firstVersion.Error?.Message));

            var secondVersion = await PutTextObjectAsync(storage, bucketName, objectKey, secondPayload);
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put direct historical presigned get version two", secondVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            var secondVersionId = Assert.IsType<string>(secondVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);

            var directGrant = await storage.PresignObjectDirectAsync(new StorageDirectObjectAccessRequest
            {
                Operation = StorageDirectObjectAccessOperation.GetObject,
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                ExpiresInSeconds = 300
            });
            Assert.True(
                directGrant.IsSuccess,
                CreateConformanceFailureMessage(settings, "presign direct historical get", directGrant.Error?.Message));
            var grant = Assert.IsType<StorageDirectObjectAccessGrant>(directGrant.Value);
            Assert.Empty(grant.Headers);
            Assert.Equal(settings.ServiceUri.Scheme, grant.Url.Scheme);
            Assert.Contains(
                $"versionId={Uri.EscapeDataString(firstVersionId)}",
                grant.Url.Query,
                StringComparison.Ordinal);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create current delete marker after presigning direct historical get", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);
            Assert.NotNull(deleteCurrent.Value.VersionId);

            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read direct historical version after current delete marker",
                    firstVersionId));

            using var httpClient = CreatePresignedHttpClient(grant.Url);
            var directReadBody = await httpClient.GetStringAsync(grant.Url);
            Assert.Equal(firstPayload, directReadBody);
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_DirectAndDelegatedHistoricalRangeReads_StayAlignedAfterCurrentDeleteMarker()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var resolver = new S3StorageObjectLocationResolver(options, client);
        var bucketName = $"compat-direct-range-{Guid.NewGuid():N}";
        const string objectKey = "docs/source.txt";
        const string firstPayload = "abcdefghijklmnopqrstuvwxyz";
        const string secondPayload = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        const long rangeStart = 5;
        const long rangeEnd = 12;
        var expectedRangeBody = firstPayload[(int)rangeStart..((int)rangeEnd + 1)];
        var totalContentLength = Encoding.UTF8.GetByteCount(firstPayload);

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for presigned historical range parity", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for presigned historical range parity", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutTextObjectAsync(storage, bucketName, objectKey, firstPayload);
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put first historical version for presigned range parity", firstVersion.Error?.Message));

            var currentVersion = await PutTextObjectAsync(storage, bucketName, objectKey, secondPayload);
            Assert.True(
                currentVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put current version for presigned range parity", currentVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, currentVersion.Value!.VersionId);

            var resolvedLocation = await resolver.ResolveReadLocationAsync(new ResolveObjectLocationRequest
            {
                ProviderName = options.ProviderName,
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5)
            });
            var delegatedLocation = Assert.IsType<StorageResolvedObjectLocation>(resolvedLocation);
            var delegatedUri = Assert.IsType<Uri>(delegatedLocation.Location);

            var directGrantResult = await storage.PresignObjectDirectAsync(new StorageDirectObjectAccessRequest
            {
                Operation = StorageDirectObjectAccessOperation.GetObject,
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                ExpiresInSeconds = 300
            });
            Assert.True(
                directGrantResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "presign direct historical range read", directGrantResult.Error?.Message));
            var directGrant = Assert.IsType<StorageDirectObjectAccessGrant>(directGrantResult.Value);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create current delete marker before historical range parity", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);

            var rangeHeaders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Range"] = $"bytes={rangeStart}-{rangeEnd}"
            };

            using var httpClient = CreatePresignedHttpClient(settings.ServiceUri);

            using (var delegatedResponse = await SendPresignedGetAsync(httpClient, delegatedUri, rangeHeaders))
            {
                Assert.True(
                    delegatedResponse.StatusCode == HttpStatusCode.PartialContent,
                    CreateConformanceFailureMessage(
                        settings,
                        "read delegated historical presigned range",
                        $"{(int)delegatedResponse.StatusCode} {delegatedResponse.ReasonPhrase}"));

                var delegatedContentRange = delegatedResponse.Content.Headers.ContentRange;
                Assert.NotNull(delegatedContentRange);
                Assert.Equal(rangeStart, delegatedContentRange.From);
                Assert.Equal(rangeEnd, delegatedContentRange.To);
                Assert.Equal(totalContentLength, delegatedContentRange.Length);
                Assert.Equal(expectedRangeBody, await delegatedResponse.Content.ReadAsStringAsync());
            }

            using (var directResponse = await SendPresignedGetAsync(httpClient, directGrant.Url, rangeHeaders))
            {
                Assert.True(
                    directResponse.StatusCode == HttpStatusCode.PartialContent,
                    CreateConformanceFailureMessage(
                        settings,
                        "read direct historical presigned range",
                        $"{(int)directResponse.StatusCode} {directResponse.ReasonPhrase}"));

                var directContentRange = directResponse.Content.Headers.ContentRange;
                Assert.NotNull(directContentRange);
                Assert.Equal(rangeStart, directContentRange.From);
                Assert.Equal(rangeEnd, directContentRange.To);
                Assert.Equal(totalContentLength, directContentRange.Length);
                Assert.Equal(expectedRangeBody, await directResponse.Content.ReadAsStringAsync());
            }

            var rangedRead = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                Range = new ObjectRange
                {
                    Start = rangeStart,
                    End = rangeEnd
                }
            });
            Assert.True(
                rangedRead.IsSuccess,
                CreateConformanceFailureMessage(settings, "read historical version range via storage service", rangedRead.Error?.Message));

            await using (var rangedResponse = rangedRead.Value!)
            {
                var rangedObject = rangedResponse.Range;
                Assert.NotNull(rangedObject);
                Assert.Equal(rangeStart, rangedObject.Start);
                Assert.Equal(rangeEnd, rangedObject.End);
                using var reader = new StreamReader(rangedResponse.Content, Encoding.UTF8);
                Assert.Equal(expectedRangeBody, await reader.ReadToEndAsync());
            }
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_DirectAndDelegatedHistoricalConditionalReads_StayAlignedAfterCurrentDeleteMarker()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var resolver = new S3StorageObjectLocationResolver(options, client);
        var bucketName = $"compat-direct-conditional-{Guid.NewGuid():N}";
        const string objectKey = "docs/source.txt";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for presigned historical conditional parity", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for presigned historical conditional parity", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutTextObjectAsync(storage, bucketName, objectKey, "conditional one");
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put first historical version for presigned conditional parity", firstVersion.Error?.Message));

            var currentVersion = await PutTextObjectAsync(storage, bucketName, objectKey, "conditional two");
            Assert.True(
                currentVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put current version for presigned conditional parity", currentVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, currentVersion.Value!.VersionId);

            var firstHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId
            });
            Assert.True(
                firstHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head historical version for presigned conditional parity", firstHead.Error?.Message));
            var firstVersionEtag = Assert.IsType<string>(firstHead.Value?.ETag);

            var resolvedLocation = await resolver.ResolveReadLocationAsync(new ResolveObjectLocationRequest
            {
                ProviderName = options.ProviderName,
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5)
            });
            var delegatedLocation = Assert.IsType<StorageResolvedObjectLocation>(resolvedLocation);
            var delegatedUri = Assert.IsType<Uri>(delegatedLocation.Location);

            var directGrantResult = await storage.PresignObjectDirectAsync(new StorageDirectObjectAccessRequest
            {
                Operation = StorageDirectObjectAccessOperation.GetObject,
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                ExpiresInSeconds = 300
            });
            Assert.True(
                directGrantResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "presign direct historical conditional read", directGrantResult.Error?.Message));
            var directGrant = Assert.IsType<StorageDirectObjectAccessGrant>(directGrantResult.Value);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create current delete marker before historical conditional parity", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);

            var conditionalHeaders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["If-None-Match"] = firstVersionEtag
            };

            using var httpClient = CreatePresignedHttpClient(settings.ServiceUri);

            using (var delegatedResponse = await SendPresignedGetAsync(httpClient, delegatedUri, conditionalHeaders))
            {
                Assert.True(
                    delegatedResponse.StatusCode == HttpStatusCode.NotModified,
                    CreateConformanceFailureMessage(
                        settings,
                        "read delegated historical presigned conditional not-modified response",
                        $"{(int)delegatedResponse.StatusCode} {delegatedResponse.ReasonPhrase}"));
                Assert.Equal(string.Empty, await delegatedResponse.Content.ReadAsStringAsync());
            }

            using (var directResponse = await SendPresignedGetAsync(httpClient, directGrant.Url, conditionalHeaders))
            {
                Assert.True(
                    directResponse.StatusCode == HttpStatusCode.NotModified,
                    CreateConformanceFailureMessage(
                        settings,
                        "read direct historical presigned conditional not-modified response",
                        $"{(int)directResponse.StatusCode} {directResponse.ReasonPhrase}"));
                Assert.Equal(string.Empty, await directResponse.Content.ReadAsStringAsync());
            }

            var storageNotModified = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = firstVersionId,
                IfNoneMatchETag = firstVersionEtag
            });
            Assert.True(
                storageNotModified.IsSuccess,
                CreateConformanceFailureMessage(settings, "read historical conditional not-modified response via storage service", storageNotModified.Error?.Message));

            await using (var notModifiedResponse = storageNotModified.Value!)
            {
                Assert.True(notModifiedResponse.IsNotModified);
                Assert.Equal(firstVersionId, notModifiedResponse.Object.VersionId);
                using var content = new MemoryStream();
                await notModifiedResponse.Content.CopyToAsync(content);
                Assert.Empty(content.ToArray());
            }
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_PresignObjectDirect_PutObject_RoundTripsContentTypeAndHistoricalVersions()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-direct-put-{Guid.NewGuid():N}";
        const string objectKey = "docs/direct-upload.txt";
        const string firstPayload = "presigned direct upload one";
        const string secondPayload = "presigned direct upload two";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for direct presigned put", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for direct presigned put", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstGrantResult = await storage.PresignObjectDirectAsync(new StorageDirectObjectAccessRequest
            {
                Operation = StorageDirectObjectAccessOperation.PutObject,
                BucketName = bucketName,
                Key = objectKey,
                ExpiresInSeconds = 300,
                ContentType = "text/plain"
            });
            Assert.True(
                firstGrantResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "presign first direct put", firstGrantResult.Error?.Message));
            var firstGrant = Assert.IsType<StorageDirectObjectAccessGrant>(firstGrantResult.Value);
            Assert.Contains(
                firstGrant.Headers,
                static header => string.Equals(header.Key, "Content-Type", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(header.Value, "text/plain", StringComparison.Ordinal));

            using var httpClient = CreatePresignedHttpClient(firstGrant.Url);

            using (var firstResponse = await SendPresignedPutAsync(httpClient, firstGrant, Encoding.UTF8.GetBytes(firstPayload)))
            {
                Assert.True(
                    firstResponse.IsSuccessStatusCode,
                    CreateConformanceFailureMessage(settings, "upload first direct presigned put", $"{(int)firstResponse.StatusCode} {firstResponse.ReasonPhrase}"));
            }

            var firstHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                firstHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head first direct presigned put object", firstHead.Error?.Message));
            Assert.Equal("text/plain", firstHead.Value!.ContentType);
            var firstVersionId = Assert.IsType<string>(firstHead.Value.VersionId);

            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read first direct presigned put object"));

            var secondGrantResult = await storage.PresignObjectDirectAsync(new StorageDirectObjectAccessRequest
            {
                Operation = StorageDirectObjectAccessOperation.PutObject,
                BucketName = bucketName,
                Key = objectKey,
                ExpiresInSeconds = 300,
                ContentType = "text/plain"
            });
            Assert.True(
                secondGrantResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "presign second direct put", secondGrantResult.Error?.Message));
            var secondGrant = Assert.IsType<StorageDirectObjectAccessGrant>(secondGrantResult.Value);

            using (var secondResponse = await SendPresignedPutAsync(httpClient, secondGrant, Encoding.UTF8.GetBytes(secondPayload)))
            {
                Assert.True(
                    secondResponse.IsSuccessStatusCode,
                    CreateConformanceFailureMessage(settings, "upload second direct presigned put", $"{(int)secondResponse.StatusCode} {secondResponse.ReasonPhrase}"));
            }

            var secondHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                secondHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head second direct presigned put object", secondHead.Error?.Message));
            Assert.Equal("text/plain", secondHead.Value!.ContentType);
            var secondVersionId = Assert.IsType<string>(secondHead.Value.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);

            Assert.Equal(
                secondPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read current direct presigned put object"));
            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read historical direct presigned put object version",
                    firstVersionId));
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_PresignObjectDirect_PutObject_CreatesNewCurrentVersionAfterDeleteMarker()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-direct-put-delete-marker-{Guid.NewGuid():N}";
        const string objectKey = "docs/direct-upload.txt";
        const string firstPayload = "presigned direct upload before delete marker";
        const string secondPayload = "presigned direct upload after delete marker";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for direct presigned put after delete marker", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for direct presigned put after delete marker", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            using var httpClient = CreatePresignedHttpClient(settings.ServiceUri);

            var firstGrantResult = await storage.PresignObjectDirectAsync(new StorageDirectObjectAccessRequest
            {
                Operation = StorageDirectObjectAccessOperation.PutObject,
                BucketName = bucketName,
                Key = objectKey,
                ExpiresInSeconds = 300,
                ContentType = "text/plain"
            });
            Assert.True(
                firstGrantResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "presign first direct put before delete marker", firstGrantResult.Error?.Message));
            var firstGrant = Assert.IsType<StorageDirectObjectAccessGrant>(firstGrantResult.Value);
            Assert.Contains(
                firstGrant.Headers,
                static header => string.Equals(header.Key, "Content-Type", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(header.Value, "text/plain", StringComparison.Ordinal));

            using (var firstResponse = await SendPresignedPutAsync(httpClient, firstGrant, Encoding.UTF8.GetBytes(firstPayload)))
            {
                Assert.True(
                    firstResponse.IsSuccessStatusCode,
                    CreateConformanceFailureMessage(settings, "upload first direct put before delete marker", $"{(int)firstResponse.StatusCode} {firstResponse.ReasonPhrase}"));
            }

            var firstHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                firstHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head first direct put before delete marker", firstHead.Error?.Message));
            Assert.Equal("text/plain", firstHead.Value!.ContentType);
            var firstVersionId = Assert.IsType<string>(firstHead.Value.VersionId);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create delete marker before second direct put", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);
            var deleteMarkerVersionId = Assert.IsType<string>(deleteCurrent.Value.VersionId);

            var secondGrantResult = await storage.PresignObjectDirectAsync(new StorageDirectObjectAccessRequest
            {
                Operation = StorageDirectObjectAccessOperation.PutObject,
                BucketName = bucketName,
                Key = objectKey,
                ExpiresInSeconds = 300,
                ContentType = "text/plain"
            });
            Assert.True(
                secondGrantResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "presign second direct put after delete marker", secondGrantResult.Error?.Message));
            var secondGrant = Assert.IsType<StorageDirectObjectAccessGrant>(secondGrantResult.Value);
            Assert.Contains(
                secondGrant.Headers,
                static header => string.Equals(header.Key, "Content-Type", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(header.Value, "text/plain", StringComparison.Ordinal));

            using (var secondResponse = await SendPresignedPutAsync(httpClient, secondGrant, Encoding.UTF8.GetBytes(secondPayload)))
            {
                Assert.True(
                    secondResponse.IsSuccessStatusCode,
                    CreateConformanceFailureMessage(settings, "upload second direct put after delete marker", $"{(int)secondResponse.StatusCode} {secondResponse.ReasonPhrase}"));
            }

            var currentHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                currentHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head current direct put after delete marker", currentHead.Error?.Message));
            Assert.Equal("text/plain", currentHead.Value!.ContentType);
            var secondVersionId = Assert.IsType<string>(currentHead.Value.VersionId);
            Assert.NotEqual(firstVersionId, secondVersionId);
            Assert.NotEqual(deleteMarkerVersionId, secondVersionId);

            Assert.Equal(
                secondPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read current direct put after delete marker"));
            Assert.Equal(
                firstPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    objectKey,
                    settings,
                    "read historical direct put version after delete marker",
                    firstVersionId));

            var versions = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
            {
                BucketName = bucketName,
                Prefix = objectKey
            }).ToListAsync();

            var versionIds = versions.Select(static entry => Assert.IsType<string>(entry.VersionId)).ToArray();
            Assert.Equal(3, versions.Count);
            Assert.Equal([secondVersionId, deleteMarkerVersionId, firstVersionId], versionIds);
            Assert.False(versions[0].IsDeleteMarker);
            Assert.True(versions[1].IsDeleteMarker);
            Assert.False(versions[2].IsDeleteMarker);
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_ListObjectVersions_FollowsMarkersAcrossDeleteMarkers()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-version-markers-{Guid.NewGuid():N}";
        const string objectKey = "docs/source.txt";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for version marker listing", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for version marker listing", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutPlainTextObjectAsync(storage, bucketName, objectKey, "version one");
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put first version for version marker listing", firstVersion.Error?.Message));
            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);

            var firstDeleteMarker = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                firstDeleteMarker.IsSuccess,
                CreateConformanceFailureMessage(settings, "create first delete marker for version marker listing", firstDeleteMarker.Error?.Message));
            Assert.True(firstDeleteMarker.Value!.IsDeleteMarker);
            var firstDeleteMarkerVersionId = Assert.IsType<string>(firstDeleteMarker.Value.VersionId);

            var secondVersion = await PutPlainTextObjectAsync(storage, bucketName, objectKey, "version two");
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put second version for version marker listing", secondVersion.Error?.Message));
            var secondVersionId = Assert.IsType<string>(secondVersion.Value?.VersionId);

            var secondDeleteMarker = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                secondDeleteMarker.IsSuccess,
                CreateConformanceFailureMessage(settings, "create second delete marker for version marker listing", secondDeleteMarker.Error?.Message));
            Assert.True(secondDeleteMarker.Value!.IsDeleteMarker);
            var secondDeleteMarkerVersionId = Assert.IsType<string>(secondDeleteMarker.Value.VersionId);

            var firstPage = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
            {
                BucketName = bucketName,
                Prefix = objectKey,
                PageSize = 2
            }).ToListAsync();

            var firstPageVersionIds = firstPage.Select(static entry => Assert.IsType<string>(entry.VersionId)).ToArray();
            Assert.Equal(2, firstPage.Count);
            Assert.Equal([secondDeleteMarkerVersionId, secondVersionId], firstPageVersionIds);
            Assert.True(firstPage[0].IsDeleteMarker);
            Assert.True(firstPage[0].IsLatest);
            Assert.False(firstPage[1].IsDeleteMarker);
            Assert.All(firstPage, entry => Assert.Equal(objectKey, entry.Key));

            var secondPage = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
            {
                BucketName = bucketName,
                Prefix = objectKey,
                KeyMarker = firstPage[^1].Key,
                VersionIdMarker = firstPage[^1].VersionId
            }).ToListAsync();

            var secondPageVersionIds = secondPage.Select(static entry => Assert.IsType<string>(entry.VersionId)).ToArray();
            Assert.Equal(2, secondPage.Count);
            Assert.Equal([firstDeleteMarkerVersionId, firstVersionId], secondPageVersionIds);
            Assert.True(secondPage[0].IsDeleteMarker);
            Assert.False(secondPage[1].IsDeleteMarker);
            Assert.All(secondPage, entry => Assert.Equal(objectKey, entry.Key));
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_SurfacesSha1MultipartCopyChecksums()
    {
        await AssertMultipartCopyChecksumConformanceAsync(
            checksumAlgorithm: "SHA1",
            checksumKey: "sha1",
            computeChecksum: static payload => ChecksumTestAlgorithms.ComputeSha1Base64(payload),
            computeCompositeChecksum: ComputeMultipartSha1Base64);
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_SurfacesCrc32cMultipartCopyChecksums()
    {
        await AssertMultipartCopyChecksumConformanceAsync(
            checksumAlgorithm: "CRC32C",
            checksumKey: "crc32c",
            computeChecksum: static payload => ChecksumTestAlgorithms.ComputeCrc32cBase64(payload),
            computeCompositeChecksum: static partChecksums => ChecksumTestAlgorithms.ComputeMultipartCrc32cBase64(partChecksums));
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_UploadPartCopy_CopiesHistoricalRanges()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-copy-part-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";
        string? pendingMultipartUploadId = null;

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(createBucket.IsSuccess, createBucket.Error?.Message);

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(enableVersioning.IsSuccess, enableVersioning.Error?.Message);

            await using var firstVersionStream = new MemoryStream(Encoding.UTF8.GetBytes("hello world"));
            var firstVersion = await storage.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                Content = firstVersionStream,
                ContentType = "text/plain"
            });
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put historical source version", firstVersion.Error?.Message));

            await using var secondVersionStream = new MemoryStream(Encoding.UTF8.GetBytes("goodbye world"));
            var secondVersion = await storage.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                Content = secondVersionStream,
                ContentType = "text/plain"
            });
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put current source version", secondVersion.Error?.Message));
            Assert.NotEqual(firstVersion.Value!.VersionId, secondVersion.Value!.VersionId);

            var initiateMultipart = await InitiateMultipartUploadAsync(storage, bucketName, destinationKey);
            Assert.True(
                initiateMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, "initiate multipart destination", initiateMultipart.Error?.Message));
            pendingMultipartUploadId = initiateMultipart.Value!.UploadId;

            var copyPart = await storage.UploadPartCopyAsync(new UploadPartCopyRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                PartNumber = 1,
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = firstVersion.Value.VersionId,
                SourceIfMatchETag = firstVersion.Value.ETag,
                SourceRange = new ObjectRange
                {
                    Start = 6,
                    End = 10
                }
            });
            Assert.True(
                copyPart.IsSuccess,
                CreateConformanceFailureMessage(settings, "upload part copy", copyPart.Error?.Message));

            var completeMultipart = await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                Parts =
                [
                    copyPart.Value!
                ]
            });
            Assert.True(
                completeMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, "complete copied multipart upload", completeMultipart.Error?.Message));
            pendingMultipartUploadId = null;

            var copiedObject = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = destinationKey
            });
            Assert.True(
                copiedObject.IsSuccess,
                CreateConformanceFailureMessage(settings, "get copied multipart object", copiedObject.Error?.Message));

            await using var copiedResponse = copiedObject.Value!;
            using var reader = new StreamReader(copiedResponse.Content, Encoding.UTF8);
            Assert.Equal("world", await reader.ReadToEndAsync());
        }
        finally
        {
            if (pendingMultipartUploadId is not null)
            {
                try
                {
                    await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = destinationKey,
                        UploadId = pendingMultipartUploadId
                    });
                }
                catch
                {
                }
            }

            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_UploadPartCopy_HistoricalRangesRemainAvailableAfterCurrentDeleteMarker()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-copy-part-delete-marker-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/copied.txt";
        string? pendingMultipartUploadId = null;

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for delete-marker historical upload part copy", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for delete-marker historical upload part copy", enableVersioning.Error?.Message));

            var firstVersion = await PutTextObjectAsync(storage, bucketName, sourceKey, "hello world");
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put historical source version for delete-marker upload part copy", firstVersion.Error?.Message));

            var currentVersion = await PutTextObjectAsync(storage, bucketName, sourceKey, "goodbye world");
            Assert.True(
                currentVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put current source version for delete-marker upload part copy", currentVersion.Error?.Message));
            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            var firstVersionEtag = Assert.IsType<string>(firstVersion.Value?.ETag);
            Assert.NotEqual(firstVersionId, currentVersion.Value!.VersionId);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create current delete marker before historical upload part copy", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);

            var initiateMultipart = await InitiateMultipartUploadAsync(storage, bucketName, destinationKey);
            Assert.True(
                initiateMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, "initiate multipart destination after delete marker", initiateMultipart.Error?.Message));
            pendingMultipartUploadId = initiateMultipart.Value!.UploadId;

            var copyPart = await storage.UploadPartCopyAsync(new UploadPartCopyRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                PartNumber = 1,
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = firstVersionId,
                SourceIfMatchETag = firstVersionEtag,
                SourceRange = new ObjectRange
                {
                    Start = 6,
                    End = 10
                }
            });
            Assert.True(
                copyPart.IsSuccess,
                CreateConformanceFailureMessage(settings, "copy historical upload part while delete marker is current", copyPart.Error?.Message));

            var completeMultipart = await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                Parts =
                [
                    copyPart.Value!
                ]
            });
            Assert.True(
                completeMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, "complete historical upload part copy while delete marker is current", completeMultipart.Error?.Message));
            pendingMultipartUploadId = null;

            Assert.Equal(
                "world",
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    destinationKey,
                    settings,
                    "read upload part copy created from historical version after current delete marker"));
        }
        finally
        {
            if (pendingMultipartUploadId is not null)
            {
                try
                {
                    await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = destinationKey,
                        UploadId = pendingMultipartUploadId
                    });
                }
                catch
                {
                }
            }

            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_UploadPartCopy_HistoricalVersionWithChecksumAndSseSurvivesCurrentDeleteMarker()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-copy-part-checksum-sse-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.bin";
        const string destinationKey = "docs/copied.bin";
        var historicalPart1 = CreateFilledBuffer(5 * 1024 * 1024, (byte)'a');
        var historicalPart2 = CreateFilledBuffer(1024 * 1024, (byte)'b');
        var historicalPayload = new byte[historicalPart1.Length + historicalPart2.Length];
        Buffer.BlockCopy(historicalPart1, 0, historicalPayload, 0, historicalPart1.Length);
        Buffer.BlockCopy(historicalPart2, 0, historicalPayload, historicalPart1.Length, historicalPart2.Length);

        var currentPayload = CreateFilledBuffer(historicalPayload.Length, (byte)'z');
        var historicalChecksum = ComputeSha256Base64(historicalPayload);
        var currentChecksum = ComputeSha256Base64(currentPayload);
        var part1Checksum = ComputeSha256Base64(historicalPart1);
        var part2Checksum = ComputeSha256Base64(historicalPart2);
        var compositeChecksum = ComputeMultipartSha256Base64(part1Checksum, part2Checksum);
        string? pendingMultipartUploadId = null;

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, "create bucket for checksum+sse historical upload part copy", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, "enable versioning for checksum+sse historical upload part copy", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            StorageResult<ObjectInfo> historicalVersion;
            await using (var historicalStream = new MemoryStream(historicalPayload, writable: false))
            {
                historicalVersion = await storage.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = sourceKey,
                    Content = historicalStream,
                    ContentLength = historicalPayload.Length,
                    ContentType = "application/octet-stream",
                    Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["sha256"] = historicalChecksum
                    },
                    ServerSideEncryption = CreateAes256Settings()
                });
            }

            Assert.True(
                historicalVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put checksum+sse historical upload part copy source version", historicalVersion.Error?.Message));

            StorageResult<ObjectInfo> currentVersion;
            await using (var currentStream = new MemoryStream(currentPayload, writable: false))
            {
                currentVersion = await storage.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = sourceKey,
                    Content = currentStream,
                    ContentLength = currentPayload.Length,
                    ContentType = "application/octet-stream",
                    Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["sha256"] = currentChecksum
                    },
                    ServerSideEncryption = CreateAes256Settings()
                });
            }

            Assert.True(
                currentVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, "put current upload part copy source version before delete marker", currentVersion.Error?.Message));
            var historicalVersionId = Assert.IsType<string>(historicalVersion.Value?.VersionId);
            Assert.NotEqual(historicalVersionId, currentVersion.Value!.VersionId);

            var historicalHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                VersionId = historicalVersionId
            });
            Assert.True(
                historicalHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head checksum+sse historical upload part copy source version", historicalHead.Error?.Message));
            AssertChecksumValue(
                historicalHead.Value!.Checksums,
                "sha256",
                historicalChecksum,
                settings,
                "head checksum+sse historical upload part copy source checksum");
            AssertServerSideEncryptionAlgorithm(
                historicalHead.Value.ServerSideEncryption,
                ObjectServerSideEncryptionAlgorithm.Aes256,
                settings,
                "head checksum+sse historical upload part copy source encryption");
            var historicalEtag = Assert.IsType<string>(historicalHead.Value.ETag);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, "create current delete marker before checksum+sse historical upload part copy", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);

            var initiateMultipart = await InitiateMultipartUploadAsync(storage, bucketName, destinationKey);
            Assert.True(
                initiateMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, "initiate checksum+sse historical multipart destination", initiateMultipart.Error?.Message));
            Assert.Equal("sha256", initiateMultipart.Value!.ChecksumAlgorithm);
            pendingMultipartUploadId = initiateMultipart.Value.UploadId;

            var sourceIfUnmodifiedSinceUtc = historicalHead.Value.LastModifiedUtc.AddMinutes(5);

            var copyPart1 = await storage.UploadPartCopyAsync(new UploadPartCopyRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                PartNumber = 1,
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = historicalVersionId,
                SourceIfMatchETag = historicalEtag,
                SourceIfUnmodifiedSinceUtc = sourceIfUnmodifiedSinceUtc,
                SourceRange = new ObjectRange
                {
                    Start = 0,
                    End = historicalPart1.Length - 1L
                },
                ChecksumAlgorithm = "SHA256"
            });
            Assert.True(
                copyPart1.IsSuccess,
                CreateConformanceFailureMessage(settings, "copy checksum+sse historical multipart part 1 after delete marker", copyPart1.Error?.Message));
            AssertChecksumValue(
                copyPart1.Value!.Checksums,
                "sha256",
                part1Checksum,
                settings,
                "checksum+sse historical multipart part 1 checksum");

            var copyPart2 = await storage.UploadPartCopyAsync(new UploadPartCopyRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                PartNumber = 2,
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = historicalVersionId,
                SourceIfMatchETag = historicalEtag,
                SourceIfUnmodifiedSinceUtc = sourceIfUnmodifiedSinceUtc,
                SourceRange = new ObjectRange
                {
                    Start = historicalPart1.Length,
                    End = historicalPayload.Length - 1L
                },
                ChecksumAlgorithm = "SHA256"
            });
            Assert.True(
                copyPart2.IsSuccess,
                CreateConformanceFailureMessage(settings, "copy checksum+sse historical multipart part 2 after delete marker", copyPart2.Error?.Message));
            AssertChecksumValue(
                copyPart2.Value!.Checksums,
                "sha256",
                part2Checksum,
                settings,
                "checksum+sse historical multipart part 2 checksum");

            var completeMultipart = await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                Parts =
                [
                    copyPart1.Value,
                    copyPart2.Value
                ]
            });
            Assert.True(
                completeMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, "complete checksum+sse historical multipart copy after delete marker", completeMultipart.Error?.Message));
            pendingMultipartUploadId = null;
            AssertChecksumValue(
                completeMultipart.Value!.Checksums,
                "sha256",
                compositeChecksum,
                settings,
                "complete checksum+sse historical multipart copy checksum after delete marker");
            AssertServerSideEncryptionAlgorithm(
                completeMultipart.Value.ServerSideEncryption,
                ObjectServerSideEncryptionAlgorithm.Aes256,
                settings,
                "complete checksum+sse historical multipart copy encryption after delete marker");

            var copiedHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = destinationKey
            });
            Assert.True(
                copiedHead.IsSuccess,
                CreateConformanceFailureMessage(settings, "head checksum+sse historical multipart copy destination", copiedHead.Error?.Message));
            AssertChecksumValue(
                copiedHead.Value!.Checksums,
                "sha256",
                compositeChecksum,
                settings,
                "head checksum+sse historical multipart copy destination checksum");
            AssertServerSideEncryptionAlgorithm(
                copiedHead.Value.ServerSideEncryption,
                ObjectServerSideEncryptionAlgorithm.Aes256,
                settings,
                "head checksum+sse historical multipart copy destination encryption");

            var copiedObject = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = destinationKey
            });
            Assert.True(
                copiedObject.IsSuccess,
                CreateConformanceFailureMessage(settings, "get checksum+sse historical multipart copy destination", copiedObject.Error?.Message));

            await using var copiedResponse = copiedObject.Value!;
            using var copiedContent = new MemoryStream();
            await copiedResponse.Content.CopyToAsync(copiedContent);
            Assert.Equal(historicalPayload, copiedContent.ToArray());
        }
        finally
        {
            if (pendingMultipartUploadId is not null)
            {
                try
                {
                    await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = destinationKey,
                        UploadId = pendingMultipartUploadId
                    });
                }
                catch
                {
                }
            }

            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_HistoricalManagedSseCopiesWithChecksumsSurviveCurrentDeleteMarkerAcrossConfiguredAlgorithms()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);

        foreach (var scenario in GetManagedServerSideEncryptionScenarios(settings))
        {
            await AssertHistoricalCopyWithChecksumAndManagedSseSurvivesCurrentDeleteMarkerAsync(storage, settings, scenario);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_HistoricalManagedSseUploadPartCopiesWithChecksumsSurviveCurrentDeleteMarkerAcrossConfiguredAlgorithms()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);

        foreach (var scenario in GetManagedServerSideEncryptionScenarios(settings))
        {
            await AssertHistoricalUploadPartCopyWithChecksumAndManagedSseSurvivesCurrentDeleteMarkerAsync(storage, settings, scenario);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_DirectAndDelegatedHistoricalManagedSseReadsExposeMatchingEncryptionMetadata()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var resolver = new S3StorageObjectLocationResolver(options, client);

        foreach (var scenario in GetManagedServerSideEncryptionScenarios(settings))
        {
            await AssertDirectAndDelegatedHistoricalManagedSseReadParityAsync(storage, resolver, options, settings, scenario);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_BucketDefaultManagedSseInheritanceAndExplicitOverridesRemainVisibleWhenConfigured()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var scenarios = GetManagedServerSideEncryptionScenarios(settings);

        foreach (var scenario in scenarios)
        {
            var explicitOverrideScenario = scenarios.FirstOrDefault(candidate => candidate.Algorithm != scenario.Algorithm);
            await AssertBucketDefaultManagedSseInheritanceOrExplicitOverrideAsync(storage, settings, scenario, explicitOverrideScenario);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task NativeS3Provider_WithConfiguredLocalCompatibleEndpoint_ExercisesMultipartUploadListingLifecycle()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-multipart-list-{Guid.NewGuid():N}";
        var pendingMultipartUploads = new List<(string Key, string UploadId)>();

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(createBucket.IsSuccess, createBucket.Error?.Message);

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(enableVersioning.IsSuccess, enableVersioning.Error?.Message);
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            const string alphaKey = "docs/alpha.bin";
            const string betaKey = "docs/beta.bin";
            const string ignoredKey = "videos/clip.bin";

            var firstAlphaUpload = await InitiateMultipartUploadAsync(storage, bucketName, alphaKey);
            Assert.True(firstAlphaUpload.IsSuccess, firstAlphaUpload.Error?.Message);
            pendingMultipartUploads.Add((alphaKey, firstAlphaUpload.Value!.UploadId));

            await Task.Delay(TimeSpan.FromMilliseconds(1100));

            var secondAlphaUpload = await InitiateMultipartUploadAsync(storage, bucketName, alphaKey);
            Assert.True(secondAlphaUpload.IsSuccess, secondAlphaUpload.Error?.Message);
            pendingMultipartUploads.Add((alphaKey, secondAlphaUpload.Value!.UploadId));
            Assert.NotEqual(firstAlphaUpload.Value.UploadId, secondAlphaUpload.Value.UploadId);

            var betaUpload = await InitiateMultipartUploadAsync(storage, bucketName, betaKey);
            Assert.True(betaUpload.IsSuccess, betaUpload.Error?.Message);
            pendingMultipartUploads.Add((betaKey, betaUpload.Value!.UploadId));

            var ignoredUpload = await InitiateMultipartUploadAsync(storage, bucketName, ignoredKey);
            Assert.True(ignoredUpload.IsSuccess, ignoredUpload.Error?.Message);
            pendingMultipartUploads.Add((ignoredKey, ignoredUpload.Value!.UploadId));

            var firstPage = await storage.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
            {
                BucketName = bucketName,
                Prefix = "docs/",
                PageSize = 2
            }).ToListAsync();

            Assert.Equal(2, firstPage.Count);
            Assert.All(firstPage, upload =>
            {
                Assert.Equal(bucketName, upload.BucketName);
                Assert.Equal(alphaKey, upload.Key);
                Assert.False(string.IsNullOrWhiteSpace(upload.UploadId));
                Assert.True(upload.InitiatedAtUtc > DateTimeOffset.UnixEpoch);
            });
            Assert.Contains(firstPage, upload => upload.UploadId == firstAlphaUpload.Value.UploadId);
            Assert.Contains(firstPage, upload => upload.UploadId == secondAlphaUpload.Value.UploadId);

            var secondPage = await storage.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
            {
                BucketName = bucketName,
                Prefix = "docs/",
                KeyMarker = firstPage[^1].Key,
                UploadIdMarker = firstPage[^1].UploadId
            }).ToListAsync();

            var remainingUpload = Assert.Single(secondPage);
            Assert.Equal(bucketName, remainingUpload.BucketName);
            Assert.Equal(betaKey, remainingUpload.Key);
            Assert.Equal(betaUpload.Value.UploadId, remainingUpload.UploadId);

            var betaPart = await UploadPartAsync(
                storage,
                bucketName,
                betaKey,
                betaUpload.Value.UploadId,
                1,
                CreateFilledBuffer(5 * 1024 * 1024, (byte)'b'));
            Assert.True(betaPart.IsSuccess, betaPart.Error?.Message);

            var completeBeta = await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = betaKey,
                UploadId = betaUpload.Value.UploadId,
                Parts =
                [
                    betaPart.Value!
                ]
            });
            Assert.True(completeBeta.IsSuccess, completeBeta.Error?.Message);
            pendingMultipartUploads.Remove((betaKey, betaUpload.Value.UploadId));

            var abortSecondAlpha = await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = alphaKey,
                UploadId = secondAlphaUpload.Value.UploadId
            });
            Assert.True(abortSecondAlpha.IsSuccess, abortSecondAlpha.Error?.Message);
            pendingMultipartUploads.Remove((alphaKey, secondAlphaUpload.Value.UploadId));

            var remainingUploads = await storage.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
            {
                BucketName = bucketName,
                Prefix = "docs/"
            }).ToListAsync();

            var remainingPendingUpload = Assert.Single(remainingUploads);
            Assert.Equal(bucketName, remainingPendingUpload.BucketName);
            Assert.Equal(alphaKey, remainingPendingUpload.Key);
            Assert.Equal(firstAlphaUpload.Value.UploadId, remainingPendingUpload.UploadId);
            Assert.DoesNotContain(remainingUploads, upload => upload.UploadId == secondAlphaUpload.Value.UploadId);
            Assert.DoesNotContain(remainingUploads, upload => upload.UploadId == betaUpload.Value.UploadId);
            Assert.DoesNotContain(remainingUploads, upload => upload.Key == ignoredKey);
        }
        finally
        {
            foreach (var pendingMultipartUpload in pendingMultipartUploads)
            {
                try
                {
                    await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = pendingMultipartUpload.Key,
                        UploadId = pendingMultipartUpload.UploadId
                    });
                }
                catch
                {
                }
            }

            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    // --- Customer Encryption (SSE-C) Conformance ---

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task S3Compatible_PutObject_WithSseC_GetObject_WithSameKey_RoundTrips()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-ssec-roundtrip-{Guid.NewGuid():N}";
        var customerKey = CreateSseCCustomerEncryptionSettings();

        try
        {
            Assert.True((await storage.CreateBucketAsync(new CreateBucketRequest { BucketName = bucketName })).IsSuccess);

            const string objectKey = "docs/ssec-roundtrip.txt";
            const string payload = "SSE-C round-trip payload";
            var payloadBytes = Encoding.UTF8.GetBytes(payload);
            using var content = new MemoryStream(payloadBytes, writable: false);

            var putResult = await storage.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                Content = content,
                ContentLength = payloadBytes.Length,
                ContentType = "text/plain",
                CustomerEncryption = customerKey
            });
            Assert.True(putResult.IsSuccess, putResult.Error?.Message);

            var getResult = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                CustomerEncryption = customerKey
            });
            Assert.True(getResult.IsSuccess, getResult.Error?.Message);
            await using var getResponse = getResult.Value!;
            using var reader = new StreamReader(getResponse.Content, Encoding.UTF8);
            Assert.Equal(payload, await reader.ReadToEndAsync());

            var getWithoutKeyResult = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.False(getWithoutKeyResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "SSE-C GET without key", "Expected 400 when reading SSE-C object without customer key."));
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task S3Compatible_HeadObject_WithSseC_ReturnsMetadata()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-ssec-head-{Guid.NewGuid():N}";
        var customerKey = CreateSseCCustomerEncryptionSettings();

        try
        {
            Assert.True((await storage.CreateBucketAsync(new CreateBucketRequest { BucketName = bucketName })).IsSuccess);

            const string objectKey = "docs/ssec-head.txt";
            var payloadBytes = Encoding.UTF8.GetBytes("SSE-C head payload");
            using var content = new MemoryStream(payloadBytes, writable: false);

            var putResult = await storage.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                Content = content,
                ContentLength = payloadBytes.Length,
                ContentType = "text/plain",
                CustomerEncryption = customerKey
            });
            Assert.True(putResult.IsSuccess, putResult.Error?.Message);

            var headResult = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                CustomerEncryption = customerKey
            });
            Assert.True(headResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "SSE-C HEAD", headResult.Error?.Message));
            Assert.NotNull(headResult.Value!.ETag);
            Assert.Equal(payloadBytes.Length, headResult.Value.ContentLength);
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task S3Compatible_CopyObject_SseCSourceToSseCDestination_RoundTrips()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-ssec-copy-{Guid.NewGuid():N}";
        var sourceKey = CreateSseCCustomerEncryptionSettings();
        var destinationKey = CreateSseCCustomerEncryptionSettings();

        try
        {
            Assert.True((await storage.CreateBucketAsync(new CreateBucketRequest { BucketName = bucketName })).IsSuccess);

            const string sourceObjectKey = "docs/ssec-source.txt";
            const string destinationObjectKey = "docs/ssec-destination.txt";
            const string payload = "SSE-C copy payload";
            var payloadBytes = Encoding.UTF8.GetBytes(payload);
            using var content = new MemoryStream(payloadBytes, writable: false);

            var putResult = await storage.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = sourceObjectKey,
                Content = content,
                ContentLength = payloadBytes.Length,
                ContentType = "text/plain",
                CustomerEncryption = sourceKey
            });
            Assert.True(putResult.IsSuccess, putResult.Error?.Message);

            var copyResult = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceObjectKey,
                DestinationBucketName = bucketName,
                DestinationKey = destinationObjectKey,
                SourceCustomerEncryption = sourceKey,
                DestinationCustomerEncryption = destinationKey
            });
            Assert.True(copyResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "SSE-C copy", copyResult.Error?.Message));

            var getResult = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = destinationObjectKey,
                CustomerEncryption = destinationKey
            });
            Assert.True(getResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "SSE-C GET after copy", getResult.Error?.Message));
            await using var getResponse = getResult.Value!;
            using var reader = new StreamReader(getResponse.Content, Encoding.UTF8);
            Assert.Equal(payload, await reader.ReadToEndAsync());
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task S3Compatible_PutObject_WithSseC_GetObject_WithWrongKey_Fails()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-ssec-wrongkey-{Guid.NewGuid():N}";
        var correctKey = CreateSseCCustomerEncryptionSettings();
        var wrongKey = CreateSseCCustomerEncryptionSettings();

        try
        {
            Assert.True((await storage.CreateBucketAsync(new CreateBucketRequest { BucketName = bucketName })).IsSuccess);

            const string objectKey = "docs/ssec-wrongkey.txt";
            var payloadBytes = Encoding.UTF8.GetBytes("SSE-C wrong key payload");
            using var content = new MemoryStream(payloadBytes, writable: false);

            var putResult = await storage.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                Content = content,
                ContentLength = payloadBytes.Length,
                ContentType = "text/plain",
                CustomerEncryption = correctKey
            });
            Assert.True(putResult.IsSuccess, putResult.Error?.Message);

            var getResult = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                CustomerEncryption = wrongKey
            });
            Assert.False(getResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "SSE-C GET with wrong key",
                    "Expected failure when reading SSE-C object with incorrect customer key."));
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    [Fact]
    [Trait("Category", "LocalS3Compatible")]
    public async Task S3Compatible_InitiateMultipartUpload_WithSseC_UploadPart_CompleteUpload_RoundTrips()
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-ssec-multipart-{Guid.NewGuid():N}";
        var customerKey = CreateSseCCustomerEncryptionSettings();
        string? pendingUploadId = null;

        try
        {
            Assert.True((await storage.CreateBucketAsync(new CreateBucketRequest { BucketName = bucketName })).IsSuccess);

            const string objectKey = "docs/ssec-multipart.bin";
            var part1Payload = CreateFilledBuffer(5 * 1024 * 1024, (byte)'a');
            var part2Payload = CreateFilledBuffer(1024 * 1024, (byte)'b');
            var expectedTotal = part1Payload.Length + part2Payload.Length;

            var initiateResult = await storage.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                ContentType = "application/octet-stream",
                CustomerEncryption = customerKey
            });
            Assert.True(initiateResult.IsSuccess, initiateResult.Error?.Message);
            pendingUploadId = initiateResult.Value!.UploadId;

            using var part1Content = new MemoryStream(part1Payload, writable: false);
            var part1Result = await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = pendingUploadId,
                PartNumber = 1,
                Content = part1Content,
                ContentLength = part1Payload.Length,
                CustomerEncryption = customerKey
            });
            Assert.True(part1Result.IsSuccess, part1Result.Error?.Message);

            using var part2Content = new MemoryStream(part2Payload, writable: false);
            var part2Result = await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = pendingUploadId,
                PartNumber = 2,
                Content = part2Content,
                ContentLength = part2Payload.Length,
                CustomerEncryption = customerKey
            });
            Assert.True(part2Result.IsSuccess, part2Result.Error?.Message);

            var completeResult = await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = pendingUploadId,
                Parts = [part1Result.Value!, part2Result.Value!]
            });
            Assert.True(completeResult.IsSuccess, completeResult.Error?.Message);
            pendingUploadId = null;

            var getResult = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                CustomerEncryption = customerKey
            });
            Assert.True(getResult.IsSuccess,
                CreateConformanceFailureMessage(settings, "SSE-C multipart GET", getResult.Error?.Message));
            await using var getResponse = getResult.Value!;
            using var ms = new MemoryStream();
            await getResponse.Content.CopyToAsync(ms);
            Assert.Equal(expectedTotal, ms.Length);
        }
        finally
        {
            if (pendingUploadId is not null)
            {
                try
                {
                    await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = "docs/ssec-multipart.bin",
                        UploadId = pendingUploadId
                    });
                }
                catch
                {
                }
            }

            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    private static async Task AssertMultipartCopyChecksumConformanceAsync(
        string checksumAlgorithm,
        string checksumKey,
        Func<string, string> computeChecksum,
        Func<string[], string> computeCompositeChecksum)
    {
        var settings = LocalS3CompatibleEndpointSettings.TryLoad();
        if (settings is null)
            return;

        var options = settings.CreateOptions();
        using var client = new AwsS3StorageClient(options);
        var storage = new S3StorageService(options, client);
        var bucketName = $"compat-{checksumKey}-{Guid.NewGuid():N}";
        var sourceKey = $"docs/multipart-{checksumKey}.bin";
        var copiedKey = $"docs/multipart-{checksumKey}-copy.bin";
        string? pendingMultipartUploadId = null;

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} create bucket", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} enable bucket versioning", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var part1Text = new string('a', 5 * 1024 * 1024);
            var part2Text = new string('b', 1024 * 1024);
            var part1Payload = Encoding.UTF8.GetBytes(part1Text);
            var part2Payload = Encoding.UTF8.GetBytes(part2Text);
            var part1Checksum = computeChecksum(part1Text);
            var part2Checksum = computeChecksum(part2Text);
            var compositeChecksum = computeCompositeChecksum(
            [
                part1Checksum,
                part2Checksum
            ]);

            var initiateMultipart = await InitiateMultipartUploadAsync(storage, bucketName, sourceKey, checksumAlgorithm);
            Assert.True(
                initiateMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} initiate multipart upload", initiateMultipart.Error?.Message));
            Assert.Equal(checksumKey, initiateMultipart.Value!.ChecksumAlgorithm);
            pendingMultipartUploadId = initiateMultipart.Value.UploadId;

            var part1 = await UploadPartAsync(
                storage,
                bucketName,
                sourceKey,
                pendingMultipartUploadId,
                1,
                part1Payload,
                checksumAlgorithm,
                checksumKey,
                part1Checksum);
            Assert.True(
                part1.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} upload multipart part 1", part1.Error?.Message));
            AssertChecksumValue(
                part1.Value!.Checksums,
                checksumKey,
                part1Checksum,
                settings,
                $"{checksumAlgorithm} multipart part 1 response");

            var part2 = await UploadPartAsync(
                storage,
                bucketName,
                sourceKey,
                pendingMultipartUploadId,
                2,
                part2Payload,
                checksumAlgorithm,
                checksumKey,
                part2Checksum);
            Assert.True(
                part2.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} upload multipart part 2", part2.Error?.Message));
            AssertChecksumValue(
                part2.Value!.Checksums,
                checksumKey,
                part2Checksum,
                settings,
                $"{checksumAlgorithm} multipart part 2 response");

            var completeMultipart = await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                UploadId = pendingMultipartUploadId,
                Parts =
                [
                    part1.Value!,
                    part2.Value!
                ]
            });
            Assert.True(
                completeMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} complete multipart upload", completeMultipart.Error?.Message));
            pendingMultipartUploadId = null;
            AssertChecksumValue(
                completeMultipart.Value!.Checksums,
                checksumKey,
                compositeChecksum,
                settings,
                $"{checksumAlgorithm} completed multipart object");

            var sourceHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey
            });
            Assert.True(
                sourceHead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} head multipart object", sourceHead.Error?.Message));
            AssertChecksumValue(
                sourceHead.Value!.Checksums,
                checksumKey,
                compositeChecksum,
                settings,
                $"{checksumAlgorithm} multipart head metadata");

            var copyResult = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                DestinationBucketName = bucketName,
                DestinationKey = copiedKey
            });
            Assert.True(
                copyResult.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} copy multipart object", copyResult.Error?.Message));
            AssertChecksumValue(
                copyResult.Value!.Checksums,
                checksumKey,
                compositeChecksum,
                settings,
                $"{checksumAlgorithm} copy result metadata");

            var copiedHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = copiedKey
            });
            Assert.True(
                copiedHead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{checksumAlgorithm} head copied object", copiedHead.Error?.Message));
            AssertChecksumValue(
                copiedHead.Value!.Checksums,
                checksumKey,
                compositeChecksum,
                settings,
                $"{checksumAlgorithm} copied object head metadata");
        }
        finally
        {
            if (pendingMultipartUploadId is not null)
            {
                try
                {
                    await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = sourceKey,
                        UploadId = pendingMultipartUploadId
                    });
                }
                catch
                {
                }
            }

            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    private static async Task AssertHistoricalCopyWithChecksumAndManagedSseSurvivesCurrentDeleteMarkerAsync(
        S3StorageService storage,
        LocalS3CompatibleEndpointSettings settings,
        ManagedServerSideEncryptionScenario scenario)
    {
        var bucketName = $"compat-copy-checksum-sse-{scenario.BucketSuffix}-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.txt";
        const string destinationKey = "docs/historical-copy.txt";
        const string historicalPayload = "checksum and sse historical payload";
        const string currentPayload = "checksum and sse current payload";
        var expectedHistoricalChecksum = ComputeSha256Base64(Encoding.UTF8.GetBytes(historicalPayload));

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} create bucket for checksum+sse historical copy", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} enable versioning for checksum+sse historical copy", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var firstVersion = await PutManagedTextObjectAsync(storage, bucketName, sourceKey, historicalPayload, scenario);
            Assert.True(
                firstVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} put checksum+sse historical copy source version", firstVersion.Error?.Message));

            var secondVersion = await PutManagedTextObjectAsync(storage, bucketName, sourceKey, currentPayload, scenario);
            Assert.True(
                secondVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} put checksum+sse current copy source version", secondVersion.Error?.Message));

            var firstVersionId = Assert.IsType<string>(firstVersion.Value?.VersionId);
            Assert.NotEqual(firstVersionId, secondVersion.Value!.VersionId);

            var firstHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                VersionId = firstVersionId
            });
            Assert.True(
                firstHead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} head checksum+sse historical copy source version", firstHead.Error?.Message));
            AssertChecksumValue(
                firstHead.Value!.Checksums,
                "sha256",
                expectedHistoricalChecksum,
                settings,
                $"{scenario.DisplayName} head checksum+sse historical copy source checksum");
            AssertManagedServerSideEncryption(
                firstHead.Value.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} head checksum+sse historical copy source encryption");
            var firstVersionEtag = Assert.IsType<string>(firstHead.Value.ETag);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} create current delete marker before checksum+sse historical copy", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);

            var historicalCopy = await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = firstVersionId,
                SourceIfMatchETag = firstVersionEtag,
                SourceIfUnmodifiedSinceUtc = firstHead.Value.LastModifiedUtc.AddMinutes(5),
                DestinationBucketName = bucketName,
                DestinationKey = destinationKey,
                ChecksumAlgorithm = "SHA256",
                DestinationServerSideEncryption = scenario.RequestEncryption
            });
            Assert.True(
                historicalCopy.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} copy checksum+sse historical version while delete marker is current", historicalCopy.Error?.Message));
            AssertChecksumValue(
                historicalCopy.Value!.Checksums,
                "sha256",
                expectedHistoricalChecksum,
                settings,
                $"{scenario.DisplayName} checksum+sse historical copy result checksum");
            AssertManagedServerSideEncryption(
                historicalCopy.Value.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} checksum+sse historical copy result encryption");

            var copiedHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = destinationKey
            });
            Assert.True(
                copiedHead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} head checksum+sse historical copy destination", copiedHead.Error?.Message));
            Assert.Equal(historicalCopy.Value.VersionId, copiedHead.Value!.VersionId);
            AssertChecksumValue(
                copiedHead.Value.Checksums,
                "sha256",
                expectedHistoricalChecksum,
                settings,
                $"{scenario.DisplayName} head checksum+sse historical copy destination checksum");
            AssertManagedServerSideEncryption(
                copiedHead.Value.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} head checksum+sse historical copy destination encryption");

            Assert.Equal(
                historicalPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    destinationKey,
                    settings,
                    $"{scenario.DisplayName} read checksum+sse historical copy after current delete marker"));
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    private static async Task AssertHistoricalUploadPartCopyWithChecksumAndManagedSseSurvivesCurrentDeleteMarkerAsync(
        S3StorageService storage,
        LocalS3CompatibleEndpointSettings settings,
        ManagedServerSideEncryptionScenario scenario)
    {
        var bucketName = $"compat-copy-part-checksum-sse-{scenario.BucketSuffix}-{Guid.NewGuid():N}";
        const string sourceKey = "docs/source.bin";
        const string destinationKey = "docs/copied.bin";
        var historicalPart1 = CreateFilledBuffer(5 * 1024 * 1024, (byte)'a');
        var historicalPart2 = CreateFilledBuffer(1024 * 1024, (byte)'b');
        var historicalPayload = new byte[historicalPart1.Length + historicalPart2.Length];
        Buffer.BlockCopy(historicalPart1, 0, historicalPayload, 0, historicalPart1.Length);
        Buffer.BlockCopy(historicalPart2, 0, historicalPayload, historicalPart1.Length, historicalPart2.Length);

        var currentPayload = CreateFilledBuffer(historicalPayload.Length, (byte)'z');
        var historicalChecksum = ComputeSha256Base64(historicalPayload);
        var part1Checksum = ComputeSha256Base64(historicalPart1);
        var part2Checksum = ComputeSha256Base64(historicalPart2);
        var compositeChecksum = ComputeMultipartSha256Base64(part1Checksum, part2Checksum);
        string? pendingMultipartUploadId = null;

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} create bucket for checksum+sse historical upload part copy", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} enable versioning for checksum+sse historical upload part copy", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var historicalVersion = await PutManagedObjectAsync(storage, bucketName, sourceKey, historicalPayload, "application/octet-stream", scenario);
            Assert.True(
                historicalVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} put checksum+sse historical upload part copy source version", historicalVersion.Error?.Message));

            var currentVersion = await PutManagedObjectAsync(storage, bucketName, sourceKey, currentPayload, "application/octet-stream", scenario);
            Assert.True(
                currentVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} put current upload part copy source version before delete marker", currentVersion.Error?.Message));
            var historicalVersionId = Assert.IsType<string>(historicalVersion.Value?.VersionId);
            Assert.NotEqual(historicalVersionId, currentVersion.Value!.VersionId);

            var historicalHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey,
                VersionId = historicalVersionId
            });
            Assert.True(
                historicalHead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} head checksum+sse historical upload part copy source version", historicalHead.Error?.Message));
            AssertChecksumValue(
                historicalHead.Value!.Checksums,
                "sha256",
                historicalChecksum,
                settings,
                $"{scenario.DisplayName} head checksum+sse historical upload part copy source checksum");
            AssertManagedServerSideEncryption(
                historicalHead.Value.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} head checksum+sse historical upload part copy source encryption");
            var historicalEtag = Assert.IsType<string>(historicalHead.Value.ETag);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = sourceKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} create current delete marker before checksum+sse historical upload part copy", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);

            var initiateMultipart = await InitiateManagedMultipartUploadAsync(storage, bucketName, destinationKey, scenario);
            Assert.True(
                initiateMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} initiate checksum+sse historical multipart destination", initiateMultipart.Error?.Message));
            Assert.Equal("sha256", initiateMultipart.Value!.ChecksumAlgorithm);
            pendingMultipartUploadId = initiateMultipart.Value.UploadId;

            var sourceIfUnmodifiedSinceUtc = historicalHead.Value.LastModifiedUtc.AddMinutes(5);

            var copyPart1 = await storage.UploadPartCopyAsync(new UploadPartCopyRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                PartNumber = 1,
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = historicalVersionId,
                SourceIfMatchETag = historicalEtag,
                SourceIfUnmodifiedSinceUtc = sourceIfUnmodifiedSinceUtc,
                SourceRange = new ObjectRange
                {
                    Start = 0,
                    End = historicalPart1.Length - 1L
                },
                ChecksumAlgorithm = "SHA256"
            });
            Assert.True(
                copyPart1.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} copy checksum+sse historical multipart part 1 after delete marker", copyPart1.Error?.Message));
            AssertChecksumValue(
                copyPart1.Value!.Checksums,
                "sha256",
                part1Checksum,
                settings,
                $"{scenario.DisplayName} checksum+sse historical multipart part 1 checksum");

            var copyPart2 = await storage.UploadPartCopyAsync(new UploadPartCopyRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                PartNumber = 2,
                SourceBucketName = bucketName,
                SourceKey = sourceKey,
                SourceVersionId = historicalVersionId,
                SourceIfMatchETag = historicalEtag,
                SourceIfUnmodifiedSinceUtc = sourceIfUnmodifiedSinceUtc,
                SourceRange = new ObjectRange
                {
                    Start = historicalPart1.Length,
                    End = historicalPayload.Length - 1L
                },
                ChecksumAlgorithm = "SHA256"
            });
            Assert.True(
                copyPart2.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} copy checksum+sse historical multipart part 2 after delete marker", copyPart2.Error?.Message));
            AssertChecksumValue(
                copyPart2.Value!.Checksums,
                "sha256",
                part2Checksum,
                settings,
                $"{scenario.DisplayName} checksum+sse historical multipart part 2 checksum");

            var completeMultipart = await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = destinationKey,
                UploadId = pendingMultipartUploadId,
                Parts =
                [
                    copyPart1.Value,
                    copyPart2.Value
                ]
            });
            Assert.True(
                completeMultipart.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} complete checksum+sse historical multipart copy after delete marker", completeMultipart.Error?.Message));
            pendingMultipartUploadId = null;
            AssertChecksumValue(
                completeMultipart.Value!.Checksums,
                "sha256",
                compositeChecksum,
                settings,
                $"{scenario.DisplayName} complete checksum+sse historical multipart copy checksum after delete marker");
            AssertManagedServerSideEncryption(
                completeMultipart.Value.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} complete checksum+sse historical multipart copy encryption after delete marker");

            var copiedHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = destinationKey
            });
            Assert.True(
                copiedHead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} head checksum+sse historical multipart copy destination", copiedHead.Error?.Message));
            AssertChecksumValue(
                copiedHead.Value!.Checksums,
                "sha256",
                compositeChecksum,
                settings,
                $"{scenario.DisplayName} head checksum+sse historical multipart copy destination checksum");
            AssertManagedServerSideEncryption(
                copiedHead.Value.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} head checksum+sse historical multipart copy destination encryption");

            var copiedObject = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = destinationKey
            });
            Assert.True(
                copiedObject.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} get checksum+sse historical multipart copy destination", copiedObject.Error?.Message));

            await using var copiedResponse = copiedObject.Value!;
            AssertManagedServerSideEncryption(
                copiedResponse.Object.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} get checksum+sse historical multipart copy destination encryption");
            using var copiedContent = new MemoryStream();
            await copiedResponse.Content.CopyToAsync(copiedContent);
            Assert.Equal(historicalPayload, copiedContent.ToArray());
        }
        finally
        {
            if (pendingMultipartUploadId is not null)
            {
                try
                {
                    await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = destinationKey,
                        UploadId = pendingMultipartUploadId
                    });
                }
                catch
                {
                }
            }

            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    private static async Task AssertDirectAndDelegatedHistoricalManagedSseReadParityAsync(
        S3StorageService storage,
        S3StorageObjectLocationResolver resolver,
        S3StorageOptions options,
        LocalS3CompatibleEndpointSettings settings,
        ManagedServerSideEncryptionScenario scenario)
    {
        var bucketName = $"compat-direct-delegated-sse-{scenario.BucketSuffix}-{Guid.NewGuid():N}";
        const string objectKey = "docs/source.txt";
        const string historicalPayload = "managed sse historical version";
        const string currentPayload = "managed sse current version";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} create bucket for direct/delegated managed SSE historical read parity", createBucket.Error?.Message));

            var enableVersioning = await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = BucketVersioningStatus.Enabled
            });
            Assert.True(
                enableVersioning.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} enable versioning for direct/delegated managed SSE historical read parity", enableVersioning.Error?.Message));
            Assert.Equal(BucketVersioningStatus.Enabled, enableVersioning.Value!.Status);

            var historicalVersion = await PutManagedTextObjectAsync(storage, bucketName, objectKey, historicalPayload, scenario);
            Assert.True(
                historicalVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} put historical managed SSE version for direct/delegated read parity", historicalVersion.Error?.Message));

            var currentVersion = await PutManagedTextObjectAsync(storage, bucketName, objectKey, currentPayload, scenario);
            Assert.True(
                currentVersion.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} put current managed SSE version for direct/delegated read parity", currentVersion.Error?.Message));

            var historicalVersionId = Assert.IsType<string>(historicalVersion.Value?.VersionId);
            Assert.NotEqual(historicalVersionId, currentVersion.Value!.VersionId);

            var historicalHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = historicalVersionId
            });
            Assert.True(
                historicalHead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} head historical managed SSE version for direct/delegated read parity", historicalHead.Error?.Message));
            AssertManagedServerSideEncryption(
                historicalHead.Value!.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} head historical managed SSE version for direct/delegated read parity");

            var resolvedLocation = await resolver.ResolveReadLocationAsync(new ResolveObjectLocationRequest
            {
                ProviderName = options.ProviderName,
                BucketName = bucketName,
                Key = objectKey,
                VersionId = historicalVersionId,
                ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(5)
            });
            var delegatedLocation = Assert.IsType<StorageResolvedObjectLocation>(resolvedLocation);
            var delegatedUri = Assert.IsType<Uri>(delegatedLocation.Location);

            var directGrantResult = await storage.PresignObjectDirectAsync(new StorageDirectObjectAccessRequest
            {
                Operation = StorageDirectObjectAccessOperation.GetObject,
                BucketName = bucketName,
                Key = objectKey,
                VersionId = historicalVersionId,
                ExpiresInSeconds = 300
            });
            Assert.True(
                directGrantResult.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} presign direct managed SSE historical read", directGrantResult.Error?.Message));
            var directGrant = Assert.IsType<StorageDirectObjectAccessGrant>(directGrantResult.Value);

            var deleteCurrent = await storage.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            Assert.True(
                deleteCurrent.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} create current delete marker before direct/delegated managed SSE historical read parity", deleteCurrent.Error?.Message));
            Assert.True(deleteCurrent.Value!.IsDeleteMarker);

            using var httpClient = CreatePresignedHttpClient(settings.ServiceUri);

            string? delegatedKmsKeyId;
            using (var delegatedResponse = await SendPresignedGetAsync(httpClient, delegatedUri))
            {
                Assert.True(
                    delegatedResponse.StatusCode == HttpStatusCode.OK,
                    CreateConformanceFailureMessage(
                        settings,
                        $"{scenario.DisplayName} read delegated historical managed SSE response",
                        $"{(int)delegatedResponse.StatusCode} {delegatedResponse.ReasonPhrase}"));
                AssertManagedServerSideEncryptionResponseHeader(
                    delegatedResponse,
                    scenario,
                    settings,
                    $"{scenario.DisplayName} delegated historical managed SSE response header");
                delegatedKmsKeyId = GetResponseHeaderValue(delegatedResponse, "x-amz-server-side-encryption-aws-kms-key-id");
                Assert.Equal(historicalPayload, await delegatedResponse.Content.ReadAsStringAsync());
            }

            string? directKmsKeyId;
            using (var directResponse = await SendPresignedGetAsync(httpClient, directGrant.Url))
            {
                Assert.True(
                    directResponse.StatusCode == HttpStatusCode.OK,
                    CreateConformanceFailureMessage(
                        settings,
                        $"{scenario.DisplayName} read direct historical managed SSE response",
                        $"{(int)directResponse.StatusCode} {directResponse.ReasonPhrase}"));
                AssertManagedServerSideEncryptionResponseHeader(
                    directResponse,
                    scenario,
                    settings,
                    $"{scenario.DisplayName} direct historical managed SSE response header");
                directKmsKeyId = GetResponseHeaderValue(directResponse, "x-amz-server-side-encryption-aws-kms-key-id");
                Assert.Equal(historicalPayload, await directResponse.Content.ReadAsStringAsync());
            }

            Assert.True(
                string.Equals(delegatedKmsKeyId, directKmsKeyId, StringComparison.Ordinal),
                $"{CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} historical managed SSE response header parity")} Expected delegated and direct KMS key identifiers to match, but found delegated '{delegatedKmsKeyId ?? "<missing>"}' and direct '{directKmsKeyId ?? "<missing>"}'.");

            if (scenario.RequestEncryption.KeyId is not null)
            {
                Assert.False(
                    string.IsNullOrWhiteSpace(delegatedKmsKeyId),
                    $"{CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} delegated historical managed SSE KMS metadata visibility")} Expected a delegated response KMS key identifier, but the endpoint returned none.");
            }

            var historicalRead = await storage.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                VersionId = historicalVersionId
            });
            Assert.True(
                historicalRead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{scenario.DisplayName} read historical managed SSE version via storage service", historicalRead.Error?.Message));

            await using var historicalReadResponse = historicalRead.Value!;
            Assert.Equal(historicalVersionId, historicalReadResponse.Object.VersionId);
            AssertManagedServerSideEncryption(
                historicalReadResponse.Object.ServerSideEncryption,
                scenario,
                settings,
                $"{scenario.DisplayName} storage historical managed SSE response encryption");
            using var reader = new StreamReader(historicalReadResponse.Content, Encoding.UTF8);
            Assert.Equal(historicalPayload, await reader.ReadToEndAsync());
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    private static async Task AssertBucketDefaultManagedSseInheritanceOrExplicitOverrideAsync(
        S3StorageService storage,
        LocalS3CompatibleEndpointSettings settings,
        ManagedServerSideEncryptionScenario defaultScenario,
        ManagedServerSideEncryptionScenario? explicitOverrideScenario)
    {
        var bucketName = $"compat-bucket-default-sse-{defaultScenario.BucketSuffix}-{Guid.NewGuid():N}";
        const string inheritedKey = "docs/default-inherited.txt";
        const string explicitOverrideKey = "docs/default-explicit.txt";
        const string inheritedPayload = "bucket default managed sse payload";
        const string explicitOverridePayload = "bucket default explicit managed sse payload";

        try
        {
            var createBucket = await storage.CreateBucketAsync(new CreateBucketRequest
            {
                BucketName = bucketName
            });
            Assert.True(
                createBucket.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{defaultScenario.DisplayName} create bucket for bucket default managed SSE", createBucket.Error?.Message));

            var putBucketEncryption = await storage.PutBucketDefaultEncryptionAsync(new PutBucketDefaultEncryptionRequest
            {
                BucketName = bucketName,
                Rule = defaultScenario.BucketDefaultRule
            });
            Assert.True(
                putBucketEncryption.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{defaultScenario.DisplayName} put bucket default managed SSE configuration", putBucketEncryption.Error?.Message));
            AssertBucketDefaultEncryptionRule(
                putBucketEncryption.Value!.Rule,
                defaultScenario,
                settings,
                $"{defaultScenario.DisplayName} put bucket default managed SSE configuration");

            var getBucketEncryption = await storage.GetBucketDefaultEncryptionAsync(bucketName);
            Assert.True(
                getBucketEncryption.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{defaultScenario.DisplayName} get bucket default managed SSE configuration", getBucketEncryption.Error?.Message));
            AssertBucketDefaultEncryptionRule(
                getBucketEncryption.Value!.Rule,
                defaultScenario,
                settings,
                $"{defaultScenario.DisplayName} get bucket default managed SSE configuration");

            var inheritedPut = await PutPlainTextObjectAsync(storage, bucketName, inheritedKey, inheritedPayload);
            Assert.True(
                inheritedPut.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{defaultScenario.DisplayName} put object inheriting bucket default managed SSE", inheritedPut.Error?.Message));

            var inheritedHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = inheritedKey
            });
            Assert.True(
                inheritedHead.IsSuccess,
                CreateConformanceFailureMessage(settings, $"{defaultScenario.DisplayName} head inherited bucket default managed SSE object", inheritedHead.Error?.Message));
            AssertManagedServerSideEncryption(
                inheritedHead.Value!.ServerSideEncryption,
                defaultScenario,
                settings,
                $"{defaultScenario.DisplayName} inherited bucket default managed SSE object encryption");

            Assert.Equal(
                inheritedPayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    inheritedKey,
                    settings,
                    $"{defaultScenario.DisplayName} read object inheriting bucket default managed SSE"));

            if (explicitOverrideScenario is null)
                return;

            var explicitOverridePut = await PutManagedTextObjectAsync(storage, bucketName, explicitOverrideKey, explicitOverridePayload, explicitOverrideScenario);
            Assert.True(
                explicitOverridePut.IsSuccess,
                CreateConformanceFailureMessage(
                    settings,
                    $"{defaultScenario.DisplayName} bucket default with explicit {explicitOverrideScenario.DisplayName} override",
                    explicitOverridePut.Error?.Message));

            var explicitOverrideHead = await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = bucketName,
                Key = explicitOverrideKey
            });
            Assert.True(
                explicitOverrideHead.IsSuccess,
                CreateConformanceFailureMessage(
                    settings,
                    $"{defaultScenario.DisplayName} head explicit {explicitOverrideScenario.DisplayName} override over bucket default managed SSE",
                    explicitOverrideHead.Error?.Message));
            AssertManagedServerSideEncryption(
                explicitOverrideHead.Value!.ServerSideEncryption,
                explicitOverrideScenario,
                settings,
                $"{defaultScenario.DisplayName} explicit {explicitOverrideScenario.DisplayName} override over bucket default managed SSE");

            Assert.Equal(
                explicitOverridePayload,
                await ReadRequiredTextObjectAsync(
                    storage,
                    bucketName,
                    explicitOverrideKey,
                    settings,
                    $"{defaultScenario.DisplayName} read explicit {explicitOverrideScenario.DisplayName} override over bucket default managed SSE"));
        }
        finally
        {
            await BestEffortDeleteBucketAsync(storage, bucketName);
        }
    }

    private static async Task<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(
        S3StorageService storage,
        string bucketName,
        string key)
    {
        return await storage.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = "application/octet-stream",
            ChecksumAlgorithm = "SHA256",
            ServerSideEncryption = CreateAes256Settings()
        });
    }

    private static async Task<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        string checksumAlgorithm)
    {
        return await storage.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = "application/octet-stream",
            ChecksumAlgorithm = checksumAlgorithm
        });
    }

    private static async Task<StorageResult<MultipartUploadInfo>> InitiateManagedMultipartUploadAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        ManagedServerSideEncryptionScenario scenario)
    {
        return await storage.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = "application/octet-stream",
            ChecksumAlgorithm = "SHA256",
            ServerSideEncryption = scenario.RequestEncryption
        });
    }

    private static async Task<StorageResult<ObjectInfo>> PutTextObjectAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        string payload)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);
        using var content = new MemoryStream(bytes, writable: false);

        return await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            Content = content,
            ContentLength = bytes.Length,
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = ComputeSha256Base64(bytes)
            },
            ServerSideEncryption = CreateAes256Settings()
        });
    }

    private static Task<StorageResult<ObjectInfo>> PutManagedTextObjectAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        string payload,
        ManagedServerSideEncryptionScenario scenario)
    {
        return PutManagedObjectAsync(storage, bucketName, key, Encoding.UTF8.GetBytes(payload), "text/plain", scenario);
    }

    private static async Task<StorageResult<ObjectInfo>> PutManagedObjectAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        byte[] payload,
        string contentType,
        ManagedServerSideEncryptionScenario scenario)
    {
        using var content = new MemoryStream(payload, writable: false);

        return await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            Content = content,
            ContentLength = payload.Length,
            ContentType = contentType,
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = ComputeSha256Base64(payload)
            },
            ServerSideEncryption = scenario.RequestEncryption
        });
    }

    private static async Task<StorageResult<ObjectInfo>> PutPlainTextObjectAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        string payload,
        IReadOnlyDictionary<string, string>? metadata = null,
        string? cacheControl = null,
        string? contentDisposition = null)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);
        using var content = new MemoryStream(bytes, writable: false);

        return await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            Content = content,
            ContentLength = bytes.Length,
            ContentType = "text/plain",
            CacheControl = cacheControl,
            ContentDisposition = contentDisposition,
            Metadata = metadata,
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = ComputeSha256Base64(bytes)
            }
        });
    }

    private static async Task<string> ReadRequiredTextObjectAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        LocalS3CompatibleEndpointSettings settings,
        string operation,
        string? versionId = null)
    {
        var getObject = await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        });
        Assert.True(
            getObject.IsSuccess,
            CreateConformanceFailureMessage(settings, operation, getObject.Error?.Message));

        await using var response = getObject.Value!;
        using var reader = new StreamReader(response.Content, Encoding.UTF8);
        return await reader.ReadToEndAsync();
    }

    private static async Task<HttpResponseMessage> SendPresignedPutAsync(
        HttpClient httpClient,
        StorageDirectObjectAccessGrant grant,
        byte[] payload)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, grant.Url)
        {
            Content = new ByteArrayContent(payload)
        };

        foreach (var (name, value) in grant.Headers)
        {
            if (!request.Headers.TryAddWithoutValidation(name, value))
            {
                request.Content.Headers.TryAddWithoutValidation(name, value);
            }
        }

        return await httpClient.SendAsync(request);
    }

    private static async Task<HttpResponseMessage> SendPresignedGetAsync(
        HttpClient httpClient,
        Uri url,
        IReadOnlyDictionary<string, string>? headers = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, url);

        if (headers is not null)
        {
            foreach (var (name, value) in headers)
            {
                request.Headers.TryAddWithoutValidation(name, value);
            }
        }

        return await httpClient.SendAsync(request);
    }

    private static async Task<StorageResult<MultipartUploadPart>> UploadPartAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        byte[] payload,
        string checksumAlgorithm,
        string checksumKey,
        string checksumValue)
    {
        using var content = new MemoryStream(payload, writable: false);
        return await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = uploadId,
            PartNumber = partNumber,
            Content = content,
            ContentLength = payload.Length,
            ChecksumAlgorithm = checksumAlgorithm,
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [checksumKey] = checksumValue
            }
        });
    }

    private static async Task<StorageResult<MultipartUploadPart>> UploadPartAsync(
        S3StorageService storage,
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        byte[] payload)
    {
        using var content = new MemoryStream(payload, writable: false);
        return await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = bucketName,
            Key = key,
            UploadId = uploadId,
            PartNumber = partNumber,
            Content = content,
            ContentLength = payload.Length,
            ChecksumAlgorithm = "SHA256",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = ComputeSha256Base64(payload)
            }
        });
    }

    private static void AssertChecksumValue(
        IReadOnlyDictionary<string, string>? checksums,
        string checksumKey,
        string expectedValue,
        LocalS3CompatibleEndpointSettings settings,
        string operation)
    {
        if (!TryGetChecksumValue(checksums, checksumKey, out var actualValue))
        {
            throw new Xunit.Sdk.XunitException(
                $"{CreateConformanceFailureMessage(settings, operation)} Available checksums: {FormatChecksums(checksums)}");
        }

        Assert.True(
            string.Equals(expectedValue, actualValue, StringComparison.Ordinal),
            $"{CreateConformanceFailureMessage(settings, operation)} Expected '{checksumKey}' checksum '{expectedValue}', but found '{actualValue}'.");
    }

    private static void AssertServerSideEncryptionAlgorithm(
        ObjectServerSideEncryptionInfo? serverSideEncryption,
        ObjectServerSideEncryptionAlgorithm expectedAlgorithm,
        LocalS3CompatibleEndpointSettings settings,
        string operation)
    {
        Assert.True(
            serverSideEncryption is not null,
            $"{CreateConformanceFailureMessage(settings, operation)} Expected server-side encryption '{expectedAlgorithm}', but the endpoint returned none.");
        Assert.Equal(expectedAlgorithm, serverSideEncryption!.Algorithm);
    }

    private static void AssertManagedServerSideEncryption(
        ObjectServerSideEncryptionInfo? serverSideEncryption,
        ManagedServerSideEncryptionScenario scenario,
        LocalS3CompatibleEndpointSettings settings,
        string operation)
    {
        AssertServerSideEncryptionAlgorithm(serverSideEncryption, scenario.Algorithm, settings, operation);

        if (scenario.RequestEncryption.KeyId is not null)
        {
            Assert.False(
                string.IsNullOrWhiteSpace(serverSideEncryption!.KeyId),
                $"{CreateConformanceFailureMessage(settings, operation)} Expected a KMS key identifier to be reported for '{scenario.DisplayName}', but the endpoint returned none.");
        }
    }

    private static void AssertManagedServerSideEncryptionResponseHeader(
        HttpResponseMessage response,
        ManagedServerSideEncryptionScenario scenario,
        LocalS3CompatibleEndpointSettings settings,
        string operation)
    {
        var actualHeaderValue = GetResponseHeaderValue(response, "x-amz-server-side-encryption");
        Assert.True(
            string.Equals(scenario.HeaderValue, actualHeaderValue, StringComparison.Ordinal),
            $"{CreateConformanceFailureMessage(settings, operation)} Expected response header 'x-amz-server-side-encryption' to equal '{scenario.HeaderValue}', but found '{actualHeaderValue ?? "<missing>"}'.");
    }

    private static void AssertBucketDefaultEncryptionRule(
        BucketDefaultEncryptionRule rule,
        ManagedServerSideEncryptionScenario scenario,
        LocalS3CompatibleEndpointSettings settings,
        string operation)
    {
        Assert.Equal(scenario.Algorithm, rule.Algorithm);

        if (scenario.RequestEncryption.KeyId is not null)
        {
            Assert.False(
                string.IsNullOrWhiteSpace(rule.KeyId),
                $"{CreateConformanceFailureMessage(settings, operation)} Expected a KMS key identifier to be reported for bucket default encryption '{scenario.DisplayName}', but the endpoint returned none.");
        }
        else
        {
            Assert.True(
                string.IsNullOrWhiteSpace(rule.KeyId),
                $"{CreateConformanceFailureMessage(settings, operation)} Expected no KMS key identifier for bucket default encryption '{scenario.DisplayName}', but found '{rule.KeyId}'.");
        }
    }

    private static bool TryGetChecksumValue(IReadOnlyDictionary<string, string>? checksums, string checksumKey, out string checksumValue)
    {
        checksumValue = string.Empty;
        if (checksums is null)
            return false;

        if (checksums.TryGetValue(checksumKey, out var directValue) && !string.IsNullOrWhiteSpace(directValue))
        {
            checksumValue = directValue;
            return true;
        }

        foreach (var (candidateKey, candidateValue) in checksums)
        {
            if (string.Equals(candidateKey, checksumKey, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrWhiteSpace(candidateValue))
            {
                checksumValue = candidateValue;
                return true;
            }
        }

        return false;
    }

    private static string FormatChecksums(IReadOnlyDictionary<string, string>? checksums)
    {
        return checksums is null || checksums.Count == 0
            ? "<none>"
            : string.Join(", ", checksums.OrderBy(static entry => entry.Key, StringComparer.OrdinalIgnoreCase).Select(static entry => $"{entry.Key}={entry.Value}"));
    }

    private static void AssertMetadataValue(
        IReadOnlyDictionary<string, string>? metadata,
        string metadataKey,
        string expectedValue,
        LocalS3CompatibleEndpointSettings settings,
        string operation)
    {
        if (metadata is null || !metadata.TryGetValue(metadataKey, out var actualValue))
        {
            throw new Xunit.Sdk.XunitException(
                $"{CreateConformanceFailureMessage(settings, operation)} Available metadata: {FormatMetadata(metadata)}");
        }

        Assert.True(
            string.Equals(expectedValue, actualValue, StringComparison.Ordinal),
            $"{CreateConformanceFailureMessage(settings, operation)} Expected metadata '{metadataKey}' to equal '{expectedValue}', but found '{actualValue}'.");
    }

    private static void AssertMetadataMissing(
        IReadOnlyDictionary<string, string>? metadata,
        string metadataKey,
        LocalS3CompatibleEndpointSettings settings,
        string operation)
    {
        Assert.True(
            metadata is null || !metadata.ContainsKey(metadataKey),
            $"{CreateConformanceFailureMessage(settings, operation)} Expected metadata '{metadataKey}' to be absent, but found: {FormatMetadata(metadata)}");
    }

    private static string FormatMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        return metadata is null || metadata.Count == 0
            ? "<none>"
            : string.Join(", ", metadata.OrderBy(static entry => entry.Key, StringComparer.OrdinalIgnoreCase).Select(static entry => $"{entry.Key}={entry.Value}"));
    }

    private static string CreateConformanceFailureMessage(
        LocalS3CompatibleEndpointSettings settings,
        string operation,
        string? detail = null)
    {
        return string.IsNullOrWhiteSpace(detail)
            ? $"S3-compatible endpoint '{settings.ServiceUrl}' did not satisfy {operation} conformance."
            : $"S3-compatible endpoint '{settings.ServiceUrl}' did not satisfy {operation} conformance: {detail}";
    }

    private static async Task BestEffortDeleteBucketAsync(S3StorageService storage, string bucketName)
    {
        try
        {
            var versions = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
            {
                BucketName = bucketName
            }).ToListAsync();

            foreach (var entry in versions.Where(static value => value.VersionId is not null))
            {
                try
                {
                    await storage.DeleteObjectAsync(new DeleteObjectRequest
                    {
                        BucketName = bucketName,
                        Key = entry.Key,
                        VersionId = entry.VersionId
                    });
                }
                catch
                {
                }
            }

            try
            {
                await storage.DeleteBucketAsync(new DeleteBucketRequest
                {
                    BucketName = bucketName
                });
            }
            catch
            {
            }
        }
        catch
        {
        }
    }

    private static HttpClient CreatePresignedHttpClient(Uri serviceUri)
    {
        if (!string.Equals(serviceUri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)
            || !serviceUri.IsLoopback)
        {
            return new HttpClient();
        }

        return new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
        });
    }

    private static IReadOnlyList<ManagedServerSideEncryptionScenario> GetManagedServerSideEncryptionScenarios(LocalS3CompatibleEndpointSettings settings)
    {
        var scenarios = new List<ManagedServerSideEncryptionScenario>
        {
            CreateManagedServerSideEncryptionScenario(ObjectServerSideEncryptionAlgorithm.Aes256)
        };

        if (settings.ManagedKmsKeyId is not null)
        {
            scenarios.Add(CreateManagedServerSideEncryptionScenario(ObjectServerSideEncryptionAlgorithm.Kms, settings.ManagedKmsKeyId));
        }

        if (settings.ManagedKmsDsseKeyId is not null)
        {
            scenarios.Add(CreateManagedServerSideEncryptionScenario(ObjectServerSideEncryptionAlgorithm.KmsDsse, settings.ManagedKmsDsseKeyId));
        }

        return scenarios;
    }

    private static ManagedServerSideEncryptionScenario CreateManagedServerSideEncryptionScenario(
        ObjectServerSideEncryptionAlgorithm algorithm,
        string? keyId = null)
    {
        return algorithm switch
        {
            ObjectServerSideEncryptionAlgorithm.Aes256 => new ManagedServerSideEncryptionScenario(
                "AES256",
                "aes256",
                "AES256",
                CreateAes256Settings()),
            ObjectServerSideEncryptionAlgorithm.Kms => new ManagedServerSideEncryptionScenario(
                "aws:kms",
                "kms",
                "aws:kms",
                CreateKmsSettings(keyId)),
            ObjectServerSideEncryptionAlgorithm.KmsDsse => new ManagedServerSideEncryptionScenario(
                "aws:kms:dsse",
                "kms-dsse",
                "aws:kms:dsse",
                CreateKmsDsseSettings(keyId)),
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Managed server-side encryption scenario is not supported.")
        };
    }

    private static ObjectServerSideEncryptionSettings CreateAes256Settings() => new()
    {
        Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
    };

    private static ObjectServerSideEncryptionSettings CreateKmsSettings(string? keyId) => new()
    {
        Algorithm = ObjectServerSideEncryptionAlgorithm.Kms,
        KeyId = keyId
    };

    private static ObjectServerSideEncryptionSettings CreateKmsDsseSettings(string? keyId) => new()
    {
        Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
        KeyId = keyId
    };

    private static ObjectCustomerEncryptionSettings CreateSseCCustomerEncryptionSettings()
    {
        var keyBytes = new byte[32];
        RandomNumberGenerator.Fill(keyBytes);
        return new ObjectCustomerEncryptionSettings
        {
            Algorithm = "AES256",
            Key = Convert.ToBase64String(keyBytes),
            KeyMd5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(keyBytes))
        };
    }

    private static string? GetResponseHeaderValue(HttpResponseMessage response, string headerName)
    {
        if (response.Headers.TryGetValues(headerName, out var headerValues))
            return headerValues.LastOrDefault();

        if (response.Content is not null
            && response.Content.Headers.TryGetValues(headerName, out var contentHeaderValues))
        {
            return contentHeaderValues.LastOrDefault();
        }

        return null;
    }

    private static string? NormalizeOptionalEnvironmentValue(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static byte[] CreateFilledBuffer(int length, byte value)
    {
        var buffer = new byte[length];
        Array.Fill(buffer, value);
        return buffer;
    }

    private static string ComputeSha256Base64(byte[] buffer)
    {
        return Convert.ToBase64String(SHA256.HashData(buffer));
    }

    private static string ComputeMultipartSha1Base64(params string[] partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        foreach (var partChecksum in partChecksums)
        {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Length}";
    }

    private static string ComputeMultipartSha256Base64(params string[] partChecksums)
    {
        using var checksum = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var partChecksum in partChecksums)
        {
            checksum.AppendData(Convert.FromBase64String(partChecksum));
        }

        return $"{Convert.ToBase64String(checksum.GetHashAndReset())}-{partChecksums.Length}";
    }

    private sealed record ManagedServerSideEncryptionScenario(
        string DisplayName,
        string BucketSuffix,
        string HeaderValue,
        ObjectServerSideEncryptionSettings RequestEncryption)
    {
        public ObjectServerSideEncryptionAlgorithm Algorithm => RequestEncryption.Algorithm;

        public BucketDefaultEncryptionRule BucketDefaultRule => new()
        {
            Algorithm = RequestEncryption.Algorithm,
            KeyId = RequestEncryption.KeyId
        };
    }

    private sealed record LocalS3CompatibleEndpointSettings(
        string ServiceUrl,
        string Region,
        bool ForcePathStyle,
        string AccessKey,
        string SecretKey,
        string? ManagedKmsKeyId,
        string? ManagedKmsDsseKeyId)
    {
        public Uri ServiceUri { get; } = new(ServiceUrl, UriKind.Absolute);

        public static LocalS3CompatibleEndpointSettings? TryLoad()
        {
            var serviceUrl = Environment.GetEnvironmentVariable("INTEGRATEDS3_S3COMPAT_SERVICE_URL");
            var accessKey = Environment.GetEnvironmentVariable("INTEGRATEDS3_S3COMPAT_ACCESS_KEY");
            var secretKey = Environment.GetEnvironmentVariable("INTEGRATEDS3_S3COMPAT_SECRET_KEY");

            if (string.IsNullOrWhiteSpace(serviceUrl)
                || string.IsNullOrWhiteSpace(accessKey)
                || string.IsNullOrWhiteSpace(secretKey))
            {
                return null;
            }

            var region = Environment.GetEnvironmentVariable("INTEGRATEDS3_S3COMPAT_REGION");
            var forcePathStyle = Environment.GetEnvironmentVariable("INTEGRATEDS3_S3COMPAT_FORCE_PATH_STYLE");
            var managedKmsKeyId = NormalizeOptionalEnvironmentValue(Environment.GetEnvironmentVariable("INTEGRATEDS3_S3COMPAT_MANAGED_SSE_KMS_KEY_ID"));
            var managedKmsDsseKeyId = NormalizeOptionalEnvironmentValue(Environment.GetEnvironmentVariable("INTEGRATEDS3_S3COMPAT_MANAGED_SSE_KMS_DSSE_KEY_ID"));

            return new LocalS3CompatibleEndpointSettings(
                serviceUrl.Trim(),
                string.IsNullOrWhiteSpace(region) ? "us-east-1" : region.Trim(),
                !bool.TryParse(forcePathStyle, out var parsedForcePathStyle) || parsedForcePathStyle,
                accessKey.Trim(),
                secretKey.Trim(),
                managedKmsKeyId,
                managedKmsDsseKeyId);
        }

        public S3StorageOptions CreateOptions() => new()
        {
            ProviderName = "s3-compatible-test",
            Region = Region,
            ServiceUrl = ServiceUrl,
            ForcePathStyle = ForcePathStyle,
            AccessKey = AccessKey,
            SecretKey = SecretKey
        };
    }
}
