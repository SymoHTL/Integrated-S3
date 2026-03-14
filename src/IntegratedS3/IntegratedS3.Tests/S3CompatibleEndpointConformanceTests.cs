using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
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

    private static string CreateConformanceFailureMessage(
        LocalS3CompatibleEndpointSettings settings,
        string operation,
        string? detail = null)
    {
        return string.IsNullOrWhiteSpace(detail)
            ? $"S3-compatible endpoint '{settings.ServiceUrl}' did not satisfy {operation} checksum conformance."
            : $"S3-compatible endpoint '{settings.ServiceUrl}' did not satisfy {operation} checksum conformance: {detail}";
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

    private static ObjectServerSideEncryptionSettings CreateAes256Settings() => new()
    {
        Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
    };

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

    private sealed record LocalS3CompatibleEndpointSettings(
        string ServiceUrl,
        string Region,
        bool ForcePathStyle,
        string AccessKey,
        string SecretKey)
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

            return new LocalS3CompatibleEndpointSettings(
                serviceUrl.Trim(),
                string.IsNullOrWhiteSpace(region) ? "us-east-1" : region.Trim(),
                !bool.TryParse(forcePathStyle, out var parsedForcePathStyle) || parsedForcePathStyle,
                accessKey.Trim(),
                secretKey.Trim());
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
