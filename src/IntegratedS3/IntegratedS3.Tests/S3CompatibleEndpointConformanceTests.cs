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
