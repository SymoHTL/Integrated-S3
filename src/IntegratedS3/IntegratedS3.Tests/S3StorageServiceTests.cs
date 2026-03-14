using System.Net;
using Amazon.Runtime;
using Amazon.S3;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Provider.S3;
using IntegratedS3.Provider.S3.DependencyInjection;
using IntegratedS3.Provider.S3.Internal;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class S3StorageServiceTests
{
    private static S3StorageService BuildService(IS3StorageClient client, S3StorageOptions? options = null)
    {
        options ??= new S3StorageOptions { ProviderName = "s3-test", Region = "us-east-1" };
        return new S3StorageService(options, client);
    }

    // --- Capabilities ---

    [Fact]
    public async Task GetCapabilities_ReportsBucketOpsNative_AndObjectOpsNative()
    {
        var svc = BuildService(new FakeS3Client());
        var caps = await svc.GetCapabilitiesAsync();

        Assert.Equal(StorageCapabilitySupport.Native, caps.BucketOperations);
        Assert.Equal(StorageCapabilitySupport.Native, caps.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Native, caps.ListObjects);
        Assert.Equal(StorageCapabilitySupport.Native, caps.Pagination);
        Assert.Equal(StorageCapabilitySupport.Native, caps.RangeRequests);
        Assert.Equal(StorageCapabilitySupport.Native, caps.ConditionalRequests);
        Assert.Equal(StorageCapabilitySupport.Native, caps.ObjectTags);
        Assert.Equal(StorageCapabilitySupport.Native, caps.Versioning);
        Assert.Equal(StorageCapabilitySupport.Native, caps.Cors);
        Assert.Equal(StorageCapabilitySupport.Native, caps.MultipartUploads);
        Assert.Equal(StorageCapabilitySupport.Native, caps.CopyOperations);
        Assert.Equal(StorageCapabilitySupport.Native, caps.Checksums);
        Assert.Equal(StorageCapabilitySupport.Native, caps.PresignedUrls);
        Assert.Equal(StorageCapabilitySupport.Native, caps.ServerSideEncryption);
        Assert.Contains(
            caps.ServerSideEncryptionDetails.Variants,
            static variant => variant.Algorithm == ObjectServerSideEncryptionAlgorithm.KmsDsse);
    }

    // --- Support state descriptor ---

    [Fact]
    public async Task GetSupportStateDescriptor_ReportsDelegatedOwnershipForNativeAndTrackGConcerns()
    {
        var svc = BuildService(new FakeS3Client());
        var desc = await svc.GetSupportStateDescriptorAsync();

        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.ObjectMetadata);
        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.ObjectTags);
        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.MultipartState);
        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.Versioning);
        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.Checksums);
        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.AccessControl);
        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.Retention);
        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.ServerSideEncryption);
        Assert.Equal(StorageSupportStateOwnership.Delegated, desc.RedirectLocations);
    }

    [Fact]
    public async Task ProviderMode_AndObjectLocationDescriptor_ReportDelegatedProviderPresigns()
    {
        var svc = BuildService(new FakeS3Client());

        var providerMode = await svc.GetProviderModeAsync();
        var objectLocation = await svc.GetObjectLocationDescriptorAsync();

        Assert.Equal(StorageProviderMode.Delegated, providerMode);
        Assert.Equal(StorageObjectAccessMode.Delegated, objectLocation.DefaultAccessMode);
        Assert.Equal([StorageObjectAccessMode.Delegated, StorageObjectAccessMode.ProxyStream], objectLocation.SupportedAccessModes);
    }

    // --- ListBucketsAsync ---

    [Fact]
    public async Task ListBucketsAsync_YieldsBucketInfoFromClient()
    {
        var created = new DateTimeOffset(2025, 1, 15, 12, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client();
        fake.Buckets.Add(new S3BucketEntry("my-bucket", created));

        var svc = BuildService(fake);
        var buckets = await svc.ListBucketsAsync().ToListAsync();

        Assert.Single(buckets);
        Assert.Equal("my-bucket", buckets[0].Name);
        Assert.Equal(created, buckets[0].CreatedAtUtc);
    }

    // --- CreateBucketAsync ---

    [Fact]
    public async Task CreateBucketAsync_RejectsVersioningEnabled_WithUnsupportedCapability()
    {
        var svc = BuildService(new FakeS3Client());

        var result = await svc.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "my-bucket",
            EnableVersioning = true
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, result.Error!.Code);
    }

    [Fact]
    public async Task CreateBucketAsync_TranslatesDuplicateBucketException_ToBucketAlreadyExists()
    {
        var fake = new FakeS3Client();
        fake.CreateBucketException = new AmazonS3Exception(
            "Bucket already exists.", ErrorType.Sender, "BucketAlreadyExists", "req-1", HttpStatusCode.Conflict);

        var svc = BuildService(fake);
        var result = await svc.CreateBucketAsync(new CreateBucketRequest { BucketName = "my-bucket" });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.BucketAlreadyExists, result.Error!.Code);
    }

    // --- HeadBucketAsync ---

    [Fact]
    public async Task HeadBucketAsync_ReturnsBucketNotFound_WhenClientReturnsNull()
    {
        var fake = new FakeS3Client { HeadBucketReturnsNull = true };

        var svc = BuildService(fake);
        var result = await svc.HeadBucketAsync("missing-bucket");

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.BucketNotFound, result.Error!.Code);
    }

    // --- DeleteBucketAsync ---

    [Fact]
    public async Task DeleteBucketAsync_TranslatesBucketNotEmptyException_ToBucketNotEmpty()
    {
        var fake = new FakeS3Client();
        fake.DeleteBucketException = new AmazonS3Exception(
            "Bucket is not empty.", ErrorType.Sender, "BucketNotEmpty", "req-2", HttpStatusCode.Conflict);

        var svc = BuildService(fake);
        var result = await svc.DeleteBucketAsync(new DeleteBucketRequest { BucketName = "my-bucket" });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.BucketNotEmpty, result.Error!.Code);
        Assert.Equal(409, result.Error.SuggestedHttpStatusCode);
    }

    // --- Bucket versioning ---

    [Fact]
    public async Task GetBucketVersioningAsync_ReturnsDisabled_WhenClientReturnsDisabled()
    {
        var fake = new FakeS3Client { VersioningStatus = BucketVersioningStatus.Disabled };
        var svc = BuildService(fake);

        var result = await svc.GetBucketVersioningAsync("my-bucket");

        Assert.True(result.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Disabled, result.Value!.Status);
        Assert.Equal("my-bucket", result.Value.BucketName);
    }

    [Fact]
    public async Task GetBucketVersioningAsync_ReturnsEnabled_WhenClientReturnsEnabled()
    {
        var fake = new FakeS3Client { VersioningStatus = BucketVersioningStatus.Enabled };
        var svc = BuildService(fake);

        var result = await svc.GetBucketVersioningAsync("my-bucket");

        Assert.True(result.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Enabled, result.Value!.Status);
    }

    [Fact]
    public async Task PutBucketVersioningAsync_Succeeds_AndReturnsRequestedStatus()
    {
        var fake = new FakeS3Client();
        var svc = BuildService(fake);

        var result = await svc.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "my-bucket",
            Status = BucketVersioningStatus.Enabled
        });

        Assert.True(result.IsSuccess);
        Assert.Equal(BucketVersioningStatus.Enabled, result.Value!.Status);
        Assert.Equal("my-bucket", result.Value.BucketName);
        Assert.Equal(BucketVersioningStatus.Enabled, fake.SetVersioningStatus);
    }

    [Fact]
    public async Task GetBucketLocationAsync_ReturnsLocationConstraint_WhenClientReturnsLocation()
    {
        var fake = new FakeS3Client
        {
            BucketLocation = new S3BucketLocationEntry("eu-central-1")
        };
        var svc = BuildService(fake);

        var result = await svc.GetBucketLocationAsync("my-bucket");

        Assert.True(result.IsSuccess);
        Assert.Equal("my-bucket", result.Value!.BucketName);
        Assert.Equal("eu-central-1", result.Value.LocationConstraint);
    }

    [Fact]
    public async Task GetBucketCorsAsync_ReturnsConfiguredRules()
    {
        var fake = new FakeS3Client
        {
            BucketCorsConfiguration = new S3CorsConfigurationEntry(
            [
                new S3CorsRuleEntry(
                    Id: "rule-1",
                    AllowedOrigins: ["https://app.example"],
                    AllowedMethods: [BucketCorsMethod.Get, BucketCorsMethod.Put],
                    AllowedHeaders: ["authorization", "x-amz-*"],
                    ExposeHeaders: ["etag"],
                    MaxAgeSeconds: 600)
            ])
        };
        var svc = BuildService(fake);

        var result = await svc.GetBucketCorsAsync("my-bucket");

        Assert.True(result.IsSuccess);
        Assert.Equal("my-bucket", result.Value!.BucketName);
        var rule = Assert.Single(result.Value.Rules);
        Assert.Equal("rule-1", rule.Id);
        Assert.Equal([BucketCorsMethod.Get, BucketCorsMethod.Put], rule.AllowedMethods);
        Assert.Equal(["https://app.example"], rule.AllowedOrigins);
    }

    [Fact]
    public async Task GetBucketCorsAsync_TranslatesNoSuchCorsConfiguration()
    {
        var fake = new FakeS3Client
        {
            GetBucketCorsException = new AmazonS3Exception(
                "No CORS configuration.", ErrorType.Sender, "NoSuchCORSConfiguration", "req-cors", HttpStatusCode.NotFound)
        };
        var svc = BuildService(fake);

        var result = await svc.GetBucketCorsAsync("my-bucket");

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.CorsConfigurationNotFound, result.Error!.Code);
    }

    [Fact]
    public async Task PutBucketCorsAsync_Succeeds_AndReturnsStoredRules()
    {
        var fake = new FakeS3Client();
        var svc = BuildService(fake);

        var result = await svc.PutBucketCorsAsync(new PutBucketCorsRequest
        {
            BucketName = "my-bucket",
            Rules =
            [
                new BucketCorsRule
                {
                    AllowedOrigins = ["https://app.example"],
                    AllowedMethods = [BucketCorsMethod.Get, BucketCorsMethod.Head],
                    AllowedHeaders = ["authorization"],
                    ExposeHeaders = ["etag"],
                    MaxAgeSeconds = 300
                }
            ]
        });

        Assert.True(result.IsSuccess);
        var storedRule = Assert.Single(result.Value!.Rules);
        Assert.Equal([BucketCorsMethod.Get, BucketCorsMethod.Head], storedRule.AllowedMethods);
        Assert.NotNull(fake.BucketCorsConfiguration);
        Assert.Single(fake.BucketCorsConfiguration!.Rules);
    }

    [Fact]
    public async Task DeleteBucketCorsAsync_ClearsConfiguration()
    {
        var fake = new FakeS3Client
        {
            BucketCorsConfiguration = new S3CorsConfigurationEntry(
            [
                new S3CorsRuleEntry(
                    Id: null,
                    AllowedOrigins: ["*"],
                    AllowedMethods: [BucketCorsMethod.Get],
                    AllowedHeaders: [],
                    ExposeHeaders: [],
                    MaxAgeSeconds: null)
            ])
        };
        var svc = BuildService(fake);

        var result = await svc.DeleteBucketCorsAsync(new DeleteBucketCorsRequest
        {
            BucketName = "my-bucket"
        });

        Assert.True(result.IsSuccess);
        Assert.True(fake.DeleteBucketCorsCalled);
        Assert.Null(fake.BucketCorsConfiguration);
    }

    // --- ListObjectsAsync ---

    [Fact]
    public async Task ListObjectsAsync_YieldsObjectInfoFromClient()
    {
        var lastModified = new DateTimeOffset(2025, 3, 1, 0, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client();
        fake.ObjectPages.Add(new S3ObjectListPage(
        [
            new S3ObjectEntry("key1", 100, null, "\"etag1\"", lastModified, null, null),
            new S3ObjectEntry("key2", 200, null, "\"etag2\"", lastModified, null, null)
        ], null));

        var svc = BuildService(fake);
        var objects = await svc.ListObjectsAsync(new ListObjectsRequest { BucketName = "my-bucket" }).ToListAsync();

        Assert.Equal(2, objects.Count);
        Assert.Equal("key1", objects[0].Key);
        Assert.Equal(100, objects[0].ContentLength);
        Assert.Equal("\"etag1\"", objects[0].ETag);
        Assert.Equal("key2", objects[1].Key);
    }

    [Fact]
    public async Task ListObjectsAsync_StopsAfterPageSize()
    {
        var lastModified = DateTimeOffset.UtcNow;
        var fake = new FakeS3Client();
        fake.ObjectPages.Add(new S3ObjectListPage(
        [
            new S3ObjectEntry("key1", 1, null, null, lastModified, null, null),
            new S3ObjectEntry("key2", 2, null, null, lastModified, null, null),
            new S3ObjectEntry("key3", 3, null, null, lastModified, null, null)
        ], null));

        var svc = BuildService(fake);
        var objects = await svc.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "my-bucket",
            PageSize = 2
        }).ToListAsync();

        Assert.Equal(2, objects.Count);
    }

    [Fact]
    public async Task ListObjectsAsync_FollowsContinuationAcrossClientPages_WhenPageSizeNeedsMoreThanOnePage()
    {
        var lastModified = DateTimeOffset.UtcNow;
        var fake = new FakeS3Client();
        fake.ObjectPages.Add(new S3ObjectListPage(
        [
            new S3ObjectEntry("key1", 1, null, null, lastModified, null, null),
            new S3ObjectEntry("key2", 2, null, null, lastModified, null, null)
        ], "page-2"));
        fake.ObjectPages.Add(new S3ObjectListPage(
        [
            new S3ObjectEntry("key3", 3, null, null, lastModified, null, null),
            new S3ObjectEntry("key4", 4, null, null, lastModified, null, null)
        ], null));

        var svc = BuildService(fake);
        var objects = await svc.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = "my-bucket",
            PageSize = 3
        }).ToListAsync();

        Assert.Equal(["key1", "key2", "key3"], objects.Select(static item => item.Key).ToArray());
        Assert.Equal(2, fake.ObjectListCalls);
    }

    // --- ListObjectVersionsAsync ---

    [Fact]
    public async Task ListObjectVersionsAsync_YieldsVersionsAndDeleteMarkers()
    {
        var lastModified = new DateTimeOffset(2025, 4, 1, 0, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client();
        fake.VersionPages.Add(new S3ObjectVersionListPage(
        [
            new S3ObjectEntry("key1", 100, null, "\"etag-v2\"", lastModified, null, "v2", IsLatest: true, IsDeleteMarker: false),
            new S3ObjectEntry("key1", 0, null, null, lastModified.AddHours(-1), null, "v1", IsLatest: false, IsDeleteMarker: false),
            new S3ObjectEntry("key2", 0, null, null, lastModified, null, "dm1", IsLatest: true, IsDeleteMarker: true)
        ], null, null));

        var svc = BuildService(fake);
        var versions = await svc.ListObjectVersionsAsync(new ListObjectVersionsRequest { BucketName = "my-bucket" }).ToListAsync();

        Assert.Equal(3, versions.Count);
        Assert.False(versions[0].IsDeleteMarker);
        Assert.True(versions[0].IsLatest);
        Assert.Equal("v2", versions[0].VersionId);
        Assert.True(versions[2].IsDeleteMarker);
        Assert.Equal("dm1", versions[2].VersionId);
    }

    [Fact]
    public async Task ListObjectVersionsAsync_FollowsMarkersAcrossClientPages_WhenPageSizeNeedsMoreThanOnePage()
    {
        var lastModified = new DateTimeOffset(2025, 4, 1, 0, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client();
        fake.VersionPages.Add(new S3ObjectVersionListPage(
        [
            new S3ObjectEntry("key1", 100, null, "\"etag-v1\"", lastModified, null, "v1", IsLatest: false, IsDeleteMarker: false),
            new S3ObjectEntry("key1", 110, null, "\"etag-v2\"", lastModified.AddMinutes(1), null, "v2", IsLatest: true, IsDeleteMarker: false)
        ], "key1", "v2"));
        fake.VersionPages.Add(new S3ObjectVersionListPage(
        [
            new S3ObjectEntry("key2", 120, null, "\"etag-v3\"", lastModified.AddMinutes(2), null, "v3", IsLatest: true, IsDeleteMarker: false),
            new S3ObjectEntry("key3", 0, null, null, lastModified.AddMinutes(3), null, "dm1", IsLatest: true, IsDeleteMarker: true)
        ], null, null));

        var svc = BuildService(fake);
        var versions = await svc.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "my-bucket",
            PageSize = 3
        }).ToListAsync();

        Assert.Equal(["v1", "v2", "v3"], versions.Select(static item => item.VersionId!).ToArray());
        Assert.Equal(2, fake.VersionListCalls);
    }

    // --- HeadObjectAsync ---

    [Fact]
    public async Task HeadObjectAsync_ReturnsObjectNotFound_WhenClientReturnsNull()
    {
        var fake = new FakeS3Client { HeadObjectReturnsNull = true };
        var svc = BuildService(fake);

        var result = await svc.HeadObjectAsync(new HeadObjectRequest { BucketName = "b", Key = "k" });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, result.Error!.Code);
    }

    [Fact]
    public async Task HeadObjectAsync_ReturnsObjectInfo_WhenObjectExists()
    {
        var lastModified = new DateTimeOffset(2025, 5, 1, 0, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client();
        fake.HeadObjectResult = new S3ObjectEntry(
            "k",
            512,
            "text/plain",
            "\"abc\"",
            lastModified,
            null,
            "v1",
            ServerSideEncryption: new ObjectServerSideEncryptionInfo
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
                KeyId = "kms-key-1"
            });
        var svc = BuildService(fake);

        var result = await svc.HeadObjectAsync(new HeadObjectRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        Assert.Equal("k", result.Value!.Key);
        Assert.Equal(512, result.Value.ContentLength);
        Assert.Equal("text/plain", result.Value.ContentType);
        Assert.Equal("\"abc\"", result.Value.ETag);
        Assert.Equal("v1", result.Value.VersionId);
        Assert.NotNull(result.Value.ServerSideEncryption);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.KmsDsse, result.Value.ServerSideEncryption!.Algorithm);
        Assert.Equal("kms-key-1", result.Value.ServerSideEncryption.KeyId);
    }

    [Fact]
    public async Task HeadObjectAsync_RejectsReadTimeServerSideEncryptionSettings()
    {
        var svc = BuildService(new FakeS3Client());

        var result = await svc.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "b",
            Key = "k",
            ServerSideEncryption = new ObjectServerSideEncryptionSettings
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
            }
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, result.Error!.Code);
    }

    // --- GetObjectAsync ---

    [Fact]
    public async Task GetObjectAsync_ReturnsContent_WhenObjectExists()
    {
        var content = new MemoryStream([1, 2, 3]);
        var lastModified = new DateTimeOffset(2025, 6, 1, 0, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client();
        fake.GetObjectResult = new S3GetObjectResult(
            new S3ObjectEntry(
                "k",
                3,
                "application/octet-stream",
                "\"etag\"",
                lastModified,
                null,
                null,
                ServerSideEncryption: new ObjectServerSideEncryptionInfo
                {
                    Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
                }),
            content,
            3);

        var svc = BuildService(fake);
        var result = await svc.GetObjectAsync(new GetObjectRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        await using var response = result.Value!;
        Assert.Equal("k", response.Object.Key);
        Assert.Equal(3, response.TotalContentLength);
        Assert.False(response.IsNotModified);
        Assert.NotNull(response.Object.ServerSideEncryption);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Aes256, response.Object.ServerSideEncryption!.Algorithm);
    }

    [Fact]
    public async Task GetObjectAsync_Returns304NotModified_OnNotModifiedS3Exception()
    {
        var lastModified = new DateTimeOffset(2025, 7, 1, 0, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client();
        fake.GetObjectException = new AmazonS3Exception("Not Modified", ErrorType.Receiver, "NotModified", "req", HttpStatusCode.NotModified);
        fake.HeadObjectResult = new S3ObjectEntry("k", 512, "text/plain", "\"etag\"", lastModified, null, null);

        var svc = BuildService(fake);
        var result = await svc.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "b",
            Key = "k",
            IfNoneMatchETag = "\"etag\""
        });

        Assert.True(result.IsSuccess);
        await using var response = result.Value!;
        Assert.True(response.IsNotModified);
        Assert.Equal(512, response.TotalContentLength);
    }

    [Fact]
    public async Task GetObjectAsync_ReturnsPreconditionFailed_On412Exception()
    {
        var fake = new FakeS3Client();
        fake.GetObjectException = new AmazonS3Exception("Precondition Failed", ErrorType.Sender, "PreconditionFailed", "req", HttpStatusCode.PreconditionFailed);

        var svc = BuildService(fake);
        var result = await svc.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "b",
            Key = "k",
            IfMatchETag = "\"wrong-etag\""
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.PreconditionFailed, result.Error!.Code);
    }

    [Fact]
    public async Task GetObjectAsync_DisposingResponse_DisposesUnderlyingS3ResultWrapper()
    {
        var owner = new TrackingDisposable();
        var fake = new FakeS3Client
        {
            GetObjectResult = new S3GetObjectResult(
                new S3ObjectEntry("k", 3, "application/octet-stream", "\"etag\"", DateTimeOffset.UtcNow, null, null),
                new MemoryStream([1, 2, 3]),
                3,
                owner)
        };

        var svc = BuildService(fake);
        var result = await svc.GetObjectAsync(new GetObjectRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        await using (result.Value!)
        {
        }

        Assert.True(owner.IsDisposed);
    }

    // --- PutObjectAsync ---

    [Fact]
    public async Task PutObjectAsync_ReturnsObjectInfo_OnSuccess()
    {
        var serverSideEncryption = new ObjectServerSideEncryptionSettings
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
            KeyId = "kms-key-1",
            Context = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant"] = "alpha"
            }
        };
        var fake = new FakeS3Client();
        fake.PutObjectResult = new S3ObjectEntry(
            "k",
            10,
            "text/plain",
            "\"new-etag\"",
            DateTimeOffset.UtcNow,
            null,
            "v1",
            Checksums: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = "put-checksum"
            },
            ServerSideEncryption: new ObjectServerSideEncryptionInfo
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
                KeyId = "kms-key-1"
            });

        var svc = BuildService(fake);
        var result = await svc.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "b",
            Key = "k",
            Content = new MemoryStream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            ContentType = "text/plain",
            ServerSideEncryption = serverSideEncryption
        });

        Assert.True(result.IsSuccess);
        Assert.Equal("k", result.Value!.Key);
        Assert.Equal("\"new-etag\"", result.Value.ETag);
        Assert.Equal("v1", result.Value.VersionId);
        Assert.Equal("put-checksum", result.Value.Checksums!["sha256"]);
        Assert.NotNull(fake.LastPutObjectServerSideEncryption);
        Assert.Same(serverSideEncryption, fake.LastPutObjectServerSideEncryption);
        Assert.NotNull(result.Value.ServerSideEncryption);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.KmsDsse, result.Value.ServerSideEncryption!.Algorithm);
        Assert.Equal("kms-key-1", result.Value.ServerSideEncryption.KeyId);
    }

    [Fact]
    public async Task PutObjectAsync_RejectsAes256WithKmsOnlyFields()
    {
        var svc = BuildService(new FakeS3Client());

        var result = await svc.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "b",
            Key = "k",
            Content = Stream.Null,
            ServerSideEncryption = new ObjectServerSideEncryptionSettings
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256,
                KeyId = "kms-key-1"
            }
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, result.Error!.Code);
    }

    [Fact]
    public async Task PutObjectAsync_RejectsManagedKmsContextWithEmptyValue()
    {
        var fake = new FakeS3Client();
        var svc = BuildService(fake);

        var result = await svc.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "b",
            Key = "k",
            Content = Stream.Null,
            ServerSideEncryption = new ObjectServerSideEncryptionSettings
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
                Context = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["tenant"] = string.Empty
                }
            }
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, result.Error!.Code);
        Assert.Contains("non-empty strings", result.Error.Message, StringComparison.Ordinal);
        Assert.Null(fake.LastPutObjectServerSideEncryption);
    }

    [Fact]
    public async Task PutObjectAsync_TranslatesException_ToBucketNotFound()
    {
        var fake = new FakeS3Client();
        fake.PutObjectException = new AmazonS3Exception("No such bucket", ErrorType.Sender, "NoSuchBucket", "req", HttpStatusCode.NotFound);

        var svc = BuildService(fake);
        var result = await svc.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "missing",
            Key = "k",
            Content = Stream.Null
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.BucketNotFound, result.Error!.Code);
    }

    // --- DeleteObjectAsync ---

    [Fact]
    public async Task DeleteObjectAsync_ReturnsSuccess_WhenObjectDeleted()
    {
        var fake = new FakeS3Client();
        fake.DeleteObjectResult = new S3DeleteObjectResult("k", "v1", false);

        var svc = BuildService(fake);
        var result = await svc.DeleteObjectAsync(new DeleteObjectRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        Assert.Equal("k", result.Value!.Key);
        Assert.Equal("v1", result.Value.VersionId);
        Assert.False(result.Value.IsDeleteMarker);
    }

    [Fact]
    public async Task DeleteObjectAsync_ReturnsDeleteMarker_WhenVersioningEnabled()
    {
        var fake = new FakeS3Client();
        fake.DeleteObjectResult = new S3DeleteObjectResult("k", "dm1", true);

        var svc = BuildService(fake);
        var result = await svc.DeleteObjectAsync(new DeleteObjectRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        Assert.True(result.Value!.IsDeleteMarker);
        Assert.Equal("dm1", result.Value.VersionId);
    }

    // --- Object tags ---

    [Fact]
    public async Task GetObjectTagsAsync_ReturnsTags_FromClient()
    {
        var fake = new FakeS3Client();
        fake.ObjectTags["k"] = new Dictionary<string, string>(StringComparer.Ordinal) { ["env"] = "prod" };

        var svc = BuildService(fake);
        var result = await svc.GetObjectTagsAsync(new GetObjectTagsRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        Assert.Equal("prod", result.Value!.Tags["env"]);
    }

    [Fact]
    public async Task PutObjectTagsAsync_StoresTags_AndReturnsThem()
    {
        var fake = new FakeS3Client();
        var svc = BuildService(fake);

        var result = await svc.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "b",
            Key = "k",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["team"] = "storage" }
        });

        Assert.True(result.IsSuccess);
        Assert.Equal("storage", result.Value!.Tags["team"]);
        Assert.True(fake.ObjectTags.ContainsKey("k"));
    }

    [Fact]
    public async Task DeleteObjectTagsAsync_ClearsTags_AndReturnsEmptySet()
    {
        var fake = new FakeS3Client();
        fake.ObjectTags["k"] = new Dictionary<string, string>(StringComparer.Ordinal) { ["env"] = "prod" };
        var svc = BuildService(fake);

        var result = await svc.DeleteObjectTagsAsync(new DeleteObjectTagsRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        Assert.Empty(result.Value!.Tags);
        Assert.False(fake.ObjectTags.ContainsKey("k"));
    }

    // --- Copy + multipart ---

    [Fact]
    public async Task CopyObjectAsync_ReturnsCopiedObjectInfo_WithChecksums()
    {
        var lastModified = new DateTimeOffset(2025, 8, 1, 0, 0, 0, TimeSpan.Zero);
        var checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sha256"] = "copied-checksum"
        };
        var destinationServerSideEncryption = new ObjectServerSideEncryptionSettings
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
            KeyId = "kms-key-copy",
            Context = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant"] = "alpha"
            }
        };
        var fake = new FakeS3Client
        {
            CopyObjectResult = new S3ObjectEntry(
                "dest-k",
                0,
                null,
                "\"copy-etag\"",
                lastModified,
                null,
                "v2",
                Checksums: checksums,
                ServerSideEncryption: new ObjectServerSideEncryptionInfo
                {
                    Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
                    KeyId = "kms-key-copy"
                }),
            HeadObjectResult = new S3ObjectEntry(
                "dest-k",
                42,
                "text/plain",
                "\"copy-etag\"",
                lastModified,
                null,
                "v2",
                Checksums: checksums,
                ServerSideEncryption: new ObjectServerSideEncryptionInfo
                {
                    Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
                    KeyId = "kms-key-copy"
                })
        };

        var svc = BuildService(fake);
        var result = await svc.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "src-b",
            SourceKey = "src-k",
            DestinationBucketName = "dest-b",
            DestinationKey = "dest-k",
            DestinationServerSideEncryption = destinationServerSideEncryption
        });

        Assert.True(result.IsSuccess);
        Assert.NotNull(fake.LastCopyRequest);
        Assert.Equal("src-k", fake.LastCopyRequest!.SourceKey);
        Assert.Equal("dest-k", fake.LastCopyRequest.DestinationKey);
        Assert.Same(destinationServerSideEncryption, fake.LastCopyRequest.DestinationServerSideEncryption);
        Assert.Equal(42, result.Value!.ContentLength);
        Assert.Equal("v2", result.Value.VersionId);
        Assert.Equal("copied-checksum", result.Value.Checksums!["sha256"]);
        Assert.NotNull(result.Value.ServerSideEncryption);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.KmsDsse, result.Value.ServerSideEncryption!.Algorithm);
        Assert.Equal("kms-key-copy", result.Value.ServerSideEncryption.KeyId);
    }

    [Fact]
    public async Task CopyObjectAsync_MergesSparseHeadMetadataAndChecksums_WithCopyResponseValues()
    {
        var lastModified = new DateTimeOffset(2025, 8, 2, 0, 0, 0, TimeSpan.Zero);
        var copyMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["content-language"] = "en",
            ["cache-control"] = "private, max-age=60"
        };
        var headMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["content-language"] = "de"
        };
        var copyChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sha256"] = "copy-sha256",
            ["crc32c"] = "copy-crc32c"
        };
        var headChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sha256"] = "head-sha256"
        };
        var fake = new FakeS3Client
        {
            CopyObjectResult = new S3ObjectEntry("dest-k", 0, "application/octet-stream", "\"copy-etag\"", lastModified, copyMetadata, "v-copy", Checksums: copyChecksums),
            HeadObjectResult = new S3ObjectEntry("dest-k", 42, null, "\"head-etag\"", lastModified.AddMinutes(1), headMetadata, null, Checksums: headChecksums)
        };

        var svc = BuildService(fake);
        var result = await svc.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "src-b",
            SourceKey = "src-k",
            DestinationBucketName = "dest-b",
            DestinationKey = "dest-k"
        });

        Assert.True(result.IsSuccess);
        Assert.Equal("application/octet-stream", result.Value!.ContentType);
        Assert.Equal("\"head-etag\"", result.Value.ETag);
        Assert.Equal("v-copy", result.Value.VersionId);
        Assert.Equal("de", result.Value.Metadata!["content-language"]);
        Assert.Equal("private, max-age=60", result.Value.Metadata["cache-control"]);
        Assert.Equal("head-sha256", result.Value.Checksums!["sha256"]);
        Assert.Equal("copy-crc32c", result.Value.Checksums["crc32c"]);
    }

    [Fact]
    public async Task CopyObjectAsync_TranslatesSourceObjectNotFound_WhenClientThrows()
    {
        var fake = new FakeS3Client
        {
            CopyObjectException = new AmazonS3Exception("Missing source", ErrorType.Sender, "NoSuchKey", "req-copy", HttpStatusCode.NotFound)
        };

        var svc = BuildService(fake);
        var result = await svc.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "src-b",
            SourceKey = "src-k",
            DestinationBucketName = "dest-b",
            DestinationKey = "dest-k"
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.ObjectNotFound, result.Error!.Code);
        Assert.Equal("src-k", result.Error.ObjectKey);
    }

    [Fact]
    public async Task CopyObjectAsync_RejectsSourceServerSideEncryptionSettings()
    {
        var fake = new FakeS3Client();
        var svc = BuildService(fake);

        var result = await svc.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "src-b",
            SourceKey = "src-k",
            DestinationBucketName = "dest-b",
            DestinationKey = "dest-k",
            SourceServerSideEncryption = new ObjectServerSideEncryptionSettings
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.Kms,
                KeyId = "kms-key-source"
            }
        });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.UnsupportedCapability, result.Error!.Code);
        Assert.Null(fake.LastCopyRequest);
    }

    [Fact]
    public async Task ListMultipartUploadsAsync_FollowsMarkersAcrossClientPages_WhenPageSizeNeedsMoreThanOnePage()
    {
        var fake = new FakeS3Client();
        fake.MultipartUploadPages.Add(new S3MultipartUploadListPage(
        [
            new MultipartUploadInfo { BucketName = "b", Key = "a", UploadId = "u1", InitiatedAtUtc = DateTimeOffset.UtcNow, ChecksumAlgorithm = "sha256" },
            new MultipartUploadInfo { BucketName = "b", Key = "a", UploadId = "u2", InitiatedAtUtc = DateTimeOffset.UtcNow.AddMinutes(1), ChecksumAlgorithm = "sha256" }
        ], "a", "u2"));
        fake.MultipartUploadPages.Add(new S3MultipartUploadListPage(
        [
            new MultipartUploadInfo { BucketName = "b", Key = "b", UploadId = "u3", InitiatedAtUtc = DateTimeOffset.UtcNow.AddMinutes(2), ChecksumAlgorithm = "sha1" },
            new MultipartUploadInfo { BucketName = "b", Key = "c", UploadId = "u4", InitiatedAtUtc = DateTimeOffset.UtcNow.AddMinutes(3), ChecksumAlgorithm = "crc32c" }
        ], null, null));

        var svc = BuildService(fake);
        var uploads = await svc.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "b",
            PageSize = 3
        }).ToListAsync();

        Assert.Equal(["u1", "u2", "u3"], uploads.Select(static upload => upload.UploadId).ToArray());
        Assert.Equal(2, fake.MultipartUploadListCalls);
    }

    [Fact]
    public async Task InitiateMultipartUploadAsync_ReturnsUploadInfo_FromClient()
    {
        var initiatedAtUtc = new DateTimeOffset(2025, 8, 1, 12, 0, 0, TimeSpan.Zero);
        var serverSideEncryption = new ObjectServerSideEncryptionSettings
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
            KeyId = "kms-multipart-key",
            Context = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["scope"] = "multipart"
            }
        };
        var fake = new FakeS3Client
        {
            InitiateMultipartUploadResult = new MultipartUploadInfo
            {
                BucketName = "b",
                Key = "k",
                UploadId = "upload-123",
                InitiatedAtUtc = initiatedAtUtc,
                ChecksumAlgorithm = "sha256"
            }
        };

        var svc = BuildService(fake);
        var result = await svc.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "b",
            Key = "k",
            ChecksumAlgorithm = "SHA256",
            ServerSideEncryption = serverSideEncryption
        });

        Assert.True(result.IsSuccess);
        Assert.Equal("upload-123", result.Value!.UploadId);
        Assert.Equal("sha256", result.Value.ChecksumAlgorithm);
        Assert.NotNull(fake.LastInitiateMultipartUploadServerSideEncryption);
        Assert.Same(serverSideEncryption, fake.LastInitiateMultipartUploadServerSideEncryption);
    }

    [Fact]
    public async Task UploadMultipartPartAsync_ReturnsPartInfo_WithChecksums()
    {
        var partChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sha256"] = "part-checksum"
        };
        var fake = new FakeS3Client
        {
            UploadMultipartPartResult = new MultipartUploadPart
            {
                PartNumber = 3,
                ETag = "\"part-etag\"",
                ContentLength = 5,
                LastModifiedUtc = new DateTimeOffset(2025, 8, 1, 13, 0, 0, TimeSpan.Zero),
                Checksums = partChecksums
            }
        };

        var svc = BuildService(fake);
        var result = await svc.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "b",
            Key = "k",
            UploadId = "upload-123",
            PartNumber = 3,
            Content = new MemoryStream([1, 2, 3, 4, 5]),
            ContentLength = 5,
            ChecksumAlgorithm = "SHA256",
            Checksums = partChecksums
        });

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.Value!.PartNumber);
        Assert.Equal("\"part-etag\"", result.Value.ETag);
        Assert.Equal("part-checksum", result.Value.Checksums!["sha256"]);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_ReturnsCompletedObjectInfo_WithChecksums()
    {
        var checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sha256"] = "complete-checksum"
        };
        var lastModified = new DateTimeOffset(2025, 8, 1, 14, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client
        {
            CompleteMultipartUploadResult = new S3ObjectEntry(
                "k",
                0,
                null,
                "\"complete-etag\"",
                lastModified,
                null,
                "v-complete",
                Checksums: checksums,
                ServerSideEncryption: new ObjectServerSideEncryptionInfo
                {
                    Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
                }),
            HeadObjectResult = new S3ObjectEntry(
                "k",
                10,
                "application/octet-stream",
                "\"complete-etag\"",
                lastModified,
                null,
                "v-complete",
                Checksums: checksums,
                ServerSideEncryption: new ObjectServerSideEncryptionInfo
                {
                    Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
                })
        };

        var svc = BuildService(fake);
        var result = await svc.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "b",
            Key = "k",
            UploadId = "upload-123",
            Parts =
            [
                new MultipartUploadPart
                {
                    PartNumber = 1,
                    ETag = "\"part-etag\"",
                    ContentLength = 5,
                    LastModifiedUtc = lastModified
                }
            ]
        });

        Assert.True(result.IsSuccess);
        Assert.Equal("v-complete", result.Value!.VersionId);
        Assert.Equal("complete-checksum", result.Value.Checksums!["sha256"]);
        Assert.Equal(10, result.Value.ContentLength);
        Assert.NotNull(result.Value.ServerSideEncryption);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Aes256, result.Value.ServerSideEncryption!.Algorithm);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_MergesSparseHeadChecksums_WithCompletionResponseValues()
    {
        var completionChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sha256"] = "completion-sha256",
            ["crc32c"] = "completion-crc32c"
        };
        var headChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sha256"] = "head-sha256"
        };
        var lastModified = new DateTimeOffset(2025, 8, 1, 14, 30, 0, TimeSpan.Zero);
        var fake = new FakeS3Client
        {
            CompleteMultipartUploadResult = new S3ObjectEntry("k", 0, null, "\"complete-etag\"", lastModified, null, "v-complete", Checksums: completionChecksums),
            HeadObjectResult = new S3ObjectEntry("k", 10, "application/octet-stream", "\"complete-etag\"", lastModified.AddMinutes(1), null, null, Checksums: headChecksums)
        };

        var svc = BuildService(fake);
        var result = await svc.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "b",
            Key = "k",
            UploadId = "upload-123",
            Parts =
            [
                new MultipartUploadPart
                {
                    PartNumber = 1,
                    ETag = "\"part-etag\"",
                    ContentLength = 5,
                    LastModifiedUtc = lastModified
                }
            ]
        });

        Assert.True(result.IsSuccess);
        Assert.Equal("v-complete", result.Value!.VersionId);
        Assert.Equal("head-sha256", result.Value.Checksums!["sha256"]);
        Assert.Equal("completion-crc32c", result.Value.Checksums["crc32c"]);
    }

    [Fact]
    public async Task AbortMultipartUploadAsync_CallsClient()
    {
        var fake = new FakeS3Client();
        var svc = BuildService(fake);

        var result = await svc.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "b",
            Key = "k",
            UploadId = "upload-123"
        });

        Assert.True(result.IsSuccess);
        Assert.True(fake.AbortMultipartUploadCalled);
    }

    // --- DI bootstrap ---

    [Fact]
    public async Task AddS3Storage_RegistersBackend_WithExpectedKindNameAndCapabilities()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3Core();
        services.AddIntegratedS3();
        services.AddS3Storage(new S3StorageOptions
        {
            ProviderName = "s3-test",
            Region = "us-east-1"
        });

        await using var sp = services.BuildServiceProvider();
        var descriptorProvider = sp.GetRequiredService<IStorageServiceDescriptorProvider>();

        var descriptor = await descriptorProvider.GetServiceDescriptorAsync();

        var provider = Assert.Single(descriptor.Providers);
        Assert.Equal("s3-test", provider.Name);
        Assert.Equal("s3", provider.Kind);
        Assert.True(provider.IsPrimary);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.BucketOperations);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.Cors);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ListObjects);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.PresignedUrls);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.Versioning);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.MultipartUploads);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.CopyOperations);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.Checksums);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ServerSideEncryption);
        Assert.Contains(
            provider.Capabilities.ServerSideEncryptionDetails.Variants,
            static variant => variant.Algorithm == ObjectServerSideEncryptionAlgorithm.KmsDsse);
        Assert.Equal(StorageObjectAccessMode.Delegated, provider.ObjectLocation.DefaultAccessMode);
        Assert.Equal([StorageObjectAccessMode.Delegated, StorageObjectAccessMode.ProxyStream], provider.ObjectLocation.SupportedAccessModes);
    }
}

internal sealed class FakeS3Client : IS3StorageClient
{
    public List<S3BucketEntry> Buckets { get; } = [];
    public AmazonS3Exception? CreateBucketException { get; set; }
    public bool HeadBucketReturnsNull { get; set; }
    public AmazonS3Exception? DeleteBucketException { get; set; }

    // Location
    public S3BucketLocationEntry BucketLocation { get; set; } = new(null);
    public AmazonS3Exception? GetBucketLocationException { get; set; }

    // Versioning
    public BucketVersioningStatus VersioningStatus { get; set; } = BucketVersioningStatus.Disabled;
    public BucketVersioningStatus? SetVersioningStatus { get; private set; }

    // CORS
    public S3CorsConfigurationEntry? BucketCorsConfiguration { get; set; }
    public AmazonS3Exception? GetBucketCorsException { get; set; }
    public AmazonS3Exception? PutBucketCorsException { get; set; }
    public AmazonS3Exception? DeleteBucketCorsException { get; set; }
    public bool DeleteBucketCorsCalled { get; private set; }

    // Object listing
    public List<S3ObjectListPage> ObjectPages { get; } = [];
    private int _objectPageIndex;
    public int ObjectListCalls { get; private set; }

    public List<S3ObjectVersionListPage> VersionPages { get; } = [];
    private int _versionPageIndex;
    public int VersionListCalls { get; private set; }

    public List<S3MultipartUploadListPage> MultipartUploadPages { get; } = [];
    private int _multipartUploadPageIndex;
    public int MultipartUploadListCalls { get; private set; }

    // Head object
    public S3ObjectEntry? HeadObjectResult { get; set; }
    public bool HeadObjectReturnsNull { get; set; }
    public Uri? PresignedGetObjectUrl { get; set; }
    public string? LastPresignedBucketName { get; private set; }
    public string? LastPresignedKey { get; private set; }
    public string? LastPresignedVersionId { get; private set; }
    public DateTimeOffset? LastPresignedExpiresAtUtc { get; private set; }
    public int PresignedGetObjectUrlCalls { get; private set; }

    // Get object
    public S3GetObjectResult? GetObjectResult { get; set; }
    public AmazonS3Exception? GetObjectException { get; set; }

    // Put object
    public S3ObjectEntry? PutObjectResult { get; set; }
    public AmazonS3Exception? PutObjectException { get; set; }
    public ObjectServerSideEncryptionSettings? LastPutObjectServerSideEncryption { get; private set; }

    // Delete object
    public S3DeleteObjectResult? DeleteObjectResult { get; set; }

    // Copy object
    public S3ObjectEntry? CopyObjectResult { get; set; }
    public AmazonS3Exception? CopyObjectException { get; set; }
    public CopyObjectRequest? LastCopyRequest { get; private set; }

    // Multipart
    public MultipartUploadInfo? InitiateMultipartUploadResult { get; set; }
    public AmazonS3Exception? InitiateMultipartUploadException { get; set; }
    public ObjectServerSideEncryptionSettings? LastInitiateMultipartUploadServerSideEncryption { get; private set; }
    public MultipartUploadPart? UploadMultipartPartResult { get; set; }
    public AmazonS3Exception? UploadMultipartPartException { get; set; }
    public S3ObjectEntry? CompleteMultipartUploadResult { get; set; }
    public AmazonS3Exception? CompleteMultipartUploadException { get; set; }
    public AmazonS3Exception? AbortMultipartUploadException { get; set; }
    public bool AbortMultipartUploadCalled { get; private set; }

    // Tags (keyed by object key)
    public Dictionary<string, Dictionary<string, string>> ObjectTags { get; } = new(StringComparer.Ordinal);

    public Task<IReadOnlyList<S3BucketEntry>> ListBucketsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<S3BucketEntry>>(Buckets);

    public Task<S3BucketEntry> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (CreateBucketException is not null) throw CreateBucketException;
        return Task.FromResult(new S3BucketEntry(bucketName, DateTimeOffset.UtcNow));
    }

    public Task<S3BucketEntry?> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromResult<S3BucketEntry?>(
            HeadBucketReturnsNull ? null : new S3BucketEntry(bucketName, DateTimeOffset.UtcNow));

    public Task DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (DeleteBucketException is not null) throw DeleteBucketException;
        return Task.CompletedTask;
    }

    public Task<S3BucketLocationEntry> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (GetBucketLocationException is not null) {
            throw GetBucketLocationException;
        }

        return Task.FromResult(BucketLocation);
    }

    public Task<S3VersioningEntry> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromResult(new S3VersioningEntry(VersioningStatus));

    public Task<S3VersioningEntry> SetBucketVersioningAsync(string bucketName, BucketVersioningStatus status, CancellationToken cancellationToken = default)
    {
        SetVersioningStatus = status;
        VersioningStatus = status;
        return Task.FromResult(new S3VersioningEntry(status));
    }

    public Task<S3CorsConfigurationEntry?> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (GetBucketCorsException is not null) {
            throw GetBucketCorsException;
        }

        return Task.FromResult(BucketCorsConfiguration);
    }

    public Task<S3CorsConfigurationEntry> SetBucketCorsAsync(string bucketName, IReadOnlyList<S3CorsRuleEntry> rules, CancellationToken cancellationToken = default)
    {
        if (PutBucketCorsException is not null) {
            throw PutBucketCorsException;
        }

        BucketCorsConfiguration = new S3CorsConfigurationEntry(rules.ToArray());
        return Task.FromResult(BucketCorsConfiguration);
    }

    public Task DeleteBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (DeleteBucketCorsException is not null) {
            throw DeleteBucketCorsException;
        }

        DeleteBucketCorsCalled = true;
        BucketCorsConfiguration = null;
        return Task.CompletedTask;
    }

    public Task<S3ObjectListPage> ListObjectsAsync(string bucketName, string? prefix, string? continuationToken, int? maxKeys, CancellationToken cancellationToken = default)
    {
        ObjectListCalls++;
        if (_objectPageIndex >= ObjectPages.Count)
            return Task.FromResult(new S3ObjectListPage([], null));

        var page = ObjectPages[_objectPageIndex++];
        return Task.FromResult(page);
    }

    public Task<S3ObjectVersionListPage> ListObjectVersionsAsync(string bucketName, string? prefix, string? delimiter, string? keyMarker, string? versionIdMarker, int? maxKeys, CancellationToken cancellationToken = default)
    {
        VersionListCalls++;
        if (_versionPageIndex >= VersionPages.Count)
            return Task.FromResult(new S3ObjectVersionListPage([], null, null));

        var page = VersionPages[_versionPageIndex++];
        return Task.FromResult(page);
    }

    public Task<S3MultipartUploadListPage> ListMultipartUploadsAsync(string bucketName, string? prefix, string? keyMarker, string? uploadIdMarker, int? maxUploads, CancellationToken cancellationToken = default)
    {
        MultipartUploadListCalls++;
        if (_multipartUploadPageIndex >= MultipartUploadPages.Count)
            return Task.FromResult(new S3MultipartUploadListPage([], null, null));

        var page = MultipartUploadPages[_multipartUploadPageIndex++];
        return Task.FromResult(page);
    }

    public Task<S3ObjectEntry?> HeadObjectAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
    {
        if (HeadObjectReturnsNull)
            return Task.FromResult<S3ObjectEntry?>(null);
        return Task.FromResult<S3ObjectEntry?>(HeadObjectResult ?? new S3ObjectEntry(key, 0, null, null, DateTimeOffset.UtcNow, null, null));
    }

    public Task<Uri> CreatePresignedGetObjectUrlAsync(
        string bucketName,
        string key,
        string? versionId,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        PresignedGetObjectUrlCalls++;
        LastPresignedBucketName = bucketName;
        LastPresignedKey = key;
        LastPresignedVersionId = versionId;
        LastPresignedExpiresAtUtc = expiresAtUtc;

        return Task.FromResult(PresignedGetObjectUrl ?? new Uri($"https://s3.test/{bucketName}/{Uri.EscapeDataString(key)}?X-Amz-Signature=test", UriKind.Absolute));
    }

    public Task<S3GetObjectResult> GetObjectAsync(string bucketName, string key, string? versionId, ObjectRange? range, string? ifMatchETag, string? ifNoneMatchETag, DateTimeOffset? ifModifiedSinceUtc, DateTimeOffset? ifUnmodifiedSinceUtc, CancellationToken cancellationToken = default)
    {
        if (GetObjectException is not null) throw GetObjectException;
        var result = GetObjectResult ?? new S3GetObjectResult(
            new S3ObjectEntry(key, 0, null, null, DateTimeOffset.UtcNow, null, null),
            Stream.Null,
            0);
        return Task.FromResult(result);
    }

    public Task<S3ObjectEntry> PutObjectAsync(string bucketName, string key, Stream content, long? contentLength, string? contentType, IReadOnlyDictionary<string, string>? metadata, IReadOnlyDictionary<string, string>? checksums, ObjectServerSideEncryptionSettings? serverSideEncryption, CancellationToken cancellationToken = default)
    {
        if (PutObjectException is not null) throw PutObjectException;
        LastPutObjectServerSideEncryption = serverSideEncryption;
        return Task.FromResult(PutObjectResult ?? new S3ObjectEntry(key, contentLength ?? 0, contentType, null, DateTimeOffset.UtcNow, metadata, null, Checksums: checksums));
    }

    public Task<S3DeleteObjectResult> DeleteObjectAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteObjectResult ?? new S3DeleteObjectResult(key, null, false));

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
        bool overwriteIfExists,
        ObjectServerSideEncryptionSettings? destinationServerSideEncryption,
        CancellationToken cancellationToken = default)
    {
        if (CopyObjectException is not null) throw CopyObjectException;

        LastCopyRequest = new CopyObjectRequest
        {
            SourceBucketName = sourceBucketName,
            SourceKey = sourceKey,
            DestinationBucketName = destinationBucketName,
            DestinationKey = destinationKey,
            SourceVersionId = sourceVersionId,
            SourceIfMatchETag = sourceIfMatchETag,
            SourceIfNoneMatchETag = sourceIfNoneMatchETag,
            SourceIfModifiedSinceUtc = sourceIfModifiedSinceUtc,
            SourceIfUnmodifiedSinceUtc = sourceIfUnmodifiedSinceUtc,
            DestinationServerSideEncryption = destinationServerSideEncryption,
            OverwriteIfExists = overwriteIfExists
        };

        return Task.FromResult(CopyObjectResult ?? new S3ObjectEntry(destinationKey, 0, null, null, DateTimeOffset.UtcNow, null, null));
    }

    public Task<MultipartUploadInfo> InitiateMultipartUploadAsync(
        string bucketName,
        string key,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        string? checksumAlgorithm,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        CancellationToken cancellationToken = default)
    {
        if (InitiateMultipartUploadException is not null) throw InitiateMultipartUploadException;
        LastInitiateMultipartUploadServerSideEncryption = serverSideEncryption;

        return Task.FromResult(InitiateMultipartUploadResult ?? new MultipartUploadInfo
        {
            BucketName = bucketName,
            Key = key,
            UploadId = "upload-1",
            InitiatedAtUtc = DateTimeOffset.UtcNow,
            ChecksumAlgorithm = checksumAlgorithm?.ToLowerInvariant()
        });
    }

    public Task<MultipartUploadPart> UploadMultipartPartAsync(
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        Stream content,
        long? contentLength,
        string? checksumAlgorithm,
        IReadOnlyDictionary<string, string>? checksums,
        CancellationToken cancellationToken = default)
    {
        if (UploadMultipartPartException is not null) throw UploadMultipartPartException;

        return Task.FromResult(UploadMultipartPartResult ?? new MultipartUploadPart
        {
            PartNumber = partNumber,
            ETag = "\"part-etag\"",
            ContentLength = contentLength ?? 0,
            LastModifiedUtc = DateTimeOffset.UtcNow,
            Checksums = checksums
        });
    }

    public Task<S3ObjectEntry> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        IReadOnlyList<MultipartUploadPart> parts,
        CancellationToken cancellationToken = default)
    {
        if (CompleteMultipartUploadException is not null) throw CompleteMultipartUploadException;

        return Task.FromResult(CompleteMultipartUploadResult ?? new S3ObjectEntry(
            key,
            0,
            null,
            "\"complete-etag\"",
            DateTimeOffset.UtcNow,
            null,
            null,
            Checksums: parts.FirstOrDefault()?.Checksums));
    }

    public Task AbortMultipartUploadAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        if (AbortMultipartUploadException is not null) throw AbortMultipartUploadException;

        AbortMultipartUploadCalled = true;
        return Task.CompletedTask;
    }

    public Task<IReadOnlyDictionary<string, string>> GetObjectTagsAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
    {
        IReadOnlyDictionary<string, string> tags = ObjectTags.TryGetValue(key, out var t)
            ? t
            : new Dictionary<string, string>(StringComparer.Ordinal);
        return Task.FromResult(tags);
    }

    public Task PutObjectTagsAsync(string bucketName, string key, string? versionId, IReadOnlyDictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        ObjectTags[key] = new Dictionary<string, string>(tags, StringComparer.Ordinal);
        return Task.CompletedTask;
    }

    public Task DeleteObjectTagsAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
    {
        ObjectTags.Remove(key);
        return Task.CompletedTask;
    }

    public void Dispose() { }
}

internal sealed class TrackingDisposable : IDisposable
{
    public bool IsDisposed { get; private set; }

    public void Dispose()
    {
        IsDisposed = true;
    }
}
