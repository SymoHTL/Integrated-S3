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
        Assert.Equal(StorageCapabilitySupport.Unsupported, caps.MultipartUploads);
        Assert.Equal(StorageCapabilitySupport.Unsupported, caps.CopyOperations);
    }

    // --- Support state descriptor ---

    [Fact]
    public async Task GetSupportStateDescriptor_ReturnsNotApplicableForAllFields()
    {
        var svc = BuildService(new FakeS3Client());
        var desc = await svc.GetSupportStateDescriptorAsync();

        Assert.Equal(StorageSupportStateOwnership.NotApplicable, desc.ObjectMetadata);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, desc.ObjectTags);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, desc.MultipartState);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, desc.Versioning);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, desc.Checksums);
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
    public async Task DeleteBucketAsync_TranslatesBucketNotEmptyException_ToPreconditionFailed()
    {
        var fake = new FakeS3Client();
        fake.DeleteBucketException = new AmazonS3Exception(
            "Bucket is not empty.", ErrorType.Sender, "BucketNotEmpty", "req-2", HttpStatusCode.Conflict);

        var svc = BuildService(fake);
        var result = await svc.DeleteBucketAsync(new DeleteBucketRequest { BucketName = "my-bucket" });

        Assert.False(result.IsSuccess);
        Assert.Equal(StorageErrorCode.PreconditionFailed, result.Error!.Code);
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
        fake.HeadObjectResult = new S3ObjectEntry("k", 512, "text/plain", "\"abc\"", lastModified, null, "v1");
        var svc = BuildService(fake);

        var result = await svc.HeadObjectAsync(new HeadObjectRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        Assert.Equal("k", result.Value!.Key);
        Assert.Equal(512, result.Value.ContentLength);
        Assert.Equal("text/plain", result.Value.ContentType);
        Assert.Equal("\"abc\"", result.Value.ETag);
        Assert.Equal("v1", result.Value.VersionId);
    }

    // --- GetObjectAsync ---

    [Fact]
    public async Task GetObjectAsync_ReturnsContent_WhenObjectExists()
    {
        var content = new MemoryStream([1, 2, 3]);
        var lastModified = new DateTimeOffset(2025, 6, 1, 0, 0, 0, TimeSpan.Zero);
        var fake = new FakeS3Client();
        fake.GetObjectResult = new S3GetObjectResult(
            new S3ObjectEntry("k", 3, "application/octet-stream", "\"etag\"", lastModified, null, null),
            content,
            3);

        var svc = BuildService(fake);
        var result = await svc.GetObjectAsync(new GetObjectRequest { BucketName = "b", Key = "k" });

        Assert.True(result.IsSuccess);
        await using var response = result.Value!;
        Assert.Equal("k", response.Object.Key);
        Assert.Equal(3, response.TotalContentLength);
        Assert.False(response.IsNotModified);
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
        var fake = new FakeS3Client();
        fake.PutObjectResult = new S3ObjectEntry("k", 10, "text/plain", "\"new-etag\"", DateTimeOffset.UtcNow, null, "v1");

        var svc = BuildService(fake);
        var result = await svc.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "b",
            Key = "k",
            Content = new MemoryStream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            ContentType = "text/plain"
        });

        Assert.True(result.IsSuccess);
        Assert.Equal("k", result.Value!.Key);
        Assert.Equal("\"new-etag\"", result.Value.ETag);
        Assert.Equal("v1", result.Value.VersionId);
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
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ListObjects);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.Versioning);
        Assert.Equal(StorageCapabilitySupport.Unsupported, provider.Capabilities.MultipartUploads);
    }
}

internal sealed class FakeS3Client : IS3StorageClient
{
    public List<S3BucketEntry> Buckets { get; } = [];
    public AmazonS3Exception? CreateBucketException { get; set; }
    public bool HeadBucketReturnsNull { get; set; }
    public AmazonS3Exception? DeleteBucketException { get; set; }

    // Versioning
    public BucketVersioningStatus VersioningStatus { get; set; } = BucketVersioningStatus.Disabled;
    public BucketVersioningStatus? SetVersioningStatus { get; private set; }

    // Object listing
    public List<S3ObjectListPage> ObjectPages { get; } = [];
    private int _objectPageIndex;
    public int ObjectListCalls { get; private set; }

    public List<S3ObjectVersionListPage> VersionPages { get; } = [];
    private int _versionPageIndex;
    public int VersionListCalls { get; private set; }

    // Head object
    public S3ObjectEntry? HeadObjectResult { get; set; }
    public bool HeadObjectReturnsNull { get; set; }

    // Get object
    public S3GetObjectResult? GetObjectResult { get; set; }
    public AmazonS3Exception? GetObjectException { get; set; }

    // Put object
    public S3ObjectEntry? PutObjectResult { get; set; }
    public AmazonS3Exception? PutObjectException { get; set; }

    // Delete object
    public S3DeleteObjectResult? DeleteObjectResult { get; set; }

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

    public Task<S3VersioningEntry> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromResult(new S3VersioningEntry(VersioningStatus));

    public Task<S3VersioningEntry> SetBucketVersioningAsync(string bucketName, BucketVersioningStatus status, CancellationToken cancellationToken = default)
    {
        SetVersioningStatus = status;
        VersioningStatus = status;
        return Task.FromResult(new S3VersioningEntry(status));
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

    public Task<S3ObjectEntry?> HeadObjectAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
    {
        if (HeadObjectReturnsNull)
            return Task.FromResult<S3ObjectEntry?>(null);
        return Task.FromResult<S3ObjectEntry?>(HeadObjectResult ?? new S3ObjectEntry(key, 0, null, null, DateTimeOffset.UtcNow, null, null));
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

    public Task<S3ObjectEntry> PutObjectAsync(string bucketName, string key, Stream content, long? contentLength, string? contentType, IReadOnlyDictionary<string, string>? metadata, CancellationToken cancellationToken = default)
    {
        if (PutObjectException is not null) throw PutObjectException;
        return Task.FromResult(PutObjectResult ?? new S3ObjectEntry(key, contentLength ?? 0, contentType, null, DateTimeOffset.UtcNow, metadata, null));
    }

    public Task<S3DeleteObjectResult> DeleteObjectAsync(string bucketName, string key, string? versionId, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteObjectResult ?? new S3DeleteObjectResult(key, null, false));

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
