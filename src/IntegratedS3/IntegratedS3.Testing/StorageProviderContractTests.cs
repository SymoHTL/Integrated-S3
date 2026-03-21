using System.Text;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Testing;

/// <summary>
/// Reusable xUnit contract tests for custom <see cref="IStorageBackend" /> implementations.
/// Optional scenarios are gated by the backend's reported capabilities and the supplied
/// <see cref="ContractOptions" />.
/// </summary>
public abstract class StorageProviderContractTests
{
    /// <summary>
    /// Gets the options that gate optional contract-test scenarios.
    /// Override to enable checksum, state-store, or algorithm-specific tests.
    /// </summary>
    protected virtual StorageProviderContractTestOptions ContractOptions => new();

    /// <summary>
    /// Creates a new <see cref="StorageProviderContractFixture"/> for the provider under test.
    /// Each test invocation receives its own fixture instance.
    /// </summary>
    /// <returns>An uninitialized or pre-initialized fixture for the storage backend.</returns>
    protected abstract StorageProviderContractFixture CreateFixture();

    /// <summary>
    /// Verifies that buckets can be created, inspected via HEAD, listed, and deleted,
    /// and that HEAD returns <see cref="StorageErrorCode.BucketNotFound"/> after deletion.
    /// </summary>
    [Fact]
    public async Task ProviderContract_BucketLifecycle_CreatesHeadsListsAndDeletesBuckets()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        Assert.NotEqual(StorageCapabilitySupport.Unsupported, capabilities.BucketOperations);

        var createBucket = RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-buckets"
        }));
        Assert.Equal("contract-buckets", createBucket.Name);

        var headBucket = RequireSuccess(await storage.HeadBucketAsync("contract-buckets"));
        Assert.Equal("contract-buckets", headBucket.Name);

        var buckets = await storage.ListBucketsAsync().ToArrayAsync();
        Assert.Contains(buckets, static bucket => bucket.Name == "contract-buckets");

        RequireSuccess(await storage.DeleteBucketAsync(new DeleteBucketRequest
        {
            BucketName = "contract-buckets"
        }));

        RequireFailure(await storage.HeadBucketAsync("contract-buckets"), StorageErrorCode.BucketNotFound);
    }

    /// <summary>
    /// Verifies a full object round-trip: PUT with content, metadata, and checksums,
    /// then LIST, HEAD, GET (including payload read-back), and DELETE with a final
    /// <see cref="StorageErrorCode.ObjectNotFound"/> confirmation.
    /// </summary>
    [Fact]
    public async Task ProviderContract_PutGetHeadListAndDeleteObject_RoundTripsContentMetadataAndChecksums()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        Assert.NotEqual(StorageCapabilitySupport.Unsupported, capabilities.ObjectCrud);

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-objects"
        }));

        const string payload = "hello integrated s3";
        var checksumAlgorithm = GetPrimaryChecksumAlgorithm();
        var expectedChecksum = ComputeChecksum(payload, checksumAlgorithm);
        IReadOnlyDictionary<string, string>? metadata = Supports(capabilities.ObjectMetadata)
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["author"] = "copilot"
            }
            : null;
        IReadOnlyDictionary<string, string>? checksums = Supports(capabilities.Checksums)
            ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [checksumAlgorithm] = expectedChecksum
            }
            : null;

        var putObject = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-objects",
            Key = "docs/object.txt",
            Content = CreateUtf8Stream(payload),
            ContentType = "text/plain",
            Metadata = metadata,
            Checksums = checksums
        }));

        Assert.Equal("contract-objects", putObject.BucketName);
        Assert.Equal("docs/object.txt", putObject.Key);
        Assert.Equal("text/plain", putObject.ContentType);

        if (Supports(capabilities.ObjectMetadata)) {
            Assert.Equal("copilot", putObject.Metadata!["author"]);
        }

        if (Supports(capabilities.Checksums)) {
            AssertChecksumEquals(putObject.Checksums, checksumAlgorithm, expectedChecksum);
        }

        if (Supports(capabilities.ListObjects)) {
            var listedObjects = await storage.ListObjectsAsync(new ListObjectsRequest
            {
                BucketName = "contract-objects"
            }).ToArrayAsync();

            var listedObject = Assert.Single(listedObjects);
            Assert.Equal("docs/object.txt", listedObject.Key);
        }

        var headObject = RequireSuccess(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-objects",
            Key = "docs/object.txt"
        }));
        Assert.Equal("text/plain", headObject.ContentType);

        if (!string.IsNullOrWhiteSpace(putObject.VersionId)) {
            Assert.Equal(putObject.VersionId, headObject.VersionId);
        }

        if (Supports(capabilities.ObjectMetadata)) {
            Assert.Equal("copilot", headObject.Metadata!["author"]);
        }

        if (Supports(capabilities.Checksums)) {
            AssertChecksumEquals(headObject.Checksums, checksumAlgorithm, expectedChecksum);
        }

        var getObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-objects",
            Key = "docs/object.txt"
        }));
        await using (getObject) {
            Assert.Equal(payload, await ReadUtf8Async(getObject));
            if (!string.IsNullOrWhiteSpace(putObject.VersionId)) {
                Assert.Equal(putObject.VersionId, getObject.Object.VersionId);
            }

            if (Supports(capabilities.ObjectMetadata)) {
                Assert.Equal("copilot", getObject.Object.Metadata!["author"]);
            }

            if (Supports(capabilities.Checksums)) {
                AssertChecksumEquals(getObject.Object.Checksums, checksumAlgorithm, expectedChecksum);
            }
        }

        RequireSuccess(await storage.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "contract-objects",
            Key = "docs/object.txt"
        }));

        RequireFailure(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-objects",
            Key = "docs/object.txt"
        }), StorageErrorCode.ObjectNotFound);
    }

    /// <summary>
    /// Verifies that a PUT with an intentionally wrong checksum is rejected with
    /// <see cref="StorageErrorCode.InvalidChecksum"/> and that the object is not persisted.
    /// </summary>
    [Fact]
    public async Task ProviderContract_InvalidChecksums_AreRejectedWithoutPersistingObjects()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.Checksums)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-invalid-checksums"
        }));

        var checksumAlgorithm = GetPrimaryChecksumAlgorithm();

        var putResult = await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-invalid-checksums",
            Key = "docs/object.txt",
            Content = CreateUtf8Stream("checksum payload"),
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [checksumAlgorithm] = "invalid-checksum"
            }
        });

        RequireFailure(putResult, StorageErrorCode.InvalidChecksum);

        RequireFailure(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-invalid-checksums",
            Key = "docs/object.txt"
        }), StorageErrorCode.ObjectNotFound);
    }

    /// <summary>
    /// Verifies that multiple checksum algorithms can be supplied on a single PUT and are
    /// persisted correctly, and that a conflicting checksum value on a subsequent PUT is
    /// rejected with <see cref="StorageErrorCode.InvalidChecksum"/>.
    /// </summary>
    [Fact]
    public async Task ProviderContract_ChecksumWrites_PersistMultipleAlgorithmsAndRejectConflicts()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.Checksums)) {
            return;
        }

        var checksumAlgorithms = GetConfiguredChecksumAlgorithms(maxCount: 2);
        if (checksumAlgorithms.Count < 2) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-checksum-conflicts"
        }));

        const string payload = "checksum conflict payload";
        var expectedChecksums = CreateChecksums(payload, checksumAlgorithms);

        var putObject = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-checksum-conflicts",
            Key = "docs/valid.txt",
            Content = CreateUtf8Stream(payload),
            ContentType = "text/plain",
            Checksums = expectedChecksums
        }));

        foreach (var expectedChecksum in expectedChecksums) {
            AssertChecksumEquals(putObject.Checksums, expectedChecksum.Key, expectedChecksum.Value);
        }

        var headObject = RequireSuccess(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-checksum-conflicts",
            Key = "docs/valid.txt"
        }));

        foreach (var expectedChecksum in expectedChecksums) {
            AssertChecksumEquals(headObject.Checksums, expectedChecksum.Key, expectedChecksum.Value);
        }

        var getObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-checksum-conflicts",
            Key = "docs/valid.txt"
        }));
        await using (getObject) {
            Assert.Equal(payload, await ReadUtf8Async(getObject));
            foreach (var expectedChecksum in expectedChecksums) {
                AssertChecksumEquals(getObject.Object.Checksums, expectedChecksum.Key, expectedChecksum.Value);
            }
        }

        var conflictingChecksums = new Dictionary<string, string>(expectedChecksums, StringComparer.OrdinalIgnoreCase)
        {
            [checksumAlgorithms[^1]] = "invalid-checksum"
        };

        RequireFailure(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-checksum-conflicts",
            Key = "docs/conflict.txt",
            Content = CreateUtf8Stream(payload),
            ContentType = "text/plain",
            Checksums = conflictingChecksums
        }), StorageErrorCode.InvalidChecksum);

        RequireFailure(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-checksum-conflicts",
            Key = "docs/conflict.txt"
        }), StorageErrorCode.ObjectNotFound);
    }

    /// <summary>
    /// Verifies that bucket versioning can be enabled at creation time and subsequently
    /// suspended, and that the suspended state is reflected by HEAD and LIST bucket operations.
    /// </summary>
    [Fact]
    public async Task ProviderContract_BucketVersioning_CanBeEnabledAndSuspended()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.Versioning)) {
            return;
        }

        var createBucket = RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-versioning",
            EnableVersioning = true
        }));
        Assert.True(createBucket.VersioningEnabled);

        var versioning = RequireSuccess(await storage.GetBucketVersioningAsync("contract-versioning"));
        Assert.Equal(BucketVersioningStatus.Enabled, versioning.Status);

        var suspended = RequireSuccess(await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
        {
            BucketName = "contract-versioning",
            Status = BucketVersioningStatus.Suspended
        }));
        Assert.Equal(BucketVersioningStatus.Suspended, suspended.Status);

        var headBucket = RequireSuccess(await storage.HeadBucketAsync("contract-versioning"));
        Assert.False(headBucket.VersioningEnabled);

        var buckets = await storage.ListBucketsAsync().ToArrayAsync();
        Assert.Contains(buckets, static bucket => bucket.Name == "contract-versioning" && !bucket.VersioningEnabled);
    }

    /// <summary>
    /// Verifies that versioned objects retain historical access: two versions of the same key
    /// can be read independently, a soft delete creates a delete marker, and
    /// <see cref="StorageErrorCode.ObjectNotFound"/> is returned for the current key while
    /// historical versions remain accessible.
    /// </summary>
    [Fact]
    public async Task ProviderContract_VersionedObjects_PreserveHistoricalAccessAndDeleteMarkers()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.Versioning)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-version-history",
            EnableVersioning = true
        }));

        var v1 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version one"),
            ContentType = "text/plain"
        }));

        var v2 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version two"),
            ContentType = "text/plain"
        }));

        Assert.False(string.IsNullOrWhiteSpace(v1.VersionId));
        Assert.False(string.IsNullOrWhiteSpace(v2.VersionId));
        Assert.NotEqual(v1.VersionId, v2.VersionId);

        var currentObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt"
        }));
        await using (currentObject) {
            Assert.Equal("version two", await ReadUtf8Async(currentObject));
            Assert.Equal(v2.VersionId, currentObject.Object.VersionId);
        }

        var historicalObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt",
            VersionId = v1.VersionId
        }));
        await using (historicalObject) {
            Assert.Equal("version one", await ReadUtf8Async(historicalObject));
            Assert.Equal(v1.VersionId, historicalObject.Object.VersionId);
        }

        var deleteCurrent = RequireSuccess(await storage.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt"
        }));
        Assert.True(deleteCurrent.IsDeleteMarker);
        Assert.False(string.IsNullOrWhiteSpace(deleteCurrent.VersionId));

        RequireFailure(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt"
        }), StorageErrorCode.ObjectNotFound);

        RequireFailure(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt"
        }), StorageErrorCode.ObjectNotFound);

        var latestHistoricalHead = RequireSuccess(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt",
            VersionId = v2.VersionId
        }));
        Assert.Equal(v2.VersionId, latestHistoricalHead.VersionId);
        Assert.False(latestHistoricalHead.IsDeleteMarker);

        var latestHistoricalObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt",
            VersionId = v2.VersionId
        }));
        await using (latestHistoricalObject) {
            Assert.Equal("version two", await ReadUtf8Async(latestHistoricalObject));
            Assert.Equal(v2.VersionId, latestHistoricalObject.Object.VersionId);
        }

        var earliestHistoricalHead = RequireSuccess(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-version-history",
            Key = "docs/history.txt",
            VersionId = v1.VersionId
        }));
        Assert.Equal(v1.VersionId, earliestHistoricalHead.VersionId);
        Assert.False(earliestHistoricalHead.IsDeleteMarker);

        var versions = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "contract-version-history",
            Prefix = "docs/history.txt"
        }).ToArrayAsync();

        Assert.Equal(3, versions.Length);
        Assert.Contains(versions, version => version.VersionId == deleteCurrent.VersionId && version.IsDeleteMarker && version.IsLatest);
        Assert.Contains(versions, version => version.VersionId == v1.VersionId && !version.IsDeleteMarker);
        Assert.Contains(versions, version => version.VersionId == v2.VersionId && !version.IsDeleteMarker);
    }

    /// <summary>
    /// Verifies that deleting a delete-marker version restores the previous latest version
    /// so that GET and HEAD return the prior object content again.
    /// </summary>
    [Fact]
    public async Task ProviderContract_DeletingDeleteMarkerVersion_RestoresPreviousLatestVersion()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.Versioning)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-delete-marker-restore",
            EnableVersioning = true
        }));

        var v1 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-delete-marker-restore",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version one"),
            ContentType = "text/plain"
        }));

        var v2 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-delete-marker-restore",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version two"),
            ContentType = "text/plain"
        }));

        var deleteCurrent = RequireSuccess(await storage.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "contract-delete-marker-restore",
            Key = "docs/history.txt"
        }));
        Assert.True(deleteCurrent.IsDeleteMarker);

        var deleteMarkerVersionId = Assert.IsType<string>(deleteCurrent.VersionId);

        var deleteMarkerDelete = RequireSuccess(await storage.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "contract-delete-marker-restore",
            Key = "docs/history.txt",
            VersionId = deleteMarkerVersionId
        }));
        Assert.True(deleteMarkerDelete.IsDeleteMarker);
        Assert.Equal(deleteMarkerVersionId, deleteMarkerDelete.VersionId);

        var currentHead = RequireSuccess(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-delete-marker-restore",
            Key = "docs/history.txt"
        }));
        Assert.Equal(v2.VersionId, currentHead.VersionId);
        Assert.True(currentHead.IsLatest);
        Assert.False(currentHead.IsDeleteMarker);

        var currentObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-delete-marker-restore",
            Key = "docs/history.txt"
        }));
        await using (currentObject) {
            Assert.Equal("version two", await ReadUtf8Async(currentObject));
            Assert.Equal(v2.VersionId, currentObject.Object.VersionId);
        }

        var versions = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "contract-delete-marker-restore",
            Prefix = "docs/history.txt"
        }).ToArrayAsync();

        Assert.Equal(2, versions.Length);
        Assert.DoesNotContain(versions, version => version.VersionId == deleteMarkerVersionId);
        Assert.Contains(versions, version => version.VersionId == v2.VersionId && version.IsLatest && !version.IsDeleteMarker);
        Assert.Contains(versions, version => version.VersionId == v1.VersionId && !version.IsDeleteMarker);
    }

    /// <summary>
    /// Verifies that paginated <c>ListObjectVersions</c> requests across multiple versions
    /// and delete markers produce the same ordered result set as an unpaginated request
    /// without repeating entries.
    /// </summary>
    [Fact]
    public async Task ProviderContract_ListObjectVersions_PaginatesAcrossDeleteMarkersWithoutRepeatingEntries()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.Versioning) || !Supports(capabilities.Pagination)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-version-pagination",
            EnableVersioning = true
        }));

        var v1 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-version-pagination",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version one"),
            ContentType = "text/plain"
        }));

        var v2 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-version-pagination",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version two"),
            ContentType = "text/plain"
        }));

        var deleteCurrent = RequireSuccess(await storage.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = "contract-version-pagination",
            Key = "docs/history.txt"
        }));
        Assert.True(deleteCurrent.IsDeleteMarker);

        var v3 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-version-pagination",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version three"),
            ContentType = "text/plain"
        }));

        var secondary = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-version-pagination",
            Key = "docs/secondary.txt",
            Content = CreateUtf8Stream("secondary version"),
            ContentType = "text/plain"
        }));

        var fullListing = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "contract-version-pagination",
            Prefix = "docs/"
        }).ToArrayAsync();

        Assert.Equal(5, fullListing.Length);
        Assert.Contains(fullListing, version => version.VersionId == deleteCurrent.VersionId && version.IsDeleteMarker && !version.IsLatest);
        Assert.Contains(fullListing, version => version.VersionId == v3.VersionId && version.IsLatest && !version.IsDeleteMarker);
        Assert.Contains(fullListing, version => version.VersionId == v2.VersionId && !version.IsDeleteMarker);
        Assert.Contains(fullListing, version => version.VersionId == v1.VersionId && !version.IsDeleteMarker);
        Assert.Contains(fullListing, version => version.VersionId == secondary.VersionId && version.Key == "docs/secondary.txt");

        var firstPage = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "contract-version-pagination",
            Prefix = "docs/",
            PageSize = 2
        }).ToArrayAsync();
        Assert.Equal(2, firstPage.Length);

        var secondPage = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "contract-version-pagination",
            Prefix = "docs/",
            KeyMarker = firstPage[^1].Key,
            VersionIdMarker = firstPage[^1].VersionId,
            PageSize = 2
        }).ToArrayAsync();
        Assert.Equal(2, secondPage.Length);

        var thirdPage = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "contract-version-pagination",
            Prefix = "docs/",
            KeyMarker = secondPage[^1].Key,
            VersionIdMarker = secondPage[^1].VersionId,
            PageSize = 2
        }).ToArrayAsync();
        Assert.Single(thirdPage);

        var exhaustedPage = await storage.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = "contract-version-pagination",
            Prefix = "docs/",
            KeyMarker = thirdPage[^1].Key,
            VersionIdMarker = thirdPage[^1].VersionId,
            PageSize = 2
        }).ToArrayAsync();
        Assert.Empty(exhaustedPage);

        var pagedListing = firstPage.Concat(secondPage).Concat(thirdPage)
            .Select(static version => (version.Key, version.VersionId, version.IsDeleteMarker, version.IsLatest))
            .ToArray();
        var fullProjection = fullListing
            .Select(static version => (version.Key, version.VersionId, version.IsDeleteMarker, version.IsLatest))
            .ToArray();

        Assert.Equal(fullProjection, pagedListing);
    }

    /// <summary>
    /// Verifies that conditional GET requests (<c>If-Match</c> / <c>If-None-Match</c>) work
    /// correctly against historical object versions, returning the expected content or
    /// <see cref="StorageErrorCode.PreconditionFailed"/>.
    /// </summary>
    [Fact]
    public async Task ProviderContract_ConditionalReads_TargetHistoricalVersions()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.Versioning) || !Supports(capabilities.ConditionalRequests)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-version-conditions",
            EnableVersioning = true
        }));

        var v1 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-version-conditions",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version one"),
            ContentType = "text/plain"
        }));

        var v2 = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-version-conditions",
            Key = "docs/history.txt",
            Content = CreateUtf8Stream("version two"),
            ContentType = "text/plain"
        }));

        Assert.False(string.IsNullOrWhiteSpace(v1.VersionId));
        Assert.False(string.IsNullOrWhiteSpace(v2.VersionId));
        Assert.NotEqual(v1.VersionId, v2.VersionId);
        Assert.NotEqual(v1.ETag, v2.ETag);

        var historicalRead = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-version-conditions",
            Key = "docs/history.txt",
            VersionId = v1.VersionId,
            IfMatchETag = QuoteETag(v1.ETag)
        }));
        await using (historicalRead) {
            Assert.False(historicalRead.IsNotModified);
            Assert.Equal("version one", await ReadUtf8Async(historicalRead));
            Assert.Equal(v1.VersionId, historicalRead.Object.VersionId);
        }

        var historicalNotModified = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-version-conditions",
            Key = "docs/history.txt",
            VersionId = v1.VersionId,
            IfNoneMatchETag = QuoteETag(v1.ETag)
        }));
        await using (historicalNotModified) {
            Assert.True(historicalNotModified.IsNotModified);
            Assert.Equal(v1.VersionId, historicalNotModified.Object.VersionId);
        }

        RequireFailure(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-version-conditions",
            Key = "docs/history.txt",
            VersionId = v1.VersionId,
            IfMatchETag = QuoteETag(v2.ETag)
        }), StorageErrorCode.PreconditionFailed);
    }

    /// <summary>
    /// Verifies that CORS rules can be stored, retrieved, and deleted on a bucket, and that
    /// versioning state is not lost when CORS configuration changes.
    /// </summary>
    [Fact]
    public async Task ProviderContract_BucketCors_RoundTripsRulesWithoutLosingVersioningState()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.Cors)) {
            return;
        }

        var supportsVersioning = Supports(capabilities.Versioning);

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-cors",
            EnableVersioning = supportsVersioning
        }));

        var putCors = RequireSuccess(await storage.PutBucketCorsAsync(new PutBucketCorsRequest
        {
            BucketName = "contract-cors",
            Rules =
            [
                new BucketCorsRule
                {
                    Id = "browser-rule",
                    AllowedOrigins = ["https://app.example"],
                    AllowedMethods = [BucketCorsMethod.Get, BucketCorsMethod.Put],
                    AllowedHeaders = ["authorization", "x-amz-*"],
                    ExposeHeaders = ["etag"],
                    MaxAgeSeconds = 900
                }
            ]
        }));

        var storedRule = Assert.Single(putCors.Rules);
        Assert.Equal("browser-rule", storedRule.Id);

        if (supportsVersioning) {
            var suspended = RequireSuccess(await storage.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = "contract-cors",
                Status = BucketVersioningStatus.Suspended
            }));
            Assert.Equal(BucketVersioningStatus.Suspended, suspended.Status);
        }

        var getCors = RequireSuccess(await storage.GetBucketCorsAsync("contract-cors"));
        storedRule = Assert.Single(getCors.Rules);
        Assert.Equal([BucketCorsMethod.Get, BucketCorsMethod.Put], storedRule.AllowedMethods);
        Assert.Equal(["authorization", "x-amz-*"], storedRule.AllowedHeaders);

        if (supportsVersioning) {
            var versioning = RequireSuccess(await storage.GetBucketVersioningAsync("contract-cors"));
            Assert.Equal(BucketVersioningStatus.Suspended, versioning.Status);
        }

        RequireSuccess(await storage.DeleteBucketCorsAsync(new DeleteBucketCorsRequest
        {
            BucketName = "contract-cors"
        }));

        RequireFailure(await storage.GetBucketCorsAsync("contract-cors"), StorageErrorCode.CorsConfigurationNotFound);

        if (supportsVersioning) {
            var preservedVersioning = RequireSuccess(await storage.GetBucketVersioningAsync("contract-cors"));
            Assert.Equal(BucketVersioningStatus.Suspended, preservedVersioning.Status);
        }
    }

    /// <summary>
    /// Verifies that bucket default encryption can be set, read back, and deleted when
    /// supported, or that all encryption operations return
    /// <see cref="StorageErrorCode.UnsupportedCapability"/> when not supported.
    /// </summary>
    [Fact]
    public async Task ProviderContract_BucketDefaultEncryption_IsExplicitlySupportedOrRejected()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-default-encryption"
        }));

        var putRequest = new PutBucketDefaultEncryptionRequest
        {
            BucketName = "contract-default-encryption",
            Rule = new BucketDefaultEncryptionRule
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
                KeyId = "alias/contract-default-key"
            }
        };

        if (!Supports(capabilities.ServerSideEncryption)) {
            RequireFailure(await storage.GetBucketDefaultEncryptionAsync("contract-default-encryption"), StorageErrorCode.UnsupportedCapability);
            RequireFailure(await storage.PutBucketDefaultEncryptionAsync(putRequest), StorageErrorCode.UnsupportedCapability);
            RequireFailure(await storage.DeleteBucketDefaultEncryptionAsync(new DeleteBucketDefaultEncryptionRequest
            {
                BucketName = "contract-default-encryption"
            }), StorageErrorCode.UnsupportedCapability);
            return;
        }

        var putDefaultEncryption = RequireSuccess(await storage.PutBucketDefaultEncryptionAsync(putRequest));
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.KmsDsse, putDefaultEncryption.Rule.Algorithm);
        Assert.Equal("alias/contract-default-key", putDefaultEncryption.Rule.KeyId);

        var getDefaultEncryption = RequireSuccess(await storage.GetBucketDefaultEncryptionAsync("contract-default-encryption"));
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.KmsDsse, getDefaultEncryption.Rule.Algorithm);
        Assert.Equal("alias/contract-default-key", getDefaultEncryption.Rule.KeyId);

        RequireSuccess(await storage.DeleteBucketDefaultEncryptionAsync(new DeleteBucketDefaultEncryptionRequest
        {
            BucketName = "contract-default-encryption"
        }));

        RequireFailure(await storage.GetBucketDefaultEncryptionAsync("contract-default-encryption"), StorageErrorCode.BucketEncryptionConfigurationNotFound);
    }

    /// <summary>
    /// Verifies that a byte-range GET request returns only the requested slice and
    /// reports correct <see cref="GetObjectResponse.Range"/> and total content length.
    /// </summary>
    [Fact]
    public async Task ProviderContract_RangeRequests_ReturnRequestedSlices()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.RangeRequests)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-range"
        }));

        RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-range",
            Key = "docs/range.txt",
            Content = CreateUtf8Stream("hello integrated s3"),
            ContentType = "text/plain"
        }));

        var getObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-range",
            Key = "docs/range.txt",
            Range = new ObjectRange
            {
                Start = 6,
                End = 15
            }
        }));
        await using (getObject) {
            Assert.Equal("integrated", await ReadUtf8Async(getObject));
            Assert.NotNull(getObject.Range);
            Assert.Equal(6, getObject.Range!.Start);
            Assert.Equal(15, getObject.Range.End);
            Assert.Equal(10, getObject.Object.ContentLength);
            Assert.Equal(19, getObject.TotalContentLength);
        }
    }

    /// <summary>
    /// Verifies conditional GET behavior for <c>If-Match</c>, <c>If-None-Match</c>,
    /// <c>If-Modified-Since</c>, and <c>If-Unmodified-Since</c> preconditions, asserting
    /// correct 304 Not Modified and <see cref="StorageErrorCode.PreconditionFailed"/> responses.
    /// </summary>
    [Fact]
    public async Task ProviderContract_ConditionalReads_HonorEntityTagAndDatePreconditions()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.ConditionalRequests)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-conditions"
        }));

        var putObject = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-conditions",
            Key = "docs/conditions.txt",
            Content = CreateUtf8Stream("hello integrated s3"),
            ContentType = "text/plain"
        }));

        var currentETag = Assert.IsType<string>(putObject.ETag);
        var lastModifiedUtc = putObject.LastModifiedUtc;

        var ifMatch = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-conditions",
            Key = "docs/conditions.txt",
            IfMatchETag = $"\"{currentETag}\""
        }));
        await using (ifMatch) {
            Assert.False(ifMatch.IsNotModified);
        }

        var ifNoneMatch = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-conditions",
            Key = "docs/conditions.txt",
            IfNoneMatchETag = $"\"{currentETag}\""
        }));
        await using (ifNoneMatch) {
            Assert.True(ifNoneMatch.IsNotModified);
        }

        RequireFailure(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-conditions",
            Key = "docs/conditions.txt",
            IfMatchETag = "\"different\""
        }), StorageErrorCode.PreconditionFailed);

        var notModifiedByDate = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-conditions",
            Key = "docs/conditions.txt",
            IfModifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        }));
        await using (notModifiedByDate) {
            Assert.True(notModifiedByDate.IsNotModified);
        }

        RequireFailure(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-conditions",
            Key = "docs/conditions.txt",
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        }), StorageErrorCode.PreconditionFailed);
    }

    /// <summary>
    /// Verifies S3-style conditional-request precedence: <c>If-Match</c> takes priority over
    /// <c>If-Unmodified-Since</c>, <c>If-None-Match</c> takes priority over
    /// <c>If-Modified-Since</c>, and that ranges combine correctly with conditions.
    /// </summary>
    [Fact]
    public async Task ProviderContract_ConditionalReads_RespectPrecedenceAndCombineWithRanges()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.ConditionalRequests)) {
            return;
        }

        var supportsRangeRequests = Supports(capabilities.RangeRequests);
        const string payload = "hello integrated s3";

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-condition-precedence"
        }));

        var putObject = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-condition-precedence",
            Key = "docs/precedence.txt",
            Content = CreateUtf8Stream(payload),
            ContentType = "text/plain"
        }));

        var currentETag = QuoteETag(putObject.ETag);
        var lastModifiedUtc = putObject.LastModifiedUtc;

        var matchedPrecondition = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-condition-precedence",
            Key = "docs/precedence.txt",
            IfMatchETag = currentETag,
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5),
            Range = supportsRangeRequests
                ? new ObjectRange
                {
                    Start = 6,
                    End = 15
                }
                : null
        }));
        await using (matchedPrecondition) {
            Assert.False(matchedPrecondition.IsNotModified);
            Assert.Equal(supportsRangeRequests ? "integrated" : payload, await ReadUtf8Async(matchedPrecondition));

            if (supportsRangeRequests) {
                Assert.NotNull(matchedPrecondition.Range);
                Assert.Equal(6, matchedPrecondition.Range!.Start);
                Assert.Equal(15, matchedPrecondition.Range.End);
                Assert.Equal(10, matchedPrecondition.Object.ContentLength);
                Assert.Equal(payload.Length, matchedPrecondition.TotalContentLength);
            }
        }

        RequireFailure(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-condition-precedence",
            Key = "docs/precedence.txt",
            IfMatchETag = "\"different\"",
            IfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        }), StorageErrorCode.PreconditionFailed);

        var notModifiedByEntityTag = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-condition-precedence",
            Key = "docs/precedence.txt",
            IfNoneMatchETag = currentETag,
            IfModifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        }));
        await using (notModifiedByEntityTag) {
            Assert.True(notModifiedByEntityTag.IsNotModified);
        }

        var ignoredDatePrecondition = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-condition-precedence",
            Key = "docs/precedence.txt",
            IfNoneMatchETag = "\"different\"",
            IfModifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        }));
        await using (ignoredDatePrecondition) {
            Assert.False(ignoredDatePrecondition.IsNotModified);
            Assert.Equal(payload, await ReadUtf8Async(ignoredDatePrecondition));
        }
    }

    /// <summary>
    /// Verifies that <c>CopyObject</c> preserves payload and metadata across buckets,
    /// and that source-side <c>If-Match</c> preconditions are enforced.
    /// </summary>
    [Fact]
    public async Task ProviderContract_CopyObject_PreservesPayloadMetadataAndPreconditions()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.CopyOperations)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-copy-source"
        }));
        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-copy-target"
        }));

        IReadOnlyDictionary<string, string>? metadata = Supports(capabilities.ObjectMetadata)
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["origin"] = "contract-tests"
            }
            : null;

        var putObject = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-copy-source",
            Key = "docs/source.txt",
            Content = CreateUtf8Stream("copy me"),
            ContentType = "text/plain",
            Metadata = metadata
        }));

        if (Supports(capabilities.ConditionalRequests)) {
            RequireFailure(await storage.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucketName = "contract-copy-source",
                SourceKey = "docs/source.txt",
                DestinationBucketName = "contract-copy-target",
                DestinationKey = "docs/blocked.txt",
                SourceIfMatchETag = "\"different\""
            }), StorageErrorCode.PreconditionFailed);
        }

        var copiedObject = RequireSuccess(await storage.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "contract-copy-source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "contract-copy-target",
            DestinationKey = "docs/copied.txt"
        }));

        Assert.Equal("contract-copy-target", copiedObject.BucketName);
        Assert.Equal("docs/copied.txt", copiedObject.Key);
        Assert.Equal("text/plain", copiedObject.ContentType);

        if (Supports(capabilities.ObjectMetadata)) {
            Assert.Equal("contract-tests", copiedObject.Metadata!["origin"]);
        }

        var downloaded = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-copy-target",
            Key = "docs/copied.txt"
        }));
        await using (downloaded) {
            Assert.Equal("copy me", await ReadUtf8Async(downloaded));
            if (Supports(capabilities.ObjectMetadata)) {
                Assert.Equal("contract-tests", downloaded.Object.Metadata!["origin"]);
            }
        }
    }

    /// <summary>
    /// Verifies S3-style source-precondition precedence for <c>CopyObject</c>:
    /// <c>If-Match</c> overrides <c>If-Unmodified-Since</c> and <c>If-None-Match</c>
    /// overrides <c>If-Modified-Since</c>.
    /// </summary>
    [Fact]
    public async Task ProviderContract_CopyObject_SourcePreconditionsRespectPrecedence()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.CopyOperations) || !Supports(capabilities.ConditionalRequests)) {
            return;
        }

        const string payload = "copy me";

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-copy-precedence-source"
        }));
        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-copy-precedence-target"
        }));

        var putObject = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-copy-precedence-source",
            Key = "docs/source.txt",
            Content = CreateUtf8Stream(payload),
            ContentType = "text/plain"
        }));

        var currentETag = QuoteETag(putObject.ETag);
        var lastModifiedUtc = putObject.LastModifiedUtc;

        var copiedByIfMatch = RequireSuccess(await storage.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "contract-copy-precedence-source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "contract-copy-precedence-target",
            DestinationKey = "docs/if-match.txt",
            SourceIfMatchETag = currentETag,
            SourceIfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        }));
        Assert.Equal("contract-copy-precedence-target", copiedByIfMatch.BucketName);
        Assert.Equal("docs/if-match.txt", copiedByIfMatch.Key);

        var copiedIfMatchPayload = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-copy-precedence-target",
            Key = "docs/if-match.txt"
        }));
        await using (copiedIfMatchPayload) {
            Assert.Equal(payload, await ReadUtf8Async(copiedIfMatchPayload));
        }

        RequireFailure(await storage.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "contract-copy-precedence-source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "contract-copy-precedence-target",
            DestinationKey = "docs/blocked-if-match.txt",
            SourceIfMatchETag = "\"different\"",
            SourceIfUnmodifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        }), StorageErrorCode.PreconditionFailed);
        RequireFailure(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-copy-precedence-target",
            Key = "docs/blocked-if-match.txt"
        }), StorageErrorCode.ObjectNotFound);

        RequireFailure(await storage.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "contract-copy-precedence-source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "contract-copy-precedence-target",
            DestinationKey = "docs/blocked-if-none-match.txt",
            SourceIfNoneMatchETag = currentETag,
            SourceIfModifiedSinceUtc = lastModifiedUtc.AddMinutes(-5)
        }), StorageErrorCode.PreconditionFailed);
        RequireFailure(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-copy-precedence-target",
            Key = "docs/blocked-if-none-match.txt"
        }), StorageErrorCode.ObjectNotFound);

        var copiedByIfNoneMatch = RequireSuccess(await storage.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucketName = "contract-copy-precedence-source",
            SourceKey = "docs/source.txt",
            DestinationBucketName = "contract-copy-precedence-target",
            DestinationKey = "docs/if-none-match.txt",
            SourceIfNoneMatchETag = "\"different\"",
            SourceIfModifiedSinceUtc = lastModifiedUtc.AddMinutes(5)
        }));
        Assert.Equal("contract-copy-precedence-target", copiedByIfNoneMatch.BucketName);
        Assert.Equal("docs/if-none-match.txt", copiedByIfNoneMatch.Key);

        var copiedIfNoneMatchPayload = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-copy-precedence-target",
            Key = "docs/if-none-match.txt"
        }));
        await using (copiedIfNoneMatchPayload) {
            Assert.Equal(payload, await ReadUtf8Async(copiedIfNoneMatchPayload));
        }
    }

    /// <summary>
    /// Verifies that object tags can be set, retrieved, and deleted for both the current
    /// and historical versions when versioning is enabled.
    /// </summary>
    [Fact]
    public async Task ProviderContract_ObjectTags_RoundTripAndDeleteAcrossCurrentAndHistoricalVersions()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.ObjectTags)) {
            return;
        }

        var supportsVersioning = Supports(capabilities.Versioning);
        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-tags",
            EnableVersioning = supportsVersioning
        }));

        var firstVersion = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt",
            Content = CreateUtf8Stream("version one"),
            ContentType = "text/plain"
        }));

        if (!supportsVersioning) {
            var putTags = RequireSuccess(await storage.PutObjectTagsAsync(new PutObjectTagsRequest
            {
                BucketName = "contract-tags",
                Key = "docs/tagged.txt",
                Tags = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["environment"] = "test",
                    ["owner"] = "copilot"
                }
            }));
            Assert.Equal("copilot", putTags.Tags["owner"]);

            var getTags = RequireSuccess(await storage.GetObjectTagsAsync(new GetObjectTagsRequest
            {
                BucketName = "contract-tags",
                Key = "docs/tagged.txt"
            }));
            Assert.Equal("test", getTags.Tags["environment"]);

            var headObject = RequireSuccess(await storage.HeadObjectAsync(new HeadObjectRequest
            {
                BucketName = "contract-tags",
                Key = "docs/tagged.txt"
            }));
            Assert.Equal("copilot", headObject.Tags!["owner"]);

            var deletedTags = RequireSuccess(await storage.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
            {
                BucketName = "contract-tags",
                Key = "docs/tagged.txt"
            }));
            Assert.Empty(deletedTags.Tags);

            var clearedTags = RequireSuccess(await storage.GetObjectTagsAsync(new GetObjectTagsRequest
            {
                BucketName = "contract-tags",
                Key = "docs/tagged.txt"
            }));
            Assert.Empty(clearedTags.Tags);
            return;
        }

        var firstVersionTags = RequireSuccess(await storage.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt",
            VersionId = firstVersion.VersionId,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "one"
            }
        }));
        Assert.Equal(firstVersion.VersionId, firstVersionTags.VersionId);

        var currentVersion = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt",
            Content = CreateUtf8Stream("version two"),
            ContentType = "text/plain"
        }));

        var currentTags = RequireSuccess(await storage.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt",
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["generation"] = "two"
            }
        }));
        Assert.Equal(currentVersion.VersionId, currentTags.VersionId);

        var deletedCurrentTags = RequireSuccess(await storage.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt"
        }));
        Assert.Equal(currentVersion.VersionId, deletedCurrentTags.VersionId);
        Assert.Empty(deletedCurrentTags.Tags);

        var rereadCurrentTags = RequireSuccess(await storage.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt"
        }));
        Assert.Equal(currentVersion.VersionId, rereadCurrentTags.VersionId);
        Assert.Empty(rereadCurrentTags.Tags);

        var historicalTags = RequireSuccess(await storage.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt",
            VersionId = firstVersion.VersionId
        }));
        Assert.Equal("one", historicalTags.Tags["generation"]);

        var deletedHistoricalTags = RequireSuccess(await storage.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt",
            VersionId = firstVersion.VersionId
        }));
        Assert.Equal(firstVersion.VersionId, deletedHistoricalTags.VersionId);
        Assert.Empty(deletedHistoricalTags.Tags);

        var rereadHistoricalTags = RequireSuccess(await storage.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = "contract-tags",
            Key = "docs/tagged.txt",
            VersionId = firstVersion.VersionId
        }));
        Assert.Equal(firstVersion.VersionId, rereadHistoricalTags.VersionId);
        Assert.Empty(rereadHistoricalTags.Tags);
    }

    /// <summary>
    /// Verifies the full multipart upload lifecycle: initiate, list in-progress uploads,
    /// upload parts, complete, and confirm that the assembled object preserves content,
    /// content type, and metadata.
    /// </summary>
    [Fact]
    public async Task ProviderContract_MultipartUploads_CompleteAndPreserveMetadata()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.MultipartUploads)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-multipart"
        }));

        var initiatedUpload = RequireSuccess(await storage.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "contract-multipart",
            Key = "docs/assembled.txt",
            ContentType = "text/plain",
            Metadata = Supports(capabilities.ObjectMetadata)
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["source"] = "multipart"
                }
                : null
        }));

        var uploads = await storage.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "contract-multipart"
        }).ToArrayAsync();
        var listedUpload = Assert.Single(uploads);
        Assert.Equal(initiatedUpload.UploadId, listedUpload.UploadId);
        Assert.Equal("docs/assembled.txt", listedUpload.Key);

        var part1 = RequireSuccess(await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "contract-multipart",
            Key = "docs/assembled.txt",
            UploadId = initiatedUpload.UploadId,
            PartNumber = 1,
            Content = CreateUtf8Stream("hello ")
        }));

        var part2 = RequireSuccess(await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "contract-multipart",
            Key = "docs/assembled.txt",
            UploadId = initiatedUpload.UploadId,
            PartNumber = 2,
            Content = CreateUtf8Stream("world")
        }));

        var completedObject = RequireSuccess(await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "contract-multipart",
            Key = "docs/assembled.txt",
            UploadId = initiatedUpload.UploadId,
            Parts = [part1, part2]
        }));

        Assert.Equal("text/plain", completedObject.ContentType);
        if (Supports(capabilities.ObjectMetadata)) {
            Assert.Equal("multipart", completedObject.Metadata!["source"]);
        }

        var assembledObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-multipart",
            Key = "docs/assembled.txt"
        }));
        await using (assembledObject) {
            Assert.Equal("hello world", await ReadUtf8Async(assembledObject));
            if (Supports(capabilities.ObjectMetadata)) {
                Assert.Equal("multipart", assembledObject.Object.Metadata!["source"]);
            }
        }

        uploads = await storage.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "contract-multipart"
        }).ToArrayAsync();
        Assert.Empty(uploads);
    }

    /// <summary>
    /// Verifies that an in-progress multipart upload can be aborted, that completing an
    /// aborted upload fails with <see cref="StorageErrorCode.MultipartConflict"/>, and that
    /// the upload no longer appears in the active upload listing.
    /// </summary>
    [Fact]
    public async Task ProviderContract_MultipartUploads_CanBeAborted()
    {
        await using var fixture = await CreateInitializedFixtureAsync();
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.MultipartUploads)) {
            return;
        }

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-multipart-abort"
        }));

        var initiatedUpload = RequireSuccess(await storage.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "contract-multipart-abort",
            Key = "docs/aborted.txt"
        }));

        var uploadedPart = RequireSuccess(await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "contract-multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiatedUpload.UploadId,
            PartNumber = 1,
            Content = CreateUtf8Stream("temporary")
        }));

        RequireSuccess(await storage.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = "contract-multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiatedUpload.UploadId
        }));

        RequireFailure(await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "contract-multipart-abort",
            Key = "docs/aborted.txt",
            UploadId = initiatedUpload.UploadId,
            Parts = [uploadedPart]
        }), StorageErrorCode.MultipartConflict);

        var uploads = await storage.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "contract-multipart-abort"
        }).ToArrayAsync();
        Assert.Empty(uploads);
    }

    /// <summary>
    /// Verifies that a registered <see cref="IStorageObjectStateStore"/> receives metadata, tags,
    /// and checksums during PUT and tag operations, and that GET returns enriched state.
    /// Also confirms <see cref="StorageSupportStateOwnership.PlatformManaged"/> ownership.
    /// </summary>
    [Fact]
    public async Task ProviderContract_PlatformObjectStateStores_RoundTripMetadataTagsAndChecksums()
    {
        if (!ContractOptions.SupportsPlatformObjectStateStore) {
            return;
        }

        await using var fixture = await CreateInitializedFixtureAsync(services => services.AddInMemoryStorageObjectStateStore());
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.ObjectMetadata) && !Supports(capabilities.ObjectTags) && !Supports(capabilities.Checksums)) {
            return;
        }

        var stateStore = fixture.GetRequiredService<InMemoryObjectStateStore>();
        const string payload = "hello external state";
        var checksumAlgorithm = GetPrimaryChecksumAlgorithm();
        var expectedChecksum = ComputeChecksum(payload, checksumAlgorithm);

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-platform-state",
            EnableVersioning = Supports(capabilities.Versioning)
        }));

        var putObject = RequireSuccess(await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-platform-state",
            Key = "docs/external.txt",
            Content = CreateUtf8Stream(payload),
            ContentType = "text/plain",
            Metadata = Supports(capabilities.ObjectMetadata)
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["owner"] = "platform-store"
                }
                : null,
            Checksums = Supports(capabilities.Checksums)
                ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    [checksumAlgorithm] = expectedChecksum
                }
                : null
        }));

        if (Supports(capabilities.ObjectTags)) {
            RequireSuccess(await storage.PutObjectTagsAsync(new PutObjectTagsRequest
            {
                BucketName = "contract-platform-state",
                Key = "docs/external.txt",
                Tags = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["environment"] = "test"
                }
            }));
        }

        var storedState = await stateStore.GetObjectInfoAsync(storage.Name, "contract-platform-state", "docs/external.txt", putObject.VersionId);
        Assert.NotNull(storedState);

        if (Supports(capabilities.ObjectMetadata)) {
            Assert.Equal("platform-store", storedState!.Metadata!["owner"]);
        }

        if (Supports(capabilities.ObjectTags)) {
            Assert.Equal("test", storedState!.Tags!["environment"]);
        }

        if (Supports(capabilities.Checksums)) {
            AssertChecksumEquals(storedState!.Checksums, checksumAlgorithm, expectedChecksum);
        }

        var getObject = RequireSuccess(await storage.GetObjectAsync(new GetObjectRequest
        {
            BucketName = "contract-platform-state",
            Key = "docs/external.txt"
        }));
        await using (getObject) {
            Assert.Equal(payload, await ReadUtf8Async(getObject));
            if (Supports(capabilities.ObjectMetadata)) {
                Assert.Equal("platform-store", getObject.Object.Metadata!["owner"]);
            }

            if (Supports(capabilities.ObjectTags)) {
                Assert.Equal("test", getObject.Object.Tags!["environment"]);
            }

            if (Supports(capabilities.Checksums)) {
                AssertChecksumEquals(getObject.Object.Checksums, checksumAlgorithm, expectedChecksum);
            }
        }

        var supportState = await storage.GetSupportStateDescriptorAsync();
        if (Supports(capabilities.ObjectMetadata)) {
            Assert.Equal(StorageSupportStateOwnership.PlatformManaged, supportState.ObjectMetadata);
        }

        if (Supports(capabilities.ObjectTags)) {
            Assert.Equal(StorageSupportStateOwnership.PlatformManaged, supportState.ObjectTags);
        }

        if (Supports(capabilities.Checksums)) {
            Assert.Equal(StorageSupportStateOwnership.PlatformManaged, supportState.Checksums);
        }
    }

    /// <summary>
    /// Verifies that a registered <see cref="IStorageMultipartStateStore"/> tracks the multipart
    /// upload lifecycle: state is created on initiate, removed after completion, and
    /// <see cref="StorageSupportStateOwnership.PlatformManaged"/> is reported.
    /// </summary>
    [Fact]
    public async Task ProviderContract_PlatformMultipartStateStores_TrackUploadLifecycle()
    {
        if (!ContractOptions.SupportsPlatformMultipartStateStore) {
            return;
        }

        await using var fixture = await CreateInitializedFixtureAsync(services => services.AddInMemoryStorageMultipartStateStore());
        var storage = fixture.Backend;
        var capabilities = await storage.GetCapabilitiesAsync();

        if (!Supports(capabilities.MultipartUploads)) {
            return;
        }

        var stateStore = fixture.GetRequiredService<InMemoryMultipartStateStore>();

        RequireSuccess(await storage.CreateBucketAsync(new CreateBucketRequest
        {
            BucketName = "contract-platform-multipart"
        }));

        var initiatedUpload = RequireSuccess(await storage.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = "contract-platform-multipart",
            Key = "docs/assembled.txt",
            ContentType = "text/plain",
            Metadata = Supports(capabilities.ObjectMetadata)
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["source"] = "external-multipart"
                }
                : null
        }));

        var uploadState = await stateStore.GetMultipartUploadStateAsync(storage.Name, "contract-platform-multipart", "docs/assembled.txt", initiatedUpload.UploadId);
        Assert.NotNull(uploadState);
        Assert.Equal("text/plain", uploadState!.ContentType);
        if (Supports(capabilities.ObjectMetadata)) {
            Assert.Equal("external-multipart", uploadState.Metadata!["source"]);
        }

        var uploads = await storage.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
        {
            BucketName = "contract-platform-multipart"
        }).ToArrayAsync();
        var listedUpload = Assert.Single(uploads);
        Assert.Equal(initiatedUpload.UploadId, listedUpload.UploadId);

        var part1 = RequireSuccess(await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "contract-platform-multipart",
            Key = "docs/assembled.txt",
            UploadId = initiatedUpload.UploadId,
            PartNumber = 1,
            Content = CreateUtf8Stream("hello ")
        }));

        var part2 = RequireSuccess(await storage.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = "contract-platform-multipart",
            Key = "docs/assembled.txt",
            UploadId = initiatedUpload.UploadId,
            PartNumber = 2,
            Content = CreateUtf8Stream("world")
        }));

        var completedObject = RequireSuccess(await storage.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = "contract-platform-multipart",
            Key = "docs/assembled.txt",
            UploadId = initiatedUpload.UploadId,
            Parts = [part1, part2]
        }));
        Assert.Equal("text/plain", completedObject.ContentType);
        if (Supports(capabilities.ObjectMetadata)) {
            Assert.Equal("external-multipart", completedObject.Metadata!["source"]);
        }

        var removedState = await stateStore.GetMultipartUploadStateAsync(storage.Name, "contract-platform-multipart", "docs/assembled.txt", initiatedUpload.UploadId);
        Assert.Null(removedState);

        var supportState = await storage.GetSupportStateDescriptorAsync();
        Assert.Equal(StorageSupportStateOwnership.PlatformManaged, supportState.MultipartState);
    }

    private async Task<StorageProviderContractFixture> CreateInitializedFixtureAsync(Action<IServiceCollection>? configureServices = null)
    {
        var fixture = CreateFixture();

        if (fixture.IsInitialized) {
            if (configureServices is not null) {
                await fixture.RestartAsync(configureServices);
            }
        }
        else {
            await fixture.InitializeAsync(configureServices);
        }

        return fixture;
    }

    private static bool Supports(StorageCapabilitySupport support) => support != StorageCapabilitySupport.Unsupported;

    private string GetPrimaryChecksumAlgorithm()
    {
        return GetConfiguredChecksumAlgorithms(maxCount: 1)[0];
    }

    private IReadOnlyList<string> GetConfiguredChecksumAlgorithms(int maxCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxCount);

        var algorithms = new List<string>(maxCount);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var configuredAlgorithm in ContractOptions.SupportedChecksumAlgorithms) {
            if (string.IsNullOrWhiteSpace(configuredAlgorithm)) {
                continue;
            }

            var normalizedAlgorithm = NormalizeChecksumAlgorithm(configuredAlgorithm);
            if (seen.Add(normalizedAlgorithm)) {
                algorithms.Add(normalizedAlgorithm);
                if (algorithms.Count == maxCount) {
                    break;
                }
            }
        }

        if (algorithms.Count == 0) {
            algorithms.Add("sha256");
        }

        return algorithms;
    }

    private static IReadOnlyDictionary<string, string> CreateChecksums(string payload, IEnumerable<string> algorithms)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(algorithms);

        var checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var algorithm in algorithms) {
            var normalizedAlgorithm = NormalizeChecksumAlgorithm(algorithm);
            checksums[normalizedAlgorithm] = ComputeChecksum(payload, normalizedAlgorithm);
        }

        return checksums;
    }

    private static string ComputeChecksum(string payload, string algorithm)
    {
        return NormalizeChecksumAlgorithm(algorithm) switch
        {
            "sha256" => ChecksumTestAlgorithms.ComputeSha256Base64(payload),
            "sha1" => ChecksumTestAlgorithms.ComputeSha1Base64(payload),
            "crc32c" => ChecksumTestAlgorithms.ComputeCrc32cBase64(payload),
            _ => throw new InvalidOperationException($"The contract harness cannot compute checksums for algorithm '{algorithm}'.")
        };
    }

    private static string NormalizeChecksumAlgorithm(string algorithm)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithm);
        return algorithm.Trim().ToLowerInvariant();
    }

    private static string QuoteETag(string? etag)
    {
        return $"\"{Assert.IsType<string>(etag)}\"";
    }

    private static void AssertChecksumEquals(IReadOnlyDictionary<string, string>? checksums, string algorithm, string expectedValue)
    {
        Assert.NotNull(checksums);
        Assert.True(TryGetChecksumValue(checksums!, algorithm, out var actualValue), $"Expected checksum '{algorithm}' to be present.");
        Assert.Equal(expectedValue, actualValue);
    }

    private static bool TryGetChecksumValue(IReadOnlyDictionary<string, string> checksums, string algorithm, out string actualValue)
    {
        ArgumentNullException.ThrowIfNull(checksums);
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithm);

        if (checksums.TryGetValue(algorithm, out actualValue!)) {
            return true;
        }

        foreach (var checksum in checksums) {
            if (string.Equals(checksum.Key, algorithm, StringComparison.OrdinalIgnoreCase)) {
                actualValue = checksum.Value;
                return true;
            }
        }

        actualValue = string.Empty;
        return false;
    }

    private static MemoryStream CreateUtf8Stream(string content)
    {
        return new MemoryStream(Encoding.UTF8.GetBytes(content));
    }

    private static async Task<string> ReadUtf8Async(GetObjectResponse response)
    {
        using var reader = new StreamReader(response.Content, Encoding.UTF8, leaveOpen: false);
        return await reader.ReadToEndAsync();
    }

    private static T RequireSuccess<T>(StorageResult<T> result)
    {
        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Value);
        return result.Value!;
    }

    private static void RequireSuccess(StorageResult result)
    {
        Assert.True(result.IsSuccess);
    }

    private static StorageError RequireFailure(StorageResult result, StorageErrorCode expectedCode)
    {
        Assert.False(result.IsSuccess);
        var error = Assert.IsType<StorageError>(result.Error);
        Assert.Equal(expectedCode, error.Code);
        return error;
    }

    private static StorageError RequireFailure<T>(StorageResult<T> result, StorageErrorCode expectedCode)
    {
        return RequireFailure((StorageResult)result, expectedCode);
    }
}
