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
    protected virtual StorageProviderContractTestOptions ContractOptions => new();

    protected abstract StorageProviderContractFixture CreateFixture();

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
        var expectedChecksum = ChecksumTestAlgorithms.ComputeSha256Base64(payload);
        IReadOnlyDictionary<string, string>? metadata = Supports(capabilities.ObjectMetadata)
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["author"] = "copilot"
            }
            : null;
        IReadOnlyDictionary<string, string>? checksums = Supports(capabilities.Checksums)
            ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = expectedChecksum
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
            Assert.Equal(expectedChecksum, putObject.Checksums!["sha256"]);
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
            Assert.Equal(expectedChecksum, headObject.Checksums!["sha256"]);
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
                Assert.Equal(expectedChecksum, getObject.Object.Checksums!["sha256"]);
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

        var putResult = await storage.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "contract-invalid-checksums",
            Key = "docs/object.txt",
            Content = CreateUtf8Stream("checksum payload"),
            ContentType = "text/plain",
            Checksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["sha256"] = "invalid-checksum"
            }
        });

        RequireFailure(putResult, StorageErrorCode.InvalidChecksum);

        RequireFailure(await storage.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = "contract-invalid-checksums",
            Key = "docs/object.txt"
        }), StorageErrorCode.ObjectNotFound);
    }

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
        var expectedChecksum = ChecksumTestAlgorithms.ComputeSha256Base64(payload);

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
                    ["sha256"] = expectedChecksum
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
            Assert.Equal(expectedChecksum, storedState!.Checksums!["sha256"]);
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
                Assert.Equal(expectedChecksum, getObject.Object.Checksums!["sha256"]);
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
