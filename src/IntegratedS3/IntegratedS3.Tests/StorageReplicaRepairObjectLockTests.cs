using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression tests for issue #108: a retention / legal-hold (Object Lock) divergence must be
/// repaired by re-applying the lock state to the replica (PutObjectRetention / PutObjectLegalHold),
/// not by re-PUTting the object body. The old code routed both operations to a body-only re-PUT,
/// so the WORM state was never written to the replica while the repair reported success.
/// </summary>
public sealed class StorageReplicaRepairObjectLockTests
{
    private const string PrimaryName = "primary";
    private const string ReplicaName = "replica";
    private const string Bucket = "compliance-bucket";
    private const string Key = "locked-object";

    [Fact]
    public async Task RepairAsync_RetentionDivergence_ReAppliesRetentionOnReplica_WithoutBodyRePut()
    {
        var retainUntil = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var primary = new RecordingLockBackend(PrimaryName, isPrimary: true)
        {
            Retention = new ObjectRetentionInfo
            {
                BucketName = Bucket,
                Key = Key,
                Mode = ObjectRetentionMode.Compliance,
                RetainUntilDateUtc = retainUntil
            }
        };
        var replica = new RecordingLockBackend(ReplicaName);

        var service = CreateService(primary, replica);

        var error = await service.RepairAsync(CreateEntry(StorageOperationType.PutObjectRetention));

        Assert.Null(error);

        // The lock state must have been re-applied on the replica…
        Assert.NotNull(replica.LastRetentionApplied);
        Assert.Equal(ObjectRetentionMode.Compliance, replica.LastRetentionApplied!.Mode);
        Assert.Equal(retainUntil, replica.LastRetentionApplied.RetainUntilDateUtc);

        // …and the object body must NOT have been re-PUT to the replica.
        Assert.Equal(0, replica.PutObjectCallCount);
        Assert.Equal(0, primary.GetObjectCallCount);
    }

    [Fact]
    public async Task RepairAsync_LegalHoldDivergence_ReAppliesLegalHoldOnReplica_WithoutBodyRePut()
    {
        var primary = new RecordingLockBackend(PrimaryName, isPrimary: true)
        {
            LegalHold = new ObjectLegalHoldInfo
            {
                BucketName = Bucket,
                Key = Key,
                Status = ObjectLegalHoldStatus.On
            }
        };
        var replica = new RecordingLockBackend(ReplicaName);

        var service = CreateService(primary, replica);

        var error = await service.RepairAsync(CreateEntry(StorageOperationType.PutObjectLegalHold));

        Assert.Null(error);

        Assert.NotNull(replica.LastLegalHoldApplied);
        Assert.Equal(ObjectLegalHoldStatus.On, replica.LastLegalHoldApplied!.Status);

        Assert.Equal(0, replica.PutObjectCallCount);
        Assert.Equal(0, primary.GetObjectCallCount);
    }

    private static StorageReplicaRepairService CreateService(RecordingLockBackend primary, RecordingLockBackend replica)
    {
        var options = Options.Create(new IntegratedS3CoreOptions());
        var evaluator = new AlwaysHealthyEvaluator();
        var probe = new AlwaysHealthyProbe();
        var monitor = new StorageBackendHealthMonitor(
            evaluator,
            probe,
            options,
            TimeProvider.System,
            NullLogger<StorageBackendHealthMonitor>.Instance);

        return new StorageReplicaRepairService(new IStorageBackend[] { primary, replica }, new NoOpCatalogStore(), monitor);
    }

    private static StorageReplicaRepairEntry CreateEntry(StorageOperationType operation) => new()
    {
        Id = "repair-108",
        Origin = StorageReplicaRepairOrigin.AsyncReplication,
        Status = StorageReplicaRepairStatus.Pending,
        Operation = operation,
        PrimaryBackendName = PrimaryName,
        ReplicaBackendName = ReplicaName,
        BucketName = Bucket,
        ObjectKey = Key,
        CreatedAtUtc = DateTimeOffset.UnixEpoch,
        UpdatedAtUtc = DateTimeOffset.UnixEpoch
    };

    private sealed class AlwaysHealthyEvaluator : IStorageBackendHealthEvaluator
    {
        public ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(StorageBackendHealthStatus.Healthy);
    }

    private sealed class AlwaysHealthyProbe : IStorageBackendHealthProbe
    {
        public ValueTask<StorageBackendHealthStatus> ProbeAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(StorageBackendHealthStatus.Healthy);
    }

    private sealed class NoOpCatalogStore : IStorageCatalogStore
    {
        public ValueTask UpsertBucketAsync(string providerName, BucketInfo bucket, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

        public ValueTask RemoveBucketAsync(string providerName, string bucketName, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

        public ValueTask<IReadOnlyList<StoredBucketEntry>> ListBucketsAsync(string? providerName = null, CancellationToken cancellationToken = default)
            => ValueTask.FromResult<IReadOnlyList<StoredBucketEntry>>([]);

        public ValueTask UpsertObjectAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

        public ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

        public ValueTask RecordReplicaVersionMappingAsync(string replicaProviderName, string bucketName, string key, string primaryVersionId, string replicaVersionId, CancellationToken cancellationToken = default)
            => ValueTask.CompletedTask;

        public ValueTask<string?> GetReplicaVersionIdForPrimaryAsync(string replicaProviderName, string bucketName, string key, string primaryVersionId, CancellationToken cancellationToken = default)
            => ValueTask.FromResult<string?>(null);

        public ValueTask<StoredObjectEntry?> GetObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
            => ValueTask.FromResult<StoredObjectEntry?>(null);

        public ValueTask<IReadOnlyList<StoredObjectEntry>> ListObjectsAsync(string? providerName = null, string? bucketName = null, string? keyPrefix = null, CancellationToken cancellationToken = default)
            => ValueTask.FromResult<IReadOnlyList<StoredObjectEntry>>([]);
    }

    /// <summary>
    /// A backend that records Object Lock re-applications and any object body re-PUT / body fetch,
    /// so a test can distinguish a lock repair from a wasteful body re-PUT.
    /// </summary>
    private sealed class RecordingLockBackend(string name, bool isPrimary = false) : IStorageBackend
    {
        public string Name => name;

        public string Kind => "test-lock";

        public bool IsPrimary => isPrimary;

        public string? Description => $"Recording lock backend '{name}'.";

        public ObjectRetentionInfo? Retention { get; set; }

        public ObjectLegalHoldInfo? LegalHold { get; set; }

        public PutObjectRetentionRequest? LastRetentionApplied { get; private set; }

        public PutObjectLegalHoldRequest? LastLegalHoldApplied { get; private set; }

        public int PutObjectCallCount { get; private set; }

        public int GetObjectCallCount { get; private set; }

        public ValueTask<StorageResult<ObjectRetentionInfo>> GetObjectRetentionAsync(GetObjectRetentionRequest request, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(Retention is null
                ? StorageResult<ObjectRetentionInfo>.Failure(new StorageError { Code = StorageErrorCode.ObjectNotFound, Message = "no retention", BucketName = request.BucketName, ObjectKey = request.Key })
                : StorageResult<ObjectRetentionInfo>.Success(Retention));

        public ValueTask<StorageResult<ObjectLegalHoldInfo>> GetObjectLegalHoldAsync(GetObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(LegalHold is null
                ? StorageResult<ObjectLegalHoldInfo>.Failure(new StorageError { Code = StorageErrorCode.ObjectNotFound, Message = "no legal hold", BucketName = request.BucketName, ObjectKey = request.Key })
                : StorageResult<ObjectLegalHoldInfo>.Success(LegalHold));

        public ValueTask<StorageResult<ObjectRetentionInfo>> PutObjectRetentionAsync(PutObjectRetentionRequest request, CancellationToken cancellationToken = default)
        {
            LastRetentionApplied = request;
            return ValueTask.FromResult(StorageResult<ObjectRetentionInfo>.Success(new ObjectRetentionInfo
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                Mode = request.Mode,
                RetainUntilDateUtc = request.RetainUntilDateUtc
            }));
        }

        public ValueTask<StorageResult<ObjectLegalHoldInfo>> PutObjectLegalHoldAsync(PutObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
        {
            LastLegalHoldApplied = request;
            return ValueTask.FromResult(StorageResult<ObjectLegalHoldInfo>.Success(new ObjectLegalHoldInfo
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId,
                Status = request.Status
            }));
        }

        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
        {
            GetObjectCallCount++;
            var response = new GetObjectResponse
            {
                Object = new ObjectInfo { BucketName = request.BucketName, Key = request.Key },
                Content = new MemoryStream([])
            };
            return ValueTask.FromResult(StorageResult<GetObjectResponse>.Success(response));
        }

        public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
        {
            PutObjectCallCount++;
            return ValueTask.FromResult(StorageResult<ObjectInfo>.Success(new ObjectInfo { BucketName = request.BucketName, Key = request.Key }));
        }

        // ── Unused members: this test only drives the Object Lock repair path. ──

        public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default) => throw NotUsed();

        public IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default) => throw NotUsed();

        private static NotSupportedException NotUsed() => new("Member not exercised by the Object Lock repair test.");
    }
}
