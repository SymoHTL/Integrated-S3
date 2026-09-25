using System.Runtime.CompilerServices;
using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Engine.Blobs;
using IntegratedS3.Engine.Metadata;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Engine;

/// <summary>
/// The engine as an <see cref="IStorageBackend"/>: S3 semantics over the metadata store and a write-once blob store.
/// Every change takes effect when one metadata transaction commits; blobs are written before it and deleted only
/// by garbage collection after it.
/// </summary>
internal sealed partial class EngineStorageBackend : IStorageBackend, IAsyncDisposable
{
    private const string DefaultObjectContentType = "binary/octet-stream";

    private readonly IntegratedS3EngineOptions _options;
    private readonly IBlobStore _blobs;
    private readonly ILogger? _logger;
    private readonly Lazy<Task<SqliteMetadataStore>> _store;
    private readonly EngineMaintenance _maintenance;

    public EngineStorageBackend(IntegratedS3EngineOptions options, IBlobStore blobs, ILogger? logger = null, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(blobs);
        ArgumentOutOfRangeException.ThrowIfNegative(options.InlineThresholdBytes);

        _options = options;
        _blobs = blobs;
        _logger = logger;
        _store = new Lazy<Task<SqliteMetadataStore>>(() => SqliteMetadataStore.OpenAsync(options.SqliteDatabasePath, timeProvider ?? TimeProvider.System));
        _maintenance = new EngineMaintenance(this, logger);
    }

    public string Name => _options.ProviderName;

    public string Kind => "engine";

    public bool IsPrimary => _options.IsPrimary;

    public string? Description => "IntegratedS3 engine (preview): SQLite metadata and write-once blobs.";

    internal IBlobStore Blobs => _blobs;

    internal IntegratedS3EngineOptions Options => _options;

    internal EngineMaintenance Maintenance => _maintenance;

    public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(new StorageCapabilities
        {
            BucketOperations = StorageCapabilitySupport.Native,
            ObjectCrud = StorageCapabilitySupport.Native,
            ObjectMetadata = StorageCapabilitySupport.Native,
            ListObjects = StorageCapabilitySupport.Native,
            Pagination = StorageCapabilitySupport.Native,
            RangeRequests = StorageCapabilitySupport.Native,
            ConditionalRequests = StorageCapabilitySupport.Native,
            MultipartUploads = StorageCapabilitySupport.Native,
            CopyOperations = StorageCapabilitySupport.Native,
            PresignedUrls = StorageCapabilitySupport.Unsupported,
            ObjectTags = StorageCapabilitySupport.Native,
            Versioning = StorageCapabilitySupport.Native,
            BatchDelete = StorageCapabilitySupport.Unsupported,
            AccessControl = StorageCapabilitySupport.Unsupported,
            Cors = StorageCapabilitySupport.Unsupported,
            ObjectLock = StorageCapabilitySupport.Unsupported,
            ServerSideEncryption = StorageCapabilitySupport.Unsupported,
            Checksums = StorageCapabilitySupport.Native,
            XmlErrors = StorageCapabilitySupport.Unsupported,
            PathStyleAddressing = StorageCapabilitySupport.Unsupported,
            VirtualHostedStyleAddressing = StorageCapabilitySupport.Unsupported
        });
    }

    public ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(new StorageSupportStateDescriptor
        {
            ObjectMetadata = StorageSupportStateOwnership.BackendOwned,
            ObjectTags = StorageSupportStateOwnership.BackendOwned,
            MultipartState = StorageSupportStateOwnership.BackendOwned,
            Versioning = StorageSupportStateOwnership.BackendOwned,
            Checksums = StorageSupportStateOwnership.BackendOwned,
            AccessControl = StorageSupportStateOwnership.NotApplicable,
            Retention = StorageSupportStateOwnership.NotApplicable,
            ServerSideEncryption = StorageSupportStateOwnership.NotApplicable,
            RedirectLocations = StorageSupportStateOwnership.NotApplicable
        });
    }

    public ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(StorageProviderMode.Managed);
    }

    public ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(new StorageObjectLocationDescriptor
        {
            SupportedAccessModes = [StorageObjectAccessMode.ProxyStream]
        });
    }

    public async ValueTask DisposeAsync()
    {
        await _maintenance.DisposeAsync();
        if (_store.IsValueCreated && _store.Value.IsCompletedSuccessfully) {
            await _store.Value.Result.DisposeAsync();
        }
    }

    // ----- Buckets -----

    public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<BucketRow> buckets;
        await using (var transaction = await BeginAsync(write: false, cancellationToken)) {
            buckets = await transaction.ListBucketsAsync(cancellationToken);
        }

        foreach (var bucket in buckets) {
            yield return ToBucketInfo(bucket);
        }
    }

    public async ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var transaction = await BeginAsync(write: true, cancellationToken);
        var now = await transaction.GetClockAsync(cancellationToken);
        var bucket = await transaction.InsertBucketAsync(
            request.BucketName,
            request.EnableVersioning ? StoredVersioning.Enabled : StoredVersioning.Disabled,
            objectLockEnabled: false,
            now,
            cancellationToken);
        if (bucket is null) {
            // One tenant: an existing name is always the caller's own bucket.
            return StorageResult<BucketInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.BucketAlreadyOwnedByYou,
                Message = $"Your previous request to create the named bucket '{request.BucketName}' succeeded and you already own it.",
                BucketName = request.BucketName,
                ProviderName = Name
            });
        }

        await transaction.CommitAsync();
        return StorageResult<BucketInfo>.Success(ToBucketInfo(bucket));
    }

    public async ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        await using var transaction = await BeginAsync(write: false, cancellationToken);
        var bucket = await transaction.GetBucketAsync(bucketName, cancellationToken);
        return bucket is null
            ? StorageResult<BucketInfo>.Failure(BucketNotFound(bucketName))
            : StorageResult<BucketInfo>.Success(ToBucketInfo(bucket));
    }

    public async ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        await using var transaction = await BeginAsync(write: false, cancellationToken);
        var bucket = await transaction.GetBucketAsync(bucketName, cancellationToken);
        return bucket is null
            ? StorageResult<BucketLocationInfo>.Failure(BucketNotFound(bucketName))
            : StorageResult<BucketLocationInfo>.Success(new BucketLocationInfo { BucketName = bucketName });
    }

    /// <summary>
    /// Deletes a bucket that holds no version and no delete marker. In-progress multipart uploads do not block it,
    /// as on an AWS general purpose bucket: they go with the bucket, and their parts' blobs are queued for garbage
    /// collection.
    /// </summary>
    public async ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var transaction = await BeginAsync(write: true, cancellationToken);
        await transaction.LockBucketAsync(request.BucketName, exclusive: true, cancellationToken);
        var bucket = await transaction.GetBucketAsync(request.BucketName, cancellationToken);
        if (bucket is null) {
            return StorageResult.Failure(BucketNotFound(request.BucketName));
        }

        if (await transaction.BucketHasVersionsAsync(bucket.Id, cancellationToken)) {
            return StorageResult.Failure(new StorageError
            {
                Code = StorageErrorCode.BucketNotEmpty,
                Message = $"Bucket '{request.BucketName}' must be empty before it can be deleted.",
                BucketName = request.BucketName,
                ProviderName = Name,
                SuggestedHttpStatusCode = 409
            });
        }

        var now = await transaction.GetClockAsync(cancellationToken);
        var uploadBlobs = await transaction.DeleteBucketAsync(bucket.Id, cancellationToken);
        await transaction.EnqueueGarbageAsync(uploadBlobs, GarbageNotBefore(now), cancellationToken);
        await transaction.CommitAsync();
        return StorageResult.Success();
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        await using var transaction = await BeginAsync(write: false, cancellationToken);
        var bucket = await transaction.GetBucketAsync(bucketName, cancellationToken);
        return bucket is null
            ? StorageResult<BucketVersioningInfo>.Failure(BucketNotFound(bucketName))
            : StorageResult<BucketVersioningInfo>.Success(new BucketVersioningInfo
            {
                BucketName = bucketName,
                Status = ToVersioningStatus(bucket.Versioning)
            });
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var target = request.Status switch
        {
            BucketVersioningStatus.Enabled => StoredVersioning.Enabled,
            BucketVersioningStatus.Suspended => StoredVersioning.Suspended,
            _ => (StoredVersioning?)null
        };
        if (target is null) {
            return StorageResult<BucketVersioningInfo>.Failure(new StorageError
            {
                Code = StorageErrorCode.InvalidArgument,
                Message = "Versioning can be set to Enabled or Suspended; a bucket never returns to unversioned.",
                BucketName = request.BucketName,
                ProviderName = Name,
                SuggestedHttpStatusCode = 400
            });
        }

        await using var transaction = await BeginAsync(write: true, cancellationToken);
        await transaction.LockBucketAsync(request.BucketName, exclusive: true, cancellationToken);
        var bucket = await transaction.GetBucketAsync(request.BucketName, cancellationToken);
        if (bucket is null) {
            return StorageResult<BucketVersioningInfo>.Failure(BucketNotFound(request.BucketName));
        }

        await transaction.SetBucketVersioningAsync(bucket.Id, target.Value, cancellationToken);
        await transaction.CommitAsync();
        return StorageResult<BucketVersioningInfo>.Success(new BucketVersioningInfo
        {
            BucketName = request.BucketName,
            Status = request.Status
        });
    }

    // ----- Shared plumbing -----

    internal async ValueTask<MetadataTransaction> BeginAsync(bool write, CancellationToken cancellationToken)
    {
        var store = await _store.Value.WaitAsync(cancellationToken);
        _maintenance.EnsureStarted();
        return await store.BeginAsync(write, cancellationToken);
    }

    internal long GarbageNotBefore(long nowUs)
        => nowUs + (long)_options.GarbageCollectionDelay.TotalMicroseconds;

    private static BucketInfo ToBucketInfo(BucketRow bucket) => new()
    {
        Name = bucket.Name,
        CreatedAtUtc = EngineClock.FromMicroseconds(bucket.CreatedUs),
        VersioningEnabled = bucket.Versioning == StoredVersioning.Enabled
    };

    private static BucketVersioningStatus ToVersioningStatus(StoredVersioning versioning) => versioning switch
    {
        StoredVersioning.Enabled => BucketVersioningStatus.Enabled,
        StoredVersioning.Suspended => BucketVersioningStatus.Suspended,
        _ => BucketVersioningStatus.Disabled
    };

    private StorageError BucketNotFound(string bucketName) => new()
    {
        Code = StorageErrorCode.BucketNotFound,
        Message = $"Bucket '{bucketName}' was not found.",
        BucketName = bucketName,
        ProviderName = Name,
        SuggestedHttpStatusCode = 404
    };

    private StorageError ObjectNotFound(string bucketName, string key, string? versionId = null) => new()
    {
        Code = StorageErrorCode.ObjectNotFound,
        Message = string.IsNullOrWhiteSpace(versionId)
            ? $"Object '{key}' was not found in bucket '{bucketName}'."
            : $"Object '{key}' with version '{versionId}' was not found in bucket '{bucketName}'.",
        BucketName = bucketName,
        ObjectKey = key,
        ProviderName = Name,
        SuggestedHttpStatusCode = 404
    };

    /// <summary>
    /// The error for reading a delete marker: 404 with the marker's headers for the current version, 405 when the
    /// marker was asked for by its version id.
    /// </summary>
    private StorageError DeleteMarkerError(string bucketName, string key, string? requestedVersionId, VersionRow marker)
        => string.IsNullOrWhiteSpace(requestedVersionId)
            ? new StorageError
            {
                Code = StorageErrorCode.ObjectNotFound,
                Message = $"Object '{key}' was not found in bucket '{bucketName}'.",
                BucketName = bucketName,
                ObjectKey = key,
                VersionId = ToApiVersionId(marker.VersionId),
                IsDeleteMarker = true,
                ProviderName = Name,
                SuggestedHttpStatusCode = 404
            }
            : new StorageError
            {
                Code = StorageErrorCode.MethodNotAllowed,
                Message = $"The specified version '{requestedVersionId}' is a delete marker and does not support this operation.",
                BucketName = bucketName,
                ObjectKey = key,
                VersionId = ToApiVersionId(marker.VersionId),
                IsDeleteMarker = true,
                LastModifiedUtc = EngineClock.FromMicroseconds(marker.LastModifiedUs),
                ProviderName = Name,
                SuggestedHttpStatusCode = 405
            };

    private StorageError Error(StorageErrorCode code, string message, string bucketName, string? key, int status) => new()
    {
        Code = code,
        Message = message,
        BucketName = bucketName,
        ObjectKey = key,
        ProviderName = Name,
        SuggestedHttpStatusCode = status
    };

    private StorageError Throttled(string bucketName, string? key) => Error(
        StorageErrorCode.Throttled,
        "The blob store asked the engine to slow down. Retry the request.",
        bucketName,
        key,
        503);

    private StorageError BlobMissing(string bucketName, string key, string locator) => Error(
        StorageErrorCode.Unknown,
        $"The data of object '{key}' is missing from the blob store (blob '{locator}').",
        bucketName,
        key,
        500);

    private StorageError BlobClaimed(string bucketName, string? key) => Error(
        StorageErrorCode.Throttled,
        "The upload took longer than the engine keeps an unreferenced blob, and its data was reclaimed. Upload it again.",
        bucketName,
        key,
        503);

    /// <summary>
    /// The API's version id: <see langword="null"/> for the null version, as the other providers report it.
    /// </summary>
    private static string? ToApiVersionId(string storedVersionId)
        => storedVersionId == VersionRow.NullVersionId ? null : storedVersionId;

    /// <summary>
    /// The stored version id for a requested one: the literal <c>null</c> addresses the null version.
    /// </summary>
    private static string ToStoredVersionId(string requestedVersionId) => requestedVersionId;

    private static bool TryEncodeKey(string key, out byte[] encoded)
    {
        try {
            encoded = KeyRange.Encode(key);
            return true;
        }
        catch (ArgumentException) {
            encoded = [];
            return false;
        }
    }

    private StorageError InvalidKey(string bucketName, string key) => Error(
        StorageErrorCode.InvalidArgument,
        "The object key is not valid Unicode.",
        bucketName,
        key,
        400);
}
