using System.Diagnostics;
using System.Runtime.CompilerServices;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Core.Services;

internal sealed class OrchestratedStorageService(
    IEnumerable<IStorageBackend> backends,
    IStorageCatalogStore catalogStore,
    IOptions<IntegratedS3CoreOptions> options,
    StorageBackendHealthMonitor backendHealthMonitor,
    IStorageReplicaRepairBacklog replicaRepairBacklog,
    IStorageReplicaRepairDispatcher replicaRepairDispatcher,
    IStorageReplicaRepairService replicaRepairService,
    TimeProvider timeProvider,
    ILogger<OrchestratedStorageService> logger) : IStorageService
{
    private readonly IStorageBackend[] _backends = backends.ToArray();
    private readonly Lazy<IStorageBackend> _primaryBackend = new(() => ResolvePrimaryBackend(backends.ToArray()));

    public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var backend = await SelectReadBackendAsync(cancellationToken);
        await foreach (var bucket in backend.ListBucketsAsync(cancellationToken).WithCancellation(cancellationToken)) {
            await catalogStore.UpsertBucketAsync(backend.Name, bucket, cancellationToken);
            yield return bucket;
        }
    }

    public async ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketInfo>.Failure(strictReplicationError);
        }

        var result = await backend.CreateBucketAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            await catalogStore.UpsertBucketAsync(backend.Name, result.Value, cancellationToken);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.CreateBucket,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaBucketCreateAsync(replicaBackend, request, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketInfo>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketLocation,
            (backend, ct) => backend.GetBucketLocationAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketVersioning,
            (backend, ct) => backend.GetBucketVersioningAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketVersioningInfo>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketVersioningAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            await RefreshCatalogBucketAsync(backend, request.BucketName, cancellationToken);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketVersioning,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaBucketVersioningAsync(replicaBackend, request, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketVersioningInfo>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketCors,
            (backend, ct) => backend.GetBucketCorsAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketCorsConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketCorsAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketCors,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketCorsAsync(replicaBackend, new PutBucketCorsRequest
                {
                    BucketName = request.BucketName,
                    Rules = CloneCorsRules(request.Rules)
                }, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketCorsConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketCorsAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketCors,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketCorsAsync(replicaBackend, request, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.HeadBucket,
            (backend, ct) => backend.HeadBucketAsync(bucketName, ct),
            async (backend, result, ct) => {
                if (result.Value is not null) {
                    await catalogStore.UpsertBucketAsync(backend.Name, result.Value, ct);
                }
            },
            cancellationToken);
    }

    public async ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            await catalogStore.RemoveBucketAsync(backend.Name, request.BucketName, cancellationToken);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucket,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaBucketDeleteAsync(replicaBackend, request, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var backend = await SelectReadBackendAsync(cancellationToken);
        await foreach (var @object in backend.ListObjectsAsync(request, cancellationToken).WithCancellation(cancellationToken)) {
            await catalogStore.UpsertObjectAsync(backend.Name, @object, cancellationToken);
            yield return @object;
        }
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var backend = await SelectReadBackendAsync(cancellationToken);
        await foreach (var @object in backend.ListObjectVersionsAsync(request, cancellationToken).WithCancellation(cancellationToken)) {
            await catalogStore.UpsertObjectAsync(backend.Name, @object, cancellationToken);
            yield return @object;
        }
    }

    public async IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var backend = await SelectReadBackendAsync(cancellationToken);
        await foreach (var upload in backend.ListMultipartUploadsAsync(request, cancellationToken).WithCancellation(cancellationToken)) {
            yield return upload;
        }
    }

    public async IAsyncEnumerable<MultipartUploadPart> ListMultipartUploadPartsAsync(ListMultipartUploadPartsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var backend = await SelectReadBackendAsync(cancellationToken);
        await foreach (var part in backend.ListMultipartUploadPartsAsync(request, cancellationToken).WithCancellation(cancellationToken)) {
            yield return part;
        }
    }

    public async ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetObject,
            (backend, ct) => backend.GetObjectAsync(request, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetObjectTags,
            (backend, ct) => backend.GetObjectTagsAsync(request, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.DestinationBucketName, request.DestinationKey, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<ObjectInfo>.Failure(strictReplicationError);
        }

        var result = await backend.CopyObjectAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            await catalogStore.UpsertObjectAsync(backend.Name, result.Value, cancellationToken);

            var wasCopiedToDestination = string.Equals(result.Value.BucketName, request.DestinationBucketName, StringComparison.Ordinal)
                && string.Equals(result.Value.Key, request.DestinationKey, StringComparison.Ordinal);

            if (wasCopiedToDestination) {
                StorageError? replicationError = options.Value.ConsistencyMode switch
                {
                    StorageConsistencyMode.WriteThroughAll => await ReplicateCopyObjectWriteThroughAsync(request, backend, result.Value.VersionId, CancellationToken.None),
                    StorageConsistencyMode.WriteToPrimaryAsyncReplicas => await DispatchAsyncReplicationAsync(
                        StorageOperationType.CopyObject,
                        backend,
                        GetReplicaBackends(backend),
                        request.DestinationBucketName,
                        request.DestinationKey,
                        result.Value.VersionId,
                        CancellationToken.None),
                    _ => null
                };

                if (replicationError is not null) {
                    return StorageResult<ObjectInfo>.Failure(replicationError);
                }
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, request.Key, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<ObjectInfo>.Failure(strictReplicationError);
        }

        StorageResult<ObjectInfo> result;
        if (UsesSynchronousReplicaWrites(backend)) {
            var tempFilePath = await BufferStreamToTempFileAsync(request.Content, cancellationToken);
            try {
                result = await PutBufferedObjectAsync(backend, request, tempFilePath, cancellationToken);
                ObserveResult(backend, result);
                if (!result.IsSuccess || result.Value is null) {
                    return result;
                }

                await catalogStore.UpsertObjectAsync(backend.Name, result.Value, cancellationToken);

                var replicationError = await ApplyReplicaWritePolicyAsync(
                    StorageOperationType.PutObject,
                    backend,
                    request.BucketName,
                    request.Key,
                    result.Value.VersionId,
                    writeThroughOperation: (replicaBackend, ct) => WriteReplicaBufferedObjectAsync(replicaBackend, request, tempFilePath, ct),
                    CancellationToken.None);
                if (replicationError is not null) {
                    return StorageResult<ObjectInfo>.Failure(replicationError);
                }

                return result;
            }
            finally {
                DeleteTempFileIfPresent(tempFilePath);
            }
        }

        result = await backend.PutObjectAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            await catalogStore.UpsertObjectAsync(backend.Name, result.Value, cancellationToken);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutObject,
                backend,
                request.BucketName,
                request.Key,
                result.Value.VersionId,
                writeThroughOperation: (replicaBackend, ct) => RepairReplicaObjectFromPrimaryAsync(backend, replicaBackend, request.BucketName, request.Key, result.Value.VersionId, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<ObjectInfo>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, request.Key, request.VersionId, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<ObjectTagSet>.Failure(strictReplicationError);
        }

        var result = await backend.PutObjectTagsAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var resolvedVersionId = GetEffectiveVersionId(request.VersionId, result.Value.VersionId);
            await RefreshCatalogObjectAsync(backend, request.BucketName, request.Key, resolvedVersionId, cancellationToken);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutObjectTags,
                backend,
                request.BucketName,
                request.Key,
                request.VersionId,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutObjectTagsAsync(replicaBackend, new PutObjectTagsRequest
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    VersionId = request.VersionId,
                    Tags = CloneTags(request.Tags) ?? new Dictionary<string, string>(StringComparer.Ordinal)
                }, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<ObjectTagSet>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, request.Key, request.VersionId, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<ObjectTagSet>.Failure(strictReplicationError);
        }

        var result = await backend.DeleteObjectTagsAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var resolvedVersionId = GetEffectiveVersionId(request.VersionId, result.Value.VersionId);
            await RefreshCatalogObjectAsync(backend, request.BucketName, request.Key, resolvedVersionId, cancellationToken);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteObjectTags,
                backend,
                request.BucketName,
                request.Key,
                request.VersionId,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteObjectTagsAsync(replicaBackend, new DeleteObjectTagsRequest
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    VersionId = request.VersionId
                }, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<ObjectTagSet>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult<MultipartUploadInfo>.Failure(replicationError);
        }

        var result = await backend.InitiateMultipartUploadAsync(request, cancellationToken);
        ObserveResult(backend, result);
        return result;
    }

    public async ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult<MultipartUploadPart>.Failure(replicationError);
        }

        var result = await backend.UploadMultipartPartAsync(request, cancellationToken);
        ObserveResult(backend, result);
        return result;
    }

    public async ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult<ObjectInfo>.Failure(replicationError);
        }

        var result = await backend.CompleteMultipartUploadAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            await catalogStore.UpsertObjectAsync(backend.Name, result.Value, cancellationToken);
        }

        return result;
    }

    public async ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult.Failure(replicationError);
        }

        var result = await backend.AbortMultipartUploadAsync(request, cancellationToken);
        ObserveResult(backend, result);
        return result;
    }

    public async ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.HeadObject,
            (backend, ct) => backend.HeadObjectAsync(request, ct),
            async (backend, result, ct) => {
                if (result.Value is not null) {
                    await catalogStore.UpsertObjectAsync(backend.Name, result.Value, ct);
                }
            },
            cancellationToken);
    }

    public async ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, request.Key, request.VersionId, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<DeleteObjectResult>.Failure(strictReplicationError);
        }

        var result = await backend.DeleteObjectAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            if (result.Value.CurrentObject is not null) {
                await catalogStore.UpsertObjectAsync(backend.Name, result.Value.CurrentObject, cancellationToken);
            }
            else if (!string.IsNullOrWhiteSpace(request.VersionId)) {
                await catalogStore.RemoveObjectAsync(backend.Name, request.BucketName, request.Key, request.VersionId, cancellationToken);
            }
            else {
                await catalogStore.RemoveObjectAsync(backend.Name, request.BucketName, request.Key, versionId: null, cancellationToken);
            }

            var repairVersionId = GetEffectiveVersionId(request.VersionId, result.Value.VersionId);
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteObject,
                backend,
                request.BucketName,
                request.Key,
                repairVersionId,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteObjectAsync(replicaBackend, request, ct),
                CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<DeleteObjectResult>.Failure(replicationError);
            }
        }

        return result;
    }

    private static IStorageBackend ResolvePrimaryBackend(IEnumerable<IStorageBackend> backends)
    {
        var resolvedBackends = backends.ToArray();
        return resolvedBackends.FirstOrDefault(static backend => backend.IsPrimary)
            ?? resolvedBackends.FirstOrDefault()
            ?? throw new InvalidOperationException("No storage backends have been registered.");
    }

    private IStorageBackend GetPrimaryBackend()
    {
        var backend = _primaryBackend.Value;
        SetProviderTags(backend);
        return backend;
    }

    private void SetProviderTags(IStorageBackend backend)
    {
        IntegratedS3CoreTelemetry.SetProvider(
            Activity.Current,
            backend.Name,
            backend.Kind,
            ReferenceEquals(backend, _primaryBackend.Value));
    }

    private async ValueTask<IStorageBackend> SelectReadBackendAsync(CancellationToken cancellationToken)
    {
        var backend = (await GetOrderedReadBackendsAsync(cancellationToken))[0];
        SetProviderTags(backend);
        return backend;
    }

    private async ValueTask<IReadOnlyList<IStorageBackend>> GetOrderedReadBackendsAsync(CancellationToken cancellationToken)
    {
        var primaryBackend = _primaryBackend.Value;
        if (_backends.Length <= 1 || options.Value.ReadRoutingMode == StorageReadRoutingMode.PrimaryOnly) {
            return [primaryBackend];
        }

        var allowReadsFromReplicasWithOutstandingRepairs = options.Value.Replication.AllowReadsFromReplicasWithOutstandingRepairs;
        var candidates = new List<ReadBackendCandidate>(_backends.Length);
        foreach (var backend in _backends) {
            var healthStatus = await backendHealthMonitor.GetStatusAsync(backend, cancellationToken);
            var hasOutstandingRepairs = !ReferenceEquals(backend, primaryBackend)
                && await replicaRepairBacklog.HasOutstandingRepairsAsync(backend.Name, cancellationToken);
            if (hasOutstandingRepairs && !allowReadsFromReplicasWithOutstandingRepairs) {
                logger.LogInformation(
                    "Skipping replica provider {Provider} for reads because outstanding repairs are pending.",
                    backend.Name);
                continue;
            }

            candidates.Add(new ReadBackendCandidate(backend, healthStatus, hasOutstandingRepairs));
        }

        if (candidates.Count == 0) {
            return [primaryBackend];
        }

        return candidates
            .OrderBy(candidate => GetReadPriority(candidate, primaryBackend, options.Value.ReadRoutingMode, allowReadsFromReplicasWithOutstandingRepairs))
            .ThenBy(candidate => GetOriginalIndex(candidate.Backend))
            .Select(candidate => candidate.Backend)
            .ToArray();
    }

    private async ValueTask<StorageResult<T>> ExecuteReadAsync<T>(
        StorageOperationType storageOperation,
        Func<IStorageBackend, CancellationToken, ValueTask<StorageResult<T>>> operation,
        Func<IStorageBackend, StorageResult<T>, CancellationToken, ValueTask>? onSuccess,
        CancellationToken cancellationToken)
    {
        StorageResult<T>? lastFailure = null;

        foreach (var backend in await GetOrderedReadBackendsAsync(cancellationToken)) {
            var result = await operation(backend, cancellationToken);
            if (result.IsSuccess) {
                backendHealthMonitor.ReportSuccess(backend);
                SetProviderTags(backend);
                if (onSuccess is not null) {
                    await onSuccess(backend, result, cancellationToken);
                }

                return result;
            }

            backendHealthMonitor.ReportFailure(backend, result.Error);
            lastFailure = result;
            if (!ShouldFailoverRead(result.Error)) {
                logger.LogWarning(
                    "Read operation {StorageOperation} failed on provider {Provider} with {ErrorCode}.",
                    storageOperation,
                    backend.Name,
                    result.Error?.Code);
                return result;
            }

            logger.LogWarning(
                "Read operation {StorageOperation} failed on provider {Provider} with {ErrorCode} and will fail over.",
                storageOperation,
                backend.Name,
                result.Error?.Code);
        }

        return lastFailure ?? StorageResult<T>.Failure(new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = "No storage backend could satisfy the read request.",
            ProviderName = _primaryBackend.Value.Name,
            SuggestedHttpStatusCode = 503
        });
    }

    private async ValueTask<StorageError?> GetStrictReplicaWritePreflightErrorAsync(
        IStorageBackend primaryBackend,
        string bucketName,
        string? objectKey,
        string? versionId,
        CancellationToken cancellationToken)
    {
        if (!UsesSynchronousReplicaWrites(primaryBackend)) {
            return null;
        }

        foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
            if (options.Value.Replication.RequireHealthyReplicasForWriteThrough) {
                var healthStatus = await backendHealthMonitor.GetStatusAsync(replicaBackend, cancellationToken);
                if (healthStatus == StorageBackendHealthStatus.Unhealthy) {
                    logger.LogWarning(
                        "Blocking strict write-through because replica provider {ReplicaProvider} is unhealthy.",
                        replicaBackend.Name);
                    return CreateUnhealthyReplicaWriteError(replicaBackend, bucketName, objectKey, versionId);
                }
            }

            if (!options.Value.Replication.RequireCurrentReplicasForWriteThrough) {
                continue;
            }

            var outstandingRepairs = await replicaRepairBacklog.ListOutstandingAsync(replicaBackend.Name, cancellationToken);
            if (outstandingRepairs.Count == 0) {
                continue;
            }

            if (outstandingRepairs.Any(entry => entry.Status == StorageReplicaRepairStatus.Failed)) {
                logger.LogWarning(
                    "Blocking strict write-through because replica provider {ReplicaProvider} has failed outstanding repairs.",
                    replicaBackend.Name);
                return CreateIncompleteReplicaWriteError(replicaBackend, bucketName, objectKey, versionId);
            }

            logger.LogWarning(
                "Blocking strict write-through because replica provider {ReplicaProvider} has pending outstanding repairs.",
                replicaBackend.Name);
            return CreateStaleReplicaWriteError(replicaBackend, bucketName, objectKey, versionId);
        }

        return null;
    }

    private async ValueTask<StorageError?> ApplyReplicaWritePolicyAsync(
        StorageOperationType operation,
        IStorageBackend primaryBackend,
        string bucketName,
        string? objectKey,
        string? versionId,
        Func<IStorageBackend, CancellationToken, ValueTask<StorageError?>> writeThroughOperation,
        CancellationToken cancellationToken)
    {
        var replicaBackends = GetReplicaBackends(primaryBackend);
        if (replicaBackends.Count == 0) {
            return null;
        }

        return options.Value.ConsistencyMode switch
        {
            StorageConsistencyMode.WriteThroughAll => await ExecuteWriteThroughReplicationAsync(
                operation,
                primaryBackend,
                replicaBackends,
                bucketName,
                objectKey,
                versionId,
                writeThroughOperation,
                cancellationToken),
            StorageConsistencyMode.WriteToPrimaryAsyncReplicas => await DispatchAsyncReplicationAsync(
                operation,
                primaryBackend,
                replicaBackends,
                bucketName,
                objectKey,
                versionId,
                cancellationToken),
            _ => null
        };
    }

    private async ValueTask<StorageError?> ExecuteWriteThroughReplicationAsync(
        StorageOperationType operation,
        IStorageBackend primaryBackend,
        IReadOnlyList<IStorageBackend> replicaBackends,
        string bucketName,
        string? objectKey,
        string? versionId,
        Func<IStorageBackend, CancellationToken, ValueTask<StorageError?>> replicaOperation,
        CancellationToken cancellationToken)
    {
        for (var index = 0; index < replicaBackends.Count; index++) {
            var replicaBackend = replicaBackends[index];
            var replicaError = await replicaOperation(replicaBackend, cancellationToken);
            if (replicaError is null) {
                continue;
            }

            logger.LogWarning(
                "Write-through replication for {StorageOperation} failed on replica provider {ReplicaProvider}. ErrorCode {ErrorCode}.",
                operation,
                replicaBackend.Name,
                replicaError.Code);

            await RecordWriteThroughFailureAsync(
                operation,
                primaryBackend.Name,
                replicaBackends,
                index,
                bucketName,
                objectKey,
                versionId,
                replicaError);

            return CreateReplicationError(replicaBackend, replicaError, bucketName, objectKey, versionId);
        }

        return null;
    }

    private async ValueTask<StorageError?> DispatchAsyncReplicationAsync(
        StorageOperationType operation,
        IStorageBackend primaryBackend,
        IReadOnlyList<IStorageBackend> replicaBackends,
        string bucketName,
        string? objectKey,
        string? versionId,
        CancellationToken cancellationToken)
    {
        StorageError? trackingError = null;

        foreach (var replicaBackend in replicaBackends) {
            var repairEntry = CreateRepairEntry(
                StorageReplicaRepairOrigin.AsyncReplication,
                StorageReplicaRepairStatus.Pending,
                operation,
                primaryBackend.Name,
                replicaBackend.Name,
                bucketName,
                objectKey,
                versionId);

            try {
                if (await replicaRepairBacklog.HasOutstandingRepairsAsync(replicaBackend.Name, cancellationToken)) {
                    logger.LogInformation(
                        "Queuing async replica repair for {StorageOperation} on {ReplicaProvider} because outstanding repairs already exist.",
                        operation,
                        replicaBackend.Name);
                    IntegratedS3CoreTelemetry.AddReplicaEvent(
                        Activity.Current,
                        "replica-repair-queued-existing-backlog",
                        operation,
                        replicaBackend.Name,
                        repairEntry.Origin,
                        repairEntry.Status);
                    await replicaRepairBacklog.AddAsync(repairEntry, cancellationToken);
                    continue;
                }

                var healthStatus = await backendHealthMonitor.GetStatusAsync(replicaBackend, cancellationToken);
                if (healthStatus == StorageBackendHealthStatus.Unhealthy) {
                    logger.LogInformation(
                        "Queuing async replica repair for {StorageOperation} on {ReplicaProvider} because the replica is unhealthy.",
                        operation,
                        replicaBackend.Name);
                    IntegratedS3CoreTelemetry.AddReplicaEvent(
                        Activity.Current,
                        "replica-repair-queued-unhealthy",
                        operation,
                        replicaBackend.Name,
                        repairEntry.Origin,
                        repairEntry.Status);
                    await replicaRepairBacklog.AddAsync(repairEntry, cancellationToken);
                    continue;
                }

                await replicaRepairDispatcher.DispatchAsync(
                    repairEntry,
                    ct => replicaRepairService.RepairAsync(repairEntry, ct),
                    cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
                throw;
            }
            catch (Exception ex) {
                logger.LogError(
                    ex,
                    "Failed to track async replica repair for provider {ReplicaProvider}.",
                    replicaBackend.Name);
                trackingError ??= CreateAsyncReplicationTrackingError(replicaBackend, ex, bucketName, objectKey, versionId);
            }
        }

        return trackingError;
    }

    private async ValueTask RecordWriteThroughFailureAsync(
        StorageOperationType operation,
        string primaryBackendName,
        IReadOnlyList<IStorageBackend> replicaBackends,
        int failedReplicaIndex,
        string bucketName,
        string? objectKey,
        string? versionId,
        StorageError error)
    {
        for (var index = failedReplicaIndex; index < replicaBackends.Count; index++) {
            var replicaBackend = replicaBackends[index];
            var repairEntry = CreateRepairEntry(
                StorageReplicaRepairOrigin.PartialWriteFailure,
                index == failedReplicaIndex ? StorageReplicaRepairStatus.Failed : StorageReplicaRepairStatus.Pending,
                operation,
                primaryBackendName,
                replicaBackend.Name,
                bucketName,
                objectKey,
                versionId,
                attemptCount: index == failedReplicaIndex ? 1 : 0,
                lastError: index == failedReplicaIndex ? error : null);

            IntegratedS3CoreTelemetry.AddReplicaEvent(
                Activity.Current,
                "replica-repair-recorded-write-through-failure",
                operation,
                replicaBackend.Name,
                repairEntry.Origin,
                repairEntry.Status,
                index == failedReplicaIndex ? error : null);
            await replicaRepairBacklog.AddAsync(repairEntry, CancellationToken.None);
        }
    }

    private async ValueTask RecordReplicaBacklogAsync(
        StorageOperationType operation,
        StorageReplicaRepairOrigin origin,
        string primaryBackendName,
        IReadOnlyList<IStorageBackend> replicaBackends,
        string bucketName,
        string? objectKey,
        string? versionId,
        StorageError? error,
        StorageReplicaRepairStatus status,
        int attemptCount)
    {
        foreach (var replicaBackend in replicaBackends) {
            var repairEntry = CreateRepairEntry(
                origin,
                status,
                operation,
                primaryBackendName,
                replicaBackend.Name,
                bucketName,
                objectKey,
                versionId,
                attemptCount,
                error);
            logger.LogInformation(
                "Recording replica backlog for {StorageOperation} on {ReplicaProvider}. Origin {Origin}. Status {Status}.",
                operation,
                replicaBackend.Name,
                origin,
                status);
            IntegratedS3CoreTelemetry.AddReplicaEvent(
                Activity.Current,
                "replica-repair-recorded-backlog",
                operation,
                replicaBackend.Name,
                origin,
                status,
                error);
            await replicaRepairBacklog.AddAsync(repairEntry, CancellationToken.None);
        }
    }

    private async ValueTask<StorageError?> ReplicateCopyObjectWriteThroughAsync(
        CopyObjectRequest request,
        IStorageBackend primaryBackend,
        string? versionId,
        CancellationToken cancellationToken)
    {
        var replicaBackends = GetReplicaBackends(primaryBackend);
        if (replicaBackends.Count == 0) {
            return null;
        }

        var sourceResponseResult = await GetObjectForReplicationAsync(
            primaryBackend,
            request.DestinationBucketName,
            request.DestinationKey,
            versionId,
            cancellationToken);
        if (!sourceResponseResult.IsSuccess || sourceResponseResult.Value is null) {
            var sourceError = sourceResponseResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, request.DestinationBucketName, request.DestinationKey, versionId);
            await RecordReplicaBacklogAsync(
                StorageOperationType.CopyObject,
                StorageReplicaRepairOrigin.PartialWriteFailure,
                primaryBackend.Name,
                replicaBackends,
                request.DestinationBucketName,
                request.DestinationKey,
                versionId,
                sourceError,
                StorageReplicaRepairStatus.Pending,
                attemptCount: 0);
            return CreateReplicationError(primaryBackend, sourceError, request.DestinationBucketName, request.DestinationKey, versionId);
        }

        await using var sourceResponse = sourceResponseResult.Value;
        var tempFilePath = await BufferStreamToTempFileAsync(sourceResponse.Content, cancellationToken);
        try {
            return await ExecuteWriteThroughReplicationAsync(
                StorageOperationType.CopyObject,
                primaryBackend,
                replicaBackends,
                request.DestinationBucketName,
                request.DestinationKey,
                versionId,
                (replicaBackend, ct) => WriteReplicaBufferedObjectAsync(
                    replicaBackend,
                    request.DestinationBucketName,
                    request.DestinationKey,
                    tempFilePath,
                    sourceResponse.Object.ContentLength,
                    sourceResponse.Object.ContentType,
                    sourceResponse.Object.Metadata,
                    sourceResponse.Object.Tags,
                    sourceResponse.Object.Checksums,
                    request.OverwriteIfExists,
                    ct),
                cancellationToken);
        }
        finally {
            DeleteTempFileIfPresent(tempFilePath);
        }
    }

    private async ValueTask<StorageError?> WriteReplicaBucketCreateAsync(IStorageBackend replicaBackend, CreateBucketRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.CreateBucketAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess) {
            if (replicaResult.Error?.Code != StorageErrorCode.BucketAlreadyExists) {
                return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket create did not succeed.");
            }

            var refreshError = await RefreshCatalogBucketAsync(replicaBackend, request.BucketName, cancellationToken);
            if (refreshError is not null) {
                return refreshError;
            }

            if (request.EnableVersioning) {
                return await WriteReplicaBucketVersioningAsync(replicaBackend, new PutBucketVersioningRequest
                {
                    BucketName = request.BucketName,
                    Status = BucketVersioningStatus.Enabled
                }, cancellationToken);
            }

            return null;
        }

        if (replicaResult.Value is null) {
            return CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket create did not return bucket metadata.");
        }

        await catalogStore.UpsertBucketAsync(replicaBackend.Name, replicaResult.Value, cancellationToken);
        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketCreateAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryHeadResult = await primaryBackend.HeadBucketAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryHeadResult);
        if (!primaryHeadResult.IsSuccess || primaryHeadResult.Value is null) {
            return primaryHeadResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket state could not be resolved for replica repair.");
        }

        var primaryVersioningResult = await primaryBackend.GetBucketVersioningAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryVersioningResult);
        if (!primaryVersioningResult.IsSuccess || primaryVersioningResult.Value is null) {
            return primaryVersioningResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket versioning state could not be resolved for replica repair.");
        }

        var createError = await WriteReplicaBucketCreateAsync(replicaBackend, new CreateBucketRequest
        {
            BucketName = bucketName,
            EnableVersioning = primaryVersioningResult.Value.VersioningEnabled
        }, cancellationToken);
        if (createError is not null) {
            return createError;
        }

        if (primaryVersioningResult.Value.Status != BucketVersioningStatus.Disabled) {
            return await WriteReplicaBucketVersioningAsync(replicaBackend, new PutBucketVersioningRequest
            {
                BucketName = bucketName,
                Status = primaryVersioningResult.Value.Status
            }, cancellationToken);
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaBucketDeleteAsync(IStorageBackend replicaBackend, DeleteBucketRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code != StorageErrorCode.BucketNotFound) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket delete did not succeed.");
        }

        await catalogStore.RemoveBucketAsync(replicaBackend.Name, request.BucketName, cancellationToken);
        return null;
    }

    private ValueTask<StorageError?> RepairReplicaBucketDeleteAsync(IStorageBackend replicaBackend, string bucketName, CancellationToken cancellationToken)
    {
        return WriteReplicaBucketDeleteAsync(replicaBackend, new DeleteBucketRequest
        {
            BucketName = bucketName
        }, cancellationToken);
    }

    private async ValueTask<StorageError?> WriteReplicaBucketVersioningAsync(IStorageBackend replicaBackend, PutBucketVersioningRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketVersioningAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket versioning update did not return versioning metadata.");
        }

        return await RefreshCatalogBucketAsync(replicaBackend, request.BucketName, cancellationToken);
    }

    private async ValueTask<StorageError?> RepairReplicaBucketVersioningAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryVersioningResult = await primaryBackend.GetBucketVersioningAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryVersioningResult);
        if (!primaryVersioningResult.IsSuccess || primaryVersioningResult.Value is null) {
            return primaryVersioningResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket versioning state could not be resolved for replica repair.");
        }

        return await WriteReplicaBucketVersioningAsync(replicaBackend, new PutBucketVersioningRequest
        {
            BucketName = bucketName,
            Status = primaryVersioningResult.Value.Status
        }, cancellationToken);
    }

    private async ValueTask<StorageError?> WriteReplicaPutBucketCorsAsync(IStorageBackend replicaBackend, PutBucketCorsRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketCorsAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket CORS update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketCorsAsync(IStorageBackend replicaBackend, DeleteBucketCorsRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketCorsAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not (StorageErrorCode.CorsConfigurationNotFound or StorageErrorCode.BucketNotFound)) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket CORS delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketCorsAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryCorsResult = await primaryBackend.GetBucketCorsAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryCorsResult);
        if (primaryCorsResult.IsSuccess && primaryCorsResult.Value is not null) {
            return await WriteReplicaPutBucketCorsAsync(replicaBackend, new PutBucketCorsRequest
            {
                BucketName = bucketName,
                Rules = CloneCorsRules(primaryCorsResult.Value.Rules)
            }, cancellationToken);
        }

        if (primaryCorsResult.Error?.Code == StorageErrorCode.CorsConfigurationNotFound) {
            return await WriteReplicaDeleteBucketCorsAsync(replicaBackend, new DeleteBucketCorsRequest
            {
                BucketName = bucketName
            }, cancellationToken);
        }

        return primaryCorsResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket CORS configuration could not be resolved for replica repair.");
    }

    private ValueTask<StorageError?> WriteReplicaBufferedObjectAsync(IStorageBackend replicaBackend, PutObjectRequest request, string tempFilePath, CancellationToken cancellationToken)
    {
        return WriteReplicaBufferedObjectAsync(
            replicaBackend,
            request.BucketName,
            request.Key,
            tempFilePath,
            request.ContentLength,
            request.ContentType,
            request.Metadata,
            request.Tags,
            request.Checksums,
            request.OverwriteIfExists,
            cancellationToken);
    }

    private async ValueTask<StorageError?> WriteReplicaBufferedObjectAsync(
        IStorageBackend replicaBackend,
        string bucketName,
        string key,
        string tempFilePath,
        long? contentLength,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        IReadOnlyDictionary<string, string>? checksums,
        bool overwriteIfExists,
        CancellationToken cancellationToken)
    {
        var replicaResult = await PutBufferedObjectAsync(
            replicaBackend,
            bucketName,
            key,
            tempFilePath,
            contentLength,
            contentType,
            metadata,
            tags,
            checksums,
            overwriteIfExists,
            cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, bucketName, key, versionId: null, "Replica object write did not return object metadata.");
        }

        await catalogStore.UpsertObjectAsync(replicaBackend.Name, replicaResult.Value, cancellationToken);
        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaObjectFromPrimaryAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken)
    {
        var sourceResponseResult = await GetObjectForReplicationAsync(primaryBackend, bucketName, key, versionId, cancellationToken);
        if (!sourceResponseResult.IsSuccess || sourceResponseResult.Value is null) {
            return sourceResponseResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, key, versionId);
        }

        await using var sourceResponse = sourceResponseResult.Value;
        var replicaResult = await replicaBackend.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            Content = sourceResponse.Content,
            ContentLength = sourceResponse.Object.ContentLength,
            ContentType = sourceResponse.Object.ContentType,
            Metadata = CloneMetadata(sourceResponse.Object.Metadata),
            Tags = CloneTags(sourceResponse.Object.Tags),
            Checksums = CloneChecksums(sourceResponse.Object.Checksums),
            OverwriteIfExists = true
        }, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, bucketName, key, versionId, "Replica object repair did not return object metadata.");
        }

        await catalogStore.UpsertObjectAsync(replicaBackend.Name, replicaResult.Value, cancellationToken);
        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaPutObjectTagsAsync(IStorageBackend replicaBackend, PutObjectTagsRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutObjectTagsAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, request.Key, request.VersionId, "Replica object tag update did not return tag metadata.");
        }

        await RefreshCatalogObjectAsync(replicaBackend, request.BucketName, request.Key, request.VersionId, cancellationToken);
        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteObjectTagsAsync(IStorageBackend replicaBackend, DeleteObjectTagsRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteObjectTagsAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, request.Key, request.VersionId, "Replica object tag delete did not return tag metadata.");
        }

        await RefreshCatalogObjectAsync(replicaBackend, request.BucketName, request.Key, request.VersionId, cancellationToken);
        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaObjectTagsFromPrimaryAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        string key,
        string? requestedVersionId,
        CancellationToken cancellationToken)
    {
        var primaryTagResult = await primaryBackend.GetObjectTagsAsync(new GetObjectTagsRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = requestedVersionId
        }, cancellationToken);
        ObserveResult(primaryBackend, primaryTagResult);
        if (!primaryTagResult.IsSuccess || primaryTagResult.Value is null) {
            return primaryTagResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, key, requestedVersionId, "Primary object tags could not be resolved for replica repair.");
        }

        StorageResult<ObjectTagSet> replicaResult;
        if (primaryTagResult.Value.Tags.Count == 0) {
            replicaResult = await replicaBackend.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
            {
                BucketName = bucketName,
                Key = key,
                VersionId = requestedVersionId
            }, cancellationToken);
        }
        else {
            replicaResult = await replicaBackend.PutObjectTagsAsync(new PutObjectTagsRequest
            {
                BucketName = bucketName,
                Key = key,
                VersionId = requestedVersionId,
                Tags = CloneTags(primaryTagResult.Value.Tags) ?? new Dictionary<string, string>(StringComparer.Ordinal)
            }, cancellationToken);
        }

        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, bucketName, key, requestedVersionId, "Replica tag repair did not return tag metadata.");
        }

        await RefreshCatalogObjectAsync(replicaBackend, bucketName, key, requestedVersionId, cancellationToken);
        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteObjectAsync(IStorageBackend replicaBackend, DeleteObjectRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteObjectAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess) {
            if (replicaResult.Error?.Code is not (StorageErrorCode.ObjectNotFound or StorageErrorCode.BucketNotFound)) {
                return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, request.Key, request.VersionId, "Replica object delete did not succeed.");
            }

            await catalogStore.RemoveObjectAsync(replicaBackend.Name, request.BucketName, request.Key, request.VersionId, cancellationToken);
            return null;
        }

        if (replicaResult.Value is null) {
            return CreateReplicaOperationError(replicaBackend, request.BucketName, request.Key, request.VersionId, "Replica object delete did not return delete metadata.");
        }

        if (replicaResult.Value.CurrentObject is not null) {
            await catalogStore.UpsertObjectAsync(replicaBackend.Name, replicaResult.Value.CurrentObject, cancellationToken);
        }
        else if (!string.IsNullOrWhiteSpace(request.VersionId)) {
            await catalogStore.RemoveObjectAsync(replicaBackend.Name, request.BucketName, request.Key, request.VersionId, cancellationToken);
        }
        else {
            await catalogStore.RemoveObjectAsync(replicaBackend.Name, request.BucketName, request.Key, versionId: null, cancellationToken);
        }

        return null;
    }

    private ValueTask<StorageError?> RepairReplicaDeleteObjectAsync(IStorageBackend replicaBackend, DeleteObjectRequest request, CancellationToken cancellationToken)
    {
        return WriteReplicaDeleteObjectAsync(replicaBackend, request, cancellationToken);
    }

    private async ValueTask<StorageResult<GetObjectResponse>> GetObjectForReplicationAsync(
        IStorageBackend primaryBackend,
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken)
    {
        var result = await primaryBackend.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        }, cancellationToken);
        ObserveResult(primaryBackend, result);
        return result;
    }

    private bool UsesSynchronousReplicaWrites(IStorageBackend primaryBackend)
    {
        return options.Value.ConsistencyMode == StorageConsistencyMode.WriteThroughAll
            && GetReplicaBackends(primaryBackend).Count > 0;
    }

    private bool UsesAsyncReplicaWrites(IStorageBackend primaryBackend)
    {
        return options.Value.ConsistencyMode == StorageConsistencyMode.WriteToPrimaryAsyncReplicas
            && GetReplicaBackends(primaryBackend).Count > 0;
    }

    private StorageError? GetMultipartReplicationError(IStorageBackend primaryBackend, string bucketName, string key)
    {
        return UsesSynchronousReplicaWrites(primaryBackend) || UsesAsyncReplicaWrites(primaryBackend)
            ? StorageError.Unsupported(
                $"Multipart uploads are not yet supported when the '{options.Value.ConsistencyMode}' consistency mode is enabled.",
                bucketName,
                key)
            : null;
    }

    private IReadOnlyList<IStorageBackend> GetReplicaBackends(IStorageBackend primaryBackend)
    {
        return _backends.Where(backend => !ReferenceEquals(backend, primaryBackend)).ToArray();
    }

    private void ObserveResult(IStorageBackend backend, StorageResult result)
    {
        if (result.IsSuccess) {
            backendHealthMonitor.ReportSuccess(backend);
            return;
        }

        backendHealthMonitor.ReportFailure(backend, result.Error);
    }

    private StorageReplicaRepairEntry CreateRepairEntry(
        StorageReplicaRepairOrigin origin,
        StorageReplicaRepairStatus status,
        StorageOperationType operation,
        string primaryBackendName,
        string replicaBackendName,
        string bucketName,
        string? objectKey,
        string? versionId,
        int attemptCount = 0,
        StorageError? lastError = null)
    {
        var now = timeProvider.GetUtcNow();
        return new StorageReplicaRepairEntry
        {
            Id = Guid.NewGuid().ToString("N"),
            Origin = origin,
            Status = status,
            Operation = operation,
            DivergenceKinds = StorageReplicaRepairEntry.GetDefaultDivergenceKinds(operation),
            PrimaryBackendName = primaryBackendName,
            ReplicaBackendName = replicaBackendName,
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            CreatedAtUtc = now,
            UpdatedAtUtc = now,
            AttemptCount = attemptCount,
            LastErrorCode = lastError?.Code,
            LastErrorMessage = lastError?.Message
        };
    }

    private async ValueTask<StorageError?> RefreshCatalogBucketAsync(IStorageBackend backend, string bucketName, CancellationToken cancellationToken)
    {
        var headResult = await backend.HeadBucketAsync(bucketName, cancellationToken);
        ObserveResult(backend, headResult);
        if (!headResult.IsSuccess || headResult.Value is null) {
            return headResult.Error ?? CreateReplicaOperationError(backend, bucketName, objectKey: null, versionId: null, message: "Bucket state could not be refreshed after replica write.");
        }

        await catalogStore.UpsertBucketAsync(backend.Name, headResult.Value, cancellationToken);
        return null;
    }

    private static async ValueTask<StorageResult<ObjectInfo>> PutBufferedObjectAsync(IStorageBackend backend, PutObjectRequest request, string tempFilePath, CancellationToken cancellationToken)
    {
        return await PutBufferedObjectAsync(
            backend,
            request.BucketName,
            request.Key,
            tempFilePath,
            request.ContentLength,
            request.ContentType,
            request.Metadata,
            request.Tags,
            request.Checksums,
            request.OverwriteIfExists,
            cancellationToken);
    }

    private static async ValueTask<StorageResult<ObjectInfo>> PutBufferedObjectAsync(
        IStorageBackend backend,
        string bucketName,
        string key,
        string tempFilePath,
        long? contentLength,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        IReadOnlyDictionary<string, string>? checksums,
        bool overwriteIfExists,
        CancellationToken cancellationToken)
    {
        await using var content = OpenBufferedReadStream(tempFilePath);
        return await backend.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            Content = content,
            ContentLength = contentLength,
            ContentType = contentType,
            Metadata = CloneMetadata(metadata),
            Tags = CloneTags(tags),
            Checksums = CloneChecksums(checksums),
            OverwriteIfExists = overwriteIfExists
        }, cancellationToken);
    }

    private static async Task<string> BufferStreamToTempFileAsync(Stream content, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(content);

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"integrateds3-orchestration-{Guid.NewGuid():N}.tmp");
        await using var tempFileStream = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
        await content.CopyToAsync(tempFileStream, cancellationToken);
        await tempFileStream.FlushAsync(cancellationToken);
        return tempFilePath;
    }

    private static Stream OpenBufferedReadStream(string tempFilePath)
    {
        return new FileStream(tempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    private static IReadOnlyDictionary<string, string>? CloneMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        return metadata is null
            ? null
            : new Dictionary<string, string>(metadata, StringComparer.OrdinalIgnoreCase);
    }

    private static IReadOnlyDictionary<string, string>? CloneChecksums(IReadOnlyDictionary<string, string>? checksums)
    {
        return checksums is null
            ? null
            : new Dictionary<string, string>(checksums, StringComparer.OrdinalIgnoreCase);
    }

    private static IReadOnlyDictionary<string, string>? CloneTags(IReadOnlyDictionary<string, string>? tags)
    {
        return tags is null
            ? null
            : new Dictionary<string, string>(tags, StringComparer.Ordinal);
    }

    private static IReadOnlyList<BucketCorsRule> CloneCorsRules(IReadOnlyList<BucketCorsRule> rules)
    {
        return rules.Select(CloneCorsRule).ToArray();
    }

    private static BucketCorsRule CloneCorsRule(BucketCorsRule rule)
    {
        return new BucketCorsRule
        {
            Id = rule.Id,
            AllowedOrigins = rule.AllowedOrigins.ToArray(),
            AllowedMethods = rule.AllowedMethods.ToArray(),
            AllowedHeaders = rule.AllowedHeaders.ToArray(),
            ExposeHeaders = rule.ExposeHeaders.ToArray(),
            MaxAgeSeconds = rule.MaxAgeSeconds
        };
    }

    private static string? GetEffectiveVersionId(string? requestedVersionId, string? resolvedVersionId)
    {
        return string.IsNullOrWhiteSpace(resolvedVersionId)
            ? requestedVersionId
            : resolvedVersionId;
    }

    private async ValueTask RefreshCatalogObjectAsync(IStorageBackend backend, string bucketName, string key, CancellationToken cancellationToken)
    {
        await RefreshCatalogObjectAsync(backend, bucketName, key, versionId: null, cancellationToken);
    }

    private async ValueTask RefreshCatalogObjectAsync(IStorageBackend backend, string bucketName, string key, string? versionId, CancellationToken cancellationToken)
    {
        var headResult = await backend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        }, cancellationToken);
        ObserveResult(backend, headResult);

        if (headResult.IsSuccess && headResult.Value is not null) {
            await catalogStore.UpsertObjectAsync(backend.Name, headResult.Value, cancellationToken);
        }
    }

    private static void DeleteTempFileIfPresent(string tempFilePath)
    {
        if (File.Exists(tempFilePath)) {
            File.Delete(tempFilePath);
        }
    }

    private static StorageError CreateReplicationError(
        IStorageBackend backend,
        StorageError? underlyingError,
        string bucketName,
        string? objectKey = null,
        string? versionId = null)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = underlyingError?.Message is { Length: > 0 } message
                ? $"Replicated write to provider '{backend.Name}' failed after the primary backend succeeded. Explicit repair is still required: {message}"
                : $"Replicated write to provider '{backend.Name}' failed after the primary backend succeeded. Explicit repair is still required.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            ProviderName = backend.Name,
            SuggestedHttpStatusCode = 503
        };
    }

    private static StorageError CreateReplicaOperationError(
        IStorageBackend backend,
        string bucketName,
        string? objectKey,
        string? versionId,
        string message)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            ProviderName = backend.Name,
            SuggestedHttpStatusCode = 503
        };
    }

    private static StorageError CreatePrimaryReplicationSourceError(
        IStorageBackend backend,
        string bucketName,
        string? objectKey,
        string? versionId,
        string? message = null)
    {
        return CreateReplicaOperationError(
            backend,
            bucketName,
            objectKey,
            versionId,
            message ?? "Primary state required for replica repair could not be resolved.");
    }

    private static StorageError CreateUnhealthyReplicaWriteError(
        IStorageBackend replicaBackend,
        string bucketName,
        string? objectKey,
        string? versionId)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = $"Strict write-through replication cannot proceed because replica provider '{replicaBackend.Name}' is unhealthy.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            ProviderName = replicaBackend.Name,
            SuggestedHttpStatusCode = 503
        };
    }

    private static StorageError CreateStaleReplicaWriteError(
        IStorageBackend replicaBackend,
        string bucketName,
        string? objectKey,
        string? versionId)
    {
        return new StorageError
        {
            Code = StorageErrorCode.PreconditionFailed,
            Message = $"Strict write-through replication cannot proceed because replica provider '{replicaBackend.Name}' has pending replica work and may be stale.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            ProviderName = replicaBackend.Name,
            SuggestedHttpStatusCode = 412
        };
    }

    private static StorageError CreateIncompleteReplicaWriteError(
        IStorageBackend replicaBackend,
        string bucketName,
        string? objectKey,
        string? versionId)
    {
        return new StorageError
        {
            Code = StorageErrorCode.PreconditionFailed,
            Message = $"Strict write-through replication cannot proceed because replica provider '{replicaBackend.Name}' has incomplete failed repair attempts.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            ProviderName = replicaBackend.Name,
            SuggestedHttpStatusCode = 412
        };
    }

    private static StorageError CreateAsyncReplicationTrackingError(
        IStorageBackend replicaBackend,
        Exception exception,
        string bucketName,
        string? objectKey,
        string? versionId)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = $"Primary write succeeded, but asynchronous replica repair could not be recorded for provider '{replicaBackend.Name}': {exception.Message}",
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            ProviderName = replicaBackend.Name,
            SuggestedHttpStatusCode = 503
        };
    }

    private bool ShouldFailoverRead(StorageError? error)
    {
        return error?.Code is StorageErrorCode.ProviderUnavailable or StorageErrorCode.Throttled;
    }

    private int GetOriginalIndex(IStorageBackend backend)
    {
        for (var index = 0; index < _backends.Length; index++) {
            if (ReferenceEquals(_backends[index], backend)) {
                return index;
            }
        }

        return int.MaxValue;
    }

    private static int GetReadPriority(
        ReadBackendCandidate candidate,
        IStorageBackend primaryBackend,
        StorageReadRoutingMode readRoutingMode,
        bool allowReadsFromReplicasWithOutstandingRepairs)
    {
        var isPrimary = ReferenceEquals(candidate.Backend, primaryBackend);
        var basePriority = readRoutingMode switch
        {
            StorageReadRoutingMode.PreferPrimary => candidate.HealthStatus switch
            {
                StorageBackendHealthStatus.Healthy when isPrimary => 0,
                StorageBackendHealthStatus.Healthy => 1,
                StorageBackendHealthStatus.Unknown when isPrimary => 2,
                StorageBackendHealthStatus.Unknown => 3,
                StorageBackendHealthStatus.Unhealthy when isPrimary => 4,
                _ => 5
            },
            StorageReadRoutingMode.PreferHealthyReplica => candidate.HealthStatus switch
            {
                StorageBackendHealthStatus.Healthy when !isPrimary => 0,
                StorageBackendHealthStatus.Healthy => 1,
                StorageBackendHealthStatus.Unknown when !isPrimary => 2,
                StorageBackendHealthStatus.Unknown => 3,
                StorageBackendHealthStatus.Unhealthy when !isPrimary => 4,
                _ => 5
            },
            _ => isPrimary ? 0 : 1
        };

        return candidate.HasOutstandingRepairs && !isPrimary && !allowReadsFromReplicasWithOutstandingRepairs
            ? basePriority + 10
            : basePriority;
    }

    private readonly record struct ReadBackendCandidate(IStorageBackend Backend, StorageBackendHealthStatus HealthStatus, bool HasOutstandingRepairs);
}

