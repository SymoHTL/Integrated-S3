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

    public async ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketDefaultEncryption,
            (backend, ct) => backend.GetBucketDefaultEncryptionAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> PutBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketDefaultEncryptionConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketDefaultEncryptionAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketDefaultEncryption,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketDefaultEncryptionAsync(replicaBackend, new PutBucketDefaultEncryptionRequest
                {
                    BucketName = request.BucketName,
                    Rule = CloneBucketDefaultEncryptionRule(request.Rule)
                }, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketDefaultEncryptionConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketDefaultEncryptionAsync(DeleteBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketDefaultEncryptionAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketDefaultEncryption,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketDefaultEncryptionAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
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
            onSuccess: (_, result, _) => {
                if (result.Value is not null) {
                    IntegratedS3CoreTelemetry.RecordStorageOperationBytes("GetObject", result.Value.TotalContentLength);
                }
                return ValueTask.CompletedTask;
            },
            cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectRetentionInfo>> GetObjectRetentionAsync(GetObjectRetentionRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetObject,
            (backend, ct) => backend.GetObjectRetentionAsync(request, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectLegalHoldInfo>> GetObjectLegalHoldAsync(GetObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetObject,
            (backend, ct) => backend.GetObjectLegalHoldAsync(request, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<GetObjectAttributesResponse>> GetObjectAttributesAsync(GetObjectAttributesRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetObjectAttributes,
            (backend, ct) => backend.GetObjectAttributesAsync(request, ct),
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
            IntegratedS3CoreTelemetry.RecordStorageOperationBytes("CopyObject", result.Value.ContentLength);

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
                IntegratedS3CoreTelemetry.RecordStorageOperationBytes("PutObject", result.Value.ContentLength);

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
            IntegratedS3CoreTelemetry.RecordStorageOperationBytes("PutObject", result.Value.ContentLength);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutObject,
                backend,
                request.BucketName,
                request.Key,
                result.Value.VersionId,
                writeThroughOperation: (replicaBackend, ct) => RepairReplicaObjectFromPrimaryAsync(backend, replicaBackend, request.BucketName, request.Key, result.Value.VersionId, ct),
                cancellationToken: CancellationToken.None);
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
        if (result.IsSuccess && result.Value is not null) {
            IntegratedS3CoreTelemetry.RecordStorageOperationBytes("UploadPart", result.Value.ContentLength);
        }
        return result;
    }

    public async ValueTask<StorageResult<MultipartUploadPart>> UploadPartCopyAsync(UploadPartCopyRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult<MultipartUploadPart>.Failure(replicationError);
        }

        var result = await backend.UploadPartCopyAsync(request, cancellationToken);
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
            IntegratedS3CoreTelemetry.RecordStorageOperationBytes("CompleteMultipartUpload", result.Value.ContentLength);
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


    // ── Bucket Tagging ──────────────────────────────────────────────────

    public async ValueTask<StorageResult<BucketTaggingConfiguration>> GetBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketTagging,
            (backend, ct) => backend.GetBucketTaggingAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketTaggingConfiguration>> PutBucketTaggingAsync(PutBucketTaggingRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketTaggingConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketTaggingAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketTagging,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketTaggingAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketTaggingConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketTaggingAsync(DeleteBucketTaggingRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketTaggingAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketTagging,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketTaggingAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Bucket Logging ──────────────────────────────────────────────────

    public async ValueTask<StorageResult<BucketLoggingConfiguration>> GetBucketLoggingAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketLogging,
            (backend, ct) => backend.GetBucketLoggingAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketLoggingConfiguration>> PutBucketLoggingAsync(PutBucketLoggingRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketLoggingConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketLoggingAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketLogging,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketLoggingAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketLoggingConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Bucket Website ──────────────────────────────────────────────────

    public async ValueTask<StorageResult<BucketWebsiteConfiguration>> GetBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketWebsite,
            (backend, ct) => backend.GetBucketWebsiteAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketWebsiteConfiguration>> PutBucketWebsiteAsync(PutBucketWebsiteRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketWebsiteConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketWebsiteAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketWebsite,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketWebsiteAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketWebsiteConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketWebsiteAsync(DeleteBucketWebsiteRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketWebsiteAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketWebsite,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketWebsiteAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Bucket Request Payment ──────────────────────────────────────────

    public async ValueTask<StorageResult<BucketRequestPaymentConfiguration>> GetBucketRequestPaymentAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketRequestPayment,
            (backend, ct) => backend.GetBucketRequestPaymentAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketRequestPaymentConfiguration>> PutBucketRequestPaymentAsync(PutBucketRequestPaymentRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketRequestPaymentConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketRequestPaymentAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketRequestPayment,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketRequestPaymentAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketRequestPaymentConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Bucket Accelerate ───────────────────────────────────────────────

    public async ValueTask<StorageResult<BucketAccelerateConfiguration>> GetBucketAccelerateAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketAccelerate,
            (backend, ct) => backend.GetBucketAccelerateAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketAccelerateConfiguration>> PutBucketAccelerateAsync(PutBucketAccelerateRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketAccelerateConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketAccelerateAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketAccelerate,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketAccelerateAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketAccelerateConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Bucket Lifecycle ────────────────────────────────────────────────

    public async ValueTask<StorageResult<BucketLifecycleConfiguration>> GetBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketLifecycle,
            (backend, ct) => backend.GetBucketLifecycleAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketLifecycleConfiguration>> PutBucketLifecycleAsync(PutBucketLifecycleRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketLifecycleConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketLifecycleAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketLifecycle,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketLifecycleAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketLifecycleConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketLifecycleAsync(DeleteBucketLifecycleRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketLifecycleAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketLifecycle,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketLifecycleAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Bucket Replication ──────────────────────────────────────────────

    public async ValueTask<StorageResult<BucketReplicationConfiguration>> GetBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketReplication,
            (backend, ct) => backend.GetBucketReplicationAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketReplicationConfiguration>> PutBucketReplicationAsync(PutBucketReplicationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketReplicationConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketReplicationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketReplication,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketReplicationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketReplicationConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketReplicationAsync(DeleteBucketReplicationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketReplicationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketReplication,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketReplicationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Bucket Notifications ────────────────────────────────────────────

    public async ValueTask<StorageResult<BucketNotificationConfiguration>> GetBucketNotificationConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketNotificationConfiguration,
            (backend, ct) => backend.GetBucketNotificationConfigurationAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketNotificationConfiguration>> PutBucketNotificationConfigurationAsync(PutBucketNotificationConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketNotificationConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketNotificationConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketNotificationConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketNotificationConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketNotificationConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Object Lock Configuration ───────────────────────────────────────

    public async ValueTask<StorageResult<ObjectLockConfiguration>> GetObjectLockConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetObjectLockConfiguration,
            (backend, ct) => backend.GetObjectLockConfigurationAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectLockConfiguration>> PutObjectLockConfigurationAsync(PutObjectLockConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<ObjectLockConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutObjectLockConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutObjectLockConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutObjectLockConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<ObjectLockConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    // ── Bucket Analytics (ID-based) ─────────────────────────────────────

    public async ValueTask<StorageResult<BucketAnalyticsConfiguration>> GetBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketAnalyticsConfiguration,
            (backend, ct) => backend.GetBucketAnalyticsConfigurationAsync(bucketName, id, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketAnalyticsConfiguration>> PutBucketAnalyticsConfigurationAsync(PutBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketAnalyticsConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketAnalyticsConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketAnalyticsConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketAnalyticsConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketAnalyticsConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketAnalyticsConfigurationAsync(DeleteBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketAnalyticsConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketAnalyticsConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketAnalyticsConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<IReadOnlyList<BucketAnalyticsConfiguration>>> ListBucketAnalyticsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.ListBucketAnalyticsConfigurations,
            (backend, ct) => backend.ListBucketAnalyticsConfigurationsAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    // ── Bucket Metrics (ID-based) ───────────────────────────────────────

    public async ValueTask<StorageResult<BucketMetricsConfiguration>> GetBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketMetricsConfiguration,
            (backend, ct) => backend.GetBucketMetricsConfigurationAsync(bucketName, id, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketMetricsConfiguration>> PutBucketMetricsConfigurationAsync(PutBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketMetricsConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketMetricsConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketMetricsConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketMetricsConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketMetricsConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketMetricsConfigurationAsync(DeleteBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketMetricsConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketMetricsConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketMetricsConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<IReadOnlyList<BucketMetricsConfiguration>>> ListBucketMetricsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.ListBucketMetricsConfigurations,
            (backend, ct) => backend.ListBucketMetricsConfigurationsAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    // ── Bucket Inventory (ID-based) ─────────────────────────────────────

    public async ValueTask<StorageResult<BucketInventoryConfiguration>> GetBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketInventoryConfiguration,
            (backend, ct) => backend.GetBucketInventoryConfigurationAsync(bucketName, id, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketInventoryConfiguration>> PutBucketInventoryConfigurationAsync(PutBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketInventoryConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketInventoryConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketInventoryConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketInventoryConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketInventoryConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketInventoryConfigurationAsync(DeleteBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketInventoryConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketInventoryConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketInventoryConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<IReadOnlyList<BucketInventoryConfiguration>>> ListBucketInventoryConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.ListBucketInventoryConfigurations,
            (backend, ct) => backend.ListBucketInventoryConfigurationsAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    // ── Bucket Intelligent-Tiering (ID-based) ───────────────────────────

    public async ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> GetBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.GetBucketIntelligentTieringConfiguration,
            (backend, ct) => backend.GetBucketIntelligentTieringConfigurationAsync(bucketName, id, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> PutBucketIntelligentTieringConfigurationAsync(PutBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<BucketIntelligentTieringConfiguration>.Failure(strictReplicationError);
        }

        var result = await backend.PutBucketIntelligentTieringConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutBucketIntelligentTieringConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutBucketIntelligentTieringConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<BucketIntelligentTieringConfiguration>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult> DeleteBucketIntelligentTieringConfigurationAsync(DeleteBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, objectKey: null, versionId: null, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult.Failure(strictReplicationError);
        }

        var result = await backend.DeleteBucketIntelligentTieringConfigurationAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess) {
            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.DeleteBucketIntelligentTieringConfiguration,
                backend,
                request.BucketName,
                objectKey: null,
                versionId: null,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaDeleteBucketIntelligentTieringConfigurationAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<IReadOnlyList<BucketIntelligentTieringConfiguration>>> ListBucketIntelligentTieringConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.ListBucketIntelligentTieringConfigurations,
            (backend, ct) => backend.ListBucketIntelligentTieringConfigurationsAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    // ── Object Lock Write Operations ────────────────────────────────────

    public async ValueTask<StorageResult<ObjectRetentionInfo>> PutObjectRetentionAsync(PutObjectRetentionRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, request.Key, request.VersionId, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<ObjectRetentionInfo>.Failure(strictReplicationError);
        }

        var result = await backend.PutObjectRetentionAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var resolvedVersionId = GetEffectiveVersionId(request.VersionId, result.Value.VersionId);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutObjectRetention,
                backend,
                request.BucketName,
                request.Key,
                resolvedVersionId,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutObjectRetentionAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<ObjectRetentionInfo>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<ObjectLegalHoldInfo>> PutObjectLegalHoldAsync(PutObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var strictReplicationError = await GetStrictReplicaWritePreflightErrorAsync(backend, request.BucketName, request.Key, request.VersionId, cancellationToken: cancellationToken);
        if (strictReplicationError is not null) {
            return StorageResult<ObjectLegalHoldInfo>.Failure(strictReplicationError);
        }

        var result = await backend.PutObjectLegalHoldAsync(request, cancellationToken);
        ObserveResult(backend, result);
        if (result.IsSuccess && result.Value is not null) {
            var resolvedVersionId = GetEffectiveVersionId(request.VersionId, result.Value.VersionId);

            var replicationError = await ApplyReplicaWritePolicyAsync(
                StorageOperationType.PutObjectLegalHold,
                backend,
                request.BucketName,
                request.Key,
                resolvedVersionId,
                writeThroughOperation: (replicaBackend, ct) => WriteReplicaPutObjectLegalHoldAsync(replicaBackend, request, ct),
                cancellationToken: CancellationToken.None);
            if (replicationError is not null) {
                return StorageResult<ObjectLegalHoldInfo>.Failure(replicationError);
            }
        }

        return result;
    }

    // ── SelectObjectContent ─────────────────────────────────────────────

    public async ValueTask<StorageResult<SelectObjectContentResponse>> SelectObjectContentAsync(SelectObjectContentRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            StorageOperationType.SelectObjectContent,
            (backend, ct) => backend.SelectObjectContentAsync(request, ct),
            onSuccess: null,
            cancellationToken);
    }

    // ── RestoreObject ───────────────────────────────────────────────────

    public async ValueTask<StorageResult<RestoreObjectResponse>> RestoreObjectAsync(RestoreObjectRequest request, CancellationToken cancellationToken = default)
    {
        var backend = GetPrimaryBackend();
        var result = await backend.RestoreObjectAsync(request, cancellationToken);
        ObserveResult(backend, result);
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
        var coalesceAcrossVersions = await CanCoalesceReplicaRepairsAcrossVersionsAsync(
            primaryBackend,
            operation,
            bucketName,
            objectKey,
            cancellationToken);

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
                coalesceAcrossVersions,
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
        var coalesceAcrossVersions = await CanCoalesceReplicaRepairsAcrossVersionsAsync(
            primaryBackend,
            operation,
            bucketName,
            objectKey,
            cancellationToken);

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
                var outstandingRepairs = await replicaRepairBacklog.ListOutstandingAsync(replicaBackend.Name, cancellationToken);
                var supersededRepairs = GetSupersededReplicaRepairs(outstandingRepairs, repairEntry, coalesceAcrossVersions);
                var hasOutstandingRepairs = outstandingRepairs.Count > supersededRepairs.Length;

                if (hasOutstandingRepairs) {
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
                    await CompleteSupersededReplicaRepairsAsync(repairEntry, supersededRepairs, CancellationToken.None);
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
                    await CompleteSupersededReplicaRepairsAsync(repairEntry, supersededRepairs, CancellationToken.None);
                    continue;
                }

                await replicaRepairDispatcher.DispatchAsync(
                    repairEntry,
                    ct => replicaRepairService.RepairAsync(repairEntry, ct),
                    cancellationToken);
                await CompleteSupersededReplicaRepairsAsync(repairEntry, supersededRepairs, CancellationToken.None);
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
        bool coalesceAcrossVersions,
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
            var outstandingRepairs = await replicaRepairBacklog.ListOutstandingAsync(replicaBackend.Name, CancellationToken.None);
            var supersededRepairs = GetSupersededReplicaRepairs(outstandingRepairs, repairEntry, coalesceAcrossVersions);

            IntegratedS3CoreTelemetry.AddReplicaEvent(
                Activity.Current,
                "replica-repair-recorded-write-through-failure",
                operation,
                replicaBackend.Name,
                repairEntry.Origin,
                repairEntry.Status,
                index == failedReplicaIndex ? error : null);
            await replicaRepairBacklog.AddAsync(repairEntry, CancellationToken.None);
            await CompleteSupersededReplicaRepairsAsync(repairEntry, supersededRepairs, CancellationToken.None);
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
        int attemptCount,
        bool coalesceAcrossVersions)
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
            var outstandingRepairs = await replicaRepairBacklog.ListOutstandingAsync(replicaBackend.Name, CancellationToken.None);
            var supersededRepairs = GetSupersededReplicaRepairs(outstandingRepairs, repairEntry, coalesceAcrossVersions);
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
            await CompleteSupersededReplicaRepairsAsync(repairEntry, supersededRepairs, CancellationToken.None);
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
            var coalesceAcrossVersions = await CanCoalesceReplicaRepairsAcrossVersionsAsync(
                primaryBackend,
                StorageOperationType.CopyObject,
                request.DestinationBucketName,
                request.DestinationKey,
                cancellationToken);
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
                attemptCount: 0,
                coalesceAcrossVersions);
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
            if (primaryVersioningResult.Error?.Code == StorageErrorCode.BucketNotFound) {
                return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
            }

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

        if (primaryCorsResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        if (primaryCorsResult.Error?.Code == StorageErrorCode.CorsConfigurationNotFound) {
            return await WriteReplicaDeleteBucketCorsAsync(replicaBackend, new DeleteBucketCorsRequest
            {
                BucketName = bucketName
            }, cancellationToken);
        }

        return primaryCorsResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket CORS configuration could not be resolved for replica repair.");
    }

    private async ValueTask<StorageError?> WriteReplicaPutBucketDefaultEncryptionAsync(IStorageBackend replicaBackend, PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketDefaultEncryptionAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket default encryption update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketDefaultEncryptionAsync(IStorageBackend replicaBackend, DeleteBucketDefaultEncryptionRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketDefaultEncryptionAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not (StorageErrorCode.BucketEncryptionConfigurationNotFound or StorageErrorCode.BucketNotFound)) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket default encryption delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketDefaultEncryptionAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryEncryptionResult = await primaryBackend.GetBucketDefaultEncryptionAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryEncryptionResult);
        if (primaryEncryptionResult.IsSuccess && primaryEncryptionResult.Value is not null) {
            return await WriteReplicaPutBucketDefaultEncryptionAsync(replicaBackend, new PutBucketDefaultEncryptionRequest
            {
                BucketName = bucketName,
                Rule = CloneBucketDefaultEncryptionRule(primaryEncryptionResult.Value.Rule)
            }, cancellationToken);
        }

        if (primaryEncryptionResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        if (primaryEncryptionResult.Error?.Code == StorageErrorCode.BucketEncryptionConfigurationNotFound) {
            return await WriteReplicaDeleteBucketDefaultEncryptionAsync(replicaBackend, new DeleteBucketDefaultEncryptionRequest
            {
                BucketName = bucketName
            }, cancellationToken);
        }

        return primaryEncryptionResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket default encryption configuration could not be resolved for replica repair.");
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
            if (sourceResponseResult.Error?.Code is StorageErrorCode.ObjectNotFound or StorageErrorCode.BucketNotFound) {
                return await WriteReplicaDeleteObjectAsync(replicaBackend, new DeleteObjectRequest
                {
                    BucketName = bucketName,
                    Key = key
                }, cancellationToken);
            }

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
            if (primaryTagResult.Error?.Code is StorageErrorCode.ObjectNotFound or StorageErrorCode.BucketNotFound) {
                return await WriteReplicaDeleteObjectAsync(replicaBackend, new DeleteObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = requestedVersionId
                }, cancellationToken);
            }

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

    // ── Bucket Tagging replication helpers ───────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketTaggingAsync(IStorageBackend replicaBackend, PutBucketTaggingRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketTaggingAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket tagging update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketTaggingAsync(IStorageBackend replicaBackend, DeleteBucketTaggingRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketTaggingAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not (StorageErrorCode.TaggingConfigurationNotFound or StorageErrorCode.BucketNotFound)) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket tagging delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketTaggingAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketTaggingAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketTaggingAsync(replicaBackend, new PutBucketTaggingRequest
            {
                BucketName = bucketName,
                Tags = primaryResult.Value.Tags
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.TaggingConfigurationNotFound) {
            return await WriteReplicaDeleteBucketTaggingAsync(replicaBackend, new DeleteBucketTaggingRequest
            {
                BucketName = bucketName
            }, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket tagging configuration could not be resolved for replica repair.");
    }

    // ── Bucket Logging replication helpers ───────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketLoggingAsync(IStorageBackend replicaBackend, PutBucketLoggingRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketLoggingAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket logging update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketLoggingAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketLoggingAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketLoggingAsync(replicaBackend, new PutBucketLoggingRequest
            {
                BucketName = bucketName,
                TargetBucket = primaryResult.Value.TargetBucket,
                TargetPrefix = primaryResult.Value.TargetPrefix
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.LoggingConfigurationNotFound) {
            return await WriteReplicaPutBucketLoggingAsync(replicaBackend, new PutBucketLoggingRequest
            {
                BucketName = bucketName
            }, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket logging configuration could not be resolved for replica repair.");
    }

    // ── Bucket Website replication helpers ───────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketWebsiteAsync(IStorageBackend replicaBackend, PutBucketWebsiteRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketWebsiteAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket website update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketWebsiteAsync(IStorageBackend replicaBackend, DeleteBucketWebsiteRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketWebsiteAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not (StorageErrorCode.WebsiteConfigurationNotFound or StorageErrorCode.BucketNotFound)) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket website delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketWebsiteAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketWebsiteAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketWebsiteAsync(replicaBackend, new PutBucketWebsiteRequest
            {
                BucketName = bucketName,
                IndexDocumentSuffix = primaryResult.Value.IndexDocumentSuffix,
                ErrorDocumentKey = primaryResult.Value.ErrorDocumentKey,
                RedirectAllRequestsTo = primaryResult.Value.RedirectAllRequestsTo,
                RoutingRules = primaryResult.Value.RoutingRules
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.WebsiteConfigurationNotFound) {
            return await WriteReplicaDeleteBucketWebsiteAsync(replicaBackend, new DeleteBucketWebsiteRequest
            {
                BucketName = bucketName
            }, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket website configuration could not be resolved for replica repair.");
    }

    // ── Bucket Request Payment replication helpers ───────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketRequestPaymentAsync(IStorageBackend replicaBackend, PutBucketRequestPaymentRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketRequestPaymentAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket request payment update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketRequestPaymentAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketRequestPaymentAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketRequestPaymentAsync(replicaBackend, new PutBucketRequestPaymentRequest
            {
                BucketName = bucketName,
                Payer = primaryResult.Value.Payer
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket request payment configuration could not be resolved for replica repair.");
    }

    // ── Bucket Accelerate replication helpers ────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketAccelerateAsync(IStorageBackend replicaBackend, PutBucketAccelerateRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketAccelerateAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket accelerate update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketAccelerateAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketAccelerateAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketAccelerateAsync(replicaBackend, new PutBucketAccelerateRequest
            {
                BucketName = bucketName,
                Status = primaryResult.Value.Status
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket accelerate configuration could not be resolved for replica repair.");
    }

    // ── Bucket Lifecycle replication helpers ─────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketLifecycleAsync(IStorageBackend replicaBackend, PutBucketLifecycleRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketLifecycleAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket lifecycle update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketLifecycleAsync(IStorageBackend replicaBackend, DeleteBucketLifecycleRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketLifecycleAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not (StorageErrorCode.LifecycleConfigurationNotFound or StorageErrorCode.BucketNotFound)) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket lifecycle delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketLifecycleAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketLifecycleAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketLifecycleAsync(replicaBackend, new PutBucketLifecycleRequest
            {
                BucketName = bucketName,
                Rules = primaryResult.Value.Rules
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.LifecycleConfigurationNotFound) {
            return await WriteReplicaDeleteBucketLifecycleAsync(replicaBackend, new DeleteBucketLifecycleRequest
            {
                BucketName = bucketName
            }, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket lifecycle configuration could not be resolved for replica repair.");
    }

    // ── Bucket Replication replication helpers ───────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketReplicationAsync(IStorageBackend replicaBackend, PutBucketReplicationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketReplicationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket replication configuration update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketReplicationAsync(IStorageBackend replicaBackend, DeleteBucketReplicationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketReplicationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not (StorageErrorCode.ReplicationConfigurationNotFound or StorageErrorCode.BucketNotFound)) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket replication configuration delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketReplicationAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketReplicationAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketReplicationAsync(replicaBackend, new PutBucketReplicationRequest
            {
                BucketName = bucketName,
                Role = primaryResult.Value.Role,
                Rules = primaryResult.Value.Rules
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.ReplicationConfigurationNotFound) {
            return await WriteReplicaDeleteBucketReplicationAsync(replicaBackend, new DeleteBucketReplicationRequest
            {
                BucketName = bucketName
            }, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket replication configuration could not be resolved for replica repair.");
    }

    // ── Bucket Notification replication helpers ──────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketNotificationConfigurationAsync(IStorageBackend replicaBackend, PutBucketNotificationConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketNotificationConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket notification configuration update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketNotificationConfigurationAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketNotificationConfigurationAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketNotificationConfigurationAsync(replicaBackend, new PutBucketNotificationConfigurationRequest
            {
                BucketName = bucketName,
                TopicConfigurations = primaryResult.Value.TopicConfigurations,
                QueueConfigurations = primaryResult.Value.QueueConfigurations,
                LambdaFunctionConfigurations = primaryResult.Value.LambdaFunctionConfigurations
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket notification configuration could not be resolved for replica repair.");
    }

    // ── Object Lock Configuration replication helpers ────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutObjectLockConfigurationAsync(IStorageBackend replicaBackend, PutObjectLockConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutObjectLockConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica object lock configuration update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaObjectLockConfigurationAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetObjectLockConfigurationAsync(bucketName, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutObjectLockConfigurationAsync(replicaBackend, new PutObjectLockConfigurationRequest
            {
                BucketName = bucketName,
                ObjectLockEnabled = primaryResult.Value.ObjectLockEnabled,
                DefaultRetention = primaryResult.Value.DefaultRetention
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.ObjectLockConfigurationNotFound) {
            return await WriteReplicaPutObjectLockConfigurationAsync(replicaBackend, new PutObjectLockConfigurationRequest
            {
                BucketName = bucketName,
                ObjectLockEnabled = false
            }, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary object lock configuration could not be resolved for replica repair.");
    }

    // ── Bucket Analytics replication helpers ─────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketAnalyticsConfigurationAsync(IStorageBackend replicaBackend, PutBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketAnalyticsConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket analytics configuration update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketAnalyticsConfigurationAsync(IStorageBackend replicaBackend, DeleteBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketAnalyticsConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not StorageErrorCode.BucketNotFound) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket analytics configuration delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketAnalyticsConfigurationAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        string id,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketAnalyticsConfigurationAsync(bucketName, id, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketAnalyticsConfigurationAsync(replicaBackend, new PutBucketAnalyticsConfigurationRequest
            {
                BucketName = bucketName,
                Id = id,
                FilterPrefix = primaryResult.Value.FilterPrefix,
                FilterTags = primaryResult.Value.FilterTags,
                StorageClassAnalysis = primaryResult.Value.StorageClassAnalysis
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket analytics configuration could not be resolved for replica repair.");
    }

    // ── Bucket Metrics replication helpers ───────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketMetricsConfigurationAsync(IStorageBackend replicaBackend, PutBucketMetricsConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketMetricsConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket metrics configuration update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketMetricsConfigurationAsync(IStorageBackend replicaBackend, DeleteBucketMetricsConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketMetricsConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not StorageErrorCode.BucketNotFound) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket metrics configuration delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketMetricsConfigurationAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        string id,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketMetricsConfigurationAsync(bucketName, id, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketMetricsConfigurationAsync(replicaBackend, new PutBucketMetricsConfigurationRequest
            {
                BucketName = bucketName,
                Id = id,
                Filter = primaryResult.Value.Filter
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket metrics configuration could not be resolved for replica repair.");
    }

    // ── Bucket Inventory replication helpers ─────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketInventoryConfigurationAsync(IStorageBackend replicaBackend, PutBucketInventoryConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketInventoryConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket inventory configuration update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketInventoryConfigurationAsync(IStorageBackend replicaBackend, DeleteBucketInventoryConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketInventoryConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not StorageErrorCode.BucketNotFound) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket inventory configuration delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketInventoryConfigurationAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        string id,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketInventoryConfigurationAsync(bucketName, id, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketInventoryConfigurationAsync(replicaBackend, new PutBucketInventoryConfigurationRequest
            {
                BucketName = bucketName,
                Id = id,
                IsEnabled = primaryResult.Value.IsEnabled,
                Destination = primaryResult.Value.Destination,
                Schedule = primaryResult.Value.Schedule,
                Filter = primaryResult.Value.Filter,
                IncludedObjectVersions = primaryResult.Value.IncludedObjectVersions,
                OptionalFields = primaryResult.Value.OptionalFields
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket inventory configuration could not be resolved for replica repair.");
    }

    // ── Bucket Intelligent-Tiering replication helpers ───────────────────

    private async ValueTask<StorageError?> WriteReplicaPutBucketIntelligentTieringConfigurationAsync(IStorageBackend replicaBackend, PutBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutBucketIntelligentTieringConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket intelligent-tiering configuration update did not return configuration metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> WriteReplicaDeleteBucketIntelligentTieringConfigurationAsync(IStorageBackend replicaBackend, DeleteBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.DeleteBucketIntelligentTieringConfigurationAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess && replicaResult.Error?.Code is not StorageErrorCode.BucketNotFound) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, objectKey: null, versionId: null, message: "Replica bucket intelligent-tiering configuration delete did not succeed.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaBucketIntelligentTieringConfigurationAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        string id,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetBucketIntelligentTieringConfigurationAsync(bucketName, id, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (primaryResult.IsSuccess && primaryResult.Value is not null) {
            return await WriteReplicaPutBucketIntelligentTieringConfigurationAsync(replicaBackend, new PutBucketIntelligentTieringConfigurationRequest
            {
                BucketName = bucketName,
                Id = id,
                Status = primaryResult.Value.Status,
                Filter = primaryResult.Value.Filter,
                Tierings = primaryResult.Value.Tierings
            }, cancellationToken);
        }

        if (primaryResult.Error?.Code == StorageErrorCode.BucketNotFound) {
            return await RepairReplicaBucketDeleteAsync(replicaBackend, bucketName, cancellationToken);
        }

        return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, objectKey: null, versionId: null, message: "Primary bucket intelligent-tiering configuration could not be resolved for replica repair.");
    }

    // ── Object Retention replication helpers ─────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutObjectRetentionAsync(IStorageBackend replicaBackend, PutObjectRetentionRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutObjectRetentionAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, request.Key, request.VersionId, "Replica object retention update did not return retention metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaObjectRetentionFromPrimaryAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        string key,
        string? requestedVersionId,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetObjectRetentionAsync(new GetObjectRetentionRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = requestedVersionId
        }, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (!primaryResult.IsSuccess || primaryResult.Value is null) {
            if (primaryResult.Error?.Code is StorageErrorCode.ObjectNotFound or StorageErrorCode.BucketNotFound) {
                return await WriteReplicaDeleteObjectAsync(replicaBackend, new DeleteObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = requestedVersionId
                }, cancellationToken);
            }

            return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, key, requestedVersionId, "Primary object retention could not be resolved for replica repair.");
        }

        return await WriteReplicaPutObjectRetentionAsync(replicaBackend, new PutObjectRetentionRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = requestedVersionId,
            Mode = primaryResult.Value.Mode,
            RetainUntilDateUtc = primaryResult.Value.RetainUntilDateUtc
        }, cancellationToken);
    }

    // ── Object Legal Hold replication helpers ────────────────────────────

    private async ValueTask<StorageError?> WriteReplicaPutObjectLegalHoldAsync(IStorageBackend replicaBackend, PutObjectLegalHoldRequest request, CancellationToken cancellationToken)
    {
        var replicaResult = await replicaBackend.PutObjectLegalHoldAsync(request, cancellationToken);
        ObserveResult(replicaBackend, replicaResult);
        if (!replicaResult.IsSuccess || replicaResult.Value is null) {
            return replicaResult.Error ?? CreateReplicaOperationError(replicaBackend, request.BucketName, request.Key, request.VersionId, "Replica object legal hold update did not return legal hold metadata.");
        }

        return null;
    }

    private async ValueTask<StorageError?> RepairReplicaObjectLegalHoldFromPrimaryAsync(
        IStorageBackend primaryBackend,
        IStorageBackend replicaBackend,
        string bucketName,
        string key,
        string? requestedVersionId,
        CancellationToken cancellationToken)
    {
        var primaryResult = await primaryBackend.GetObjectLegalHoldAsync(new GetObjectLegalHoldRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = requestedVersionId
        }, cancellationToken);
        ObserveResult(primaryBackend, primaryResult);
        if (!primaryResult.IsSuccess || primaryResult.Value is null) {
            if (primaryResult.Error?.Code is StorageErrorCode.ObjectNotFound or StorageErrorCode.BucketNotFound) {
                return await WriteReplicaDeleteObjectAsync(replicaBackend, new DeleteObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = requestedVersionId
                }, cancellationToken);
            }

            return primaryResult.Error ?? CreatePrimaryReplicationSourceError(primaryBackend, bucketName, key, requestedVersionId, "Primary object legal hold could not be resolved for replica repair.");
        }

        return await WriteReplicaPutObjectLegalHoldAsync(replicaBackend, new PutObjectLegalHoldRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = requestedVersionId,
            Status = primaryResult.Value.Status ?? ObjectLegalHoldStatus.Off
        }, cancellationToken);
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

    private async ValueTask<bool> CanCoalesceReplicaRepairsAcrossVersionsAsync(
        IStorageBackend primaryBackend,
        StorageOperationType operation,
        string bucketName,
        string? objectKey,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(objectKey)) {
            return true;
        }

        if (operation is not (
            StorageOperationType.CopyObject
            or StorageOperationType.DeleteObject
            or StorageOperationType.DeleteObjectTags
            or StorageOperationType.PutObject
            or StorageOperationType.PutObjectTags)) {
            return false;
        }

        var versioningResult = await primaryBackend.GetBucketVersioningAsync(bucketName, cancellationToken);
        if (!versioningResult.IsSuccess || versioningResult.Value is null) {
            logger.LogDebug(
                "Skipping replica repair coalescing across versions for bucket {BucketName} because versioning state could not be resolved. ErrorCode {ErrorCode}.",
                bucketName,
                versioningResult.Error?.Code);
            return false;
        }

        return versioningResult.Value.Status == BucketVersioningStatus.Disabled;
    }

    private async ValueTask CompleteSupersededReplicaRepairsAsync(
        StorageReplicaRepairEntry repairEntry,
        IReadOnlyList<StorageReplicaRepairEntry> supersededRepairs,
        CancellationToken cancellationToken)
    {
        foreach (var supersededRepair in supersededRepairs) {
            logger.LogInformation(
                "Coalescing superseded replica repair {SupersededRepairId} for {ReplicaProvider} into repair {RepairId}.",
                supersededRepair.Id,
                repairEntry.ReplicaBackendName,
                repairEntry.Id);
            await replicaRepairBacklog.MarkCompletedAsync(supersededRepair.Id, cancellationToken);
        }
    }

    private static StorageReplicaRepairEntry[] GetSupersededReplicaRepairs(
        IReadOnlyList<StorageReplicaRepairEntry> outstandingRepairs,
        StorageReplicaRepairEntry repairEntry,
        bool coalesceAcrossVersions)
    {
        return outstandingRepairs
            .Where(existingRepair => IsSupersededReplicaRepair(existingRepair, repairEntry, coalesceAcrossVersions))
            .ToArray();
    }

    private static bool IsSupersededReplicaRepair(
        StorageReplicaRepairEntry existingRepair,
        StorageReplicaRepairEntry repairEntry,
        bool coalesceAcrossVersions)
    {
        if (!string.Equals(existingRepair.ReplicaBackendName, repairEntry.ReplicaBackendName, StringComparison.Ordinal)
            || existingRepair.Operation != repairEntry.Operation
            || !string.Equals(existingRepair.BucketName, repairEntry.BucketName, StringComparison.Ordinal)
            || !string.Equals(existingRepair.ObjectKey, repairEntry.ObjectKey, StringComparison.Ordinal)) {
            return false;
        }

        return string.Equals(existingRepair.VersionId, repairEntry.VersionId, StringComparison.Ordinal)
            || coalesceAcrossVersions;
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

    private static BucketDefaultEncryptionRule CloneBucketDefaultEncryptionRule(BucketDefaultEncryptionRule rule)
    {
        return new BucketDefaultEncryptionRule
        {
            Algorithm = rule.Algorithm,
            KeyId = rule.KeyId
        };
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

