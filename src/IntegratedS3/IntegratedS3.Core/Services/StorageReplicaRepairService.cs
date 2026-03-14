using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

internal sealed class StorageReplicaRepairService(
    IEnumerable<IStorageBackend> backends,
    IStorageCatalogStore catalogStore,
    StorageBackendHealthMonitor backendHealthMonitor) : IStorageReplicaRepairService
{
    private readonly IStorageBackend[] _backends = backends.ToArray();

    public async ValueTask<StorageError?> RepairAsync(StorageReplicaRepairEntry entry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var primaryBackend = ResolveBackend(entry.PrimaryBackendName);
        if (primaryBackend is null) {
            return CreateMissingBackendError(entry.PrimaryBackendName, entry.BucketName, entry.ObjectKey, entry.VersionId);
        }

        var replicaBackend = ResolveBackend(entry.ReplicaBackendName);
        if (replicaBackend is null) {
            return CreateMissingBackendError(entry.ReplicaBackendName, entry.BucketName, entry.ObjectKey, entry.VersionId);
        }

        return entry.Operation switch
        {
            StorageOperationType.CreateBucket => await RepairReplicaBucketCreateAsync(primaryBackend, replicaBackend, entry.BucketName, cancellationToken),
            StorageOperationType.PutBucketVersioning => await RepairReplicaBucketVersioningAsync(primaryBackend, replicaBackend, entry.BucketName, cancellationToken),
            StorageOperationType.PutBucketCors or StorageOperationType.DeleteBucketCors => await RepairReplicaBucketCorsAsync(primaryBackend, replicaBackend, entry.BucketName, cancellationToken),
            StorageOperationType.DeleteBucket => await RepairReplicaBucketDeleteAsync(replicaBackend, entry.BucketName, cancellationToken),
            StorageOperationType.CopyObject or StorageOperationType.PutObject => string.IsNullOrWhiteSpace(entry.ObjectKey)
                ? CreateInvalidRepairTargetError(entry, "Object repairs require an object key.")
                : await RepairReplicaObjectFromPrimaryAsync(primaryBackend, replicaBackend, entry.BucketName, entry.ObjectKey, entry.VersionId, cancellationToken),
            StorageOperationType.PutObjectTags or StorageOperationType.DeleteObjectTags => string.IsNullOrWhiteSpace(entry.ObjectKey)
                ? CreateInvalidRepairTargetError(entry, "Object tag repairs require an object key.")
                : await RepairReplicaObjectTagsFromPrimaryAsync(primaryBackend, replicaBackend, entry.BucketName, entry.ObjectKey, entry.VersionId, cancellationToken),
            StorageOperationType.DeleteObject => string.IsNullOrWhiteSpace(entry.ObjectKey)
                ? CreateInvalidRepairTargetError(entry, "Object delete repairs require an object key.")
                : await RepairReplicaDeleteObjectAsync(replicaBackend, new DeleteObjectRequest
                {
                    BucketName = entry.BucketName,
                    Key = entry.ObjectKey,
                    VersionId = entry.VersionId
                }, cancellationToken),
            _ => CreateUnsupportedRepairError(entry)
        };
    }

    private IStorageBackend? ResolveBackend(string backendName)
    {
        return _backends.FirstOrDefault(backend => string.Equals(backend.Name, backendName, StringComparison.Ordinal));
    }

    private void ObserveResult(IStorageBackend backend, StorageResult result)
    {
        if (result.IsSuccess) {
            backendHealthMonitor.ReportSuccess(backend);
            return;
        }

        backendHealthMonitor.ReportFailure(backend, result.Error);
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

    private async ValueTask<StorageError?> RepairReplicaDeleteObjectAsync(IStorageBackend replicaBackend, DeleteObjectRequest request, CancellationToken cancellationToken)
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

    private static StorageError CreateMissingBackendError(string backendName, string bucketName, string? objectKey, string? versionId)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = $"Replica repair could not be executed because provider '{backendName}' is not registered.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            VersionId = versionId,
            ProviderName = backendName,
            SuggestedHttpStatusCode = 503
        };
    }

    private static StorageError CreateInvalidRepairTargetError(StorageReplicaRepairEntry entry, string message)
    {
        return new StorageError
        {
            Code = StorageErrorCode.Unknown,
            Message = message,
            BucketName = entry.BucketName,
            ObjectKey = entry.ObjectKey,
            VersionId = entry.VersionId,
            ProviderName = entry.ReplicaBackendName,
            SuggestedHttpStatusCode = 400
        };
    }

    private static StorageError CreateUnsupportedRepairError(StorageReplicaRepairEntry entry)
    {
        return new StorageError
        {
            Code = StorageErrorCode.UnsupportedCapability,
            Message = $"Storage operation '{entry.Operation}' does not currently expose host-invokable replica repair semantics.",
            BucketName = entry.BucketName,
            ObjectKey = entry.ObjectKey,
            VersionId = entry.VersionId,
            ProviderName = entry.ReplicaBackendName,
            SuggestedHttpStatusCode = 501
        };
    }
}
