using System.Collections.Concurrent;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

internal sealed class InMemoryStorageAuthorizationCompatibilityService(OrchestratedStorageService storageService)
    : IStorageAuthorizationCompatibilityService
{
    private readonly ConcurrentDictionary<string, BucketCompatibilityState> _bucketStates = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<ObjectCompatibilityKey, StorageCannedAcl> _objectAcls = new();

    public ValueTask RecordBucketCreatedAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        cancellationToken.ThrowIfCancellationRequested();

        _bucketStates.AddOrUpdate(
            bucketName,
            static _ => new BucketCompatibilityState(StorageCannedAcl.Private, null),
            static (_, _) => new BucketCompatibilityState(StorageCannedAcl.Private, null));

        return ValueTask.CompletedTask;
    }

    public ValueTask RecordBucketDeletedAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        cancellationToken.ThrowIfCancellationRequested();

        _bucketStates.TryRemove(bucketName, out _);
        foreach (var objectKey in _objectAcls.Keys.Where(existing => string.Equals(existing.BucketName, bucketName, StringComparison.Ordinal)).ToArray()) {
            _objectAcls.TryRemove(objectKey, out _);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask RecordObjectWrittenAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        cancellationToken.ThrowIfCancellationRequested();

        _objectAcls[new ObjectCompatibilityKey(bucketName, key)] = StorageCannedAcl.Private;
        return ValueTask.CompletedTask;
    }

    public ValueTask RecordObjectDeletedAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        cancellationToken.ThrowIfCancellationRequested();

        _objectAcls.TryRemove(new ObjectCompatibilityKey(bucketName, key), out _);
        return ValueTask.CompletedTask;
    }

    public async ValueTask<StorageResult<StorageCannedAcl>> GetBucketAclAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        var exists = await EnsureBucketExistsAsync(bucketName, cancellationToken);
        if (!exists.IsSuccess) {
            return StorageResult<StorageCannedAcl>.Failure(exists.Error!);
        }

        return StorageResult<StorageCannedAcl>.Success(GetBucketState(bucketName).BucketAcl);
    }

    public async ValueTask<StorageResult> PutBucketAclAsync(PutBucketAclCompatibilityRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var exists = await EnsureBucketExistsAsync(request.BucketName, cancellationToken);
        if (!exists.IsSuccess) {
            return exists;
        }

        _bucketStates.AddOrUpdate(
            request.BucketName,
            _ => new BucketCompatibilityState(request.CannedAcl, null),
            (_, existing) => existing with { BucketAcl = request.CannedAcl });

        return StorageResult.Success();
    }

    public async ValueTask<StorageResult<StorageCannedAcl>> GetObjectAclAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        var exists = await EnsureObjectExistsAsync(bucketName, key, cancellationToken);
        if (!exists.IsSuccess) {
            return StorageResult<StorageCannedAcl>.Failure(exists.Error!);
        }

        return StorageResult<StorageCannedAcl>.Success(GetObjectAclValue(bucketName, key));
    }

    public async ValueTask<StorageResult> PutObjectAclAsync(PutObjectAclCompatibilityRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var exists = await EnsureObjectExistsAsync(request.BucketName, request.Key, cancellationToken);
        if (!exists.IsSuccess) {
            return exists;
        }

        _objectAcls[new ObjectCompatibilityKey(request.BucketName, request.Key)] = request.CannedAcl;
        return StorageResult.Success();
    }

    public async ValueTask<StorageResult<BucketPolicyCompatibilityDocument?>> GetBucketPolicyAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        var exists = await EnsureBucketExistsAsync(bucketName, cancellationToken);
        if (!exists.IsSuccess) {
            return StorageResult<BucketPolicyCompatibilityDocument?>.Failure(exists.Error!);
        }

        return StorageResult<BucketPolicyCompatibilityDocument?>.Success(GetBucketState(bucketName).Policy);
    }

    public async ValueTask<StorageResult> PutBucketPolicyAsync(PutBucketPolicyCompatibilityRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var exists = await EnsureBucketExistsAsync(request.BucketName, cancellationToken);
        if (!exists.IsSuccess) {
            return exists;
        }

        _bucketStates.AddOrUpdate(
            request.BucketName,
            _ => new BucketCompatibilityState(StorageCannedAcl.Private, request.Policy),
            (_, existing) => existing with { Policy = request.Policy });

        return StorageResult.Success();
    }

    public async ValueTask<StorageResult> DeleteBucketPolicyAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        var exists = await EnsureBucketExistsAsync(bucketName, cancellationToken);
        if (!exists.IsSuccess) {
            return exists;
        }

        _bucketStates.AddOrUpdate(
            bucketName,
            static _ => new BucketCompatibilityState(StorageCannedAcl.Private, null),
            static (_, existing) => existing with { Policy = null });

        return StorageResult.Success();
    }

    public ValueTask<bool> IsAllowedAsync(StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(request.BucketName)) {
            return ValueTask.FromResult(false);
        }

        var bucketState = GetBucketState(request.BucketName);
        var bucketAllowsPublicList = bucketState.BucketAcl == StorageCannedAcl.PublicRead
            || bucketState.Policy?.AllowsPublicList == true;

        switch (request.Operation) {
            case StorageOperationType.HeadBucket:
            case StorageOperationType.ListObjects:
                return ValueTask.FromResult(bucketAllowsPublicList);
            case StorageOperationType.GetObject:
            case StorageOperationType.HeadObject:
                if (bucketState.Policy?.AllowsPublicRead == true) {
                    return ValueTask.FromResult(true);
                }

                if (!string.IsNullOrWhiteSpace(request.VersionId) || string.IsNullOrWhiteSpace(request.Key)) {
                    return ValueTask.FromResult(false);
                }

                return ValueTask.FromResult(GetObjectAclValue(request.BucketName, request.Key) == StorageCannedAcl.PublicRead);
            default:
                return ValueTask.FromResult(false);
        }
    }

    private async ValueTask<StorageResult> EnsureBucketExistsAsync(string bucketName, CancellationToken cancellationToken)
    {
        var result = await storageService.HeadBucketAsync(bucketName, cancellationToken);
        return result.IsSuccess
            ? StorageResult.Success()
            : StorageResult.Failure(result.Error!);
    }

    private async ValueTask<StorageResult> EnsureObjectExistsAsync(string bucketName, string key, CancellationToken cancellationToken)
    {
        var result = await storageService.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = bucketName,
            Key = key
        }, cancellationToken);

        return result.IsSuccess
            ? StorageResult.Success()
            : StorageResult.Failure(result.Error!);
    }

    private BucketCompatibilityState GetBucketState(string bucketName)
    {
        return _bucketStates.TryGetValue(bucketName, out var state)
            ? state
            : new BucketCompatibilityState(StorageCannedAcl.Private, null);
    }

    private StorageCannedAcl GetObjectAclValue(string bucketName, string key)
    {
        return _objectAcls.TryGetValue(new ObjectCompatibilityKey(bucketName, key), out var cannedAcl)
            ? cannedAcl
            : StorageCannedAcl.Private;
    }

    private readonly record struct ObjectCompatibilityKey(string BucketName, string Key);

    private sealed record BucketCompatibilityState(StorageCannedAcl BucketAcl, BucketPolicyCompatibilityDocument? Policy);
}
