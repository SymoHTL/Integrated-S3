using IntegratedS3.Abstractions.Models;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

internal sealed class NullStorageCatalogStore : IStorageCatalogStore
{
    public ValueTask UpsertBucketAsync(string providerName, BucketInfo bucket, CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask RemoveBucketAsync(string providerName, string bucketName, CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<StoredBucketEntry>> ListBucketsAsync(string? providerName = null, CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult<IReadOnlyList<StoredBucketEntry>>([]);
    }

    public ValueTask UpsertObjectAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask RecordReplicaVersionMappingAsync(string replicaProviderName, string bucketName, string key, string primaryVersionId, string replicaVersionId, CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask<string?> GetReplicaVersionIdForPrimaryAsync(string replicaProviderName, string bucketName, string key, string primaryVersionId, CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult<string?>(null);
    }

    public ValueTask<StoredObjectEntry?> GetObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult<StoredObjectEntry?>(null);
    }

    public ValueTask<IReadOnlyList<StoredObjectEntry>> ListObjectsAsync(string? providerName = null, string? bucketName = null, string? keyPrefix = null, CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult<IReadOnlyList<StoredObjectEntry>>([]);
    }
}