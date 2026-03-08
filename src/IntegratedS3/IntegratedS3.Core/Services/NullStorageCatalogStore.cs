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

    public ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<StoredObjectEntry>> ListObjectsAsync(string? providerName = null, string? bucketName = null, CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult<IReadOnlyList<StoredObjectEntry>>([]);
    }
}