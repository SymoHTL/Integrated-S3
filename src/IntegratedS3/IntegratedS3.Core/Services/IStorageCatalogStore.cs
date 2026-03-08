using IntegratedS3.Abstractions.Models;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

public interface IStorageCatalogStore
{
    ValueTask UpsertBucketAsync(string providerName, BucketInfo bucket, CancellationToken cancellationToken = default);

    ValueTask RemoveBucketAsync(string providerName, string bucketName, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StoredBucketEntry>> ListBucketsAsync(string? providerName = null, CancellationToken cancellationToken = default);

    ValueTask UpsertObjectAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default);

    ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StoredObjectEntry>> ListObjectsAsync(string? providerName = null, string? bucketName = null, CancellationToken cancellationToken = default);
}
