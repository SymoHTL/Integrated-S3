using IntegratedS3.Abstractions.Models;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Persistence contract for the IntegratedS3 catalog, storing bucket and object metadata
/// across one or more storage providers.
/// </summary>
public interface IStorageCatalogStore
{
    /// <summary>
    /// Creates or updates the catalog entry for a bucket on the specified provider.
    /// </summary>
    /// <param name="providerName">The storage provider that owns the bucket.</param>
    /// <param name="bucket">The <see cref="BucketInfo"/> describing the bucket to upsert.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask UpsertBucketAsync(string providerName, BucketInfo bucket, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the catalog entry for a bucket on the specified provider.
    /// </summary>
    /// <param name="providerName">The storage provider that owns the bucket.</param>
    /// <param name="bucketName">The name of the bucket to remove.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask RemoveBucketAsync(string providerName, string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists bucket entries in the catalog, optionally filtered by provider.
    /// </summary>
    /// <param name="providerName">If specified, limits results to this provider; otherwise lists all providers.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A read-only list of <see cref="StoredBucketEntry"/> instances.</returns>
    ValueTask<IReadOnlyList<StoredBucketEntry>> ListBucketsAsync(string? providerName = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or updates the catalog entry for an object on the specified provider.
    /// </summary>
    /// <param name="providerName">The storage provider that owns the object.</param>
    /// <param name="object">The <see cref="ObjectInfo"/> describing the object to upsert.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask UpsertObjectAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the catalog entry for an object on the specified provider.
    /// </summary>
    /// <param name="providerName">The storage provider that owns the object.</param>
    /// <param name="bucketName">The bucket containing the object.</param>
    /// <param name="key">The object key.</param>
    /// <param name="versionId">The specific version to remove, or <see langword="null"/> for the latest version.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists object entries in the catalog, optionally filtered by provider and bucket.
    /// </summary>
    /// <param name="providerName">If specified, limits results to this provider.</param>
    /// <param name="bucketName">If specified, limits results to this bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A read-only list of <see cref="StoredObjectEntry"/> instances.</returns>
    ValueTask<IReadOnlyList<StoredObjectEntry>> ListObjectsAsync(string? providerName = null, string? bucketName = null, CancellationToken cancellationToken = default);
}
