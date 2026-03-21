using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Services;

/// <summary>
/// Defines the contract for persisting and querying object metadata state used by the orchestration layer.
/// </summary>
public interface IStorageObjectStateStore
{
    /// <summary>
    /// Gets a value indicating who owns the object metadata state data.
    /// </summary>
    StorageSupportStateOwnership Ownership { get; }

    /// <summary>
    /// Retrieves stored metadata for a specific object version.
    /// </summary>
    /// <param name="providerName">The backend provider name.</param>
    /// <param name="bucketName">The name of the bucket containing the object.</param>
    /// <param name="key">The object key.</param>
    /// <param name="versionId">An optional specific version identifier.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The stored <see cref="ObjectInfo"/>, or <c>null</c> if not found.</returns>
    ValueTask<ObjectInfo?> GetObjectInfoAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates all stored object versions within a bucket, optionally filtered by prefix.
    /// </summary>
    /// <param name="providerName">The backend provider name.</param>
    /// <param name="bucketName">The name of the bucket to list.</param>
    /// <param name="prefix">An optional key prefix filter.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An asynchronous sequence of <see cref="ObjectInfo"/> entries.</returns>
    IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(string providerName, string bucketName, string? prefix = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or updates the stored metadata for an object.
    /// </summary>
    /// <param name="providerName">The backend provider name.</param>
    /// <param name="object">The <see cref="ObjectInfo"/> to persist.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask UpsertObjectInfoAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes stored metadata for an object version.
    /// </summary>
    /// <param name="providerName">The backend provider name.</param>
    /// <param name="bucketName">The name of the bucket containing the object.</param>
    /// <param name="key">The object key.</param>
    /// <param name="versionId">An optional specific version identifier.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask RemoveObjectInfoAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default);
}