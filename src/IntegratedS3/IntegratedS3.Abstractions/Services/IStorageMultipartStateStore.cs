using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Services;

/// <summary>
/// Defines the contract for persisting and querying in-progress multipart upload state.
/// </summary>
public interface IStorageMultipartStateStore
{
    /// <summary>
    /// Gets a value indicating who owns the multipart upload state data.
    /// </summary>
    StorageSupportStateOwnership Ownership { get; }

    /// <summary>
    /// Retrieves the state for a specific multipart upload.
    /// </summary>
    /// <param name="providerName">The backend provider name.</param>
    /// <param name="bucketName">The name of the bucket containing the upload.</param>
    /// <param name="key">The object key of the upload.</param>
    /// <param name="uploadId">The multipart upload identifier.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The stored <see cref="MultipartUploadState"/>, or <c>null</c> if not found.</returns>
    ValueTask<MultipartUploadState?> GetMultipartUploadStateAsync(
        string providerName,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates active multipart uploads within a bucket, optionally filtered by prefix.
    /// </summary>
    /// <param name="providerName">The backend provider name.</param>
    /// <param name="bucketName">The name of the bucket to list.</param>
    /// <param name="prefix">An optional key prefix filter.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An asynchronous sequence of <see cref="MultipartUploadState"/> entries.</returns>
    IAsyncEnumerable<MultipartUploadState> ListMultipartUploadStatesAsync(
        string providerName,
        string bucketName,
        string? prefix = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or updates the state for a multipart upload.
    /// </summary>
    /// <param name="providerName">The backend provider name.</param>
    /// <param name="state">The <see cref="MultipartUploadState"/> to persist.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask UpsertMultipartUploadStateAsync(
        string providerName,
        MultipartUploadState state,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes state for a completed or aborted multipart upload.
    /// </summary>
    /// <param name="providerName">The backend provider name.</param>
    /// <param name="bucketName">The name of the bucket containing the upload.</param>
    /// <param name="key">The object key of the upload.</param>
    /// <param name="uploadId">The multipart upload identifier.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask RemoveMultipartUploadStateAsync(
        string providerName,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default);
}
