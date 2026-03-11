using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Services;

public interface IStorageMultipartStateStore
{
    StorageSupportStateOwnership Ownership { get; }

    ValueTask<MultipartUploadState?> GetMultipartUploadStateAsync(
        string providerName,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<MultipartUploadState> ListMultipartUploadStatesAsync(
        string providerName,
        string bucketName,
        string? prefix = null,
        CancellationToken cancellationToken = default);

    ValueTask UpsertMultipartUploadStateAsync(
        string providerName,
        MultipartUploadState state,
        CancellationToken cancellationToken = default);

    ValueTask RemoveMultipartUploadStateAsync(
        string providerName,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default);
}
