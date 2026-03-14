using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Minimal first-party client for IntegratedS3 presign endpoints.
/// </summary>
public interface IIntegratedS3Client
{
    /// <summary>
    /// Requests a presigned object operation from the configured IntegratedS3 host.
    /// </summary>
    ValueTask<StoragePresignedRequest> PresignObjectAsync(
        StoragePresignRequest request,
        CancellationToken cancellationToken = default);
}
