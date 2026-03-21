using IntegratedS3.Core.Models;

namespace IntegratedS3.Client;

/// <summary>
/// Client abstraction for communicating with an IntegratedS3 host to obtain presigned storage requests.
/// </summary>
public interface IIntegratedS3Client
{
    /// <summary>
    /// Sends a presign request to the IntegratedS3 host and returns the resulting presigned URL and headers.
    /// </summary>
    /// <param name="request">The <see cref="StoragePresignRequest"/> describing the operation, bucket, key, and options.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StoragePresignedRequest"/> containing the presigned URL, HTTP method, and required headers.</returns>
    ValueTask<StoragePresignedRequest> PresignObjectAsync(
        StoragePresignRequest request,
        CancellationToken cancellationToken = default);
}
