using System.Security.Claims;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Strategy for generating presigned URLs, pluggable by storage provider.
/// Implementations translate a presign request into a provider-specific presigned URL.
/// </summary>
public interface IStoragePresignStrategy
{
    /// <summary>
    /// Generates a presigned URL for the specified object operation using the provider's signing mechanism.
    /// </summary>
    /// <param name="principal">The <see cref="ClaimsPrincipal"/> representing the caller.</param>
    /// <param name="request">The <see cref="StoragePresignRequest"/> describing the object and operation to presign.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="StoragePresignedRequest"/> on success.</returns>
    ValueTask<StorageResult<StoragePresignedRequest>> PresignObjectAsync(
        ClaimsPrincipal principal,
        StoragePresignRequest request,
        CancellationToken cancellationToken = default);
}
