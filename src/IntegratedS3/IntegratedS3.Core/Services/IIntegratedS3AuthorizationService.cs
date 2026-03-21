using System.Security.Claims;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Defines the authorization contract for storage operations within IntegratedS3.
/// </summary>
public interface IIntegratedS3AuthorizationService
{
    /// <summary>
    /// Determines whether the specified principal is authorized to perform the requested storage operation.
    /// </summary>
    /// <param name="principal">The <see cref="ClaimsPrincipal"/> representing the caller.</param>
    /// <param name="request">The <see cref="StorageAuthorizationRequest"/> describing the operation to authorize.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or the reason for denial.</returns>
    ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default);
}