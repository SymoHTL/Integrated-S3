using System.Security.Claims;

namespace IntegratedS3.AspNetCore;

/// <summary>
/// Resolves the access key credential to use when generating first-party presigned URLs.
/// The default implementation selects from configured
/// <see cref="IntegratedS3AccessKeyCredential"/> instances.
/// Register a custom implementation to provide dynamic or per-user credential resolution.
/// </summary>
public interface IIntegratedS3PresignCredentialResolver
{
    /// <summary>
    /// Resolves credentials for presigned URL generation based on the current user principal.
    /// </summary>
    /// <param name="principal">The <see cref="ClaimsPrincipal"/> representing the current user.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// A <see cref="IntegratedS3PresignCredentialResolutionResult"/> indicating success or failure
    /// of the credential resolution.
    /// </returns>
    ValueTask<IntegratedS3PresignCredentialResolutionResult> ResolveAsync(
        ClaimsPrincipal principal,
        CancellationToken cancellationToken = default);
}
