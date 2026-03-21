using Microsoft.AspNetCore.Http;

namespace IntegratedS3.AspNetCore.Services;

/// <summary>
/// Authenticates incoming HTTP requests for IntegratedS3 endpoints.
/// The default implementation performs AWS Signature V4/V4a verification.
/// Register a custom implementation to support alternative authentication schemes.
/// </summary>
public interface IIntegratedS3RequestAuthenticator
{
    /// <summary>
    /// Authenticates the given HTTP request and returns the authentication result.
    /// </summary>
    /// <param name="httpContext">The <see cref="HttpContext"/> of the incoming request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// An <see cref="IntegratedS3RequestAuthenticationResult"/> indicating the outcome of the
    /// authentication attempt.
    /// </returns>
    ValueTask<IntegratedS3RequestAuthenticationResult> AuthenticateAsync(HttpContext httpContext, CancellationToken cancellationToken = default);
}
