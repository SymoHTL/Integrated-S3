using System.Security.Claims;

namespace IntegratedS3.AspNetCore.Services;

/// <summary>
/// Result of an IntegratedS3 request authentication attempt.
/// Use the <see cref="NoResult"/>, <see cref="Success"/>, or <see cref="Failure"/> factory methods
/// to create instances.
/// </summary>
public sealed class IntegratedS3RequestAuthenticationResult
{
    /// <summary>
    /// Creates a result indicating no authentication was attempted (passthrough).
    /// </summary>
    /// <returns>An <see cref="IntegratedS3RequestAuthenticationResult"/> with no authentication attempt.</returns>
    public static IntegratedS3RequestAuthenticationResult NoResult() => new();

    /// <summary>
    /// Creates a successful authentication result with an authenticated <see cref="ClaimsPrincipal"/>.
    /// </summary>
    /// <param name="principal">The authenticated <see cref="ClaimsPrincipal"/>.</param>
    /// <returns>A successful <see cref="IntegratedS3RequestAuthenticationResult"/>.</returns>
    public static IntegratedS3RequestAuthenticationResult Success(ClaimsPrincipal principal) => new()
    {
        HasAttemptedAuthentication = true,
        Succeeded = true,
        Principal = principal
    };

    /// <summary>
    /// Creates a failed authentication result with an S3-compatible error code, message, and HTTP status code.
    /// </summary>
    /// <param name="errorCode">An S3-compatible error code (e.g., <c>"AccessDenied"</c>, <c>"SignatureDoesNotMatch"</c>).</param>
    /// <param name="errorMessage">A human-readable error message describing the failure.</param>
    /// <param name="statusCode">The HTTP status code to return on failure. Defaults to 403.</param>
    /// <returns>A failed <see cref="IntegratedS3RequestAuthenticationResult"/>.</returns>
    public static IntegratedS3RequestAuthenticationResult Failure(string errorCode, string errorMessage, int statusCode = 403) => new()
    {
        HasAttemptedAuthentication = true,
        ErrorCode = errorCode,
        ErrorMessage = errorMessage,
        StatusCode = statusCode
    };

    /// <summary>
    /// Gets a value indicating whether the authenticator attempted authentication (versus skipping the request).
    /// </summary>
    public bool HasAttemptedAuthentication { get; init; }

    /// <summary>
    /// Gets a value indicating whether authentication succeeded.
    /// </summary>
    public bool Succeeded { get; init; }

    /// <summary>
    /// Gets the authenticated principal on success, or <see langword="null"/> otherwise.
    /// </summary>
    public ClaimsPrincipal? Principal { get; init; }

    /// <summary>
    /// Gets the S3-compatible error code on failure (e.g., <c>"AccessDenied"</c>, <c>"SignatureDoesNotMatch"</c>),
    /// or <see langword="null"/> on success.
    /// </summary>
    public string? ErrorCode { get; init; }

    /// <summary>
    /// Gets a human-readable error message on failure, or <see langword="null"/> on success.
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// Gets the HTTP status code to return on failure. Defaults to 403.
    /// </summary>
    public int StatusCode { get; init; } = 403;
}
