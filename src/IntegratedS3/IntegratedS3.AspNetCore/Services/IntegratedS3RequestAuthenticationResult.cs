using System.Security.Claims;

namespace IntegratedS3.AspNetCore.Services;

public sealed class IntegratedS3RequestAuthenticationResult
{
    public static IntegratedS3RequestAuthenticationResult NoResult() => new();

    public static IntegratedS3RequestAuthenticationResult Success(ClaimsPrincipal principal) => new()
    {
        HasAttemptedAuthentication = true,
        Succeeded = true,
        Principal = principal
    };

    public static IntegratedS3RequestAuthenticationResult Failure(string errorCode, string errorMessage, int statusCode = 403) => new()
    {
        HasAttemptedAuthentication = true,
        ErrorCode = errorCode,
        ErrorMessage = errorMessage,
        StatusCode = statusCode
    };

    public bool HasAttemptedAuthentication { get; init; }

    public bool Succeeded { get; init; }

    public ClaimsPrincipal? Principal { get; init; }

    public string? ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    public int StatusCode { get; init; } = 403;
}
