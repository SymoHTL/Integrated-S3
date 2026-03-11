using System.Security.Claims;
using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.Services;

internal sealed class ConfiguredIntegratedS3PresignCredentialResolver(IOptions<IntegratedS3Options> options) : IIntegratedS3PresignCredentialResolver
{
    private const string AccessKeyClaimType = "integrateds3:access-key-id";

    public ValueTask<IntegratedS3PresignCredentialResolutionResult> ResolveAsync(
        ClaimsPrincipal principal,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principal);
        cancellationToken.ThrowIfCancellationRequested();

        var settings = options.Value;
        if (principal.FindFirst(AccessKeyClaimType)?.Value is { Length: > 0 } claimedAccessKeyId) {
            return ValueTask.FromResult(TryResolveByAccessKeyId(
                settings,
                claimedAccessKeyId,
                $"The current principal references access key '{claimedAccessKeyId}', but that credential is not configured."));
        }

        if (!string.IsNullOrWhiteSpace(settings.PresignAccessKeyId)) {
            return ValueTask.FromResult(TryResolveByAccessKeyId(
                settings,
                settings.PresignAccessKeyId,
                $"The configured presign access key '{settings.PresignAccessKeyId}' was not found."));
        }

        if (settings.AccessKeyCredentials.Count == 1) {
            return ValueTask.FromResult(IntegratedS3PresignCredentialResolutionResult.Success(settings.AccessKeyCredentials[0]));
        }

        if (settings.AccessKeyCredentials.Count == 0) {
            return ValueTask.FromResult(IntegratedS3PresignCredentialResolutionResult.Failure(
                "No access key credentials are configured for first-party presign generation."));
        }

        return ValueTask.FromResult(IntegratedS3PresignCredentialResolutionResult.Failure(
            "Multiple access key credentials are configured. Set PresignAccessKeyId or register a custom IIntegratedS3PresignCredentialResolver."));
    }

    private static IntegratedS3PresignCredentialResolutionResult TryResolveByAccessKeyId(
        IntegratedS3Options options,
        string accessKeyId,
        string failureMessage)
    {
        var credential = options.AccessKeyCredentials.FirstOrDefault(candidate =>
            string.Equals(candidate.AccessKeyId, accessKeyId, StringComparison.Ordinal));

        return credential is not null
            ? IntegratedS3PresignCredentialResolutionResult.Success(credential)
            : IntegratedS3PresignCredentialResolutionResult.Failure(failureMessage);
    }
}
