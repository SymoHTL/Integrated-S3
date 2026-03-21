using System.Diagnostics.CodeAnalysis;
using IntegratedS3.Protocol;
using Microsoft.AspNetCore.Http;

namespace IntegratedS3.AspNetCore.Services;

internal sealed class AwsChunkedTrailerSigningContext
{
    public required S3SigV4CredentialScope CredentialScope { get; init; }

    public required DateTimeOffset SignedAtUtc { get; init; }

    public required string SecretAccessKey { get; init; }

    /// <summary>
    /// When true the trailer signature must be verified with ECDSA (SigV4a) instead of HMAC (SigV4).
    /// </summary>
    public bool IsSigV4a { get; init; }

    /// <summary>
    /// The access key ID, required for SigV4a ECDSA key derivation.
    /// </summary>
    public string? AccessKeyId { get; init; }
}

internal static class AwsChunkedTrailerSigningContextStore
{
    private static readonly object ItemKey = new();

    public static void Set(HttpContext httpContext, AwsChunkedTrailerSigningContext context)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(context);

        httpContext.Items[ItemKey] = context;
    }

    public static bool TryGet(HttpContext httpContext, [NotNullWhen(true)] out AwsChunkedTrailerSigningContext? context)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        if (httpContext.Items.TryGetValue(ItemKey, out var value)
            && value is AwsChunkedTrailerSigningContext signingContext) {
            context = signingContext;
            return true;
        }

        context = null;
        return false;
    }
}
