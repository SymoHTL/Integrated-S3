using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.AspNetCore;

/// <summary>
/// Configuration options for the IntegratedS3 service.
/// </summary>
/// <remarks>
/// Bound from the <c>IConfiguration</c> "IntegratedS3" section or set programmatically
/// via <c>Action&lt;IntegratedS3Options&gt;</c> during service registration.
/// </remarks>
public sealed class IntegratedS3Options
{
    /// <summary>
    /// Display name for the service instance. Defaults to <c>"Integrated S3"</c>.
    /// </summary>
    /// <remarks>
    /// Appears in service discovery and health check metadata.
    /// </remarks>
    public string ServiceName { get; set; } = "Integrated S3";

    /// <summary>
    /// URL path prefix for all S3-compatible endpoints. Defaults to <c>"/integrated-s3"</c>.
    /// </summary>
    /// <remarks>
    /// Must start with <c>"/"</c>.
    /// </remarks>
    public string RoutePrefix { get; set; } = "/integrated-s3";

    /// <summary>
    /// When <see langword="true"/>, enables AWS Signature V4 and V4a request authentication.
    /// </summary>
    /// <remarks>
    /// Required for presigned URL generation and S3-compatible SDK authentication.
    /// When enabled, configure <see cref="AccessKeyCredentials"/> with at least one credential pair.
    /// </remarks>
    public bool EnableAwsSignatureV4Authentication { get; set; }

    /// <summary>
    /// Expected AWS region in the SigV4 credential scope. Defaults to <c>"us-east-1"</c>.
    /// </summary>
    public string SignatureAuthenticationRegion { get; set; } = "us-east-1";

    /// <summary>
    /// Expected AWS service name in the SigV4 credential scope. Defaults to <c>"s3"</c>.
    /// </summary>
    public string SignatureAuthenticationService { get; set; } = "s3";

    /// <summary>
    /// Maximum clock skew tolerance in minutes for SigV4 authentication. Defaults to <c>5</c>.
    /// </summary>
    public int AllowedSignatureClockSkewMinutes { get; set; } = 5;

    /// <summary>
    /// Maximum allowed expiry for presigned URLs in seconds. Defaults to <c>3600</c> (1 hour).
    /// </summary>
    public int MaximumPresignedUrlExpirySeconds { get; set; } = 60 * 60;

    /// <summary>
    /// List of access key / secret key pairs for SigV4 authentication.
    /// </summary>
    /// <remarks>
    /// Each <see cref="IntegratedS3AccessKeyCredential"/> can optionally include a session token,
    /// display name, and scope restrictions.
    /// </remarks>
    public List<IntegratedS3AccessKeyCredential> AccessKeyCredentials { get; set; } = [];

    /// <summary>
    /// Access key ID to use for first-party presigned URL generation.
    /// </summary>
    /// <remarks>
    /// When <see langword="null"/>, the system auto-selects from <see cref="AccessKeyCredentials"/>.
    /// </remarks>
    public string? PresignAccessKeyId { get; set; }

    /// <summary>
    /// Public base URL for presigned URL generation when no active HTTP request is available.
    /// </summary>
    /// <remarks>
    /// Use this for background or non-request presign scenarios where the current request URL
    /// cannot be inferred.
    /// </remarks>
    public string? PresignPublicBaseUrl { get; set; }

    /// <summary>
    /// Enables virtual-hosted-style bucket addressing (e.g., <c>bucket.host.example.com</c>).
    /// </summary>
    /// <remarks>
    /// When enabled, configure <see cref="VirtualHostedStyleHostSuffixes"/> to define which host
    /// suffixes are recognized.
    /// </remarks>
    public bool EnableVirtualHostedStyleAddressing { get; set; }

    /// <summary>
    /// Host suffixes to recognize as virtual-hosted-style bucket requests.
    /// </summary>
    /// <remarks>
    /// For example, adding <c>"s3.example.com"</c> causes requests to <c>mybucket.s3.example.com</c>
    /// to be interpreted as targeting the <c>mybucket</c> bucket.
    /// Only evaluated when <see cref="EnableVirtualHostedStyleAddressing"/> is <see langword="true"/>.
    /// </remarks>
    public List<string> VirtualHostedStyleHostSuffixes { get; set; } = [];

    /// <summary>
    /// Configured storage provider descriptors.
    /// </summary>
    /// <remarks>
    /// These are typically populated from configuration. When storage backends are registered via DI,
    /// runtime provider information takes precedence over these descriptors.
    /// </remarks>
    public List<StorageProviderDescriptor> Providers { get; set; } = [];

    /// <summary>
    /// Declared storage capabilities when no backends are registered.
    /// </summary>
    /// <remarks>
    /// Overridden by backend-reported capabilities when backends are available.
    /// </remarks>
    public StorageCapabilities Capabilities { get; set; } = new();
}
