using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.AspNetCore;

/// <summary>
/// Configures the public HTTP surface, authentication, presign defaults, and provider descriptors exposed by the ASP.NET host.
/// </summary>
public sealed class IntegratedS3Options
{
    /// <summary>The service name returned by the service document.</summary>
    public string ServiceName { get; set; } = "Integrated S3";

    /// <summary>The route prefix used for the IntegratedS3 HTTP surface.</summary>
    public string RoutePrefix { get; set; } = "/integrated-s3";

    /// <summary>Whether AWS Signature Version 4 request authentication is enabled.</summary>
    public bool EnableAwsSignatureV4Authentication { get; set; }

    /// <summary>The region string used when validating Signature Version 4 requests.</summary>
    public string SignatureAuthenticationRegion { get; set; } = "us-east-1";

    /// <summary>The service identifier used when validating Signature Version 4 requests.</summary>
    public string SignatureAuthenticationService { get; set; } = "s3";

    /// <summary>The permitted request clock skew for Signature Version 4 validation.</summary>
    public int AllowedSignatureClockSkewMinutes { get; set; } = 5;

    /// <summary>The maximum presigned URL lifetime accepted by the host.</summary>
    public int MaximumPresignedUrlExpirySeconds { get; set; } = 60 * 60;

    /// <summary>Static access keys that the host should accept for Signature Version 4 requests.</summary>
    public List<IntegratedS3AccessKeyCredential> AccessKeyCredentials { get; set; } = [];

    /// <summary>The access key identifier that the host should use when minting proxy presigned URLs.</summary>
    public string? PresignAccessKeyId { get; set; }

    /// <summary>The public base URL used when proxy presigned URLs must be generated outside the current request.</summary>
    public string? PresignPublicBaseUrl { get; set; }

    /// <summary>Whether virtual-hosted-style addressing is enabled for incoming requests.</summary>
    public bool EnableVirtualHostedStyleAddressing { get; set; }

    /// <summary>The host suffixes that should be treated as virtual-hosted-style bucket routes.</summary>
    public List<string> VirtualHostedStyleHostSuffixes { get; set; } = [];

    /// <summary>The provider descriptors surfaced by the host's metadata endpoints.</summary>
    public List<StorageProviderDescriptor> Providers { get; set; } = [];

    /// <summary>The deployment-wide capability descriptor surfaced by the host.</summary>
    public StorageCapabilities Capabilities { get; set; } = new();
}
