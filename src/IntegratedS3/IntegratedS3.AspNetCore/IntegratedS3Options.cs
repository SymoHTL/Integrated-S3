using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.AspNetCore;

public sealed class IntegratedS3Options
{
    public string ServiceName { get; set; } = "Integrated S3";

    public string RoutePrefix { get; set; } = "/integrated-s3";

    public bool EnableAwsSignatureV4Authentication { get; set; }

    public string SignatureAuthenticationRegion { get; set; } = "us-east-1";

    public string SignatureAuthenticationService { get; set; } = "s3";

    public int AllowedSignatureClockSkewMinutes { get; set; } = 5;

    public int MaximumPresignedUrlExpirySeconds { get; set; } = 60 * 60;

    public List<IntegratedS3AccessKeyCredential> AccessKeyCredentials { get; set; } = [];

    public bool EnableVirtualHostedStyleAddressing { get; set; }

    public List<string> VirtualHostedStyleHostSuffixes { get; set; } = [];

    public List<StorageProviderDescriptor> Providers { get; set; } = [];

    public StorageCapabilities Capabilities { get; set; } = new();
}
