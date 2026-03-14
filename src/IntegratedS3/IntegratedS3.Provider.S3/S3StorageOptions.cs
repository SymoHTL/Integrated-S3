namespace IntegratedS3.Provider.S3;

public sealed class S3StorageOptions
{
    public string ProviderName { get; set; } = "s3-primary";
    public bool IsPrimary { get; set; } = true;

    /// <summary>
    /// AWS region / signing region. When <see cref="ServiceUrl"/> is set, the
    /// native client still uses this value as the authentication region so
    /// custom S3-compatible endpoints can validate SigV4 requests.
    /// </summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    /// Custom S3-compatible endpoint base URL. When set, the native client
    /// preserves the endpoint scheme for delegated presigns and uses required-only
    /// flexible checksum modes for broader MinIO/LocalStack compatibility.
    /// </summary>
    public string? ServiceUrl { get; set; }

    /// <summary>
    /// Force path-style bucket addressing. This is usually required for local
    /// S3-compatible endpoints such as MinIO or LocalStack.
    /// </summary>
    public bool ForcePathStyle { get; set; }

    /// <summary>
    /// AWS/S3-compatible access key ID. When both <see cref="AccessKey"/> and
    /// <see cref="SecretKey"/> are non-empty, explicit credentials are used instead
    /// of the ambient credential chain. Required for self-hosted endpoints such as
    /// MinIO or LocalStack.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// AWS/S3-compatible secret access key that corresponds to <see cref="AccessKey"/>.
    /// </summary>
    public string? SecretKey { get; set; }
}
