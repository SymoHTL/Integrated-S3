namespace IntegratedS3.Provider.S3;

public sealed class S3StorageOptions
{
    public string ProviderName { get; set; } = "s3-primary";
    public bool IsPrimary { get; set; } = true;
    public string Region { get; set; } = "us-east-1";
    public string? ServiceUrl { get; set; }
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
