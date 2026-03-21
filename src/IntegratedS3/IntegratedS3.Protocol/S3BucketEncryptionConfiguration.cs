namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the default encryption configuration for an S3 bucket.
/// </summary>
public sealed class S3BucketEncryptionConfiguration
{
    /// <summary>The list of server-side encryption rules applied to the bucket.</summary>
    public IReadOnlyList<S3BucketEncryptionRule> Rules { get; init; } = [];
}

/// <summary>
/// Represents a single server-side encryption rule for an S3 bucket.
/// </summary>
public sealed class S3BucketEncryptionRule
{
    /// <summary>The default encryption settings for objects stored in the bucket.</summary>
    public required S3BucketEncryptionByDefault DefaultEncryption { get; init; }

    /// <summary>Whether an S3 Bucket Key is enabled for server-side encryption with AWS KMS.</summary>
    public bool? BucketKeyEnabled { get; init; }
}

/// <summary>
/// Specifies the default server-side encryption algorithm and optional KMS key for an S3 bucket.
/// </summary>
public sealed class S3BucketEncryptionByDefault
{
    /// <summary>The server-side encryption algorithm (e.g., <c>aws:kms</c>, <c>AES256</c>).</summary>
    public string? SseAlgorithm { get; init; }

    /// <summary>The AWS KMS key ID used for server-side encryption.</summary>
    public string? KmsMasterKeyId { get; init; }
}
