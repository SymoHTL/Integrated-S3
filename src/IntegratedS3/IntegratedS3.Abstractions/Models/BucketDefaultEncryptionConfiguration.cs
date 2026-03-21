namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Default server-side encryption configuration for a bucket.
/// </summary>
public sealed class BucketDefaultEncryptionConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The default encryption rule applied to new objects in this bucket.
    /// </summary>
    public required BucketDefaultEncryptionRule Rule { get; init; }
}

/// <summary>
/// A default encryption rule specifying the algorithm and optional key for new objects.
/// </summary>
public sealed class BucketDefaultEncryptionRule
{
    /// <summary>
    /// The server-side encryption algorithm to apply by default.
    /// </summary>
    public required ObjectServerSideEncryptionAlgorithm Algorithm { get; init; }

    /// <summary>
    /// The KMS key identifier, or <see langword="null"/> for S3-managed keys.
    /// </summary>
    public string? KeyId { get; init; }

    /// <summary>
    /// Whether an S3 Bucket Key is enabled for SSE-KMS encryption.
    /// </summary>
    public bool BucketKeyEnabled { get; init; }
}
