namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Server access logging configuration for a bucket.
/// </summary>
public sealed class BucketLoggingConfiguration
{
    /// <summary>
    /// The name of the bucket being configured.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The name of the bucket that receives the access logs, or <see langword="null"/> if logging is disabled.
    /// </summary>
    public string? TargetBucket { get; init; }

    /// <summary>
    /// The key prefix prepended to log object keys in the target bucket.
    /// </summary>
    public string? TargetPrefix { get; init; }
}
