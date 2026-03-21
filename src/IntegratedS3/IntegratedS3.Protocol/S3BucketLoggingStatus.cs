namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the server access logging configuration for an S3 bucket.
/// </summary>
public sealed class S3BucketLoggingStatus
{
    /// <summary>The logging configuration details, or <c>null</c> if logging is disabled.</summary>
    public S3LoggingEnabled? LoggingEnabled { get; init; }
}

/// <summary>
/// Specifies the target bucket and prefix for S3 server access log delivery.
/// </summary>
public sealed class S3LoggingEnabled
{
    /// <summary>The name of the bucket where access logs are delivered.</summary>
    public string TargetBucket { get; init; } = string.Empty;

    /// <summary>The key prefix for delivered access log objects.</summary>
    public string TargetPrefix { get; init; } = string.Empty;
}
