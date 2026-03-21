namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketLogging operation.</summary>
public sealed class PutBucketLoggingRequest
{
    /// <summary>The name of the bucket to configure logging for.</summary>
    public required string BucketName { get; init; }

    /// <summary>The name of the bucket where access logs will be stored.</summary>
    public string? TargetBucket { get; init; }

    /// <summary>The key prefix for access log objects.</summary>
    public string? TargetPrefix { get; init; }
}
