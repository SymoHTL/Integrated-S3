namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Lifecycle configuration for a bucket, containing the rules that govern object expiration and transitions.
/// </summary>
public sealed class BucketLifecycleConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The lifecycle rules applied to this bucket.
    /// </summary>
    public IReadOnlyList<BucketLifecycleRule> Rules { get; init; } = [];
}
