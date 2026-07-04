namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the ownership controls configuration for an S3 bucket.
/// </summary>
public sealed class S3OwnershipControlsConfiguration
{
    /// <summary>
    /// The object ownership setting. One of <c>BucketOwnerPreferred</c>,
    /// <c>ObjectWriter</c>, or <c>BucketOwnerEnforced</c>.
    /// </summary>
    public string ObjectOwnership { get; init; } = string.Empty;
}
