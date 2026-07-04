namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Ownership controls configuration for a bucket.
/// Mirrors the S3 <c>OwnershipControls</c> element, which carries a single
/// <c>Rule</c> with an <c>ObjectOwnership</c> value.
/// </summary>
public sealed class BucketOwnershipControlsConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The object ownership setting for the bucket. One of
    /// <c>BucketOwnerPreferred</c>, <c>ObjectWriter</c>, or <c>BucketOwnerEnforced</c>.
    /// </summary>
    public string ObjectOwnership { get; init; } = string.Empty;
}
