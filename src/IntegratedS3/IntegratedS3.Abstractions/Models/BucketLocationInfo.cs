namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Region or location information for a bucket.
/// </summary>
public sealed class BucketLocationInfo
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The region or location constraint for the bucket, or <see langword="null"/> for the default region.
    /// </summary>
    public string? LocationConstraint { get; init; }
}
