namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Versioning status for a bucket.
/// </summary>
public sealed class BucketVersioningInfo
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The current versioning status of the bucket.
    /// </summary>
    public BucketVersioningStatus Status { get; init; } = BucketVersioningStatus.Disabled;

    /// <summary>
    /// Convenience property that returns <see langword="true"/> when <see cref="Status"/> is <see cref="BucketVersioningStatus.Enabled"/>.
    /// </summary>
    public bool VersioningEnabled => Status == BucketVersioningStatus.Enabled;
}

/// <summary>
/// The versioning status of a bucket.
/// </summary>
public enum BucketVersioningStatus
{
    /// <summary>
    /// Versioning has never been enabled on this bucket.
    /// </summary>
    Disabled,

    /// <summary>
    /// Versioning is active; new object versions are created on every write.
    /// </summary>
    Enabled,

    /// <summary>
    /// Versioning is paused; existing versions are retained but new writes do not create versions.
    /// </summary>
    Suspended
}