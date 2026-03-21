namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Transfer Acceleration configuration for a bucket.
/// </summary>
public sealed class BucketAccelerateConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The current Transfer Acceleration status.
    /// </summary>
    public BucketAccelerateStatus Status { get; init; } = BucketAccelerateStatus.Suspended;
}

/// <summary>
/// Transfer Acceleration status for a bucket.
/// </summary>
public enum BucketAccelerateStatus
{
    /// <summary>
    /// Transfer Acceleration is active.
    /// </summary>
    Enabled,

    /// <summary>
    /// Transfer Acceleration is paused.
    /// </summary>
    Suspended
}
