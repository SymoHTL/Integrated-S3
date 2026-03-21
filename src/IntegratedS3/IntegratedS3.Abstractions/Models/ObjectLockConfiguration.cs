namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Bucket-level Object Lock settings.
/// </summary>
public sealed class ObjectLockConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// Whether Object Lock is enabled for this bucket.
    /// </summary>
    public bool ObjectLockEnabled { get; init; }

    /// <summary>
    /// The default retention policy applied to new objects, or <see langword="null"/> if none is configured.
    /// </summary>
    public ObjectLockDefaultRetention? DefaultRetention { get; init; }
}

/// <summary>
/// Default retention policy for new objects in a bucket with Object Lock enabled.
/// </summary>
public sealed class ObjectLockDefaultRetention
{
    /// <summary>
    /// The retention mode applied by default.
    /// </summary>
    public ObjectRetentionMode Mode { get; init; }

    /// <summary>
    /// The number of days to retain objects. Mutually exclusive with <see cref="Years"/>.
    /// </summary>
    public int? Days { get; init; }

    /// <summary>
    /// The number of years to retain objects. Mutually exclusive with <see cref="Days"/>.
    /// </summary>
    public int? Years { get; init; }
}
