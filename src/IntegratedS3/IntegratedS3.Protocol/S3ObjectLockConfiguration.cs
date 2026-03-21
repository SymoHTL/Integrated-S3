namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the S3 Object Lock configuration for a bucket.
/// </summary>
public sealed class S3ObjectLockConfiguration
{
    /// <summary>Whether Object Lock is enabled on the bucket (<c>Enabled</c>).</summary>
    public string? ObjectLockEnabled { get; init; }

    /// <summary>The default retention settings applied to new objects in the bucket.</summary>
    public S3ObjectLockDefaultRetention? DefaultRetention { get; init; }
}

/// <summary>
/// Specifies the default Object Lock retention mode and period for new objects in a bucket.
/// </summary>
public sealed class S3ObjectLockDefaultRetention
{
    /// <summary>The retention mode (<c>GOVERNANCE</c> or <c>COMPLIANCE</c>).</summary>
    public string? Mode { get; init; }

    /// <summary>The number of days for the default retention period.</summary>
    public int? Days { get; init; }

    /// <summary>The number of years for the default retention period.</summary>
    public int? Years { get; init; }
}
