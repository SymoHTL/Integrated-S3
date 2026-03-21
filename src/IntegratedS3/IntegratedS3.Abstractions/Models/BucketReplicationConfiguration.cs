namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Cross-region or same-region replication configuration for a bucket.
/// </summary>
public sealed class BucketReplicationConfiguration
{
    /// <summary>
    /// The name of the source bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The IAM role ARN used for replication, or <see langword="null"/> if not set.
    /// </summary>
    public string? Role { get; init; }

    /// <summary>
    /// The replication rules for this bucket.
    /// </summary>
    public IReadOnlyList<BucketReplicationRule> Rules { get; init; } = [];
}

/// <summary>
/// A single replication rule specifying what to replicate and where.
/// </summary>
public sealed class BucketReplicationRule
{
    /// <summary>
    /// An optional identifier for this rule.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// Whether this replication rule is enabled or disabled.
    /// </summary>
    public BucketReplicationRuleStatus Status { get; init; } = BucketReplicationRuleStatus.Disabled;

    /// <summary>
    /// The object key prefix to which this rule applies, or <see langword="null"/> for all objects.
    /// </summary>
    public string? FilterPrefix { get; init; }

    /// <summary>
    /// The destination configuration for replicated objects.
    /// </summary>
    public BucketReplicationDestination Destination { get; init; } = new();

    /// <summary>
    /// The priority of this rule when multiple rules match. Higher numbers take precedence.
    /// </summary>
    public int? Priority { get; init; }

    /// <summary>
    /// Whether delete markers are replicated to the destination.
    /// </summary>
    public bool DeleteMarkerReplication { get; init; }
}

/// <summary>
/// Whether a replication rule is enabled or disabled.
/// </summary>
public enum BucketReplicationRuleStatus
{
    /// <summary>
    /// The replication rule is active.
    /// </summary>
    Enabled,

    /// <summary>
    /// The replication rule is inactive.
    /// </summary>
    Disabled
}

/// <summary>
/// The destination configuration for a replication rule.
/// </summary>
public sealed class BucketReplicationDestination
{
    /// <summary>
    /// The ARN or name of the destination bucket.
    /// </summary>
    public string Bucket { get; init; } = string.Empty;

    /// <summary>
    /// The storage class for replicated objects, or <see langword="null"/> to use the destination bucket default.
    /// </summary>
    public string? StorageClass { get; init; }

    /// <summary>
    /// The account ID of the destination bucket owner, or <see langword="null"/> for same-account replication.
    /// </summary>
    public string? Account { get; init; }
}
