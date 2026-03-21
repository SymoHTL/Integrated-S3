namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the cross-region replication configuration for an S3 bucket.
/// </summary>
public sealed class S3ReplicationConfiguration
{
    /// <summary>The IAM role ARN that S3 assumes when replicating objects.</summary>
    public string? Role { get; init; }

    /// <summary>The list of replication rules for the bucket.</summary>
    public IReadOnlyList<S3ReplicationRule> Rules { get; init; } = [];
}

/// <summary>
/// Represents a single replication rule within an <see cref="S3ReplicationConfiguration"/>.
/// </summary>
public sealed class S3ReplicationRule
{
    /// <summary>An optional identifier for this replication rule.</summary>
    public string? Id { get; init; }

    /// <summary>Whether the rule is <c>Enabled</c> or <c>Disabled</c>.</summary>
    public string Status { get; init; } = "Disabled";

    /// <summary>The object key prefix used to filter which objects are replicated.</summary>
    public string? FilterPrefix { get; init; }

    /// <summary>The destination configuration for replicated objects.</summary>
    public S3ReplicationDestination Destination { get; init; } = new();

    /// <summary>The priority of this rule when multiple rules match.</summary>
    public int? Priority { get; init; }

    /// <summary>Whether delete markers are replicated.</summary>
    public bool? DeleteMarkerReplication { get; init; }
}

/// <summary>
/// Specifies the destination bucket and optional settings for replicated objects.
/// </summary>
public sealed class S3ReplicationDestination
{
    /// <summary>The ARN of the destination bucket.</summary>
    public string Bucket { get; init; } = string.Empty;

    /// <summary>The storage class for replicated objects in the destination bucket.</summary>
    public string? StorageClass { get; init; }

    /// <summary>The account ID of the destination bucket owner.</summary>
    public string? Account { get; init; }
}
