namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the lifecycle configuration for an S3 bucket, controlling automatic object transitions and expiration.
/// </summary>
public sealed class S3LifecycleConfiguration
{
    /// <summary>The list of lifecycle rules for the bucket.</summary>
    public IReadOnlyList<S3LifecycleRule> Rules { get; init; } = [];
}

/// <summary>
/// Represents a single lifecycle rule that defines transition and expiration actions for matching objects.
/// </summary>
public sealed class S3LifecycleRule
{
    /// <summary>An optional identifier for this lifecycle rule.</summary>
    public string? Id { get; init; }

    /// <summary>The object key prefix used to filter which objects this rule applies to.</summary>
    public string? FilterPrefix { get; init; }

    /// <summary>The tags used to filter which objects this rule applies to.</summary>
    public IReadOnlyList<S3LifecycleFilterTag>? FilterTags { get; init; }

    /// <summary>Whether the rule is <c>Enabled</c> or <c>Disabled</c>.</summary>
    public string Status { get; init; } = "Disabled";

    /// <summary>The number of days after creation before objects expire.</summary>
    public int? ExpirationDays { get; init; }

    /// <summary>The date on which objects expire (ISO 8601 format).</summary>
    public string? ExpirationDate { get; init; }

    /// <summary>Whether to remove expired object delete markers with no noncurrent versions.</summary>
    public bool? ExpiredObjectDeleteMarker { get; init; }

    /// <summary>The number of days after becoming noncurrent before versions expire.</summary>
    public int? NoncurrentVersionExpirationDays { get; init; }

    /// <summary>The number of days after initiation before incomplete multipart uploads are aborted.</summary>
    public int? AbortIncompleteMultipartUploadDaysAfterInitiation { get; init; }

    /// <summary>Storage class transitions for current object versions.</summary>
    public IReadOnlyList<S3LifecycleTransition> Transitions { get; init; } = [];

    /// <summary>Storage class transitions for noncurrent object versions.</summary>
    public IReadOnlyList<S3LifecycleNoncurrentVersionTransition> NoncurrentVersionTransitions { get; init; } = [];
}

/// <summary>
/// Represents a tag filter used in an <see cref="S3LifecycleRule"/>.
/// </summary>
public sealed class S3LifecycleFilterTag
{
    /// <summary>The tag key.</summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>The tag value.</summary>
    public string Value { get; init; } = string.Empty;
}

/// <summary>
/// Defines when current object versions transition to a different storage class.
/// </summary>
public sealed class S3LifecycleTransition
{
    /// <summary>The number of days after creation before the transition occurs.</summary>
    public int? Days { get; init; }

    /// <summary>The date on which the transition occurs (ISO 8601 format).</summary>
    public string? Date { get; init; }

    /// <summary>The target storage class for the transition.</summary>
    public string StorageClass { get; init; } = string.Empty;
}

/// <summary>
/// Defines when noncurrent object versions transition to a different storage class.
/// </summary>
public sealed class S3LifecycleNoncurrentVersionTransition
{
    /// <summary>The number of days after becoming noncurrent before the transition occurs.</summary>
    public int? NoncurrentDays { get; init; }

    /// <summary>The target storage class for the transition.</summary>
    public string StorageClass { get; init; } = string.Empty;
}
