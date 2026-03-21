namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// A single lifecycle rule that governs object expiration, transition, and cleanup.
/// </summary>
public sealed class BucketLifecycleRule
{
    /// <summary>
    /// An optional identifier for this rule.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// The object key prefix to which this rule applies, or <see langword="null"/> for all objects.
    /// </summary>
    public string? FilterPrefix { get; init; }

    /// <summary>
    /// Tag filters that must all match for this rule to apply.
    /// </summary>
    public IReadOnlyDictionary<string, string>? FilterTags { get; init; }

    /// <summary>
    /// Whether this lifecycle rule is enabled or disabled.
    /// </summary>
    public BucketLifecycleRuleStatus Status { get; init; } = BucketLifecycleRuleStatus.Disabled;

    /// <summary>
    /// The number of days after object creation to expire the object.
    /// </summary>
    public int? ExpirationDays { get; init; }

    /// <summary>
    /// The specific date on which to expire matching objects.
    /// </summary>
    public DateTimeOffset? ExpirationDate { get; init; }

    /// <summary>
    /// Whether to remove expired object delete markers when they are the only remaining version.
    /// </summary>
    public bool? ExpiredObjectDeleteMarker { get; init; }

    /// <summary>
    /// The number of days after an object version becomes noncurrent to expire it.
    /// </summary>
    public int? NoncurrentVersionExpirationDays { get; init; }

    /// <summary>
    /// The number of days after initiation to abort incomplete multipart uploads.
    /// </summary>
    public int? AbortIncompleteMultipartUploadDaysAfterInitiation { get; init; }

    /// <summary>
    /// Storage class transitions for current object versions.
    /// </summary>
    public IReadOnlyList<BucketLifecycleTransition> Transitions { get; init; } = [];

    /// <summary>
    /// Storage class transitions for noncurrent object versions.
    /// </summary>
    public IReadOnlyList<BucketLifecycleNoncurrentVersionTransition> NoncurrentVersionTransitions { get; init; } = [];
}

/// <summary>
/// Whether a lifecycle rule is enabled or disabled.
/// </summary>
public enum BucketLifecycleRuleStatus
{
    /// <summary>
    /// The lifecycle rule is active.
    /// </summary>
    Enabled,

    /// <summary>
    /// The lifecycle rule is inactive.
    /// </summary>
    Disabled
}

/// <summary>
/// Defines a transition for current object versions to a different storage class.
/// </summary>
public sealed class BucketLifecycleTransition
{
    /// <summary>
    /// The number of days after object creation to transition, or <see langword="null"/> if <see cref="Date"/> is used.
    /// </summary>
    public int? Days { get; init; }

    /// <summary>
    /// The specific date on which to transition matching objects, or <see langword="null"/> if <see cref="Days"/> is used.
    /// </summary>
    public DateTimeOffset? Date { get; init; }

    /// <summary>
    /// The target storage class for the transition.
    /// </summary>
    public string StorageClass { get; init; } = string.Empty;
}

/// <summary>
/// Defines a transition for noncurrent object versions to a different storage class.
/// </summary>
public sealed class BucketLifecycleNoncurrentVersionTransition
{
    /// <summary>
    /// The number of days after a version becomes noncurrent to transition it.
    /// </summary>
    public int? NoncurrentDays { get; init; }

    /// <summary>
    /// The target storage class for the transition.
    /// </summary>
    public string StorageClass { get; init; } = string.Empty;
}
