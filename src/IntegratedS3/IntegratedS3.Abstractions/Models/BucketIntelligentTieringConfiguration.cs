namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// S3 Intelligent-Tiering configuration for a bucket.
/// </summary>
public sealed class BucketIntelligentTieringConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The unique identifier for this Intelligent-Tiering configuration.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Whether this configuration is enabled (e.g., "Enabled" or "Disabled").
    /// </summary>
    public string Status { get; init; } = "Enabled";

    /// <summary>
    /// An optional filter to scope which objects are included.
    /// </summary>
    public BucketIntelligentTieringFilter? Filter { get; init; }

    /// <summary>
    /// The tiering definitions that control when objects move between access tiers.
    /// </summary>
    public IReadOnlyList<BucketIntelligentTiering> Tierings { get; init; } = [];
}

/// <summary>
/// Filter that scopes an Intelligent-Tiering configuration to specific objects.
/// </summary>
public sealed class BucketIntelligentTieringFilter
{
    /// <summary>
    /// The object key prefix filter, or <see langword="null"/> for all objects.
    /// </summary>
    public string? Prefix { get; init; }

    /// <summary>
    /// Tag filters that must all match for the configuration to apply.
    /// </summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } = new Dictionary<string, string>(StringComparer.Ordinal);
}

/// <summary>
/// A single tiering definition that moves objects to a specific access tier after a number of days.
/// </summary>
public sealed class BucketIntelligentTiering
{
    /// <summary>
    /// The target access tier (e.g., "ARCHIVE_ACCESS" or "DEEP_ARCHIVE_ACCESS").
    /// </summary>
    public string AccessTier { get; init; } = string.Empty;

    /// <summary>
    /// The number of consecutive days of no access before objects are moved to this tier.
    /// </summary>
    public int Days { get; init; }
}
