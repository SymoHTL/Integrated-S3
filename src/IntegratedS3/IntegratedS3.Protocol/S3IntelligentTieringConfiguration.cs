namespace IntegratedS3.Protocol;

/// <summary>
/// Represents an S3 Intelligent-Tiering storage class configuration for a bucket.
/// </summary>
public sealed class S3IntelligentTieringConfiguration
{
    /// <summary>The unique identifier for this Intelligent-Tiering configuration.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>The status of the configuration (<c>Enabled</c> or <c>Disabled</c>).</summary>
    public string Status { get; init; } = "Enabled";

    /// <summary>The filter used to identify the subset of objects to which this configuration applies.</summary>
    public S3IntelligentTieringFilter? Filter { get; init; }

    /// <summary>The list of storage class tiering definitions.</summary>
    public IReadOnlyList<S3Tiering> Tierings { get; init; } = [];
}

/// <summary>
/// Represents the filter criteria for an <see cref="S3IntelligentTieringConfiguration"/>.
/// </summary>
public sealed class S3IntelligentTieringFilter
{
    /// <summary>The object key prefix used to filter objects.</summary>
    public string? Prefix { get; init; }

    /// <summary>The tags used to filter objects.</summary>
    public IReadOnlyList<S3Tag> Tags { get; init; } = [];
}

/// <summary>
/// Represents a single access tier and its associated transition period within Intelligent-Tiering.
/// </summary>
public sealed class S3Tiering
{
    /// <summary>The S3 Intelligent-Tiering access tier (e.g., <c>ARCHIVE_ACCESS</c>).</summary>
    public string AccessTier { get; init; } = string.Empty;

    /// <summary>The number of consecutive days of no access before objects move to this tier.</summary>
    public int Days { get; init; }
}

/// <summary>
/// Represents the result of listing S3 Intelligent-Tiering configurations for a bucket.
/// </summary>
public sealed class S3ListIntelligentTieringConfigurationsResult
{
    /// <summary>The list of <see cref="S3IntelligentTieringConfiguration"/> entries.</summary>
    public IReadOnlyList<S3IntelligentTieringConfiguration> IntelligentTieringConfigurations { get; init; } = [];

    /// <summary>Indicates whether the result list was truncated.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The continuation token included in the request.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>The token to use to retrieve the next page of results.</summary>
    public string? NextContinuationToken { get; init; }
}
