namespace IntegratedS3.Protocol;

/// <summary>
/// Represents an S3 inventory configuration that controls what object metadata is reported and where.
/// </summary>
public sealed class S3InventoryConfiguration
{
    /// <summary>The unique identifier for this inventory configuration.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>Whether the inventory is enabled.</summary>
    public bool IsEnabled { get; init; }

    /// <summary>The destination where inventory results are published.</summary>
    public S3InventoryDestination? Destination { get; init; }

    /// <summary>The schedule that determines how frequently inventory results are produced.</summary>
    public S3InventorySchedule? Schedule { get; init; }

    /// <summary>An optional filter to limit which objects are included in the inventory.</summary>
    public S3InventoryFilter? Filter { get; init; }

    /// <summary>Which object versions to include (<c>All</c> or <c>Current</c>).</summary>
    public string IncludedObjectVersions { get; init; } = "All";

    /// <summary>Additional object metadata fields to include in the inventory output.</summary>
    public IReadOnlyList<string> OptionalFields { get; init; } = [];
}

/// <summary>
/// Wraps the destination configuration for an S3 inventory.
/// </summary>
public sealed class S3InventoryDestination
{
    /// <summary>The S3 bucket destination for inventory results.</summary>
    public S3InventoryS3BucketDestination? S3BucketDestination { get; init; }
}

/// <summary>
/// Specifies the S3 bucket, format, and optional prefix for inventory output.
/// </summary>
public sealed class S3InventoryS3BucketDestination
{
    /// <summary>The output format of the inventory (e.g., <c>CSV</c>, <c>ORC</c>, <c>Parquet</c>).</summary>
    public string Format { get; init; } = "CSV";

    /// <summary>The account ID that owns the destination bucket.</summary>
    public string? AccountId { get; init; }

    /// <summary>The ARN of the destination bucket.</summary>
    public string Bucket { get; init; } = string.Empty;

    /// <summary>The key prefix for inventory output files.</summary>
    public string? Prefix { get; init; }
}

/// <summary>
/// Represents the frequency at which inventory results are produced.
/// </summary>
public sealed class S3InventorySchedule
{
    /// <summary>The inventory generation frequency (<c>Daily</c> or <c>Weekly</c>).</summary>
    public string Frequency { get; init; } = "Daily";
}

/// <summary>
/// Represents a filter that limits the objects included in an S3 inventory.
/// </summary>
public sealed class S3InventoryFilter
{
    /// <summary>The object key prefix used to filter inventory results.</summary>
    public string? Prefix { get; init; }
}

/// <summary>
/// Represents the result of listing S3 inventory configurations for a bucket.
/// </summary>
public sealed class S3ListInventoryConfigurationsResult
{
    /// <summary>The list of <see cref="S3InventoryConfiguration"/> entries.</summary>
    public IReadOnlyList<S3InventoryConfiguration> InventoryConfigurations { get; init; } = [];

    /// <summary>Indicates whether the result list was truncated.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The continuation token included in the request.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>The token to use to retrieve the next page of results.</summary>
    public string? NextContinuationToken { get; init; }
}
