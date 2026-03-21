namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Inventory configuration for a bucket, defining what object metadata to report and where to store it.
/// </summary>
public sealed class BucketInventoryConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The unique identifier for this inventory configuration.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Whether this inventory configuration is enabled.
    /// </summary>
    public bool IsEnabled { get; init; }

    /// <summary>
    /// The destination where inventory results are published.
    /// </summary>
    public BucketInventoryDestination? Destination { get; init; }

    /// <summary>
    /// The schedule for generating inventory reports.
    /// </summary>
    public BucketInventorySchedule? Schedule { get; init; }

    /// <summary>
    /// An optional filter to scope which objects are included in the inventory.
    /// </summary>
    public BucketInventoryFilter? Filter { get; init; }

    /// <summary>
    /// Which object versions to include (e.g., "All" or "Current").
    /// </summary>
    public string IncludedObjectVersions { get; init; } = "All";

    /// <summary>
    /// Additional optional fields to include in the inventory report (e.g., "Size", "LastModifiedDate").
    /// </summary>
    public IReadOnlyList<string> OptionalFields { get; init; } = [];
}

/// <summary>
/// The destination configuration for inventory reports.
/// </summary>
public sealed class BucketInventoryDestination
{
    /// <summary>
    /// The S3 bucket destination for inventory reports.
    /// </summary>
    public BucketInventoryS3BucketDestination? S3BucketDestination { get; init; }
}

/// <summary>
/// S3 bucket destination for inventory report delivery.
/// </summary>
public sealed class BucketInventoryS3BucketDestination
{
    /// <summary>
    /// The output format for inventory reports (e.g., "CSV", "ORC", "Parquet").
    /// </summary>
    public string Format { get; init; } = "CSV";

    /// <summary>
    /// The account ID of the destination bucket owner, or <see langword="null"/> for same-account destinations.
    /// </summary>
    public string? AccountId { get; init; }

    /// <summary>
    /// The ARN or name of the destination bucket.
    /// </summary>
    public string Bucket { get; init; } = string.Empty;

    /// <summary>
    /// The key prefix for inventory reports in the destination bucket.
    /// </summary>
    public string? Prefix { get; init; }
}

/// <summary>
/// Schedule for generating inventory reports.
/// </summary>
public sealed class BucketInventorySchedule
{
    /// <summary>
    /// The frequency of inventory report generation (e.g., "Daily" or "Weekly").
    /// </summary>
    public string Frequency { get; init; } = "Daily";
}

/// <summary>
/// Filter that scopes an inventory configuration to specific objects.
/// </summary>
public sealed class BucketInventoryFilter
{
    /// <summary>
    /// The object key prefix filter, or <see langword="null"/> for all objects.
    /// </summary>
    public string? Prefix { get; init; }
}
