namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Analytics configuration for a bucket, defining filters and export destinations for storage class analysis.
/// </summary>
public sealed class BucketAnalyticsConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The unique identifier for this analytics configuration.
    /// </summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>
    /// The object key prefix filter, or <see langword="null"/> for all objects.
    /// </summary>
    public string? FilterPrefix { get; init; }

    /// <summary>
    /// Tag filters that must all match for analysis to apply.
    /// </summary>
    public IReadOnlyDictionary<string, string>? FilterTags { get; init; }

    /// <summary>
    /// The storage class analysis configuration.
    /// </summary>
    public BucketAnalyticsStorageClassAnalysis? StorageClassAnalysis { get; init; }
}

/// <summary>
/// Storage class analysis settings for an analytics configuration.
/// </summary>
public sealed class BucketAnalyticsStorageClassAnalysis
{
    /// <summary>
    /// The data export settings for storage class analysis results.
    /// </summary>
    public BucketAnalyticsDataExport? DataExport { get; init; }
}

/// <summary>
/// Data export settings for analytics results.
/// </summary>
public sealed class BucketAnalyticsDataExport
{
    /// <summary>
    /// The version of the output schema (e.g., "V_1").
    /// </summary>
    public string OutputSchemaVersion { get; init; } = "V_1";

    /// <summary>
    /// The S3 bucket destination for exported analytics data.
    /// </summary>
    public BucketAnalyticsS3BucketDestination? Destination { get; init; }
}

/// <summary>
/// S3 bucket destination for analytics data export.
/// </summary>
public sealed class BucketAnalyticsS3BucketDestination
{
    /// <summary>
    /// The output format for exported data (e.g., "CSV").
    /// </summary>
    public string Format { get; init; } = "CSV";

    /// <summary>
    /// The account ID of the destination bucket owner, or <see langword="null"/> for same-account destinations.
    /// </summary>
    public string? BucketAccountId { get; init; }

    /// <summary>
    /// The ARN or name of the destination bucket.
    /// </summary>
    public string Bucket { get; init; } = string.Empty;

    /// <summary>
    /// The key prefix for exported data in the destination bucket.
    /// </summary>
    public string? Prefix { get; init; }
}
