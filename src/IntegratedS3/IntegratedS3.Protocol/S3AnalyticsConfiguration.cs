namespace IntegratedS3.Protocol;

/// <summary>
/// Represents an S3 analytics configuration for a bucket.
/// </summary>
public sealed class S3AnalyticsConfiguration
{
    /// <summary>The unique identifier for this analytics configuration.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>The object key prefix used to filter analytics data.</summary>
    public string? FilterPrefix { get; init; }

    /// <summary>The tags used to filter analytics data.</summary>
    public IReadOnlyList<S3AnalyticsFilterTag>? FilterTags { get; init; }

    /// <summary>The storage class analysis settings for this configuration.</summary>
    public S3StorageClassAnalysis? StorageClassAnalysis { get; init; }
}

/// <summary>
/// Represents a tag filter used in an <see cref="S3AnalyticsConfiguration"/>.
/// </summary>
public sealed class S3AnalyticsFilterTag
{
    /// <summary>The tag key.</summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>The tag value.</summary>
    public string Value { get; init; } = string.Empty;
}

/// <summary>
/// Contains the storage class analysis settings for an <see cref="S3AnalyticsConfiguration"/>.
/// </summary>
public sealed class S3StorageClassAnalysis
{
    /// <summary>The data export configuration for storage class analysis.</summary>
    public S3StorageClassAnalysisDataExport? DataExport { get; init; }
}

/// <summary>
/// Represents the data export settings for S3 storage class analysis.
/// </summary>
public sealed class S3StorageClassAnalysisDataExport
{
    /// <summary>The version of the output schema (e.g., <c>V_1</c>).</summary>
    public string OutputSchemaVersion { get; init; } = "V_1";

    /// <summary>The destination where analytics data is exported.</summary>
    public S3AnalyticsS3BucketDestination? Destination { get; init; }
}

/// <summary>
/// Represents the S3 bucket destination for analytics data export.
/// </summary>
public sealed class S3AnalyticsS3BucketDestination
{
    /// <summary>The output format of the analytics data (e.g., <c>CSV</c>).</summary>
    public string Format { get; init; } = "CSV";

    /// <summary>The account ID that owns the destination bucket.</summary>
    public string? BucketAccountId { get; init; }

    /// <summary>The ARN of the destination bucket.</summary>
    public string Bucket { get; init; } = string.Empty;

    /// <summary>The key prefix for the exported analytics data.</summary>
    public string? Prefix { get; init; }
}

/// <summary>
/// Represents the result of listing S3 analytics configurations for a bucket.
/// </summary>
public sealed class S3ListAnalyticsConfigurationsResult
{
    /// <summary>The list of <see cref="S3AnalyticsConfiguration"/> entries.</summary>
    public IReadOnlyList<S3AnalyticsConfiguration> AnalyticsConfigurations { get; init; } = [];

    /// <summary>Indicates whether the result list was truncated.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The continuation token included in the request.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>The token to use to retrieve the next page of results.</summary>
    public string? NextContinuationToken { get; init; }
}
