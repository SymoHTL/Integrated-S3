namespace IntegratedS3.Protocol;

/// <summary>
/// Represents an S3 metrics configuration for a bucket, used for CloudWatch request metrics.
/// </summary>
public sealed class S3MetricsConfiguration
{
    /// <summary>The unique identifier for this metrics configuration.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>The filter used to identify the subset of objects to which this configuration applies.</summary>
    public S3MetricsFilter? Filter { get; init; }
}

/// <summary>
/// Represents the filter criteria for an <see cref="S3MetricsConfiguration"/>.
/// </summary>
public sealed class S3MetricsFilter
{
    /// <summary>The object key prefix used to filter metrics.</summary>
    public string? Prefix { get; init; }

    /// <summary>The access point ARN used to filter metrics.</summary>
    public string? AccessPointArn { get; init; }

    /// <summary>The tags used to filter metrics.</summary>
    public IReadOnlyList<S3Tag> Tags { get; init; } = [];
}

/// <summary>
/// Represents the result of listing S3 metrics configurations for a bucket.
/// </summary>
public sealed class S3ListMetricsConfigurationsResult
{
    /// <summary>The list of <see cref="S3MetricsConfiguration"/> entries.</summary>
    public IReadOnlyList<S3MetricsConfiguration> MetricsConfigurations { get; init; } = [];

    /// <summary>Indicates whether the result list was truncated.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The continuation token included in the request.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>The token to use to retrieve the next page of results.</summary>
    public string? NextContinuationToken { get; init; }
}
