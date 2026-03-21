namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// CloudWatch request metrics configuration for a bucket.
/// </summary>
public sealed class BucketMetricsConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The unique identifier for this metrics configuration.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// An optional filter to scope which objects are included in the metrics.
    /// </summary>
    public BucketMetricsFilter? Filter { get; init; }
}

/// <summary>
/// Filter that scopes a metrics configuration to specific objects.
/// </summary>
public sealed class BucketMetricsFilter
{
    /// <summary>
    /// The object key prefix filter, or <see langword="null"/> for all objects.
    /// </summary>
    public string? Prefix { get; init; }

    /// <summary>
    /// The access point ARN filter, or <see langword="null"/> if not applicable.
    /// </summary>
    public string? AccessPointArn { get; init; }

    /// <summary>
    /// Tag filters that must all match for metrics to apply.
    /// </summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } = new Dictionary<string, string>(StringComparer.Ordinal);
}
