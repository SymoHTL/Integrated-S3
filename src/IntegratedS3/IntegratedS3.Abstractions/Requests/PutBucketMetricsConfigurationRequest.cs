using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketMetricsConfiguration operation.</summary>
public sealed class PutBucketMetricsConfigurationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The identifier for the metrics configuration.</summary>
    public required string Id { get; init; }

    /// <summary>The filter used to select objects for metrics collection.</summary>
    public BucketMetricsFilter? Filter { get; init; }
}
