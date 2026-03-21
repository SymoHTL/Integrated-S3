using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketAnalyticsConfiguration operation.</summary>
public sealed class PutBucketAnalyticsConfigurationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The identifier for the analytics configuration.</summary>
    public required string Id { get; init; }

    /// <summary>The key prefix used to filter objects for analytics.</summary>
    public string? FilterPrefix { get; init; }

    /// <summary>Tag filters used to select objects for analytics.</summary>
    public IReadOnlyDictionary<string, string>? FilterTags { get; init; }

    /// <summary>The storage class analysis configuration.</summary>
    public BucketAnalyticsStorageClassAnalysis? StorageClassAnalysis { get; init; }
}
