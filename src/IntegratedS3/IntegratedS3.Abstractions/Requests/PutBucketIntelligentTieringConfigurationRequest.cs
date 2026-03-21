using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketIntelligentTieringConfiguration operation.</summary>
public sealed class PutBucketIntelligentTieringConfigurationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The identifier for the Intelligent-Tiering configuration.</summary>
    public required string Id { get; init; }

    /// <summary>The status of the configuration ("Enabled" or "Disabled").</summary>
    public string Status { get; init; } = "Enabled";

    /// <summary>The filter used to select objects for Intelligent-Tiering.</summary>
    public BucketIntelligentTieringFilter? Filter { get; init; }

    /// <summary>The Intelligent-Tiering access tier configurations.</summary>
    public IReadOnlyList<BucketIntelligentTiering> Tierings { get; init; } = [];
}
