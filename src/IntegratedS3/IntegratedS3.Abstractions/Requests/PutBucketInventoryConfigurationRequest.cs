using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketInventoryConfiguration operation.</summary>
public sealed class PutBucketInventoryConfigurationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The identifier for the inventory configuration.</summary>
    public required string Id { get; init; }

    /// <summary>When <see langword="true"/>, enables the inventory configuration.</summary>
    public bool IsEnabled { get; init; }

    /// <summary>The destination for the inventory report output.</summary>
    public BucketInventoryDestination? Destination { get; init; }

    /// <summary>The schedule for generating inventory reports.</summary>
    public BucketInventorySchedule? Schedule { get; init; }

    /// <summary>The filter used to select objects for the inventory.</summary>
    public BucketInventoryFilter? Filter { get; init; }

    /// <summary>Which object versions to include in the inventory ("All" or "Current").</summary>
    public string IncludedObjectVersions { get; init; } = "All";

    /// <summary>Additional optional fields to include in the inventory report.</summary>
    public IReadOnlyList<string> OptionalFields { get; init; } = [];
}
