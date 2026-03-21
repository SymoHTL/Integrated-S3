namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the RestoreObject operation (Glacier restore).</summary>
public sealed class RestoreObjectRequest
{
    /// <summary>The name of the bucket containing the archived object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The key of the archived object to restore.</summary>
    public required string Key { get; init; }

    /// <summary>The version identifier of the object to restore.</summary>
    public string? VersionId { get; init; }

    /// <summary>The number of days the restored copy should remain accessible.</summary>
    public int? Days { get; init; }

    /// <summary>
    /// Glacier retrieval tier: "Expedited", "Standard", or "Bulk".
    /// </summary>
    public string? GlacierTier { get; init; }
}
