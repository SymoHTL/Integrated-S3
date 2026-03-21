namespace IntegratedS3.Protocol;

/// <summary>
/// Represents a request to restore an archived S3 object (e.g., from Glacier).
/// </summary>
public sealed class S3RestoreRequest
{
    /// <summary>The number of days the restored copy remains accessible.</summary>
    public int? Days { get; init; }

    /// <summary>The retrieval tier for the restore request (e.g., <c>Expedited</c>, <c>Standard</c>, <c>Bulk</c>).</summary>
    public string? GlacierJobTier { get; init; }
}
