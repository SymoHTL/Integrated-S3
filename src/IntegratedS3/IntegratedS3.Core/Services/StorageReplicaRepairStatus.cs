namespace IntegratedS3.Core.Services;

/// <summary>
/// Describes the lifecycle state of a replica repair entry.
/// </summary>
public enum StorageReplicaRepairStatus
{
    /// <summary>The repair is queued and has not yet been attempted.</summary>
    Pending,

    /// <summary>A repair attempt is currently executing.</summary>
    InProgress,

    /// <summary>The most recent repair attempt failed; the entry may be retried.</summary>
    Failed,

    /// <summary>The repair was completed successfully.</summary>
    Completed
}
