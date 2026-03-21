namespace IntegratedS3.Core.Services;

/// <summary>
/// Replica lag diagnostics for a single replica backend, summarizing how far behind
/// the replica is relative to the primary.
/// </summary>
public sealed record StorageAdminReplicaLagDiagnostics
{
    /// <summary>Indicates whether this replica has any outstanding (incomplete) repairs.</summary>
    public bool HasOutstandingRepairs { get; init; }

    /// <summary>Indicates whether the replica is fully caught up with the primary.</summary>
    public bool IsCurrent { get; init; }

    /// <summary>Total number of outstanding repairs for this replica.</summary>
    public int OutstandingRepairCount { get; init; }

    /// <summary>Number of repairs in the <see cref="StorageReplicaRepairStatus.Pending"/> state for this replica.</summary>
    public int PendingRepairCount { get; init; }

    /// <summary>Number of repairs in the <see cref="StorageReplicaRepairStatus.InProgress"/> state for this replica.</summary>
    public int InProgressRepairCount { get; init; }

    /// <summary>Number of repairs in the <see cref="StorageReplicaRepairStatus.Failed"/> state for this replica.</summary>
    public int FailedRepairCount { get; init; }

    /// <summary>Creation timestamp of the oldest outstanding repair for this replica, or <see langword="null"/>.</summary>
    public DateTimeOffset? OldestOutstandingRepairCreatedAtUtc { get; init; }

    /// <summary>Timestamp of the most recent repair activity for this replica, or <see langword="null"/>.</summary>
    public DateTimeOffset? LatestRepairActivityAtUtc { get; init; }

    /// <summary>Estimated replication lag for this replica based on outstanding repairs, or <see langword="null"/>.</summary>
    public TimeSpan? ApproximateLag { get; init; }
}
