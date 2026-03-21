namespace IntegratedS3.Core.Services;

/// <summary>
/// Aggregate diagnostics for the replica repair subsystem across all replica backends.
/// </summary>
public sealed record StorageAdminRepairDiagnostics
{
    /// <summary>Total number of repairs that are not yet completed (pending + in-progress + failed).</summary>
    public int OutstandingRepairCount { get; init; }

    /// <summary>Number of repairs in the <see cref="StorageReplicaRepairStatus.Pending"/> state.</summary>
    public int PendingRepairCount { get; init; }

    /// <summary>Number of repairs in the <see cref="StorageReplicaRepairStatus.InProgress"/> state.</summary>
    public int InProgressRepairCount { get; init; }

    /// <summary>Number of repairs in the <see cref="StorageReplicaRepairStatus.Failed"/> state.</summary>
    public int FailedRepairCount { get; init; }

    /// <summary>Names of replica backends that have at least one outstanding repair.</summary>
    public string[] ReplicaBackendsWithOutstandingRepairs { get; init; } = [];

    /// <summary>Creation timestamp of the oldest outstanding repair, or <see langword="null"/> if there are none.</summary>
    public DateTimeOffset? OldestOutstandingRepairCreatedAtUtc { get; init; }

    /// <summary>Timestamp of the most recent repair activity, or <see langword="null"/> if no activity has occurred.</summary>
    public DateTimeOffset? LatestRepairActivityAtUtc { get; init; }

    /// <summary>Estimated maximum replication lag based on the oldest outstanding repair, or <see langword="null"/>.</summary>
    public TimeSpan? ApproximateMaxReplicaLag { get; init; }

    /// <summary>The full list of outstanding repair entries.</summary>
    public StorageReplicaRepairEntry[] OutstandingRepairs { get; init; } = [];
}
