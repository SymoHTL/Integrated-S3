namespace IntegratedS3.Core.Services;

public sealed record StorageAdminRepairDiagnostics
{
    public int OutstandingRepairCount { get; init; }

    public int PendingRepairCount { get; init; }

    public int InProgressRepairCount { get; init; }

    public int FailedRepairCount { get; init; }

    public string[] ReplicaBackendsWithOutstandingRepairs { get; init; } = [];

    public DateTimeOffset? OldestOutstandingRepairCreatedAtUtc { get; init; }

    public DateTimeOffset? LatestRepairActivityAtUtc { get; init; }

    public TimeSpan? ApproximateMaxReplicaLag { get; init; }

    public StorageReplicaRepairEntry[] OutstandingRepairs { get; init; } = [];
}
