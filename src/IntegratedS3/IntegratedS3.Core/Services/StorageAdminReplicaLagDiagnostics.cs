namespace IntegratedS3.Core.Services;

public sealed record StorageAdminReplicaLagDiagnostics
{
    public bool HasOutstandingRepairs { get; init; }

    public bool IsCurrent { get; init; }

    public int OutstandingRepairCount { get; init; }

    public int PendingRepairCount { get; init; }

    public int InProgressRepairCount { get; init; }

    public int FailedRepairCount { get; init; }

    public DateTimeOffset? OldestOutstandingRepairCreatedAtUtc { get; init; }

    public DateTimeOffset? LatestRepairActivityAtUtc { get; init; }

    public TimeSpan? ApproximateLag { get; init; }
}
