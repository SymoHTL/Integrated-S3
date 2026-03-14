using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

internal sealed class StorageAdminDiagnosticsProvider(
    IEnumerable<IStorageBackend> backends,
    IStorageReplicaRepairBacklog repairBacklog,
    StorageBackendHealthMonitor healthMonitor,
    TimeProvider timeProvider)
    : IStorageAdminDiagnosticsProvider
{
    private readonly IStorageBackend[] _backends = backends.ToArray();

    public async ValueTask<StorageAdminDiagnostics> GetDiagnosticsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var observedAtUtc = timeProvider.GetUtcNow();
        var outstandingRepairs = await repairBacklog.ListOutstandingAsync(cancellationToken: cancellationToken);
        var orderedRepairs = outstandingRepairs
            .OrderBy(entry => entry.CreatedAtUtc)
            .ThenBy(entry => entry.ReplicaBackendName, StringComparer.Ordinal)
            .ToArray();
        var repairsByReplica = orderedRepairs
            .GroupBy(entry => entry.ReplicaBackendName, StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => (IReadOnlyList<StorageReplicaRepairEntry>)group.ToArray(),
                StringComparer.Ordinal);

        var providers = new StorageAdminProviderDiagnostics[_backends.Length];
        for (var index = 0; index < _backends.Length; index++) {
            cancellationToken.ThrowIfCancellationRequested();

            var backend = _backends[index];
            repairsByReplica.TryGetValue(backend.Name, out var replicaRepairs);

            providers[index] = new StorageAdminProviderDiagnostics
            {
                BackendName = backend.Name,
                Kind = backend.Kind,
                IsPrimary = backend.IsPrimary,
                Description = backend.Description,
                Mode = await backend.GetProviderModeAsync(cancellationToken),
                HealthStatus = await healthMonitor.GetStatusAsync(backend, cancellationToken),
                ReplicaLag = backend.IsPrimary
                    ? null
                    : CreateReplicaLagDiagnostics(replicaRepairs ?? [], observedAtUtc)
            };
        }

        return new StorageAdminDiagnostics
        {
            ObservedAtUtc = observedAtUtc,
            Providers = providers,
            Repairs = CreateRepairDiagnostics(orderedRepairs, observedAtUtc)
        };
    }

    private static StorageAdminReplicaLagDiagnostics CreateReplicaLagDiagnostics(
        IReadOnlyList<StorageReplicaRepairEntry> repairs,
        DateTimeOffset observedAtUtc)
    {
        var aggregate = AggregateRepairs(repairs, observedAtUtc);
        return new StorageAdminReplicaLagDiagnostics
        {
            HasOutstandingRepairs = aggregate.OutstandingRepairCount > 0,
            IsCurrent = aggregate.OutstandingRepairCount == 0,
            OutstandingRepairCount = aggregate.OutstandingRepairCount,
            PendingRepairCount = aggregate.PendingRepairCount,
            InProgressRepairCount = aggregate.InProgressRepairCount,
            FailedRepairCount = aggregate.FailedRepairCount,
            OldestOutstandingRepairCreatedAtUtc = aggregate.OldestOutstandingRepairCreatedAtUtc,
            LatestRepairActivityAtUtc = aggregate.LatestRepairActivityAtUtc,
            ApproximateLag = aggregate.ApproximateLag
        };
    }

    private static StorageAdminRepairDiagnostics CreateRepairDiagnostics(
        StorageReplicaRepairEntry[] outstandingRepairs,
        DateTimeOffset observedAtUtc)
    {
        var aggregate = AggregateRepairs(outstandingRepairs, observedAtUtc);
        return new StorageAdminRepairDiagnostics
        {
            OutstandingRepairCount = aggregate.OutstandingRepairCount,
            PendingRepairCount = aggregate.PendingRepairCount,
            InProgressRepairCount = aggregate.InProgressRepairCount,
            FailedRepairCount = aggregate.FailedRepairCount,
            ReplicaBackendsWithOutstandingRepairs = outstandingRepairs
                .Select(entry => entry.ReplicaBackendName)
                .Distinct(StringComparer.Ordinal)
                .Order(StringComparer.Ordinal)
                .ToArray(),
            OldestOutstandingRepairCreatedAtUtc = aggregate.OldestOutstandingRepairCreatedAtUtc,
            LatestRepairActivityAtUtc = aggregate.LatestRepairActivityAtUtc,
            ApproximateMaxReplicaLag = aggregate.ApproximateLag,
            OutstandingRepairs = outstandingRepairs
        };
    }

    private static RepairAggregate AggregateRepairs(
        IReadOnlyList<StorageReplicaRepairEntry> repairs,
        DateTimeOffset observedAtUtc)
    {
        var pendingRepairCount = 0;
        var inProgressRepairCount = 0;
        var failedRepairCount = 0;
        DateTimeOffset? oldestOutstandingRepairCreatedAtUtc = null;
        DateTimeOffset? latestRepairActivityAtUtc = null;

        foreach (var repair in repairs) {
            switch (repair.Status) {
                case StorageReplicaRepairStatus.Pending:
                    pendingRepairCount++;
                    break;
                case StorageReplicaRepairStatus.InProgress:
                    inProgressRepairCount++;
                    break;
                case StorageReplicaRepairStatus.Failed:
                    failedRepairCount++;
                    break;
            }

            oldestOutstandingRepairCreatedAtUtc = !oldestOutstandingRepairCreatedAtUtc.HasValue || repair.CreatedAtUtc < oldestOutstandingRepairCreatedAtUtc.Value
                ? repair.CreatedAtUtc
                : oldestOutstandingRepairCreatedAtUtc;
            latestRepairActivityAtUtc = !latestRepairActivityAtUtc.HasValue || repair.UpdatedAtUtc > latestRepairActivityAtUtc.Value
                ? repair.UpdatedAtUtc
                : latestRepairActivityAtUtc;
        }

        return new RepairAggregate(
            repairs.Count,
            pendingRepairCount,
            inProgressRepairCount,
            failedRepairCount,
            oldestOutstandingRepairCreatedAtUtc,
            latestRepairActivityAtUtc,
            oldestOutstandingRepairCreatedAtUtc.HasValue
                ? observedAtUtc - oldestOutstandingRepairCreatedAtUtc.Value
                : null);
    }

    private readonly record struct RepairAggregate(
        int OutstandingRepairCount,
        int PendingRepairCount,
        int InProgressRepairCount,
        int FailedRepairCount,
        DateTimeOffset? OldestOutstandingRepairCreatedAtUtc,
        DateTimeOffset? LatestRepairActivityAtUtc,
        TimeSpan? ApproximateLag);
}
