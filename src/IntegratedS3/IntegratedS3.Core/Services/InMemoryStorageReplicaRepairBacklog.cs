using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Observability;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Core.Services;

internal sealed class InMemoryStorageReplicaRepairBacklog : IStorageReplicaRepairBacklog
{
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<InMemoryStorageReplicaRepairBacklog> _logger;
    private readonly ConcurrentDictionary<string, StorageReplicaRepairEntry> _entries = new(StringComparer.Ordinal);
    private readonly ObservableGauge<long> _backlogSizeGauge;
    private readonly ObservableGauge<double> _backlogOldestAgeGauge;

    public InMemoryStorageReplicaRepairBacklog(
        TimeProvider timeProvider,
        ILogger<InMemoryStorageReplicaRepairBacklog> logger)
    {
        _timeProvider = timeProvider;
        _logger = logger;
        _backlogSizeGauge = IntegratedS3Observability.Meter.CreateObservableGauge<long>(
            IntegratedS3Observability.Metrics.ReplicaRepairBacklogSize,
            ObserveBacklogSize,
            unit: "{entry}",
            description: "Outstanding replica repair backlog size by replica backend and status.");
        _backlogOldestAgeGauge = IntegratedS3Observability.Meter.CreateObservableGauge<double>(
            IntegratedS3Observability.Metrics.ReplicaRepairOldestAge,
            ObserveOldestAge,
            unit: "s",
            description: "Age of the oldest outstanding replica repair by replica backend.");
    }

    public ValueTask AddAsync(StorageReplicaRepairEntry entry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        cancellationToken.ThrowIfCancellationRequested();

        _entries[entry.Id] = entry;
        _logger.LogInformation(
            "Queued replica repair {RepairId} for {ReplicaBackend}. Origin {Origin}. Status {Status}.",
            entry.Id,
            entry.ReplicaBackendName,
            entry.Origin,
            entry.Status);
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> HasOutstandingRepairsAsync(string replicaBackendName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(replicaBackendName);
        cancellationToken.ThrowIfCancellationRequested();

        var hasOutstandingRepairs = _entries.Values.Any(entry =>
            entry.Status != StorageReplicaRepairStatus.Completed
            && string.Equals(entry.ReplicaBackendName, replicaBackendName, StringComparison.Ordinal));
        return ValueTask.FromResult(hasOutstandingRepairs);
    }

    public ValueTask<IReadOnlyList<StorageReplicaRepairEntry>> ListOutstandingAsync(string? replicaBackendName = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        IEnumerable<StorageReplicaRepairEntry> entries = _entries.Values.Where(static entry => entry.Status != StorageReplicaRepairStatus.Completed);
        if (!string.IsNullOrWhiteSpace(replicaBackendName)) {
            entries = entries.Where(entry => string.Equals(entry.ReplicaBackendName, replicaBackendName, StringComparison.Ordinal));
        }

        IReadOnlyList<StorageReplicaRepairEntry> result = entries
            .OrderBy(entry => entry.CreatedAtUtc)
            .ThenBy(entry => entry.ReplicaBackendName, StringComparer.Ordinal)
            .ToArray();
        return ValueTask.FromResult(result);
    }

    public ValueTask MarkInProgressAsync(string repairId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(repairId);
        cancellationToken.ThrowIfCancellationRequested();

        UpdateEntry(repairId, existing => existing with
        {
            Status = StorageReplicaRepairStatus.InProgress,
            AttemptCount = existing.AttemptCount + 1,
            LastErrorCode = null,
            LastErrorMessage = null,
            UpdatedAtUtc = _timeProvider.GetUtcNow()
        });
        if (_entries.TryGetValue(repairId, out var entry)) {
            _logger.LogInformation(
                "Replica repair {RepairId} is now in progress for {ReplicaBackend}. AttemptCount {AttemptCount}.",
                repairId,
                entry.ReplicaBackendName,
                entry.AttemptCount);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask MarkCompletedAsync(string repairId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(repairId);
        cancellationToken.ThrowIfCancellationRequested();

        if (_entries.TryRemove(repairId, out var entry)) {
            _logger.LogInformation(
                "Replica repair {RepairId} completed for {ReplicaBackend}.",
                repairId,
                entry.ReplicaBackendName);
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask MarkFailedAsync(string repairId, StorageError error, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(repairId);
        ArgumentNullException.ThrowIfNull(error);
        cancellationToken.ThrowIfCancellationRequested();

        UpdateEntry(repairId, existing => existing with
        {
            Status = StorageReplicaRepairStatus.Failed,
            LastErrorCode = error.Code,
            LastErrorMessage = error.Message,
            UpdatedAtUtc = _timeProvider.GetUtcNow()
        });
        if (_entries.TryGetValue(repairId, out var entry)) {
            _logger.LogWarning(
                "Replica repair {RepairId} failed for {ReplicaBackend}. ErrorCode {ErrorCode}.",
                repairId,
                entry.ReplicaBackendName,
                error.Code);
        }

        return ValueTask.CompletedTask;
    }

    private IEnumerable<Measurement<long>> ObserveBacklogSize()
    {
        return _entries.Values
            .Where(static entry => entry.Status != StorageReplicaRepairStatus.Completed)
            .GroupBy(static entry => (entry.ReplicaBackendName, entry.Status))
            .Select(group => new Measurement<long>(
                group.LongCount(),
                new KeyValuePair<string, object?>(
                    IntegratedS3Observability.Tags.ReplicaBackend,
                    group.Key.ReplicaBackendName),
                new KeyValuePair<string, object?>(
                    IntegratedS3Observability.Tags.RepairStatus,
                    group.Key.Status.ToString())));
    }

    private IEnumerable<Measurement<double>> ObserveOldestAge()
    {
        var now = _timeProvider.GetUtcNow();
        return _entries.Values
            .Where(static entry => entry.Status != StorageReplicaRepairStatus.Completed)
            .GroupBy(static entry => entry.ReplicaBackendName)
            .Select(group => new Measurement<double>(
                (now - group.Min(static entry => entry.CreatedAtUtc)).TotalSeconds,
                new KeyValuePair<string, object?>(
                    IntegratedS3Observability.Tags.ReplicaBackend,
                    group.Key)));
    }

    private void UpdateEntry(string repairId, Func<StorageReplicaRepairEntry, StorageReplicaRepairEntry> update)
    {
        while (_entries.TryGetValue(repairId, out var existing)) {
            var updated = update(existing);
            if (_entries.TryUpdate(repairId, updated, existing)) {
                return;
            }
        }
    }
}
