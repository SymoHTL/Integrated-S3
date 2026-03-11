using System.Collections.Concurrent;
using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

internal sealed class InMemoryStorageReplicaRepairBacklog(TimeProvider timeProvider) : IStorageReplicaRepairBacklog
{
    private readonly ConcurrentDictionary<string, StorageReplicaRepairEntry> _entries = new(StringComparer.Ordinal);

    public ValueTask AddAsync(StorageReplicaRepairEntry entry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        cancellationToken.ThrowIfCancellationRequested();

        _entries[entry.Id] = entry;
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
            UpdatedAtUtc = timeProvider.GetUtcNow()
        });

        return ValueTask.CompletedTask;
    }

    public ValueTask MarkCompletedAsync(string repairId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(repairId);
        cancellationToken.ThrowIfCancellationRequested();

        _entries.TryRemove(repairId, out _);
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
            UpdatedAtUtc = timeProvider.GetUtcNow()
        });

        return ValueTask.CompletedTask;
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
