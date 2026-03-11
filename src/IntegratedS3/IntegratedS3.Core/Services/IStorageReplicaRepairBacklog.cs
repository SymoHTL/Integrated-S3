using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

public interface IStorageReplicaRepairBacklog
{
    ValueTask AddAsync(StorageReplicaRepairEntry entry, CancellationToken cancellationToken = default);

    ValueTask<bool> HasOutstandingRepairsAsync(string replicaBackendName, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StorageReplicaRepairEntry>> ListOutstandingAsync(string? replicaBackendName = null, CancellationToken cancellationToken = default);

    ValueTask MarkInProgressAsync(string repairId, CancellationToken cancellationToken = default);

    ValueTask MarkCompletedAsync(string repairId, CancellationToken cancellationToken = default);

    ValueTask MarkFailedAsync(string repairId, StorageError error, CancellationToken cancellationToken = default);
}
