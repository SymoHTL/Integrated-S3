using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Tracks storage objects that need cross-backend replica repair due to detected inconsistencies.
/// </summary>
public interface IStorageReplicaRepairBacklog
{
    /// <summary>
    /// Enqueues a new repair entry into the backlog.
    /// </summary>
    /// <param name="entry">The <see cref="StorageReplicaRepairEntry"/> describing the repair work needed.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask AddAsync(StorageReplicaRepairEntry entry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Determines whether the specified replica backend has any outstanding (pending or in-progress) repairs.
    /// </summary>
    /// <param name="replicaBackendName">The name of the replica backend to check.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns><see langword="true"/> if outstanding repairs exist; otherwise <see langword="false"/>.</returns>
    ValueTask<bool> HasOutstandingRepairsAsync(string replicaBackendName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists outstanding repair entries, optionally filtered by replica backend.
    /// </summary>
    /// <param name="replicaBackendName">If specified, limits results to this backend; otherwise lists all backends.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A read-only list of outstanding <see cref="StorageReplicaRepairEntry"/> instances.</returns>
    ValueTask<IReadOnlyList<StorageReplicaRepairEntry>> ListOutstandingAsync(string? replicaBackendName = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks the specified repair entry as in-progress.
    /// </summary>
    /// <param name="repairId">The unique identifier of the repair entry.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask MarkInProgressAsync(string repairId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks the specified repair entry as successfully completed.
    /// </summary>
    /// <param name="repairId">The unique identifier of the repair entry.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask MarkCompletedAsync(string repairId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks the specified repair entry as failed with the given error.
    /// </summary>
    /// <param name="repairId">The unique identifier of the repair entry.</param>
    /// <param name="error">The <see cref="StorageError"/> describing the failure.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask MarkFailedAsync(string repairId, StorageError error, CancellationToken cancellationToken = default);
}
