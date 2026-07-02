using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

/// <summary>Applies pending replica repair entries to bring a replica backend back in sync with the primary.</summary>
public interface IStorageReplicaRepairService
{
    /// <summary>Attempts to repair the divergence described by <paramref name="entry"/>.</summary>
    /// <param name="entry">The repair backlog entry to apply.</param>
    /// <param name="cancellationToken">A token to cancel the repair attempt.</param>
    /// <returns>The error that prevented the repair, or <see langword="null"/> when the repair succeeded.</returns>
    ValueTask<StorageError?> RepairAsync(StorageReplicaRepairEntry entry, CancellationToken cancellationToken = default);
}
