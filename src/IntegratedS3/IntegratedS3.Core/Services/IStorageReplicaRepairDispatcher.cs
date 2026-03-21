using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Dispatches repair jobs for replica inconsistencies, coordinating the execution of
/// repair operations and tracking their outcome in the backlog.
/// </summary>
public interface IStorageReplicaRepairDispatcher
{
    /// <summary>
    /// Dispatches a repair operation for the specified entry, executing the provided repair delegate
    /// and updating the backlog with the result.
    /// </summary>
    /// <param name="entry">The <see cref="StorageReplicaRepairEntry"/> describing the object to repair.</param>
    /// <param name="repairOperation">A delegate that performs the actual repair and returns a <see cref="StorageError"/> on failure, or <see langword="null"/> on success.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask DispatchAsync(
        StorageReplicaRepairEntry entry,
        Func<CancellationToken, ValueTask<StorageError?>> repairOperation,
        CancellationToken cancellationToken = default);
}
