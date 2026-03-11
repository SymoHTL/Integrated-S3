using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

public interface IStorageReplicaRepairDispatcher
{
    ValueTask DispatchAsync(
        StorageReplicaRepairEntry entry,
        Func<CancellationToken, ValueTask<StorageError?>> repairOperation,
        CancellationToken cancellationToken = default);
}
