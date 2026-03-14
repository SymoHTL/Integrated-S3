using IntegratedS3.Abstractions.Errors;

namespace IntegratedS3.Core.Services;

public interface IStorageReplicaRepairService
{
    ValueTask<StorageError?> RepairAsync(StorageReplicaRepairEntry entry, CancellationToken cancellationToken = default);
}
