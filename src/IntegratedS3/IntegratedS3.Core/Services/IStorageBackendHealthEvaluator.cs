using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

public interface IStorageBackendHealthEvaluator
{
    ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default);
}