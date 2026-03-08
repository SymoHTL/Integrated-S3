using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

internal sealed class DefaultStorageBackendHealthEvaluator : IStorageBackendHealthEvaluator
{
    public ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(backend);
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(StorageBackendHealthStatus.Healthy);
    }
}