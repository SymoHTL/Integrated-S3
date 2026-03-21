using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Evaluates the overall health of a storage backend, potentially combining probe results
/// with cached state or additional heuristics.
/// </summary>
public interface IStorageBackendHealthEvaluator
{
    /// <summary>
    /// Returns the evaluated health status for the specified storage backend.
    /// </summary>
    /// <param name="backend">The <see cref="IStorageBackend"/> to evaluate.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageBackendHealthStatus"/> representing the evaluated health.</returns>
    ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default);
}