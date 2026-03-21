using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Health-check probe that tests connectivity and responsiveness of a single storage backend.
/// </summary>
public interface IStorageBackendHealthProbe
{
    /// <summary>
    /// Probes the specified storage backend and returns its current health status.
    /// </summary>
    /// <param name="backend">The <see cref="IStorageBackend"/> to probe.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageBackendHealthStatus"/> representing the probe result.</returns>
    ValueTask<StorageBackendHealthStatus> ProbeAsync(IStorageBackend backend, CancellationToken cancellationToken = default);
}
