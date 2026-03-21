using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Abstractions.Services;

/// <summary>
/// Provides discovery of the storage capabilities supported by the current service configuration.
/// </summary>
public interface IStorageCapabilityProvider
{
    /// <summary>
    /// Returns the aggregated <see cref="StorageCapabilities"/> across all registered storage backends.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageCapabilities"/> describing the supported feature set.</returns>
    ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default);
}
