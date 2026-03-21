using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Services;

/// <summary>
/// Provides metadata describing the storage service and its registered providers.
/// </summary>
public interface IStorageServiceDescriptorProvider
{
    /// <summary>
    /// Returns a descriptor containing the service name, provider list, and aggregated capabilities.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageServiceDescriptor"/> describing the storage service.</returns>
    ValueTask<StorageServiceDescriptor> GetServiceDescriptorAsync(CancellationToken cancellationToken = default);
}
