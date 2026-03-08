using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Abstractions.Services;

public interface IStorageCapabilityProvider
{
    ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default);
}
