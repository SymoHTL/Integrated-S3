using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Services;

public interface IStorageServiceDescriptorProvider
{
    ValueTask<StorageServiceDescriptor> GetServiceDescriptorAsync(CancellationToken cancellationToken = default);
}
