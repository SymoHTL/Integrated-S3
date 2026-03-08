using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.AspNetCore.Endpoints;

public sealed class StorageServiceDocument
{
    public string ServiceName { get; init; } = string.Empty;

    public StorageProviderDocument[] Providers { get; init; } = [];

    public StorageCapabilities Capabilities { get; init; } = new();

    public static StorageServiceDocument FromDescriptor(StorageServiceDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);

        return new StorageServiceDocument
        {
            ServiceName = descriptor.ServiceName,
            Providers = descriptor.Providers.Select(StorageProviderDocument.FromDescriptor).ToArray(),
            Capabilities = descriptor.Capabilities
        };
    }
}
