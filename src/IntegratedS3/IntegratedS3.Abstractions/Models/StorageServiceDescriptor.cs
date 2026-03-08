using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Abstractions.Models;

public sealed class StorageServiceDescriptor
{
    public string ServiceName { get; init; } = string.Empty;

    public IReadOnlyList<StorageProviderDescriptor> Providers { get; init; } = Array.Empty<StorageProviderDescriptor>();

    public StorageCapabilities Capabilities { get; init; } = new();
}
