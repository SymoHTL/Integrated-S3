using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.AspNetCore.Endpoints;

public sealed class StorageProviderDocument
{
    public string Name { get; init; } = string.Empty;

    public string Kind { get; init; } = string.Empty;

    public bool IsPrimary { get; init; }

    public string? Description { get; init; }

    public StorageProviderMode Mode { get; init; }

    public StorageCapabilities Capabilities { get; init; } = new();

    public StorageObjectLocationDescriptor ObjectLocation { get; init; } = new();

    public StorageSupportStateDescriptor SupportState { get; init; } = new();

    public static StorageProviderDocument FromDescriptor(StorageProviderDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);

        return new StorageProviderDocument
        {
            Name = descriptor.Name,
            Kind = descriptor.Kind,
            IsPrimary = descriptor.IsPrimary,
            Description = descriptor.Description,
            Mode = descriptor.Mode,
            Capabilities = descriptor.Capabilities,
            ObjectLocation = new StorageObjectLocationDescriptor
            {
                DefaultAccessMode = descriptor.ObjectLocation.DefaultAccessMode,
                SupportedAccessModes = descriptor.ObjectLocation.SupportedAccessModes.Count == 0
                    ? [descriptor.ObjectLocation.DefaultAccessMode]
                    : [.. descriptor.ObjectLocation.SupportedAccessModes]
            },
            SupportState = descriptor.SupportState
        };
    }
}
