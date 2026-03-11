using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Abstractions.Models;

public sealed class StorageProviderDescriptor
{
    public string Name { get; set; } = string.Empty;

    public string Kind { get; set; } = string.Empty;

    public bool IsPrimary { get; set; }

    public string? Description { get; set; }

    public StorageProviderMode Mode { get; set; } = StorageProviderMode.Managed;

    public StorageCapabilities Capabilities { get; set; } = new();

    public StorageObjectLocationDescriptor ObjectLocation { get; set; } = new();

    public StorageSupportStateDescriptor SupportState { get; set; } = new();
}
