using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Abstractions.Models;

public sealed class StorageProviderDescriptor
{
    public string Name { get; set; } = string.Empty;

    public string Kind { get; set; } = string.Empty;

    public bool IsPrimary { get; set; }

    public string? Description { get; set; }

    public StorageCapabilities Capabilities { get; set; } = new();
}
