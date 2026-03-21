using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Top-level service descriptor exposing all registered providers and aggregated capabilities.
/// </summary>
public sealed class StorageServiceDescriptor
{
    /// <summary>
    /// The display name of the storage service.
    /// </summary>
    public string ServiceName { get; init; } = string.Empty;

    /// <summary>
    /// The list of registered storage providers.
    /// </summary>
    public IReadOnlyList<StorageProviderDescriptor> Providers { get; init; } = Array.Empty<StorageProviderDescriptor>();

    /// <summary>
    /// The aggregated capabilities across all registered providers.
    /// </summary>
    public StorageCapabilities Capabilities { get; init; } = new();
}
