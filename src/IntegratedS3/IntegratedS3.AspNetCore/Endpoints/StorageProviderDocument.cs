using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.AspNetCore.Endpoints;

/// <summary>
/// JSON-serializable document representing a storage provider's metadata in API responses.
/// Used by the service discovery endpoint.
/// </summary>
public sealed class StorageProviderDocument
{
    /// <summary>
    /// Gets the unique name of the storage provider.
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Gets the kind of storage provider (e.g., <c>"disk"</c>, <c>"s3"</c>).
    /// </summary>
    public string Kind { get; init; } = string.Empty;

    /// <summary>
    /// Gets a value indicating whether this provider is the primary storage backend.
    /// </summary>
    public bool IsPrimary { get; init; }

    /// <summary>
    /// Gets an optional human-readable description of the storage provider.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Gets the operating mode of the storage provider.
    /// </summary>
    public StorageProviderMode Mode { get; init; }

    /// <summary>
    /// Gets the capabilities supported by this storage provider.
    /// </summary>
    public StorageCapabilities Capabilities { get; init; } = new();

    /// <summary>
    /// Gets the object location descriptor for this storage provider.
    /// </summary>
    public StorageObjectLocationDescriptor ObjectLocation { get; init; } = new();

    /// <summary>
    /// Gets the support state descriptor for this storage provider.
    /// </summary>
    public StorageSupportStateDescriptor SupportState { get; init; } = new();

    /// <summary>
    /// Creates a <see cref="StorageProviderDocument"/> from a <see cref="StorageProviderDescriptor"/>.
    /// </summary>
    /// <param name="descriptor">The storage provider descriptor to convert.</param>
    /// <returns>A new <see cref="StorageProviderDocument"/> populated from the descriptor.</returns>
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
