using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Full descriptor for a registered storage provider, including its capabilities and configuration.
/// </summary>
public sealed class StorageProviderDescriptor
{
    /// <summary>
    /// The unique name of this provider registration.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// The kind of provider (e.g., "Disk", "S3").
    /// </summary>
    public string Kind { get; set; } = string.Empty;

    /// <summary>
    /// Whether this is the primary (default) storage provider.
    /// </summary>
    public bool IsPrimary { get; set; }

    /// <summary>
    /// An optional human-readable description of the provider.
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// How the provider operates within IntegratedS3.
    /// </summary>
    public StorageProviderMode Mode { get; set; } = StorageProviderMode.Managed;

    /// <summary>
    /// The capabilities supported by this provider.
    /// </summary>
    public StorageCapabilities Capabilities { get; set; } = new();

    /// <summary>
    /// Object location capabilities of this provider.
    /// </summary>
    public StorageObjectLocationDescriptor ObjectLocation { get; set; } = new();

    /// <summary>
    /// The current support state of this provider.
    /// </summary>
    public StorageSupportStateDescriptor SupportState { get; set; } = new();
}
