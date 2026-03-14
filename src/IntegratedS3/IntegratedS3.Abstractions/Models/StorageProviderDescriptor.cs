using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Describes a provider as exposed to configuration, service documents, and capability reporting.
/// </summary>
public sealed class StorageProviderDescriptor
{
    /// <summary>The provider name reported to consumers.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>The provider kind, such as <c>disk</c> or <c>s3</c>.</summary>
    public string Kind { get; set; } = string.Empty;

    /// <summary>Whether this provider is the deployment's primary write target.</summary>
    public bool IsPrimary { get; set; }

    /// <summary>An optional human-readable description for dashboards and service documents.</summary>
    public string? Description { get; set; }

    /// <summary>The high-level orchestration mode used for this provider.</summary>
    public StorageProviderMode Mode { get; set; } = StorageProviderMode.Managed;

    /// <summary>The runtime capability descriptor exposed for this provider.</summary>
    public StorageCapabilities Capabilities { get; set; } = new();

    /// <summary>The object-location access modes advertised by this provider.</summary>
    public StorageObjectLocationDescriptor ObjectLocation { get; set; } = new();

    /// <summary>The state-ownership descriptor for advanced features exposed by this provider.</summary>
    public StorageSupportStateDescriptor SupportState { get; set; } = new();
}
