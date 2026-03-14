namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Describes the object-location access modes advertised by a provider or deployment.
/// </summary>
public sealed class StorageObjectLocationDescriptor
{
    /// <summary>The access mode used when the caller does not request a specific mode.</summary>
    public StorageObjectAccessMode DefaultAccessMode { get; set; } = StorageObjectAccessMode.ProxyStream;

    /// <summary>The full set of access modes that the provider may expose for object reads.</summary>
    public List<StorageObjectAccessMode> SupportedAccessModes { get; set; } = [];
}
