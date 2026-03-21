namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Describes the object location capabilities of a storage provider.
/// </summary>
public sealed class StorageObjectLocationDescriptor
{
    /// <summary>
    /// The default <see cref="StorageObjectAccessMode"/> used when no specific mode is requested.
    /// </summary>
    public StorageObjectAccessMode DefaultAccessMode { get; set; } = StorageObjectAccessMode.ProxyStream;

    /// <summary>
    /// The access modes supported by this provider.
    /// </summary>
    public List<StorageObjectAccessMode> SupportedAccessModes { get; set; } = [];
}
