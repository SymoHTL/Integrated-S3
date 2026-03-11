namespace IntegratedS3.Abstractions.Models;

public sealed class StorageObjectLocationDescriptor
{
    public StorageObjectAccessMode DefaultAccessMode { get; set; } = StorageObjectAccessMode.ProxyStream;

    public List<StorageObjectAccessMode> SupportedAccessModes { get; set; } = [];
}
