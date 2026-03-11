namespace IntegratedS3.Abstractions.Models;

public sealed class StorageResolvedObjectLocation
{
    public StorageObjectAccessMode AccessMode { get; set; } = StorageObjectAccessMode.ProxyStream;

    public Uri? Location { get; set; }

    public DateTimeOffset? ExpiresAtUtc { get; set; }

    public Dictionary<string, string> Headers { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
