namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskObjectMetadata
{
    public string? ContentType { get; init; }

    public Dictionary<string, string>? Metadata { get; init; }
}
