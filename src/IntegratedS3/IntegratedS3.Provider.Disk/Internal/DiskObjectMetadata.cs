namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskObjectMetadata
{
    public string? VersionId { get; init; }

    public bool IsLatest { get; init; }

    public bool IsDeleteMarker { get; init; }

    public DateTimeOffset? LastModifiedUtc { get; init; }

    public string? ContentType { get; init; }

    public string? CacheControl { get; init; }

    public string? ContentDisposition { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentLanguage { get; init; }

    public DateTimeOffset? ExpiresUtc { get; init; }

    public Dictionary<string, string>? Metadata { get; init; }

    public Dictionary<string, string>? Tags { get; init; }

    public Dictionary<string, string>? Checksums { get; init; }
}
