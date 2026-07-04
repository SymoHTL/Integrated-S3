namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskObjectMetadata
{
    public string? VersionId { get; init; }

    public bool IsLatest { get; init; }

    public bool IsDeleteMarker { get; init; }

    /// <summary>
    /// The S3 content ETag persisted at write time so it is independent of filesystem mtime.
    /// Single-part objects store the lowercase-hex MD5 of the content; multipart objects store the
    /// composite form <c>&lt;hex(MD5(concat(partMd5Bytes)))&gt;-&lt;partCount&gt;</c>. Null for delete
    /// markers and for legacy metadata written before this field existed (callers fall back to
    /// deriving the ETag from the stored checksums).
    /// </summary>
    public string? ETag { get; init; }

    public DateTimeOffset? LastModifiedUtc { get; init; }

    public string? ContentType { get; init; }

    public string? CacheControl { get; init; }

    public string? ContentDisposition { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentLanguage { get; init; }

    public DateTimeOffset? ExpiresUtc { get; init; }

    /// <summary>
    /// The verbatim <c>Expires</c> header value as supplied at write time. AWS treats <c>Expires</c>
    /// as an opaque string that round-trips unchanged; this preserves it so GET/HEAD echo it back
    /// exactly. Null for legacy metadata written before this field existed.
    /// </summary>
    public string? Expires { get; init; }

    public Dictionary<string, string>? Metadata { get; init; }

    public Dictionary<string, string>? Tags { get; init; }

    public Dictionary<string, string>? Checksums { get; init; }
}
