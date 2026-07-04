namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskMultipartUploadState
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public DateTimeOffset InitiatedAtUtc { get; init; }

    public string? ContentType { get; init; }

    public string? CacheControl { get; init; }

    public string? ContentDisposition { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentLanguage { get; init; }

    public DateTimeOffset? ExpiresUtc { get; init; }

    /// <summary>
    /// The verbatim <c>Expires</c> header value supplied at upload initiation, preserved so the
    /// completed object echoes it back unchanged. Null for legacy state written before this field.
    /// </summary>
    public string? Expires { get; init; }

    public Dictionary<string, string>? Metadata { get; init; }

    public Dictionary<string, string>? Tags { get; init; }

    public string? ChecksumAlgorithm { get; init; }

    /// <summary>
    /// The storage class supplied at upload initiation, applied to the completed object. Null for
    /// legacy state written before this field existed.
    /// </summary>
    public string? StorageClass { get; init; }
}
