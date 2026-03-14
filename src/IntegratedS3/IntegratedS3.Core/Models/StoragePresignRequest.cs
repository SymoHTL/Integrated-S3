namespace IntegratedS3.Core.Models;

public sealed class StoragePresignRequest
{
    public required StoragePresignOperation Operation { get; init; }

    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required int ExpiresInSeconds { get; init; }

    public string? VersionId { get; init; }

    public string? ContentType { get; init; }

    /// <summary>
    /// Optional checksum algorithm for checksum-aware PUT uploads.
    /// The value uses the provider-agnostic lower-case form (<c>sha256</c>, <c>sha1</c>, <c>crc32</c>, <c>crc32c</c>).
    /// </summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>
    /// Optional checksum values keyed by lower-case provider-agnostic algorithm name.
    /// When supplied for PUT presigns, the values may be signed into the returned grant so callers can replay them safely.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>
    /// The caller's preferred access mode for the returned presigned grant.
    /// When <see langword="null" />, the server keeps proxy-mode issuance as the stable default
    /// rather than inferring <see cref="StorageAccessMode.Direct" /> or
    /// <see cref="StorageAccessMode.Delegated" /> from provider discovery.
    /// Strategies may honor, downgrade, or ignore an explicit preference depending on provider capabilities.
    /// </summary>
    public StorageAccessMode? PreferredAccessMode { get; init; }
}
