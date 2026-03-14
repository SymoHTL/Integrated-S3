using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Core.Models;

public sealed class StoredObjectEntry
{
    public string ProviderName { get; init; } = string.Empty;

    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public string? VersionId { get; init; }

    public bool IsLatest { get; init; }

    public bool IsDeleteMarker { get; init; }

    public long ContentLength { get; init; }

    public string? ContentType { get; init; }

    public string? CacheControl { get; init; }

    public string? ContentDisposition { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentLanguage { get; init; }

    public DateTimeOffset? ExpiresUtc { get; init; }

    public string? ETag { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    public ObjectServerSideEncryptionInfo? ServerSideEncryption { get; init; }

    public DateTimeOffset LastSyncedAtUtc { get; init; }
}
