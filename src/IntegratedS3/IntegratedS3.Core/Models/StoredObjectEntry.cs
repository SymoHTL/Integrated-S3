using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Core.Models;

/// <summary>
/// Represents the stored metadata of a single object version in the catalog.
/// </summary>
public sealed class StoredObjectEntry
{
    /// <summary>The name of the storage provider that holds this object.</summary>
    public string ProviderName { get; init; } = string.Empty;

    /// <summary>The bucket that contains this object.</summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>The object key (path) within the bucket.</summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>The version identifier, or <see langword="null"/> if versioning is not enabled.</summary>
    public string? VersionId { get; init; }

    /// <summary>Indicates whether this version is the current (latest) version of the object.</summary>
    public bool IsLatest { get; init; }

    /// <summary>Indicates whether this entry represents a delete marker rather than an actual object.</summary>
    public bool IsDeleteMarker { get; init; }

    /// <summary>The size of the object body in bytes.</summary>
    public long ContentLength { get; init; }

    /// <summary>The MIME content type of the object.</summary>
    public string? ContentType { get; init; }

    /// <summary>The <c>Cache-Control</c> directive for the object.</summary>
    public string? CacheControl { get; init; }

    /// <summary>The <c>Content-Disposition</c> header value for the object.</summary>
    public string? ContentDisposition { get; init; }

    /// <summary>The <c>Content-Encoding</c> applied to the object (e.g. <c>gzip</c>).</summary>
    public string? ContentEncoding { get; init; }

    /// <summary>The language the object content is intended for.</summary>
    public string? ContentLanguage { get; init; }

    /// <summary>The UTC date/time after which the object should be considered stale, or <see langword="null"/> if not set.</summary>
    public DateTimeOffset? ExpiresUtc { get; init; }

    /// <summary>The entity tag (ETag) for the object, typically an MD5 or multipart hash.</summary>
    public string? ETag { get; init; }

    /// <summary>The UTC date/time the object was last modified.</summary>
    public DateTimeOffset LastModifiedUtc { get; init; }

    /// <summary>User-defined metadata key/value pairs associated with the object.</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>Object tags as key/value pairs.</summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    /// <summary>Checksum values keyed by lower-case algorithm name (e.g. <c>sha256</c>, <c>crc32</c>).</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>The Object Lock retention mode, or <see langword="null"/> if not configured.</summary>
    public ObjectRetentionMode? RetentionMode { get; init; }

    /// <summary>The UTC date/time until which the object is retained under Object Lock, or <see langword="null"/>.</summary>
    public DateTimeOffset? RetainUntilDateUtc { get; init; }

    /// <summary>The legal-hold status of the object, or <see langword="null"/> if not set.</summary>
    public ObjectLegalHoldStatus? LegalHoldStatus { get; init; }

    /// <summary>Server-side encryption information for the object, or <see langword="null"/> if unencrypted.</summary>
    public ObjectServerSideEncryptionInfo? ServerSideEncryption { get; init; }

    /// <summary>The UTC date/time when this catalog entry was last synchronized from the provider.</summary>
    public DateTimeOffset LastSyncedAtUtc { get; init; }
}
