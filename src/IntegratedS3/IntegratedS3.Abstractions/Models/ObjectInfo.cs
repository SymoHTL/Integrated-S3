namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Complete metadata for a stored object.
/// </summary>
public sealed class ObjectInfo
{
    /// <summary>
    /// The name of the bucket that contains this object.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The object key (path) within the bucket.
    /// </summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>
    /// The version identifier, or <see langword="null"/> when versioning is not enabled.
    /// </summary>
    public string? VersionId { get; init; }

    /// <summary>
    /// Whether this version is the latest version of the object.
    /// </summary>
    public bool IsLatest { get; init; }

    /// <summary>
    /// Whether this version is a delete marker.
    /// </summary>
    public bool IsDeleteMarker { get; init; }

    /// <summary>
    /// The size of the object content in bytes.
    /// </summary>
    public long ContentLength { get; init; }

    /// <summary>
    /// The MIME type of the object content.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// The <c>Cache-Control</c> header value for the object.
    /// </summary>
    public string? CacheControl { get; init; }

    /// <summary>
    /// The <c>Content-Disposition</c> header value for the object.
    /// </summary>
    public string? ContentDisposition { get; init; }

    /// <summary>
    /// The <c>Content-Encoding</c> header value for the object.
    /// </summary>
    public string? ContentEncoding { get; init; }

    /// <summary>
    /// The <c>Content-Language</c> header value for the object.
    /// </summary>
    public string? ContentLanguage { get; init; }

    /// <summary>
    /// The expiration date of the object in UTC, or <see langword="null"/> if not set.
    /// </summary>
    public DateTimeOffset? ExpiresUtc { get; init; }

    /// <summary>
    /// The entity tag (ETag) for the object, typically the MD5 hash of the content.
    /// </summary>
    public string? ETag { get; init; }

    /// <summary>
    /// The date and time the object was last modified, in UTC.
    /// </summary>
    public DateTimeOffset LastModifiedUtc { get; init; }

    /// <summary>
    /// User-defined key-value metadata associated with the object.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>
    /// User-defined tags associated with the object.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    /// <summary>
    /// Checksum algorithm-to-value map for content integrity verification.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>
    /// The object retention mode, or <see langword="null"/> if no retention is configured.
    /// </summary>
    public ObjectRetentionMode? RetentionMode { get; init; }

    /// <summary>
    /// The date until which the object is retained, in UTC.
    /// </summary>
    public DateTimeOffset? RetainUntilDateUtc { get; init; }

    /// <summary>
    /// The legal hold status for the object, or <see langword="null"/> if not set.
    /// </summary>
    public ObjectLegalHoldStatus? LegalHoldStatus { get; init; }

    /// <summary>
    /// Server-side encryption information for the object.
    /// </summary>
    public ObjectServerSideEncryptionInfo? ServerSideEncryption { get; init; }

    /// <summary>
    /// Customer-provided encryption information for the object.
    /// </summary>
    public ObjectCustomerEncryptionInfo? CustomerEncryption { get; init; }
}
