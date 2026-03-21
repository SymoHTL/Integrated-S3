using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Core.Persistence;

/// <summary>
/// EF Core entity representing an object entry in the IntegratedS3 catalog.
/// </summary>
public sealed class ObjectCatalogRecord
{
    /// <summary>Gets or sets the auto-generated primary key.</summary>
    public int Id { get; set; }

    /// <summary>Gets or sets the name of the storage provider that owns this object.</summary>
    public string ProviderName { get; set; } = string.Empty;

    /// <summary>Gets or sets the bucket name containing this object.</summary>
    public string BucketName { get; set; } = string.Empty;

    /// <summary>Gets or sets the object key (path) within the bucket.</summary>
    public string Key { get; set; } = string.Empty;

    /// <summary>Gets or sets the version identifier, or <see langword="null"/> for unversioned objects.</summary>
    public string? VersionId { get; set; }

    /// <summary>Gets or sets a value indicating whether this is the latest version of the object.</summary>
    public bool IsLatest { get; set; }

    /// <summary>Gets or sets a value indicating whether this record represents a delete marker.</summary>
    public bool IsDeleteMarker { get; set; }

    /// <summary>Gets or sets the size of the object in bytes.</summary>
    public long ContentLength { get; set; }

    /// <summary>Gets or sets the MIME content type of the object.</summary>
    public string? ContentType { get; set; }

    /// <summary>Gets or sets the <c>Cache-Control</c> header value for the object.</summary>
    public string? CacheControl { get; set; }

    /// <summary>Gets or sets the <c>Content-Disposition</c> header value for the object.</summary>
    public string? ContentDisposition { get; set; }

    /// <summary>Gets or sets the <c>Content-Encoding</c> header value for the object.</summary>
    public string? ContentEncoding { get; set; }

    /// <summary>Gets or sets the <c>Content-Language</c> header value for the object.</summary>
    public string? ContentLanguage { get; set; }

    /// <summary>Gets or sets the UTC expiration timestamp for the object, if any.</summary>
    public DateTimeOffset? ExpiresUtc { get; set; }

    /// <summary>Gets or sets the entity tag (ETag) of the object.</summary>
    public string? ETag { get; set; }

    /// <summary>Gets or sets the UTC timestamp when the object was last modified.</summary>
    public DateTimeOffset LastModifiedUtc { get; set; }

    /// <summary>Gets or sets the serialized JSON representation of user-defined metadata.</summary>
    public string? MetadataJson { get; set; }

    /// <summary>Gets or sets the serialized JSON representation of object tags.</summary>
    public string? TagsJson { get; set; }

    /// <summary>Gets or sets the serialized JSON representation of object checksums.</summary>
    public string? ChecksumsJson { get; set; }

    /// <summary>Gets or sets the object-lock retention mode, if any.</summary>
    public ObjectRetentionMode? RetentionMode { get; set; }

    /// <summary>Gets or sets the UTC date until which the object is retained under object lock.</summary>
    public DateTimeOffset? RetainUntilDateUtc { get; set; }

    /// <summary>Gets or sets the legal hold status of the object, if any.</summary>
    public ObjectLegalHoldStatus? LegalHoldStatus { get; set; }

    /// <summary>Gets or sets the server-side encryption algorithm applied to the object.</summary>
    public ObjectServerSideEncryptionAlgorithm? ServerSideEncryptionAlgorithm { get; set; }

    /// <summary>Gets or sets the server-side encryption key identifier, if applicable.</summary>
    public string? ServerSideEncryptionKeyId { get; set; }

    /// <summary>Gets or sets the UTC timestamp of the last catalog synchronization for this object.</summary>
    public DateTimeOffset LastSyncedAtUtc { get; set; }
}
