namespace IntegratedS3.Core.Persistence;

/// <summary>
/// EF Core entity representing the state of an in-progress multipart upload in the IntegratedS3 catalog.
/// </summary>
public sealed class MultipartUploadCatalogRecord
{
    /// <summary>Gets or sets the auto-generated primary key.</summary>
    public int Id { get; set; }

    /// <summary>Gets or sets the name of the storage provider that owns this upload.</summary>
    public string ProviderName { get; set; } = string.Empty;

    /// <summary>Gets or sets the bucket name for the multipart upload.</summary>
    public string BucketName { get; set; } = string.Empty;

    /// <summary>Gets or sets the object key (path) that the upload targets.</summary>
    public string Key { get; set; } = string.Empty;

    /// <summary>Gets or sets the provider-assigned multipart upload identifier.</summary>
    public string UploadId { get; set; } = string.Empty;

    /// <summary>Gets or sets the UTC timestamp when the multipart upload was initiated.</summary>
    public DateTimeOffset InitiatedAtUtc { get; set; }

    /// <summary>Gets or sets the MIME content type for the completed object.</summary>
    public string? ContentType { get; set; }

    /// <summary>Gets or sets the <c>Cache-Control</c> header value for the completed object.</summary>
    public string? CacheControl { get; set; }

    /// <summary>Gets or sets the <c>Content-Disposition</c> header value for the completed object.</summary>
    public string? ContentDisposition { get; set; }

    /// <summary>Gets or sets the <c>Content-Encoding</c> header value for the completed object.</summary>
    public string? ContentEncoding { get; set; }

    /// <summary>Gets or sets the <c>Content-Language</c> header value for the completed object.</summary>
    public string? ContentLanguage { get; set; }

    /// <summary>Gets or sets the UTC expiration timestamp for the completed object, if any.</summary>
    public DateTimeOffset? ExpiresUtc { get; set; }

    /// <summary>Gets or sets the serialized JSON representation of user-defined metadata.</summary>
    public string? MetadataJson { get; set; }

    /// <summary>Gets or sets the serialized JSON representation of object tags.</summary>
    public string? TagsJson { get; set; }

    /// <summary>Gets or sets the checksum algorithm used for part-level integrity verification.</summary>
    public string? ChecksumAlgorithm { get; set; }

    /// <summary>Gets or sets the UTC timestamp of the last catalog synchronization for this upload.</summary>
    public DateTimeOffset LastSyncedAtUtc { get; set; }
}
