using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the CopyObject operation.</summary>
public sealed class CopyObjectRequest
{
    /// <summary>The name of the bucket containing the source object.</summary>
    public required string SourceBucketName { get; init; }

    /// <summary>The key of the source object to copy.</summary>
    public required string SourceKey { get; init; }

    /// <summary>The name of the destination bucket.</summary>
    public required string DestinationBucketName { get; init; }

    /// <summary>The key for the destination object.</summary>
    public required string DestinationKey { get; init; }

    /// <summary>The version identifier of the source object to copy.</summary>
    public string? SourceVersionId { get; init; }

    /// <summary>Copy only if the source object ETag matches this value.</summary>
    public string? SourceIfMatchETag { get; init; }

    /// <summary>Copy only if the source object ETag does not match this value.</summary>
    public string? SourceIfNoneMatchETag { get; init; }

    /// <summary>Copy only if the source object was modified after this date (UTC).</summary>
    public DateTimeOffset? SourceIfModifiedSinceUtc { get; init; }

    /// <summary>Copy only if the source object was not modified after this date (UTC).</summary>
    public DateTimeOffset? SourceIfUnmodifiedSinceUtc { get; init; }

    /// <summary>Whether to copy or replace the source object metadata.</summary>
    public CopyObjectMetadataDirective MetadataDirective { get; init; } = CopyObjectMetadataDirective.Copy;

    /// <summary>The MIME type for the destination object (used when replacing metadata).</summary>
    public string? ContentType { get; init; }

    /// <summary>The Cache-Control header for the destination object.</summary>
    public string? CacheControl { get; init; }

    /// <summary>The Content-Disposition header for the destination object.</summary>
    public string? ContentDisposition { get; init; }

    /// <summary>The Content-Encoding header for the destination object.</summary>
    public string? ContentEncoding { get; init; }

    /// <summary>The Content-Language header for the destination object.</summary>
    public string? ContentLanguage { get; init; }

    /// <summary>The expiration date for the destination object, in UTC.</summary>
    public DateTimeOffset? ExpiresUtc { get; init; }

    /// <summary>User-defined metadata for the destination object (used when replacing metadata).</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>Server-side encryption settings for the source object.</summary>
    public ObjectServerSideEncryptionSettings? SourceServerSideEncryption { get; init; }

    /// <summary>Server-side encryption settings for the destination object.</summary>
    public ObjectServerSideEncryptionSettings? DestinationServerSideEncryption { get; init; }

    /// <summary>Customer-provided encryption settings for the source object.</summary>
    public ObjectCustomerEncryptionSettings? SourceCustomerEncryption { get; init; }

    /// <summary>Customer-provided encryption settings for the destination object.</summary>
    public ObjectCustomerEncryptionSettings? DestinationCustomerEncryption { get; init; }

    /// <summary>Whether to copy or replace the source object tags.</summary>
    public ObjectTaggingDirective TaggingDirective { get; init; } = ObjectTaggingDirective.Copy;

    /// <summary>Tag key-value pairs for the destination object (used when replacing tags).</summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    /// <summary>The checksum algorithm to use for the copy.</summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>Checksum values for the destination object keyed by algorithm name.</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>The storage class for the destination object.</summary>
    public string? StorageClass { get; init; }

    /// <summary>When <see langword="true"/>, overwrites an existing destination object. Defaults to <see langword="true"/>.</summary>
    public bool OverwriteIfExists { get; init; } = true;
}
