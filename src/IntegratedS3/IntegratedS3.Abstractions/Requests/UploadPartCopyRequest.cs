using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the UploadPartCopy operation in a multipart upload.</summary>
public sealed class UploadPartCopyRequest
{
    /// <summary>The name of the destination bucket for the multipart upload.</summary>
    public required string BucketName { get; init; }

    /// <summary>The destination object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The upload identifier returned by the InitiateMultipartUpload operation.</summary>
    public required string UploadId { get; init; }

    /// <summary>The part number identifying this part within the upload (1–10000).</summary>
    public required int PartNumber { get; init; }

    /// <summary>The name of the bucket containing the source object.</summary>
    public required string SourceBucketName { get; init; }

    /// <summary>The key of the source object to copy from.</summary>
    public required string SourceKey { get; init; }

    /// <summary>The version identifier of the source object.</summary>
    public string? SourceVersionId { get; init; }

    /// <summary>Copy only if the source object ETag matches this value.</summary>
    public string? SourceIfMatchETag { get; init; }

    /// <summary>Copy only if the source object ETag does not match this value.</summary>
    public string? SourceIfNoneMatchETag { get; init; }

    /// <summary>Copy only if the source object was modified after this date (UTC).</summary>
    public DateTimeOffset? SourceIfModifiedSinceUtc { get; init; }

    /// <summary>Copy only if the source object was not modified after this date (UTC).</summary>
    public DateTimeOffset? SourceIfUnmodifiedSinceUtc { get; init; }

    /// <summary>The byte range to copy from the source object.</summary>
    public ObjectRange? SourceRange { get; init; }

    /// <summary>The checksum algorithm to use for part integrity verification.</summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>Checksum values for the part keyed by algorithm name.</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>Customer-provided encryption settings for the source object.</summary>
    public ObjectCustomerEncryptionSettings? SourceCustomerEncryption { get; init; }

    /// <summary>Customer-provided encryption settings for the destination object.</summary>
    public ObjectCustomerEncryptionSettings? DestinationCustomerEncryption { get; init; }
}
