using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the UploadPart operation in a multipart upload.</summary>
public sealed class UploadMultipartPartRequest
{
    /// <summary>The name of the bucket for the multipart upload.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The upload identifier returned by the InitiateMultipartUpload operation.</summary>
    public required string UploadId { get; init; }

    /// <summary>The part number identifying this part within the upload (1–10000).</summary>
    public required int PartNumber { get; init; }

    /// <summary>The part data stream to upload.</summary>
    public required Stream Content { get; init; }

    /// <summary>The size of the part data in bytes, if known.</summary>
    public long? ContentLength { get; init; }

    /// <summary>The checksum algorithm to use for part integrity verification.</summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>Checksum values for the part keyed by algorithm name.</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>Customer-provided encryption settings for the upload.</summary>
    public ObjectCustomerEncryptionSettings? CustomerEncryption { get; init; }
}
