namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of an S3 ListParts operation for a multipart upload.
/// </summary>
public sealed class S3ListPartsResult
{
    /// <summary>The name of the bucket.</summary>
    public required string Bucket { get; init; }

    /// <summary>The object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The upload ID identifying the multipart upload.</summary>
    public required string UploadId { get; init; }

    /// <summary>The part number marker indicating where listing began.</summary>
    public int PartNumberMarker { get; init; }

    /// <summary>The part number marker to use for the next page of results.</summary>
    public int? NextPartNumberMarker { get; init; }

    /// <summary>The maximum number of parts requested.</summary>
    public int MaxParts { get; init; }

    /// <summary>Indicates whether the result list was truncated.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The encoding type used for object keys in the response.</summary>
    public string? EncodingType { get; init; }

    /// <summary>The user who initiated the multipart upload.</summary>
    public S3BucketOwner? Initiator { get; init; }

    /// <summary>The owner of the object being uploaded.</summary>
    public S3BucketOwner? Owner { get; init; }

    /// <summary>The storage class used for the uploaded object.</summary>
    public string StorageClass { get; init; } = "STANDARD";

    /// <summary>The checksum algorithm used for the multipart upload.</summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>The checksum type used for the multipart upload.</summary>
    public string? ChecksumType { get; init; }

    /// <summary>The list of uploaded parts.</summary>
    public IReadOnlyList<S3ListPartEntry> Parts { get; init; } = [];
}

/// <summary>
/// Represents a single uploaded part in a <see cref="S3ListPartsResult"/>.
/// </summary>
public sealed class S3ListPartEntry
{
    /// <summary>The part number.</summary>
    public int PartNumber { get; init; }

    /// <summary>The entity tag (ETag) of the uploaded part.</summary>
    public required string ETag { get; init; }

    /// <summary>The date and time the part was last modified, in UTC.</summary>
    public DateTimeOffset LastModifiedUtc { get; init; }

    /// <summary>The size of the part in bytes.</summary>
    public long Size { get; init; }

    /// <summary>The CRC-32 checksum of the part.</summary>
    public string? ChecksumCrc32 { get; init; }

    /// <summary>The CRC-32C checksum of the part.</summary>
    public string? ChecksumCrc32c { get; init; }

    /// <summary>The CRC-64/NVME checksum of the part.</summary>
    public string? ChecksumCrc64Nvme { get; init; }

    /// <summary>The SHA-1 checksum of the part.</summary>
    public string? ChecksumSha1 { get; init; }

    /// <summary>The SHA-256 checksum of the part.</summary>
    public string? ChecksumSha256 { get; init; }
}
