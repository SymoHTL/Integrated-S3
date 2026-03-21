namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of a completed S3 multipart upload.
/// </summary>
public sealed class S3CompleteMultipartUploadResult
{
    /// <summary>The URI that identifies the newly created object.</summary>
    public string? Location { get; init; }

    /// <summary>The name of the bucket containing the object.</summary>
    public required string Bucket { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The entity tag (ETag) of the assembled object.</summary>
    public required string ETag { get; init; }

    /// <summary>The CRC-32 checksum of the object.</summary>
    public string? ChecksumCrc32 { get; init; }

    /// <summary>The CRC-32C checksum of the object.</summary>
    public string? ChecksumCrc32c { get; init; }

    /// <summary>The SHA-1 checksum of the object.</summary>
    public string? ChecksumSha1 { get; init; }

    /// <summary>The SHA-256 checksum of the object.</summary>
    public string? ChecksumSha256 { get; init; }

    /// <summary>The checksum type used for the object.</summary>
    public string? ChecksumType { get; init; }
}
