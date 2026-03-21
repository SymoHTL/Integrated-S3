namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of an S3 InitiateMultipartUpload operation.
/// </summary>
public sealed class S3InitiateMultipartUploadResult
{
    /// <summary>The name of the bucket to which the multipart upload was initiated.</summary>
    public required string Bucket { get; init; }

    /// <summary>The object key for which the multipart upload was initiated.</summary>
    public required string Key { get; init; }

    /// <summary>The upload ID that uniquely identifies this multipart upload.</summary>
    public required string UploadId { get; init; }

    /// <summary>The checksum algorithm used for the multipart upload, if specified.</summary>
    public string? ChecksumAlgorithm { get; init; }
}
