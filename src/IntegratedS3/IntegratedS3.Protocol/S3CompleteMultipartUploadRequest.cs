namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the request body for the CompleteMultipartUpload S3 operation.
/// </summary>
public sealed class S3CompleteMultipartUploadRequest
{
    /// <summary>The list of parts that compose the completed multipart upload.</summary>
    public IReadOnlyList<S3CompleteMultipartUploadPart> Parts { get; init; } = [];
}

/// <summary>
/// Identifies a single part within a <see cref="S3CompleteMultipartUploadRequest"/>.
/// </summary>
public sealed class S3CompleteMultipartUploadPart
{
    /// <summary>The part number identifying this part in the multipart upload.</summary>
    public required int PartNumber { get; init; }

    /// <summary>The ETag returned when the part was uploaded.</summary>
    public required string ETag { get; init; }

    /// <summary>Optional checksums for the part, keyed by algorithm name.</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }
}
