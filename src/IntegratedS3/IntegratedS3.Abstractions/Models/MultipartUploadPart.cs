namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Metadata for a single uploaded part of a multipart upload.
/// </summary>
public sealed class MultipartUploadPart
{
    /// <summary>
    /// The part number (1-based) within the multipart upload.
    /// </summary>
    public required int PartNumber { get; init; }

    /// <summary>
    /// The entity tag (ETag) returned when the part was uploaded.
    /// </summary>
    public required string ETag { get; init; }

    /// <summary>
    /// The size of the part in bytes.
    /// </summary>
    public long ContentLength { get; init; }

    /// <summary>
    /// The date and time the part was last modified, in UTC.
    /// </summary>
    public DateTimeOffset LastModifiedUtc { get; init; }

    /// <summary>
    /// Checksum algorithm-to-value map for the part's content integrity verification.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }
}
