namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Full state of an in-progress multipart upload, including content headers and metadata specified at initiation.
/// Used by state stores to persist upload context between part uploads and completion.
/// </summary>
public class MultipartUploadState
{
    /// <summary>
    /// The name of the bucket that contains the upload.
    /// </summary>
    public required string BucketName { get; init; }

    /// <summary>
    /// The object key for the multipart upload.
    /// </summary>
    public required string Key { get; init; }

    /// <summary>
    /// The unique identifier for this multipart upload.
    /// </summary>
    public required string UploadId { get; init; }

    /// <summary>
    /// The date and time when the multipart upload was initiated, in UTC.
    /// </summary>
    public DateTimeOffset InitiatedAtUtc { get; init; }

    /// <summary>
    /// The MIME type specified at upload initiation.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// The <c>Cache-Control</c> header specified at upload initiation.
    /// </summary>
    public string? CacheControl { get; init; }

    /// <summary>
    /// The <c>Content-Disposition</c> header specified at upload initiation.
    /// </summary>
    public string? ContentDisposition { get; init; }

    /// <summary>
    /// The <c>Content-Encoding</c> header specified at upload initiation.
    /// </summary>
    public string? ContentEncoding { get; init; }

    /// <summary>
    /// The <c>Content-Language</c> header specified at upload initiation.
    /// </summary>
    public string? ContentLanguage { get; init; }

    /// <summary>
    /// The expiration date specified at upload initiation, in UTC.
    /// </summary>
    public DateTimeOffset? ExpiresUtc { get; init; }

    /// <summary>
    /// User-defined key-value metadata specified at upload initiation.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>
    /// Tags specified at upload initiation.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    /// <summary>
    /// The checksum algorithm specified at upload initiation, or <see langword="null"/> if none was specified.
    /// </summary>
    public string? ChecksumAlgorithm { get; init; }
}
