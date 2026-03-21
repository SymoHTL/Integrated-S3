namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Metadata about an in-progress multipart upload.
/// </summary>
public sealed class MultipartUploadInfo
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
    /// The checksum algorithm used for the upload, or <see langword="null"/> if none was specified.
    /// </summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>
    /// Server-side encryption information for the upload.
    /// </summary>
    public ObjectServerSideEncryptionInfo? ServerSideEncryption { get; init; }

    /// <summary>
    /// Customer-provided encryption information for the upload.
    /// </summary>
    public ObjectCustomerEncryptionInfo? CustomerEncryption { get; init; }
}
