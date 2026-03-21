namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of an S3 ListMultipartUploads operation.
/// </summary>
public sealed class S3ListMultipartUploadsResult
{
    /// <summary>The name of the bucket.</summary>
    public required string Bucket { get; init; }

    /// <summary>The key prefix used to filter uploads.</summary>
    public string? Prefix { get; init; }

    /// <summary>The delimiter used to group common prefixes.</summary>
    public string? Delimiter { get; init; }

    /// <summary>The encoding type used for object keys in the response.</summary>
    public string? EncodingType { get; init; }

    /// <summary>The key marker indicating where listing began.</summary>
    public string? KeyMarker { get; init; }

    /// <summary>The upload ID marker indicating where listing began.</summary>
    public string? UploadIdMarker { get; init; }

    /// <summary>The key marker to use for the next page of results.</summary>
    public string? NextKeyMarker { get; init; }

    /// <summary>The upload ID marker to use for the next page of results.</summary>
    public string? NextUploadIdMarker { get; init; }

    /// <summary>The maximum number of uploads requested.</summary>
    public int MaxUploads { get; init; }

    /// <summary>Indicates whether the result list was truncated.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The list of in-progress multipart uploads.</summary>
    public IReadOnlyList<S3MultipartUploadEntry> Uploads { get; init; } = [];

    /// <summary>The list of common prefixes when a delimiter is specified.</summary>
    public IReadOnlyList<S3ListBucketCommonPrefix> CommonPrefixes { get; init; } = [];
}

/// <summary>
/// Represents a single in-progress multipart upload entry.
/// </summary>
public sealed class S3MultipartUploadEntry
{
    /// <summary>The object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The upload ID that uniquely identifies this multipart upload.</summary>
    public required string UploadId { get; init; }

    /// <summary>The user who initiated the multipart upload.</summary>
    public S3BucketOwner? Initiator { get; init; }

    /// <summary>The owner of the object being uploaded.</summary>
    public S3BucketOwner? Owner { get; init; }

    /// <summary>The date and time when the multipart upload was initiated, in UTC.</summary>
    public DateTimeOffset InitiatedAtUtc { get; init; }

    /// <summary>The storage class used for the uploaded object.</summary>
    public string StorageClass { get; init; } = "STANDARD";

    /// <summary>The checksum algorithm used for this multipart upload.</summary>
    public string? ChecksumAlgorithm { get; init; }
}
