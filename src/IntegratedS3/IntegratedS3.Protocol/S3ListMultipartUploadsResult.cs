namespace IntegratedS3.Protocol;

public sealed class S3ListMultipartUploadsResult
{
    public required string Bucket { get; init; }

    public string? Prefix { get; init; }

    public string? Delimiter { get; init; }

    public string? EncodingType { get; init; }

    public string? KeyMarker { get; init; }

    public string? UploadIdMarker { get; init; }

    public string? NextKeyMarker { get; init; }

    public string? NextUploadIdMarker { get; init; }

    public int MaxUploads { get; init; }

    public bool IsTruncated { get; init; }

    public IReadOnlyList<S3MultipartUploadEntry> Uploads { get; init; } = [];

    public IReadOnlyList<S3ListBucketCommonPrefix> CommonPrefixes { get; init; } = [];
}

public sealed class S3MultipartUploadEntry
{
    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public S3BucketOwner? Initiator { get; init; }

    public S3BucketOwner? Owner { get; init; }

    public DateTimeOffset InitiatedAtUtc { get; init; }

    public string StorageClass { get; init; } = "STANDARD";

    public string? ChecksumAlgorithm { get; init; }
}
