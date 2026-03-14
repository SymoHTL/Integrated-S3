namespace IntegratedS3.Protocol;

public sealed class S3ListBucketResult
{
    public required string Name { get; init; }

    public bool IsV2 { get; init; } = true;

    public string? Prefix { get; init; }

    public string? Delimiter { get; init; }

    public string? Marker { get; init; }

    public string? StartAfter { get; init; }

    public string? ContinuationToken { get; init; }

    public string? NextMarker { get; init; }

    public string? NextContinuationToken { get; init; }

    public string? EncodingType { get; init; }

    public int KeyCount { get; init; }

    public int MaxKeys { get; init; }

    public bool IsTruncated { get; init; }

    public IReadOnlyList<S3ListBucketObject> Contents { get; init; } = [];

    public IReadOnlyList<S3ListBucketCommonPrefix> CommonPrefixes { get; init; } = [];
}

public sealed class S3ListBucketCommonPrefix
{
    public required string Prefix { get; init; }
}

public sealed class S3ListBucketObject
{
    public required string Key { get; init; }

    public string? ETag { get; init; }

    public long Size { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public string StorageClass { get; init; } = "STANDARD";

    public S3BucketOwner? Owner { get; init; }
}
