namespace IntegratedS3.Protocol;

public sealed class S3ListPartsResult
{
    public required string Bucket { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public int PartNumberMarker { get; init; }

    public int? NextPartNumberMarker { get; init; }

    public int MaxParts { get; init; }

    public bool IsTruncated { get; init; }

    public string StorageClass { get; init; } = "STANDARD";

    public string? ChecksumAlgorithm { get; init; }

    public string? ChecksumType { get; init; }

    public IReadOnlyList<S3ListPartEntry> Parts { get; init; } = [];
}

public sealed class S3ListPartEntry
{
    public required int PartNumber { get; init; }

    public required string ETag { get; init; }

    public long Size { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public string? ChecksumCrc32 { get; init; }

    public string? ChecksumCrc32c { get; init; }

    public string? ChecksumCrc64Nvme { get; init; }

    public string? ChecksumSha1 { get; init; }

    public string? ChecksumSha256 { get; init; }
}
