namespace IntegratedS3.Abstractions.Responses;

public sealed class GetObjectAttributesResponse
{
    public string? VersionId { get; init; }

    public bool IsDeleteMarker { get; init; }

    public DateTimeOffset? LastModifiedUtc { get; init; }

    public string? ETag { get; init; }

    public long? ObjectSize { get; init; }

    public string? StorageClass { get; init; }

    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    public ObjectPartsInfo? ObjectParts { get; init; }
}

public sealed class ObjectPartsInfo
{
    public int TotalPartsCount { get; init; }

    public int? PartNumberMarker { get; init; }

    public int? NextPartNumberMarker { get; init; }

    public int? MaxParts { get; init; }

    public bool IsTruncated { get; init; }

    public IReadOnlyList<ObjectPartInfo>? Parts { get; init; }
}

public sealed class ObjectPartInfo
{
    public int PartNumber { get; init; }

    public long Size { get; init; }

    public string? ChecksumCrc32 { get; init; }

    public string? ChecksumCrc32C { get; init; }

    public string? ChecksumSha1 { get; init; }

    public string? ChecksumSha256 { get; init; }

    public string? ChecksumCrc64Nvme { get; init; }
}
