namespace IntegratedS3.Abstractions.Responses;

/// <summary>Contains selected attributes of an object without returning the full body.</summary>
public sealed class GetObjectAttributesResponse
{
    /// <summary>The version identifier of the object, if versioning is enabled.</summary>
    public string? VersionId { get; init; }

    /// <summary><see langword="true"/> if the object version is a delete marker.</summary>
    public bool IsDeleteMarker { get; init; }

    /// <summary>The date and time the object was last modified, in UTC.</summary>
    public DateTimeOffset? LastModifiedUtc { get; init; }

    /// <summary>The entity tag of the object.</summary>
    public string? ETag { get; init; }

    /// <summary>The size of the object in bytes.</summary>
    public long? ObjectSize { get; init; }

    /// <summary>The storage class of the object.</summary>
    public string? StorageClass { get; init; }

    /// <summary>The checksum values associated with the object.</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>Information about the parts of a multipart-uploaded object.</summary>
    public ObjectPartsInfo? ObjectParts { get; init; }
}

/// <summary>Information about the parts of a multipart-uploaded object.</summary>
public sealed class ObjectPartsInfo
{
    /// <summary>The total number of parts in the multipart upload.</summary>
    public int TotalPartsCount { get; init; }

    /// <summary>The part number used as a starting point for the listing.</summary>
    public int? PartNumberMarker { get; init; }

    /// <summary>The part number to use as the marker for the next listing request.</summary>
    public int? NextPartNumberMarker { get; init; }

    /// <summary>The maximum number of parts returned per page.</summary>
    public int? MaxParts { get; init; }

    /// <summary><see langword="true"/> if additional parts remain beyond this page.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The list of part details.</summary>
    public IReadOnlyList<ObjectPartInfo>? Parts { get; init; }
}

/// <summary>Checksum details for an individual part of a multipart-uploaded object.</summary>
public sealed class ObjectPartInfo
{
    /// <summary>The part number identifying this part within the upload.</summary>
    public int PartNumber { get; init; }

    /// <summary>The size of the part in bytes.</summary>
    public long Size { get; init; }

    /// <summary>The CRC-32 checksum of the part, if available.</summary>
    public string? ChecksumCrc32 { get; init; }

    /// <summary>The CRC-32C checksum of the part, if available.</summary>
    public string? ChecksumCrc32C { get; init; }

    /// <summary>The SHA-1 checksum of the part, if available.</summary>
    public string? ChecksumSha1 { get; init; }

    /// <summary>The SHA-256 checksum of the part, if available.</summary>
    public string? ChecksumSha256 { get; init; }

    /// <summary>The CRC-64/NVME checksum of the part, if available.</summary>
    public string? ChecksumCrc64Nvme { get; init; }
}
