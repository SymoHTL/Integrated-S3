namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of an S3 ListObjectVersions operation.
/// </summary>
public sealed class S3ListObjectVersionsResult
{
    /// <summary>The name of the bucket.</summary>
    public required string Name { get; init; }

    /// <summary>The key prefix used to filter results.</summary>
    public string? Prefix { get; init; }

    /// <summary>The delimiter used to group common prefixes.</summary>
    public string? Delimiter { get; init; }

    /// <summary>The key marker indicating where listing began.</summary>
    public string? KeyMarker { get; init; }

    /// <summary>The version ID marker indicating where listing began.</summary>
    public string? VersionIdMarker { get; init; }

    /// <summary>The key marker to use for the next page of results.</summary>
    public string? NextKeyMarker { get; init; }

    /// <summary>The version ID marker to use for the next page of results.</summary>
    public string? NextVersionIdMarker { get; init; }

    /// <summary>The maximum number of keys requested.</summary>
    public int MaxKeys { get; init; }

    /// <summary>Indicates whether the result list was truncated.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The list of object versions and delete markers.</summary>
    public IReadOnlyList<S3ObjectVersionEntry> Versions { get; init; } = [];

    /// <summary>The encoding type used for object keys in the response.</summary>
    public string? EncodingType { get; init; }

    /// <summary>The list of common prefixes when a delimiter is specified.</summary>
    public IReadOnlyList<S3ListBucketCommonPrefix> CommonPrefixes { get; init; } = [];
}

/// <summary>
/// Represents a single object version or delete marker in a ListObjectVersions result.
/// </summary>
public sealed class S3ObjectVersionEntry
{
    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The version ID of the object.</summary>
    public required string VersionId { get; init; }

    /// <summary>Whether this version is the latest (current) version.</summary>
    public bool IsLatest { get; init; }

    /// <summary>Whether this entry is a delete marker rather than an object version.</summary>
    public bool IsDeleteMarker { get; init; }

    /// <summary>The entity tag (ETag) of the object version.</summary>
    public string? ETag { get; init; }

    /// <summary>The size of the object version in bytes.</summary>
    public long Size { get; init; }

    /// <summary>The date and time the version was last modified, in UTC.</summary>
    public DateTimeOffset LastModifiedUtc { get; init; }

    /// <summary>The storage class of the object version.</summary>
    public string StorageClass { get; init; } = "STANDARD";
}