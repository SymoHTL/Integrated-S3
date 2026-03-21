namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of an S3 ListObjects (v1) or ListObjectsV2 operation.
/// </summary>
public sealed class S3ListBucketResult
{
    /// <summary>The name of the bucket.</summary>
    public required string Name { get; init; }

    /// <summary>Whether this result uses the ListObjectsV2 format.</summary>
    public bool IsV2 { get; init; } = true;

    /// <summary>The key prefix used to filter results.</summary>
    public string? Prefix { get; init; }

    /// <summary>The delimiter used to group common prefixes.</summary>
    public string? Delimiter { get; init; }

    /// <summary>The marker indicating where listing began (ListObjects v1).</summary>
    public string? Marker { get; init; }

    /// <summary>The key to start after when listing (ListObjectsV2).</summary>
    public string? StartAfter { get; init; }

    /// <summary>The continuation token used in the request (ListObjectsV2).</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>The marker to use for the next page of results (ListObjects v1).</summary>
    public string? NextMarker { get; init; }

    /// <summary>The token to use to retrieve the next page of results (ListObjectsV2).</summary>
    public string? NextContinuationToken { get; init; }

    /// <summary>The encoding type used for object keys in the response.</summary>
    public string? EncodingType { get; init; }

    /// <summary>The number of keys returned in this response.</summary>
    public int KeyCount { get; init; }

    /// <summary>The maximum number of keys requested.</summary>
    public int MaxKeys { get; init; }

    /// <summary>Indicates whether the result list was truncated.</summary>
    public bool IsTruncated { get; init; }

    /// <summary>The list of objects matching the request.</summary>
    public IReadOnlyList<S3ListBucketObject> Contents { get; init; } = [];

    /// <summary>The list of common prefixes when a delimiter is specified.</summary>
    public IReadOnlyList<S3ListBucketCommonPrefix> CommonPrefixes { get; init; } = [];
}

/// <summary>
/// Represents a common prefix entry returned when a delimiter is used in a ListObjects request.
/// </summary>
public sealed class S3ListBucketCommonPrefix
{
    /// <summary>The common prefix string.</summary>
    public required string Prefix { get; init; }
}

/// <summary>
/// Represents a single object entry in a ListObjects result.
/// </summary>
public sealed class S3ListBucketObject
{
    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The entity tag (ETag) of the object.</summary>
    public string? ETag { get; init; }

    /// <summary>The size of the object in bytes.</summary>
    public long Size { get; init; }

    /// <summary>The date and time the object was last modified, in UTC.</summary>
    public DateTimeOffset LastModifiedUtc { get; init; }

    /// <summary>The storage class of the object.</summary>
    public string StorageClass { get; init; } = "STANDARD";

    /// <summary>The owner of the object, if requested.</summary>
    public S3BucketOwner? Owner { get; init; }
}
