namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of the S3 ListBuckets operation, containing the owner and all buckets.
/// </summary>
public sealed class S3ListAllMyBucketsResult
{
    /// <summary>The owner of the listed buckets.</summary>
    public S3BucketOwner Owner { get; init; } = new();

    /// <summary>The list of buckets owned by the requester.</summary>
    public IReadOnlyList<S3BucketListEntry> Buckets { get; init; } = [];
}

/// <summary>
/// Represents the owner of an S3 bucket or object.
/// </summary>
public sealed class S3BucketOwner
{
    /// <summary>The canonical user ID of the owner.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>The display name of the owner.</summary>
    public string DisplayName { get; init; } = string.Empty;
}

/// <summary>
/// Represents a single bucket entry in a ListBuckets result.
/// </summary>
public sealed class S3BucketListEntry
{
    /// <summary>The name of the bucket.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>The date and time the bucket was created, in UTC.</summary>
    public DateTimeOffset CreationDateUtc { get; init; }
}