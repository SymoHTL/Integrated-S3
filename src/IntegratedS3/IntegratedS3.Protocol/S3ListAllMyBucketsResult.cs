namespace IntegratedS3.Protocol;

public sealed class S3ListAllMyBucketsResult
{
    public S3BucketOwner Owner { get; init; } = new();

    public IReadOnlyList<S3BucketListEntry> Buckets { get; init; } = [];
}

public sealed class S3BucketOwner
{
    public string Id { get; init; } = string.Empty;

    public string DisplayName { get; init; } = string.Empty;
}

public sealed class S3BucketListEntry
{
    public string Name { get; init; } = string.Empty;

    public DateTimeOffset CreationDateUtc { get; init; }
}