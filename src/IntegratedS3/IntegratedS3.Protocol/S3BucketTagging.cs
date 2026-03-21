namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the tag set applied to an S3 bucket.
/// </summary>
public sealed class S3BucketTagging
{
    /// <summary>The collection of <see cref="S3Tag"/> entries assigned to the bucket.</summary>
    public IReadOnlyList<S3Tag> TagSet { get; init; } = [];
}

/// <summary>
/// Represents a key-value tag associated with an S3 resource.
/// </summary>
public sealed class S3Tag
{
    /// <summary>The tag key.</summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>The tag value.</summary>
    public string Value { get; init; } = string.Empty;
}
