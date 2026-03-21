namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the tag set applied to an S3 object.
/// </summary>
public sealed class S3ObjectTagging
{
    /// <summary>The collection of <see cref="S3ObjectTag"/> entries assigned to the object.</summary>
    public IReadOnlyList<S3ObjectTag> TagSet { get; init; } = [];
}

/// <summary>
/// Represents a key-value tag associated with an S3 object.
/// </summary>
public sealed class S3ObjectTag
{
    /// <summary>The tag key.</summary>
    public required string Key { get; init; }

    /// <summary>The tag value.</summary>
    public required string Value { get; init; }
}
