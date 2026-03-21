namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutObjectTagging operation.</summary>
public sealed class PutObjectTagsRequest
{
    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The version identifier of the object.</summary>
    public string? VersionId { get; init; }

    /// <summary>The tag key-value pairs to set on the object.</summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } = new Dictionary<string, string>(StringComparer.Ordinal);
}
