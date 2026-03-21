namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketTagging operation.</summary>
public sealed class PutBucketTaggingRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The tag key-value pairs to set on the bucket.</summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } = new Dictionary<string, string>();
}
