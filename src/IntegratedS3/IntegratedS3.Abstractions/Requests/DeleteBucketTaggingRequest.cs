namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketTagging operation.</summary>
public sealed class DeleteBucketTaggingRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }
}
