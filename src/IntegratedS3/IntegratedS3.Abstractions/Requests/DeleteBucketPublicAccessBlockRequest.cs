namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeletePublicAccessBlock operation.</summary>
public sealed class DeleteBucketPublicAccessBlockRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }
}
