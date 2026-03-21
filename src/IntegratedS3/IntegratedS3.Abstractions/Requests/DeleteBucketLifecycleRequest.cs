namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketLifecycle operation.</summary>
public sealed class DeleteBucketLifecycleRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }
}
