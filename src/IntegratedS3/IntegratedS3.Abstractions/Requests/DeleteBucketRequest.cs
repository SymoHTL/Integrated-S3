namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucket operation.</summary>
public sealed class DeleteBucketRequest
{
    /// <summary>The name of the bucket to delete.</summary>
    public required string BucketName { get; init; }
}
