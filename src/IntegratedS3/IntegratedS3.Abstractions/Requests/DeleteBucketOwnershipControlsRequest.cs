namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketOwnershipControls operation.</summary>
public sealed class DeleteBucketOwnershipControlsRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }
}
