namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketReplication operation.</summary>
public sealed class DeleteBucketReplicationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }
}
