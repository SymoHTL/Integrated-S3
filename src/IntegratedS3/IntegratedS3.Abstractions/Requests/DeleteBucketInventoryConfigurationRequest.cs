namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketInventoryConfiguration operation.</summary>
public sealed class DeleteBucketInventoryConfigurationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The identifier of the inventory configuration to delete.</summary>
    public required string Id { get; init; }
}
