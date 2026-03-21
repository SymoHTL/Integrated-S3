namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketAnalyticsConfiguration operation.</summary>
public sealed class DeleteBucketAnalyticsConfigurationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The identifier of the analytics configuration to delete.</summary>
    public required string Id { get; init; }
}
