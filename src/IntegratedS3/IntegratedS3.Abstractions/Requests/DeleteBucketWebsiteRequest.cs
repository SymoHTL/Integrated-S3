namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketWebsite operation.</summary>
public sealed class DeleteBucketWebsiteRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }
}
