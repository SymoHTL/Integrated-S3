namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketCors operation.</summary>
public sealed class DeleteBucketCorsRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }
}
