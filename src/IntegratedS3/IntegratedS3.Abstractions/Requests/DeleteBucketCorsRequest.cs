namespace IntegratedS3.Abstractions.Requests;

public sealed class DeleteBucketCorsRequest
{
    public required string BucketName { get; init; }
}
