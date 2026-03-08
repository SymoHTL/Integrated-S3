namespace IntegratedS3.Abstractions.Requests;

public sealed class DeleteBucketRequest
{
    public required string BucketName { get; init; }
}
