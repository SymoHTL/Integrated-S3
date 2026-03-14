namespace IntegratedS3.Abstractions.Requests;

public sealed class DeleteBucketDefaultEncryptionRequest
{
    public required string BucketName { get; init; }
}
