namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteBucketEncryption operation.</summary>
public sealed class DeleteBucketDefaultEncryptionRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }
}
