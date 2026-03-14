using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class PutBucketDefaultEncryptionRequest
{
    public required string BucketName { get; init; }

    public required BucketDefaultEncryptionRule Rule { get; init; }
}
