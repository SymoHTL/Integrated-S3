using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketEncryption operation.</summary>
public sealed class PutBucketDefaultEncryptionRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The default encryption rule to apply to the bucket.</summary>
    public required BucketDefaultEncryptionRule Rule { get; init; }
}
