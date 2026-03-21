using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketReplication operation.</summary>
public sealed class PutBucketReplicationRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The IAM role ARN used for replication.</summary>
    public string? Role { get; init; }

    /// <summary>The replication rules to apply to the bucket.</summary>
    public IReadOnlyList<BucketReplicationRule> Rules { get; init; } = [];
}
