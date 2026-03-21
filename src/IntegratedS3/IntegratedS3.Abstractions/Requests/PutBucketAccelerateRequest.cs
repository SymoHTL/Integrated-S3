using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketAccelerateConfiguration operation.</summary>
public sealed class PutBucketAccelerateRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The transfer acceleration status to set on the bucket.</summary>
    public BucketAccelerateStatus Status { get; init; } = BucketAccelerateStatus.Suspended;
}
