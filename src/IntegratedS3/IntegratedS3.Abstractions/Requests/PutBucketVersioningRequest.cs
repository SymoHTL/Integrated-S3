using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketVersioning operation.</summary>
public sealed class PutBucketVersioningRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The versioning status to set on the bucket.</summary>
    public required BucketVersioningStatus Status { get; init; }
}