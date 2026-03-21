using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketLifecycleConfiguration operation.</summary>
public sealed class PutBucketLifecycleRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The lifecycle rules to apply to the bucket.</summary>
    public IReadOnlyList<BucketLifecycleRule> Rules { get; init; } = [];
}
