namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutPublicAccessBlock operation.</summary>
public sealed class PutBucketPublicAccessBlockRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>Whether Amazon S3 should block public ACLs for this bucket.</summary>
    public bool BlockPublicAcls { get; init; }

    /// <summary>Whether Amazon S3 should ignore public ACLs for this bucket.</summary>
    public bool IgnorePublicAcls { get; init; }

    /// <summary>Whether Amazon S3 should block public bucket policies for this bucket.</summary>
    public bool BlockPublicPolicy { get; init; }

    /// <summary>Whether Amazon S3 should restrict public bucket policies for this bucket.</summary>
    public bool RestrictPublicBuckets { get; init; }
}
