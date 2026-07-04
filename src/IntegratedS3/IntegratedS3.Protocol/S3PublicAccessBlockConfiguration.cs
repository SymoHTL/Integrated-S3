namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the public access block configuration for an S3 bucket.
/// </summary>
public sealed class S3PublicAccessBlockConfiguration
{
    /// <summary>Whether Amazon S3 should block public ACLs for this bucket.</summary>
    public bool BlockPublicAcls { get; init; }

    /// <summary>Whether Amazon S3 should ignore public ACLs for this bucket.</summary>
    public bool IgnorePublicAcls { get; init; }

    /// <summary>Whether Amazon S3 should block public bucket policies for this bucket.</summary>
    public bool BlockPublicPolicy { get; init; }

    /// <summary>Whether Amazon S3 should restrict public bucket policies for this bucket.</summary>
    public bool RestrictPublicBuckets { get; init; }
}
