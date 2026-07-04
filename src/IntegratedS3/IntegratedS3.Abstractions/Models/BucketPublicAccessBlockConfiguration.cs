namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Public access block configuration for a bucket.
/// Mirrors the S3 <c>PublicAccessBlockConfiguration</c> element.
/// </summary>
public sealed class BucketPublicAccessBlockConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>Whether Amazon S3 should block public ACLs for this bucket.</summary>
    public bool BlockPublicAcls { get; init; }

    /// <summary>Whether Amazon S3 should ignore public ACLs for this bucket.</summary>
    public bool IgnorePublicAcls { get; init; }

    /// <summary>Whether Amazon S3 should block public bucket policies for this bucket.</summary>
    public bool BlockPublicPolicy { get; init; }

    /// <summary>Whether Amazon S3 should restrict public bucket policies for this bucket.</summary>
    public bool RestrictPublicBuckets { get; init; }
}
