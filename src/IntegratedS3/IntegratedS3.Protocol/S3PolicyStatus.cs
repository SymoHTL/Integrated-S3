namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the policy status (GetBucketPolicyStatus) for an S3 bucket.
/// </summary>
public sealed class S3PolicyStatus
{
    /// <summary>Whether the bucket is public according to its policy and public-access state.</summary>
    public bool IsPublic { get; init; }
}
