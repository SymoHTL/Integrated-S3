namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the versioning configuration for an S3 bucket.
/// </summary>
public sealed class S3BucketVersioningConfiguration
{
    /// <summary>The versioning status (<c>Enabled</c> or <c>Suspended</c>).</summary>
    public string? Status { get; init; }
}