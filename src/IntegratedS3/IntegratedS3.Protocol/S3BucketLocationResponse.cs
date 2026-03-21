namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the response to a GetBucketLocation request, indicating the region where the bucket resides.
/// </summary>
public sealed class S3BucketLocationResponse
{
    /// <summary>The region constraint of the bucket (e.g., <c>us-west-2</c>), or <c>null</c> for <c>us-east-1</c>.</summary>
    public string? LocationConstraint { get; init; }
}
