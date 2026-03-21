namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the S3 Transfer Acceleration configuration for a bucket.
/// </summary>
public sealed class S3AccelerateConfiguration
{
    /// <summary>The transfer acceleration status (<c>Enabled</c> or <c>Suspended</c>).</summary>
    public string? Status { get; init; }
}
