namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the legal hold status of an S3 object.
/// </summary>
public sealed class S3ObjectLegalHold
{
    /// <summary>The legal hold status (<c>ON</c> or <c>OFF</c>).</summary>
    public string? Status { get; init; }
}
