namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the retention settings for an individual S3 object.
/// </summary>
public sealed class S3ObjectRetention
{
    /// <summary>The retention mode (<c>GOVERNANCE</c> or <c>COMPLIANCE</c>).</summary>
    public string? Mode { get; init; }

    /// <summary>The date and time until which the object is retained, in UTC.</summary>
    public DateTimeOffset? RetainUntilDateUtc { get; init; }
}
