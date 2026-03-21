namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Metadata about a storage bucket.
/// </summary>
public sealed class BucketInfo
{
    /// <summary>
    /// The bucket name.
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// When the bucket was created, in UTC.
    /// </summary>
    public DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>
    /// Whether object versioning is currently enabled for this bucket.
    /// </summary>
    public bool VersioningEnabled { get; init; }
}
