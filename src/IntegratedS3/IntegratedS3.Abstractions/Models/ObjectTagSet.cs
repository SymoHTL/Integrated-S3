namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Tag set associated with a specific object version.
/// </summary>
public sealed class ObjectTagSet
{
    /// <summary>
    /// The name of the bucket that contains the object.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The object key within the bucket.
    /// </summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>
    /// The version identifier, or <see langword="null"/> for the current version.
    /// </summary>
    public string? VersionId { get; init; }

    /// <summary>
    /// The key-value tags assigned to this object version.
    /// </summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } = new Dictionary<string, string>(StringComparer.Ordinal);
}
