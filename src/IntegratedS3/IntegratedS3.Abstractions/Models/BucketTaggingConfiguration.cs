namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Tag set configuration for a bucket.
/// </summary>
public sealed class BucketTaggingConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The key-value tags assigned to this bucket.
    /// </summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } = new Dictionary<string, string>();
}
