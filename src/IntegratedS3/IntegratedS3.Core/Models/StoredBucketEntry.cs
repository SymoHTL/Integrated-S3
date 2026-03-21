namespace IntegratedS3.Core.Models;

/// <summary>
/// Represents the stored metadata of a bucket in the catalog.
/// </summary>
public sealed class StoredBucketEntry
{
    /// <summary>The name of the storage provider that hosts this bucket.</summary>
    public string ProviderName { get; init; } = string.Empty;

    /// <summary>The bucket name.</summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>The UTC date/time the bucket was created.</summary>
    public DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>Indicates whether object versioning is enabled on this bucket.</summary>
    public bool VersioningEnabled { get; init; }

    /// <summary>The UTC date/time when this catalog entry was last synchronized from the provider.</summary>
    public DateTimeOffset LastSyncedAtUtc { get; init; }
}
