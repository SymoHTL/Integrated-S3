namespace IntegratedS3.Core.Models;

public sealed class StoredBucketEntry
{
    public string ProviderName { get; init; } = string.Empty;

    public string BucketName { get; init; } = string.Empty;

    public DateTimeOffset CreatedAtUtc { get; init; }

    public bool VersioningEnabled { get; init; }

    public DateTimeOffset LastSyncedAtUtc { get; init; }
}
