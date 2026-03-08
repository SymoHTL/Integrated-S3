namespace IntegratedS3.Core.Models;

public sealed class StoredObjectEntry
{
    public string ProviderName { get; init; } = string.Empty;

    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public long ContentLength { get; init; }

    public string? ContentType { get; init; }

    public string? ETag { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public DateTimeOffset LastSyncedAtUtc { get; init; }
}
