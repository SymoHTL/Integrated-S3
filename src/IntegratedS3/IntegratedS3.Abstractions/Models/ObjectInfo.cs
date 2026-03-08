namespace IntegratedS3.Abstractions.Models;

public sealed class ObjectInfo
{
    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public string? VersionId { get; init; }

    public long ContentLength { get; init; }

    public string? ContentType { get; init; }

    public string? ETag { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }
}
