namespace IntegratedS3.Abstractions.Models;

public sealed class ObjectInfo
{
    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public string? VersionId { get; init; }

    public bool IsLatest { get; init; }

    public bool IsDeleteMarker { get; init; }

    public long ContentLength { get; init; }

    public string? ContentType { get; init; }

    public string? ETag { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    public ObjectServerSideEncryptionInfo? ServerSideEncryption { get; init; }
}
