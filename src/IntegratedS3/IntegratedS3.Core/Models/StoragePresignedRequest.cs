namespace IntegratedS3.Core.Models;

public sealed class StoragePresignedRequest
{
    public required StoragePresignOperation Operation { get; init; }

    public required StorageAccessMode AccessMode { get; init; }

    public required string Method { get; init; }

    public required Uri Url { get; init; }

    public required DateTimeOffset ExpiresAtUtc { get; init; }

    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public string? VersionId { get; init; }

    public string? ContentType { get; init; }

    public IReadOnlyList<StoragePresignedHeader> Headers { get; init; } = [];
}
