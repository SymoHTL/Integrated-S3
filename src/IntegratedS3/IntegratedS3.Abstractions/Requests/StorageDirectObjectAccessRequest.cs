using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class StorageDirectObjectAccessRequest
{
    public required StorageDirectObjectAccessOperation Operation { get; init; }

    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required int ExpiresInSeconds { get; init; }

    public string? VersionId { get; init; }

    public string? ContentType { get; init; }

    public string? ChecksumAlgorithm { get; init; }

    public IReadOnlyDictionary<string, string>? Checksums { get; init; }
}
