namespace IntegratedS3.Core.Models;

public sealed class StorageAuthorizationRequest
{
    public required StorageOperationType Operation { get; init; }

    public string? BucketName { get; init; }

    public string? Key { get; init; }

    public string? SourceBucketName { get; init; }

    public string? SourceKey { get; init; }

    public string? VersionId { get; init; }

    public bool IncludesMetadata { get; init; }
}