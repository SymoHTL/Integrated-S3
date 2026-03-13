using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class CopyObjectRequest
{
    public required string SourceBucketName { get; init; }

    public required string SourceKey { get; init; }

    public required string DestinationBucketName { get; init; }

    public required string DestinationKey { get; init; }

    public string? SourceVersionId { get; init; }

    public string? SourceIfMatchETag { get; init; }

    public string? SourceIfNoneMatchETag { get; init; }

    public DateTimeOffset? SourceIfModifiedSinceUtc { get; init; }

    public DateTimeOffset? SourceIfUnmodifiedSinceUtc { get; init; }

    public ObjectServerSideEncryptionSettings? SourceServerSideEncryption { get; init; }

    public ObjectServerSideEncryptionSettings? DestinationServerSideEncryption { get; init; }

    public bool OverwriteIfExists { get; init; } = true;
}
