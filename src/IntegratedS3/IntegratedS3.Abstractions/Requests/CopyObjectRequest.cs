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

    public CopyObjectMetadataDirective MetadataDirective { get; init; } = CopyObjectMetadataDirective.Copy;

    public string? ContentType { get; init; }

    public string? CacheControl { get; init; }

    public string? ContentDisposition { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentLanguage { get; init; }

    public DateTimeOffset? ExpiresUtc { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public ObjectServerSideEncryptionSettings? SourceServerSideEncryption { get; init; }

    public ObjectServerSideEncryptionSettings? DestinationServerSideEncryption { get; init; }

    public bool OverwriteIfExists { get; init; } = true;
}
