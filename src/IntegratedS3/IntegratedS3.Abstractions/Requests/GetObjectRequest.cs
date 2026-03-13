using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class GetObjectRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public string? VersionId { get; init; }

    public ObjectRange? Range { get; init; }

    public ObjectServerSideEncryptionSettings? ServerSideEncryption { get; init; }

    public string? IfMatchETag { get; init; }

    public string? IfNoneMatchETag { get; init; }

    public DateTimeOffset? IfModifiedSinceUtc { get; init; }

    public DateTimeOffset? IfUnmodifiedSinceUtc { get; init; }
}
