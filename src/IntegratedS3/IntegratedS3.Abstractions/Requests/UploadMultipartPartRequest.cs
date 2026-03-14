using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class UploadMultipartPartRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public required int PartNumber { get; init; }

    public Stream? Content { get; init; }

    public long? ContentLength { get; init; }

    public string? ChecksumAlgorithm { get; init; }

    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    public string? CopySourceBucketName { get; init; }

    public string? CopySourceKey { get; init; }

    public string? CopySourceVersionId { get; init; }

    public string? CopySourceIfMatchETag { get; init; }

    public string? CopySourceIfNoneMatchETag { get; init; }

    public DateTimeOffset? CopySourceIfModifiedSinceUtc { get; init; }

    public DateTimeOffset? CopySourceIfUnmodifiedSinceUtc { get; init; }

    public ObjectRange? CopySourceRange { get; init; }
}
