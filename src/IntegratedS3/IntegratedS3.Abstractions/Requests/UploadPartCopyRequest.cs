using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class UploadPartCopyRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public required int PartNumber { get; init; }

    public required string SourceBucketName { get; init; }

    public required string SourceKey { get; init; }

    public string? SourceVersionId { get; init; }

    public string? SourceIfMatchETag { get; init; }

    public string? SourceIfNoneMatchETag { get; init; }

    public DateTimeOffset? SourceIfModifiedSinceUtc { get; init; }

    public DateTimeOffset? SourceIfUnmodifiedSinceUtc { get; init; }

    public ObjectRange? SourceRange { get; init; }

    public string? ChecksumAlgorithm { get; init; }

    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    public ObjectCustomerEncryptionSettings? SourceCustomerEncryption { get; init; }

    public ObjectCustomerEncryptionSettings? DestinationCustomerEncryption { get; init; }
}
