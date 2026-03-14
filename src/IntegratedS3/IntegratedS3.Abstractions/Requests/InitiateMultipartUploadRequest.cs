using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class InitiateMultipartUploadRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public string? ContentType { get; init; }

    public string? CacheControl { get; init; }

    public string? ContentDisposition { get; init; }

    public string? ContentEncoding { get; init; }

    public string? ContentLanguage { get; init; }

    public DateTimeOffset? ExpiresUtc { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    public string? ChecksumAlgorithm { get; init; }

    public ObjectServerSideEncryptionSettings? ServerSideEncryption { get; init; }
}
