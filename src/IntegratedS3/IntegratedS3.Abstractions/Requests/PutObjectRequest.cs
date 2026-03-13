using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class PutObjectRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required Stream Content { get; init; }

    public long? ContentLength { get; init; }

    public string? ContentType { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    public ObjectServerSideEncryptionSettings? ServerSideEncryption { get; init; }

    public bool OverwriteIfExists { get; init; } = true;
}
