namespace IntegratedS3.Abstractions.Requests;

public sealed class InitiateMultipartUploadRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public string? ContentType { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }
}
