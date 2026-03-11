namespace IntegratedS3.Protocol;

public sealed class S3SigV4PresignParameters
{
    public required string HttpMethod { get; init; }

    public required string Path { get; init; }

    public IReadOnlyList<KeyValuePair<string, string?>> QueryParameters { get; init; } = [];

    public IReadOnlyList<KeyValuePair<string, string?>> Headers { get; init; } = [];

    public IReadOnlyList<string> SignedHeaders { get; init; } = [];

    public required string AccessKeyId { get; init; }

    public required string SecretAccessKey { get; init; }

    public required string Region { get; init; }

    public required string Service { get; init; }

    public required DateTimeOffset SignedAtUtc { get; init; }

    public required int ExpiresInSeconds { get; init; }

    public string PayloadHash { get; init; } = "UNSIGNED-PAYLOAD";

    public string? SecurityToken { get; init; }
}
