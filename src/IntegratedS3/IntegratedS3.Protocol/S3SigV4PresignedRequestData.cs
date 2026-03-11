namespace IntegratedS3.Protocol;

public sealed class S3SigV4PresignedRequestData
{
    public required S3SigV4CredentialScope CredentialScope { get; init; }

    public required DateTimeOffset SignedAtUtc { get; init; }

    public required DateTimeOffset ExpiresAtUtc { get; init; }

    public required string Signature { get; init; }

    public required S3SigV4CanonicalRequest CanonicalRequest { get; init; }

    public required IReadOnlyList<KeyValuePair<string, string?>> QueryParameters { get; init; }
}
