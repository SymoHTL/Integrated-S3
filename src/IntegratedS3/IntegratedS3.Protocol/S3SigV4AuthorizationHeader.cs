namespace IntegratedS3.Protocol;

public sealed class S3SigV4AuthorizationHeader
{
    public required string Algorithm { get; init; }

    public required S3SigV4CredentialScope CredentialScope { get; init; }

    public required IReadOnlyList<string> SignedHeaders { get; init; }

    public required string Signature { get; init; }

    public string? SecurityToken { get; init; }
}
