namespace IntegratedS3.Protocol;

public sealed class S3SigV4CredentialScope
{
    public required string AccessKeyId { get; init; }

    public required string DateStamp { get; init; }

    public required string Region { get; init; }

    public required string Service { get; init; }

    public required string Terminator { get; init; }

    public string Scope => $"{DateStamp}/{Region}/{Service}/{Terminator}";
}
