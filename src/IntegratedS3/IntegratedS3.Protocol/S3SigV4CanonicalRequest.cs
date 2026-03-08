namespace IntegratedS3.Protocol;

public sealed class S3SigV4CanonicalRequest
{
    public required string CanonicalRequest { get; init; }

    public required string CanonicalRequestHashHex { get; init; }

    public required string CanonicalUri { get; init; }

    public required string CanonicalQueryString { get; init; }

    public required string CanonicalHeaders { get; init; }

    public required string SignedHeaders { get; init; }

    public required string PayloadHash { get; init; }
}
