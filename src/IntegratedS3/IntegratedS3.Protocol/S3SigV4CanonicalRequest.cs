namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the components and derived values of an AWS SigV4 canonical request.
/// </summary>
public sealed class S3SigV4CanonicalRequest
{
    /// <summary>
    /// The full canonical request string as defined by the AWS SigV4 specification.
    /// </summary>
    public required string CanonicalRequest { get; init; }

    /// <summary>
    /// The lowercase hex-encoded SHA-256 hash of <see cref="CanonicalRequest"/>.
    /// </summary>
    public required string CanonicalRequestHashHex { get; init; }

    /// <summary>
    /// The URI-encoded canonical URI path component.
    /// </summary>
    public required string CanonicalUri { get; init; }

    /// <summary>
    /// The sorted, URI-encoded canonical query string component.
    /// </summary>
    public required string CanonicalQueryString { get; init; }

    /// <summary>
    /// The canonical headers string, containing only the signed headers in sorted order.
    /// </summary>
    public required string CanonicalHeaders { get; init; }

    /// <summary>
    /// The semicolon-delimited list of signed header names in sorted, lowercase form.
    /// </summary>
    public required string SignedHeaders { get; init; }

    /// <summary>
    /// The payload hash value used in the canonical request (e.g. a SHA-256 hex hash or <c>UNSIGNED-PAYLOAD</c>).
    /// </summary>
    public required string PayloadHash { get; init; }
}
