namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the parsed components of an AWS SigV4 or SigV4a presigned request extracted from query parameters.
/// </summary>
public sealed class S3SigV4PresignedRequest
{
    /// <summary>
    /// The signing algorithm (e.g. <c>AWS4-HMAC-SHA256</c> or <c>AWS4-ECDSA-P256-SHA256</c>).
    /// </summary>
    public required string Algorithm { get; init; }

    /// <summary>
    /// The credential scope identifying the access key, date, region, and service.
    /// </summary>
    public required S3SigV4CredentialScope CredentialScope { get; init; }

    /// <summary>
    /// The UTC timestamp at which the presigned URL was created.
    /// </summary>
    public required DateTimeOffset SignedAtUtc { get; init; }

    /// <summary>
    /// The number of seconds after <see cref="SignedAtUtc"/> before the presigned URL expires.
    /// </summary>
    public required int ExpiresSeconds { get; init; }

    /// <summary>
    /// The list of lowercase header names included in the signature.
    /// </summary>
    public required IReadOnlyList<string> SignedHeaders { get; init; }

    /// <summary>
    /// The hex-encoded request signature.
    /// </summary>
    public required string Signature { get; init; }

    /// <summary>
    /// The optional STS security token, or <see langword="null"/> if not present.
    /// </summary>
    public string? SecurityToken { get; init; }

    /// <summary>
    /// For SigV4a: the X-Amz-Region-Set value (comma-separated regions or "*"). Null for SigV4.
    /// </summary>
    public IReadOnlyList<string>? RegionSet { get; init; }
}
