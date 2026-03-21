namespace IntegratedS3.Protocol;

/// <summary>
/// Contains the computed output of a SigV4 or SigV4a presigning operation, including the signature,
/// canonical request, and the complete set of query parameters for the presigned URL.
/// </summary>
public sealed class S3SigV4PresignedRequestData
{
    /// <summary>
    /// The credential scope used during signing.
    /// </summary>
    public required S3SigV4CredentialScope CredentialScope { get; init; }

    /// <summary>
    /// The UTC timestamp at which the presigned URL was created.
    /// </summary>
    public required DateTimeOffset SignedAtUtc { get; init; }

    /// <summary>
    /// The UTC timestamp at which the presigned URL expires.
    /// </summary>
    public required DateTimeOffset ExpiresAtUtc { get; init; }

    /// <summary>
    /// The hex-encoded request signature.
    /// </summary>
    public required string Signature { get; init; }

    /// <summary>
    /// The canonical request that was signed.
    /// </summary>
    public required S3SigV4CanonicalRequest CanonicalRequest { get; init; }

    /// <summary>
    /// The complete set of query parameters for the presigned URL, including the <c>X-Amz-Signature</c> parameter.
    /// </summary>
    public required IReadOnlyList<KeyValuePair<string, string?>> QueryParameters { get; init; }
}
