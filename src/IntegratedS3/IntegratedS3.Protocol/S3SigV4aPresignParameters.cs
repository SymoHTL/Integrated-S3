namespace IntegratedS3.Protocol;

/// <summary>
/// Input parameters for generating a SigV4a presigned URL via <see cref="S3SigV4aPresigner.Presign"/>.
/// </summary>
public sealed class S3SigV4aPresignParameters
{
    /// <summary>
    /// The HTTP method for the presigned request (e.g. <c>GET</c>, <c>PUT</c>).
    /// </summary>
    public required string HttpMethod { get; init; }

    /// <summary>
    /// The URI path of the target resource.
    /// </summary>
    public required string Path { get; init; }

    /// <summary>
    /// Additional query parameters to include in the presigned URL.
    /// </summary>
    public IReadOnlyList<KeyValuePair<string, string?>> QueryParameters { get; init; } = [];

    /// <summary>
    /// The request headers to include when building the canonical request.
    /// </summary>
    public IReadOnlyList<KeyValuePair<string, string?>> Headers { get; init; } = [];

    /// <summary>
    /// The names of headers to include in the signature.
    /// </summary>
    public IReadOnlyList<string> SignedHeaders { get; init; } = [];

    /// <summary>
    /// The AWS access key ID.
    /// </summary>
    public required string AccessKeyId { get; init; }

    /// <summary>
    /// The AWS secret access key used for ECDSA key derivation.
    /// </summary>
    public required string SecretAccessKey { get; init; }

    /// <summary>
    /// The set of regions this presigned request is valid for.
    /// Defaults to <c>["*"]</c> (all regions).
    /// </summary>
    public IReadOnlyList<string> RegionSet { get; init; } = ["*"];

    /// <summary>
    /// The AWS service name for the credential scope (e.g. <c>s3</c>).
    /// </summary>
    public required string Service { get; init; }

    /// <summary>
    /// The UTC timestamp at which the presigned URL is created.
    /// </summary>
    public required DateTimeOffset SignedAtUtc { get; init; }

    /// <summary>
    /// The number of seconds until the presigned URL expires.
    /// </summary>
    public required int ExpiresInSeconds { get; init; }

    /// <summary>
    /// The payload hash value. Defaults to <c>UNSIGNED-PAYLOAD</c>.
    /// </summary>
    public string PayloadHash { get; init; } = "UNSIGNED-PAYLOAD";

    /// <summary>
    /// The optional STS session token to include as <c>X-Amz-Security-Token</c>.
    /// </summary>
    public string? SecurityToken { get; init; }
}
