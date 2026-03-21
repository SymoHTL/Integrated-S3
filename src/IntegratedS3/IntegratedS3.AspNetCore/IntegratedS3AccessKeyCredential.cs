namespace IntegratedS3.AspNetCore;

/// <summary>
/// Represents an access key credential pair for AWS Signature V4 authentication.
/// </summary>
/// <remarks>
/// Used by the built-in authenticator to verify incoming requests and by the presign
/// strategy to generate presigned URLs. Configure instances of this class in
/// <see cref="IntegratedS3Options.AccessKeyCredentials"/>.
/// </remarks>
public sealed class IntegratedS3AccessKeyCredential
{
    /// <summary>
    /// Unique identifier for this access key. Used in the <c>Authorization</c> header credential scope.
    /// </summary>
    public string AccessKeyId { get; set; } = string.Empty;

    /// <summary>
    /// Secret key used to compute HMAC signatures.
    /// </summary>
    /// <remarks>
    /// Keep this value confidential. It should never be logged or exposed in diagnostics output.
    /// </remarks>
    public string SecretAccessKey { get; set; } = string.Empty;

    /// <summary>
    /// Optional session token for temporary credentials.
    /// </summary>
    /// <remarks>
    /// When present, verified against the <c>X-Amz-Security-Token</c> header in incoming requests.
    /// </remarks>
    public string? SessionToken { get; set; }

    /// <summary>
    /// Optional human-readable name for this credential, used in logs and diagnostics.
    /// </summary>
    public string? DisplayName { get; set; }

    /// <summary>
    /// Optional list of scope restrictions for this credential.
    /// </summary>
    /// <remarks>
    /// Can be used by custom authorization logic to limit which operations this credential
    /// can perform.
    /// </remarks>
    public List<string> Scopes { get; set; } = [];
}
