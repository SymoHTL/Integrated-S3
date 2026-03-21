namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the credential scope portion of an AWS SigV4 signature, identifying the access key,
/// date, region, service, and terminator used for signing key derivation.
/// </summary>
public sealed class S3SigV4CredentialScope
{
    /// <summary>
    /// The AWS access key ID.
    /// </summary>
    public required string AccessKeyId { get; init; }

    /// <summary>
    /// The date stamp in <c>yyyyMMdd</c> format.
    /// </summary>
    public required string DateStamp { get; init; }

    /// <summary>
    /// The AWS region (e.g. <c>us-east-1</c>), or <c>*</c> for SigV4a.
    /// </summary>
    public required string Region { get; init; }

    /// <summary>
    /// The AWS service name (e.g. <c>s3</c>).
    /// </summary>
    public required string Service { get; init; }

    /// <summary>
    /// The scope terminator, always <c>aws4_request</c>.
    /// </summary>
    public required string Terminator { get; init; }

    /// <summary>
    /// The formatted scope string: <c>{DateStamp}/{Region}/{Service}/{Terminator}</c>.
    /// </summary>
    public string Scope => $"{DateStamp}/{Region}/{Service}/{Terminator}";
}
