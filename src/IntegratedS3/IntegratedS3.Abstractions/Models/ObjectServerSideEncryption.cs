using System.Text.Json.Serialization;

namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Server-side encryption settings for a write request.
/// </summary>
public sealed class ObjectServerSideEncryptionSettings
{
    /// <summary>
    /// The server-side encryption algorithm to use.
    /// </summary>
    public required ObjectServerSideEncryptionAlgorithm Algorithm { get; init; }

    /// <summary>
    /// The identifier of the encryption key (KMS key ARN), or <see langword="null"/> for S3-managed keys.
    /// </summary>
    public string? KeyId { get; init; }

    /// <summary>
    /// Additional encryption context key-value pairs for KMS encryption.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Context { get; init; }
}

/// <summary>
/// Server-side encryption information returned in object responses.
/// </summary>
public sealed class ObjectServerSideEncryptionInfo
{
    /// <summary>
    /// The server-side encryption algorithm that was used.
    /// </summary>
    public required ObjectServerSideEncryptionAlgorithm Algorithm { get; init; }

    /// <summary>
    /// The identifier of the encryption key (KMS key ARN), or <see langword="null"/> for S3-managed keys.
    /// </summary>
    public string? KeyId { get; init; }

    /// <summary>
    /// Whether an S3 Bucket Key was used for SSE-KMS encryption.
    /// </summary>
    public bool BucketKeyEnabled { get; init; }
}

/// <summary>
/// Server-side encryption algorithms supported by S3.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<ObjectServerSideEncryptionAlgorithm>))]
public enum ObjectServerSideEncryptionAlgorithm
{
    /// <summary>
    /// S3-managed AES-256 encryption.
    /// </summary>
    Aes256,

    /// <summary>
    /// AWS KMS-managed encryption.
    /// </summary>
    Kms,

    /// <summary>
    /// AWS KMS-managed encryption with dual-layer server-side encryption.
    /// </summary>
    KmsDsse
}
