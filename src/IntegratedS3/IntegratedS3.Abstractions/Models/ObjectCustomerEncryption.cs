namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// SSE-C request-side settings: the customer-provided key material needed
/// to encrypt (write path) or decrypt (read path) an object.
/// </summary>
public sealed class ObjectCustomerEncryptionSettings
{
    /// <summary>Algorithm — always "AES256" for SSE-C today.</summary>
    public required string Algorithm { get; init; }

    /// <summary>Base64-encoded 256-bit encryption key.</summary>
    public required string Key { get; init; }

    /// <summary>Base64-encoded MD5 digest of the encryption key.</summary>
    public required string KeyMd5 { get; init; }
}

/// <summary>
/// SSE-C response-side info echoed back by the storage provider.
/// </summary>
public sealed class ObjectCustomerEncryptionInfo
{
    /// <summary>Algorithm echoed back — always "AES256" for SSE-C today.</summary>
    public required string Algorithm { get; init; }

    /// <summary>Base64-encoded MD5 digest of the key that was used.</summary>
    public required string KeyMd5 { get; init; }
}
