namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Presigned URL grant for direct object access, allowing clients to interact with objects without proxying through IntegratedS3.
/// </summary>
public sealed class StorageDirectObjectAccessGrant
{
    /// <summary>
    /// The presigned URL the client should use to access the object.
    /// </summary>
    public required Uri Url { get; init; }

    /// <summary>
    /// The UTC date and time when the presigned URL expires.
    /// </summary>
    public required DateTimeOffset ExpiresAtUtc { get; init; }

    /// <summary>
    /// Headers that the client must include when using the presigned URL.
    /// </summary>
    public Dictionary<string, string> Headers { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
