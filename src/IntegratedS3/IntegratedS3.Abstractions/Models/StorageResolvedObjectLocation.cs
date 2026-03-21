namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Resolved physical location for accessing an object, including access mode and optional expiration.
/// </summary>
public sealed class StorageResolvedObjectLocation
{
    /// <summary>
    /// How the object content should be accessed.
    /// </summary>
    public StorageObjectAccessMode AccessMode { get; set; } = StorageObjectAccessMode.ProxyStream;

    /// <summary>
    /// The URI to access the object, or <see langword="null"/> for proxy-streamed content.
    /// </summary>
    public Uri? Location { get; set; }

    /// <summary>
    /// The UTC date and time when the location expires, or <see langword="null"/> if it does not expire.
    /// </summary>
    public DateTimeOffset? ExpiresAtUtc { get; set; }

    /// <summary>
    /// Headers to include when accessing the object at this location.
    /// </summary>
    public Dictionary<string, string> Headers { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
