namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Describes how object content is delivered to clients.
/// </summary>
public enum StorageObjectAccessMode
{
    /// <summary>
    /// Content is proxied through IntegratedS3 to the client.
    /// </summary>
    ProxyStream,

    /// <summary>
    /// The client is redirected to a direct URL for the object content.
    /// </summary>
    Redirect,

    /// <summary>
    /// The backend provides the content delivery mechanism.
    /// </summary>
    Delegated,

    /// <summary>
    /// Content streams directly from the backend without transformation.
    /// </summary>
    Passthrough
}
