namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Describes how callers access object content once a location has been resolved.
/// </summary>
public enum StorageObjectAccessMode
{
    /// <summary>The IntegratedS3 host proxies the object stream.</summary>
    ProxyStream,

    /// <summary>The caller is redirected to a plain provider URL.</summary>
    Redirect,

    /// <summary>The caller receives a delegated provider-managed access grant.</summary>
    Delegated,

    /// <summary>The caller uses a provider URL directly without additional host mediation.</summary>
    Passthrough
}
