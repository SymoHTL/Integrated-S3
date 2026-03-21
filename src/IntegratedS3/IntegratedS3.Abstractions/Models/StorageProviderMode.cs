namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Describes how a storage provider operates within IntegratedS3.
/// </summary>
public enum StorageProviderMode
{
    /// <summary>
    /// Fully managed by IntegratedS3; all storage operations are handled internally.
    /// </summary>
    Managed,

    /// <summary>
    /// The backend handles operations but IntegratedS3 routes requests to it.
    /// </summary>
    Delegated,

    /// <summary>
    /// Requests are forwarded directly to the backend without IntegratedS3 processing.
    /// </summary>
    Passthrough,

    /// <summary>
    /// A combination of managed and delegated behavior depending on the operation.
    /// </summary>
    Hybrid
}
