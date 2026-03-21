namespace IntegratedS3.Core.Services;

/// <summary>
/// Represents the health state of a storage backend.
/// </summary>
public enum StorageBackendHealthStatus
{
    /// <summary>
    /// The health of the backend has not been determined.
    /// </summary>
    Unknown,

    /// <summary>
    /// The backend is reachable and operating normally.
    /// </summary>
    Healthy,

    /// <summary>
    /// The backend is unreachable or experiencing errors.
    /// </summary>
    Unhealthy
}