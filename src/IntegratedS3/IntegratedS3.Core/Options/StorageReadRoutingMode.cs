namespace IntegratedS3.Core.Options;

/// <summary>
/// Controls how read requests are routed across available storage backends.
/// </summary>
public enum StorageReadRoutingMode
{
    /// <summary>
    /// All reads are served exclusively from the primary backend.
    /// </summary>
    PrimaryOnly,

    /// <summary>
    /// Reads are served from the primary backend when it is healthy; otherwise,
    /// a healthy replica is selected as a fallback.
    /// </summary>
    PreferPrimary,

    /// <summary>
    /// Reads are preferentially routed to a healthy replica, falling back to the
    /// primary backend when no healthy replica is available.
    /// </summary>
    PreferHealthyReplica
}