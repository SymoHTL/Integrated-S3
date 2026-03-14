namespace IntegratedS3.Core.Options;

/// <summary>
/// Configures orchestration behavior in the core service layer.
/// </summary>
public sealed class IntegratedS3CoreOptions
{
    /// <summary>The write-consistency strategy used across registered backends.</summary>
    public StorageConsistencyMode ConsistencyMode { get; set; } = StorageConsistencyMode.PrimaryOnly;

    /// <summary>The read-routing policy used when multiple backends are available.</summary>
    public StorageReadRoutingMode ReadRoutingMode { get; set; } = StorageReadRoutingMode.PrimaryOnly;

    /// <summary>Replication and repair-backlog configuration.</summary>
    public StorageReplicationOptions Replication { get; set; } = new();

    /// <summary>Backend-health probing and evaluation configuration.</summary>
    public StorageBackendHealthOptions BackendHealth { get; set; } = new();
}
