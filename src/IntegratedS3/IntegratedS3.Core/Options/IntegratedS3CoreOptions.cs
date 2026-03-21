namespace IntegratedS3.Core.Options;

/// <summary>
/// Root configuration for the IntegratedS3 core orchestration layer.
/// Controls consistency, read routing, replication, and backend health behavior.
/// </summary>
public sealed class IntegratedS3CoreOptions
{
    /// <summary>
    /// Gets or sets the write-consistency mode that determines how writes are
    /// propagated across storage backends. Defaults to
    /// <see cref="StorageConsistencyMode.PrimaryOnly"/>.
    /// </summary>
    public StorageConsistencyMode ConsistencyMode { get; set; } = StorageConsistencyMode.PrimaryOnly;

    /// <summary>
    /// Gets or sets the read-routing strategy that determines how read requests
    /// are distributed across available backends. Defaults to
    /// <see cref="StorageReadRoutingMode.PrimaryOnly"/>.
    /// </summary>
    public StorageReadRoutingMode ReadRoutingMode { get; set; } = StorageReadRoutingMode.PrimaryOnly;

    /// <summary>
    /// Gets or sets the options controlling multi-backend replication behavior.
    /// </summary>
    public StorageReplicationOptions Replication { get; set; } = new();

    /// <summary>
    /// Gets or sets the options controlling health monitoring of storage backends.
    /// </summary>
    public StorageBackendHealthOptions BackendHealth { get; set; } = new();
}
