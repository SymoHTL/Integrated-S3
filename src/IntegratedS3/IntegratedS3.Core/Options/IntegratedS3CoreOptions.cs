namespace IntegratedS3.Core.Options;

public sealed class IntegratedS3CoreOptions
{
	public StorageConsistencyMode ConsistencyMode { get; set; } = StorageConsistencyMode.PrimaryOnly;

	public StorageReadRoutingMode ReadRoutingMode { get; set; } = StorageReadRoutingMode.PrimaryOnly;

	public StorageReplicationOptions Replication { get; set; } = new();

	public StorageBackendHealthOptions BackendHealth { get; set; } = new();
}
