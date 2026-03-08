namespace IntegratedS3.Core.Options;

public enum StorageReadRoutingMode
{
    PrimaryOnly,
    PreferPrimary,
    PreferHealthyReplica
}