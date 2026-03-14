namespace IntegratedS3.Core.Services;

[Flags]
public enum StorageReplicaRepairDivergenceKind
{
    None = 0,
    Content = 1,
    Metadata = 2,
    Version = 4
}
