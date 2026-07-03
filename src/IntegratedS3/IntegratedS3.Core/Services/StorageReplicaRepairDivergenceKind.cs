namespace IntegratedS3.Core.Services;

/// <summary>The kind(s) of divergence between a primary and a replica backend that a repair entry addresses.</summary>
[Flags]
public enum StorageReplicaRepairDivergenceKind
{
    /// <summary>No divergence recorded.</summary>
    None = 0,

    /// <summary>The object content differs between primary and replica.</summary>
    Content = 1,

    /// <summary>Bucket or object metadata differs between primary and replica.</summary>
    Metadata = 2,

    /// <summary>Version state (e.g. versioning configuration or version history) differs between primary and replica.</summary>
    Version = 4
}
