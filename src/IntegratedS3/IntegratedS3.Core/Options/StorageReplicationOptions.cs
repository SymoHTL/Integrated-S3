namespace IntegratedS3.Core.Options;

public sealed class StorageReplicationOptions
{
    public bool RequireHealthyReplicasForWriteThrough { get; set; } = true;

    public bool RequireCurrentReplicasForWriteThrough { get; set; } = true;

    public bool AllowReadsFromReplicasWithOutstandingRepairs { get; set; }

    // This only controls the default in-process dispatcher; durable replay remains a host concern.
    public bool AttemptInProcessAsyncReplicaWrites { get; set; } = true;
}
