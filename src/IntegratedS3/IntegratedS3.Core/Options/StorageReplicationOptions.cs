namespace IntegratedS3.Core.Options;

/// <summary>
/// Options controlling multi-backend replication behavior, including write-through
/// requirements and replica read eligibility.
/// </summary>
public sealed class StorageReplicationOptions
{
    /// <summary>
    /// Gets or sets whether write-through operations require all target replicas to
    /// be in a healthy state. Defaults to <see langword="true"/>.
    /// </summary>
    public bool RequireHealthyReplicasForWriteThrough { get; set; } = true;

    /// <summary>
    /// Gets or sets whether write-through operations require all target replicas to
    /// be current (no outstanding repair backlog). Defaults to <see langword="true"/>.
    /// </summary>
    public bool RequireCurrentReplicasForWriteThrough { get; set; } = true;

    /// <summary>
    /// Gets or sets whether read requests may be served from replicas that have
    /// outstanding repair entries. Defaults to <see langword="false"/>.
    /// </summary>
    public bool AllowReadsFromReplicasWithOutstandingRepairs { get; set; }

    /// <summary>
    /// Gets or sets whether the default in-process dispatcher should attempt
    /// asynchronous replica writes. Durable replay remains a host concern.
    /// Defaults to <see langword="true"/>.
    /// </summary>
    // This only controls the default in-process dispatcher; durable replay remains a host concern.
    public bool AttemptInProcessAsyncReplicaWrites { get; set; } = true;
}
