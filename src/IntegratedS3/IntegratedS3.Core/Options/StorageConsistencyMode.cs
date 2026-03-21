namespace IntegratedS3.Core.Options;

/// <summary>
/// Controls read-after-write consistency behavior by determining how writes are
/// propagated across storage backends.
/// </summary>
public enum StorageConsistencyMode
{
    /// <summary>
    /// Writes are sent only to the primary backend. Replicas are not written to.
    /// </summary>
    PrimaryOnly,

    /// <summary>
    /// Writes are synchronously propagated to all backends before returning success,
    /// providing strong consistency across replicas.
    /// </summary>
    WriteThroughAll,

    /// <summary>
    /// Writes are committed to the primary backend synchronously and then replicated
    /// to other backends asynchronously.
    /// </summary>
    WriteToPrimaryAsyncReplicas
}
