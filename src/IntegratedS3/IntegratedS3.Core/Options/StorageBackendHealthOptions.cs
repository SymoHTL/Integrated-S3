namespace IntegratedS3.Core.Options;

/// <summary>
/// Options controlling health monitoring of storage backends, including snapshot
/// caching, active probing, and timeout behavior.
/// </summary>
public sealed class StorageBackendHealthOptions
{
    /// <summary>
    /// Gets or sets whether dynamic health snapshots are enabled, allowing the
    /// system to cache and reuse health state between checks. Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnableDynamicSnapshots { get; set; } = true;

    /// <summary>
    /// Gets or sets whether active health probing is enabled. When enabled, backends
    /// are periodically probed rather than relying solely on passive observation.
    /// Defaults to <see langword="false"/>.
    /// </summary>
    public bool EnableActiveProbing { get; set; }

    /// <summary>
    /// Gets or sets the time-to-live for a cached health snapshot when the backend
    /// is considered healthy. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan HealthySnapshotTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the time-to-live for a cached health snapshot when the backend
    /// is considered unhealthy. Defaults to 10 seconds.
    /// </summary>
    public TimeSpan UnhealthySnapshotTtl { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets the timeout for individual active health probes. Defaults to 3 seconds.
    /// </summary>
    public TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromSeconds(3);
}
