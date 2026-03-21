namespace IntegratedS3.AspNetCore.Maintenance;

/// <summary>
/// Configuration options for a scheduled IntegratedS3 maintenance job.
/// </summary>
public sealed class IntegratedS3MaintenanceJobOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether the job is active.
    /// Defaults to <see langword="true"/>.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether to run the job immediately on application startup.
    /// Defaults to <see langword="false"/>.
    /// </summary>
    public bool RunOnStartup { get; set; }

    /// <summary>
    /// Gets or sets the time between job executions.
    /// Defaults to 5 minutes. Must be greater than <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromMinutes(5);
}
