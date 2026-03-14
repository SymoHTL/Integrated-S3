namespace IntegratedS3.AspNetCore.Maintenance;

public sealed class IntegratedS3MaintenanceJobOptions
{
    public bool Enabled { get; set; } = true;

    public bool RunOnStartup { get; set; }

    public TimeSpan Interval { get; set; } = TimeSpan.FromMinutes(5);
}
