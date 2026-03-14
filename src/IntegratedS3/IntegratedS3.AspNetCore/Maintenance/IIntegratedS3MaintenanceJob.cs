namespace IntegratedS3.AspNetCore.Maintenance;

public interface IIntegratedS3MaintenanceJob
{
    ValueTask ExecuteAsync(CancellationToken cancellationToken = default);
}
