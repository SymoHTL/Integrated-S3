namespace IntegratedS3.AspNetCore.Maintenance;

/// <summary>
/// Defines a maintenance job that can be scheduled to run periodically
/// by the IntegratedS3 maintenance infrastructure.
/// </summary>
public interface IIntegratedS3MaintenanceJob
{
    /// <summary>
    /// Executes the maintenance job.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    ValueTask ExecuteAsync(CancellationToken cancellationToken = default);
}
