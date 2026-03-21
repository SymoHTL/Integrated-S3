namespace IntegratedS3.AspNetCore.Maintenance;

/// <summary>
/// Represents a maintenance job as a delegate.
/// Receives a scoped <see cref="IServiceProvider"/> for resolving services.
/// </summary>
/// <param name="serviceProvider">A scoped service provider for the job execution.</param>
/// <param name="cancellationToken">A token to cancel the operation.</param>
/// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
public delegate ValueTask IntegratedS3MaintenanceJobDelegate(IServiceProvider serviceProvider, CancellationToken cancellationToken);
