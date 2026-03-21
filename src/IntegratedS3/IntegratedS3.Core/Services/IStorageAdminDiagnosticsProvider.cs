namespace IntegratedS3.Core.Services;

/// <summary>
/// Provides admin-level diagnostics for the IntegratedS3 storage system, including
/// provider status, repair backlogs, and replica health.
/// </summary>
public interface IStorageAdminDiagnosticsProvider
{
    /// <summary>
    /// Collects and returns a snapshot of current storage system diagnostics.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageAdminDiagnostics"/> containing the diagnostics snapshot.</returns>
    ValueTask<StorageAdminDiagnostics> GetDiagnosticsAsync(CancellationToken cancellationToken = default);
}
