namespace IntegratedS3.Core.Services;

public interface IStorageAdminDiagnosticsProvider
{
    ValueTask<StorageAdminDiagnostics> GetDiagnosticsAsync(CancellationToken cancellationToken = default);
}
