namespace IntegratedS3.Core.Services;

public sealed record StorageAdminDiagnostics
{
    public DateTimeOffset ObservedAtUtc { get; init; }

    public StorageAdminProviderDiagnostics[] Providers { get; init; } = [];

    public StorageAdminRepairDiagnostics Repairs { get; init; } = new();
}
