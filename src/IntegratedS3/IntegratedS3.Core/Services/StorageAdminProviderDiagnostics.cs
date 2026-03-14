using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Core.Services;

public sealed record StorageAdminProviderDiagnostics
{
    public string BackendName { get; init; } = string.Empty;

    public string Kind { get; init; } = string.Empty;

    public bool IsPrimary { get; init; }

    public string? Description { get; init; }

    public StorageProviderMode Mode { get; init; }

    public StorageBackendHealthStatus HealthStatus { get; init; }

    public StorageAdminReplicaLagDiagnostics? ReplicaLag { get; init; }
}
