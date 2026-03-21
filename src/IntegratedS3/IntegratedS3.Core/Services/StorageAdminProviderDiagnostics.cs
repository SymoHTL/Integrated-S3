using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Diagnostics for a single configured storage backend (provider).
/// </summary>
public sealed record StorageAdminProviderDiagnostics
{
    /// <summary>The unique name identifying this backend in the configuration.</summary>
    public string BackendName { get; init; } = string.Empty;

    /// <summary>The provider kind (e.g. <c>S3</c>, <c>disk</c>).</summary>
    public string Kind { get; init; } = string.Empty;

    /// <summary>Indicates whether this backend is the primary (authoritative) storage backend.</summary>
    public bool IsPrimary { get; init; }

    /// <summary>An optional human-readable description of the backend.</summary>
    public string? Description { get; init; }

    /// <summary>The operational mode of the provider (e.g. read-write, read-only).</summary>
    public StorageProviderMode Mode { get; init; }

    /// <summary>The current health status of the backend.</summary>
    public StorageBackendHealthStatus HealthStatus { get; init; }

    /// <summary>Replica lag diagnostics for this backend, or <see langword="null"/> if it is the primary.</summary>
    public StorageAdminReplicaLagDiagnostics? ReplicaLag { get; init; }
}
