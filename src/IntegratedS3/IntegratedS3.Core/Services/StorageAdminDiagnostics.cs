namespace IntegratedS3.Core.Services;

/// <summary>
/// Top-level administrative diagnostics snapshot covering all storage providers and the repair subsystem.
/// </summary>
public sealed record StorageAdminDiagnostics
{
    /// <summary>The UTC date/time when this diagnostics snapshot was captured.</summary>
    public DateTimeOffset ObservedAtUtc { get; init; }

    /// <summary>Per-provider diagnostics for each configured storage backend.</summary>
    public StorageAdminProviderDiagnostics[] Providers { get; init; } = [];

    /// <summary>Aggregate diagnostics for the replica repair subsystem.</summary>
    public StorageAdminRepairDiagnostics Repairs { get; init; } = new();
}
