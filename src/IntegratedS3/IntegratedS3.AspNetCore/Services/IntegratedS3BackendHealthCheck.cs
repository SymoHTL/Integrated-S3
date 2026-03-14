using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Services;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace IntegratedS3.AspNetCore.Services;

internal sealed class IntegratedS3BackendHealthCheck(
    IEnumerable<IStorageBackend> backends,
    StorageBackendHealthMonitor healthMonitor) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var orderedBackends = backends
            .OrderByDescending(static backend => backend.IsPrimary)
            .ThenBy(static backend => backend.Name, StringComparer.Ordinal)
            .ToArray();

        var data = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["backendCount"] = orderedBackends.Length
        };

        if (orderedBackends.Length == 0) {
            return new HealthCheckResult(
                context.Registration.FailureStatus,
                "No IntegratedS3 storage backends are registered.",
                data: data);
        }

        var unhealthyBackends = new List<string>();
        var unknownBackends = new List<string>();

        foreach (var backend in orderedBackends) {
            var status = await healthMonitor.GetStatusAsync(backend, cancellationToken);
            data[$"backend:{backend.Name}"] = status.ToString();
            data[$"backend:{backend.Name}:kind"] = backend.Kind;
            data[$"backend:{backend.Name}:role"] = backend.IsPrimary ? "primary" : "replica";

            switch (status) {
                case StorageBackendHealthStatus.Unhealthy:
                    unhealthyBackends.Add(backend.Name);
                    break;
                case StorageBackendHealthStatus.Unknown:
                    unknownBackends.Add(backend.Name);
                    break;
            }
        }

        if (unhealthyBackends.Count > 0) {
            return new HealthCheckResult(
                context.Registration.FailureStatus,
                $"IntegratedS3 backend readiness found unhealthy backend(s): {string.Join(", ", unhealthyBackends)}.",
                data: data);
        }

        if (unknownBackends.Count > 0) {
            return HealthCheckResult.Degraded(
                $"IntegratedS3 backend readiness found backend(s) with unknown health: {string.Join(", ", unknownBackends)}.",
                data: data);
        }

        return HealthCheckResult.Healthy(
            $"IntegratedS3 backend readiness is healthy across {orderedBackends.Length} backend(s).",
            data: data);
    }
}
