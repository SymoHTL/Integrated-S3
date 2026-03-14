using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Core.Services;

internal sealed class StorageBackendHealthMonitor
{
    private readonly IStorageBackendHealthEvaluator _healthEvaluator;
    private readonly IStorageBackendHealthProbe _healthProbe;
    private readonly IOptions<IntegratedS3CoreOptions> _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<StorageBackendHealthMonitor> _logger;
    private readonly ConcurrentDictionary<string, StorageBackendHealthSnapshot> _snapshots = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _probeLocks = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, StorageBackendHealthStatus> _latestStatuses = new(StringComparer.Ordinal);
    private readonly ObservableGauge<int> _healthStatusGauge;

    public StorageBackendHealthMonitor(
        IStorageBackendHealthEvaluator healthEvaluator,
        IStorageBackendHealthProbe healthProbe,
        IOptions<IntegratedS3CoreOptions> options,
        TimeProvider timeProvider,
        ILogger<StorageBackendHealthMonitor> logger)
    {
        _healthEvaluator = healthEvaluator;
        _healthProbe = healthProbe;
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger;
        _healthStatusGauge = IntegratedS3Observability.Meter.CreateObservableGauge<int>(
            IntegratedS3Observability.Metrics.BackendHealthStatus,
            ObserveStatuses,
            unit: "{state}",
            description: "Latest observed storage backend health status (healthy=1, unhealthy=0, unknown=-1).");
    }

    public async ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(backend);
        cancellationToken.ThrowIfCancellationRequested();

        var evaluatorStatus = await _healthEvaluator.GetStatusAsync(backend, cancellationToken);
        var healthOptions = _options.Value.BackendHealth;
        if (!healthOptions.EnableDynamicSnapshots) {
            TrackStatus(backend, evaluatorStatus, "evaluator");
            return evaluatorStatus;
        }

        var now = _timeProvider.GetUtcNow();
        if (TryGetSnapshot(backend.Name, now, out var snapshot)) {
            var combinedSnapshotStatus = CombineStatuses(evaluatorStatus, snapshot.Status);
            TrackStatus(backend, combinedSnapshotStatus, "snapshot");
            return combinedSnapshotStatus;
        }

        if (!healthOptions.EnableActiveProbing || evaluatorStatus == StorageBackendHealthStatus.Unhealthy) {
            TrackStatus(backend, evaluatorStatus, "evaluator");
            return evaluatorStatus;
        }

        var probeStatus = await ProbeAsync(backend, healthOptions, cancellationToken);
        var combinedProbeStatus = CombineStatuses(evaluatorStatus, probeStatus);
        TrackStatus(backend, combinedProbeStatus, "probe");
        return combinedProbeStatus;
    }

    public void ReportSuccess(IStorageBackend backend)
    {
        ArgumentNullException.ThrowIfNull(backend);
        UpdateSnapshot(backend.Name, StorageBackendHealthStatus.Healthy, _options.Value.BackendHealth.HealthySnapshotTtl);
        TrackStatus(backend, StorageBackendHealthStatus.Healthy, "operation");
    }

    public void ReportFailure(IStorageBackend backend, StorageError? error)
    {
        ArgumentNullException.ThrowIfNull(backend);

        var status = error?.Code is StorageErrorCode.ProviderUnavailable or StorageErrorCode.Throttled
            ? StorageBackendHealthStatus.Unhealthy
            : StorageBackendHealthStatus.Healthy;
        var ttl = status == StorageBackendHealthStatus.Unhealthy
            ? _options.Value.BackendHealth.UnhealthySnapshotTtl
            : _options.Value.BackendHealth.HealthySnapshotTtl;

        UpdateSnapshot(backend.Name, status, ttl);
        TrackStatus(backend, status, "operation");
    }

    private async ValueTask<StorageBackendHealthStatus> ProbeAsync(IStorageBackend backend, StorageBackendHealthOptions healthOptions, CancellationToken cancellationToken)
    {
        var probeLock = _probeLocks.GetOrAdd(backend.Name, static _ => new SemaphoreSlim(1, 1));
        await probeLock.WaitAsync(cancellationToken);
        try {
            var now = _timeProvider.GetUtcNow();
            if (TryGetSnapshot(backend.Name, now, out var snapshot)) {
                return snapshot.Status;
            }

            using var timeoutSource = CreateProbeTimeoutSource(healthOptions.ProbeTimeout, cancellationToken);
            var probeToken = timeoutSource?.Token ?? cancellationToken;
            StorageBackendHealthStatus probeStatus;
            try {
                probeStatus = await _healthProbe.ProbeAsync(backend, probeToken);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) {
                _logger.LogWarning("IntegratedS3 health probe timed out for provider {Provider}.", backend.Name);
                probeStatus = StorageBackendHealthStatus.Unhealthy;
            }
            catch (Exception exception) {
                _logger.LogWarning(exception, "IntegratedS3 health probe failed unexpectedly for provider {Provider}.", backend.Name);
                probeStatus = StorageBackendHealthStatus.Unknown;
            }

            UpdateSnapshot(
                backend.Name,
                probeStatus,
                probeStatus == StorageBackendHealthStatus.Unhealthy
                    ? healthOptions.UnhealthySnapshotTtl
                    : healthOptions.HealthySnapshotTtl);

            return probeStatus;
        }
        finally {
            probeLock.Release();
        }
    }

    private void UpdateSnapshot(string backendName, StorageBackendHealthStatus status, TimeSpan ttl)
    {
        if (!_options.Value.BackendHealth.EnableDynamicSnapshots || ttl <= TimeSpan.Zero) {
            _snapshots.TryRemove(backendName, out _);
            return;
        }

        var observedAtUtc = _timeProvider.GetUtcNow();
        _snapshots[backendName] = new StorageBackendHealthSnapshot(status, observedAtUtc + ttl);
    }

    private bool TryGetSnapshot(string backendName, DateTimeOffset now, out StorageBackendHealthSnapshot snapshot)
    {
        if (_snapshots.TryGetValue(backendName, out snapshot)) {
            if (snapshot.ExpiresAtUtc > now) {
                return true;
            }

            _snapshots.TryRemove(new KeyValuePair<string, StorageBackendHealthSnapshot>(backendName, snapshot));
        }

        snapshot = default;
        return false;
    }

    private static CancellationTokenSource? CreateProbeTimeoutSource(TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (timeout <= TimeSpan.Zero) {
            return null;
        }

        var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        return timeoutSource;
    }

    private static StorageBackendHealthStatus CombineStatuses(StorageBackendHealthStatus evaluatorStatus, StorageBackendHealthStatus snapshotStatus)
    {
        if (evaluatorStatus == StorageBackendHealthStatus.Unhealthy || snapshotStatus == StorageBackendHealthStatus.Unhealthy) {
            return StorageBackendHealthStatus.Unhealthy;
        }

        if (evaluatorStatus == StorageBackendHealthStatus.Healthy || snapshotStatus == StorageBackendHealthStatus.Healthy) {
            return StorageBackendHealthStatus.Healthy;
        }

        return StorageBackendHealthStatus.Unknown;
    }

    private IEnumerable<Measurement<int>> ObserveStatuses()
    {
        return _latestStatuses.Select(entry => new Measurement<int>(
            ToMetricValue(entry.Value),
            new KeyValuePair<string, object?>(
                IntegratedS3Observability.Tags.Provider,
                entry.Key)));
    }

    private void TrackStatus(IStorageBackend backend, StorageBackendHealthStatus status, string source)
    {
        if (_latestStatuses.TryGetValue(backend.Name, out var previousStatus)
            && previousStatus == status) {
            return;
        }

        _latestStatuses[backend.Name] = status;
        if (status == StorageBackendHealthStatus.Unhealthy) {
            _logger.LogWarning(
                "IntegratedS3 provider {Provider} health changed to {Status} via {Source}.",
                backend.Name,
                status,
                source);
            return;
        }

        _logger.LogInformation(
            "IntegratedS3 provider {Provider} health changed to {Status} via {Source}.",
            backend.Name,
            status,
            source);
    }

    private static int ToMetricValue(StorageBackendHealthStatus status)
    {
        return status switch
        {
            StorageBackendHealthStatus.Healthy => 1,
            StorageBackendHealthStatus.Unhealthy => 0,
            _ => -1
        };
    }

    private readonly record struct StorageBackendHealthSnapshot(StorageBackendHealthStatus Status, DateTimeOffset ExpiresAtUtc);
}
