using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Core.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Core.Services;

internal sealed class InProcessStorageReplicaRepairDispatcher : IStorageReplicaRepairDispatcher, IAsyncDisposable
{
    /// <summary>Maximum time <see cref="DisposeAsync"/> waits for in-flight repairs to drain before giving up.</summary>
    internal static readonly TimeSpan DefaultDrainTimeout = TimeSpan.FromSeconds(10);

    private static readonly Histogram<double> ReplicaRepairDuration = IntegratedS3Observability.Meter.CreateHistogram<double>(
        IntegratedS3Observability.Metrics.ReplicaRepairDuration,
        unit: "ms",
        description: "Duration of in-process replica repair executions.");

    private readonly IStorageReplicaRepairBacklog _repairBacklog;
    private readonly IOptions<IntegratedS3CoreOptions> _options;
    private readonly ILogger<InProcessStorageReplicaRepairDispatcher> _logger;

    private readonly ConcurrentDictionary<string, SemaphoreSlim> _dispatchLocks = new(StringComparer.Ordinal);

    // Tracks outstanding dispatch tasks so shutdown can drain them instead of abandoning in-flight repairs.
    private readonly ConcurrentDictionary<Task, byte> _outstandingDispatches = new();

    // Signalled on shutdown/dispose; linked into every dispatched repair so they observe application-stopping.
    private readonly CancellationTokenSource _shutdownCts = new();

    private readonly TimeSpan _drainTimeout;
    private int _disposed;

    public InProcessStorageReplicaRepairDispatcher(
        IStorageReplicaRepairBacklog repairBacklog,
        IOptions<IntegratedS3CoreOptions> options,
        ILogger<InProcessStorageReplicaRepairDispatcher> logger)
        : this(repairBacklog, options, logger, DefaultDrainTimeout)
    {
    }

    internal InProcessStorageReplicaRepairDispatcher(
        IStorageReplicaRepairBacklog repairBacklog,
        IOptions<IntegratedS3CoreOptions> options,
        ILogger<InProcessStorageReplicaRepairDispatcher> logger,
        TimeSpan drainTimeout)
    {
        _repairBacklog = repairBacklog;
        _options = options;
        _logger = logger;
        _drainTimeout = drainTimeout;
    }

    public async ValueTask DispatchAsync(
        StorageReplicaRepairEntry entry,
        Func<CancellationToken, ValueTask<StorageError?>> repairOperation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(repairOperation);

        await _repairBacklog.AddAsync(entry, cancellationToken);
        _logger.LogInformation(
            "Dispatching replica repair {RepairId} for {ReplicaBackend}. Origin {Origin}.",
            entry.Id,
            entry.ReplicaBackendName,
            entry.Origin);
        IntegratedS3CoreTelemetry.AddReplicaEvent(
            Activity.Current,
            "replica-repair-queued",
            entry.Operation,
            entry.ReplicaBackendName,
            entry.Origin,
            entry.Status);
        if (!_options.Value.Replication.AttemptInProcessAsyncReplicaWrites) {
            return;
        }

        // If the dispatcher is already shutting down, do not start new background work; the entry stays
        // Pending in the backlog so a replay job (or the next boot) can pick it up.
        if (_shutdownCts.IsCancellationRequested) {
            _logger.LogInformation(
                "Skipping in-process dispatch for replica repair {RepairId} because the dispatcher is shutting down. Entry left Pending.",
                entry.Id);
            return;
        }

        var dispatchLock = _dispatchLocks.GetOrAdd(entry.ReplicaBackendName, static _ => new SemaphoreSlim(1, 1));

        // Track the dispatched task so DisposeAsync can await it on shutdown, then prune it from the
        // tracking set once it finishes so the set does not grow unbounded over the process lifetime.
        var dispatch = Task.Run(() => RunDispatchAsync(entry, dispatchLock, repairOperation), CancellationToken.None);
        _outstandingDispatches.TryAdd(dispatch, 0);
        _ = dispatch.ContinueWith(
            static (completed, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(completed, out _),
            _outstandingDispatches,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private async Task RunDispatchAsync(
        StorageReplicaRepairEntry entry,
        SemaphoreSlim dispatchLock,
        Func<CancellationToken, ValueTask<StorageError?>> repairOperation)
    {
        var repairToken = _shutdownCts.Token;
        using var activity = StartRepairActivity(entry);
        var startedAt = Stopwatch.GetTimestamp();
        StorageError? observedError = null;
        var succeeded = false;

        try {
            try {
                await dispatchLock.WaitAsync(repairToken);
            }
            catch (OperationCanceledException) {
                // Shutdown observed before we ever marked the entry in-progress; it is still Pending, nothing to revert.
                _logger.LogInformation(
                    "Replica repair {RepairId} for {ReplicaBackend} was not started because the dispatcher is shutting down. Entry left Pending.",
                    entry.Id,
                    entry.ReplicaBackendName);
                return;
            }

            try {
                await _repairBacklog.MarkInProgressAsync(entry.Id, CancellationToken.None);

                try {
                    observedError = await repairOperation(repairToken);
                }
                catch (OperationCanceledException) when (repairToken.IsCancellationRequested) {
                    // Interrupted by shutdown after being marked in-progress: revert to a re-runnable state so the
                    // entry is never stranded InProgress with no live owner.
                    await RevertInterruptedAsync(entry);
                    return;
                }
                catch (Exception ex) {
                    observedError = CreateDispatchError(entry, ex);
                }

                if (observedError is null) {
                    succeeded = true;
                    await _repairBacklog.MarkCompletedAsync(entry.Id, CancellationToken.None);
                    activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
                    _logger.LogInformation(
                        "Replica repair {RepairId} completed successfully for {ReplicaBackend}.",
                        entry.Id,
                        entry.ReplicaBackendName);
                    return;
                }

                IntegratedS3CoreTelemetry.MarkFailure(activity, observedError);
                await _repairBacklog.MarkFailedAsync(entry.Id, observedError, CancellationToken.None);
                _logger.LogWarning(
                    "Replica repair {RepairId} failed for {ReplicaBackend}. ErrorCode {ErrorCode}.",
                    entry.Id,
                    entry.ReplicaBackendName,
                    observedError.Code);
            }
            catch (OperationCanceledException) when (repairToken.IsCancellationRequested) {
                await RevertInterruptedAsync(entry);
            }
            catch (Exception ex) {
                observedError ??= CreateDispatchError(entry, ex);
                IntegratedS3CoreTelemetry.MarkFailure(activity, observedError);
                _logger.LogError(
                    ex,
                    "In-process replica repair dispatch for repair {RepairId} targeting provider {ReplicaBackend} failed unexpectedly.",
                    entry.Id,
                    entry.ReplicaBackendName);

                try {
                    await _repairBacklog.MarkFailedAsync(entry.Id, observedError, CancellationToken.None);
                }
                catch (Exception backlogException) {
                    _logger.LogError(
                        backlogException,
                        "Failed to mark replica repair {RepairId} as failed after an unexpected dispatch exception.",
                        entry.Id);
                }
            }
        }
        finally {
            RecordRepairDuration(entry, succeeded, observedError, Stopwatch.GetElapsedTime(startedAt));
            dispatchLock.Release();
        }
    }

    /// <summary>
    /// Reverts an in-progress repair that was interrupted by shutdown back to <see cref="StorageReplicaRepairStatus.Pending"/>
    /// so it is never stranded in <see cref="StorageReplicaRepairStatus.InProgress"/> with no live task owning it.
    /// </summary>
    private async Task RevertInterruptedAsync(StorageReplicaRepairEntry entry)
    {
        _logger.LogInformation(
            "Replica repair {RepairId} for {ReplicaBackend} was interrupted by shutdown; reverting to Pending so it can be retried.",
            entry.Id,
            entry.ReplicaBackendName);

        try {
            await _repairBacklog.RevertToPendingAsync(entry.Id, CancellationToken.None);
        }
        catch (Exception ex) {
            _logger.LogError(
                ex,
                "Failed to revert interrupted replica repair {RepairId} to Pending during shutdown.",
                entry.Id);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) {
            return;
        }

        // Signal every in-flight repair to observe application-stopping.
        await _shutdownCts.CancelAsync();

        var outstanding = _outstandingDispatches.Keys.ToArray();
        if (outstanding.Length > 0) {
            _logger.LogInformation(
                "Draining {OutstandingCount} in-flight replica repair dispatch task(s) during shutdown.",
                outstanding.Length);

            try {
                var drain = Task.WhenAll(outstanding);
                var completed = await Task.WhenAny(drain, Task.Delay(_drainTimeout)).ConfigureAwait(false);
                if (completed != drain) {
                    _logger.LogWarning(
                        "Timed out after {DrainTimeout} draining in-flight replica repair dispatch tasks during shutdown; " +
                        "any still-running entries may remain InProgress until reconciled.",
                        _drainTimeout);
                }
                else {
                    // Observe faulted tasks without rethrowing (each task already handles its own errors).
                    await drain.ConfigureAwait(false);
                }
            }
            catch (Exception ex) {
                _logger.LogWarning(
                    ex,
                    "One or more replica repair dispatch tasks faulted while draining during shutdown.");
            }
        }

        foreach (var dispatchLock in _dispatchLocks.Values) {
            dispatchLock.Dispose();
        }

        _dispatchLocks.Clear();
        _outstandingDispatches.Clear();
        _shutdownCts.Dispose();
    }

    private static Activity? StartRepairActivity(StorageReplicaRepairEntry entry)
    {
        var activity = IntegratedS3Observability.ActivitySource.StartActivity("IntegratedS3.ReplicaRepair", ActivityKind.Internal);
        if (activity is null) {
            return null;
        }

        activity.SetTag(IntegratedS3Observability.Tags.Operation, entry.Operation.ToString());
        activity.SetTag(IntegratedS3Observability.Tags.PrimaryProvider, entry.PrimaryBackendName);
        activity.SetTag(IntegratedS3Observability.Tags.ReplicaBackend, entry.ReplicaBackendName);
        activity.SetTag(IntegratedS3Observability.Tags.RepairOrigin, entry.Origin.ToString());
        activity.SetTag(IntegratedS3Observability.Tags.RepairStatus, entry.Status.ToString());
        activity.SetTag("integrateds3.repair_id", entry.Id);
        return activity;
    }

    private static void RecordRepairDuration(StorageReplicaRepairEntry entry, bool succeeded, StorageError? error, TimeSpan duration)
    {
        var tags = new TagList
        {
            { IntegratedS3Observability.Tags.Operation, entry.Operation.ToString() },
            { IntegratedS3Observability.Tags.PrimaryProvider, entry.PrimaryBackendName },
            { IntegratedS3Observability.Tags.ReplicaBackend, entry.ReplicaBackendName },
            { IntegratedS3Observability.Tags.RepairOrigin, entry.Origin.ToString() },
            { IntegratedS3Observability.Tags.Result, succeeded ? "success" : "failure" }
        };

        if (error is not null) {
            tags.Add(IntegratedS3Observability.Tags.ErrorCode, error.Code.ToString());
        }

        ReplicaRepairDuration.Record(duration.TotalMilliseconds, tags);
    }

    private static StorageError CreateDispatchError(StorageReplicaRepairEntry entry, Exception exception)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = $"Asynchronous replica repair for provider '{entry.ReplicaBackendName}' failed during in-process dispatch: {exception.Message}",
            BucketName = entry.BucketName,
            ObjectKey = entry.ObjectKey,
            VersionId = entry.VersionId,
            ProviderName = entry.ReplicaBackendName,
            SuggestedHttpStatusCode = 503
        };
    }
}
