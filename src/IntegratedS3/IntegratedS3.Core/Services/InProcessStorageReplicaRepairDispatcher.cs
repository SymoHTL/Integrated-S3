using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Core.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Core.Services;

internal sealed class InProcessStorageReplicaRepairDispatcher(
    IStorageReplicaRepairBacklog repairBacklog,
    IOptions<IntegratedS3CoreOptions> options,
    ILogger<InProcessStorageReplicaRepairDispatcher> logger) : IStorageReplicaRepairDispatcher
{
    private static readonly Histogram<double> ReplicaRepairDuration = IntegratedS3Observability.Meter.CreateHistogram<double>(
        IntegratedS3Observability.Metrics.ReplicaRepairDuration,
        unit: "ms",
        description: "Duration of in-process replica repair executions.");

    private readonly ConcurrentDictionary<string, SemaphoreSlim> _dispatchLocks = new(StringComparer.Ordinal);

    public async ValueTask DispatchAsync(
        StorageReplicaRepairEntry entry,
        Func<CancellationToken, ValueTask<StorageError?>> repairOperation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(repairOperation);

        await repairBacklog.AddAsync(entry, cancellationToken);
        logger.LogInformation(
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
        if (!options.Value.Replication.AttemptInProcessAsyncReplicaWrites) {
            return;
        }

        var dispatchLock = _dispatchLocks.GetOrAdd(entry.ReplicaBackendName, static _ => new SemaphoreSlim(1, 1));
        _ = Task.Run(() => RunDispatchAsync(entry, dispatchLock, repairOperation));
    }

    private async Task RunDispatchAsync(
        StorageReplicaRepairEntry entry,
        SemaphoreSlim dispatchLock,
        Func<CancellationToken, ValueTask<StorageError?>> repairOperation)
    {
        using var activity = StartRepairActivity(entry);
        var startedAt = Stopwatch.GetTimestamp();
        StorageError? observedError = null;
        var succeeded = false;

        await dispatchLock.WaitAsync(CancellationToken.None);
        try {
            try {
                await repairBacklog.MarkInProgressAsync(entry.Id, CancellationToken.None);

                try {
                    observedError = await repairOperation(CancellationToken.None);
                }
                catch (Exception ex) {
                    observedError = CreateDispatchError(entry, ex);
                }

                if (observedError is null) {
                    succeeded = true;
                    await repairBacklog.MarkCompletedAsync(entry.Id, CancellationToken.None);
                    activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
                    logger.LogInformation(
                        "Replica repair {RepairId} completed successfully for {ReplicaBackend}.",
                        entry.Id,
                        entry.ReplicaBackendName);
                    return;
                }

                IntegratedS3CoreTelemetry.MarkFailure(activity, observedError);
                await repairBacklog.MarkFailedAsync(entry.Id, observedError, CancellationToken.None);
                logger.LogWarning(
                    "Replica repair {RepairId} failed for {ReplicaBackend}. ErrorCode {ErrorCode}.",
                    entry.Id,
                    entry.ReplicaBackendName,
                    observedError.Code);
            }
            catch (Exception ex) {
                observedError ??= CreateDispatchError(entry, ex);
                IntegratedS3CoreTelemetry.MarkFailure(activity, observedError);
                logger.LogError(
                    ex,
                    "In-process replica repair dispatch for repair {RepairId} targeting provider {ReplicaBackend} failed unexpectedly.",
                    entry.Id,
                    entry.ReplicaBackendName);

                try {
                    await repairBacklog.MarkFailedAsync(entry.Id, observedError, CancellationToken.None);
                }
                catch (Exception backlogException) {
                    logger.LogError(
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
