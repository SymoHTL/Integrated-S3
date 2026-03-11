using System.Collections.Concurrent;
using System.Diagnostics;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Core.Options;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Core.Services;

internal sealed class InProcessStorageReplicaRepairDispatcher(
    IStorageReplicaRepairBacklog repairBacklog,
    IOptions<IntegratedS3CoreOptions> options) : IStorageReplicaRepairDispatcher
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _dispatchLocks = new(StringComparer.Ordinal);

    public async ValueTask DispatchAsync(
        StorageReplicaRepairEntry entry,
        Func<CancellationToken, ValueTask<StorageError?>> repairOperation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(repairOperation);

        await repairBacklog.AddAsync(entry, cancellationToken);
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
        await dispatchLock.WaitAsync(CancellationToken.None);
        try {
            try {
                await repairBacklog.MarkInProgressAsync(entry.Id, CancellationToken.None);

                StorageError? error;
                try {
                    error = await repairOperation(CancellationToken.None);
                }
                catch (Exception ex) {
                    error = CreateDispatchError(entry, ex);
                }

                if (error is null) {
                    await repairBacklog.MarkCompletedAsync(entry.Id, CancellationToken.None);
                    return;
                }

                await repairBacklog.MarkFailedAsync(entry.Id, error, CancellationToken.None);
            }
            catch (Exception ex) {
                Trace.TraceError(
                    "In-process replica repair dispatch for repair '{0}' targeting provider '{1}' failed unexpectedly: {2}",
                    entry.Id,
                    entry.ReplicaBackendName,
                    ex);

                try {
                    await repairBacklog.MarkFailedAsync(entry.Id, CreateDispatchError(entry, ex), CancellationToken.None);
                }
                catch (Exception backlogException) {
                    Trace.TraceError(
                        "Failed to mark replica repair '{0}' as failed after an unexpected dispatch exception: {1}",
                        entry.Id,
                        backlogException);
                }
            }
        }
        finally {
            dispatchLock.Release();
        }
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
