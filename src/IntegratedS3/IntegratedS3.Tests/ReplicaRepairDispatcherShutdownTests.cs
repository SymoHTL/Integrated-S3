using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression tests for issue #142: the in-process replica-repair dispatcher must track its
/// dispatched tasks, tie them to the dispatcher/host lifetime, and never strand a backlog entry in
/// <see cref="StorageReplicaRepairStatus.InProgress"/> when a repair is interrupted by shutdown.
/// </summary>
[Collection(ObservabilityTestCollection.Name)]
public sealed class ReplicaRepairDispatcherShutdownTests
{
    private static StorageReplicaRepairEntry CreateEntry(string id = "repair-1") => new()
    {
        Id = id,
        Origin = StorageReplicaRepairOrigin.AsyncReplication,
        Status = StorageReplicaRepairStatus.Pending,
        Operation = StorageOperationType.PutObject,
        PrimaryBackendName = "primary",
        ReplicaBackendName = "replica",
        BucketName = "bucket",
        ObjectKey = "key",
        CreatedAtUtc = DateTimeOffset.UnixEpoch,
        UpdatedAtUtc = DateTimeOffset.UnixEpoch
    };

    private static InMemoryStorageReplicaRepairBacklog CreateBacklog()
        => new(TimeProvider.System, NullLogger<InMemoryStorageReplicaRepairBacklog>.Instance);

    private static InProcessStorageReplicaRepairDispatcher CreateDispatcher(
        IStorageReplicaRepairBacklog backlog,
        TimeSpan? drainTimeout = null)
        => new(
            backlog,
            Options.Create(new IntegratedS3CoreOptions()),
            NullLogger<InProcessStorageReplicaRepairDispatcher>.Instance,
            drainTimeout ?? TimeSpan.FromSeconds(10));

    [Fact]
    public async Task RepairInterruptedByShutdown_RevertsBacklogEntryToPending_NotStrandedInProgress()
    {
        var backlog = CreateBacklog();
        await using var dispatcher = CreateDispatcher(backlog);
        var entry = CreateEntry();

        var repairStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // The repair blocks until the dispatcher-supplied token (host application-stopping) is cancelled.
        await dispatcher.DispatchAsync(entry, async ct =>
        {
            repairStarted.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return null; // unreachable: the delay throws on cancellation.
        });

        // Ensure the repair actually began and the entry was marked InProgress before we shut down.
        await repairStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitForStatusAsync(backlog, entry.Id, StorageReplicaRepairStatus.InProgress);

        // Shutdown: cancels the linked token and drains the in-flight repair.
        await dispatcher.DisposeAsync();

        var outstanding = await backlog.ListOutstandingAsync();
        var reverted = Assert.Single(outstanding);
        Assert.Equal(entry.Id, reverted.Id);
        Assert.Equal(StorageReplicaRepairStatus.Pending, reverted.Status);
        Assert.NotEqual(StorageReplicaRepairStatus.InProgress, reverted.Status);
    }

    [Fact]
    public async Task DispatchedTasks_AreTrackedAndAwaitedOnShutdown()
    {
        var backlog = CreateBacklog();
        await using var dispatcher = CreateDispatcher(backlog);
        var entry = CreateEntry();

        var repairStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedCancellation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await dispatcher.DispatchAsync(entry, async ct =>
        {
            repairStarted.TrySetResult();
            try {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException) {
                observedCancellation.TrySetResult();
                throw;
            }

            return null;
        });

        await repairStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // DisposeAsync must not return until the tracked, in-flight dispatch has drained.
        await dispatcher.DisposeAsync();

        // If DisposeAsync had returned without draining, this would still be incomplete.
        Assert.True(observedCancellation.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task SuccessfulRepair_MarksEntryCompleted()
    {
        var backlog = CreateBacklog();
        await using var dispatcher = CreateDispatcher(backlog);
        var entry = CreateEntry();

        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await dispatcher.DispatchAsync(entry, _ =>
        {
            completed.TrySetResult();
            return ValueTask.FromResult<StorageError?>(null);
        });

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Draining also guarantees the task finished before we assert.
        await dispatcher.DisposeAsync();

        var outstanding = await backlog.ListOutstandingAsync();
        Assert.Empty(outstanding); // Completed entries are removed from the in-memory backlog.
    }

    private static async Task WaitForStatusAsync(
        IStorageReplicaRepairBacklog backlog,
        string repairId,
        StorageReplicaRepairStatus expected)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline) {
            var outstanding = await backlog.ListOutstandingAsync();
            var entry = outstanding.FirstOrDefault(e => e.Id == repairId);
            if (entry is not null && entry.Status == expected) {
                return;
            }

            await Task.Delay(10);
        }

        Assert.Fail($"Repair {repairId} did not reach status {expected} within the timeout.");
    }
}
