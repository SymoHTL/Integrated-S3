using IntegratedS3.Abstractions.Blobs;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Engine;

/// <summary>
/// The engine's background upkeep: garbage collection deletes the blobs that commits dereferenced once their delay
/// has passed, and the orphan sweep walks the blob store for blobs that no commit ever referenced (a crash between
/// writing a body and committing it) and deletes them once they are older than the grace period. It starts with the
/// first metadata transaction and stops when the engine is disposed.
/// </summary>
internal sealed class EngineMaintenance(EngineStorageBackend engine, ILogger? logger) : IAsyncDisposable
{
    internal const string SweepCursorState = "orphan_sweep_cursor";
    private const int GarbageBatchSize = 100;
    private const int SweepPageSize = 1000;

    // ponytail: ten pages per interval sweeps about 29 million blobs a day at the default interval; a larger
    // store needs more pages per round, or one sweep per blob store directory, before its orphans age past a day.
    private const int SweepPagesPerRound = 10;

    private readonly CancellationTokenSource _stopping = new();
    private Task? _loop;
    private int _started;

    public void EnsureStarted()
    {
        if (engine.Options.MaintenanceInterval <= TimeSpan.Zero || Interlocked.Exchange(ref _started, 1) == 1) {
            return;
        }

        _loop = Task.Run(() => RunAsync(_stopping.Token));
    }

    public async ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref _started, 1);
        await _stopping.CancelAsync();
        if (_loop is not null) {
            await _loop;
        }

        _stopping.Dispose();
    }

    /// <summary>
    /// Runs one round: all due garbage, then a few pages of the orphan sweep. A throttling blob store ends the round
    /// early; the next round continues where it stopped.
    /// </summary>
    internal async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        try {
            await CollectGarbageAsync(cancellationToken);
            await SweepOrphansAsync(cancellationToken);
        }
        catch (BlobStoreThrottledException exception) {
            logger?.LogDebug(exception, "Engine maintenance: the blob store is throttling; the round ends early");
        }
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(engine.Options.MaintenanceInterval);
        try {
            do {
                try {
                    await RunOnceAsync(cancellationToken);
                }
                catch (Exception exception) when (exception is not OperationCanceledException) {
                    logger?.LogWarning(exception, "Engine maintenance failed; it runs again in {Interval}", engine.Options.MaintenanceInterval);
                }
            }
            while (await timer.WaitForNextTickAsync(cancellationToken));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            // Disposed.
        }
    }

    private async Task CollectGarbageAsync(CancellationToken cancellationToken)
    {
        while (true) {
            List<string> due;
            await using (var transaction = await engine.BeginAsync(write: false, cancellationToken)) {
                due = await transaction.ListDueGarbageAsync(await transaction.GetClockAsync(cancellationToken), GarbageBatchSize, cancellationToken);
            }

            if (due.Count == 0) {
                return;
            }

            // Delete first, forget after: a crash in between deletes again next round, which is harmless.
            foreach (var locator in due) {
                await engine.Blobs.DeleteAsync(locator, cancellationToken);
            }

            await using (var transaction = await engine.BeginAsync(write: true, cancellationToken)) {
                foreach (var locator in due) {
                    await transaction.ForgetCollectedBlobAsync(locator, cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
            }

            if (due.Count < GarbageBatchSize) {
                return;
            }
        }
    }

    private async Task SweepOrphansAsync(CancellationToken cancellationToken)
    {
        var grace = (long)engine.Options.OrphanGracePeriod.TotalMicroseconds;
        for (var round = 0; round < SweepPagesPerRound; round++) {
            string? cursor;
            await using (var transaction = await engine.BeginAsync(write: false, cancellationToken)) {
                cursor = await transaction.GetStateAsync(SweepCursorState, cancellationToken);
            }

            var page = await engine.Blobs.ListAsync(cursor, SweepPageSize, cancellationToken);
            var claimed = new List<string>();
            await using (var transaction = await engine.BeginAsync(write: true, cancellationToken)) {
                var now = await transaction.GetClockAsync(cancellationToken);
                var known = await transaction.ClassifyBlobsAsync(page.Entries.Select(static entry => entry.Locator), cancellationToken);
                foreach (var entry in page.Entries) {
                    if (known.TryGetValue(entry.Locator, out var swept)) {
                        // Claimed by an earlier round that stopped before its delete: finish it.
                        if (swept) {
                            claimed.Add(entry.Locator);
                        }

                        continue;
                    }

                    // The age comes from the database clock, never from the store's timestamps.
                    var firstSeen = await transaction.NoteOrphanCandidateAsync(entry.Locator, now, cancellationToken);
                    if (now - firstSeen >= grace && await transaction.ClaimOrphanAsync(entry.Locator, cancellationToken)) {
                        claimed.Add(entry.Locator);
                    }
                }

                await transaction.SetStateAsync(SweepCursorState, page.NextCursor, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
            }

            // ponytail: a claimed blob keeps its swept row after the delete, so a commit that arrives later still
            // fails; the rows of such blobs, and the candidate rows of blobs that vanished on their own, are never
            // pruned. Both need a crash or a lost race, so they stay few; prune them when they do not.
            foreach (var locator in claimed) {
                logger?.LogInformation("Engine maintenance: deleting orphaned blob {Locator}", locator);
                await engine.Blobs.DeleteAsync(locator, cancellationToken);
            }

            if (page.NextCursor is null) {
                return;
            }
        }
    }
}
