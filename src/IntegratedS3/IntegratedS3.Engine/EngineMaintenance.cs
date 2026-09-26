using System.Globalization;
using IntegratedS3.Abstractions.Blobs;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Engine;

/// <summary>
/// The engine's background upkeep: garbage collection deletes the blobs that commits dereferenced once their delay
/// has passed, and the orphan sweep walks the blob store for blobs that no commit ever referenced (a crash between
/// writing a body and committing it) and deletes them once they are older than the grace period, unless the store
/// looks like it belongs to another database. It starts with the first metadata transaction and stops when the
/// engine is disposed.
/// </summary>
internal sealed class EngineMaintenance(EngineStorageBackend engine, ILogger? logger) : IAsyncDisposable
{
    internal const string SweepCursorState = "orphan_sweep_cursor";
    internal const string SweepWalkState = "orphan_sweep_walk";
    internal const string SweepLastWalkState = "orphan_sweep_last_walk";
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

                await transaction.CommitAsync();
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
                var walk = SweepWalk.Parse(await transaction.GetStateAsync(SweepWalkState, cancellationToken));
                var last = SweepWalk.Parse(await transaction.GetStateAsync(SweepLastWalkState, cancellationToken));

                var mayClaim = last.AllowsClaims;
                var known = await transaction.ClassifyBlobsAsync(page.Entries.Select(static entry => entry.Locator), cancellationToken);
                foreach (var entry in page.Entries) {
                    if (known.TryGetValue(entry.Locator, out var swept)) {
                        if (swept) {
                            // Claimed by an earlier round that stopped before its delete: finish it.
                            claimed.Add(entry.Locator);
                        }
                        else {
                            walk = walk with { Referenced = walk.Referenced + 1 };
                        }

                        continue;
                    }

                    // The age comes from the database clock, never from the store's timestamps.
                    walk = walk with { Unreferenced = walk.Unreferenced + 1 };
                    var firstSeen = await transaction.NoteOrphanCandidateAsync(entry.Locator, now, walk.Number, cancellationToken);
                    if (mayClaim && now - firstSeen >= grace && await transaction.ClaimOrphanAsync(entry.Locator, cancellationToken)) {
                        claimed.Add(entry.Locator);
                    }
                }

                if (page.NextCursor is null) {
                    await transaction.DropStaleOrphanCandidatesAsync(walk.Number, cancellationToken);
                    await transaction.SetStateAsync(SweepLastWalkState, walk.ToString(), cancellationToken);
                    await transaction.SetStateAsync(SweepWalkState, new SweepWalk(walk.Number + 1, 0, 0).ToString(), cancellationToken);
                    if (walk.Unreferenced > 0 && !walk.AllowsClaims) {
                        logger?.LogWarning(
                            "Engine maintenance: the orphan sweep deletes nothing in its next walk. This walk found {Unreferenced} blobs that no row references and {Referenced} that are referenced; a metadata database that does not belong to this blob store looks like that. Check the configured paths.",
                            walk.Unreferenced,
                            walk.Referenced);
                    }
                }
                else {
                    await transaction.SetStateAsync(SweepWalkState, walk.ToString(), cancellationToken);
                }

                await transaction.SetStateAsync(SweepCursorState, page.NextCursor, cancellationToken);
                await transaction.CommitAsync();
            }

            // ponytail: a claimed blob keeps its swept row after the delete, so a commit that arrives later still
            // fails, and the row is never pruned. Only a crash or a lost race makes one, so they stay few; prune
            // them when they do not.
            foreach (var locator in claimed) {
                logger?.LogInformation("Engine maintenance: deleting orphaned blob {Locator}", locator);
                await engine.Blobs.DeleteAsync(locator, cancellationToken);
            }

            if (page.NextCursor is null) {
                return;
            }
        }
    }

    /// <summary>
    /// One walk of the orphan sweep over the whole blob store: its number, and how many referenced and unreferenced
    /// blobs it found so far. Stored in <c>engine_state</c> as three numbers.
    /// </summary>
    private readonly record struct SweepWalk(long Number, long Referenced, long Unreferenced)
    {
        /// <summary>
        /// Gets whether the next walk may claim orphans. A metadata database that does not belong to the blob store
        /// (fresh, another deployment's, restored from an old backup) sees mostly blobs it does not know, so a walk
        /// that found no referenced blob, or more unreferenced blobs than referenced ones, stops the claims.
        /// </summary>
        public bool AllowsClaims => Referenced > 0 && Unreferenced <= Referenced;

        public static SweepWalk Parse(string? value)
        {
            if (value?.Split(' ') is not [var number, var referenced, var unreferenced]) {
                return default;
            }

            return new SweepWalk(
                long.Parse(number, CultureInfo.InvariantCulture),
                long.Parse(referenced, CultureInfo.InvariantCulture),
                long.Parse(unreferenced, CultureInfo.InvariantCulture));
        }

        public override string ToString()
            => string.Create(CultureInfo.InvariantCulture, $"{Number} {Referenced} {Unreferenced}");
    }
}
