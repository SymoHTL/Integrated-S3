using IntegratedS3.Abstractions.Models;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Core.Persistence;
using IntegratedS3.Core.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression tests for the transactional atomicity (issue #110) and optimistic-concurrency safety
/// (issue #111) of <c>EntityFrameworkStorageCatalogStore.UpsertObjectAsync</c> and
/// <c>RemoveBucketAsync</c>. They use a file-backed SQLite database so that each catalog operation,
/// which runs in its own service-provider scope / <see cref="DbContext"/>, shares durable state.
/// </summary>
public sealed class EntityFrameworkCatalogAtomicityTests : IDisposable
{
    private readonly string _databasePath =
        Path.Combine(Path.GetTempPath(), "IntegratedS3.CatalogAtomicity", $"{Guid.NewGuid():N}.db");

    public EntityFrameworkCatalogAtomicityTests()
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_databasePath)!);
    }

    public void Dispose()
    {
        // SQLite pooling can keep the file handle open; break the pool then best-effort delete.
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try {
            if (File.Exists(_databasePath)) {
                File.Delete(_databasePath);
            }
        }
        catch (IOException) {
            // The temp file is disposable; ignore a locked-file failure on cleanup.
        }
    }

    private ServiceProvider BuildProvider(bool failNextSaveChanges = false)
    {
        var interceptor = new ThrowOnceSavingInterceptor { Armed = failNextSaveChanges };

        var services = new ServiceCollection();
        services.AddSingleton(interceptor);
        services.AddDbContext<TestCatalogDbContext>(options => options
            .UseSqlite($"Data Source={_databasePath}")
            .AddInterceptors(interceptor));
        services.AddIntegratedS3Core();
        services.AddEntityFrameworkStorageCatalog<TestCatalogDbContext>(options => options.EnsureCreated = true);
        return services.BuildServiceProvider();
    }

    private static ObjectInfo NewObject(string key, string versionId, bool isLatest)
    {
        return new ObjectInfo
        {
            BucketName = "catalog-bucket",
            Key = key,
            VersionId = versionId,
            IsLatest = isLatest,
            ContentLength = 16,
            LastModifiedUtc = DateTimeOffset.Parse("2026-03-01T00:00:00Z")
        };
    }

    [Fact]
    public async Task UpsertObjectAsync_WhenSaveFailsMidOperation_LeavesPreviousLatestUntouched()
    {
        const string key = "docs/report.txt";

        // Seed an initial latest version.
        await using (var seedProvider = BuildProvider()) {
            var seedStore = seedProvider.GetRequiredService<IStorageCatalogStore>();
            await seedStore.UpsertObjectAsync("catalog-disk", NewObject(key, "version-001", isLatest: true));
        }

        // Attempt a second upsert whose SaveChanges is forced to fail *after* the demote statement has run.
        await using (var failingProvider = BuildProvider(failNextSaveChanges: true)) {
            var failingStore = failingProvider.GetRequiredService<IStorageCatalogStore>();
            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await failingStore.UpsertObjectAsync("catalog-disk", NewObject(key, "version-002", isLatest: true)));
        }

        // The transaction must have rolled the demote back: exactly one row, still version-001, still latest.
        await using var verifyProvider = BuildProvider();
        var verifyStore = verifyProvider.GetRequiredService<IStorageCatalogStore>();
        var objects = await verifyStore.ListObjectsAsync("catalog-disk", "catalog-bucket");

        var forKey = objects.Where(o => o.Key == key).ToArray();
        var survivor = Assert.Single(forKey);
        Assert.Equal("version-001", survivor.VersionId);
        Assert.True(survivor.IsLatest, "The pre-existing latest version must remain latest after a rolled-back upsert.");
        Assert.Single(forKey, o => o.IsLatest);
    }

    [Fact]
    public async Task UpsertObjectAsync_ConcurrentWritesToSameKey_LeaveExactlyOneLatest()
    {
        const string key = "docs/hot.txt";

        await using var provider = BuildProvider();
        var store = provider.GetRequiredService<IStorageCatalogStore>();

        // Ensure the bucket + schema exist before the racing writers start, to isolate the race to the
        // demote/insert of the object rows.
        await store.UpsertObjectAsync("catalog-disk", NewObject(key, "seed", isLatest: true));

        const int writers = 12;
        var barrier = new Barrier(writers);
        var tasks = Enumerable.Range(0, writers).Select(i => Task.Run(async () => {
            barrier.SignalAndWait();
            await store.UpsertObjectAsync("catalog-disk", NewObject(key, $"version-{i:D3}", isLatest: true));
        })).ToArray();

        await Task.WhenAll(tasks);

        var objects = await store.ListObjectsAsync("catalog-disk", "catalog-bucket");
        var forKey = objects.Where(o => o.Key == key).ToArray();

        // Every writer's version row must exist, but exactly one may be flagged latest.
        Assert.Single(forKey, o => o.IsLatest);
        Assert.Equal(writers + 1, forKey.Length);
    }

    [Fact]
    public async Task RemoveBucketAsync_RemovesBucketAndObjectsAtomically()
    {
        await using var provider = BuildProvider();
        var store = provider.GetRequiredService<IStorageCatalogStore>();

        await store.UpsertObjectAsync("catalog-disk", NewObject("a.txt", "v1", isLatest: true));
        await store.UpsertObjectAsync("catalog-disk", NewObject("b.txt", "v1", isLatest: true));

        await store.RemoveBucketAsync("catalog-disk", "catalog-bucket");

        var buckets = await store.ListBucketsAsync("catalog-disk");
        Assert.DoesNotContain(buckets, b => b.BucketName == "catalog-bucket");

        var objects = await store.ListObjectsAsync("catalog-disk", "catalog-bucket");
        Assert.Empty(objects);
    }

    [Fact]
    public async Task RemoveBucketAsync_WhenSaveFailsMidOperation_LeavesBucketAndObjectsIntact()
    {
        // Seed a bucket with objects.
        await using (var seedProvider = BuildProvider()) {
            var seedStore = seedProvider.GetRequiredService<IStorageCatalogStore>();
            await seedStore.UpsertObjectAsync("catalog-disk", NewObject("a.txt", "v1", isLatest: true));
            await seedStore.UpsertObjectAsync("catalog-disk", NewObject("b.txt", "v1", isLatest: true));
        }

        // Force the delete to fail; the interceptor throws on the first mutating command in the transaction.
        await using (var failingProvider = BuildProvider(failNextSaveChanges: true)) {
            var failingStore = failingProvider.GetRequiredService<IStorageCatalogStore>();
            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await failingStore.RemoveBucketAsync("catalog-disk", "catalog-bucket"));
        }

        // The whole removal must have rolled back: bucket and both object rows still present.
        await using var verifyProvider = BuildProvider();
        var verifyStore = verifyProvider.GetRequiredService<IStorageCatalogStore>();

        var buckets = await verifyStore.ListBucketsAsync("catalog-disk");
        Assert.Contains(buckets, b => b.BucketName == "catalog-bucket");

        var objects = await verifyStore.ListObjectsAsync("catalog-disk", "catalog-bucket");
        Assert.Equal(2, objects.Count);
    }

    private sealed class TestCatalogDbContext(DbContextOptions<TestCatalogDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.MapIntegratedS3Catalog();
        }
    }

    /// <summary>
    /// A <see cref="ISaveChangesInterceptor"/> / <see cref="IDbCommandInterceptor"/> that throws once on the first
    /// mutating operation after it is armed, simulating a mid-operation crash (dropped connection / timeout) so the
    /// enclosing transaction's rollback behaviour can be asserted.
    /// </summary>
    private sealed class ThrowOnceSavingInterceptor : DbCommandInterceptor
    {
        public bool Armed { get; set; }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
            System.Data.Common.DbCommand command,
            CommandEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            MaybeThrow(command);
            return base.NonQueryExecutingAsync(command, eventData, result, cancellationToken);
        }

        public override InterceptionResult<int> NonQueryExecuting(
            System.Data.Common.DbCommand command,
            CommandEventData eventData,
            InterceptionResult<int> result)
        {
            MaybeThrow(command);
            return base.NonQueryExecuting(command, eventData, result);
        }

        public override ValueTask<InterceptionResult<System.Data.Common.DbDataReader>> ReaderExecutingAsync(
            System.Data.Common.DbCommand command,
            CommandEventData eventData,
            InterceptionResult<System.Data.Common.DbDataReader> result,
            CancellationToken cancellationToken = default)
        {
            MaybeThrow(command);
            return base.ReaderExecutingAsync(command, eventData, result, cancellationToken);
        }

        private void MaybeThrow(System.Data.Common.DbCommand command)
        {
            if (!Armed) {
                return;
            }

            // Only trip on data-mutating statements so schema creation and reads are unaffected.
            var text = command.CommandText;
            if (text.Contains("UPDATE", StringComparison.OrdinalIgnoreCase)
                || text.Contains("INSERT", StringComparison.OrdinalIgnoreCase)
                || text.Contains("DELETE", StringComparison.OrdinalIgnoreCase)) {
                Armed = false;
                throw new InvalidOperationException("Simulated mid-operation failure.");
            }
        }
    }
}
