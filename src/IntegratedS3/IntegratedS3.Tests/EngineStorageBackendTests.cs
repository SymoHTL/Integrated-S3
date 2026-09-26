using System.Text;
using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Engine;
using IntegratedS3.Engine.Metadata;
using IntegratedS3.Testing;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// The engine's own behaviour, below the provider contract: where bodies go, how blob store limits are met, and the
/// background upkeep that deletes blobs. The contract facts run in <see cref="EngineStorageContractTests"/>.
/// </summary>
public sealed class EngineStorageBackendTests
{
    private const string Bucket = "engine-tests";

    [Fact]
    public async Task Put_UpToTheInlineThreshold_StoresNoBlob_AndOneByteMoreStoresOne()
    {
        var store = new InMemoryBlobStore();
        await using var fixture = CreateFixture(store, configure: static options => options.InlineThresholdBytes = 16);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);

        await PutAsync(engine, "inline", new string('i', 16));
        Assert.Equal(0, store.Count);

        await PutAsync(engine, "blob", new string('b', 17));
        Assert.Equal(1, store.Count);

        Assert.Equal(new string('i', 16), await GetTextAsync(engine, "inline"));
        Assert.Equal(new string('b', 17), await GetTextAsync(engine, "blob"));
    }

    [Fact]
    public async Task Put_LargerThanTheStoresMaxBlobSize_IsSplit_AndRangesAcrossBlobsReadBack_WithoutRangeReads()
    {
        var store = new InMemoryBlobStore(new InMemoryBlobStoreOptions
        {
            MaxBlobSize = 64 * 1024,
            SupportsRangeReads = false
        });
        await using var fixture = CreateFixture(store, configure: static options => options.InlineThresholdBytes = 0);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);
        var body = new byte[200_000];
        new Random(288).NextBytes(body);

        RequireSuccess(await engine.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = "split",
            Content = new MemoryStream(body)
        }));

        Assert.Equal(4, store.Count);
        Assert.Equal(body, await GetBytesAsync(engine, "split"));
        Assert.Equal(body[65_530..131_081], await GetBytesAsync(engine, "split", new ObjectRange { Start = 65_530, End = 131_080 }));
        Assert.Equal(body[196_700..], await GetBytesAsync(engine, "split", new ObjectRange { Start = 196_700, End = 199_999 }));
    }

    [Fact]
    public async Task BlobStoreThrottling_AnswersSlowDown503_WhileInlineObjectsStillRead()
    {
        var store = new HookedBlobStore(new InMemoryBlobStore());
        await using var fixture = CreateFixture(store, configure: static options => options.InlineThresholdBytes = 16);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);
        await PutAsync(engine, "inline", "small");
        await PutAsync(engine, "blob", new string('b', 100));

        store.Throttle = true;

        var put = RequireFailure(await engine.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = "throttled",
            Content = new MemoryStream(new byte[100])
        }), StorageErrorCode.Throttled);
        Assert.Equal(503, put.SuggestedHttpStatusCode);

        var get = RequireFailure(await engine.GetObjectAsync(new GetObjectRequest
        {
            BucketName = Bucket,
            Key = "blob"
        }), StorageErrorCode.Throttled);
        Assert.Equal(503, get.SuggestedHttpStatusCode);

        Assert.Equal("small", await GetTextAsync(engine, "inline"));
        RequireFailure(await engine.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = Bucket,
            Key = "throttled"
        }), StorageErrorCode.ObjectNotFound);
    }

    [Fact]
    public async Task GarbageCollection_DeletesAReplacedBlob_OnlyAfterItsDelay()
    {
        var store = new InMemoryBlobStore();
        var time = new ManualTimeProvider();
        await using var fixture = CreateFixture(store, time, static options => options.InlineThresholdBytes = 0);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);
        await PutAsync(engine, "key", "first");
        await PutAsync(engine, "key", "second");
        Assert.Equal(2, store.Count);

        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromMinutes(59));
        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        Assert.Equal(2, store.Count);

        time.Advance(TimeSpan.FromMinutes(2));
        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        Assert.Equal(1, store.Count);
        Assert.Equal("second", await GetTextAsync(engine, "key"));
    }

    [Fact]
    public async Task OrphanSweep_DeletesAnUnreferencedBlob_OnlyAfterTheGracePeriod_AndNeverAReferencedOne()
    {
        var store = new InMemoryBlobStore();
        var time = new ManualTimeProvider();
        await using var fixture = CreateFixture(store, time, static options => options.InlineThresholdBytes = 0);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);
        await PutAsync(engine, "a", "object a");
        await PutAsync(engine, "b", "object b");
        var orphan = await WriteBlobAsync(store, "left by a crash");

        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromHours(23));
        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        Assert.True(await ExistsAsync(store, orphan));

        time.Advance(TimeSpan.FromHours(2));
        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        Assert.False(await ExistsAsync(store, orphan));

        for (var round = 0; round < 3; round++) {
            time.Advance(TimeSpan.FromDays(2));
            await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        }

        Assert.Equal(2, store.Count);
        Assert.Equal("object a", await GetTextAsync(engine, "a"));
        Assert.Equal("object b", await GetTextAsync(engine, "b"));
    }

    [Fact]
    public async Task OrphanSweep_OverABlobStoreItsDatabaseDoesNotKnow_DeletesNothing()
    {
        var store = new InMemoryBlobStore();
        var time = new ManualTimeProvider();
        await using var fixture = CreateFixture(store, time, static options => {
            options.InlineThresholdBytes = 0;
            options.OrphanGracePeriod = TimeSpan.Zero;
        });
        var engine = Engine(fixture);

        // A walk over an empty store proves nothing about blobs that turn up later.
        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        for (var index = 0; index < 5; index++) {
            await WriteBlobAsync(store, $"another deployment's blob {index}");
        }

        for (var round = 0; round < 3; round++) {
            time.Advance(TimeSpan.FromDays(1));
            await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        }

        Assert.Equal(5, store.Count);

        // New writes do not arm the sweep while the unknown blobs outnumber the known ones.
        await CreateBucketAsync(engine);
        await PutAsync(engine, "new", "written through this database");
        for (var round = 0; round < 3; round++) {
            time.Advance(TimeSpan.FromDays(1));
            await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        }

        Assert.Equal(6, store.Count);
    }

    [Fact]
    public async Task OrphanSweep_DropsTheCandidateRowOfABlobThatWentAway()
    {
        var store = new InMemoryBlobStore();
        await using var fixture = CreateFixture(store, configure: static options => options.InlineThresholdBytes = 0);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);
        await PutAsync(engine, "kept", "referenced");
        var orphan = await WriteBlobAsync(store, "gone before its grace period ends");

        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        Assert.Equal(1, await CountRowsAsync(fixture, "orphan_candidates"));

        await store.DeleteAsync(orphan);
        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        Assert.Equal(0, await CountRowsAsync(fixture, "orphan_candidates"));
    }

    [Fact]
    public async Task OrphanSweep_DropsTheCandidateRowOfABlobWhoseUploadCommitted()
    {
        var store = new HookedBlobStore(new InMemoryBlobStore());
        await using var fixture = CreateFixture(store, configure: static options => options.InlineThresholdBytes = 0);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);
        await PutAsync(engine, "first", "referenced before the slow upload");
        await engine.Maintenance.RunOnceAsync(CancellationToken.None);

        // The sweep walks the store while an upload's blob is written and not yet committed.
        store.AfterWrite = async _ => await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        await PutAsync(engine, "slow", "a slow upload");
        store.AfterWrite = null;
        Assert.Equal(1, await CountRowsAsync(fixture, "orphan_candidates"));

        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        Assert.Equal(0, await CountRowsAsync(fixture, "orphan_candidates"));
        Assert.Equal(2, store.Count);
        Assert.Equal("a slow upload", await GetTextAsync(engine, "slow"));
    }

    [Fact]
    public async Task Commit_OfABlobTheSweepClaimed_FailsWithSlowDown_AndStoresNothing()
    {
        var store = new HookedBlobStore(new InMemoryBlobStore());
        await using var fixture = CreateFixture(store, configure: static options => options.InlineThresholdBytes = 0);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);

        // The upload outlived the grace period: the sweep claimed its blob between the write and the commit.
        store.AfterWrite = async locator => {
            await using var transaction = await engine.BeginAsync(write: true, CancellationToken.None);
            Assert.True(await transaction.ClaimOrphanAsync(locator, CancellationToken.None));
            await transaction.CommitAsync();
        };

        var error = RequireFailure(await engine.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = "late",
            Content = new MemoryStream("too slow"u8.ToArray())
        }), StorageErrorCode.Throttled);
        Assert.Equal(503, error.SuggestedHttpStatusCode);

        RequireFailure(await engine.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = Bucket,
            Key = "late"
        }), StorageErrorCode.ObjectNotFound);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task ClaimOrphan_OfABlobACommitReferenced_ClaimsNothing()
    {
        var store = new InMemoryBlobStore();
        await using var fixture = CreateFixture(store);
        var engine = Engine(fixture);
        var locator = await WriteBlobAsync(store, "committed first");

        await using (var transaction = await engine.BeginAsync(write: true, CancellationToken.None)) {
            Assert.True(await transaction.ReferenceBlobsAsync([locator], CancellationToken.None));
            await transaction.CommitAsync();
        }

        await using (var transaction = await engine.BeginAsync(write: true, CancellationToken.None)) {
            Assert.False(await transaction.ClaimOrphanAsync(locator, CancellationToken.None));
            await transaction.CommitAsync();
        }

        await using (var transaction = await engine.BeginAsync(write: false, CancellationToken.None)) {
            Assert.Equal(new Dictionary<string, bool> { [locator] = false }, await transaction.ClassifyBlobsAsync([locator], CancellationToken.None));
        }
    }

    [Fact]
    public async Task OrphanSweep_FinishesAClaimWhoseDeleteNeverRan()
    {
        var store = new InMemoryBlobStore();
        await using var fixture = CreateFixture(store);
        var engine = Engine(fixture);
        var locator = await WriteBlobAsync(store, "claimed, then the node stopped");
        await using (var transaction = await engine.BeginAsync(write: true, CancellationToken.None)) {
            Assert.True(await transaction.ClaimOrphanAsync(locator, CancellationToken.None));
            await transaction.CommitAsync();
        }

        await engine.Maintenance.RunOnceAsync(CancellationToken.None);

        Assert.False(await ExistsAsync(store, locator));
    }

    [Fact]
    public async Task DeleteBucket_WithAnUploadInProgress_Succeeds_AndCollectsItsParts()
    {
        var store = new InMemoryBlobStore();
        var time = new ManualTimeProvider();
        await using var fixture = CreateFixture(store, time);
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);
        var upload = RequireSuccess(await engine.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = Bucket,
            Key = "unfinished"
        }));
        RequireSuccess(await engine.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = Bucket,
            Key = "unfinished",
            UploadId = upload.UploadId,
            PartNumber = 1,
            Content = new MemoryStream("part one"u8.ToArray())
        }));
        Assert.Equal(1, store.Count);

        RequireSuccess(await engine.DeleteBucketAsync(new DeleteBucketRequest { BucketName = Bucket }));
        time.Advance(TimeSpan.FromHours(2));
        await engine.Maintenance.RunOnceAsync(CancellationToken.None);
        Assert.Equal(0, store.Count);

        await CreateBucketAsync(engine);
        Assert.Empty(await engine.ListMultipartUploadsAsync(new ListMultipartUploadsRequest { BucketName = Bucket }).ToArrayAsync());
        RequireFailure(await engine.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = Bucket,
            Key = "unfinished",
            UploadId = upload.UploadId,
            PartNumber = 2,
            Content = new MemoryStream("part two"u8.ToArray())
        }), StorageErrorCode.NoSuchUpload);
    }

    [Fact]
    public async Task Restart_KeepsObjectsVersionsTagsAndUploads()
    {
        await using var fixture = CreateFixture(configure: static options => options.InlineThresholdBytes = 16);
        var engine = Engine(fixture);
        RequireSuccess(await engine.CreateBucketAsync(new CreateBucketRequest { BucketName = Bucket, EnableVersioning = true }));
        var first = await PutAsync(engine, "doc", "first version, stored in a blob");
        await PutAsync(engine, "doc", "second");
        RequireSuccess(await engine.PutObjectTagsAsync(new PutObjectTagsRequest
        {
            BucketName = Bucket,
            Key = "doc",
            Tags = new Dictionary<string, string> { ["state"] = "kept" }
        }));
        var upload = RequireSuccess(await engine.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = Bucket,
            Key = "upload"
        }));
        var part = RequireSuccess(await engine.UploadMultipartPartAsync(new UploadMultipartPartRequest
        {
            BucketName = Bucket,
            Key = "upload",
            UploadId = upload.UploadId,
            PartNumber = 1,
            Content = new MemoryStream("uploaded before the restart"u8.ToArray())
        }));

        await fixture.RestartAsync();
        engine = Engine(fixture);

        Assert.Equal("second", await GetTextAsync(engine, "doc"));
        Assert.Equal("first version, stored in a blob", await GetTextAsync(engine, "doc", versionId: first.VersionId));
        var tags = RequireSuccess(await engine.GetObjectTagsAsync(new GetObjectTagsRequest { BucketName = Bucket, Key = "doc" }));
        Assert.Equal("kept", tags.Tags["state"]);
        RequireSuccess(await engine.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = Bucket,
            Key = "upload",
            UploadId = upload.UploadId,
            Parts = [part]
        }));
        Assert.Equal("uploaded before the restart", await GetTextAsync(engine, "upload"));
    }

    [Fact]
    public async Task Open_ADatabaseWithANewerSchema_IsRefused()
    {
        await using var fixture = CreateFixture();
        await CreateBucketAsync(Engine(fixture));
        await fixture.RestartAsync();

        await using (var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = DatabasePath(fixture), Pooling = false }.ToString())) {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "UPDATE engine_schema SET version = version + 1;";
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Engine(fixture).HeadBucketAsync(Bucket));
        Assert.Contains($"schema version {SqliteMetadataStore.SchemaVersion + 1}", refused.Message);
    }

    [Fact]
    public async Task ListObjectVersions_ResumesAfterAVersionIdMarkerWhoseVersionWasDeleted()
    {
        await using var fixture = CreateFixture();
        var engine = Engine(fixture);
        RequireSuccess(await engine.CreateBucketAsync(new CreateBucketRequest { BucketName = Bucket, EnableVersioning = true }));
        var versions = new List<string>();
        for (var index = 1; index <= 4; index++) {
            versions.Add((await PutAsync(engine, "key", $"version {index}")).VersionId!);
        }

        await PutAsync(engine, "later", "a later key");

        var firstPage = await engine.ListObjectVersionsAsync(new ListObjectVersionsRequest { BucketName = Bucket, PageSize = 2 })
            .Select(static version => version.VersionId!).ToArrayAsync();
        Assert.Equal([versions[3], versions[2]], firstPage);

        RequireSuccess(await engine.DeleteObjectAsync(new DeleteObjectRequest { BucketName = Bucket, Key = "key", VersionId = versions[2] }));

        var rest = await engine.ListObjectVersionsAsync(new ListObjectVersionsRequest
        {
            BucketName = Bucket,
            KeyMarker = "key",
            VersionIdMarker = versions[2]
        }).Select(static version => (version.Key, version.VersionId)).ToArrayAsync();
        Assert.Equal(("key", versions[1]), rest[0]);
        Assert.Equal(("key", versions[0]), rest[1]);
        Assert.Equal("later", rest[2].Key);
        Assert.Equal(3, rest.Length);
    }

    [Fact]
    public async Task Put_OfAKeyThatIsNotValidUnicode_IsRejectedWithInvalidArgument()
    {
        await using var fixture = CreateFixture();
        var engine = Engine(fixture);
        await CreateBucketAsync(engine);
        const string loneSurrogate = "bad\uD800key";

        var put = RequireFailure(await engine.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = loneSurrogate,
            Content = new MemoryStream("never stored"u8.ToArray())
        }), StorageErrorCode.InvalidArgument);
        Assert.Equal(400, put.SuggestedHttpStatusCode);
        RequireFailure(await engine.HeadObjectAsync(new HeadObjectRequest { BucketName = Bucket, Key = loneSurrogate }), StorageErrorCode.InvalidArgument);
        Assert.Empty(await engine.ListObjectsAsync(new ListObjectsRequest { BucketName = Bucket }).ToArrayAsync());
    }

    // ----- Helpers -----

    private static EngineStorageFixture CreateFixture(IBlobStore? store = null, TimeProvider? time = null, Action<IntegratedS3EngineOptions>? configure = null)
        => new(services => {
            if (store is not null) {
                services.AddSingleton(store);
            }

            if (time is not null) {
                services.AddSingleton(time);
            }
        }, configure);

    private static EngineStorageBackend Engine(EngineStorageFixture fixture)
        => Assert.IsType<EngineStorageBackend>(fixture.Backend);

    private static string DatabasePath(EngineStorageFixture fixture) => Path.Combine(fixture.RootPath, "metadata.db");

    private static async Task CreateBucketAsync(EngineStorageBackend engine)
        => RequireSuccess(await engine.CreateBucketAsync(new CreateBucketRequest { BucketName = Bucket }));

    private static async Task<ObjectInfo> PutAsync(EngineStorageBackend engine, string key, string text)
        => RequireSuccess(await engine.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = key,
            Content = new MemoryStream(Encoding.UTF8.GetBytes(text))
        }));

    private static async Task<string> GetTextAsync(EngineStorageBackend engine, string key, string? versionId = null)
        => Encoding.UTF8.GetString(await GetBytesAsync(engine, key, versionId: versionId));

    private static async Task<byte[]> GetBytesAsync(EngineStorageBackend engine, string key, ObjectRange? range = null, string? versionId = null)
    {
        await using var response = RequireSuccess(await engine.GetObjectAsync(new GetObjectRequest
        {
            BucketName = Bucket,
            Key = key,
            Range = range,
            VersionId = versionId
        }));
        using var buffer = new MemoryStream();
        await response.Content.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private static async Task<string> WriteBlobAsync(IBlobStore store, string text)
        => (await store.WriteAsync(new MemoryStream(Encoding.UTF8.GetBytes(text)))).Locator;

    private static async Task<bool> ExistsAsync(IBlobStore store, string locator)
    {
        try {
            await using var stream = await store.OpenReadAsync(locator);
            return true;
        }
        catch (BlobNotFoundException) {
            return false;
        }
    }

    private static async Task<long> CountRowsAsync(EngineStorageFixture fixture, string table)
    {
        await using var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = DatabasePath(fixture), Pooling = false }.ToString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT count(*) FROM {table};";
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static T RequireSuccess<T>(StorageResult<T> result)
    {
        Assert.True(result.IsSuccess, result.Error?.Message);
        return result.Value!;
    }

    private static void RequireSuccess(StorageResult result) => Assert.True(result.IsSuccess, result.Error?.Message);

    private static StorageError RequireFailure(StorageResult result, StorageErrorCode code)
    {
        Assert.False(result.IsSuccess);
        Assert.Equal(code, result.Error!.Code);
        return result.Error;
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 9, 26, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    /// <summary>
    /// An in-memory store that can be told to throttle every call, and that runs a hook after each write.
    /// </summary>
    private sealed class HookedBlobStore(InMemoryBlobStore inner) : IBlobStore
    {
        public bool Throttle { get; set; }

        public Func<string, Task>? AfterWrite { get; set; }

        public BlobStoreCapabilities Capabilities => inner.Capabilities;

        public int Count => inner.Count;

        public async ValueTask<BlobWriteResult> WriteAsync(Stream content, long? length = null, CancellationToken cancellationToken = default)
        {
            ThrowIfThrottled();
            var written = await inner.WriteAsync(content, length, cancellationToken);
            if (AfterWrite is { } hook) {
                await hook(written.Locator);
            }

            return written;
        }

        public ValueTask<Stream> OpenReadAsync(string locator, long offset = 0, long? length = null, CancellationToken cancellationToken = default)
        {
            ThrowIfThrottled();
            return inner.OpenReadAsync(locator, offset, length, cancellationToken);
        }

        public ValueTask DeleteAsync(string locator, CancellationToken cancellationToken = default)
        {
            ThrowIfThrottled();
            return inner.DeleteAsync(locator, cancellationToken);
        }

        public ValueTask<BlobListPage> ListAsync(string? cursor, int maxEntries, CancellationToken cancellationToken = default)
        {
            ThrowIfThrottled();
            return inner.ListAsync(cursor, maxEntries, cancellationToken);
        }

        private void ThrowIfThrottled()
        {
            if (Throttle) {
                throw new BlobStoreThrottledException("Throttled by the test.", TimeSpan.FromMilliseconds(1));
            }
        }
    }
}
