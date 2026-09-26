using IntegratedS3.Abstractions.Blobs;
using Xunit;

namespace IntegratedS3.Testing;

/// <summary>
/// Reusable xUnit contract tests for <see cref="IBlobStore"/> implementations. Derive from this class, return
/// a new empty store from <see cref="CreateStoreAsync"/> and another instance over its storage from
/// <see cref="ReopenAsync"/>, and run it in your CI. Every call retries when the store throttles, as the engine
/// does, so a store that throttles still passes. Each fact asserts on both sides of a capability, so no fact
/// passes without checking something. The suite writes forward-only streams, as the engine does.
/// </summary>
public abstract class BlobStoreContractTests
{
    private const int MaxThrottledAttempts = 50;
    private const int MaxListPages = 100;

    /// <summary>
    /// Creates the store under test. Each test calls it once and expects an empty store.
    /// </summary>
    /// <returns>A new, empty store.</returns>
    protected abstract ValueTask<IBlobStore> CreateStoreAsync();

    /// <summary>
    /// Opens another instance over the storage of <paramref name="store"/>, as another node or a restarted process
    /// opens it. A store whose storage is the instance itself returns <paramref name="store"/>.
    /// </summary>
    /// <param name="store">A store <see cref="CreateStoreAsync"/> returned.</param>
    /// <returns>Another instance over the same storage.</returns>
    protected abstract ValueTask<IBlobStore> ReopenAsync(IBlobStore store);

    /// <summary>
    /// Verifies that a blob reads back byte for byte, and that the write reports the number of bytes stored.
    /// </summary>
    /// <param name="size">The blob size in bytes.</param>
    /// <param name="declareLength">Whether the write declares the length; the engine does not always know it.</param>
    [Theory]
    [InlineData(0, true)]
    [InlineData(1, true)]
    [InlineData(100_003, true)]
    [InlineData(0, false)]
    [InlineData(100_003, false)]
    public async Task BlobStoreContract_Write_ThenRead_ReturnsTheSameBytes(int size, bool declareLength)
    {
        var store = await CreateStoreAsync();
        var bytes = CreateBytes(size, seed: size);

        var written = await WriteAsync(store, bytes, declareLength);

        Assert.Equal(size, written.Length);
        Assert.False(string.IsNullOrEmpty(written.Locator));
        Assert.Equal(bytes, await ReadAllAsync(store, written.Locator));
    }

    /// <summary>
    /// Verifies that a blob is readable through another instance of the store as soon as the write returns: a
    /// store never acknowledges a write it has only buffered.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_Write_IsReadableThroughAnotherInstance_AsSoonAsItReturns()
    {
        var store = await CreateStoreAsync();
        var bytes = CreateBytes(5000, seed: 21);
        var written = await WriteAsync(store, bytes);

        var other = await ReopenAsync(store);

        Assert.Equal(bytes, await ReadAllAsync(other, written.Locator));
    }

    /// <summary>
    /// Verifies that another instance of the store resumes a listing from a cursor the first one returned, as the
    /// orphan sweep does after a restart.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_List_ResumesFromACursor_InAnotherInstance()
    {
        var store = await CreateStoreAsync();
        var written = new List<string>();
        for (var index = 0; index < 5; index++) {
            written.Add((await WriteAsync(store, CreateBytes(10, seed: 300 + index))).Locator);
        }

        var first = await RetryAsync(() => store.ListAsync(null, maxEntries: 2).AsTask());
        Assert.NotNull(first.NextCursor);
        var seen = first.Entries.Select(static entry => entry.Locator).ToList();
        var other = await ReopenAsync(store);
        var cursor = first.NextCursor;
        var pages = 1;
        while (cursor is not null) {
            var page = await RetryAsync(() => other.ListAsync(cursor, maxEntries: 2).AsTask());
            seen.AddRange(page.Entries.Select(static entry => entry.Locator));
            cursor = page.NextCursor;
            Assert.True(++pages <= MaxListPages, "The listing did not end.");
        }

        Assert.Equal(written.Order(StringComparer.Ordinal), seen.Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// Verifies that blobs are write-once: two writes of the same bytes get two locators, and neither a later
    /// write nor deleting one blob changes what another locator reads.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_EveryWriteGetsItsOwnLocator_AndLaterWritesAndDeletesLeaveOtherBlobsUnchanged()
    {
        var store = await CreateStoreAsync();
        var first = CreateBytes(4096, seed: 1);

        var original = await WriteAsync(store, first);
        var sameBytes = await WriteAsync(store, first);
        var other = await WriteAsync(store, CreateBytes(4096, seed: 2));

        Assert.Equal(3, new[] { original.Locator, sameBytes.Locator, other.Locator }.Distinct(StringComparer.Ordinal).Count());

        await DeleteAsync(store, sameBytes.Locator);

        Assert.Equal(first, await ReadAllAsync(store, original.Locator));
        Assert.Equal(CreateBytes(4096, seed: 2), await ReadAllAsync(store, other.Locator));
    }

    /// <summary>
    /// Verifies that concurrent writes each get a distinct locator that reads back their own bytes.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_ConcurrentWrites_EachReadBackTheirOwnBytes()
    {
        var store = await CreateStoreAsync();
        var payloads = Enumerable.Range(0, 32).Select(static index => CreateBytes(1000 + index, seed: index)).ToArray();

        // Task.Run, so that a store whose writes complete synchronously is still called from several threads at once.
        var written = await Task.WhenAll(payloads.Select(payload => Task.Run(() => WriteAsync(store, payload))));

        Assert.Equal(payloads.Length, written.Select(static result => result.Locator).Distinct(StringComparer.Ordinal).Count());
        for (var index = 0; index < payloads.Length; index++) {
            Assert.Equal(payloads[index], await ReadAllAsync(store, written[index].Locator));
        }
    }

    /// <summary>
    /// Verifies byte ranges. A store that serves ranges returns exactly the requested bytes, clipped at the end
    /// of the blob. A store that does not still serves the whole blob from its start.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_RangeRead_ReturnsExactlyTheRequestedBytes_OrTheWholeBlobWithoutRangeSupport()
    {
        var store = await CreateStoreAsync();
        var bytes = CreateBytes(10_000, seed: 7);
        var written = await WriteAsync(store, bytes);

        Assert.Equal(bytes, await ReadAllAsync(store, written.Locator));
        if (!store.Capabilities.SupportsRangeReads) {
            return;
        }

        Assert.Equal(bytes[..100], await ReadAllAsync(store, written.Locator, 0, 100));
        Assert.Equal(bytes[1234..5678], await ReadAllAsync(store, written.Locator, 1234, 5678 - 1234));
        Assert.Equal(bytes[9990..], await ReadAllAsync(store, written.Locator, 9990, null));
        Assert.Equal(bytes[9990..], await ReadAllAsync(store, written.Locator, 9990, 500));
        Assert.Empty(await ReadAllAsync(store, written.Locator, 10_000, null));
        Assert.Empty(await ReadAllAsync(store, written.Locator, 20_000, null));
        Assert.Empty(await ReadAllAsync(store, written.Locator, 20_000, 5));
        Assert.Empty(await ReadAllAsync(store, written.Locator, 500, 0));
    }

    /// <summary>
    /// Verifies the size limit. A store with <see cref="BlobStoreCapabilities.MaxBlobSize"/> accepts a blob of
    /// exactly that size and refuses one byte more, with or without a declared length. A store without a limit
    /// accepts a blob larger than any limit the constrained test store uses. The fact holds the blob in memory, so
    /// it checks limits up to <see cref="Array.MaxLength"/> - 1 bytes, the largest limit whose one byte more still
    /// fits in an array; a store whose own limit is larger declares a smaller one, which only makes the engine split
    /// a body into more blobs.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_Write_HonoursMaxBlobSize()
    {
        var store = await CreateStoreAsync();

        if (store.Capabilities.MaxBlobSize is not { } max) {
            var large = CreateBytes(9 * 1024 * 1024, seed: 3);
            var written = await WriteAsync(store, large);
            Assert.Equal(large.Length, written.Length);
            Assert.Equal(large, await ReadAllAsync(store, written.Locator));
            return;
        }

        Assert.InRange(max, 1, Array.MaxLength - 1);
        var exact = CreateBytes((int)max, seed: 4);
        var accepted = await WriteAsync(store, exact);
        Assert.Equal(max, accepted.Length);

        var tooLarge = CreateBytes((int)max + 1, seed: 5);
        await Assert.ThrowsAnyAsync<ArgumentException>(() => WriteAsync(store, tooLarge, declareLength: false));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => WriteAsync(store, tooLarge, declareLength: true));
    }

    /// <summary>
    /// Verifies that reading a deleted blob, or any string the store never issued as a locator, throws
    /// <see cref="BlobNotFoundException"/>.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_OpenRead_DeletedOrNeverIssuedLocator_ThrowsBlobNotFound()
    {
        var store = await CreateStoreAsync();
        var written = await WriteAsync(store, CreateBytes(64, seed: 6));
        await DeleteAsync(store, written.Locator);

        foreach (var locator in new[] { written.Locator, "not-a-locator", "../outside", "..\\outside", written.Locator + "x" }) {
            await Assert.ThrowsAsync<BlobNotFoundException>(() => RetryAsync(() => store.OpenReadAsync(locator).AsTask()));
        }
    }

    /// <summary>
    /// Verifies that deleting is idempotent: a second delete, and a delete of a locator the store never issued,
    /// both succeed.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_Delete_IsIdempotent()
    {
        var store = await CreateStoreAsync();
        var written = await WriteAsync(store, CreateBytes(64, seed: 8));

        await DeleteAsync(store, written.Locator);
        await DeleteAsync(store, written.Locator);
        await DeleteAsync(store, "not-a-locator");

        await Assert.ThrowsAsync<BlobNotFoundException>(() => RetryAsync(() => store.OpenReadAsync(written.Locator).AsTask()));
    }

    /// <summary>
    /// Verifies that retrying a delete through another instance, as the engine does after a restart, removes nothing
    /// else: the other blob still reads back byte for byte. A store that keeps several blobs in one container tracks
    /// which of them are live, so the retry is a no-op.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_Delete_RetriedThroughAnotherInstance_LeavesTheOtherBlobUnchanged()
    {
        var store = await CreateStoreAsync();
        var deleted = await WriteAsync(store, CreateBytes(3000, seed: 40));
        var keptBytes = CreateBytes(3000, seed: 41);
        var kept = await WriteAsync(store, keptBytes);

        await DeleteAsync(store, deleted.Locator);
        var other = await ReopenAsync(store);
        await DeleteAsync(other, deleted.Locator);

        Assert.Equal(keptBytes, await ReadAllAsync(store, kept.Locator));
        Assert.Equal(keptBytes, await ReadAllAsync(other, kept.Locator));
        await Assert.ThrowsAsync<BlobNotFoundException>(() => RetryAsync(() => other.OpenReadAsync(deleted.Locator).AsTask()));
    }

    /// <summary>
    /// Verifies the listing: walking it page by page from a cursor returns every stored blob exactly once, with
    /// its length, and leaves out deleted blobs. Pages may be shorter than asked, even empty before the end, as a
    /// store that lists a sparse backing collection returns them.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_List_WalksEveryBlobExactlyOnce_AcrossPagesAndCursors()
    {
        var store = await CreateStoreAsync();
        var written = new List<BlobWriteResult>();
        for (var index = 0; index < 7; index++) {
            written.Add(await WriteAsync(store, CreateBytes(100 + index, seed: 100 + index)));
        }

        var deleted = written[3];
        await DeleteAsync(store, deleted.Locator);
        written.RemoveAt(3);

        var seen = new List<BlobListEntry>();
        string? cursor = null;
        var pages = 0;
        do {
            var page = await RetryAsync(() => store.ListAsync(cursor, maxEntries: 3).AsTask());
            Assert.True(page.Entries.Count <= 3, $"A page asked for at most 3 entries returned {page.Entries.Count}.");
            seen.AddRange(page.Entries);
            cursor = page.NextCursor;
            Assert.True(++pages <= MaxListPages, "The listing did not end.");
        }
        while (cursor is not null);

        Assert.Equal(
            written.Select(static blob => blob.Locator).Order(StringComparer.Ordinal),
            seen.Select(static entry => entry.Locator).Order(StringComparer.Ordinal));
        Assert.DoesNotContain(seen, entry => entry.Locator == deleted.Locator);
        foreach (var entry in seen) {
            Assert.Equal(written.Single(blob => blob.Locator == entry.Locator).Length, entry.Length);
        }
    }

    /// <summary>
    /// Verifies that an empty store lists nothing, and that its listing ends.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_List_OfAnEmptyStore_IsEmptyAndComplete()
    {
        var store = await CreateStoreAsync();

        string? cursor = null;
        var pages = 0;
        do {
            var page = await RetryAsync(() => store.ListAsync(cursor, maxEntries: 10).AsTask());
            Assert.Empty(page.Entries);
            cursor = page.NextCursor;
            Assert.True(++pages <= MaxListPages, "The listing did not end.");
        }
        while (cursor is not null);
    }

    /// <summary>
    /// Verifies the listing guarantee the orphan sweep relies on: a blob that exists for the whole walk appears
    /// exactly once, even when blobs the walk has already returned are deleted between pages (the last one too,
    /// which a cursor that names a locator points at), and a deleted blob does not appear again.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_List_ABlobThatExistsForTheWholeWalk_AppearsExactlyOnce_WhenSeenBlobsAreDeletedMidWalk()
    {
        var store = await CreateStoreAsync();
        var written = new List<string>();
        for (var index = 0; index < 7; index++) {
            written.Add((await WriteAsync(store, CreateBytes(50, seed: 200 + index))).Locator);
        }

        var seen = new List<string>();
        string? cursor = null;
        string? deletedMidWalk = null;
        var pages = 0;
        do {
            var page = await RetryAsync(() => store.ListAsync(cursor, maxEntries: 2).AsTask());
            seen.AddRange(page.Entries.Select(static entry => entry.Locator));
            cursor = page.NextCursor;
            if (deletedMidWalk is null && seen.Count > 0 && cursor is not null) {
                // The first blob seen, and the last one, which a cursor that names a locator points at: the sweep
                // deletes both kinds between pages.
                deletedMidWalk = seen[0];
                await DeleteAsync(store, deletedMidWalk);
                if (seen[^1] != deletedMidWalk) {
                    await DeleteAsync(store, seen[^1]);
                }
            }

            Assert.True(++pages <= MaxListPages, "The listing did not end.");
        }
        while (cursor is not null);

        Assert.NotNull(deletedMidWalk);
        Assert.Equal(written.Order(StringComparer.Ordinal), seen.Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// Verifies that strings close to a live blob's locator, which the store never issued, name no blob, an
    /// uppercase copy included, and that the live blob still reads back.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_OpenRead_VariantsOfALiveLocator_ThrowBlobNotFound()
    {
        var store = await CreateStoreAsync();
        var bytes = CreateBytes(64, seed: 11);
        var live = await WriteAsync(store, bytes);

        var variants = new[] { live.Locator.ToUpperInvariant(), live.Locator + "x", live.Locator[..^1], live.Locator + "/" }
            .Where(variant => variant.Length > 0 && !string.Equals(variant, live.Locator, StringComparison.Ordinal))
            .Distinct(StringComparer.Ordinal);
        foreach (var variant in variants) {
            await Assert.ThrowsAsync<BlobNotFoundException>(() => RetryAsync(() => store.OpenReadAsync(variant).AsTask()));
        }

        Assert.Equal(bytes, await ReadAllAsync(store, live.Locator));
    }

    /// <summary>
    /// Creates deterministic test bytes.
    /// </summary>
    /// <param name="size">The number of bytes.</param>
    /// <param name="seed">The seed; equal seeds and sizes give equal bytes.</param>
    /// <returns>The bytes.</returns>
    protected static byte[] CreateBytes(int size, int seed)
    {
        var bytes = new byte[size];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }

    /// <summary>
    /// Writes <paramref name="bytes"/> as a new blob, retrying while the store throttles.
    /// </summary>
    /// <param name="store">The store.</param>
    /// <param name="bytes">The bytes to write.</param>
    /// <param name="declareLength">Whether to pass the length to the store.</param>
    /// <returns>The write result.</returns>
    protected static Task<BlobWriteResult> WriteAsync(IBlobStore store, byte[] bytes, bool declareLength = true)
        => RetryAsync(async () => {
            // The engine hands a store a forward-only stream (a request body under its digest wrappers) and keeps
            // using it afterwards, so the suite does the same.
            var content = new ForwardOnlyStream(bytes);
            var result = await store.WriteAsync(content, declareLength ? bytes.Length : null);
            Assert.False(content.Disposed, "The store disposed the caller's content stream.");
            return result;
        });

    /// <summary>
    /// Deletes a blob, retrying while the store throttles.
    /// </summary>
    /// <param name="store">The store.</param>
    /// <param name="locator">The blob's locator.</param>
    /// <returns>A task that completes when the blob is deleted.</returns>
    protected static Task DeleteAsync(IBlobStore store, string locator)
        => RetryAsync(async () => {
            await store.DeleteAsync(locator);
            return true;
        });

    /// <summary>
    /// Reads a blob, or a range of it, to its end, retrying while the store throttles.
    /// </summary>
    /// <param name="store">The store.</param>
    /// <param name="locator">The blob's locator.</param>
    /// <param name="offset">The first byte.</param>
    /// <param name="length">The number of bytes, or <see langword="null"/> for all.</param>
    /// <returns>The bytes read.</returns>
    protected static Task<byte[]> ReadAllAsync(IBlobStore store, string locator, long offset = 0, long? length = null)
        => RetryAsync(async () => {
            await using var stream = await store.OpenReadAsync(locator, offset, length);
            using var buffer = new MemoryStream();
            await stream.CopyToAsync(buffer);
            return buffer.ToArray();
        });

    /// <summary>
    /// Runs <paramref name="operation"/>, retrying with a short backoff while it throws
    /// <see cref="BlobStoreThrottledException"/>, as the engine does.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="operation">The operation.</param>
    /// <returns>The operation's result.</returns>
    protected static async Task<T> RetryAsync<T>(Func<Task<T>> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        for (var attempt = 1; ; attempt++) {
            try {
                return await operation();
            }
            catch (BlobStoreThrottledException exception) when (attempt < MaxThrottledAttempts) {
                await Task.Delay(exception.RetryAfter ?? TimeSpan.FromMilliseconds(attempt));
            }
        }
    }

    // A read-only stream with no length, no position and no seeking, which records whether it was disposed.
    private sealed class ForwardOnlyStream(byte[] bytes) : Stream
    {
        private readonly MemoryStream _inner = new(bytes, writable: false);

        public bool Disposed { get; private set; }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(buffer, cancellationToken);

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }
}
