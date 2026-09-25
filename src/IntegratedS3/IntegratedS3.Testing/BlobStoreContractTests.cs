using IntegratedS3.Abstractions.Blobs;
using Xunit;

namespace IntegratedS3.Testing;

/// <summary>
/// Reusable xUnit contract tests for <see cref="IBlobStore"/> implementations. Derive from this class, return
/// a new empty store from <see cref="CreateStoreAsync"/>, and run it in your CI. Every call retries when the
/// store throttles, as the engine does, so a store that throttles still passes. Each fact asserts on both
/// sides of a capability, so no fact passes without checking something.
/// </summary>
public abstract class BlobStoreContractTests
{
    private const int MaxThrottledAttempts = 50;

    /// <summary>
    /// Creates the store under test. Each test calls it once and expects an empty store.
    /// </summary>
    /// <returns>A new, empty store.</returns>
    protected abstract ValueTask<IBlobStore> CreateStoreAsync();

    /// <summary>
    /// Verifies that a blob reads back byte for byte, and that the write reports the number of bytes stored.
    /// </summary>
    /// <param name="size">The blob size in bytes.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100_003)]
    public async Task BlobStoreContract_Write_ThenRead_ReturnsTheSameBytes(int size)
    {
        var store = await CreateStoreAsync();
        var bytes = CreateBytes(size, seed: size);

        var written = await WriteAsync(store, bytes);

        Assert.Equal(size, written.Length);
        Assert.False(string.IsNullOrEmpty(written.Locator));
        Assert.Equal(bytes, await ReadAllAsync(store, written.Locator));
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

        var written = await Task.WhenAll(payloads.Select(payload => WriteAsync(store, payload)));

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
        Assert.Empty(await ReadAllAsync(store, written.Locator, 500, 0));
    }

    /// <summary>
    /// Verifies the size limit. A store with <see cref="BlobStoreCapabilities.MaxBlobSize"/> accepts a blob of
    /// exactly that size and refuses one byte more, with or without a declared length. A store without a limit
    /// accepts a blob larger than any limit the constrained test store uses.
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

        Assert.InRange(max, 1, int.MaxValue - 1);
        var exact = CreateBytes((int)max, seed: 4);
        var accepted = await WriteAsync(store, exact);
        Assert.Equal(max, accepted.Length);

        var tooLarge = CreateBytes((int)max + 1, seed: 5);
        await Assert.ThrowsAnyAsync<ArgumentException>(() => WriteAsync(store, tooLarge, declareLength: false));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => WriteAsync(store, tooLarge, declareLength: true));
    }

    /// <summary>
    /// Verifies that reading a deleted blob, or any string the store never issued as a locator, throws
    /// <see cref="BlobNotFoundException"/>, and never reads anything outside the store.
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
    /// Verifies the listing: walking it page by page from a cursor returns every stored blob exactly once, with
    /// its length and creation time, and leaves out deleted blobs. Pages may be shorter than asked.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_List_WalksEveryBlobExactlyOnce_AcrossPagesAndCursors()
    {
        var store = await CreateStoreAsync();
        var before = DateTimeOffset.UtcNow.AddMinutes(-5);
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
            Assert.True(page.Entries.Count > 0 || page.NextCursor is null, "A page with no entries must end the listing.");
            seen.AddRange(page.Entries);
            cursor = page.NextCursor;
            Assert.True(++pages <= 20, "The listing did not end.");
        }
        while (cursor is not null);

        Assert.Equal(
            written.Select(static blob => blob.Locator).Order(StringComparer.Ordinal),
            seen.Select(static entry => entry.Locator).Order(StringComparer.Ordinal));
        Assert.DoesNotContain(seen, entry => entry.Locator == deleted.Locator);
        foreach (var entry in seen) {
            Assert.Equal(written.Single(blob => blob.Locator == entry.Locator).Length, entry.Length);
            Assert.InRange(entry.CreatedUtc, before, DateTimeOffset.UtcNow.AddMinutes(5));
        }
    }

    /// <summary>
    /// Verifies that an empty store lists nothing and ends the listing at once.
    /// </summary>
    [Fact]
    public async Task BlobStoreContract_List_OfAnEmptyStore_IsEmptyAndComplete()
    {
        var store = await CreateStoreAsync();

        var page = await RetryAsync(() => store.ListAsync(null, maxEntries: 10).AsTask());

        Assert.Empty(page.Entries);
        Assert.Null(page.NextCursor);
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
        => RetryAsync(() => store.WriteAsync(new MemoryStream(bytes, writable: false), declareLength ? bytes.Length : null).AsTask());

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
}
