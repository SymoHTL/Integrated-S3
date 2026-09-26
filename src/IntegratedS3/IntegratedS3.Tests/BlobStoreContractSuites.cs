using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Engine.Blobs;
using IntegratedS3.Testing;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class LocalDiskBlobStoreContractTests : BlobStoreContractTests, IDisposable
{
    // The store's root sits one level down, so a test can put a file beside it that no locator may reach.
    private readonly string _parentPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Tests", "blobs-" + Guid.NewGuid().ToString("N"));

    private string RootPath => Path.Combine(_parentPath, "root");

    protected override ValueTask<IBlobStore> CreateStoreAsync()
        => ValueTask.FromResult<IBlobStore>(new LocalDiskBlobStore(RootPath));

    protected override ValueTask<IBlobStore> ReopenAsync(IBlobStore store)
        => ValueTask.FromResult<IBlobStore>(new LocalDiskBlobStore(RootPath));

    // The contract's never-issued locators are malformed for this store; a well-formed one has no directories yet.
    [Fact]
    public async Task Delete_OfAWellFormedLocatorWhoseDirectoriesDoNotExist_Succeeds()
    {
        const string locator = "0123456789abcdef0123456789abcdef";
        var store = new LocalDiskBlobStore(RootPath);

        await store.DeleteAsync(locator);

        await Assert.ThrowsAsync<BlobNotFoundException>(() => store.OpenReadAsync(locator).AsTask());
    }

    // root/ab/cd/<locator> climbs four levels to the root's parent, where the target exists: without the
    // locator check this reads and deletes it.
    [Fact]
    public async Task EscapingLocators_NeitherReadNorDeleteAFileOutsideTheRoot()
    {
        var store = new LocalDiskBlobStore(RootPath);
        var outside = Path.Combine(_parentPath, "outside.txt");
        await File.WriteAllTextAsync(outside, "outside");

        foreach (var locator in new[] { "abcd/../../../../outside.txt", "abcd\\..\\..\\..\\..\\outside.txt" }) {
            await Assert.ThrowsAsync<BlobNotFoundException>(() => store.OpenReadAsync(locator).AsTask());
            await store.DeleteAsync(locator);
            Assert.Equal("outside", await File.ReadAllTextAsync(outside));
        }

        // No separator at all: root/../../<locator> is a file in the root's grandparent when the locator starts
        // with four dots.
        var dotted = "...." + Guid.NewGuid().ToString("N")[..28];
        var grandparentFile = Path.Combine(Path.GetDirectoryName(_parentPath)!, dotted);
        await File.WriteAllTextAsync(grandparentFile, "outside");
        try {
            await Assert.ThrowsAsync<BlobNotFoundException>(() => store.OpenReadAsync(dotted).AsTask());
            await store.DeleteAsync(dotted);
            Assert.Equal("outside", await File.ReadAllTextAsync(grandparentFile));
        }
        finally {
            File.Delete(grandparentFile);
        }
    }

    [Fact]
    public async Task Listing_WalksBothDirectoryLevelsInLocatorOrder_PinsTheCursors_AndSkipsStrayEntries()
    {
        var store = new LocalDiskBlobStore(RootPath);
        string[] locators =
        [
            "aa000000000000000000000000000001",
            "aa000000000000000000000000000002",
            "aa010000000000000000000000000000",
            "ab000000000000000000000000000000",
            "ff00ffffffffffffffffffffffffffff",
        ];
        foreach (var locator in locators) {
            Plant(Path.Combine(locator[..2], locator[2..4], locator));
        }

        // What a shared directory collects; none of it is a blob.
        Plant("desktop.ini");
        Plant(Path.Combine("lost+found", "aa", "aa000000000000000000000000000003"));
        Plant(Path.Combine("aa", "00", "abc"));
        Plant(Path.Combine("aa", "00", ".nfs0000000000000001"));
        Plant(Path.Combine("aa", "00", "aa00000000000000000000000000000z"));
        Plant(Path.Combine("aa", "00", "bb000000000000000000000000000000"));

        var expectedCursors = new Dictionary<int, string?[]>
        {
            [1] = [locators[0], locators[1], locators[2], locators[3], locators[4], null],
            [2] = [locators[1], locators[3], null],
            [5] = [locators[4], null],
        };
        foreach (var (pageSize, cursors) in expectedCursors) {
            var seen = new List<string>();
            var seenCursors = new List<string?>();
            string? cursor = null;
            do {
                var page = await store.ListAsync(cursor, pageSize);
                seen.AddRange(page.Entries.Select(static entry => entry.Locator));
                cursor = page.NextCursor;
                seenCursors.Add(cursor);
            }
            while (cursor is not null && seenCursors.Count < 20);

            Assert.Equal(locators, seen);
            Assert.Equal(cursors, seenCursors);
        }
    }

    // Garbage collection deletes a blob that a slow reader may still stream (FileShare.Delete on Windows).
    [Fact]
    public async Task Delete_WhileAReaderStreams_Succeeds_AndTheReaderStillGetsEveryByte()
    {
        var store = new LocalDiskBlobStore(RootPath);
        var bytes = CreateBytes(200_000, seed: 5);
        var written = await store.WriteAsync(new MemoryStream(bytes), bytes.Length);

        await using var reader = await store.OpenReadAsync(written.Locator);
        var head = new byte[1000];
        await reader.ReadExactlyAsync(head);

        await store.DeleteAsync(written.Locator);

        await Assert.ThrowsAsync<BlobNotFoundException>(() => store.OpenReadAsync(written.Locator).AsTask());
        Assert.Empty((await store.ListAsync(null, 10)).Entries);
        using var rest = new MemoryStream();
        await reader.CopyToAsync(rest);
        Assert.Equal(bytes, head.Concat(rest.ToArray()).ToArray());
    }

    [Fact]
    public async Task SyncReads_OfARange_ReturnExactlyTheRange()
    {
        var store = new LocalDiskBlobStore(RootPath);
        var bytes = CreateBytes(10_000, seed: 6);
        var written = await store.WriteAsync(new MemoryStream(bytes), bytes.Length);

        using var stream = await store.OpenReadAsync(written.Locator, 1234, 4444);
        using var copy = new MemoryStream();
        stream.CopyTo(copy);

        Assert.Equal(bytes[1234..5678], copy.ToArray());
    }

    // The byte[] overloads, sync and async, write at the offset into the caller's buffer.
    [Fact]
    public async Task ByteArrayReads_OfARange_FillTheCallersBufferFromItsOffset()
    {
        var store = new LocalDiskBlobStore(RootPath);
        var bytes = CreateBytes(10_000, seed: 9);
        var written = await store.WriteAsync(new MemoryStream(bytes), bytes.Length);

        using var sync = await store.OpenReadAsync(written.Locator, 1234, 4444);
        await using var asynchronous = await store.OpenReadAsync(written.Locator, 1234, 4444);
        var syncBuffer = new byte[7 + 4444];
        var asyncBuffer = new byte[7 + 4444];
        for (int total = 0, read; total < 4444; total += read) {
            read = sync.Read(syncBuffer, 7 + total, 4444 - total);
            Assert.True(read > 0);
        }

        for (int total = 0, read; total < 4444; total += read) {
            read = await asynchronous.ReadAsync(asyncBuffer, 7 + total, 4444 - total);
            Assert.True(read > 0);
        }

        Assert.Equal(bytes[1234..5678], syncBuffer[7..]);
        Assert.Equal(bytes[1234..5678], asyncBuffer[7..]);
    }

    [Fact]
    public async Task Write_CancelledMidStream_ThrowsAndLeavesNothingListed()
    {
        var store = new LocalDiskBlobStore(RootPath);
        using var cts = new CancellationTokenSource();
        var content = new CancellingStream(new byte[300_000], cancelAfterBytes: 100_000, cts);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.WriteAsync(content, null, cts.Token).AsTask());

        Assert.Empty((await store.ListAsync(null, 10)).Entries);
    }

    // A client that goes away mid-body: the write fails, and its partial file is gone before the sweep's grace period.
    [Fact]
    public async Task Write_WhoseContentFailsMidStream_ThrowsAndLeavesNothingListed()
    {
        var store = new LocalDiskBlobStore(RootPath);

        await Assert.ThrowsAsync<IOException>(() => store.WriteAsync(new FailingStream(new byte[300_000], failAfterBytes: 100_000), null).AsTask());

        Assert.Empty((await store.ListAsync(null, 10)).Entries);
    }

    // CreateNew fails on an existing file, as a locator collision would, and that file is another write's blob.
    [Fact]
    public async Task Write_ToALiveBlobsLocator_Throws_AndLeavesThatBlobUnchanged()
    {
        var store = new LocalDiskBlobStore(RootPath);
        var bytes = CreateBytes(1000, seed: 12);
        var live = await store.WriteAsync(new MemoryStream(bytes), bytes.Length);

        await Assert.ThrowsAnyAsync<IOException>(() => store.WriteToAsync(live.Locator, new MemoryStream(CreateBytes(10, seed: 13)), CancellationToken.None).AsTask());

        await using var stream = await store.OpenReadAsync(live.Locator);
        using var copy = new MemoryStream();
        await stream.CopyToAsync(copy);
        Assert.Equal(bytes, copy.ToArray());
    }

    // A deleted or unmounted root is not an empty store: every call throws an IOException that names it, and a
    // write does not create it again.
    [Fact]
    public async Task EveryCall_WithTheRootGone_ThrowsAnIOExceptionNamingIt_AndAWriteDoesNotCreateIt()
    {
        var store = new LocalDiskBlobStore(RootPath);
        var written = await store.WriteAsync(new MemoryStream([1, 2, 3]), 3);
        var root = Path.GetFullPath(RootPath);
        Directory.Delete(root, recursive: true);

        await AssertRootMissingAsync(() => store.OpenReadAsync(written.Locator).AsTask());
        await AssertRootMissingAsync(() => store.OpenReadAsync("not-a-locator").AsTask());
        await AssertRootMissingAsync(() => store.DeleteAsync(written.Locator).AsTask());
        await AssertRootMissingAsync(() => store.DeleteAsync("not-a-locator").AsTask());
        await AssertRootMissingAsync(() => store.ListAsync(null, 10).AsTask());
        await AssertRootMissingAsync(() => store.WriteAsync(new MemoryStream([4, 5, 6]), 3).AsTask());

        Assert.False(Directory.Exists(root));

        async Task AssertRootMissingAsync(Func<Task> call)
        {
            var exception = await Assert.ThrowsAsync<IOException>(call);
            Assert.Contains(root, exception.Message, StringComparison.Ordinal);
        }
    }

    // Directory creation races: 512 writes from two instances into one empty root.
    [Fact]
    public async Task ConcurrentWriters_OnTwoStoreInstances_OverOneRoot_AllReadBack()
    {
        var first = new LocalDiskBlobStore(RootPath, flushToDisk: false);
        var second = new LocalDiskBlobStore(RootPath, flushToDisk: false);
        var payloads = Enumerable.Range(0, 512).Select(static index => BitConverter.GetBytes(index)).ToArray();

        var written = await Task.WhenAll(payloads.Select((payload, index) => Task.Run(() =>
            (index % 2 == 0 ? first : second).WriteAsync(new MemoryStream(payload), payload.Length).AsTask())));

        for (var index = 0; index < payloads.Length; index++) {
            await using var stream = await first.OpenReadAsync(written[index].Locator);
            using var copy = new MemoryStream();
            await stream.CopyToAsync(copy);
            Assert.Equal(payloads[index], copy.ToArray());
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(_parentPath)) {
            Directory.Delete(_parentPath, recursive: true);
        }
    }

    private void Plant(string relativePath)
    {
        var path = Path.Combine(RootPath, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllBytes(path, [1, 2, 3]);
    }

    private sealed class CancellingStream(byte[] data, int cancelAfterBytes, CancellationTokenSource cts) : MemoryStream(data, writable: false)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var read = await base.ReadAsync(buffer, cancellationToken);
            if (Position >= cancelAfterBytes) {
                await cts.CancelAsync();
            }

            return read;
        }
    }

    private sealed class FailingStream(byte[] data, int failAfterBytes) : MemoryStream(data, writable: false)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Position >= failAfterBytes) {
                throw new IOException("The client went away.");
            }

            return await base.ReadAsync(buffer, cancellationToken);
        }
    }
}

public sealed class InMemoryBlobStoreContractTests : BlobStoreContractTests
{
    protected override ValueTask<IBlobStore> CreateStoreAsync()
        => ValueTask.FromResult<IBlobStore>(new InMemoryBlobStore());

    // An in-memory store's storage is the instance itself.
    protected override ValueTask<IBlobStore> ReopenAsync(IBlobStore store) => ValueTask.FromResult(store);
}

public sealed class ConstrainedInMemoryBlobStoreContractTests : BlobStoreContractTests
{
    protected override ValueTask<IBlobStore> CreateStoreAsync()
        => ValueTask.FromResult<IBlobStore>(InMemoryBlobStore.CreateConstrained());

    // An in-memory store's storage is the instance itself.
    protected override ValueTask<IBlobStore> ReopenAsync(IBlobStore store) => ValueTask.FromResult(store);
}
