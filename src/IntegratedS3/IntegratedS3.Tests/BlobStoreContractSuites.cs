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

    [Fact]
    public async Task Write_CancelledMidStream_ThrowsAndLeavesNothingListed()
    {
        var store = new LocalDiskBlobStore(RootPath);
        using var cts = new CancellationTokenSource();
        var content = new CancellingStream(new byte[300_000], cancelAfterBytes: 100_000, cts);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.WriteAsync(content, null, cts.Token).AsTask());

        Assert.Empty((await store.ListAsync(null, 10)).Entries);
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
}

public sealed class InMemoryBlobStoreContractTests : BlobStoreContractTests
{
    protected override ValueTask<IBlobStore> CreateStoreAsync()
        => ValueTask.FromResult<IBlobStore>(new InMemoryBlobStore());
}

public sealed class ConstrainedInMemoryBlobStoreContractTests : BlobStoreContractTests
{
    protected override ValueTask<IBlobStore> CreateStoreAsync()
        => ValueTask.FromResult<IBlobStore>(InMemoryBlobStore.CreateConstrained());
}
