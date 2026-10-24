using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Engine.Metadata;
using static IntegratedS3.Shared.ObjectChecksums;

namespace IntegratedS3.Engine.Blobs;

/// <summary>
/// An object body as the engine stored it: inline bytes when small, otherwise a manifest of whole blobs.
/// </summary>
internal sealed class StoredBody
{
    public required long Length { get; init; }

    public byte[]? InlineData { get; init; }

    public IReadOnlyList<Extent> Manifest { get; init; } = [];

    /// <summary>
    /// Gets the digests of the body, MD5 always among them.
    /// </summary>
    public required IReadOnlyDictionary<string, string> Checksums { get; init; }

    public IEnumerable<string> Locators => Manifest.Select(static extent => extent.Locator);
}

internal static class BodyWriter
{
    /// <summary>
    /// Reads <paramref name="body"/> to its end, computing its digests on the way, and stores it: inline when it is
    /// at most <paramref name="inlineThreshold"/> bytes, otherwise as blobs no larger than the store accepts. No
    /// metadata is written; on failure the blobs written so far are deleted.
    /// </summary>
    public static async Task<StoredBody> WriteAsync(IBlobStore store, Stream body, ChecksumAlgorithms algorithms, int inlineThreshold, CancellationToken cancellationToken)
    {
        using var source = new BodySource(body, algorithms);
        var head = new byte[inlineThreshold + 1];
        var headLength = await source.ReadAtMostAsync(head, cancellationToken);
        if (headLength <= inlineThreshold) {
            return new StoredBody
            {
                Length = headLength,
                InlineData = head.AsSpan(0, headLength).ToArray(),
                Checksums = source.CompleteChecksums()
            };
        }

        source.Unread(head.AsMemory(0, headLength));
        var maxBlobSize = store.Capabilities.MaxBlobSize is > 0 and var limit ? limit : long.MaxValue;
        var manifest = new List<Extent>();
        long length = 0;
        try {
            do {
                // ponytail: the length hint stays unset because a body's declared length is not verified here;
                // pass it once a store needs it (an S3 store would buffer or use multipart without it). A throttled
                // write is not retried: it may have consumed part of a body that cannot be replayed, so the client
                // gets SlowDown instead.
                await using var chunk = new ChunkStream(source, maxBlobSize);
                var written = await store.WriteAsync(chunk, length: null, cancellationToken);
                if (written.Length != chunk.BytesRead) {
                    manifest.Add(new Extent(written.Locator, 0, written.Length));
                    throw new InvalidOperationException(
                        $"The blob store reported {written.Length} bytes for a blob it was given {chunk.BytesRead} bytes of.");
                }

                manifest.Add(new Extent(written.Locator, 0, written.Length));
                length += written.Length;
            }
            while (await source.HasMoreAsync(cancellationToken));
        }
        catch {
            await DeleteQuietlyAsync(store, manifest.Select(static extent => extent.Locator));
            throw;
        }

        return new StoredBody { Length = length, Manifest = manifest, Checksums = source.CompleteChecksums() };
    }

    /// <summary>
    /// Deletes blobs that no committed row references, ignoring failures: the orphan sweep reclaims what is left.
    /// </summary>
    public static async Task DeleteQuietlyAsync(IBlobStore store, IEnumerable<string> locators)
    {
        foreach (var locator in locators) {
            try {
                await store.DeleteAsync(locator, CancellationToken.None);
            }
            catch (Exception exception) when (exception is not OutOfMemoryException) {
                // Left for the orphan sweep.
            }
        }
    }

}

/// <summary>
/// The request body, read once, hashed as it is read, with bytes that can be pushed back.
/// </summary>
internal sealed class BodySource : IDisposable
{
    private readonly Stream _body;
    private ChecksumComputation _checksums;
    private ReadOnlyMemory<byte> _pending;
    private bool _ended;

    public BodySource(Stream body, ChecksumAlgorithms algorithms)
    {
        _body = body;
        _checksums = new ChecksumComputation(algorithms);
    }

    public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (buffer.IsEmpty) {
            return 0;
        }

        if (!_pending.IsEmpty) {
            var count = Math.Min(buffer.Length, _pending.Length);
            _pending[..count].CopyTo(buffer);
            _pending = _pending[count..];
            return count;
        }

        if (_ended) {
            return 0;
        }

        var read = await _body.ReadAsync(buffer, cancellationToken);
        if (read == 0) {
            _ended = true;
            return 0;
        }

        _checksums.Append(buffer.Span[..read]);
        return read;
    }

    public async ValueTask<int> ReadAtMostAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var total = 0;
        while (total < buffer.Length) {
            var read = await ReadAsync(buffer[total..], cancellationToken);
            if (read == 0) {
                break;
            }

            total += read;
        }

        return total;
    }

    /// <summary>
    /// Pushes bytes back to be read again. Only valid when nothing is pending.
    /// </summary>
    public void Unread(ReadOnlyMemory<byte> bytes)
    {
        if (!_pending.IsEmpty) {
            throw new InvalidOperationException("Bytes are already pending.");
        }

        _pending = bytes;
    }

    public async ValueTask<bool> HasMoreAsync(CancellationToken cancellationToken)
    {
        if (!_pending.IsEmpty) {
            return true;
        }

        var next = new byte[1];
        if (await ReadAsync(next, cancellationToken) == 0) {
            return false;
        }

        _pending = next;
        return true;
    }

    /// <summary>
    /// Returns the digests of everything read. Call once, after the body has ended.
    /// </summary>
    public IReadOnlyDictionary<string, string> CompleteChecksums() => _checksums.ToDictionary();

    public void Dispose() => _checksums.Dispose();
}

/// <summary>
/// A read-only view of at most <c>limit</c> bytes of a <see cref="BodySource"/>: the content of one blob.
/// </summary>
internal sealed class ChunkStream(BodySource source, long limit) : Stream
{
    public long BytesRead { get; private set; }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => BytesRead;
        set => throw new NotSupportedException();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var allowed = (int)Math.Min(buffer.Length, limit - BytesRead);
        if (allowed <= 0) {
            return 0;
        }

        var read = await source.ReadAsync(buffer[..allowed], cancellationToken);
        BytesRead += read;
        return read;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override int Read(byte[] buffer, int offset, int count)
        => ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
