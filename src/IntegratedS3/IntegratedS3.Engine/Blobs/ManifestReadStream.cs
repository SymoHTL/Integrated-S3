using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Engine.Metadata;

namespace IntegratedS3.Engine.Blobs;

/// <summary>
/// A forward-only stream over a byte range of an object whose bytes are the concatenation of its manifest's
/// extents. Each blob is opened when the read reaches it. A store without range reads is read from the start of
/// the blob, skipping to the range.
/// </summary>
internal sealed class ManifestReadStream : Stream
{
    private readonly IBlobStore _store;
    private readonly IReadOnlyList<Extent> _extents;
    private readonly long _end;
    private long _position;
    private int _extentIndex;
    private long _extentStart;
    private Stream? _current;
    private long _currentRemaining;

    private ManifestReadStream(IBlobStore store, IReadOnlyList<Extent> extents, long start, long end)
    {
        _store = store;
        _extents = extents;
        _position = start;
        _end = end;
    }

    /// <summary>
    /// Opens bytes [<paramref name="start"/>, <paramref name="start"/> + <paramref name="length"/>) of the object.
    /// The first blob is opened before this returns, so a missing blob or a throttled store surfaces here, before
    /// a response has started.
    /// </summary>
    public static async Task<Stream> OpenAsync(IBlobStore store, IReadOnlyList<Extent> extents, long start, long length, CancellationToken cancellationToken)
    {
        var stream = new ManifestReadStream(store, extents, start, start + length);
        try {
            if (length > 0) {
                await stream.OpenCurrentAsync(cancellationToken);
            }
        }
        catch {
            await stream.DisposeAsync();
            throw;
        }

        return stream;
    }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.IsEmpty || _position >= _end) {
            return 0;
        }

        if (_current is null) {
            await OpenCurrentAsync(cancellationToken);
        }

        var read = await _current!.ReadAsync(buffer[..(int)Math.Min(buffer.Length, _currentRemaining)], cancellationToken);
        if (read == 0) {
            throw new IOException($"Blob '{_extents[_extentIndex].Locator}' ended before its manifest said it would.");
        }

        _position += read;
        _currentRemaining -= read;
        if (_currentRemaining == 0) {
            await _current.DisposeAsync();
            _current = null;
            _extentStart += _extents[_extentIndex].Length;
            _extentIndex++;
        }

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

    public override async ValueTask DisposeAsync()
    {
        if (_current is not null) {
            await _current.DisposeAsync();
            _current = null;
        }

        await base.DisposeAsync();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) {
            _current?.Dispose();
            _current = null;
        }

        base.Dispose(disposing);
    }

    private async Task OpenCurrentAsync(CancellationToken cancellationToken)
    {
        // Skip the extents that end at or before the position.
        while (_extentIndex < _extents.Count && _extentStart + _extents[_extentIndex].Length <= _position) {
            _extentStart += _extents[_extentIndex].Length;
            _extentIndex++;
        }

        if (_extentIndex == _extents.Count) {
            throw new IOException("The object's manifest is shorter than its recorded size.");
        }

        var extent = _extents[_extentIndex];
        var within = _position - _extentStart;
        var count = Math.Min(extent.Length - within, _end - _position);
        var offset = extent.Offset + within;

        if (_store.Capabilities.SupportsRangeReads) {
            _current = await _store.OpenReadAsync(extent.Locator, offset, count, cancellationToken);
        }
        else {
            var whole = await _store.OpenReadAsync(extent.Locator, cancellationToken: cancellationToken);
            try {
                await SkipAsync(whole, offset, cancellationToken);
            }
            catch {
                await whole.DisposeAsync();
                throw;
            }

            _current = whole;
        }

        _currentRemaining = count;
    }

    private static async Task SkipAsync(Stream stream, long count, CancellationToken cancellationToken)
    {
        if (count == 0) {
            return;
        }

        var scratch = new byte[(int)Math.Min(count, 81920)];
        while (count > 0) {
            var read = await stream.ReadAsync(scratch.AsMemory(0, (int)Math.Min(count, scratch.Length)), cancellationToken);
            if (read == 0) {
                throw new IOException("A blob ended before the start of the requested range.");
            }

            count -= read;
        }
    }
}
