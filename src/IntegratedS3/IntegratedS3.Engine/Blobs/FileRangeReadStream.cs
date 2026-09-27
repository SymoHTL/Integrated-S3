using Microsoft.Win32.SafeHandles;

namespace IntegratedS3.Engine.Blobs;

/// <summary>
/// A forward-only read stream over the bytes [start, end) of an open file. It owns the handle.
/// </summary>
internal sealed class FileRangeReadStream : Stream
{
    private readonly SafeFileHandle _handle;
    private readonly long _start;
    private readonly long _end;
    private long _position;

    public FileRangeReadStream(SafeFileHandle handle, long start, long end)
    {
        _handle = handle;
        _start = start;
        _end = end;
        _position = start;
    }

    public override bool CanRead => !_handle.IsClosed;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => _end - _start;

    public override long Position
    {
        get => _position - _start;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        var read = RandomAccess.Read(_handle, Limit(buffer), _position);
        _position += read;
        return read;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var read = await RandomAccess.ReadAsync(_handle, Limit(buffer), _position, cancellationToken);
        _position += read;
        return read;
    }

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing) {
            _handle.Dispose();
        }

        base.Dispose(disposing);
    }

    private Span<byte> Limit(Span<byte> buffer) => buffer[..(int)Math.Min(buffer.Length, _end - _position)];

    private Memory<byte> Limit(Memory<byte> buffer) => buffer[..(int)Math.Min(buffer.Length, _end - _position)];
}
