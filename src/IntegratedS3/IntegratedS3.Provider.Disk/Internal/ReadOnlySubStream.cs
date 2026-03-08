namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class ReadOnlySubStream(Stream innerStream, long length) : Stream
{
    private readonly Stream _innerStream = innerStream;
    private long _remaining = length;

    public override bool CanRead => _innerStream.CanRead;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => _remaining;

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(buffer);

        if (_remaining <= 0) {
            return 0;
        }

        var bytesRead = _innerStream.Read(buffer, offset, (int)Math.Min(count, _remaining));
        _remaining -= bytesRead;
        return bytesRead;
    }

    public override int Read(Span<byte> buffer)
    {
        if (_remaining <= 0) {
            return 0;
        }

        var bytesRead = _innerStream.Read(buffer[..(int)Math.Min(buffer.Length, _remaining)]);
        _remaining -= bytesRead;
        return bytesRead;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_remaining <= 0) {
            return 0;
        }

        var bytesRead = await _innerStream.ReadAsync(buffer[..(int)Math.Min(buffer.Length, _remaining)], cancellationToken);
        _remaining -= bytesRead;
        return bytesRead;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return base.ReadAsync(buffer, offset, count, cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        throw new NotSupportedException();
    }

    public override ValueTask DisposeAsync()
    {
        return _innerStream.DisposeAsync();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) {
            _innerStream.Dispose();
        }

        base.Dispose(disposing);
    }
}