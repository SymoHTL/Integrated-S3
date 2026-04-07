namespace IntegratedS3.Core.Internal;

/// <summary>
/// A read-only stream wrapper that counts actual bytes read from the inner stream
/// and invokes a callback with the total on dispose.
/// </summary>
internal sealed class MeteringStream(Stream innerStream, Action<long> onDispose) : Stream
{
    private long _bytesRead;

    public override bool CanRead => innerStream.CanRead;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => innerStream.Length;

    public override long Position
    {
        get => innerStream.Position;
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var bytesRead = innerStream.Read(buffer, offset, count);
        _bytesRead += bytesRead;
        return bytesRead;
    }

    public override int Read(Span<byte> buffer)
    {
        var bytesRead = innerStream.Read(buffer);
        _bytesRead += bytesRead;
        return bytesRead;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var bytesRead = await innerStream.ReadAsync(buffer, cancellationToken);
        _bytesRead += bytesRead;
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

    public override async ValueTask DisposeAsync()
    {
        onDispose(_bytesRead);
        await innerStream.DisposeAsync();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) {
            onDispose(_bytesRead);
            innerStream.Dispose();
        }

        base.Dispose(disposing);
    }
}
