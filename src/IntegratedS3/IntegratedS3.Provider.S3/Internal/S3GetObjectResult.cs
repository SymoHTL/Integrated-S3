namespace IntegratedS3.Provider.S3.Internal;

/// <summary>Wraps the streaming response from an S3 GetObject call, including object metadata and content stream.</summary>
internal sealed class S3GetObjectResult : IAsyncDisposable
{
    public S3ObjectEntry Entry { get; }
    public Stream Content { get; }
    public long TotalContentLength { get; }

    public S3GetObjectResult(
        S3ObjectEntry entry,
        Stream content,
        long totalContentLength,
        IDisposable? responseWrapper = null)
    {
        Entry = entry;
        Content = responseWrapper is null ? content : new ResponseOwnedStream(content, responseWrapper);
        TotalContentLength = totalContentLength;
    }

    public ValueTask DisposeAsync() => Content.DisposeAsync();

    private sealed class ResponseOwnedStream(Stream inner, IDisposable owner) : Stream
    {
        private readonly Stream _inner = inner;
        private readonly IDisposable _owner = owner;
        private bool _disposed;

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush() => _inner.Flush();

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(buffer, cancellationToken);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        public override void SetLength(long value) => _inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

        public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.WriteAsync(buffer, cancellationToken);

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.WriteAsync(buffer, offset, count, cancellationToken);

        public override async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;
            try
            {
                await _inner.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _owner.Dispose();
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                base.Dispose(disposing);
                return;
            }

            _disposed = true;
            try
            {
                if (disposing)
                    _inner.Dispose();
            }
            finally
            {
                _owner.Dispose();
                base.Dispose(disposing);
            }
        }
    }
}
