using Amazon.S3.Model;

namespace IntegratedS3.Provider.S3.Internal;

/// <summary>
/// A read-only, forward-only <see cref="Stream"/> that incrementally streams the record payloads
/// of an AWS S3 Select <see cref="Amazon.S3.Model.SelectObjectContentResponse"/> without buffering
/// the full result set in memory.
/// </summary>
/// <remarks>
/// The stream lazily pulls <see cref="RecordsEvent"/> items from the SDK event stream as the caller
/// reads. Non-record events (stats, progress, continuation, end) are skipped. Disposing this stream
/// deterministically disposes the current record payload, the event-stream enumerator and the owning
/// SDK event stream (<c>SelectObjectContentResponse.Payload</c>), releasing the underlying HTTP
/// connection even when the caller aborts mid-read or an exception is thrown while draining.
/// </remarks>
internal sealed class SelectRecordsStream : Stream
{
    // The disposable resource that owns the underlying HTTP response / streaming socket is the SDK
    // event stream (Payload : ISelectObjectContentEventStream : IDisposable). Disposing it releases
    // the connection. (SelectObjectContentResponse itself is not IDisposable.)
    private readonly IDisposable? _ownedPayload;
    private readonly IAsyncEnumerator<IS3Event>? _enumerator;
    private Stream? _currentPayload;
    private bool _finished;
    private bool _disposed;

    public SelectRecordsStream(
        Amazon.S3.Model.SelectObjectContentResponse response,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(response);

        _ownedPayload = response.Payload;

        // The concrete SDK payload implements IAsyncEnumerable<IS3Event>; enumerating it drives the
        // event-stream processing loop and observes the supplied cancellation token internally.
        if (response.Payload is IAsyncEnumerable<IS3Event> asyncEvents)
        {
            _enumerator = asyncEvents.GetAsyncEnumerator(cancellationToken);
        }
        else
        {
            // No payload (e.g. empty result) — the stream reads as empty and disposal still releases
            // any owned SDK resource.
            _finished = true;
        }
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (buffer.IsEmpty)
            return 0;

        while (true)
        {
            if (_currentPayload is not null)
            {
                var read = await _currentPayload.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
                if (read > 0)
                    return read;

                // Current record fully consumed; move on to the next event.
                await _currentPayload.DisposeAsync().ConfigureAwait(false);
                _currentPayload = null;
            }

            if (!await AdvanceToNextRecordAsync(cancellationToken).ConfigureAwait(false))
                return 0;
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        // Bridge the synchronous path onto the async pull loop. Callers (the endpoint) always use the
        // async path; this exists only for completeness / CopyTo fallbacks.
        return ReadAsync(buffer.AsMemory(offset, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    private async ValueTask<bool> AdvanceToNextRecordAsync(CancellationToken cancellationToken)
    {
        if (_finished || _enumerator is null)
            return false;

        while (await _enumerator.MoveNextAsync().ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_enumerator.Current is RecordsEvent { Payload: { } payload })
            {
                _currentPayload = payload;
                return true;
            }
            // Skip stats/progress/continuation/end events — they carry no record bytes.
        }

        _finished = true;
        return false;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        try
        {
            if (_currentPayload is not null)
            {
                await _currentPayload.DisposeAsync().ConfigureAwait(false);
                _currentPayload = null;
            }

            if (_enumerator is not null)
                await _enumerator.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _ownedPayload?.Dispose();
        }

        await base.DisposeAsync().ConfigureAwait(false);
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
            {
                _currentPayload?.Dispose();
                _currentPayload = null;

                if (_enumerator is not null)
                {
                    // Best-effort synchronous drain of the async enumerator disposal.
                    _enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            }
        }
        finally
        {
            _ownedPayload?.Dispose();
            base.Dispose(disposing);
        }
    }

    public override void Flush() => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
