using System.Text;
using Amazon.S3.Model;
using IntegratedS3.Provider.S3.Internal;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Regression tests for issue #109: <c>AwsS3StorageClient.SelectObjectContentAsync</c> used to buffer
/// the entire S3 Select result into a detached <see cref="MemoryStream"/>, never dispose the SDK event
/// stream (leaking the underlying HTTP connection), and ignore the <see cref="CancellationToken"/>.
/// The fix routes the result through <see cref="SelectRecordsStream"/>, which streams the
/// record payloads incrementally, disposes the owning SDK event stream on every path (completion,
/// exception, cancellation), and honors cancellation on the drain loop. These tests pin that behavior.
/// </summary>
public sealed class SelectRecordsStreamTests
{
    [Fact]
    public async Task ReadAsync_StreamsRecordsIncrementally_AndSkipsNonRecordEvents()
    {
        var payload = new FakeEventStream(
            new RecordsEvent { Payload = new MemoryStream(Encoding.UTF8.GetBytes("hello,")) },
            new StatsEvent(),                       // non-record event must be skipped, not emitted
            new RecordsEvent { Payload = new MemoryStream(Encoding.UTF8.GetBytes("world")) },
            new EndEvent());
        var response = new SelectObjectContentResponse { Payload = payload };

        await using var stream = new SelectRecordsStream(response, CancellationToken.None);

        // Read through a small buffer so the drain loop must advance across multiple records.
        var buffer = new byte[4];
        var sink = new MemoryStream();
        int read;
        while ((read = await stream.ReadAsync(buffer)) > 0)
            sink.Write(buffer, 0, read);

        Assert.Equal("hello,world", Encoding.UTF8.GetString(sink.ToArray()));
        // Streamed lazily: the fake was never fully enumerated ahead of the reads.
        Assert.True(payload.WasEnumerated);
    }

    [Fact]
    public async Task DisposeAsync_DisposesSdkEventStream_OnNormalCompletion()
    {
        var payload = new FakeEventStream(
            new RecordsEvent { Payload = new MemoryStream(Encoding.UTF8.GetBytes("data")) },
            new EndEvent());
        var response = new SelectObjectContentResponse { Payload = payload };

        var stream = new SelectRecordsStream(response, CancellationToken.None);
        await stream.CopyToAsync(Stream.Null);
        await stream.DisposeAsync();

        Assert.True(payload.Disposed);
    }

    [Fact]
    public async Task DisposeAsync_DisposesSdkEventStream_WhenDrainThrows()
    {
        // The fake faults on the second MoveNextAsync — mimics an ExceptionReceived on the event stream.
        var payload = new FakeEventStream(throwAfter: 1,
            new RecordsEvent { Payload = new MemoryStream(Encoding.UTF8.GetBytes("partial")) });
        var response = new SelectObjectContentResponse { Payload = payload };

        var stream = new SelectRecordsStream(response, CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var buffer = new byte[64];
            // First read drains "partial"; the next advance faults.
            while (await stream.ReadAsync(buffer) > 0) { }
        });

        // Even though the drain threw, disposing the wrapper must release the SDK event stream.
        await stream.DisposeAsync();
        Assert.True(payload.Disposed);
    }

    [Fact]
    public async Task ReadAsync_HonorsCancellation_AndDisposeStillReleasesEventStream()
    {
        var payload = new FakeEventStream(
            new RecordsEvent { Payload = new MemoryStream(Encoding.UTF8.GetBytes("x")) },
            new RecordsEvent { Payload = new MemoryStream(Encoding.UTF8.GetBytes("y")) });
        var response = new SelectObjectContentResponse { Payload = payload };

        using var cts = new CancellationTokenSource();
        var stream = new SelectRecordsStream(response, cts.Token);

        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            var buffer = new byte[64];
            while (await stream.ReadAsync(buffer, cts.Token) > 0) { }
        });

        await stream.DisposeAsync();
        Assert.True(payload.Disposed);
    }

    [Fact]
    public async Task ReadAsync_ReturnsEmpty_AndDisposesCleanly_WhenNoPayload()
    {
        var response = new SelectObjectContentResponse { Payload = null };
        await using var stream = new SelectRecordsStream(response, CancellationToken.None);

        var buffer = new byte[16];
        Assert.Equal(0, await stream.ReadAsync(buffer));
    }

    /// <summary>
    /// Minimal spy implementing the surface <see cref="SelectRecordsStream"/> actually uses:
    /// <see cref="IAsyncEnumerable{T}"/> (the async drain) plus <see cref="IDisposable"/> (connection
    /// release). Tracks disposal and enumeration, and can fault mid-stream.
    /// </summary>
    private sealed class FakeEventStream : ISelectObjectContentEventStream, IAsyncEnumerable<IS3Event>
    {
        private readonly IReadOnlyList<IS3Event> _events;
        private readonly int? _throwAfter;

        public FakeEventStream(params IS3Event[] events) : this(throwAfter: null, events) { }

        public FakeEventStream(int? throwAfter, params IS3Event[] events)
        {
            _events = events;
            _throwAfter = throwAfter;
        }

        public bool Disposed { get; private set; }
        public bool WasEnumerated { get; private set; }

        public async IAsyncEnumerator<IS3Event> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            WasEnumerated = true;
            var index = 0;
            foreach (var evt in _events)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_throwAfter is { } n && index == n)
                    throw new InvalidOperationException("event stream faulted");
                index++;
                yield return evt;
                await Task.Yield();
            }

            if (_throwAfter is { } after && index == after)
                throw new InvalidOperationException("event stream faulted");
        }

        public void Dispose() => Disposed = true;

        // Unused by the wrapper — present only to satisfy the interface.
        public IEnumerator<IS3Event> GetEnumerator() => _events.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => _events.GetEnumerator();
        public int BufferSize { get; set; }
        public void StartProcessing() { }
        public Task StartProcessingAsync() => Task.CompletedTask;

#pragma warning disable CS0067 // events required by the interface but never raised by the fake
        public event EventHandler<Amazon.Runtime.EventStreams.EventStreamEventReceivedArgs<IS3Event>>? EventReceived;
        public event EventHandler<Amazon.Runtime.EventStreams.EventStreamExceptionReceivedArgs<S3EventStreamException>>? ExceptionReceived;
        public event EventHandler<Amazon.Runtime.EventStreams.EventStreamEventReceivedArgs<RecordsEvent>>? RecordsEventReceived;
        public event EventHandler<Amazon.Runtime.EventStreams.EventStreamEventReceivedArgs<StatsEvent>>? StatsEventReceived;
        public event EventHandler<Amazon.Runtime.EventStreams.EventStreamEventReceivedArgs<ProgressEvent>>? ProgressEventReceived;
        public event EventHandler<Amazon.Runtime.EventStreams.EventStreamEventReceivedArgs<ContinuationEvent>>? ContinuationEventReceived;
        public event EventHandler<Amazon.Runtime.EventStreams.EventStreamEventReceivedArgs<EndEvent>>? EndEventReceived;
#pragma warning restore CS0067
    }
}
