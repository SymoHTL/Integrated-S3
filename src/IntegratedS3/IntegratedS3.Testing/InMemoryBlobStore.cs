using System.Collections.Concurrent;
using IntegratedS3.Abstractions.Blobs;

namespace IntegratedS3.Testing;

/// <summary>
/// An in-memory <see cref="IBlobStore"/> for tests, with optional constraints that imitate a limited remote
/// store: a maximum blob size, no range reads, short listing pages, and injected throttling. The constraints
/// are deterministic, so a failure reproduces. <see cref="CreateConstrained"/> turns all of them on.
/// </summary>
public sealed class InMemoryBlobStore : IBlobStore
{
    private readonly ConcurrentDictionary<string, StoredBlob> _blobs = new(StringComparer.Ordinal);
    private readonly InMemoryBlobStoreOptions _options;
    private long _calls;
    private long _deletes;

    /// <summary>
    /// Initializes a store with the given constraints, or none.
    /// </summary>
    /// <param name="options">The constraints; <see langword="null"/> for an unconstrained store.</param>
    public InMemoryBlobStore(InMemoryBlobStoreOptions? options = null)
    {
        _options = options ?? new InMemoryBlobStoreOptions();
        Capabilities = new BlobStoreCapabilities
        {
            MaxBlobSize = _options.MaxBlobSize,
            SupportsRangeReads = _options.SupportsRangeReads
        };
    }

    /// <summary>
    /// Creates a store with every constraint on: 8 MiB blobs, no range reads, at most two entries per listing
    /// page, every fifth call and every third delete throttled.
    /// </summary>
    /// <returns>The constrained store.</returns>
    public static InMemoryBlobStore CreateConstrained() => new(new InMemoryBlobStoreOptions
    {
        MaxBlobSize = 8 * 1024 * 1024,
        SupportsRangeReads = false,
        MaxListPageSize = 2,
        ThrottleEveryNthCall = 5,
        ThrottleEveryNthDelete = 3
    });

    /// <inheritdoc />
    public BlobStoreCapabilities Capabilities { get; }

    /// <summary>
    /// Gets the number of blobs held.
    /// </summary>
    public int Count => _blobs.Count;

    /// <inheritdoc />
    public async ValueTask<BlobWriteResult> WriteAsync(Stream content, long? length = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);
        ThrowIfThrottled();

        if (length > _options.MaxBlobSize) {
            throw new ArgumentException($"The blob would be {length} bytes; this store accepts at most {_options.MaxBlobSize}.", nameof(content));
        }

        using var buffer = new MemoryStream();
        await content.CopyToAsync(buffer, cancellationToken);
        if (buffer.Length > _options.MaxBlobSize) {
            throw new ArgumentException($"The blob is {buffer.Length} bytes; this store accepts at most {_options.MaxBlobSize}.", nameof(content));
        }

        var locator = "mem-" + Guid.NewGuid().ToString("N");
        _blobs[locator] = new StoredBlob(buffer.ToArray(), DateTimeOffset.UtcNow);
        return new BlobWriteResult { Locator = locator, Length = buffer.Length };
    }

    /// <inheritdoc />
    public ValueTask<Stream> OpenReadAsync(string locator, long offset = 0, long? length = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        if (!_options.SupportsRangeReads && (offset != 0 || length is not null)) {
            throw new ArgumentException("This store does not serve byte ranges; its capabilities say so.", nameof(offset));
        }

        ThrowIfThrottled();

        if (locator is null || !_blobs.TryGetValue(locator, out var blob)) {
            throw new BlobNotFoundException(locator ?? string.Empty);
        }

        var start = (int)Math.Min(offset, blob.Data.Length);
        var count = length is { } requested ? (int)Math.Min(requested, blob.Data.Length - start) : blob.Data.Length - start;
        return ValueTask.FromResult<Stream>(new MemoryStream(blob.Data, start, count, writable: false));
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(string locator, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfThrottled();
        if (_options.ThrottleEveryNthDelete > 0 && Interlocked.Increment(ref _deletes) % _options.ThrottleEveryNthDelete == 0) {
            throw new BlobStoreThrottledException("Injected delete rate limit.", TimeSpan.FromMilliseconds(1));
        }

        if (locator is not null) {
            _blobs.TryRemove(locator, out _);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<BlobListPage> ListAsync(string? cursor, int maxEntries, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEntries);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfThrottled();

        var pageSize = Math.Min(maxEntries, _options.MaxListPageSize ?? int.MaxValue);
        var page = _blobs
            .Where(pair => cursor is null || string.CompareOrdinal(pair.Key, cursor) > 0)
            .OrderBy(static pair => pair.Key, StringComparer.Ordinal)
            .Take(pageSize + 1)
            .ToList();

        var entries = page.Take(pageSize)
            .Select(static pair => new BlobListEntry { Locator = pair.Key, Length = pair.Value.Data.Length, CreatedUtc = pair.Value.CreatedUtc })
            .ToList();
        return ValueTask.FromResult(new BlobListPage
        {
            Entries = entries,
            NextCursor = page.Count > pageSize ? entries[^1].Locator : null
        });
    }

    private void ThrowIfThrottled()
    {
        if (_options.ThrottleEveryNthCall > 0 && Interlocked.Increment(ref _calls) % _options.ThrottleEveryNthCall == 0) {
            throw new BlobStoreThrottledException("Injected throttling.", TimeSpan.FromMilliseconds(1));
        }
    }

    private sealed record StoredBlob(byte[] Data, DateTimeOffset CreatedUtc);
}

/// <summary>
/// Constraints for an <see cref="InMemoryBlobStore"/>. The defaults impose none.
/// </summary>
public sealed class InMemoryBlobStoreOptions
{
    /// <summary>
    /// Gets the largest blob the store accepts, or <see langword="null"/> for no limit.
    /// </summary>
    public long? MaxBlobSize { get; init; }

    /// <summary>
    /// Gets whether reads honour an offset and a length. When <see langword="false"/>, a ranged read throws, so
    /// a caller that ignores the capability fails. Defaults to <see langword="true"/>.
    /// </summary>
    public bool SupportsRangeReads { get; init; } = true;

    /// <summary>
    /// Gets the largest listing page returned, however many entries were asked for; <see langword="null"/> for
    /// no limit.
    /// </summary>
    public int? MaxListPageSize { get; init; }

    /// <summary>
    /// Gets the interval of injected throttling: every Nth call of any operation throws
    /// <see cref="BlobStoreThrottledException"/>. 0 turns it off.
    /// </summary>
    public int ThrottleEveryNthCall { get; init; }

    /// <summary>
    /// Gets the interval of the injected delete rate limit: every Nth delete throws
    /// <see cref="BlobStoreThrottledException"/>. 0 turns it off.
    /// </summary>
    public int ThrottleEveryNthDelete { get; init; }
}
