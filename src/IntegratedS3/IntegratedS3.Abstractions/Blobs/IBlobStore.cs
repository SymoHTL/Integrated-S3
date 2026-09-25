namespace IntegratedS3.Abstractions.Blobs;

/// <summary>
/// A store of write-once blobs: the byte layer under the storage engine, which keeps every object's
/// metadata and decides which blobs make up an object. A store never overwrites, renames, appends to or
/// edits a blob, so it needs no locking of its own, and a blob that no metadata row references yet is
/// invisible to clients.
/// </summary>
/// <remarks>
/// Implementations run the shared contract suite, <c>BlobStoreContractTests</c> in the
/// <c>IntegratedS3.Testing</c> package, in their own CI. A store declares what it can do in
/// <see cref="Capabilities"/>, and the engine adapts to it instead of checking which store it has.
/// </remarks>
public interface IBlobStore
{
    /// <summary>
    /// Gets what this store can do: its largest blob and whether it serves byte ranges.
    /// </summary>
    BlobStoreCapabilities Capabilities { get; }

    /// <summary>
    /// Reads <paramref name="content"/> to its end and stores it as a new blob.
    /// </summary>
    /// <param name="content">The bytes of the blob. The store reads it to its end and does not dispose it.</param>
    /// <param name="length">
    /// The exact number of bytes <paramref name="content"/> yields, when the caller knows it; otherwise
    /// <see langword="null"/>. Never larger than <see cref="BlobStoreCapabilities.MaxBlobSize"/>.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the write. A cancelled write returns no locator.</param>
    /// <returns>
    /// The new blob's locator, assigned by the store and unique for every write (two writes of the same bytes
    /// get two locators), and the number of bytes stored.
    /// </returns>
    /// <exception cref="ArgumentException">The content is larger than <see cref="BlobStoreCapabilities.MaxBlobSize"/>.</exception>
    /// <exception cref="BlobStoreThrottledException">The store asks the caller to slow down.</exception>
    ValueTask<BlobWriteResult> WriteAsync(Stream content, long? length = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a blob for reading.
    /// </summary>
    /// <param name="locator">A locator returned by <see cref="WriteAsync"/>.</param>
    /// <param name="offset">The first byte to return. Always 0 when <see cref="BlobStoreCapabilities.SupportsRangeReads"/> is <see langword="false"/>.</param>
    /// <param name="length">
    /// The number of bytes to return, or <see langword="null"/> to read to the end. Always <see langword="null"/>
    /// when <see cref="BlobStoreCapabilities.SupportsRangeReads"/> is <see langword="false"/>.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the open.</param>
    /// <returns>A stream of the requested bytes, which the caller disposes.</returns>
    /// <exception cref="BlobNotFoundException">No blob exists at <paramref name="locator"/>.</exception>
    /// <exception cref="BlobStoreThrottledException">The store asks the caller to slow down.</exception>
    ValueTask<Stream> OpenReadAsync(string locator, long offset = 0, long? length = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a blob. Deleting a blob that does not exist, or no longer exists, succeeds.
    /// </summary>
    /// <param name="locator">A locator returned by <see cref="WriteAsync"/>.</param>
    /// <param name="cancellationToken">A token to cancel the delete.</param>
    /// <exception cref="BlobStoreThrottledException">The store asks the caller to slow down.</exception>
    ValueTask DeleteAsync(string locator, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the stored blobs one page at a time, in an order that stays stable while blobs are added and
    /// removed, so that the orphan sweep can walk a slow or rate-limited store in small steps.
    /// </summary>
    /// <param name="cursor">
    /// <see langword="null"/> to start at the beginning, or the <see cref="BlobListPage.NextCursor"/> of the
    /// previous page to resume after it, also from a later process.
    /// </param>
    /// <param name="maxEntries">The largest number of entries to return. The store may return fewer.</param>
    /// <param name="cancellationToken">A token to cancel the listing.</param>
    /// <returns>
    /// The next entries. A blob that exists for the whole walk appears exactly once; a blob written or
    /// deleted during the walk may or may not appear.
    /// </returns>
    /// <exception cref="BlobStoreThrottledException">The store asks the caller to slow down.</exception>
    ValueTask<BlobListPage> ListAsync(string? cursor, int maxEntries, CancellationToken cancellationToken = default);
}
