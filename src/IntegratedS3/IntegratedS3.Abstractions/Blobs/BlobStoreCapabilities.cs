namespace IntegratedS3.Abstractions.Blobs;

/// <summary>
/// What an <see cref="IBlobStore"/> can do. The engine adapts to these values and never checks which store
/// it has.
/// </summary>
public sealed class BlobStoreCapabilities
{
    /// <summary>
    /// Gets the largest blob the store accepts, in bytes, or <see langword="null"/> when it has no limit. The
    /// engine splits a larger body into several blobs.
    /// </summary>
    public long? MaxBlobSize { get; init; }

    /// <summary>
    /// Gets whether <see cref="IBlobStore.OpenReadAsync"/> honours an offset and a length. When
    /// <see langword="false"/>, the engine reads a blob from its start and skips to the byte it needs.
    /// Defaults to <see langword="true"/>.
    /// </summary>
    public bool SupportsRangeReads { get; init; } = true;
}
