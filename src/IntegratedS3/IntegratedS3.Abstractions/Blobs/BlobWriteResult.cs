namespace IntegratedS3.Abstractions.Blobs;

/// <summary>
/// The outcome of <see cref="IBlobStore.WriteAsync"/>.
/// </summary>
public sealed class BlobWriteResult
{
    /// <summary>
    /// Gets the new blob's locator: opaque to the caller, assigned by the store, unique for every write.
    /// </summary>
    public required string Locator { get; init; }

    /// <summary>
    /// Gets the number of bytes stored.
    /// </summary>
    public required long Length { get; init; }
}
