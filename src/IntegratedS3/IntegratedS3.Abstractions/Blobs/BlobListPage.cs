namespace IntegratedS3.Abstractions.Blobs;

/// <summary>
/// One page of <see cref="IBlobStore.ListAsync"/>.
/// </summary>
public sealed class BlobListPage
{
    /// <summary>
    /// Gets the blobs on this page.
    /// </summary>
    public required IReadOnlyList<BlobListEntry> Entries { get; init; }

    /// <summary>
    /// Gets the cursor that resumes the listing after this page, or <see langword="null"/> when the listing is
    /// complete.
    /// </summary>
    public string? NextCursor { get; init; }
}

/// <summary>
/// One blob in a <see cref="BlobListPage"/>.
/// </summary>
public sealed class BlobListEntry
{
    /// <summary>
    /// Gets the blob's locator, as <see cref="IBlobStore.WriteAsync"/> returned it.
    /// </summary>
    public required string Locator { get; init; }

    /// <summary>
    /// Gets the blob's size in bytes.
    /// </summary>
    public required long Length { get; init; }
}
