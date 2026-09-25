namespace IntegratedS3.Abstractions.Blobs;

/// <summary>
/// Thrown by <see cref="IBlobStore.OpenReadAsync"/> when no blob exists at the locator.
/// </summary>
public sealed class BlobNotFoundException : Exception
{
    /// <summary>
    /// Initializes a new instance for the given locator.
    /// </summary>
    /// <param name="locator">The locator that names no blob.</param>
    public BlobNotFoundException(string locator)
        : base($"No blob exists at locator '{locator}'.")
    {
        Locator = locator;
    }

    /// <summary>
    /// Initializes a new instance for the given locator, with the store's own error.
    /// </summary>
    /// <param name="locator">The locator that names no blob.</param>
    /// <param name="innerException">The store's own error.</param>
    public BlobNotFoundException(string locator, Exception innerException)
        : base($"No blob exists at locator '{locator}'.", innerException)
    {
        Locator = locator;
    }

    /// <summary>
    /// Gets the locator that names no blob.
    /// </summary>
    public string Locator { get; }
}
