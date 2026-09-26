namespace IntegratedS3.Abstractions.Blobs;

/// <summary>
/// Thrown by an <see cref="IBlobStore"/> that asks its caller to slow down. The engine answers the client
/// with 503 <c>SlowDown</c> and backs off in background work.
/// </summary>
public sealed class BlobStoreThrottledException : Exception
{
    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="message">What the store reported.</param>
    /// <param name="retryAfter">How long the store asks the caller to wait, when it says.</param>
    /// <param name="innerException">The store's own error, if any.</param>
    public BlobStoreThrottledException(string message, TimeSpan? retryAfter = null, Exception? innerException = null)
        : base(message, innerException)
    {
        RetryAfter = retryAfter;
    }

    /// <summary>
    /// Gets how long the store asks the caller to wait, or <see langword="null"/> when it does not say.
    /// </summary>
    public TimeSpan? RetryAfter { get; }
}
