namespace IntegratedS3.Abstractions.Errors;

/// <summary>
/// Carries structured details about a failed storage operation.
/// </summary>
public sealed class StorageError
{
    /// <summary>
    /// Gets the machine-readable error code classifying the failure.
    /// </summary>
    public required StorageErrorCode Code { get; init; }

    /// <summary>
    /// Gets a human-readable description of the error.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the name of the bucket involved in the failed operation, if applicable.
    /// </summary>
    public string? BucketName { get; init; }

    /// <summary>
    /// Gets the key of the object involved in the failed operation, if applicable.
    /// </summary>
    public string? ObjectKey { get; init; }

    /// <summary>
    /// Gets the version identifier of the object involved, if applicable.
    /// </summary>
    public string? VersionId { get; init; }

    /// <summary>
    /// Gets a value indicating whether the error relates to a delete marker rather than an actual object version.
    /// </summary>
    public bool IsDeleteMarker { get; init; }

    /// <summary>
    /// Gets the last-modified timestamp (UTC) of the object at the time of the error, if available.
    /// </summary>
    public DateTimeOffset? LastModifiedUtc { get; init; }

    /// <summary>
    /// Gets the name of the storage provider that produced this error, if known.
    /// </summary>
    public string? ProviderName { get; init; }

    /// <summary>
    /// Gets a suggested HTTP status code that best represents this error to an S3-compatible client.
    /// </summary>
    public int? SuggestedHttpStatusCode { get; init; }

    /// <summary>
    /// Creates a <see cref="StorageError"/> with <see cref="StorageErrorCode.UnsupportedCapability"/>
    /// and an HTTP 501 suggested status code.
    /// </summary>
    /// <param name="message">A human-readable description of the unsupported capability.</param>
    /// <param name="bucketName">The bucket name, if applicable.</param>
    /// <param name="objectKey">The object key, if applicable.</param>
    /// <returns>A <see cref="StorageError"/> indicating an unsupported operation.</returns>
    public static StorageError Unsupported(string message, string? bucketName = null, string? objectKey = null)
    {
        return new StorageError
        {
            Code = StorageErrorCode.UnsupportedCapability,
            Message = message,
            BucketName = bucketName,
            ObjectKey = objectKey,
            SuggestedHttpStatusCode = 501
        };
    }
}
