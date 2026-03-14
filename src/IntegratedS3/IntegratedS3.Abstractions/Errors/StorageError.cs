namespace IntegratedS3.Abstractions.Errors;

/// <summary>
/// Canonical storage error payload used across provider, orchestration, and HTTP layers.
/// </summary>
public sealed class StorageError
{
    /// <summary>The canonical error code.</summary>
    public required StorageErrorCode Code { get; init; }

    /// <summary>A human-readable error message.</summary>
    public required string Message { get; init; }

    /// <summary>The bucket associated with the error, when relevant.</summary>
    public string? BucketName { get; init; }

    /// <summary>The object key associated with the error, when relevant.</summary>
    public string? ObjectKey { get; init; }

    /// <summary>The object version identifier associated with the error, when relevant.</summary>
    public string? VersionId { get; init; }

    /// <summary>Whether the error refers to a delete marker rather than a live object version.</summary>
    public bool IsDeleteMarker { get; init; }

    /// <summary>The last-modified timestamp associated with the failing object or version, when available.</summary>
    public DateTimeOffset? LastModifiedUtc { get; init; }

    /// <summary>The provider name that produced the error, when available.</summary>
    public string? ProviderName { get; init; }

    /// <summary>An HTTP status code that the caller may surface when translating this error for HTTP clients.</summary>
    public int? SuggestedHttpStatusCode { get; init; }

    /// <summary>
    /// Creates an unsupported-capability error with the conventional HTTP 501 suggestion.
    /// </summary>
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
