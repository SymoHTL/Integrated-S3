namespace IntegratedS3.Protocol;

/// <summary>
/// Represents an S3 XML error response returned when a request fails.
/// </summary>
public sealed class S3ErrorResponse
{
    /// <summary>The S3 error code (e.g., <c>NoSuchBucket</c>, <c>AccessDenied</c>).</summary>
    public required string Code { get; init; }

    /// <summary>A human-readable description of the error.</summary>
    public required string Message { get; init; }

    /// <summary>The resource associated with the error.</summary>
    public string? Resource { get; init; }

    /// <summary>The request ID assigned by S3 for debugging purposes.</summary>
    public string? RequestId { get; init; }

    /// <summary>The name of the bucket related to the error, if applicable.</summary>
    public string? BucketName { get; init; }

    /// <summary>The object key related to the error, if applicable.</summary>
    public string? Key { get; init; }
}