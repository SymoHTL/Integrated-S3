namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of an S3 multi-object delete operation.
/// </summary>
public sealed class S3DeleteObjectsResult
{
    /// <summary>The list of successfully deleted objects.</summary>
    public IReadOnlyList<S3DeletedObjectResult> Deleted { get; init; } = [];

    /// <summary>The list of objects that could not be deleted.</summary>
    public IReadOnlyList<S3DeleteObjectError> Errors { get; init; } = [];
}

/// <summary>
/// Represents a successfully deleted object in a multi-object delete result.
/// </summary>
public sealed class S3DeletedObjectResult
{
    /// <summary>The key of the deleted object.</summary>
    public required string Key { get; init; }

    /// <summary>The version ID of the deleted object.</summary>
    public string? VersionId { get; init; }

    /// <summary>Indicates whether a delete marker was created.</summary>
    public bool DeleteMarker { get; init; }

    /// <summary>The version ID of the delete marker created.</summary>
    public string? DeleteMarkerVersionId { get; init; }
}

/// <summary>
/// Represents an error for a single object in a multi-object delete result.
/// </summary>
public sealed class S3DeleteObjectError
{
    /// <summary>The key of the object that failed to delete.</summary>
    public required string Key { get; init; }

    /// <summary>The version ID of the object that failed to delete.</summary>
    public string? VersionId { get; init; }

    /// <summary>The S3 error code.</summary>
    public required string Code { get; init; }

    /// <summary>A human-readable description of the error.</summary>
    public required string Message { get; init; }
}
