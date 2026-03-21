namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the request body for an S3 multi-object delete operation.
/// </summary>
public sealed class S3DeleteObjectsRequest
{
    /// <summary>When <c>true</c>, the response only includes errors (quiet mode).</summary>
    public bool Quiet { get; init; }

    /// <summary>The list of objects to delete.</summary>
    public IReadOnlyList<S3DeleteObjectIdentifier> Objects { get; init; } = [];
}

/// <summary>
/// Identifies a single object (and optional version) to delete in a multi-object delete request.
/// </summary>
public sealed class S3DeleteObjectIdentifier
{
    /// <summary>The key of the object to delete.</summary>
    public required string Key { get; init; }

    /// <summary>The version ID of the object to delete, or <c>null</c> for the current version.</summary>
    public string? VersionId { get; init; }
}
