namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Result of a delete operation on an object.
/// </summary>
public sealed class DeleteObjectResult
{
    /// <summary>
    /// The name of the bucket that contained the object.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The object key that was deleted.
    /// </summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>
    /// The version identifier of the deleted version or the created delete marker.
    /// </summary>
    public string? VersionId { get; init; }

    /// <summary>
    /// <see langword="true"/> if a delete marker was created rather than permanently deleting an object version.
    /// </summary>
    public bool IsDeleteMarker { get; init; }

    /// <summary>
    /// The current version of the object after the deletion, or <see langword="null"/> if the object no longer exists.
    /// </summary>
    public ObjectInfo? CurrentObject { get; init; }
}