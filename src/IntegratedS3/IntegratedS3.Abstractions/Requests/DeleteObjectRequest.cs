namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the DeleteObject operation.</summary>
public sealed class DeleteObjectRequest
{
    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key to delete.</summary>
    public required string Key { get; init; }

    /// <summary>The version identifier of the object to delete.</summary>
    public string? VersionId { get; init; }

    /// <summary>When <see langword="true"/>, permanently deletes the object instead of creating a delete marker.</summary>
    public bool BypassDeleteMarkerCreation { get; init; }
}
