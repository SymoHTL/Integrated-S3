namespace IntegratedS3.Provider.S3.Internal;

/// <summary>Result returned by a single S3 object delete operation.</summary>
internal sealed record S3DeleteObjectResult(
    string Key,
    string? VersionId,
    bool IsDeleteMarker);
