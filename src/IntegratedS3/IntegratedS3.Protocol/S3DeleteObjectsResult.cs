namespace IntegratedS3.Protocol;

public sealed class S3DeleteObjectsResult
{
    public IReadOnlyList<S3DeletedObjectResult> Deleted { get; init; } = [];

    public IReadOnlyList<S3DeleteObjectError> Errors { get; init; } = [];
}

public sealed class S3DeletedObjectResult
{
    public required string Key { get; init; }

    public string? VersionId { get; init; }
}

public sealed class S3DeleteObjectError
{
    public required string Key { get; init; }

    public string? VersionId { get; init; }

    public required string Code { get; init; }

    public required string Message { get; init; }
}
