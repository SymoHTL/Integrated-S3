namespace IntegratedS3.Protocol;

public sealed class S3DeleteObjectsRequest
{
    public bool Quiet { get; init; }

    public IReadOnlyList<S3DeleteObjectIdentifier> Objects { get; init; } = [];
}

public sealed class S3DeleteObjectIdentifier
{
    public required string Key { get; init; }

    public string? VersionId { get; init; }
}
