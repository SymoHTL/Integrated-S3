namespace IntegratedS3.Abstractions.Requests;

public sealed class GetObjectRetentionRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public string? VersionId { get; init; }
}
