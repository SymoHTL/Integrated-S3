namespace IntegratedS3.Abstractions.Requests;

public sealed class GetObjectAttributesRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public string? VersionId { get; init; }

    public IReadOnlyList<string> ObjectAttributes { get; init; } = [];
}
