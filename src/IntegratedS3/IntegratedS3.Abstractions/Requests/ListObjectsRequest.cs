namespace IntegratedS3.Abstractions.Requests;

public sealed class ListObjectsRequest
{
    public required string BucketName { get; init; }

    public string? Prefix { get; init; }

    public string? ContinuationToken { get; init; }

    public int? PageSize { get; init; }

    public bool IncludeVersions { get; init; }
}
