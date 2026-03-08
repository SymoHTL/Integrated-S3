namespace IntegratedS3.Protocol;

public sealed class S3CopyObjectResult
{
    public required string ETag { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }
}