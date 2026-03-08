namespace IntegratedS3.Protocol;

public sealed class S3ErrorResponse
{
    public required string Code { get; init; }

    public required string Message { get; init; }

    public string? Resource { get; init; }

    public string? RequestId { get; init; }

    public string? BucketName { get; init; }

    public string? Key { get; init; }
}