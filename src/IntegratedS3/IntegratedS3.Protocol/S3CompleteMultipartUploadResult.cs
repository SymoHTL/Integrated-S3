namespace IntegratedS3.Protocol;

public sealed class S3CompleteMultipartUploadResult
{
    public string? Location { get; init; }

    public required string Bucket { get; init; }

    public required string Key { get; init; }

    public required string ETag { get; init; }
}
