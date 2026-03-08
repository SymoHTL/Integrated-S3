namespace IntegratedS3.Protocol;

public sealed class S3InitiateMultipartUploadResult
{
    public required string Bucket { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }
}
