namespace IntegratedS3.Abstractions.Requests;

public sealed class AbortMultipartUploadRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }
}
