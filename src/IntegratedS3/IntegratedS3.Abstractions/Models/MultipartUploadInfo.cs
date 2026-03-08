namespace IntegratedS3.Abstractions.Models;

public sealed class MultipartUploadInfo
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public DateTimeOffset InitiatedAtUtc { get; init; }
}
