using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class CompleteMultipartUploadRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public IReadOnlyList<MultipartUploadPart> Parts { get; init; } = [];
}
