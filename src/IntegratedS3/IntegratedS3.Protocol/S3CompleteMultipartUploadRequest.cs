namespace IntegratedS3.Protocol;

public sealed class S3CompleteMultipartUploadRequest
{
    public IReadOnlyList<S3CompleteMultipartUploadPart> Parts { get; init; } = [];
}

public sealed class S3CompleteMultipartUploadPart
{
    public required int PartNumber { get; init; }

    public required string ETag { get; init; }
}
