namespace IntegratedS3.Abstractions.Requests;

public sealed class UploadMultipartPartRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public required int PartNumber { get; init; }

    public required Stream Content { get; init; }

    public long? ContentLength { get; init; }
}
