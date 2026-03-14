namespace IntegratedS3.Abstractions.Requests;

public sealed class ListMultipartUploadPartsRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public int? PartNumberMarker { get; init; }

    public int? PageSize { get; init; }
}
