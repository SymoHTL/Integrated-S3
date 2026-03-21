namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the ListParts operation on a multipart upload.</summary>
public sealed class ListMultipartPartsRequest
{
    /// <summary>The name of the bucket for the multipart upload.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The upload identifier of the multipart upload to list parts for.</summary>
    public required string UploadId { get; init; }

    /// <summary>The part number after which to begin listing.</summary>
    public int? PartNumberMarker { get; init; }

    /// <summary>The maximum number of parts to return per page.</summary>
    public int? PageSize { get; init; }
}
