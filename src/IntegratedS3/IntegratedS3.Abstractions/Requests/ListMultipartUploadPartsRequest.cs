namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request to list the parts that have been uploaded for a multipart upload.</summary>
public sealed class ListMultipartUploadPartsRequest
{
    /// <summary>The bucket containing the multipart upload.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key of the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The identifier of the multipart upload whose parts are listed.</summary>
    public required string UploadId { get; init; }

    /// <summary>The part number after which the listing begins, or <see langword="null"/> to start from the first part.</summary>
    public int? PartNumberMarker { get; init; }

    /// <summary>The maximum number of parts to return per page, or <see langword="null"/> for the provider default.</summary>
    public int? PageSize { get; init; }
}
