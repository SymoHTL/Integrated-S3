namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the AbortMultipartUpload operation.</summary>
public sealed class AbortMultipartUploadRequest
{
    /// <summary>The name of the bucket for the multipart upload.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The upload identifier of the multipart upload to abort.</summary>
    public required string UploadId { get; init; }
}
