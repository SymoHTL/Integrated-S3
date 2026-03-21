using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the CompleteMultipartUpload operation.</summary>
public sealed class CompleteMultipartUploadRequest
{
    /// <summary>The name of the bucket for the multipart upload.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The upload identifier returned by the InitiateMultipartUpload operation.</summary>
    public required string UploadId { get; init; }

    /// <summary>The list of parts to assemble into the final object.</summary>
    public IReadOnlyList<MultipartUploadPart> Parts { get; init; } = [];
}
