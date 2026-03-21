namespace IntegratedS3.AspNetCore;

/// <summary>
/// Identifies a feature group of IntegratedS3 endpoints.
/// Used to selectively enable, disable, or configure specific endpoint groups.
/// </summary>
public enum IntegratedS3EndpointFeature
{
    /// <summary>
    /// S3 service-level endpoints such as ListBuckets and service discovery.
    /// </summary>
    Service,

    /// <summary>
    /// Bucket-level endpoints such as CreateBucket, DeleteBucket, ListObjects, and HeadBucket.
    /// </summary>
    Bucket,

    /// <summary>
    /// Object-level endpoints such as GetObject, PutObject, DeleteObject, HeadObject, and CopyObject.
    /// </summary>
    Object,

    /// <summary>
    /// Multipart upload endpoints such as CreateMultipartUpload, UploadPart, CompleteMultipartUpload, and AbortMultipartUpload.
    /// </summary>
    Multipart,

    /// <summary>
    /// Administrative and diagnostic endpoints.
    /// </summary>
    Admin
}
