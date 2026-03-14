using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;

namespace IntegratedS3.Abstractions.Services;

/// <summary>
/// Application-facing orchestration contract for bucket, object, multipart, versioning, tagging, and CORS operations.
/// </summary>
public interface IStorageService
{
    /// <summary>
    /// Asynchronously enumerates the buckets available to the current request.
    /// </summary>
    IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a bucket.
    /// </summary>
    ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the location constraint for a bucket.
    /// </summary>
    ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the versioning configuration for a bucket.
    /// </summary>
    ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a versioning configuration to a bucket.
    /// </summary>
    ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the CORS configuration for a bucket.
    /// </summary>
    ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the CORS configuration for a bucket.
    /// </summary>
    ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the CORS configuration for a bucket.
    /// </summary>
    ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads bucket metadata without enumerating objects.
    /// </summary>
    ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a bucket.
    /// </summary>
    ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously enumerates the current objects in a bucket.
    /// </summary>
    IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously enumerates the current and historical object versions in a bucket.
    /// </summary>
    IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously enumerates in-progress multipart uploads for a bucket.
    /// </summary>
    IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously enumerates the parts that have been uploaded for a multipart session.
    /// </summary>
    IAsyncEnumerable<MultipartUploadPart> ListMultipartUploadPartsAsync(ListMultipartUploadPartsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads object metadata and, when successful, returns a response that owns the readable object stream.
    /// </summary>
    ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current or version-specific tag set for an object.
    /// </summary>
    ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies an object and returns metadata for the written destination object.
    /// </summary>
    ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes an object from the request payload stream and returns metadata for the stored object.
    /// </summary>
    ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the current or version-specific tag set for an object.
    /// </summary>
    ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the current or version-specific tag set for an object.
    /// </summary>
    ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts a multipart upload session.
    /// </summary>
    ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Uploads a multipart part.
    /// </summary>
    ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Completes a multipart upload and returns metadata for the assembled object.
    /// </summary>
    ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Aborts a multipart upload.
    /// </summary>
    ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads object metadata without returning the object body.
    /// </summary>
    ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes an object and returns the resulting delete-marker/version information when available.
    /// </summary>
    ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default);
}
