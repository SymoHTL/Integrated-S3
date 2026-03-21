using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;

namespace IntegratedS3.Abstractions.Services;

/// <summary>
/// Primary consumer-facing interface for interacting with storage.
/// Orchestrates operations across one or more <see cref="IStorageBackend"/> implementations.
/// </summary>
public interface IStorageService
{
    /// <summary>
    /// Enumerates all buckets visible to the current caller.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="BucketInfo"/> for each accessible bucket.</returns>
    IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new storage bucket.
    /// </summary>
    /// <param name="request">The bucket creation parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the created <see cref="BucketInfo"/> on success,
    /// or a <see cref="Errors.StorageError"/> (e.g., <see cref="Errors.StorageErrorCode.BucketAlreadyExists"/>) on failure.</returns>
    ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the region or location constraint for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketLocationInfo"/> on success,
    /// or a <see cref="Errors.StorageError"/> on failure.</returns>
    ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the versioning state of the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketVersioningInfo"/> on success.</returns>
    ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the versioning state of the specified bucket.
    /// </summary>
    /// <param name="request">The versioning configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the updated <see cref="BucketVersioningInfo"/> on success.</returns>
    ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the CORS configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketCorsConfiguration"/> on success,
    /// or a <see cref="Errors.StorageError"/> with <see cref="Errors.StorageErrorCode.CorsConfigurationNotFound"/> if none exists.</returns>
    ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the CORS configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The CORS configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketCorsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the CORS configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete CORS request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the default server-side encryption configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketDefaultEncryptionConfiguration"/> on success,
    /// or a <see cref="Errors.StorageError"/> with <see cref="Errors.StorageErrorCode.BucketEncryptionConfigurationNotFound"/> if none exists.</returns>
    ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the default server-side encryption configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The encryption configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketDefaultEncryptionConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> PutBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the default encryption configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete encryption request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketDefaultEncryptionAsync(DeleteBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether the specified bucket exists and is accessible to the caller.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketInfo"/> on success,
    /// or a <see cref="Errors.StorageError"/> with <see cref="Errors.StorageErrorCode.BucketNotFound"/> if it does not exist.</returns>
    ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the specified bucket.
    /// </summary>
    /// <param name="request">The bucket deletion request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success, or a <see cref="Errors.StorageError"/>
    /// (e.g., <see cref="Errors.StorageErrorCode.BucketNotEmpty"/>) on failure.</returns>
    ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists objects in the specified bucket, optionally filtered by prefix and delimiter.
    /// </summary>
    /// <param name="request">The listing parameters (bucket name, prefix, delimiter, pagination).</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="ObjectInfo"/> for each matching object or common prefix.</returns>
    IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all versions of objects in the specified bucket.
    /// </summary>
    /// <param name="request">The listing parameters (bucket name, prefix, pagination).</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="ObjectInfo"/> for each object version.</returns>
    IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists in-progress multipart uploads for the specified bucket.
    /// </summary>
    /// <param name="request">The listing parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="MultipartUploadInfo"/> for each active upload.</returns>
    IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the parts that have been uploaded for a specific multipart upload.
    /// </summary>
    /// <param name="request">The listing parameters including the upload ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="MultipartUploadPart"/> for each uploaded part.</returns>
    IAsyncEnumerable<MultipartUploadPart> ListMultipartPartsAsync(ListMultipartPartsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves an object's data stream and metadata from the specified bucket.
    /// </summary>
    /// <param name="request">The get-object parameters (bucket, key, optional range and conditional headers).</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="GetObjectResponse"/> on success,
    /// or a <see cref="Errors.StorageError"/> (e.g., <see cref="Errors.StorageErrorCode.ObjectNotFound"/>,
    /// <see cref="Errors.StorageErrorCode.InvalidRange"/>, <see cref="Errors.StorageErrorCode.PreconditionFailed"/>) on failure.</returns>
    ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the retention configuration for the specified object.
    /// </summary>
    /// <param name="request">The request identifying the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectRetentionInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectRetentionInfo>> GetObjectRetentionAsync(GetObjectRetentionRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the legal-hold status for the specified object.
    /// </summary>
    /// <param name="request">The request identifying the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectLegalHoldInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectLegalHoldInfo>> GetObjectLegalHoldAsync(GetObjectLegalHoldRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves selected attributes of the specified object (e.g., checksum, size, parts).
    /// </summary>
    /// <param name="request">The request identifying the object and desired attributes.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="GetObjectAttributesResponse"/> on success.</returns>
    ValueTask<StorageResult<GetObjectAttributesResponse>> GetObjectAttributesAsync(GetObjectAttributesRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the tag set associated with the specified object.
    /// </summary>
    /// <param name="request">The request identifying the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectTagSet"/> on success.</returns>
    ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies an object within or between buckets.
    /// </summary>
    /// <param name="request">The copy parameters including source and destination.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectInfo"/> of the new copy on success.</returns>
    ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Uploads an object to the specified bucket, creating or overwriting it.
    /// </summary>
    /// <param name="request">The put-object parameters including key, content stream, and metadata.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectInfo"/> of the stored object on success.</returns>
    ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the tag set on the specified object, replacing any existing tags.
    /// </summary>
    /// <param name="request">The request containing the object identifier and new tags.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="ObjectTagSet"/> on success.</returns>
    ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes all tags from the specified object.
    /// </summary>
    /// <param name="request">The request identifying the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the resulting (empty) <see cref="ObjectTagSet"/> on success.</returns>
    ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Initiates a multipart upload and returns an upload ID for subsequent part uploads.
    /// </summary>
    /// <param name="request">The multipart initiation parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="MultipartUploadInfo"/> (including the upload ID) on success.</returns>
    ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Uploads a single part of a multipart upload.
    /// </summary>
    /// <param name="request">The part upload parameters including upload ID, part number, and content stream.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="MultipartUploadPart"/> with its ETag on success.</returns>
    ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies a byte range from an existing object as a part of a multipart upload.
    /// </summary>
    /// <param name="request">The part-copy parameters including source object, byte range, and target upload ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="MultipartUploadPart"/> on success.</returns>
    ValueTask<StorageResult<MultipartUploadPart>> UploadPartCopyAsync(UploadPartCopyRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Completes a multipart upload by assembling previously uploaded parts into a single object.
    /// </summary>
    /// <param name="request">The completion request including the upload ID and part manifest.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the final assembled <see cref="ObjectInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Aborts an in-progress multipart upload and discards all uploaded parts.
    /// </summary>
    /// <param name="request">The abort request including the upload ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves object metadata without downloading the content body.
    /// </summary>
    /// <param name="request">The head-object parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectInfo"/> metadata on success,
    /// or a <see cref="Errors.StorageError"/> with <see cref="Errors.StorageErrorCode.ObjectNotFound"/> if the object does not exist.</returns>
    ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes an object, or creates a delete marker if versioning is enabled on the bucket.
    /// </summary>
    /// <param name="request">The delete-object parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="DeleteObjectResult"/> on success.</returns>
    ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the tagging configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketTaggingConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketTaggingConfiguration>> GetBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the tagging configuration on the specified bucket, replacing any existing tags.
    /// </summary>
    /// <param name="request">The tagging configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketTaggingConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketTaggingConfiguration>> PutBucketTaggingAsync(PutBucketTaggingRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the tagging configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete tagging request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketTaggingAsync(DeleteBucketTaggingRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the server access logging configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketLoggingConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketLoggingConfiguration>> GetBucketLoggingAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the server access logging configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The logging configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketLoggingConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketLoggingConfiguration>> PutBucketLoggingAsync(PutBucketLoggingRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the static website hosting configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketWebsiteConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketWebsiteConfiguration>> GetBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the static website hosting configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The website configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketWebsiteConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketWebsiteConfiguration>> PutBucketWebsiteAsync(PutBucketWebsiteRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the static website hosting configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete website request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketWebsiteAsync(DeleteBucketWebsiteRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the request-payment configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketRequestPaymentConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketRequestPaymentConfiguration>> GetBucketRequestPaymentAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the request-payment configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The request-payment configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketRequestPaymentConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketRequestPaymentConfiguration>> PutBucketRequestPaymentAsync(PutBucketRequestPaymentRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the transfer-acceleration configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketAccelerateConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketAccelerateConfiguration>> GetBucketAccelerateAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the transfer-acceleration configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The accelerate configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketAccelerateConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketAccelerateConfiguration>> PutBucketAccelerateAsync(PutBucketAccelerateRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the lifecycle configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketLifecycleConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketLifecycleConfiguration>> GetBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the lifecycle configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The lifecycle configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketLifecycleConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketLifecycleConfiguration>> PutBucketLifecycleAsync(PutBucketLifecycleRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the lifecycle configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete lifecycle request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketLifecycleAsync(DeleteBucketLifecycleRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the replication configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketReplicationConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketReplicationConfiguration>> GetBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the replication configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The replication configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketReplicationConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketReplicationConfiguration>> PutBucketReplicationAsync(PutBucketReplicationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the replication configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete replication request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketReplicationAsync(DeleteBucketReplicationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the event notification configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketNotificationConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketNotificationConfiguration>> GetBucketNotificationConfigurationAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the event notification configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The notification configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketNotificationConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketNotificationConfiguration>> PutBucketNotificationConfigurationAsync(PutBucketNotificationConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the object-lock configuration for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectLockConfiguration"/> on success.</returns>
    ValueTask<StorageResult<ObjectLockConfiguration>> GetObjectLockConfigurationAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the object-lock configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The object-lock configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="ObjectLockConfiguration"/> on success.</returns>
    ValueTask<StorageResult<ObjectLockConfiguration>> PutObjectLockConfigurationAsync(PutObjectLockConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a specific analytics configuration for the bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="id">The identifier of the analytics configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketAnalyticsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketAnalyticsConfiguration>> GetBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or updates an analytics configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The analytics configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketAnalyticsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketAnalyticsConfiguration>> PutBucketAnalyticsConfigurationAsync(PutBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a specific analytics configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete analytics request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketAnalyticsConfigurationAsync(DeleteBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all analytics configurations for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the list of <see cref="BucketAnalyticsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<IReadOnlyList<BucketAnalyticsConfiguration>>> ListBucketAnalyticsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a specific metrics configuration for the bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="id">The identifier of the metrics configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketMetricsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketMetricsConfiguration>> GetBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or updates a metrics configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The metrics configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketMetricsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketMetricsConfiguration>> PutBucketMetricsConfigurationAsync(PutBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a specific metrics configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete metrics request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketMetricsConfigurationAsync(DeleteBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all metrics configurations for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the list of <see cref="BucketMetricsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<IReadOnlyList<BucketMetricsConfiguration>>> ListBucketMetricsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a specific inventory configuration for the bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="id">The identifier of the inventory configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketInventoryConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketInventoryConfiguration>> GetBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or updates an inventory configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The inventory configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketInventoryConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketInventoryConfiguration>> PutBucketInventoryConfigurationAsync(PutBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a specific inventory configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete inventory request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketInventoryConfigurationAsync(DeleteBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all inventory configurations for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the list of <see cref="BucketInventoryConfiguration"/> on success.</returns>
    ValueTask<StorageResult<IReadOnlyList<BucketInventoryConfiguration>>> ListBucketInventoryConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a specific intelligent-tiering configuration for the bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="id">The identifier of the intelligent-tiering configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketIntelligentTieringConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> GetBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or updates an intelligent-tiering configuration for the specified bucket.
    /// </summary>
    /// <param name="request">The intelligent-tiering configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketIntelligentTieringConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> PutBucketIntelligentTieringConfigurationAsync(PutBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a specific intelligent-tiering configuration from the specified bucket.
    /// </summary>
    /// <param name="request">The delete intelligent-tiering request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketIntelligentTieringConfigurationAsync(DeleteBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all intelligent-tiering configurations for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the list of <see cref="BucketIntelligentTieringConfiguration"/> on success.</returns>
    ValueTask<StorageResult<IReadOnlyList<BucketIntelligentTieringConfiguration>>> ListBucketIntelligentTieringConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the retention configuration on the specified object.
    /// </summary>
    /// <param name="request">The retention parameters to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="ObjectRetentionInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectRetentionInfo>> PutObjectRetentionAsync(PutObjectRetentionRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the legal-hold status on the specified object.
    /// </summary>
    /// <param name="request">The legal-hold parameters to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="ObjectLegalHoldInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectLegalHoldInfo>> PutObjectLegalHoldAsync(PutObjectLegalHoldRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs an SQL-like query against the content of an object (e.g., CSV or JSON).
    /// </summary>
    /// <param name="request">The select-object-content parameters including expression and input/output format.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="SelectObjectContentResponse"/> on success.</returns>
    ValueTask<StorageResult<SelectObjectContentResponse>> SelectObjectContentAsync(SelectObjectContentRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Initiates a restore of an archived object to a temporarily accessible copy.
    /// </summary>
    /// <param name="request">The restore-object parameters including restore tier and days.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="RestoreObjectResponse"/> on success.</returns>
    ValueTask<StorageResult<RestoreObjectResponse>> RestoreObjectAsync(RestoreObjectRequest request, CancellationToken cancellationToken = default);
}
