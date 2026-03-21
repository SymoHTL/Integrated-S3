using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;

namespace IntegratedS3.Abstractions.Services;

/// <summary>
/// Defines the contract that storage provider backends must implement to plug into IntegratedS3.
/// Each method mirrors <see cref="IStorageService"/> but operates at the single-backend level.
/// </summary>
public interface IStorageBackend
{
    /// <summary>
    /// Gets the unique identifier for this storage backend instance.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the provider type (e.g., "disk", "s3") of this backend.
    /// </summary>
    string Kind { get; }

    /// <summary>
    /// Gets a value indicating whether this backend is the primary storage backend.
    /// </summary>
    bool IsPrimary { get; }

    /// <summary>
    /// Gets a human-readable description of this storage backend, or <see langword="null"/> if not provided.
    /// </summary>
    string? Description { get; }

    /// <summary>
    /// Returns the <see cref="StorageCapabilities"/> supported by this backend.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The capabilities this backend supports.</returns>
    ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a descriptor indicating the support state of each storage feature in this backend.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageSupportStateDescriptor"/> describing feature support levels.</returns>
    ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the operational mode of this storage provider (e.g., read-only, read-write).
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageProviderMode"/> describing the current mode.</returns>
    ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a descriptor for resolving object storage locations within this backend.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageObjectLocationDescriptor"/> for this backend.</returns>
    ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Generates a presigned URL for direct object access. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>; providers should override to support this feature.
    /// </summary>
    /// <param name="request">The presign request parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="StorageDirectObjectAccessGrant"/> on success.</returns>
    ValueTask<StorageResult<StorageDirectObjectAccessGrant>> PresignObjectDirectAsync(
        StorageDirectObjectAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(StorageResult<StorageDirectObjectAccessGrant>.Failure(
            StorageError.Unsupported(
                "Direct object presign generation is not implemented by this storage backend.",
                request.BucketName,
                request.Key)));
    }

    /// <summary>
    /// Enumerates all buckets managed by this backend.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="BucketInfo"/>.</returns>
    IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new storage bucket in this backend.
    /// </summary>
    /// <param name="request">The bucket creation parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the created <see cref="BucketInfo"/> on success.</returns>
    ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the location constraint for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketLocationInfo"/> on success.</returns>
    ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLocationInfo>.Failure(StorageError.Unsupported("Bucket location is not implemented by this storage backend.", bucketName)));

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
    /// Retrieves the CORS configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketCorsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(StorageError.Unsupported("Bucket CORS is not implemented by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the CORS configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The CORS configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketCorsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(StorageError.Unsupported("Bucket CORS is not implemented by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes the CORS configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete CORS request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket CORS is not implemented by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the default encryption configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketDefaultEncryptionConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketDefaultEncryptionConfiguration>.Failure(StorageError.Unsupported("Bucket default encryption is not implemented by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the default encryption configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The encryption configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketDefaultEncryptionConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> PutBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketDefaultEncryptionConfiguration>.Failure(StorageError.Unsupported("Bucket default encryption is not implemented by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes the default encryption configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete encryption request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketDefaultEncryptionAsync(DeleteBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket default encryption is not implemented by this storage backend.", request.BucketName)));

    /// <summary>
    /// Checks whether the specified bucket exists and is accessible.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketInfo"/> on success.</returns>
    ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the specified bucket from this backend.
    /// </summary>
    /// <param name="request">The bucket deletion request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists objects in the specified bucket.
    /// </summary>
    /// <param name="request">The listing parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="ObjectInfo"/>.</returns>
    IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all versions of objects in the specified bucket.
    /// </summary>
    /// <param name="request">The listing parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="ObjectInfo"/> for each object version.</returns>
    IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists in-progress multipart uploads. The default implementation throws <see cref="NotSupportedException"/>.
    /// </summary>
    /// <param name="request">The listing parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="MultipartUploadInfo"/>.</returns>
    IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Multipart upload listing is not implemented by this storage backend.");

    /// <summary>
    /// Lists uploaded parts for a multipart upload. The default implementation throws <see cref="NotSupportedException"/>.
    /// </summary>
    /// <param name="request">The listing parameters including the upload ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async stream of <see cref="MultipartUploadPart"/>.</returns>
    IAsyncEnumerable<MultipartUploadPart> ListMultipartPartsAsync(ListMultipartPartsRequest request, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Multipart part listing is not implemented by this storage backend.");

    /// <summary>
    /// Retrieves an object's data stream and metadata from the specified bucket.
    /// </summary>
    /// <param name="request">The get-object parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="GetObjectResponse"/> on success.</returns>
    ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the retention configuration for the specified object. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The request identifying the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectRetentionInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectRetentionInfo>> GetObjectRetentionAsync(GetObjectRetentionRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(StorageResult<ObjectRetentionInfo>.Failure(
            StorageError.Unsupported(
                "Object retention metadata is not implemented by this storage backend.",
                request.BucketName,
                request.Key)));
    }

    /// <summary>
    /// Retrieves the legal-hold status for the specified object. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The request identifying the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectLegalHoldInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectLegalHoldInfo>> GetObjectLegalHoldAsync(GetObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(StorageResult<ObjectLegalHoldInfo>.Failure(
            StorageError.Unsupported(
                "Object legal-hold metadata is not implemented by this storage backend.",
                request.BucketName,
                request.Key)));
    }

    /// <summary>
    /// Retrieves selected attributes of the specified object. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The request identifying the object and desired attributes.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="GetObjectAttributesResponse"/> on success.</returns>
    ValueTask<StorageResult<GetObjectAttributesResponse>> GetObjectAttributesAsync(GetObjectAttributesRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(StorageResult<GetObjectAttributesResponse>.Failure(
            StorageError.Unsupported(
                "GetObjectAttributes is not supported by this storage backend.",
                request.BucketName,
                request.Key)));
    }

    /// <summary>
    /// Retrieves the tag set associated with the specified object.
    /// </summary>
    /// <param name="request">The request identifying the object.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectTagSet"/> on success.</returns>
    ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies an object within or between buckets in this backend.
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
    /// The default implementation returns <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The part-copy parameters including source object, byte range, and target upload ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="MultipartUploadPart"/> on success.</returns>
    ValueTask<StorageResult<MultipartUploadPart>> UploadPartCopyAsync(UploadPartCopyRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(StorageResult<MultipartUploadPart>.Failure(
            StorageError.Unsupported(
                "Multipart part copy is not implemented by this storage backend.",
                request.BucketName,
                request.Key)));
    }

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
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectInfo"/> metadata on success.</returns>
    ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes an object, or creates a delete marker if versioning is enabled.
    /// </summary>
    /// <param name="request">The delete-object parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="DeleteObjectResult"/> on success.</returns>
    ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the tagging configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketTaggingConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketTaggingConfiguration>> GetBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketTaggingConfiguration>.Failure(StorageError.Unsupported("Bucket tagging is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the tagging configuration on the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The tagging configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketTaggingConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketTaggingConfiguration>> PutBucketTaggingAsync(PutBucketTaggingRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketTaggingConfiguration>.Failure(StorageError.Unsupported("Bucket tagging is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes the tagging configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete tagging request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketTaggingAsync(DeleteBucketTaggingRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket tagging is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the server access logging configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketLoggingConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketLoggingConfiguration>> GetBucketLoggingAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLoggingConfiguration>.Failure(StorageError.Unsupported("Bucket logging is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the server access logging configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The logging configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketLoggingConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketLoggingConfiguration>> PutBucketLoggingAsync(PutBucketLoggingRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLoggingConfiguration>.Failure(StorageError.Unsupported("Bucket logging is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the static website hosting configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketWebsiteConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketWebsiteConfiguration>> GetBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketWebsiteConfiguration>.Failure(StorageError.Unsupported("Bucket website is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the static website hosting configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The website configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketWebsiteConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketWebsiteConfiguration>> PutBucketWebsiteAsync(PutBucketWebsiteRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketWebsiteConfiguration>.Failure(StorageError.Unsupported("Bucket website is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes the static website hosting configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete website request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketWebsiteAsync(DeleteBucketWebsiteRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket website is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the request-payment configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketRequestPaymentConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketRequestPaymentConfiguration>> GetBucketRequestPaymentAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketRequestPaymentConfiguration>.Failure(StorageError.Unsupported("Bucket request payment is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the request-payment configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The request-payment configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketRequestPaymentConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketRequestPaymentConfiguration>> PutBucketRequestPaymentAsync(PutBucketRequestPaymentRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketRequestPaymentConfiguration>.Failure(StorageError.Unsupported("Bucket request payment is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the transfer-acceleration configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketAccelerateConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketAccelerateConfiguration>> GetBucketAccelerateAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketAccelerateConfiguration>.Failure(StorageError.Unsupported("Bucket accelerate is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the transfer-acceleration configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The accelerate configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketAccelerateConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketAccelerateConfiguration>> PutBucketAccelerateAsync(PutBucketAccelerateRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketAccelerateConfiguration>.Failure(StorageError.Unsupported("Bucket accelerate is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the lifecycle configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketLifecycleConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketLifecycleConfiguration>> GetBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLifecycleConfiguration>.Failure(StorageError.Unsupported("Bucket lifecycle is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the lifecycle configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The lifecycle configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketLifecycleConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketLifecycleConfiguration>> PutBucketLifecycleAsync(PutBucketLifecycleRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLifecycleConfiguration>.Failure(StorageError.Unsupported("Bucket lifecycle is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes the lifecycle configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete lifecycle request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketLifecycleAsync(DeleteBucketLifecycleRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket lifecycle is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the replication configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketReplicationConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketReplicationConfiguration>> GetBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketReplicationConfiguration>.Failure(StorageError.Unsupported("Bucket replication configuration is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the replication configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The replication configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketReplicationConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketReplicationConfiguration>> PutBucketReplicationAsync(PutBucketReplicationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketReplicationConfiguration>.Failure(StorageError.Unsupported("Bucket replication configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes the replication configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete replication request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketReplicationAsync(DeleteBucketReplicationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket replication configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the event notification configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketNotificationConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketNotificationConfiguration>> GetBucketNotificationConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketNotificationConfiguration>.Failure(StorageError.Unsupported("Bucket notification configuration is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the event notification configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The notification configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketNotificationConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketNotificationConfiguration>> PutBucketNotificationConfigurationAsync(PutBucketNotificationConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketNotificationConfiguration>.Failure(StorageError.Unsupported("Bucket notification configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves the object-lock configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectLockConfiguration"/> on success.</returns>
    ValueTask<StorageResult<ObjectLockConfiguration>> GetObjectLockConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectLockConfiguration>.Failure(StorageError.Unsupported("Object lock configuration is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the object-lock configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The object-lock configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="ObjectLockConfiguration"/> on success.</returns>
    ValueTask<StorageResult<ObjectLockConfiguration>> PutObjectLockConfigurationAsync(PutObjectLockConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectLockConfiguration>.Failure(StorageError.Unsupported("Object lock configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Retrieves a specific analytics configuration for the bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="id">The identifier of the analytics configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketAnalyticsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketAnalyticsConfiguration>> GetBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketAnalyticsConfiguration>.Failure(StorageError.Unsupported("Bucket analytics configuration is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Creates or updates an analytics configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The analytics configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketAnalyticsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketAnalyticsConfiguration>> PutBucketAnalyticsConfigurationAsync(PutBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketAnalyticsConfiguration>.Failure(StorageError.Unsupported("Bucket analytics configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes a specific analytics configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete analytics request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketAnalyticsConfigurationAsync(DeleteBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket analytics configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Lists all analytics configurations for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the list of <see cref="BucketAnalyticsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<IReadOnlyList<BucketAnalyticsConfiguration>>> ListBucketAnalyticsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<IReadOnlyList<BucketAnalyticsConfiguration>>.Failure(StorageError.Unsupported("Listing bucket analytics configurations is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Retrieves a specific metrics configuration for the bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="id">The identifier of the metrics configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketMetricsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketMetricsConfiguration>> GetBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketMetricsConfiguration>.Failure(StorageError.Unsupported("Bucket metrics configuration is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Creates or updates a metrics configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The metrics configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketMetricsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketMetricsConfiguration>> PutBucketMetricsConfigurationAsync(PutBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketMetricsConfiguration>.Failure(StorageError.Unsupported("Bucket metrics configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes a specific metrics configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete metrics request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketMetricsConfigurationAsync(DeleteBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket metrics configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Lists all metrics configurations for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the list of <see cref="BucketMetricsConfiguration"/> on success.</returns>
    ValueTask<StorageResult<IReadOnlyList<BucketMetricsConfiguration>>> ListBucketMetricsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<IReadOnlyList<BucketMetricsConfiguration>>.Failure(StorageError.Unsupported("Listing bucket metrics configurations is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Retrieves a specific inventory configuration for the bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="id">The identifier of the inventory configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketInventoryConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketInventoryConfiguration>> GetBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketInventoryConfiguration>.Failure(StorageError.Unsupported("Bucket inventory configuration is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Creates or updates an inventory configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The inventory configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketInventoryConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketInventoryConfiguration>> PutBucketInventoryConfigurationAsync(PutBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketInventoryConfiguration>.Failure(StorageError.Unsupported("Bucket inventory configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes a specific inventory configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete inventory request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketInventoryConfigurationAsync(DeleteBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket inventory configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Lists all inventory configurations for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the list of <see cref="BucketInventoryConfiguration"/> on success.</returns>
    ValueTask<StorageResult<IReadOnlyList<BucketInventoryConfiguration>>> ListBucketInventoryConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<IReadOnlyList<BucketInventoryConfiguration>>.Failure(StorageError.Unsupported("Listing bucket inventory configurations is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Retrieves a specific intelligent-tiering configuration for the bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="id">The identifier of the intelligent-tiering configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketIntelligentTieringConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> GetBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketIntelligentTieringConfiguration>.Failure(StorageError.Unsupported("Bucket intelligent-tiering configuration is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Creates or updates an intelligent-tiering configuration for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The intelligent-tiering configuration to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="BucketIntelligentTieringConfiguration"/> on success.</returns>
    ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> PutBucketIntelligentTieringConfigurationAsync(PutBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketIntelligentTieringConfiguration>.Failure(StorageError.Unsupported("Bucket intelligent-tiering configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Deletes a specific intelligent-tiering configuration from the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The delete intelligent-tiering request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or failure.</returns>
    ValueTask<StorageResult> DeleteBucketIntelligentTieringConfigurationAsync(DeleteBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket intelligent-tiering configuration is not supported by this storage backend.", request.BucketName)));

    /// <summary>
    /// Lists all intelligent-tiering configurations for the specified bucket. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="bucketName">The name of the bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the list of <see cref="BucketIntelligentTieringConfiguration"/> on success.</returns>
    ValueTask<StorageResult<IReadOnlyList<BucketIntelligentTieringConfiguration>>> ListBucketIntelligentTieringConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<IReadOnlyList<BucketIntelligentTieringConfiguration>>.Failure(StorageError.Unsupported("Listing bucket intelligent-tiering configurations is not supported by this storage backend.", bucketName)));

    /// <summary>
    /// Sets the retention configuration on the specified object. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The retention parameters to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="ObjectRetentionInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectRetentionInfo>> PutObjectRetentionAsync(PutObjectRetentionRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectRetentionInfo>.Failure(StorageError.Unsupported("Object retention is not supported by this storage backend.", request.BucketName, request.Key)));

    /// <summary>
    /// Sets the legal-hold status on the specified object. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The legal-hold parameters to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the applied <see cref="ObjectLegalHoldInfo"/> on success.</returns>
    ValueTask<StorageResult<ObjectLegalHoldInfo>> PutObjectLegalHoldAsync(PutObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectLegalHoldInfo>.Failure(StorageError.Unsupported("Object legal hold is not supported by this storage backend.", request.BucketName, request.Key)));

    /// <summary>
    /// Runs an SQL-like query against the content of an object. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The select-object-content parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="SelectObjectContentResponse"/> on success.</returns>
    ValueTask<StorageResult<SelectObjectContentResponse>> SelectObjectContentAsync(SelectObjectContentRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<SelectObjectContentResponse>.Failure(StorageError.Unsupported("SelectObjectContent is not supported by this storage backend.", request.BucketName, request.Key)));

    /// <summary>
    /// Initiates a restore of an archived object. The default implementation returns
    /// <see cref="StorageErrorCode.UnsupportedCapability"/>.
    /// </summary>
    /// <param name="request">The restore-object parameters.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="RestoreObjectResponse"/> on success.</returns>
    ValueTask<StorageResult<RestoreObjectResponse>> RestoreObjectAsync(RestoreObjectRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<RestoreObjectResponse>.Failure(StorageError.Unsupported("RestoreObject is not supported by this storage backend.", request.BucketName, request.Key)));
}
