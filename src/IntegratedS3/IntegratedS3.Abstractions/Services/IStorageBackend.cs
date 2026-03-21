using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;

namespace IntegratedS3.Abstractions.Services;

public interface IStorageBackend
{
    string Name { get; }

    string Kind { get; }

    bool IsPrimary { get; }

    string? Description { get; }

    ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default);

    ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default);

    ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default);

    ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default);

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

    IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLocationInfo>.Failure(StorageError.Unsupported("Bucket location is not implemented by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(StorageError.Unsupported("Bucket CORS is not implemented by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketCorsConfiguration>.Failure(StorageError.Unsupported("Bucket CORS is not implemented by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket CORS is not implemented by this storage backend.", request.BucketName)));

    ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketDefaultEncryptionConfiguration>.Failure(StorageError.Unsupported("Bucket default encryption is not implemented by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> PutBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketDefaultEncryptionConfiguration>.Failure(StorageError.Unsupported("Bucket default encryption is not implemented by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketDefaultEncryptionAsync(DeleteBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket default encryption is not implemented by this storage backend.", request.BucketName)));

    ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Multipart upload listing is not implemented by this storage backend.");

    IAsyncEnumerable<MultipartUploadPart> ListMultipartPartsAsync(ListMultipartPartsRequest request, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Multipart part listing is not implemented by this storage backend.");

    ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default);

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

    ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default);

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

    ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default);

    // Bucket Tagging
    ValueTask<StorageResult<BucketTaggingConfiguration>> GetBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketTaggingConfiguration>.Failure(StorageError.Unsupported("Bucket tagging is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketTaggingConfiguration>> PutBucketTaggingAsync(PutBucketTaggingRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketTaggingConfiguration>.Failure(StorageError.Unsupported("Bucket tagging is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketTaggingAsync(DeleteBucketTaggingRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket tagging is not supported by this storage backend.", request.BucketName)));

    // Bucket Logging
    ValueTask<StorageResult<BucketLoggingConfiguration>> GetBucketLoggingAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLoggingConfiguration>.Failure(StorageError.Unsupported("Bucket logging is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketLoggingConfiguration>> PutBucketLoggingAsync(PutBucketLoggingRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLoggingConfiguration>.Failure(StorageError.Unsupported("Bucket logging is not supported by this storage backend.", request.BucketName)));

    // Bucket Website
    ValueTask<StorageResult<BucketWebsiteConfiguration>> GetBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketWebsiteConfiguration>.Failure(StorageError.Unsupported("Bucket website is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketWebsiteConfiguration>> PutBucketWebsiteAsync(PutBucketWebsiteRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketWebsiteConfiguration>.Failure(StorageError.Unsupported("Bucket website is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketWebsiteAsync(DeleteBucketWebsiteRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket website is not supported by this storage backend.", request.BucketName)));

    // Bucket Request Payment
    ValueTask<StorageResult<BucketRequestPaymentConfiguration>> GetBucketRequestPaymentAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketRequestPaymentConfiguration>.Failure(StorageError.Unsupported("Bucket request payment is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketRequestPaymentConfiguration>> PutBucketRequestPaymentAsync(PutBucketRequestPaymentRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketRequestPaymentConfiguration>.Failure(StorageError.Unsupported("Bucket request payment is not supported by this storage backend.", request.BucketName)));

    // Bucket Accelerate
    ValueTask<StorageResult<BucketAccelerateConfiguration>> GetBucketAccelerateAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketAccelerateConfiguration>.Failure(StorageError.Unsupported("Bucket accelerate is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketAccelerateConfiguration>> PutBucketAccelerateAsync(PutBucketAccelerateRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketAccelerateConfiguration>.Failure(StorageError.Unsupported("Bucket accelerate is not supported by this storage backend.", request.BucketName)));

    // Bucket Lifecycle
    ValueTask<StorageResult<BucketLifecycleConfiguration>> GetBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLifecycleConfiguration>.Failure(StorageError.Unsupported("Bucket lifecycle is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketLifecycleConfiguration>> PutBucketLifecycleAsync(PutBucketLifecycleRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketLifecycleConfiguration>.Failure(StorageError.Unsupported("Bucket lifecycle is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketLifecycleAsync(DeleteBucketLifecycleRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket lifecycle is not supported by this storage backend.", request.BucketName)));

    // Bucket Replication
    ValueTask<StorageResult<BucketReplicationConfiguration>> GetBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketReplicationConfiguration>.Failure(StorageError.Unsupported("Bucket replication configuration is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketReplicationConfiguration>> PutBucketReplicationAsync(PutBucketReplicationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketReplicationConfiguration>.Failure(StorageError.Unsupported("Bucket replication configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketReplicationAsync(DeleteBucketReplicationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket replication configuration is not supported by this storage backend.", request.BucketName)));

    // Bucket Notifications
    ValueTask<StorageResult<BucketNotificationConfiguration>> GetBucketNotificationConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketNotificationConfiguration>.Failure(StorageError.Unsupported("Bucket notification configuration is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketNotificationConfiguration>> PutBucketNotificationConfigurationAsync(PutBucketNotificationConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketNotificationConfiguration>.Failure(StorageError.Unsupported("Bucket notification configuration is not supported by this storage backend.", request.BucketName)));

    // Object Lock Configuration (bucket-level)
    ValueTask<StorageResult<ObjectLockConfiguration>> GetObjectLockConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectLockConfiguration>.Failure(StorageError.Unsupported("Object lock configuration is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<ObjectLockConfiguration>> PutObjectLockConfigurationAsync(PutObjectLockConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectLockConfiguration>.Failure(StorageError.Unsupported("Object lock configuration is not supported by this storage backend.", request.BucketName)));

    // Bucket Analytics
    ValueTask<StorageResult<BucketAnalyticsConfiguration>> GetBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketAnalyticsConfiguration>.Failure(StorageError.Unsupported("Bucket analytics configuration is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketAnalyticsConfiguration>> PutBucketAnalyticsConfigurationAsync(PutBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketAnalyticsConfiguration>.Failure(StorageError.Unsupported("Bucket analytics configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketAnalyticsConfigurationAsync(DeleteBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket analytics configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult<IReadOnlyList<BucketAnalyticsConfiguration>>> ListBucketAnalyticsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<IReadOnlyList<BucketAnalyticsConfiguration>>.Failure(StorageError.Unsupported("Listing bucket analytics configurations is not supported by this storage backend.", bucketName)));

    // Bucket Metrics
    ValueTask<StorageResult<BucketMetricsConfiguration>> GetBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketMetricsConfiguration>.Failure(StorageError.Unsupported("Bucket metrics configuration is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketMetricsConfiguration>> PutBucketMetricsConfigurationAsync(PutBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketMetricsConfiguration>.Failure(StorageError.Unsupported("Bucket metrics configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketMetricsConfigurationAsync(DeleteBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket metrics configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult<IReadOnlyList<BucketMetricsConfiguration>>> ListBucketMetricsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<IReadOnlyList<BucketMetricsConfiguration>>.Failure(StorageError.Unsupported("Listing bucket metrics configurations is not supported by this storage backend.", bucketName)));

    // Bucket Inventory
    ValueTask<StorageResult<BucketInventoryConfiguration>> GetBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketInventoryConfiguration>.Failure(StorageError.Unsupported("Bucket inventory configuration is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketInventoryConfiguration>> PutBucketInventoryConfigurationAsync(PutBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketInventoryConfiguration>.Failure(StorageError.Unsupported("Bucket inventory configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketInventoryConfigurationAsync(DeleteBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket inventory configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult<IReadOnlyList<BucketInventoryConfiguration>>> ListBucketInventoryConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<IReadOnlyList<BucketInventoryConfiguration>>.Failure(StorageError.Unsupported("Listing bucket inventory configurations is not supported by this storage backend.", bucketName)));

    // Bucket Intelligent-Tiering
    ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> GetBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketIntelligentTieringConfiguration>.Failure(StorageError.Unsupported("Bucket intelligent-tiering configuration is not supported by this storage backend.", bucketName)));

    ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> PutBucketIntelligentTieringConfigurationAsync(PutBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<BucketIntelligentTieringConfiguration>.Failure(StorageError.Unsupported("Bucket intelligent-tiering configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult> DeleteBucketIntelligentTieringConfigurationAsync(DeleteBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult.Failure(StorageError.Unsupported("Bucket intelligent-tiering configuration is not supported by this storage backend.", request.BucketName)));

    ValueTask<StorageResult<IReadOnlyList<BucketIntelligentTieringConfiguration>>> ListBucketIntelligentTieringConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<IReadOnlyList<BucketIntelligentTieringConfiguration>>.Failure(StorageError.Unsupported("Listing bucket intelligent-tiering configurations is not supported by this storage backend.", bucketName)));

    // Object Lock Write Operations
    ValueTask<StorageResult<ObjectRetentionInfo>> PutObjectRetentionAsync(PutObjectRetentionRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectRetentionInfo>.Failure(StorageError.Unsupported("Object retention is not supported by this storage backend.", request.BucketName, request.Key)));

    ValueTask<StorageResult<ObjectLegalHoldInfo>> PutObjectLegalHoldAsync(PutObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<ObjectLegalHoldInfo>.Failure(StorageError.Unsupported("Object legal hold is not supported by this storage backend.", request.BucketName, request.Key)));

    // SelectObjectContent
    ValueTask<StorageResult<SelectObjectContentResponse>> SelectObjectContentAsync(SelectObjectContentRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<SelectObjectContentResponse>.Failure(StorageError.Unsupported("SelectObjectContent is not supported by this storage backend.", request.BucketName, request.Key)));

    // RestoreObject
    ValueTask<StorageResult<RestoreObjectResponse>> RestoreObjectAsync(RestoreObjectRequest request, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(StorageResult<RestoreObjectResponse>.Failure(StorageError.Unsupported("RestoreObject is not supported by this storage backend.", request.BucketName, request.Key)));
}
