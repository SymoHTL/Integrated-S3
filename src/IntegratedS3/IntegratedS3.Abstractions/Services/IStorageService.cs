using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;

namespace IntegratedS3.Abstractions.Services;

public interface IStorageService
{
    IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> PutBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> DeleteBucketDefaultEncryptionAsync(DeleteBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<MultipartUploadPart> ListMultipartPartsAsync(ListMultipartPartsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectRetentionInfo>> GetObjectRetentionAsync(GetObjectRetentionRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectLegalHoldInfo>> GetObjectLegalHoldAsync(GetObjectLegalHoldRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<GetObjectAttributesResponse>> GetObjectAttributesAsync(GetObjectAttributesRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<MultipartUploadPart>> UploadPartCopyAsync(UploadPartCopyRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default);

    // Bucket Tagging
    ValueTask<StorageResult<BucketTaggingConfiguration>> GetBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketTaggingConfiguration>> PutBucketTaggingAsync(PutBucketTaggingRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult> DeleteBucketTaggingAsync(DeleteBucketTaggingRequest request, CancellationToken cancellationToken = default);

    // Bucket Logging
    ValueTask<StorageResult<BucketLoggingConfiguration>> GetBucketLoggingAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketLoggingConfiguration>> PutBucketLoggingAsync(PutBucketLoggingRequest request, CancellationToken cancellationToken = default);

    // Bucket Website
    ValueTask<StorageResult<BucketWebsiteConfiguration>> GetBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketWebsiteConfiguration>> PutBucketWebsiteAsync(PutBucketWebsiteRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult> DeleteBucketWebsiteAsync(DeleteBucketWebsiteRequest request, CancellationToken cancellationToken = default);

    // Bucket Request Payment
    ValueTask<StorageResult<BucketRequestPaymentConfiguration>> GetBucketRequestPaymentAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketRequestPaymentConfiguration>> PutBucketRequestPaymentAsync(PutBucketRequestPaymentRequest request, CancellationToken cancellationToken = default);

    // Bucket Accelerate
    ValueTask<StorageResult<BucketAccelerateConfiguration>> GetBucketAccelerateAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketAccelerateConfiguration>> PutBucketAccelerateAsync(PutBucketAccelerateRequest request, CancellationToken cancellationToken = default);

    // Bucket Lifecycle
    ValueTask<StorageResult<BucketLifecycleConfiguration>> GetBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketLifecycleConfiguration>> PutBucketLifecycleAsync(PutBucketLifecycleRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult> DeleteBucketLifecycleAsync(DeleteBucketLifecycleRequest request, CancellationToken cancellationToken = default);

    // Bucket Replication
    ValueTask<StorageResult<BucketReplicationConfiguration>> GetBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketReplicationConfiguration>> PutBucketReplicationAsync(PutBucketReplicationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult> DeleteBucketReplicationAsync(DeleteBucketReplicationRequest request, CancellationToken cancellationToken = default);

    // Bucket Notifications
    ValueTask<StorageResult<BucketNotificationConfiguration>> GetBucketNotificationConfigurationAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketNotificationConfiguration>> PutBucketNotificationConfigurationAsync(PutBucketNotificationConfigurationRequest request, CancellationToken cancellationToken = default);

    // Object Lock Configuration (bucket-level)
    ValueTask<StorageResult<ObjectLockConfiguration>> GetObjectLockConfigurationAsync(string bucketName, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<ObjectLockConfiguration>> PutObjectLockConfigurationAsync(PutObjectLockConfigurationRequest request, CancellationToken cancellationToken = default);

    // Bucket Analytics
    ValueTask<StorageResult<BucketAnalyticsConfiguration>> GetBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketAnalyticsConfiguration>> PutBucketAnalyticsConfigurationAsync(PutBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult> DeleteBucketAnalyticsConfigurationAsync(DeleteBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<IReadOnlyList<BucketAnalyticsConfiguration>>> ListBucketAnalyticsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default);

    // Bucket Metrics
    ValueTask<StorageResult<BucketMetricsConfiguration>> GetBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketMetricsConfiguration>> PutBucketMetricsConfigurationAsync(PutBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult> DeleteBucketMetricsConfigurationAsync(DeleteBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<IReadOnlyList<BucketMetricsConfiguration>>> ListBucketMetricsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default);

    // Bucket Inventory
    ValueTask<StorageResult<BucketInventoryConfiguration>> GetBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketInventoryConfiguration>> PutBucketInventoryConfigurationAsync(PutBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult> DeleteBucketInventoryConfigurationAsync(DeleteBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<IReadOnlyList<BucketInventoryConfiguration>>> ListBucketInventoryConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default);

    // Bucket Intelligent-Tiering
    ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> GetBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> PutBucketIntelligentTieringConfigurationAsync(PutBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult> DeleteBucketIntelligentTieringConfigurationAsync(DeleteBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<IReadOnlyList<BucketIntelligentTieringConfiguration>>> ListBucketIntelligentTieringConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default);

    // Object Lock Write Operations
    ValueTask<StorageResult<ObjectRetentionInfo>> PutObjectRetentionAsync(PutObjectRetentionRequest request, CancellationToken cancellationToken = default);
    ValueTask<StorageResult<ObjectLegalHoldInfo>> PutObjectLegalHoldAsync(PutObjectLegalHoldRequest request, CancellationToken cancellationToken = default);

    // SelectObjectContent
    ValueTask<StorageResult<SelectObjectContentResponse>> SelectObjectContentAsync(SelectObjectContentRequest request, CancellationToken cancellationToken = default);

    // RestoreObject
    ValueTask<StorageResult<RestoreObjectResponse>> RestoreObjectAsync(RestoreObjectRequest request, CancellationToken cancellationToken = default);
}
