using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;

namespace IntegratedS3.Provider.S3.Internal;

internal interface IS3StorageClient : IDisposable
{
    // Bucket operations
    Task<IReadOnlyList<S3BucketEntry>> ListBucketsAsync(CancellationToken cancellationToken = default);
    Task<S3BucketEntry> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<S3BucketEntry?> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<S3BucketLocationEntry> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<S3BucketLocationEntry>(new NotSupportedException("Bucket location is not implemented by this S3 storage client."));

    // Bucket versioning
    Task<S3VersioningEntry> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<S3VersioningEntry> SetBucketVersioningAsync(string bucketName, BucketVersioningStatus status, CancellationToken cancellationToken = default);

    // Bucket CORS
    Task<S3CorsConfigurationEntry?> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<S3CorsConfigurationEntry?>(new NotSupportedException("Bucket CORS is not implemented by this S3 storage client."));
    Task<S3CorsConfigurationEntry> SetBucketCorsAsync(string bucketName, IReadOnlyList<S3CorsRuleEntry> rules, CancellationToken cancellationToken = default)
        => Task.FromException<S3CorsConfigurationEntry>(new NotSupportedException("Bucket CORS is not implemented by this S3 storage client."));
    Task DeleteBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket CORS is not implemented by this S3 storage client."));

    Task<BucketDefaultEncryptionConfiguration> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketDefaultEncryptionConfiguration>(new NotSupportedException("Bucket default encryption is not implemented by this S3 storage client."));
    Task<BucketDefaultEncryptionConfiguration> SetBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketDefaultEncryptionConfiguration>(new NotSupportedException("Bucket default encryption is not implemented by this S3 storage client."));
    Task DeleteBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket default encryption is not implemented by this S3 storage client."));

    // Object listing
    Task<S3ObjectListPage> ListObjectsAsync(
        string bucketName,
        string? prefix,
        string? continuationToken,
        int? maxKeys,
        CancellationToken cancellationToken = default);

    Task<S3ObjectVersionListPage> ListObjectVersionsAsync(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? versionIdMarker,
        int? maxKeys,
        CancellationToken cancellationToken = default);

    // Object CRUD
    Task<S3ObjectEntry?> HeadObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        ObjectCustomerEncryptionSettings? customerEncryption,
        CancellationToken cancellationToken = default);

    Task<Uri> CreatePresignedGetObjectUrlAsync(
        string bucketName,
        string key,
        string? versionId,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default);

    Task<Uri> CreatePresignedPutObjectUrlAsync(
        string bucketName,
        string key,
        string? contentType,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default);

    Task<S3GetObjectResult> GetObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        ObjectRange? range,
        string? ifMatchETag,
        string? ifNoneMatchETag,
        DateTimeOffset? ifModifiedSinceUtc,
        DateTimeOffset? ifUnmodifiedSinceUtc,
        ObjectCustomerEncryptionSettings? customerEncryption,
        CancellationToken cancellationToken = default);

    Task<ObjectRetentionInfo> GetObjectRetentionAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);

    Task<ObjectLegalHoldInfo> GetObjectLegalHoldAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);

    Task<GetObjectAttributesResponse> GetObjectAttributesAsync(
        GetObjectAttributesRequest request,
        CancellationToken cancellationToken = default)
        => Task.FromException<GetObjectAttributesResponse>(new NotSupportedException("GetObjectAttributes is not supported."));

    Task<S3ObjectEntry> PutObjectAsync(
        string bucketName,
        string key,
        Stream content,
        long? contentLength,
        string? contentType,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        IReadOnlyDictionary<string, string>? checksums,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        ObjectCustomerEncryptionSettings? customerEncryption,
        string? storageClass,
        string? ifMatchETag,
        string? ifNoneMatchETag,
        CancellationToken cancellationToken = default);

    Task<S3DeleteObjectResult> DeleteObjectAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);

    // Copy + multipart
    Task<S3ObjectEntry> CopyObjectAsync(
        string sourceBucketName,
        string sourceKey,
        string destinationBucketName,
        string destinationKey,
        string? sourceVersionId,
        string? sourceIfMatchETag,
        string? sourceIfNoneMatchETag,
        DateTimeOffset? sourceIfModifiedSinceUtc,
        DateTimeOffset? sourceIfUnmodifiedSinceUtc,
        CopyObjectMetadataDirective metadataDirective,
        string? contentType,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        bool overwriteIfExists,
        ObjectTaggingDirective taggingDirective,
        IReadOnlyDictionary<string, string>? tags,
        string? checksumAlgorithm,
        IReadOnlyDictionary<string, string>? checksums,
        ObjectServerSideEncryptionSettings? destinationServerSideEncryption,
        ObjectCustomerEncryptionSettings? sourceCustomerEncryption,
        ObjectCustomerEncryptionSettings? destinationCustomerEncryption,
        string? storageClass,
        CancellationToken cancellationToken = default);

    Task<MultipartUploadInfo> InitiateMultipartUploadAsync(
        string bucketName,
        string key,
        string? contentType,
        string? cacheControl,
        string? contentDisposition,
        string? contentEncoding,
        string? contentLanguage,
        DateTimeOffset? expiresUtc,
        IReadOnlyDictionary<string, string>? metadata,
        IReadOnlyDictionary<string, string>? tags,
        string? checksumAlgorithm,
        ObjectServerSideEncryptionSettings? serverSideEncryption,
        ObjectCustomerEncryptionSettings? customerEncryption,
        string? storageClass,
        CancellationToken cancellationToken = default);

    Task<MultipartUploadPart> UploadMultipartPartAsync(
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        Stream content,
        long? contentLength,
        string? checksumAlgorithm,
        IReadOnlyDictionary<string, string>? checksums,
        ObjectCustomerEncryptionSettings? customerEncryption,
        CancellationToken cancellationToken = default);

    Task<MultipartUploadPart> UploadPartCopyAsync(
        UploadPartCopyRequest request,
        CancellationToken cancellationToken = default);

    Task<S3ObjectEntry> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        IReadOnlyList<MultipartUploadPart> parts,
        CancellationToken cancellationToken = default);

    Task AbortMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default);

    Task<S3MultipartUploadListPage> ListMultipartUploadsAsync(
        string bucketName,
        string? prefix,
        string? keyMarker,
        string? uploadIdMarker,
        int? maxUploads,
        CancellationToken cancellationToken = default);

    Task<S3MultipartPartListPage> ListMultipartPartsAsync(
        string bucketName,
        string key,
        string uploadId,
        int? partNumberMarker,
        int? maxParts,
        CancellationToken cancellationToken = default);

    // Bucket Tagging
    Task<BucketTaggingConfiguration> GetBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketTaggingConfiguration>(new NotSupportedException("Bucket tagging is not implemented by this S3 storage client."));
    Task<BucketTaggingConfiguration> PutBucketTaggingAsync(PutBucketTaggingRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketTaggingConfiguration>(new NotSupportedException("Bucket tagging is not implemented by this S3 storage client."));
    Task DeleteBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket tagging is not implemented by this S3 storage client."));

    // Bucket Logging
    Task<BucketLoggingConfiguration> GetBucketLoggingAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketLoggingConfiguration>(new NotSupportedException("Bucket logging is not implemented by this S3 storage client."));
    Task<BucketLoggingConfiguration> PutBucketLoggingAsync(PutBucketLoggingRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketLoggingConfiguration>(new NotSupportedException("Bucket logging is not implemented by this S3 storage client."));

    // Bucket Website
    Task<BucketWebsiteConfiguration> GetBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketWebsiteConfiguration>(new NotSupportedException("Bucket website is not implemented by this S3 storage client."));
    Task<BucketWebsiteConfiguration> PutBucketWebsiteAsync(PutBucketWebsiteRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketWebsiteConfiguration>(new NotSupportedException("Bucket website is not implemented by this S3 storage client."));
    Task DeleteBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket website is not implemented by this S3 storage client."));

    // Bucket Request Payment
    Task<BucketRequestPaymentConfiguration> GetBucketRequestPaymentAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketRequestPaymentConfiguration>(new NotSupportedException("Bucket request payment is not implemented by this S3 storage client."));
    Task<BucketRequestPaymentConfiguration> PutBucketRequestPaymentAsync(PutBucketRequestPaymentRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketRequestPaymentConfiguration>(new NotSupportedException("Bucket request payment is not implemented by this S3 storage client."));

    // Bucket Accelerate
    Task<BucketAccelerateConfiguration> GetBucketAccelerateAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketAccelerateConfiguration>(new NotSupportedException("Bucket accelerate is not implemented by this S3 storage client."));
    Task<BucketAccelerateConfiguration> PutBucketAccelerateAsync(PutBucketAccelerateRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketAccelerateConfiguration>(new NotSupportedException("Bucket accelerate is not implemented by this S3 storage client."));

    // Bucket Lifecycle
    Task<BucketLifecycleConfiguration> GetBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketLifecycleConfiguration>(new NotSupportedException("Bucket lifecycle is not implemented by this S3 storage client."));
    Task<BucketLifecycleConfiguration> PutBucketLifecycleAsync(PutBucketLifecycleRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketLifecycleConfiguration>(new NotSupportedException("Bucket lifecycle is not implemented by this S3 storage client."));
    Task DeleteBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket lifecycle is not implemented by this S3 storage client."));

    // Bucket Replication
    Task<BucketReplicationConfiguration> GetBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketReplicationConfiguration>(new NotSupportedException("Bucket replication is not implemented by this S3 storage client."));
    Task<BucketReplicationConfiguration> PutBucketReplicationAsync(PutBucketReplicationRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketReplicationConfiguration>(new NotSupportedException("Bucket replication is not implemented by this S3 storage client."));
    Task DeleteBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket replication is not implemented by this S3 storage client."));

    // Bucket Notifications
    Task<BucketNotificationConfiguration> GetBucketNotificationConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<BucketNotificationConfiguration>(new NotSupportedException("Bucket notification configuration is not implemented by this S3 storage client."));
    Task<BucketNotificationConfiguration> PutBucketNotificationConfigurationAsync(PutBucketNotificationConfigurationRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketNotificationConfiguration>(new NotSupportedException("Bucket notification configuration is not implemented by this S3 storage client."));

    // Object Lock Configuration (bucket-level)
    Task<ObjectLockConfiguration> GetObjectLockConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<ObjectLockConfiguration>(new NotSupportedException("Object lock configuration is not implemented by this S3 storage client."));
    Task<ObjectLockConfiguration> PutObjectLockConfigurationAsync(PutObjectLockConfigurationRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<ObjectLockConfiguration>(new NotSupportedException("Object lock configuration is not implemented by this S3 storage client."));

    // Bucket Analytics
    Task<BucketAnalyticsConfiguration> GetBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => Task.FromException<BucketAnalyticsConfiguration>(new NotSupportedException("Bucket analytics configuration is not implemented by this S3 storage client."));
    Task<BucketAnalyticsConfiguration> PutBucketAnalyticsConfigurationAsync(PutBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketAnalyticsConfiguration>(new NotSupportedException("Bucket analytics configuration is not implemented by this S3 storage client."));
    Task DeleteBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket analytics configuration is not implemented by this S3 storage client."));
    Task<IReadOnlyList<BucketAnalyticsConfiguration>> ListBucketAnalyticsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<IReadOnlyList<BucketAnalyticsConfiguration>>(new NotSupportedException("Listing bucket analytics configurations is not implemented by this S3 storage client."));

    // Bucket Metrics
    Task<BucketMetricsConfiguration> GetBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => Task.FromException<BucketMetricsConfiguration>(new NotSupportedException("Bucket metrics configuration is not implemented by this S3 storage client."));
    Task<BucketMetricsConfiguration> PutBucketMetricsConfigurationAsync(PutBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketMetricsConfiguration>(new NotSupportedException("Bucket metrics configuration is not implemented by this S3 storage client."));
    Task DeleteBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket metrics configuration is not implemented by this S3 storage client."));
    Task<IReadOnlyList<BucketMetricsConfiguration>> ListBucketMetricsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<IReadOnlyList<BucketMetricsConfiguration>>(new NotSupportedException("Listing bucket metrics configurations is not implemented by this S3 storage client."));

    // Bucket Inventory
    Task<BucketInventoryConfiguration> GetBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => Task.FromException<BucketInventoryConfiguration>(new NotSupportedException("Bucket inventory configuration is not implemented by this S3 storage client."));
    Task<BucketInventoryConfiguration> PutBucketInventoryConfigurationAsync(PutBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketInventoryConfiguration>(new NotSupportedException("Bucket inventory configuration is not implemented by this S3 storage client."));
    Task DeleteBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket inventory configuration is not implemented by this S3 storage client."));
    Task<IReadOnlyList<BucketInventoryConfiguration>> ListBucketInventoryConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<IReadOnlyList<BucketInventoryConfiguration>>(new NotSupportedException("Listing bucket inventory configurations is not implemented by this S3 storage client."));

    // Bucket Intelligent-Tiering
    Task<BucketIntelligentTieringConfiguration> GetBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => Task.FromException<BucketIntelligentTieringConfiguration>(new NotSupportedException("Bucket intelligent-tiering configuration is not implemented by this S3 storage client."));
    Task<BucketIntelligentTieringConfiguration> PutBucketIntelligentTieringConfigurationAsync(PutBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<BucketIntelligentTieringConfiguration>(new NotSupportedException("Bucket intelligent-tiering configuration is not implemented by this S3 storage client."));
    Task DeleteBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("Bucket intelligent-tiering configuration is not implemented by this S3 storage client."));
    Task<IReadOnlyList<BucketIntelligentTieringConfiguration>> ListBucketIntelligentTieringConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
        => Task.FromException<IReadOnlyList<BucketIntelligentTieringConfiguration>>(new NotSupportedException("Listing bucket intelligent-tiering configurations is not implemented by this S3 storage client."));

    // Object Lock Write Operations
    Task<ObjectRetentionInfo> PutObjectRetentionAsync(Abstractions.Requests.PutObjectRetentionRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<ObjectRetentionInfo>(new NotSupportedException("Object retention is not implemented by this S3 storage client."));
    Task<ObjectLegalHoldInfo> PutObjectLegalHoldAsync(Abstractions.Requests.PutObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<ObjectLegalHoldInfo>(new NotSupportedException("Object legal hold is not implemented by this S3 storage client."));

    // Restore Object
    Task<S3RestoreObjectResult> RestoreObjectAsync(Abstractions.Requests.RestoreObjectRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<S3RestoreObjectResult>(new NotSupportedException("RestoreObject is not implemented by this S3 storage client."));

    // Select Object Content
    Task<S3SelectObjectContentResult> SelectObjectContentAsync(Abstractions.Requests.SelectObjectContentRequest request, CancellationToken cancellationToken = default)
        => Task.FromException<S3SelectObjectContentResult>(new NotSupportedException("SelectObjectContent is not implemented by this S3 storage client."));

    // Object tags
    Task<IReadOnlyDictionary<string, string>> GetObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);

    Task PutObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken = default);

    Task DeleteObjectTagsAsync(
        string bucketName,
        string key,
        string? versionId,
        CancellationToken cancellationToken = default);
}
