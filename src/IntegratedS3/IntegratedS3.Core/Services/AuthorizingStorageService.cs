using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Models;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Core.Services;

internal sealed class AuthorizingStorageService(
    OrchestratedStorageService inner,
    IIntegratedS3AuthorizationService authorizationService,
    IStorageAuthorizationCompatibilityService authorizationCompatibilityService,
    IIntegratedS3RequestContextAccessor requestContextAccessor,
    ILogger<AuthorizingStorageService> logger) : IStorageService
{
    public IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedEnumerableAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListBuckets
        }, inner.ListBucketsAsync, cancellationToken);
    }

    public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.CreateBucket,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.CreateBucketAsync(request, innerCancellationToken), (_, innerCancellationToken) => authorizationCompatibilityService.RecordBucketCreatedAsync(request.BucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketLocationInfo>> GetBucketLocationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketLocation,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketLocationAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketVersioning,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketVersioningAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketVersioning,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketVersioningAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketCorsConfiguration>> GetBucketCorsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketCors,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketCorsAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketCorsConfiguration>> PutBucketCorsAsync(PutBucketCorsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketCors,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketCorsAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketCorsAsync(DeleteBucketCorsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketCors,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketCorsAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> GetBucketDefaultEncryptionAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketDefaultEncryption,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketDefaultEncryptionAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketDefaultEncryptionConfiguration>> PutBucketDefaultEncryptionAsync(PutBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketDefaultEncryption,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketDefaultEncryptionAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketDefaultEncryptionAsync(DeleteBucketDefaultEncryptionRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketDefaultEncryption,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketDefaultEncryptionAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.HeadBucket,
            BucketName = bucketName
        }, innerCancellationToken => inner.HeadBucketAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucket,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketAsync(request, innerCancellationToken), innerCancellationToken => authorizationCompatibilityService.RecordBucketDeletedAsync(request.BucketName, innerCancellationToken), cancellationToken);
    }

    public IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedEnumerableAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListObjects,
            BucketName = request.BucketName,
            Key = request.Prefix
        }, innerCancellationToken => inner.ListObjectsAsync(request, innerCancellationToken), cancellationToken);
    }

    public IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedEnumerableAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListObjectVersions,
            BucketName = request.BucketName,
            Key = request.Prefix
        }, innerCancellationToken => inner.ListObjectVersionsAsync(request, innerCancellationToken), cancellationToken);
    }

    public IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedEnumerableAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListMultipartUploads,
            BucketName = request.BucketName,
            Key = request.Prefix
        }, innerCancellationToken => inner.ListMultipartUploadsAsync(request, innerCancellationToken), cancellationToken);
    }

    public IAsyncEnumerable<MultipartUploadPart> ListMultipartPartsAsync(ListMultipartPartsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedEnumerableAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListMultipartParts,
            BucketName = request.BucketName,
            Key = request.Key
        }, innerCancellationToken => inner.ListMultipartPartsAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetObject,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.GetObjectAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectRetentionInfo>> GetObjectRetentionAsync(GetObjectRetentionRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetObject,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.GetObjectRetentionAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectLegalHoldInfo>> GetObjectLegalHoldAsync(GetObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetObject,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.GetObjectLegalHoldAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetObjectTags,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.GetObjectTagsAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.CopyObject,
            BucketName = request.DestinationBucketName,
            Key = request.DestinationKey,
            SourceBucketName = request.SourceBucketName,
            SourceKey = request.SourceKey,
            VersionId = request.SourceVersionId,
            IncludesMetadata = true
        }, innerCancellationToken => inner.CopyObjectAsync(request, innerCancellationToken), (_, innerCancellationToken) => authorizationCompatibilityService.RecordObjectWrittenAsync(request.DestinationBucketName, request.DestinationKey, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutObject,
            BucketName = request.BucketName,
            Key = request.Key,
            IncludesMetadata = request.Metadata is not null
        }, innerCancellationToken => inner.PutObjectAsync(request, innerCancellationToken), (_, innerCancellationToken) => authorizationCompatibilityService.RecordObjectWrittenAsync(request.BucketName, request.Key, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutObjectTags,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.PutObjectTagsAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteObjectTags,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.DeleteObjectTagsAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.InitiateMultipartUpload,
            BucketName = request.BucketName,
            Key = request.Key,
            IncludesMetadata = request.Metadata is not null
        }, innerCancellationToken => inner.InitiateMultipartUploadAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.UploadMultipartPart,
            BucketName = request.BucketName,
            Key = request.Key
        }, innerCancellationToken => inner.UploadMultipartPartAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<MultipartUploadPart>> UploadPartCopyAsync(UploadPartCopyRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.UploadPartCopy,
            BucketName = request.BucketName,
            Key = request.Key,
            SourceBucketName = request.SourceBucketName,
            SourceKey = request.SourceKey,
            VersionId = request.SourceVersionId
        }, innerCancellationToken => inner.UploadPartCopyAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.CompleteMultipartUpload,
            BucketName = request.BucketName,
            Key = request.Key
        }, innerCancellationToken => inner.CompleteMultipartUploadAsync(request, innerCancellationToken), (_, innerCancellationToken) => authorizationCompatibilityService.RecordObjectWrittenAsync(request.BucketName, request.Key, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.AbortMultipartUpload,
            BucketName = request.BucketName,
            Key = request.Key
        }, innerCancellationToken => inner.AbortMultipartUploadAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.HeadObject,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.HeadObjectAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteObject,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.DeleteObjectAsync(request, innerCancellationToken), (_, innerCancellationToken) => authorizationCompatibilityService.RecordObjectDeletedAsync(request.BucketName, request.Key, innerCancellationToken), cancellationToken);
    }

    // Bucket Tagging
    public ValueTask<StorageResult<BucketTaggingConfiguration>> GetBucketTaggingAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketTagging,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketTaggingAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketTaggingConfiguration>> PutBucketTaggingAsync(PutBucketTaggingRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketTagging,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketTaggingAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketTaggingAsync(DeleteBucketTaggingRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketTagging,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketTaggingAsync(request, innerCancellationToken), cancellationToken);
    }

    // Bucket Logging
    public ValueTask<StorageResult<BucketLoggingConfiguration>> GetBucketLoggingAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketLogging,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketLoggingAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketLoggingConfiguration>> PutBucketLoggingAsync(PutBucketLoggingRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketLogging,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketLoggingAsync(request, innerCancellationToken), cancellationToken);
    }

    // Bucket Website
    public ValueTask<StorageResult<BucketWebsiteConfiguration>> GetBucketWebsiteAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketWebsite,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketWebsiteAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketWebsiteConfiguration>> PutBucketWebsiteAsync(PutBucketWebsiteRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketWebsite,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketWebsiteAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketWebsiteAsync(DeleteBucketWebsiteRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketWebsite,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketWebsiteAsync(request, innerCancellationToken), cancellationToken);
    }

    // Bucket Request Payment
    public ValueTask<StorageResult<BucketRequestPaymentConfiguration>> GetBucketRequestPaymentAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketRequestPayment,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketRequestPaymentAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketRequestPaymentConfiguration>> PutBucketRequestPaymentAsync(PutBucketRequestPaymentRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketRequestPayment,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketRequestPaymentAsync(request, innerCancellationToken), cancellationToken);
    }

    // Bucket Accelerate
    public ValueTask<StorageResult<BucketAccelerateConfiguration>> GetBucketAccelerateAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketAccelerate,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketAccelerateAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketAccelerateConfiguration>> PutBucketAccelerateAsync(PutBucketAccelerateRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketAccelerate,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketAccelerateAsync(request, innerCancellationToken), cancellationToken);
    }

    // Bucket Lifecycle
    public ValueTask<StorageResult<BucketLifecycleConfiguration>> GetBucketLifecycleAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketLifecycle,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketLifecycleAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketLifecycleConfiguration>> PutBucketLifecycleAsync(PutBucketLifecycleRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketLifecycle,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketLifecycleAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketLifecycleAsync(DeleteBucketLifecycleRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketLifecycle,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketLifecycleAsync(request, innerCancellationToken), cancellationToken);
    }

    // Bucket Replication
    public ValueTask<StorageResult<BucketReplicationConfiguration>> GetBucketReplicationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketReplication,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketReplicationAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketReplicationConfiguration>> PutBucketReplicationAsync(PutBucketReplicationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketReplication,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketReplicationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketReplicationAsync(DeleteBucketReplicationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketReplication,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketReplicationAsync(request, innerCancellationToken), cancellationToken);
    }

    // Bucket Notifications
    public ValueTask<StorageResult<BucketNotificationConfiguration>> GetBucketNotificationConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketNotificationConfiguration,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketNotificationConfigurationAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketNotificationConfiguration>> PutBucketNotificationConfigurationAsync(PutBucketNotificationConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketNotificationConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketNotificationConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    // Object Lock Configuration (bucket-level)
    public ValueTask<StorageResult<ObjectLockConfiguration>> GetObjectLockConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetObjectLockConfiguration,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetObjectLockConfigurationAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectLockConfiguration>> PutObjectLockConfigurationAsync(PutObjectLockConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutObjectLockConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutObjectLockConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    // Bucket Analytics
    public ValueTask<StorageResult<BucketAnalyticsConfiguration>> GetBucketAnalyticsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketAnalyticsConfiguration,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketAnalyticsConfigurationAsync(bucketName, id, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketAnalyticsConfiguration>> PutBucketAnalyticsConfigurationAsync(PutBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketAnalyticsConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketAnalyticsConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketAnalyticsConfigurationAsync(DeleteBucketAnalyticsConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketAnalyticsConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketAnalyticsConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<IReadOnlyList<BucketAnalyticsConfiguration>>> ListBucketAnalyticsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListBucketAnalyticsConfigurations,
            BucketName = bucketName
        }, innerCancellationToken => inner.ListBucketAnalyticsConfigurationsAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    // Bucket Metrics
    public ValueTask<StorageResult<BucketMetricsConfiguration>> GetBucketMetricsConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketMetricsConfiguration,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketMetricsConfigurationAsync(bucketName, id, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketMetricsConfiguration>> PutBucketMetricsConfigurationAsync(PutBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketMetricsConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketMetricsConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketMetricsConfigurationAsync(DeleteBucketMetricsConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketMetricsConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketMetricsConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<IReadOnlyList<BucketMetricsConfiguration>>> ListBucketMetricsConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListBucketMetricsConfigurations,
            BucketName = bucketName
        }, innerCancellationToken => inner.ListBucketMetricsConfigurationsAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    // Bucket Inventory
    public ValueTask<StorageResult<BucketInventoryConfiguration>> GetBucketInventoryConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketInventoryConfiguration,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketInventoryConfigurationAsync(bucketName, id, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketInventoryConfiguration>> PutBucketInventoryConfigurationAsync(PutBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketInventoryConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketInventoryConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketInventoryConfigurationAsync(DeleteBucketInventoryConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketInventoryConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketInventoryConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<IReadOnlyList<BucketInventoryConfiguration>>> ListBucketInventoryConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListBucketInventoryConfigurations,
            BucketName = bucketName
        }, innerCancellationToken => inner.ListBucketInventoryConfigurationsAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    // Bucket Intelligent-Tiering
    public ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> GetBucketIntelligentTieringConfigurationAsync(string bucketName, string id, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.GetBucketIntelligentTieringConfiguration,
            BucketName = bucketName
        }, innerCancellationToken => inner.GetBucketIntelligentTieringConfigurationAsync(bucketName, id, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<BucketIntelligentTieringConfiguration>> PutBucketIntelligentTieringConfigurationAsync(PutBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutBucketIntelligentTieringConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.PutBucketIntelligentTieringConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult> DeleteBucketIntelligentTieringConfigurationAsync(DeleteBucketIntelligentTieringConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteBucketIntelligentTieringConfiguration,
            BucketName = request.BucketName
        }, innerCancellationToken => inner.DeleteBucketIntelligentTieringConfigurationAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<IReadOnlyList<BucketIntelligentTieringConfiguration>>> ListBucketIntelligentTieringConfigurationsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.ListBucketIntelligentTieringConfigurations,
            BucketName = bucketName
        }, innerCancellationToken => inner.ListBucketIntelligentTieringConfigurationsAsync(bucketName, innerCancellationToken), cancellationToken);
    }

    // Object Lock Write Operations
    public ValueTask<StorageResult<ObjectRetentionInfo>> PutObjectRetentionAsync(PutObjectRetentionRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutObjectRetention,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.PutObjectRetentionAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectLegalHoldInfo>> PutObjectLegalHoldAsync(PutObjectLegalHoldRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutObjectLegalHold,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.PutObjectLegalHoldAsync(request, innerCancellationToken), cancellationToken);
    }

    // SelectObjectContent
    public ValueTask<StorageResult<SelectObjectContentResponse>> SelectObjectContentAsync(SelectObjectContentRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.SelectObjectContent,
            BucketName = request.BucketName,
            Key = request.Key
        }, innerCancellationToken => inner.SelectObjectContentAsync(request, innerCancellationToken), cancellationToken);
    }

    // RestoreObject
    public ValueTask<StorageResult<RestoreObjectResponse>> RestoreObjectAsync(RestoreObjectRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.RestoreObject,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.RestoreObjectAsync(request, innerCancellationToken), cancellationToken);
    }

    private async ValueTask<StorageResult> AuthorizeAsync(StorageAuthorizationRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        var principal = requestContextAccessor.Current?.Principal ?? new ClaimsPrincipal(new ClaimsIdentity());
        var result = await authorizationService.AuthorizeAsync(principal, request, cancellationToken);
        if (result.IsSuccess) {
            return result;
        }

        if ((result.Error is null || result.Error.Code == StorageErrorCode.AccessDenied)
            && await authorizationCompatibilityService.IsAllowedAsync(request, cancellationToken)) {
            return StorageResult.Success();
        }

        var error = result.Error ?? CreateAccessDeniedError(request);
        IntegratedS3CoreTelemetry.RecordAuthorizationFailure(request, error);
        logger.LogWarning(
            "IntegratedS3 authorization denied for {Operation}. ErrorCode {ErrorCode}.",
            request.Operation,
            error.Code);

        return StorageResult.Failure(error);
    }

    private async ValueTask<StorageResult<T>> ExecuteAuthorizedAsync<T>(
        StorageAuthorizationRequest authorizationRequest,
        Func<CancellationToken, ValueTask<StorageResult<T>>> action,
        CancellationToken cancellationToken)
    {
        return await ExecuteAuthorizedAsync(authorizationRequest, action, onSuccess: null, cancellationToken);
    }

    private async ValueTask<StorageResult<T>> ExecuteAuthorizedAsync<T>(
        StorageAuthorizationRequest authorizationRequest,
        Func<CancellationToken, ValueTask<StorageResult<T>>> action,
        Func<T, CancellationToken, ValueTask>? onSuccess,
        CancellationToken cancellationToken)
    {
        var requestContext = requestContextAccessor.Current;
        using var scope = IntegratedS3CoreTelemetry.BeginOperationScope(logger, authorizationRequest, requestContext);
        using var activity = IntegratedS3CoreTelemetry.StartOperationActivity(authorizationRequest, requestContext);
        var startedAt = Stopwatch.GetTimestamp();

        try {
            var authorizationResult = await AuthorizeAsync(authorizationRequest, cancellationToken);
            if (!authorizationResult.IsSuccess) {
                IntegratedS3CoreTelemetry.MarkFailure(activity, authorizationResult.Error);
                IntegratedS3CoreTelemetry.RecordStorageOperation(authorizationRequest, authorizationResult, Stopwatch.GetElapsedTime(startedAt));
                return StorageResult<T>.Failure(authorizationResult.Error!);
            }

            var result = await action(cancellationToken);
            if (result.IsSuccess && result.Value is not null && onSuccess is not null) {
                await onSuccess(result.Value, cancellationToken);
            }

            if (!result.IsSuccess) {
                IntegratedS3CoreTelemetry.MarkFailure(activity, result.Error);
                logger.LogWarning(
                    "IntegratedS3 operation {Operation} failed with {ErrorCode}.",
                    authorizationRequest.Operation,
                    result.Error?.Code);
            }
            else {
                activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
                logger.LogDebug("IntegratedS3 operation {Operation} completed successfully.", authorizationRequest.Operation);
            }

            IntegratedS3CoreTelemetry.RecordStorageOperation(authorizationRequest, result, Stopwatch.GetElapsedTime(startedAt));
            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            IntegratedS3CoreTelemetry.MarkCancelled(activity);
            throw;
        }
        catch (Exception exception) {
            IntegratedS3CoreTelemetry.MarkFailure(activity, "UnhandledException", exception.Message);
            IntegratedS3CoreTelemetry.RecordStorageOperationFailure(authorizationRequest, "UnhandledException", Stopwatch.GetElapsedTime(startedAt));
            logger.LogError(exception, "IntegratedS3 operation {Operation} failed unexpectedly.", authorizationRequest.Operation);
            throw;
        }
    }

    private async ValueTask<StorageResult> ExecuteAuthorizedAsync(
        StorageAuthorizationRequest authorizationRequest,
        Func<CancellationToken, ValueTask<StorageResult>> action,
        CancellationToken cancellationToken)
    {
        return await ExecuteAuthorizedAsync(authorizationRequest, action, onSuccess: null, cancellationToken);
    }

    private async ValueTask<StorageResult> ExecuteAuthorizedAsync(
        StorageAuthorizationRequest authorizationRequest,
        Func<CancellationToken, ValueTask<StorageResult>> action,
        Func<CancellationToken, ValueTask>? onSuccess,
        CancellationToken cancellationToken)
    {
        var requestContext = requestContextAccessor.Current;
        using var scope = IntegratedS3CoreTelemetry.BeginOperationScope(logger, authorizationRequest, requestContext);
        using var activity = IntegratedS3CoreTelemetry.StartOperationActivity(authorizationRequest, requestContext);
        var startedAt = Stopwatch.GetTimestamp();

        try {
            var authorizationResult = await AuthorizeAsync(authorizationRequest, cancellationToken);
            if (!authorizationResult.IsSuccess) {
                IntegratedS3CoreTelemetry.MarkFailure(activity, authorizationResult.Error);
                IntegratedS3CoreTelemetry.RecordStorageOperation(authorizationRequest, authorizationResult, Stopwatch.GetElapsedTime(startedAt));
                return authorizationResult;
            }

            var result = await action(cancellationToken);
            if (result.IsSuccess && onSuccess is not null) {
                await onSuccess(cancellationToken);
            }

            if (!result.IsSuccess) {
                IntegratedS3CoreTelemetry.MarkFailure(activity, result.Error);
                logger.LogWarning(
                    "IntegratedS3 operation {Operation} failed with {ErrorCode}.",
                    authorizationRequest.Operation,
                    result.Error?.Code);
            }
            else {
                activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
                logger.LogDebug("IntegratedS3 operation {Operation} completed successfully.", authorizationRequest.Operation);
            }

            IntegratedS3CoreTelemetry.RecordStorageOperation(authorizationRequest, result, Stopwatch.GetElapsedTime(startedAt));
            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            IntegratedS3CoreTelemetry.MarkCancelled(activity);
            throw;
        }
        catch (Exception exception) {
            IntegratedS3CoreTelemetry.MarkFailure(activity, "UnhandledException", exception.Message);
            IntegratedS3CoreTelemetry.RecordStorageOperationFailure(authorizationRequest, "UnhandledException", Stopwatch.GetElapsedTime(startedAt));
            logger.LogError(exception, "IntegratedS3 operation {Operation} failed unexpectedly.", authorizationRequest.Operation);
            throw;
        }
    }

    private async IAsyncEnumerable<T> ExecuteAuthorizedEnumerableAsync<T>(
        StorageAuthorizationRequest authorizationRequest,
        Func<CancellationToken, IAsyncEnumerable<T>> action,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var requestContext = requestContextAccessor.Current;
        using var scope = IntegratedS3CoreTelemetry.BeginOperationScope(logger, authorizationRequest, requestContext);
        using var activity = IntegratedS3CoreTelemetry.StartOperationActivity(authorizationRequest, requestContext);
        var startedAt = Stopwatch.GetTimestamp();

        var authorizationResult = await AuthorizeAsync(authorizationRequest, cancellationToken);
        if (!authorizationResult.IsSuccess) {
            IntegratedS3CoreTelemetry.MarkFailure(activity, authorizationResult.Error);
            IntegratedS3CoreTelemetry.RecordStorageOperation(authorizationRequest, authorizationResult, Stopwatch.GetElapsedTime(startedAt));
            throw new StorageAuthorizationException(authorizationResult.Error!);
        }

        var completedSuccessfully = false;
        try {
            await foreach (var item in action(cancellationToken).WithCancellation(cancellationToken)) {
                yield return item;
            }

            completedSuccessfully = true;
        }
        finally {
            if (completedSuccessfully) {
                activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
                IntegratedS3CoreTelemetry.RecordStorageOperation(authorizationRequest, StorageResult.Success(), Stopwatch.GetElapsedTime(startedAt));
                logger.LogDebug("IntegratedS3 operation {Operation} streamed successfully.", authorizationRequest.Operation);
            }
            else if (cancellationToken.IsCancellationRequested) {
                IntegratedS3CoreTelemetry.MarkCancelled(activity);
            }
            else {
                IntegratedS3CoreTelemetry.MarkFailure(activity, "UnhandledException", "Streaming failed");
                IntegratedS3CoreTelemetry.RecordStorageOperationFailure(authorizationRequest, "UnhandledException", Stopwatch.GetElapsedTime(startedAt));
            }
        }
    }

    private static StorageError CreateAccessDeniedError(StorageAuthorizationRequest request)
    {
        return new StorageError
        {
            Code = StorageErrorCode.AccessDenied,
            Message = $"The current principal is not authorized to perform '{request.Operation}'.",
            BucketName = request.BucketName,
            ObjectKey = request.Key,
            SuggestedHttpStatusCode = 403
        };
    }
}
