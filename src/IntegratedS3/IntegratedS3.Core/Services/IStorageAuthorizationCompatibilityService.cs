using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

public interface IStorageAuthorizationCompatibilityService
{
    ValueTask RecordBucketCreatedAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask RecordBucketDeletedAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask RecordObjectWrittenAsync(string bucketName, string key, CancellationToken cancellationToken = default);

    ValueTask RecordObjectDeletedAsync(string bucketName, string key, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<StorageCannedAcl>> GetBucketAclAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> PutBucketAclAsync(PutBucketAclCompatibilityRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<StorageCannedAcl>> GetObjectAclAsync(string bucketName, string key, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> PutObjectAclAsync(PutObjectAclCompatibilityRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<BucketPolicyCompatibilityDocument?>> GetBucketPolicyAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> PutBucketPolicyAsync(PutBucketPolicyCompatibilityRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> DeleteBucketPolicyAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<bool> IsAllowedAsync(StorageAuthorizationRequest request, CancellationToken cancellationToken = default);
}
