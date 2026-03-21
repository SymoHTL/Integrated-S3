using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

/// <summary>
/// Provides S3-compatible ACL and bucket-policy authorization, allowing IntegratedS3 to
/// emulate the S3 authorization model for compatibility with existing clients and workflows.
/// </summary>
public interface IStorageAuthorizationCompatibilityService
{
    /// <summary>
    /// Records that a bucket was created, initializing its default ACL state.
    /// </summary>
    /// <param name="bucketName">The name of the newly created bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask RecordBucketCreatedAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records that a bucket was deleted, removing its ACL and policy state.
    /// </summary>
    /// <param name="bucketName">The name of the deleted bucket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask RecordBucketDeletedAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records that an object was written, initializing its default ACL state.
    /// </summary>
    /// <param name="bucketName">The bucket containing the object.</param>
    /// <param name="key">The object key.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask RecordObjectWrittenAsync(string bucketName, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records that an object was deleted, removing its ACL state.
    /// </summary>
    /// <param name="bucketName">The bucket containing the object.</param>
    /// <param name="key">The object key.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask RecordObjectDeletedAsync(string bucketName, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the canned ACL currently applied to the specified bucket.
    /// </summary>
    /// <param name="bucketName">The bucket name.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="StorageCannedAcl"/>.</returns>
    ValueTask<StorageResult<StorageCannedAcl>> GetBucketAclAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a canned ACL to the specified bucket.
    /// </summary>
    /// <param name="request">The <see cref="PutBucketAclCompatibilityRequest"/> describing the ACL to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or the reason for failure.</returns>
    ValueTask<StorageResult> PutBucketAclAsync(PutBucketAclCompatibilityRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the canned ACL currently applied to the specified object.
    /// </summary>
    /// <param name="bucketName">The bucket containing the object.</param>
    /// <param name="key">The object key.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="StorageCannedAcl"/>.</returns>
    ValueTask<StorageResult<StorageCannedAcl>> GetObjectAclAsync(string bucketName, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the full ACL compatibility state for the specified object, including inherited bucket ACL.
    /// </summary>
    /// <param name="bucketName">The bucket containing the object.</param>
    /// <param name="key">The object key.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="ObjectAclCompatibilityState"/>.</returns>
    ValueTask<StorageResult<ObjectAclCompatibilityState>> GetObjectAclStateAsync(string bucketName, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a canned ACL to the specified object.
    /// </summary>
    /// <param name="request">The <see cref="PutObjectAclCompatibilityRequest"/> describing the ACL to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or the reason for failure.</returns>
    ValueTask<StorageResult> PutObjectAclAsync(PutObjectAclCompatibilityRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the bucket policy document for the specified bucket, or <see langword="null"/> if none is set.
    /// </summary>
    /// <param name="bucketName">The bucket name.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult{T}"/> containing the <see cref="BucketPolicyCompatibilityDocument"/> or <see langword="null"/>.</returns>
    ValueTask<StorageResult<BucketPolicyCompatibilityDocument?>> GetBucketPolicyAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a bucket policy to the specified bucket.
    /// </summary>
    /// <param name="request">The <see cref="PutBucketPolicyCompatibilityRequest"/> containing the policy to apply.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or the reason for failure.</returns>
    ValueTask<StorageResult> PutBucketPolicyAsync(PutBucketPolicyCompatibilityRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the bucket policy for the specified bucket.
    /// </summary>
    /// <param name="bucketName">The bucket name.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="StorageResult"/> indicating success or the reason for failure.</returns>
    ValueTask<StorageResult> DeleteBucketPolicyAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Evaluates whether the specified storage operation is allowed under the current ACL and policy rules.
    /// </summary>
    /// <param name="request">The <see cref="StorageAuthorizationRequest"/> to evaluate.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns><see langword="true"/> if the operation is allowed; otherwise <see langword="false"/>.</returns>
    ValueTask<bool> IsAllowedAsync(StorageAuthorizationRequest request, CancellationToken cancellationToken = default);
}
