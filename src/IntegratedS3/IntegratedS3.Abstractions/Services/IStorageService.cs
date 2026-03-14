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

    ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default);

    IAsyncEnumerable<MultipartUploadInfo> ListMultipartUploadsAsync(ListMultipartUploadsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default);

    ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default);
}
