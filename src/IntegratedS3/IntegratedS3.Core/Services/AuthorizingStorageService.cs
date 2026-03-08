using System.Runtime.CompilerServices;
using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

internal sealed class AuthorizingStorageService(
    OrchestratedStorageService inner,
    IIntegratedS3AuthorizationService authorizationService,
    IIntegratedS3RequestContextAccessor requestContextAccessor) : IStorageService
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
        }, innerCancellationToken => inner.CreateBucketAsync(request, innerCancellationToken), cancellationToken);
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
        }, innerCancellationToken => inner.DeleteBucketAsync(request, innerCancellationToken), cancellationToken);
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
        }, innerCancellationToken => inner.CopyObjectAsync(request, innerCancellationToken), cancellationToken);
    }

    public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.PutObject,
            BucketName = request.BucketName,
            Key = request.Key,
            IncludesMetadata = request.Metadata is not null
        }, innerCancellationToken => inner.PutObjectAsync(request, innerCancellationToken), cancellationToken);
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

    public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.CompleteMultipartUpload,
            BucketName = request.BucketName,
            Key = request.Key
        }, innerCancellationToken => inner.CompleteMultipartUploadAsync(request, innerCancellationToken), cancellationToken);
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

    public ValueTask<StorageResult> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
    {
        return ExecuteAuthorizedAsync(new StorageAuthorizationRequest
        {
            Operation = StorageOperationType.DeleteObject,
            BucketName = request.BucketName,
            Key = request.Key,
            VersionId = request.VersionId
        }, innerCancellationToken => inner.DeleteObjectAsync(request, innerCancellationToken), cancellationToken);
    }

    private async ValueTask<StorageResult> AuthorizeAsync(StorageAuthorizationRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        var principal = requestContextAccessor.Current?.Principal ?? new ClaimsPrincipal(new ClaimsIdentity());
        var result = await authorizationService.AuthorizeAsync(principal, request, cancellationToken);
        if (result.IsSuccess) {
            return result;
        }

        return StorageResult.Failure(result.Error ?? CreateAccessDeniedError(request));
    }

    private async ValueTask<StorageResult<T>> ExecuteAuthorizedAsync<T>(
        StorageAuthorizationRequest authorizationRequest,
        Func<CancellationToken, ValueTask<StorageResult<T>>> action,
        CancellationToken cancellationToken)
    {
        var authorizationResult = await AuthorizeAsync(authorizationRequest, cancellationToken);
        if (!authorizationResult.IsSuccess) {
            return StorageResult<T>.Failure(authorizationResult.Error!);
        }

        return await action(cancellationToken);
    }

    private async ValueTask<StorageResult> ExecuteAuthorizedAsync(
        StorageAuthorizationRequest authorizationRequest,
        Func<CancellationToken, ValueTask<StorageResult>> action,
        CancellationToken cancellationToken)
    {
        var authorizationResult = await AuthorizeAsync(authorizationRequest, cancellationToken);
        if (!authorizationResult.IsSuccess) {
            return authorizationResult;
        }

        return await action(cancellationToken);
    }

    private async IAsyncEnumerable<T> ExecuteAuthorizedEnumerableAsync<T>(
        StorageAuthorizationRequest authorizationRequest,
        Func<CancellationToken, IAsyncEnumerable<T>> action,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var authorizationResult = await AuthorizeAsync(authorizationRequest, cancellationToken);
        if (!authorizationResult.IsSuccess) {
            throw new StorageAuthorizationException(authorizationResult.Error!);
        }

        await foreach (var item in action(cancellationToken).WithCancellation(cancellationToken)) {
            yield return item;
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