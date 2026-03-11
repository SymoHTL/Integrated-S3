using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

internal sealed class AuthorizingStoragePresignService(
    IIntegratedS3AuthorizationService authorizationService,
    IStoragePresignStrategy strategy) : IStoragePresignService
{
    public async ValueTask<StorageResult<StoragePresignedRequest>> PresignObjectAsync(
        ClaimsPrincipal principal,
        StoragePresignRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        ValidateRequest(request);

        var authorizationResult = await authorizationService.AuthorizeAsync(
            principal,
            CreateAuthorizationRequest(request),
            cancellationToken);

        if (!authorizationResult.IsSuccess) {
            return StorageResult<StoragePresignedRequest>.Failure(authorizationResult.Error ?? CreateAccessDeniedError(request));
        }

        return await strategy.PresignObjectAsync(principal, request, cancellationToken);
    }

    private static StorageAuthorizationRequest CreateAuthorizationRequest(StoragePresignRequest request)
    {
        return request.Operation switch
        {
            StoragePresignOperation.GetObject => new StorageAuthorizationRequest
            {
                Operation = StorageOperationType.PresignGetObject,
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId
            },
            StoragePresignOperation.PutObject => new StorageAuthorizationRequest
            {
                Operation = StorageOperationType.PresignPutObject,
                BucketName = request.BucketName,
                Key = request.Key
            },
            _ => throw new ArgumentOutOfRangeException(nameof(request), request.Operation, "The requested presign operation is not supported.")
        };
    }

    private static void ValidateRequest(StoragePresignRequest request)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(request.BucketName);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.Key);

        if (request.ExpiresInSeconds <= 0) {
            throw new ArgumentOutOfRangeException(nameof(request.ExpiresInSeconds), request.ExpiresInSeconds, "The presign expiry must be a positive number of seconds.");
        }

        if (request.Operation == StoragePresignOperation.PutObject
            && !string.IsNullOrWhiteSpace(request.VersionId)) {
            throw new ArgumentException("Presigned uploads do not support version-specific targets.", nameof(request));
        }

        if (request.Operation == StoragePresignOperation.GetObject
            && !string.IsNullOrWhiteSpace(request.ContentType)) {
            throw new ArgumentException("ContentType is only supported for presigned uploads.", nameof(request));
        }
    }

    private static StorageError CreateAccessDeniedError(StoragePresignRequest request)
    {
        return new StorageError
        {
            Code = StorageErrorCode.AccessDenied,
            Message = $"The current principal is not authorized to create a presigned request for '{request.Operation}'.",
            BucketName = request.BucketName,
            ObjectKey = request.Key,
            VersionId = request.VersionId,
            SuggestedHttpStatusCode = 403
        };
    }
}
