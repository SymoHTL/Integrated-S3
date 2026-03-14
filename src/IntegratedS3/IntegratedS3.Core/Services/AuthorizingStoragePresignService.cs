using System.Diagnostics;
using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Core.Services;

internal sealed class AuthorizingStoragePresignService(
    IIntegratedS3AuthorizationService authorizationService,
    IStoragePresignStrategy strategy,
    IIntegratedS3RequestContextAccessor requestContextAccessor,
    ILogger<AuthorizingStoragePresignService> logger) : IStoragePresignService
{
    public async ValueTask<StorageResult<StoragePresignedRequest>> PresignObjectAsync(
        ClaimsPrincipal principal,
        StoragePresignRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var authorizationRequest = CreateAuthorizationRequest(request);
        var existing = requestContextAccessor.Current;
        var requestContext = new IntegratedS3RequestContext
        {
            Principal = principal,
            CorrelationId = existing?.CorrelationId,
            RequestId = existing?.RequestId
        };
        using var scope = IntegratedS3CoreTelemetry.BeginOperationScope(logger, authorizationRequest, requestContext);
        using var activity = IntegratedS3CoreTelemetry.StartOperationActivity(authorizationRequest, requestContext);
        var startedAt = Stopwatch.GetTimestamp();

        try {
            ValidateRequest(request);

            var authorizationResult = await authorizationService.AuthorizeAsync(
                principal,
                authorizationRequest,
                cancellationToken);

            if (!authorizationResult.IsSuccess) {
                var error = authorizationResult.Error ?? CreateAccessDeniedError(request);
                IntegratedS3CoreTelemetry.RecordAuthorizationFailure(authorizationRequest, error);
                IntegratedS3CoreTelemetry.MarkFailure(activity, error);
                IntegratedS3CoreTelemetry.RecordStorageOperation(authorizationRequest, StorageResult.Failure(error), Stopwatch.GetElapsedTime(startedAt));
                logger.LogWarning(
                    "IntegratedS3 presign authorization denied for {Operation}. ErrorCode {ErrorCode}.",
                    authorizationRequest.Operation,
                    error.Code);
                return StorageResult<StoragePresignedRequest>.Failure(error);
            }

            var result = await strategy.PresignObjectAsync(principal, request, cancellationToken);
            if (!result.IsSuccess) {
                IntegratedS3CoreTelemetry.MarkFailure(activity, result.Error);
                logger.LogWarning(
                    "IntegratedS3 presign operation {Operation} failed with {ErrorCode}.",
                    authorizationRequest.Operation,
                    result.Error?.Code);
            }
            else {
                activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
                logger.LogDebug("IntegratedS3 presign operation {Operation} completed successfully.", authorizationRequest.Operation);
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
            logger.LogError(exception, "IntegratedS3 presign operation {Operation} failed unexpectedly.", authorizationRequest.Operation);
            throw;
        }
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
