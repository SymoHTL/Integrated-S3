using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

internal sealed class UnsupportedStoragePresignStrategy : IStoragePresignStrategy
{
    public ValueTask<StorageResult<StoragePresignedRequest>> PresignObjectAsync(
        ClaimsPrincipal principal,
        StoragePresignRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(StorageResult<StoragePresignedRequest>.Failure(new StorageError
        {
            Code = StorageErrorCode.UnsupportedCapability,
            Message = "No storage presign strategy is configured for the current application.",
            BucketName = request.BucketName,
            ObjectKey = request.Key,
            VersionId = request.VersionId,
            SuggestedHttpStatusCode = 501
        }));
    }
}
