using System.Security.Claims;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

public interface IStoragePresignStrategy
{
    ValueTask<StorageResult<StoragePresignedRequest>> PresignObjectAsync(
        ClaimsPrincipal principal,
        StoragePresignRequest request,
        CancellationToken cancellationToken = default);
}
