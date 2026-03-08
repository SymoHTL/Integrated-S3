using System.Security.Claims;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

internal sealed class AllowAllIntegratedS3AuthorizationService : IIntegratedS3AuthorizationService
{
    public ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(request);

        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(StorageResult.Success());
    }
}