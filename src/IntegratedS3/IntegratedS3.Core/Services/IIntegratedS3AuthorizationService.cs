using System.Security.Claims;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

public interface IIntegratedS3AuthorizationService
{
    ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default);
}