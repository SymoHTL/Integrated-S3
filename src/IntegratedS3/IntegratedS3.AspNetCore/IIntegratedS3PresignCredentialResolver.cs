using System.Security.Claims;

namespace IntegratedS3.AspNetCore;

public interface IIntegratedS3PresignCredentialResolver
{
    ValueTask<IntegratedS3PresignCredentialResolutionResult> ResolveAsync(
        ClaimsPrincipal principal,
        CancellationToken cancellationToken = default);
}
