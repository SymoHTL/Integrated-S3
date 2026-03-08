using Microsoft.AspNetCore.Http;

namespace IntegratedS3.AspNetCore.Services;

public interface IIntegratedS3RequestAuthenticator
{
    ValueTask<IntegratedS3RequestAuthenticationResult> AuthenticateAsync(HttpContext httpContext, CancellationToken cancellationToken = default);
}
