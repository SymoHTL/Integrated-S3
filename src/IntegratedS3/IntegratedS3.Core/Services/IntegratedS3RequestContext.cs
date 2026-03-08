using System.Security.Claims;

namespace IntegratedS3.Core.Services;

public sealed class IntegratedS3RequestContext
{
    public ClaimsPrincipal Principal { get; init; } = new(new ClaimsIdentity());
}