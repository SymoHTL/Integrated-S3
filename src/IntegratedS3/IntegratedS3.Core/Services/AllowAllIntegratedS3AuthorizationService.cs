using System.Security.Claims;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;

namespace IntegratedS3.Core.Services;

/// <summary>
/// The default <see cref="IIntegratedS3AuthorizationService"/>. It authorizes <b>every</b> request — the
/// <c>scope</c> claims carried by authenticated principals are <b>not</b> consulted, so any valid credential can
/// act on any bucket or object.
/// </summary>
/// <remarks>
/// This permissive default exists for backward compatibility and simple single-tenant deployments. For any
/// deployment that provisions scoped keys and expects least-privilege, register the scope-enforcing service via
/// <c>services.AddIntegratedS3ScopeBasedAuthorization()</c> (see
/// <see cref="ScopeBasedIntegratedS3AuthorizationService"/>), or provide a custom
/// <see cref="IIntegratedS3AuthorizationService"/>. Note that authorization is separate from authentication:
/// enabling SigV4 only verifies <em>who</em> a caller is, not <em>what</em> they may do.
/// </remarks>
internal sealed class AllowAllIntegratedS3AuthorizationService : IIntegratedS3AuthorizationService
{
    public ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(request);

        cancellationToken.ThrowIfCancellationRequested();

        // WARNING: allow-all. Scope claims are intentionally ignored here. Register
        // ScopeBasedIntegratedS3AuthorizationService (AddIntegratedS3ScopeBasedAuthorization) to enforce scopes.
        return ValueTask.FromResult(StorageResult.Success());
    }
}