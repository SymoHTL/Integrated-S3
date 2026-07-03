namespace IntegratedS3.Core.Options;

/// <summary>
/// Configures <see cref="Services.ScopeBasedIntegratedS3AuthorizationService"/>, the opt-in authorization
/// service that enforces the <c>scope</c> claims carried by an authenticated principal.
/// </summary>
/// <remarks>
/// <para>Scope grammar (a principal is granted an operation when <em>any</em> of its scopes matches):</para>
/// <list type="bullet">
///   <item><description><c>admin</c> or <c>*</c> — grants every operation.</description></item>
///   <item><description><c>read</c> — grants read-class operations (all <c>List*</c>, <c>Get*</c>, <c>Head*</c>, and read presigns).</description></item>
///   <item><description><c>write</c> — grants write-class operations (everything that is not read-class).</description></item>
///   <item><description><c>bucket:NAME</c> — grants every operation on bucket <c>NAME</c>.</description></item>
///   <item><description><c>bucket:NAME:read</c> / <c>bucket:NAME:write</c> — grants read/write-class operations on bucket <c>NAME</c>.</description></item>
/// </list>
/// <para>Bucket names are matched case-sensitively (S3 semantics); the <c>admin</c>/<c>read</c>/<c>write</c>
/// keywords are matched case-insensitively.</para>
/// </remarks>
public sealed class ScopeBasedIntegratedS3AuthorizationOptions
{
    /// <summary>
    /// When <see langword="true"/>, a principal that carries no <c>scope</c> claims is granted all operations.
    /// Defaults to <see langword="false"/> (least privilege): unscoped principals are denied.
    /// </summary>
    /// <remarks>
    /// Set to <see langword="true"/> only when you want authenticated-but-unscoped keys to retain full access
    /// (for example while migrating existing keys that were provisioned without scopes).
    /// </remarks>
    public bool AllowUnscopedPrincipals { get; set; }
}
