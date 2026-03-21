namespace IntegratedS3.AspNetCore;

/// <summary>
/// Authorization options applied to IntegratedS3 endpoint route groups.
/// </summary>
/// <remarks>
/// Configurable per feature group (service, bucket, object, multipart, admin) to control
/// access policies. Instances of this class are used by <see cref="IntegratedS3Options"/>
/// to apply ASP.NET Core authorization conventions to mapped endpoint groups.
/// </remarks>
public sealed class IntegratedS3EndpointAuthorizationOptions
{
    /// <summary>
    /// When <see langword="true"/>, applies <c>RequireAuthorization()</c> to the route group.
    /// Unauthenticated requests will be rejected.
    /// </summary>
    public bool RequireAuthorization { get; set; }

    /// <summary>
    /// When <see langword="true"/>, applies <c>AllowAnonymous()</c> to the route group.
    /// </summary>
    /// <remarks>
    /// Overrides <see cref="RequireAuthorization"/> when both are set.
    /// </remarks>
    public bool AllowAnonymous { get; set; }

    /// <summary>
    /// Named authorization policies to apply to the route group.
    /// </summary>
    /// <remarks>
    /// Each policy must be registered in the ASP.NET Core authorization system via
    /// <c>AddAuthorization</c> before it can be referenced here.
    /// </remarks>
    public string[] PolicyNames { get; set; } = [];

    internal bool HasConventions => RequireAuthorization || AllowAnonymous || PolicyNames.Length > 0;

    internal IntegratedS3EndpointAuthorizationOptions Clone()
    {
        return new IntegratedS3EndpointAuthorizationOptions
        {
            RequireAuthorization = RequireAuthorization,
            AllowAnonymous = AllowAnonymous,
            PolicyNames = (PolicyNames ?? [])
                .Where(static policyName => !string.IsNullOrWhiteSpace(policyName))
                .Select(static policyName => policyName.Trim())
                .Distinct(StringComparer.Ordinal)
                .ToArray()
        };
    }
}
