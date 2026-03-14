namespace IntegratedS3.AspNetCore;

public sealed class IntegratedS3EndpointAuthorizationOptions
{
    public bool RequireAuthorization { get; set; }

    public bool AllowAnonymous { get; set; }

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
