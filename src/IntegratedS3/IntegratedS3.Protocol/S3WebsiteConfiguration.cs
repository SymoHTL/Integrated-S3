namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the static website hosting configuration for an S3 bucket.
/// </summary>
public sealed class S3WebsiteConfiguration
{
    /// <summary>The index document configuration.</summary>
    public S3WebsiteIndexDocument? IndexDocument { get; init; }

    /// <summary>The error document configuration.</summary>
    public S3WebsiteErrorDocument? ErrorDocument { get; init; }

    /// <summary>The redirect configuration when all requests should be redirected to another host.</summary>
    public S3WebsiteRedirectAllRequestsTo? RedirectAllRequestsTo { get; init; }

    /// <summary>The routing rules for conditional redirects.</summary>
    public IReadOnlyList<S3WebsiteRoutingRule> RoutingRules { get; init; } = [];
}

/// <summary>
/// Specifies the suffix appended to request paths that target a directory (e.g., <c>index.html</c>).
/// </summary>
public sealed class S3WebsiteIndexDocument
{
    /// <summary>The suffix appended when a request targets a directory.</summary>
    public string Suffix { get; init; } = string.Empty;
}

/// <summary>
/// Specifies the object key returned when an error occurs.
/// </summary>
public sealed class S3WebsiteErrorDocument
{
    /// <summary>The object key of the error document.</summary>
    public string Key { get; init; } = string.Empty;
}

/// <summary>
/// Specifies that all requests to the website endpoint should be redirected to another host.
/// </summary>
public sealed class S3WebsiteRedirectAllRequestsTo
{
    /// <summary>The host name to redirect all requests to.</summary>
    public string HostName { get; init; } = string.Empty;

    /// <summary>The protocol to use for the redirect (<c>http</c> or <c>https</c>).</summary>
    public string? Protocol { get; init; }
}

/// <summary>
/// Represents a conditional routing rule for an S3 static website.
/// </summary>
public sealed class S3WebsiteRoutingRule
{
    /// <summary>The condition that must be met for the redirect to apply.</summary>
    public S3WebsiteRoutingRuleCondition? Condition { get; init; }

    /// <summary>The redirect destination and behavior.</summary>
    public required S3WebsiteRoutingRuleRedirect Redirect { get; init; }
}

/// <summary>
/// Specifies the conditions under which an <see cref="S3WebsiteRoutingRule"/> redirect is applied.
/// </summary>
public sealed class S3WebsiteRoutingRuleCondition
{
    /// <summary>The key prefix that must match for the condition to be met.</summary>
    public string? KeyPrefixEquals { get; init; }

    /// <summary>The HTTP error code that must be returned for the condition to be met.</summary>
    public int? HttpErrorCodeReturnedEquals { get; init; }
}

/// <summary>
/// Specifies the redirect target for an <see cref="S3WebsiteRoutingRule"/>.
/// </summary>
public sealed class S3WebsiteRoutingRuleRedirect
{
    /// <summary>The host name to redirect to.</summary>
    public string? HostName { get; init; }

    /// <summary>The protocol to use for the redirect (<c>http</c> or <c>https</c>).</summary>
    public string? Protocol { get; init; }

    /// <summary>The prefix replacement for the object key in the redirect.</summary>
    public string? ReplaceKeyPrefixWith { get; init; }

    /// <summary>The full key replacement for the object key in the redirect.</summary>
    public string? ReplaceKeyWith { get; init; }

    /// <summary>The HTTP redirect status code (e.g., <c>301</c>, <c>302</c>).</summary>
    public int? HttpRedirectCode { get; init; }
}
