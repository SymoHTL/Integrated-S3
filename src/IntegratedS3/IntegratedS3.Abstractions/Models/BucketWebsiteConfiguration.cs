namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Static website hosting configuration for a bucket.
/// </summary>
public sealed class BucketWebsiteConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The suffix appended to requests for a directory (e.g., "index.html").
    /// </summary>
    public string? IndexDocumentSuffix { get; init; }

    /// <summary>
    /// The object key of the custom error document.
    /// </summary>
    public string? ErrorDocumentKey { get; init; }

    /// <summary>
    /// Redirect configuration that redirects all requests to a different host, or <see langword="null"/> if not set.
    /// </summary>
    public BucketWebsiteRedirectAllRequestsTo? RedirectAllRequestsTo { get; init; }

    /// <summary>
    /// Routing rules that define conditional redirects for specific object key prefixes or HTTP error codes.
    /// </summary>
    public IReadOnlyList<BucketWebsiteRoutingRule> RoutingRules { get; init; } = [];
}

/// <summary>
/// Redirects all website requests to a specified host and optional protocol.
/// </summary>
public sealed class BucketWebsiteRedirectAllRequestsTo
{
    /// <summary>
    /// The host name to redirect all requests to.
    /// </summary>
    public required string HostName { get; init; }

    /// <summary>
    /// The protocol to use for the redirect (e.g., "http" or "https"), or <see langword="null"/> to preserve the original protocol.
    /// </summary>
    public string? Protocol { get; init; }
}

/// <summary>
/// A routing rule that defines a condition and a redirect action for website requests.
/// </summary>
public sealed class BucketWebsiteRoutingRule
{
    /// <summary>
    /// The condition that triggers this routing rule, or <see langword="null"/> to match all requests.
    /// </summary>
    public BucketWebsiteRoutingRuleCondition? Condition { get; init; }

    /// <summary>
    /// The redirect action to perform when the condition is met.
    /// </summary>
    public required BucketWebsiteRoutingRuleRedirect Redirect { get; init; }
}

/// <summary>
/// Condition that must be met for a website routing rule to apply.
/// </summary>
public sealed class BucketWebsiteRoutingRuleCondition
{
    /// <summary>
    /// The object key prefix that triggers the rule, or <see langword="null"/> if not applicable.
    /// </summary>
    public string? KeyPrefixEquals { get; init; }

    /// <summary>
    /// The HTTP error code that triggers the rule, or <see langword="null"/> if not applicable.
    /// </summary>
    public int? HttpErrorCodeReturnedEquals { get; init; }
}

/// <summary>
/// Redirect action performed by a website routing rule.
/// </summary>
public sealed class BucketWebsiteRoutingRuleRedirect
{
    /// <summary>
    /// The host name to redirect to, or <see langword="null"/> to preserve the original host.
    /// </summary>
    public string? HostName { get; init; }

    /// <summary>
    /// The protocol to use for the redirect (e.g., "http" or "https").
    /// </summary>
    public string? Protocol { get; init; }

    /// <summary>
    /// The key prefix replacement in the redirect location.
    /// </summary>
    public string? ReplaceKeyPrefixWith { get; init; }

    /// <summary>
    /// The full key replacement in the redirect location.
    /// </summary>
    public string? ReplaceKeyWith { get; init; }

    /// <summary>
    /// The HTTP redirect status code (e.g., 301, 302).
    /// </summary>
    public int? HttpRedirectCode { get; init; }
}
