using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutBucketWebsite operation.</summary>
public sealed class PutBucketWebsiteRequest
{
    /// <summary>The name of the bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The suffix appended to request paths for the index document (e.g. "index.html").</summary>
    public string? IndexDocumentSuffix { get; init; }

    /// <summary>The object key for the custom error document.</summary>
    public string? ErrorDocumentKey { get; init; }

    /// <summary>Redirect configuration that redirects all requests to a specified host.</summary>
    public BucketWebsiteRedirectAllRequestsTo? RedirectAllRequestsTo { get; init; }

    /// <summary>The routing rules for conditional redirects.</summary>
    public IReadOnlyList<BucketWebsiteRoutingRule> RoutingRules { get; init; } = [];
}
