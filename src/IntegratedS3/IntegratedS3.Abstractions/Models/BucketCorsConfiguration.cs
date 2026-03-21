namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// CORS (Cross-Origin Resource Sharing) configuration for a bucket.
/// </summary>
public sealed class BucketCorsConfiguration
{
    /// <summary>
    /// The name of the bucket.
    /// </summary>
    public string BucketName { get; init; } = string.Empty;

    /// <summary>
    /// The list of CORS rules applied to this bucket.
    /// </summary>
    public IReadOnlyList<BucketCorsRule> Rules { get; init; } = [];
}

/// <summary>
/// A single CORS rule that identifies allowed origins, methods, and headers.
/// </summary>
public sealed class BucketCorsRule
{
    /// <summary>
    /// An optional identifier for this rule.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// Origins (e.g., domain names) allowed to make cross-origin requests.
    /// </summary>
    public IReadOnlyList<string> AllowedOrigins { get; init; } = [];

    /// <summary>
    /// HTTP methods allowed for cross-origin requests.
    /// </summary>
    public IReadOnlyList<BucketCorsMethod> AllowedMethods { get; init; } = [];

    /// <summary>
    /// Headers allowed in cross-origin requests.
    /// </summary>
    public IReadOnlyList<string> AllowedHeaders { get; init; } = [];

    /// <summary>
    /// Response headers that the browser is allowed to expose to the requesting client.
    /// </summary>
    public IReadOnlyList<string> ExposeHeaders { get; init; } = [];

    /// <summary>
    /// The time in seconds that the browser may cache the preflight response.
    /// </summary>
    public int? MaxAgeSeconds { get; init; }
}

/// <summary>
/// HTTP methods supported by CORS rules.
/// </summary>
public enum BucketCorsMethod
{
    /// <summary>HTTP GET.</summary>
    Get,
    /// <summary>HTTP PUT.</summary>
    Put,
    /// <summary>HTTP POST.</summary>
    Post,
    /// <summary>HTTP DELETE.</summary>
    Delete,
    /// <summary>HTTP HEAD.</summary>
    Head
}
