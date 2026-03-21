namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the Cross-Origin Resource Sharing (CORS) configuration for an S3 bucket.
/// </summary>
public sealed class S3CorsConfiguration
{
    /// <summary>The list of CORS rules for the bucket.</summary>
    public IReadOnlyList<S3CorsRule> Rules { get; init; } = [];
}

/// <summary>
/// Represents a single CORS rule within an <see cref="S3CorsConfiguration"/>.
/// </summary>
public sealed class S3CorsRule
{
    /// <summary>An optional identifier for this CORS rule.</summary>
    public string? Id { get; init; }

    /// <summary>The origins allowed to make cross-origin requests.</summary>
    public IReadOnlyList<string> AllowedOrigins { get; init; } = [];

    /// <summary>The HTTP methods allowed for cross-origin requests.</summary>
    public IReadOnlyList<string> AllowedMethods { get; init; } = [];

    /// <summary>The headers allowed in preflight requests.</summary>
    public IReadOnlyList<string> AllowedHeaders { get; init; } = [];

    /// <summary>The response headers exposed to the client.</summary>
    public IReadOnlyList<string> ExposeHeaders { get; init; } = [];

    /// <summary>The time in seconds the browser may cache the preflight response.</summary>
    public int? MaxAgeSeconds { get; init; }
}
