namespace IntegratedS3.Protocol;

public sealed class S3CorsConfiguration
{
    public IReadOnlyList<S3CorsRule> Rules { get; init; } = [];
}

public sealed class S3CorsRule
{
    public string? Id { get; init; }

    public IReadOnlyList<string> AllowedOrigins { get; init; } = [];

    public IReadOnlyList<string> AllowedMethods { get; init; } = [];

    public IReadOnlyList<string> AllowedHeaders { get; init; } = [];

    public IReadOnlyList<string> ExposeHeaders { get; init; } = [];

    public int? MaxAgeSeconds { get; init; }
}
