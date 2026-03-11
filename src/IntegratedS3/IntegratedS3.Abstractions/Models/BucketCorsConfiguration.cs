namespace IntegratedS3.Abstractions.Models;

public sealed class BucketCorsConfiguration
{
    public string BucketName { get; init; } = string.Empty;

    public IReadOnlyList<BucketCorsRule> Rules { get; init; } = [];
}

public sealed class BucketCorsRule
{
    public string? Id { get; init; }

    public IReadOnlyList<string> AllowedOrigins { get; init; } = [];

    public IReadOnlyList<BucketCorsMethod> AllowedMethods { get; init; } = [];

    public IReadOnlyList<string> AllowedHeaders { get; init; } = [];

    public IReadOnlyList<string> ExposeHeaders { get; init; } = [];

    public int? MaxAgeSeconds { get; init; }
}

public enum BucketCorsMethod
{
    Get,
    Put,
    Post,
    Delete,
    Head
}
