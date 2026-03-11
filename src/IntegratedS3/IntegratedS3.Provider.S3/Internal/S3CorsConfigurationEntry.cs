using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3.Internal;

internal sealed record S3CorsConfigurationEntry(
    IReadOnlyList<S3CorsRuleEntry> Rules);

internal sealed record S3CorsRuleEntry(
    string? Id,
    IReadOnlyList<string> AllowedOrigins,
    IReadOnlyList<BucketCorsMethod> AllowedMethods,
    IReadOnlyList<string> AllowedHeaders,
    IReadOnlyList<string> ExposeHeaders,
    int? MaxAgeSeconds);
