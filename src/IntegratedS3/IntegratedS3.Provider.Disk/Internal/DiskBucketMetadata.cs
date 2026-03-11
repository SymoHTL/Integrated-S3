using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskBucketMetadata
{
    public BucketVersioningStatus VersioningStatus { get; init; } = BucketVersioningStatus.Disabled;

    public DiskBucketCorsConfiguration? CorsConfiguration { get; init; }
}

internal sealed class DiskBucketCorsConfiguration
{
    public DiskBucketCorsRule[] Rules { get; init; } = [];
}

internal sealed class DiskBucketCorsRule
{
    public string? Id { get; init; }

    public string[] AllowedOrigins { get; init; } = [];

    public string[] AllowedMethods { get; init; } = [];

    public string[] AllowedHeaders { get; init; } = [];

    public string[] ExposeHeaders { get; init; } = [];

    public int? MaxAgeSeconds { get; init; }
}
