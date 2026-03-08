namespace IntegratedS3.Abstractions.Models;

public sealed class BucketInfo
{
    public string Name { get; init; } = string.Empty;

    public DateTimeOffset CreatedAtUtc { get; init; }

    public bool VersioningEnabled { get; init; }
}
