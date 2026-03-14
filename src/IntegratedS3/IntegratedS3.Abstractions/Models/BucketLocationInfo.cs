namespace IntegratedS3.Abstractions.Models;

public sealed class BucketLocationInfo
{
    public string BucketName { get; init; } = string.Empty;

    public string? LocationConstraint { get; init; }
}
