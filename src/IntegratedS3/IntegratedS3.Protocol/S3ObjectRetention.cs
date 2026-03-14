namespace IntegratedS3.Protocol;

public sealed class S3ObjectRetention
{
    public string? Mode { get; init; }

    public DateTimeOffset? RetainUntilDateUtc { get; init; }
}
