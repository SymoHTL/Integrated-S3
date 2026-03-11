namespace IntegratedS3.Abstractions.Requests;

public sealed class ResolveObjectLocationRequest
{
    public string ProviderName { get; set; } = string.Empty;

    public string BucketName { get; set; } = string.Empty;

    public string Key { get; set; } = string.Empty;

    public string? VersionId { get; set; }

    public DateTimeOffset? ExpiresAtUtc { get; set; }
}
