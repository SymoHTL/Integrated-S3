namespace IntegratedS3.Core.Persistence;

public sealed class ObjectCatalogRecord
{
    public int Id { get; set; }

    public string ProviderName { get; set; } = string.Empty;

    public string BucketName { get; set; } = string.Empty;

    public string Key { get; set; } = string.Empty;

    public long ContentLength { get; set; }

    public string? ContentType { get; set; }

    public string? ETag { get; set; }

    public DateTimeOffset LastModifiedUtc { get; set; }

    public string? MetadataJson { get; set; }

    public DateTimeOffset LastSyncedAtUtc { get; set; }
}