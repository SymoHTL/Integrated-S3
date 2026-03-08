namespace IntegratedS3.Core.Persistence;

public sealed class BucketCatalogRecord
{
    public int Id { get; set; }

    public string ProviderName { get; set; } = string.Empty;

    public string BucketName { get; set; } = string.Empty;

    public DateTimeOffset CreatedAtUtc { get; set; }

    public bool VersioningEnabled { get; set; }

    public DateTimeOffset LastSyncedAtUtc { get; set; }

    public List<ObjectCatalogRecord> Objects { get; set; } = [];
}