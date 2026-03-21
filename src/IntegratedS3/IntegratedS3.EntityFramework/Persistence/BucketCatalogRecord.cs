namespace IntegratedS3.Core.Persistence;

/// <summary>
/// EF Core entity representing a bucket entry in the IntegratedS3 catalog.
/// </summary>
public sealed class BucketCatalogRecord
{
    /// <summary>Gets or sets the auto-generated primary key.</summary>
    public int Id { get; set; }

    /// <summary>Gets or sets the name of the storage provider that owns this bucket.</summary>
    public string ProviderName { get; set; } = string.Empty;

    /// <summary>Gets or sets the bucket name.</summary>
    public string BucketName { get; set; } = string.Empty;

    /// <summary>Gets or sets the UTC timestamp when the bucket was created.</summary>
    public DateTimeOffset CreatedAtUtc { get; set; }

    /// <summary>Gets or sets a value indicating whether versioning is enabled on the bucket.</summary>
    public bool VersioningEnabled { get; set; }

    /// <summary>Gets or sets the UTC timestamp of the last catalog synchronization for this bucket.</summary>
    public DateTimeOffset LastSyncedAtUtc { get; set; }

    /// <summary>Gets or sets the collection of <see cref="ObjectCatalogRecord"/> entries belonging to this bucket.</summary>
    public List<ObjectCatalogRecord> Objects { get; set; } = [];
}