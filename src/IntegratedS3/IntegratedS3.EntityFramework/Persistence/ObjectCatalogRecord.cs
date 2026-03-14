using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Core.Persistence;

public sealed class ObjectCatalogRecord
{
    public int Id { get; set; }

    public string ProviderName { get; set; } = string.Empty;

    public string BucketName { get; set; } = string.Empty;

    public string Key { get; set; } = string.Empty;

    public string? VersionId { get; set; }

    public bool IsLatest { get; set; }

    public bool IsDeleteMarker { get; set; }

    public long ContentLength { get; set; }

    public string? ContentType { get; set; }

    public string? CacheControl { get; set; }

    public string? ContentDisposition { get; set; }

    public string? ContentEncoding { get; set; }

    public string? ContentLanguage { get; set; }

    public DateTimeOffset? ExpiresUtc { get; set; }

    public string? ETag { get; set; }

    public DateTimeOffset LastModifiedUtc { get; set; }

    public string? MetadataJson { get; set; }

    public string? TagsJson { get; set; }

    public string? ChecksumsJson { get; set; }

    public ObjectServerSideEncryptionAlgorithm? ServerSideEncryptionAlgorithm { get; set; }

    public string? ServerSideEncryptionKeyId { get; set; }

    public DateTimeOffset LastSyncedAtUtc { get; set; }
}
