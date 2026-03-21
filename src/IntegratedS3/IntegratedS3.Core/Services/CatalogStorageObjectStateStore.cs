using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Models;
using System.Runtime.CompilerServices;

namespace IntegratedS3.Core.Services;

/// <summary>
/// An <see cref="IStorageObjectStateStore"/> backed by the <see cref="IStorageCatalogStore"/>,
/// reading and writing object metadata through the catalog persistence layer.
/// </summary>
public sealed class CatalogStorageObjectStateStore(IStorageCatalogStore catalogStore) : IStorageObjectStateStore
{
    /// <inheritdoc />
    public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.PlatformManaged;

    /// <inheritdoc />
    public async ValueTask<ObjectInfo?> GetObjectInfoAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
    {
        var objects = await catalogStore.ListObjectsAsync(providerName, bucketName, cancellationToken);
        var entry = string.IsNullOrWhiteSpace(versionId)
            ? objects.FirstOrDefault(existing => string.Equals(existing.Key, key, StringComparison.Ordinal) && existing.IsLatest)
            : objects.FirstOrDefault(existing => string.Equals(existing.Key, key, StringComparison.Ordinal)
                && string.Equals(existing.VersionId, versionId, StringComparison.Ordinal));
        if (entry is null) {
            return null;
        }

        return ToObjectInfo(entry);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(string providerName, string bucketName, string? prefix = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var objects = await catalogStore.ListObjectsAsync(providerName, bucketName, cancellationToken);
        foreach (var entry in objects
                     .Where(existing => string.IsNullOrWhiteSpace(prefix) || existing.Key.StartsWith(prefix, StringComparison.Ordinal))
                     .OrderBy(existing => existing.Key, StringComparer.Ordinal)
                     .ThenByDescending(existing => existing.IsLatest)
                     .ThenByDescending(existing => existing.VersionId, StringComparer.Ordinal)) {
            cancellationToken.ThrowIfCancellationRequested();
            yield return ToObjectInfo(entry);
        }
    }

    /// <inheritdoc />
    public ValueTask UpsertObjectInfoAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default)
    {
        return catalogStore.UpsertObjectAsync(providerName, @object, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RemoveObjectInfoAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
    {
        return catalogStore.RemoveObjectAsync(providerName, bucketName, key, versionId, cancellationToken);
    }

    private static ObjectInfo ToObjectInfo(StoredObjectEntry entry)
    {
        return new ObjectInfo
        {
            BucketName = entry.BucketName,
            Key = entry.Key,
            VersionId = entry.VersionId,
            IsLatest = entry.IsLatest,
            IsDeleteMarker = entry.IsDeleteMarker,
            ContentLength = entry.ContentLength,
            ContentType = entry.ContentType,
            CacheControl = entry.CacheControl,
            ContentDisposition = entry.ContentDisposition,
            ContentEncoding = entry.ContentEncoding,
            ContentLanguage = entry.ContentLanguage,
            ExpiresUtc = entry.ExpiresUtc,
            ETag = entry.ETag,
            LastModifiedUtc = entry.LastModifiedUtc,
            Metadata = entry.Metadata,
            Tags = entry.Tags,
            Checksums = entry.Checksums,
            RetentionMode = entry.RetentionMode,
            RetainUntilDateUtc = entry.RetainUntilDateUtc,
            LegalHoldStatus = entry.LegalHoldStatus,
            ServerSideEncryption = entry.ServerSideEncryption
        };
    }
}
