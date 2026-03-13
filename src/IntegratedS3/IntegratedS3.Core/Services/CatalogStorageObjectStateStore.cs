using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Services;
using System.Runtime.CompilerServices;

namespace IntegratedS3.Core.Services;

public sealed class CatalogStorageObjectStateStore(IStorageCatalogStore catalogStore) : IStorageObjectStateStore
{
    public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.PlatformManaged;

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

        return new ObjectInfo
        {
            BucketName = entry.BucketName,
            Key = entry.Key,
            VersionId = entry.VersionId,
            IsLatest = entry.IsLatest,
            IsDeleteMarker = entry.IsDeleteMarker,
            ContentLength = entry.ContentLength,
            ContentType = entry.ContentType,
            ETag = entry.ETag,
            LastModifiedUtc = entry.LastModifiedUtc,
            Metadata = entry.Metadata,
            Tags = entry.Tags,
            Checksums = entry.Checksums,
            ServerSideEncryption = entry.ServerSideEncryption
        };
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(string providerName, string bucketName, string? prefix = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var objects = await catalogStore.ListObjectsAsync(providerName, bucketName, cancellationToken);
        foreach (var entry in objects
                     .Where(existing => string.IsNullOrWhiteSpace(prefix) || existing.Key.StartsWith(prefix, StringComparison.Ordinal))
                     .OrderBy(existing => existing.Key, StringComparer.Ordinal)
                     .ThenByDescending(existing => existing.IsLatest)
                     .ThenByDescending(existing => existing.VersionId, StringComparer.Ordinal)) {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new ObjectInfo
            {
                BucketName = entry.BucketName,
                Key = entry.Key,
                VersionId = entry.VersionId,
                IsLatest = entry.IsLatest,
                IsDeleteMarker = entry.IsDeleteMarker,
                ContentLength = entry.ContentLength,
                ContentType = entry.ContentType,
                ETag = entry.ETag,
                LastModifiedUtc = entry.LastModifiedUtc,
                Metadata = entry.Metadata,
                Tags = entry.Tags,
                Checksums = entry.Checksums,
                ServerSideEncryption = entry.ServerSideEncryption
            };
        }
    }

    public ValueTask UpsertObjectInfoAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default)
    {
        return catalogStore.UpsertObjectAsync(providerName, @object, cancellationToken);
    }

    public ValueTask RemoveObjectInfoAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
    {
        return catalogStore.RemoveObjectAsync(providerName, bucketName, key, versionId, cancellationToken);
    }
}
