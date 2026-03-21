using System.Runtime.CompilerServices;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Testing;

/// <summary>
/// In-memory <see cref="IStorageObjectStateStore" /> implementation for provider tests.
/// </summary>
/// <remarks>
/// This implementation is intended for unit and integration testing only. 
/// It is not thread-safe and should not be used as a production singleton service.
/// </remarks>
public sealed class InMemoryObjectStateStore : IStorageObjectStateStore
{
    private readonly Dictionary<(string ProviderName, string BucketName, string Key, string? VersionId), ObjectInfo> _objects = new();

    /// <inheritdoc />
    public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.PlatformManaged;

    /// <inheritdoc />
    public ValueTask<ObjectInfo?> GetObjectInfoAsync(
        string providerName,
        string bucketName,
        string key,
        string? versionId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!string.IsNullOrWhiteSpace(versionId)) {
            return ValueTask.FromResult(_objects.TryGetValue((providerName, bucketName, key, versionId), out var byVersion) ? byVersion : null);
        }

        var current = _objects.Where(existing => string.Equals(existing.Key.ProviderName, providerName, StringComparison.Ordinal)
                && string.Equals(existing.Key.BucketName, bucketName, StringComparison.Ordinal)
                && string.Equals(existing.Key.Key, key, StringComparison.Ordinal))
            .Select(existing => existing.Value)
            .FirstOrDefault(existing => existing.IsLatest);
        return ValueTask.FromResult(current);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(
        string providerName,
        string bucketName,
        string? prefix = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var entry in _objects.Where(existing => string.Equals(existing.Key.ProviderName, providerName, StringComparison.Ordinal)
                     && string.Equals(existing.Key.BucketName, bucketName, StringComparison.Ordinal)
                     && (string.IsNullOrWhiteSpace(prefix) || existing.Key.Key.StartsWith(prefix, StringComparison.Ordinal)))
                 .OrderBy(existing => existing.Value.Key, StringComparer.Ordinal)
                 .ThenByDescending(existing => existing.Value.IsLatest)
                 .ThenByDescending(existing => existing.Value.VersionId, StringComparer.Ordinal)) {
            cancellationToken.ThrowIfCancellationRequested();
            yield return entry.Value;
            await Task.Yield();
        }
    }

    /// <inheritdoc />
    public ValueTask UpsertObjectInfoAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (@object.IsLatest) {
            foreach (var existingKey in _objects.Keys.Where(existing => existing.ProviderName == providerName
                         && existing.BucketName == @object.BucketName
                         && existing.Key == @object.Key).ToArray()) {
                var existing = _objects[existingKey];
                _objects[existingKey] = new ObjectInfo
                {
                    BucketName = existing.BucketName,
                    Key = existing.Key,
                    VersionId = existing.VersionId,
                    IsLatest = false,
                    IsDeleteMarker = existing.IsDeleteMarker,
                    ContentLength = existing.ContentLength,
                    ContentType = existing.ContentType,
                    ETag = existing.ETag,
                    LastModifiedUtc = existing.LastModifiedUtc,
                    Metadata = existing.Metadata,
                    Tags = existing.Tags,
                    Checksums = existing.Checksums,
                    RetentionMode = existing.RetentionMode,
                    RetainUntilDateUtc = existing.RetainUntilDateUtc,
                    LegalHoldStatus = existing.LegalHoldStatus,
                    ServerSideEncryption = existing.ServerSideEncryption
                };
            }
        }

        _objects[(providerName, @object.BucketName, @object.Key, @object.VersionId)] = @object;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RemoveObjectInfoAsync(
        string providerName,
        string bucketName,
        string key,
        string? versionId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        foreach (var existingKey in _objects.Keys.Where(existing => existing.ProviderName == providerName
                     && existing.BucketName == bucketName
                     && existing.Key == key
                     && (string.IsNullOrWhiteSpace(versionId) || string.Equals(existing.VersionId, versionId, StringComparison.Ordinal))).ToArray()) {
            _objects.Remove(existingKey);
        }

        return ValueTask.CompletedTask;
    }
}
