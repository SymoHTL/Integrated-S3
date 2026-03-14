using System.Text.Json;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Persistence;
using IntegratedS3.Core.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IntegratedS3.EntityFramework.Services;

internal sealed class EntityFrameworkStorageCatalogStore<TDbContext>(
    IServiceProvider serviceProvider,
    IOptions<EntityFrameworkCatalogOptions> options) : IStorageCatalogStore
    where TDbContext : DbContext
{
    private readonly SemaphoreSlim _initializationLock = new(1, 1);
    private bool _initialized;

    public async ValueTask UpsertBucketAsync(string providerName, BucketInfo bucket, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        var buckets = dbContext.Set<BucketCatalogRecord>();
        var record = await buckets.SingleOrDefaultAsync(
            existing => existing.ProviderName == providerName && existing.BucketName == bucket.Name,
            cancellationToken);

        if (record is null) {
            record = new BucketCatalogRecord
            {
                ProviderName = providerName,
                BucketName = bucket.Name
            };
            buckets.Add(record);
        }

        record.CreatedAtUtc = bucket.CreatedAtUtc;
        record.VersioningEnabled = bucket.VersioningEnabled;
        record.LastSyncedAtUtc = DateTimeOffset.UtcNow;
        await dbContext.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask RemoveBucketAsync(string providerName, string bucketName, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        await dbContext.Set<ObjectCatalogRecord>()
            .Where(existing => existing.ProviderName == providerName && existing.BucketName == bucketName)
            .ExecuteDeleteAsync(cancellationToken);

        await dbContext.Set<BucketCatalogRecord>()
            .Where(existing => existing.ProviderName == providerName && existing.BucketName == bucketName)
            .ExecuteDeleteAsync(cancellationToken);
    }

    public async ValueTask<IReadOnlyList<StoredBucketEntry>> ListBucketsAsync(string? providerName = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        var query = dbContext.Set<BucketCatalogRecord>().AsNoTracking().AsQueryable();
        if (!string.IsNullOrWhiteSpace(providerName)) {
            query = query.Where(bucket => bucket.ProviderName == providerName);
        }

        return await query
            .OrderBy(bucket => bucket.ProviderName)
            .ThenBy(bucket => bucket.BucketName)
            .Select(bucket => new StoredBucketEntry
            {
                ProviderName = bucket.ProviderName,
                BucketName = bucket.BucketName,
                CreatedAtUtc = bucket.CreatedAtUtc,
                VersioningEnabled = bucket.VersioningEnabled,
                LastSyncedAtUtc = bucket.LastSyncedAtUtc
            })
            .ToArrayAsync(cancellationToken);
    }

    public async ValueTask UpsertObjectAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        var buckets = dbContext.Set<BucketCatalogRecord>();
        var objects = dbContext.Set<ObjectCatalogRecord>();

        var bucketRecord = await buckets.SingleOrDefaultAsync(
            existing => existing.ProviderName == providerName && existing.BucketName == @object.BucketName,
            cancellationToken);

        if (bucketRecord is null) {
            bucketRecord = new BucketCatalogRecord
            {
                ProviderName = providerName,
                BucketName = @object.BucketName,
                CreatedAtUtc = @object.LastModifiedUtc,
                VersioningEnabled = false,
                LastSyncedAtUtc = DateTimeOffset.UtcNow
            };
            buckets.Add(bucketRecord);
        }
        else {
            bucketRecord.LastSyncedAtUtc = DateTimeOffset.UtcNow;
        }

        if (@object.IsLatest) {
            await objects
                .Where(existing => existing.ProviderName == providerName
                    && existing.BucketName == @object.BucketName
                    && existing.Key == @object.Key
                    && existing.IsLatest)
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(static existing => existing.IsLatest, false), cancellationToken);
        }

        var record = await objects.SingleOrDefaultAsync(
            existing => existing.ProviderName == providerName
                && existing.BucketName == @object.BucketName
                && existing.Key == @object.Key
                && existing.VersionId == @object.VersionId,
            cancellationToken);

        if (record is null) {
            record = new ObjectCatalogRecord
            {
                ProviderName = providerName,
                BucketName = @object.BucketName,
                Key = @object.Key
            };
            objects.Add(record);
        }

        record.VersionId = @object.VersionId;
        record.IsLatest = @object.IsLatest;
        record.IsDeleteMarker = @object.IsDeleteMarker;
        record.ContentLength = @object.ContentLength;
        record.ContentType = @object.ContentType;
        record.CacheControl = @object.CacheControl;
        record.ContentDisposition = @object.ContentDisposition;
        record.ContentEncoding = @object.ContentEncoding;
        record.ContentLanguage = @object.ContentLanguage;
        record.ExpiresUtc = @object.ExpiresUtc;
        record.ETag = @object.ETag;
        record.LastModifiedUtc = @object.LastModifiedUtc;
        record.MetadataJson = @object.Metadata is null ? null : JsonSerializer.Serialize(@object.Metadata);
        record.TagsJson = @object.Tags is null ? null : JsonSerializer.Serialize(@object.Tags);
        record.ChecksumsJson = @object.Checksums is null ? null : JsonSerializer.Serialize(@object.Checksums);
        record.RetentionMode = @object.RetentionMode;
        record.RetainUntilDateUtc = @object.RetainUntilDateUtc;
        record.LegalHoldStatus = @object.LegalHoldStatus;
        record.ServerSideEncryptionAlgorithm = @object.ServerSideEncryption?.Algorithm;
        record.ServerSideEncryptionKeyId = @object.ServerSideEncryption?.KeyId;
        record.LastSyncedAtUtc = DateTimeOffset.UtcNow;

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        var query = dbContext.Set<ObjectCatalogRecord>()
            .Where(existing => existing.ProviderName == providerName && existing.BucketName == bucketName && existing.Key == key);

        if (!string.IsNullOrWhiteSpace(versionId)) {
            query = query.Where(existing => existing.VersionId == versionId);
        }

        await query.ExecuteDeleteAsync(cancellationToken);
    }

    public async ValueTask<IReadOnlyList<StoredObjectEntry>> ListObjectsAsync(string? providerName = null, string? bucketName = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        var query = dbContext.Set<ObjectCatalogRecord>().AsNoTracking().AsQueryable();
        if (!string.IsNullOrWhiteSpace(providerName)) {
            query = query.Where(@object => @object.ProviderName == providerName);
        }

        if (!string.IsNullOrWhiteSpace(bucketName)) {
            query = query.Where(@object => @object.BucketName == bucketName);
        }

        return await query
            .OrderBy(@object => @object.ProviderName)
            .ThenBy(@object => @object.BucketName)
            .ThenBy(@object => @object.Key)
            .Select(@object => new StoredObjectEntry
            {
                ProviderName = @object.ProviderName,
                BucketName = @object.BucketName,
                Key = @object.Key,
                VersionId = @object.VersionId,
                IsLatest = @object.IsLatest,
                IsDeleteMarker = @object.IsDeleteMarker,
                ContentLength = @object.ContentLength,
                ContentType = @object.ContentType,
                CacheControl = @object.CacheControl,
                ContentDisposition = @object.ContentDisposition,
                ContentEncoding = @object.ContentEncoding,
                ContentLanguage = @object.ContentLanguage,
                ExpiresUtc = @object.ExpiresUtc,
                ETag = @object.ETag,
                LastModifiedUtc = @object.LastModifiedUtc,
                Metadata = string.IsNullOrWhiteSpace(@object.MetadataJson)
                    ? null
                    : JsonSerializer.Deserialize<Dictionary<string, string>>(@object.MetadataJson),
                Tags = string.IsNullOrWhiteSpace(@object.TagsJson)
                    ? null
                    : JsonSerializer.Deserialize<Dictionary<string, string>>(@object.TagsJson),
                Checksums = string.IsNullOrWhiteSpace(@object.ChecksumsJson)
                    ? null
                    : JsonSerializer.Deserialize<Dictionary<string, string>>(@object.ChecksumsJson),
                RetentionMode = @object.RetentionMode,
                RetainUntilDateUtc = @object.RetainUntilDateUtc,
                LegalHoldStatus = @object.LegalHoldStatus,
                ServerSideEncryption = @object.ServerSideEncryptionAlgorithm.HasValue
                    ? new ObjectServerSideEncryptionInfo
                    {
                        Algorithm = @object.ServerSideEncryptionAlgorithm.Value,
                        KeyId = @object.ServerSideEncryptionKeyId
                    }
                    : null,
                LastSyncedAtUtc = @object.LastSyncedAtUtc
            })
            .ToArrayAsync(cancellationToken);
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized || !options.Value.EnsureCreated) {
            _initialized = true;
            return;
        }

        await _initializationLock.WaitAsync(cancellationToken);
        try {
            if (_initialized) {
                return;
            }

            await using var scope = serviceProvider.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
            ValidateModel(dbContext);
            await dbContext.Database.EnsureCreatedAsync(cancellationToken);
            _initialized = true;
        }
        finally {
            _initializationLock.Release();
        }
    }

    private static void ValidateModel(TDbContext dbContext)
    {
        if (dbContext.Model.FindEntityType(typeof(BucketCatalogRecord)) is not null
            && dbContext.Model.FindEntityType(typeof(ObjectCatalogRecord)) is not null) {
            return;
        }

        throw new InvalidOperationException(
            $"The DbContext '{typeof(TDbContext).FullName}' is not configured for the IntegratedS3 catalog. " +
            $"Call modelBuilder.MapIntegratedS3Catalog() from OnModelCreating before registering AddEntityFrameworkStorageCatalog<{typeof(TDbContext).Name}>().");
    }
}
