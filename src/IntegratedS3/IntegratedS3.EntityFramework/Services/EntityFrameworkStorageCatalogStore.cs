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

        var record = await objects.SingleOrDefaultAsync(
            existing => existing.ProviderName == providerName && existing.BucketName == @object.BucketName && existing.Key == @object.Key,
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

        record.ContentLength = @object.ContentLength;
        record.ContentType = @object.ContentType;
        record.ETag = @object.ETag;
        record.LastModifiedUtc = @object.LastModifiedUtc;
        record.MetadataJson = @object.Metadata is null ? null : JsonSerializer.Serialize(@object.Metadata);
        record.LastSyncedAtUtc = DateTimeOffset.UtcNow;

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        await dbContext.Set<ObjectCatalogRecord>()
            .Where(existing => existing.ProviderName == providerName && existing.BucketName == bucketName && existing.Key == key)
            .ExecuteDeleteAsync(cancellationToken);
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
                ContentLength = @object.ContentLength,
                ContentType = @object.ContentType,
                ETag = @object.ETag,
                LastModifiedUtc = @object.LastModifiedUtc,
                Metadata = string.IsNullOrWhiteSpace(@object.MetadataJson)
                    ? null
                    : JsonSerializer.Deserialize<Dictionary<string, string>>(@object.MetadataJson),
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