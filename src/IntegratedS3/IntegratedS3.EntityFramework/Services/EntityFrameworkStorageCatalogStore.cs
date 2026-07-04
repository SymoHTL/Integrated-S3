using System.Text.Json;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Persistence;
using IntegratedS3.Core.Services;
using IntegratedS3.EntityFramework.Serialization;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IntegratedS3.EntityFramework.Services;

internal sealed class EntityFrameworkStorageCatalogStore<TDbContext>(
    IServiceProvider serviceProvider,
    IOptions<EntityFrameworkCatalogOptions> options) : IStorageCatalogStore
    where TDbContext : DbContext
{
    /// <summary>
    /// Maximum number of times <see cref="UpsertObjectAsync"/> re-runs its unit of work after a detected
    /// concurrency conflict before surfacing the failure to the caller.
    /// </summary>
    private const int MaxUpsertConcurrencyRetries = 16;

    private readonly SemaphoreSlim _initializationLock = new(1, 1);
    private bool _initialized;

    /// <summary>
    /// Determines whether <paramref name="exception"/> represents a recoverable optimistic-concurrency conflict:
    /// either EF's concurrency-token check failing, or a unique-index violation from the filtered "single latest
    /// per key" index when two writers raced to insert a new latest version for the same key.
    /// </summary>
    private static bool IsConcurrencyConflict(Exception exception)
        => exception is DbUpdateConcurrencyException
           || (exception is DbUpdateException dbUpdate && IsUniqueConstraintViolation(dbUpdate))
           || IsTransientLockConflict(exception);

    /// <summary>
    /// Detects a transient database-lock / write-serialization conflict (e.g. SQLite <c>database is locked</c> /
    /// <c>SQLITE_BUSY</c>) that resolves on retry, in a provider-agnostic way.
    /// </summary>
    private static bool IsTransientLockConflict(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException) {
            var message = current.Message;
            if (message.Contains("database is locked", StringComparison.OrdinalIgnoreCase)
                || message.Contains("database table is locked", StringComparison.OrdinalIgnoreCase)
                || message.Contains("SQLITE_BUSY", StringComparison.OrdinalIgnoreCase)
                || message.Contains("deadlock", StringComparison.OrdinalIgnoreCase)) {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Heuristically detects a unique-constraint / unique-index violation from a <see cref="DbUpdateException"/>
    /// in a provider-agnostic way (the concrete DB exception type differs per provider).
    /// </summary>
    private static bool IsUniqueConstraintViolation(DbUpdateException exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException) {
            var message = current.Message;
            if (message.Contains("UNIQUE constraint failed", StringComparison.OrdinalIgnoreCase)
                || message.Contains("duplicate key", StringComparison.OrdinalIgnoreCase)
                || message.Contains("unique index", StringComparison.OrdinalIgnoreCase)
                || message.Contains("unique constraint", StringComparison.OrdinalIgnoreCase)
                || message.Contains("violation of unique", StringComparison.OrdinalIgnoreCase)) {
                return true;
            }
        }

        return false;
    }

    public async ValueTask UpsertBucketAsync(string providerName, BucketInfo bucket, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        var buckets = dbContext.Set<BucketCatalogRecord>();
        var record = await buckets.SingleOrDefaultAsync(
            existing => existing.ProviderName == providerName && existing.BucketName == bucket.Name,
            cancellationToken);

        if (record is null) {
            record = new BucketCatalogRecord
            {
                ProviderName = providerName,
                BucketName = bucket.Name,
                CreatedAtUtc = bucket.CreatedAtUtc,
                VersioningEnabled = bucket.VersioningEnabled,
                LastSyncedAtUtc = DateTimeOffset.UtcNow
            };
            buckets.Add(record);
        }
        else {
            record.CreatedAtUtc = bucket.CreatedAtUtc;
            record.VersioningEnabled = bucket.VersioningEnabled;
            record.LastSyncedAtUtc = DateTimeOffset.UtcNow;
            dbContext.Update(record);
        }

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask RemoveBucketAsync(string providerName, string bucketName, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        // Delete the object rows and the bucket row inside a single transaction so a mid-operation failure
        // (crash, dropped connection, timeout) can never leave the bucket row present with its objects gone.
        // The execution strategy owns the transaction boundary so it composes with connection-resiliency retries.
        var strategy = dbContext.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync(async ct => {
            await using var transaction = await dbContext.Database.BeginTransactionAsync(ct);

            await dbContext.Set<ObjectCatalogRecord>()
                .Where(existing => existing.ProviderName == providerName && existing.BucketName == bucketName)
                .ExecuteDeleteAsync(ct);

            await dbContext.Set<BucketCatalogRecord>()
                .Where(existing => existing.ProviderName == providerName && existing.BucketName == bucketName)
                .ExecuteDeleteAsync(ct);

            await transaction.CommitAsync(ct);
        }, cancellationToken);
    }

    public async ValueTask<IReadOnlyList<StoredBucketEntry>> ListBucketsAsync(string? providerName = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        var query = dbContext.Set<BucketCatalogRecord>().AsQueryable();
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

        // The demote-existing-latest + insert-new-latest sequence is a read-modify-write that must be atomic and
        // concurrency-safe. On a conflict (a concurrent writer demoted/inserted between our read and write) EF raises
        // a DbUpdateConcurrencyException (concurrency token) or a unique-index violation (filtered single-latest
        // index). We retry the whole unit of work on a fresh DbContext up to a bounded number of times; exhausting
        // the budget surfaces the underlying exception rather than silently losing the update.
        for (var attempt = 0; ; attempt++) {
            try {
                await UpsertObjectCoreAsync(providerName, @object, cancellationToken);
                return;
            }
            catch (Exception ex) when (attempt < MaxUpsertConcurrencyRetries && IsConcurrencyConflict(ex)) {
                // Transient concurrency conflict: back off (capped, jittered), then retry on a fresh scope/DbContext.
                var backoffMs = Math.Min(200, 5 * (attempt + 1)) + Random.Shared.Next(0, 15);
                await Task.Delay(TimeSpan.FromMilliseconds(backoffMs), cancellationToken);
            }
        }
    }

    private async ValueTask UpsertObjectCoreAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken)
    {
        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        // Wrap the demote + insert in a single transaction so a partial failure can never leave the key with no
        // latest version (old row already demoted, new row never inserted) or an inconsistent bucket/object state.
        var strategy = dbContext.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync(async ct => {
            await using var transaction = await dbContext.Database.BeginTransactionAsync(ct);

            var buckets = dbContext.Set<BucketCatalogRecord>();
            var objects = dbContext.Set<ObjectCatalogRecord>();

            var bucketRecord = await buckets.SingleOrDefaultAsync(
                existing => existing.ProviderName == providerName && existing.BucketName == @object.BucketName,
                ct);

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
                dbContext.Update(bucketRecord);
            }

            if (@object.IsLatest) {
                await objects
                    .Where(existing => existing.ProviderName == providerName
                        && existing.BucketName == @object.BucketName
                        && existing.Key == @object.Key
                        && existing.IsLatest)
                    .ExecuteUpdateAsync(setters => setters
                        .SetProperty(static existing => existing.IsLatest, false), ct);
            }

            var record = await objects.SingleOrDefaultAsync(
                existing => existing.ProviderName == providerName
                    && existing.BucketName == @object.BucketName
                    && existing.Key == @object.Key
                    && existing.VersionId == @object.VersionId,
                ct);

            var isNewObject = record is null;
            // The record was read with NoTracking, so EF has no original-value baseline for the concurrency
            // check. Capture the token value that was actually persisted so the update's WHERE clause targets it.
            var originalVersion = record?.Version ?? 0;
            record ??= new ObjectCatalogRecord
            {
                ProviderName = providerName,
                BucketName = @object.BucketName,
                Key = @object.Key
            };

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
            record.MetadataJson = SerializeDictionary(@object.Metadata);
            record.TagsJson = SerializeDictionary(@object.Tags);
            record.ChecksumsJson = SerializeDictionary(@object.Checksums);
            record.RetentionMode = @object.RetentionMode;
            record.RetainUntilDateUtc = @object.RetainUntilDateUtc;
            record.LegalHoldStatus = @object.LegalHoldStatus;
            record.ServerSideEncryptionAlgorithm = @object.ServerSideEncryption?.Algorithm;
            record.ServerSideEncryptionKeyId = @object.ServerSideEncryption?.KeyId;
            record.LastSyncedAtUtc = DateTimeOffset.UtcNow;
            // Advance the optimistic-concurrency token so a concurrent update to the same version is detected.
            record.Version = originalVersion + 1;

            if (isNewObject) {
                objects.Add(record);
            }
            else {
                dbContext.Update(record);
                // Update() (with NoTracking reads) treats the new Version as the original value, which would make
                // the concurrency WHERE clause target the incremented token and match zero rows even without a
                // concurrent writer. Pin the original value to what was actually read from the database.
                dbContext.Entry(record).Property(static existing => existing.Version).OriginalValue = originalVersion;
            }

            await dbContext.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
        }, cancellationToken);
    }

    public async ValueTask RemoveObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        var query = dbContext.Set<ObjectCatalogRecord>()
            .Where(existing => existing.ProviderName == providerName && existing.BucketName == bucketName && existing.Key == key);

        if (!string.IsNullOrWhiteSpace(versionId)) {
            query = query.Where(existing => existing.VersionId == versionId);
        }

        await query.ExecuteDeleteAsync(cancellationToken);
    }

    public async ValueTask<StoredObjectEntry?> GetObjectAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        // Push the key (and version) predicate into the query so the database returns only the matching row(s),
        // using the (ProviderName, BucketName, Key, IsLatest) / unique (ProviderName, BucketName, Key, VersionId)
        // indexes, instead of materializing the entire bucket catalog for a single point lookup.
        var query = dbContext.Set<ObjectCatalogRecord>()
            .Where(@object => @object.ProviderName == providerName
                && @object.BucketName == bucketName
                && @object.Key == key);

        query = string.IsNullOrWhiteSpace(versionId)
            ? query.Where(@object => @object.IsLatest)
            : query.Where(@object => @object.VersionId == versionId);

        return await query
            .Select(ProjectEntry)
            .FirstOrDefaultAsync(cancellationToken);
    }

    public async ValueTask<IReadOnlyList<StoredObjectEntry>> ListObjectsAsync(string? providerName = null, string? bucketName = null, string? keyPrefix = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        var query = dbContext.Set<ObjectCatalogRecord>().AsQueryable();
        if (!string.IsNullOrWhiteSpace(providerName)) {
            query = query.Where(@object => @object.ProviderName == providerName);
        }

        if (!string.IsNullOrWhiteSpace(bucketName)) {
            query = query.Where(@object => @object.BucketName == bucketName);
        }

        // Push the key-prefix predicate into the query so unrelated keys are never materialized. A whitespace-only
        // prefix is treated as "no filter", preserving the prior behaviour of the in-memory prefix check.
        if (!string.IsNullOrWhiteSpace(keyPrefix)) {
            query = query.Where(@object => @object.Key.StartsWith(keyPrefix));
        }

        return await query
            .OrderBy(@object => @object.ProviderName)
            .ThenBy(@object => @object.BucketName)
            .ThenBy(@object => @object.Key)
            .Select(ProjectEntry)
            .ToArrayAsync(cancellationToken);
    }

    /// <summary>
    /// Shared projection from an <see cref="ObjectCatalogRecord"/> to a <see cref="StoredObjectEntry"/>. Declared as
    /// a LINQ expression tree so EF Core translates the column selection into SQL and both the point lookup and the
    /// list query materialize the exact same shape (JSON columns are deserialized client-side).
    /// </summary>
    private static readonly System.Linq.Expressions.Expression<Func<ObjectCatalogRecord, StoredObjectEntry>> ProjectEntry =
        @object => new StoredObjectEntry
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
                Metadata = DeserializeDictionary(@object.MetadataJson),
                Tags = DeserializeDictionary(@object.TagsJson),
                Checksums = DeserializeDictionary(@object.ChecksumsJson),
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
        };

    /// <summary>
    /// Serializes a metadata/tags/checksums dictionary to its JSON-column representation via the source-generated
    /// <see cref="EntityFrameworkCatalogJsonSerializerContext"/>, returning <see langword="null"/> for a null source
    /// so the column stays NULL. The source is materialized into a concrete <see cref="Dictionary{TKey,TValue}"/>
    /// (when it is not already one) so the generated <see cref="Dictionary{TKey,TValue}"/> type info applies; with
    /// default options this yields the same JSON shape as the previous reflection-based path.
    /// </summary>
    private static string? SerializeDictionary(IReadOnlyDictionary<string, string>? source)
    {
        if (source is null) {
            return null;
        }

        var dictionary = source as Dictionary<string, string> ?? new Dictionary<string, string>(source);
        return JsonSerializer.Serialize(dictionary, EntityFrameworkCatalogJsonSerializerContext.Default.DictionaryStringString);
    }

    /// <summary>
    /// Deserializes a JSON-column value back into a dictionary via the source-generated
    /// <see cref="EntityFrameworkCatalogJsonSerializerContext"/>, treating null/blank input as "no value".
    /// </summary>
    private static Dictionary<string, string>? DeserializeDictionary(string? json)
        => string.IsNullOrWhiteSpace(json)
            ? null
            : JsonSerializer.Deserialize(json, EntityFrameworkCatalogJsonSerializerContext.Default.DictionaryStringString);

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
            var dbContext = ResolveDbContext(scope);
            await dbContext.Database.EnsureCreatedAsync(cancellationToken);
            _initialized = true;
        }
        finally {
            _initializationLock.Release();
        }
    }

    private TDbContext ResolveDbContext(IServiceScope scope)
    {
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        dbContext.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
        ValidateModel(dbContext);
        return dbContext;
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
