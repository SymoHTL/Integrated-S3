using System.Runtime.CompilerServices;
using System.Text.Json;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Persistence;
using IntegratedS3.EntityFramework.Serialization;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IntegratedS3.EntityFramework.Services;

internal sealed class EntityFrameworkStorageMultipartStateStore<TDbContext>(
    IServiceProvider serviceProvider,
    IOptions<EntityFrameworkCatalogOptions> options) : IStorageMultipartStateStore
    where TDbContext : DbContext
{
    private readonly SemaphoreSlim _initializationLock = new(1, 1);
    private bool _initialized;

    public StorageSupportStateOwnership Ownership => StorageSupportStateOwnership.PlatformManaged;

    public async ValueTask<MultipartUploadState?> GetMultipartUploadStateAsync(
        string providerName,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        var record = await dbContext.Set<MultipartUploadCatalogRecord>()
            .SingleOrDefaultAsync(existing =>
                existing.ProviderName == providerName
                && existing.BucketName == bucketName
                && existing.Key == key
                && existing.UploadId == uploadId,
                cancellationToken);

        if (record is null) {
            return null;
        }

        return ToMultipartUploadState(record);
    }

    public async IAsyncEnumerable<MultipartUploadState> ListMultipartUploadStatesAsync(
        string providerName,
        string bucketName,
        string? prefix = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        var query = dbContext.Set<MultipartUploadCatalogRecord>()
            .Where(existing =>
                existing.ProviderName == providerName
                && existing.BucketName == bucketName);

        if (!string.IsNullOrWhiteSpace(prefix)) {
            query = query.Where(existing => existing.Key.StartsWith(prefix));
        }

        // Ordering remains client-side for portability (SQLite does not support
        // DateTimeOffset in ORDER BY). Server-side filtering above still eliminates
        // the bulk of unnecessary data transfer.
        var records = await query.ToListAsync(cancellationToken);

        foreach (var record in records
                     .OrderBy(existing => existing.Key, StringComparer.Ordinal)
                     .ThenBy(existing => existing.InitiatedAtUtc)
                     .ThenBy(existing => existing.UploadId, StringComparer.Ordinal)) {
            cancellationToken.ThrowIfCancellationRequested();
            yield return ToMultipartUploadState(record);
        }
    }

    public async ValueTask UpsertMultipartUploadStateAsync(
        string providerName,
        MultipartUploadState state,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        var states = dbContext.Set<MultipartUploadCatalogRecord>();
        var record = await states.SingleOrDefaultAsync(existing =>
            existing.ProviderName == providerName
            && existing.BucketName == state.BucketName
            && existing.Key == state.Key
            && existing.UploadId == state.UploadId,
            cancellationToken);

        var isNew = record is null;
        record ??= new MultipartUploadCatalogRecord
        {
            ProviderName = providerName,
            BucketName = state.BucketName,
            Key = state.Key,
            UploadId = state.UploadId
        };

        record.InitiatedAtUtc = state.InitiatedAtUtc;
        record.ContentType = state.ContentType;
        record.CacheControl = state.CacheControl;
        record.ContentDisposition = state.ContentDisposition;
        record.ContentEncoding = state.ContentEncoding;
        record.ContentLanguage = state.ContentLanguage;
        record.ExpiresUtc = state.ExpiresUtc;
        record.ChecksumAlgorithm = state.ChecksumAlgorithm;
        record.MetadataJson = SerializeDictionary(state.Metadata);
        record.TagsJson = SerializeDictionary(state.Tags);
        record.LastSyncedAtUtc = DateTimeOffset.UtcNow;

        if (isNew)
            states.Add(record);
        else
            dbContext.Update(record);

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask RemoveMultipartUploadStateAsync(
        string providerName,
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var scope = serviceProvider.CreateAsyncScope();
        var dbContext = ResolveDbContext(scope);

        await dbContext.Set<MultipartUploadCatalogRecord>()
            .Where(existing => existing.ProviderName == providerName
                && existing.BucketName == bucketName
                && existing.Key == key
                && existing.UploadId == uploadId)
            .ExecuteDeleteAsync(cancellationToken);
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
        if (dbContext.Model.FindEntityType(typeof(MultipartUploadCatalogRecord)) is not null) {
            return;
        }

        throw new InvalidOperationException(
            $"The DbContext '{typeof(TDbContext).FullName}' is not configured for the IntegratedS3 multipart catalog. " +
            $"Call modelBuilder.MapIntegratedS3Catalog() from OnModelCreating before registering AddEntityFrameworkStorageCatalog<{typeof(TDbContext).Name}>().");
    }

    private static MultipartUploadState ToMultipartUploadState(MultipartUploadCatalogRecord record)
    {
        return new MultipartUploadState
        {
            BucketName = record.BucketName,
            Key = record.Key,
            UploadId = record.UploadId,
            InitiatedAtUtc = record.InitiatedAtUtc,
            ContentType = record.ContentType,
            CacheControl = record.CacheControl,
            ContentDisposition = record.ContentDisposition,
            ContentEncoding = record.ContentEncoding,
            ContentLanguage = record.ContentLanguage,
            ExpiresUtc = record.ExpiresUtc,
            ChecksumAlgorithm = record.ChecksumAlgorithm,
            Metadata = DeserializeDictionary(record.MetadataJson),
            Tags = DeserializeDictionary(record.TagsJson)
        };
    }

    /// <summary>
    /// Serializes a metadata/tags dictionary to its JSON-column representation via the source-generated
    /// <see cref="EntityFrameworkCatalogJsonSerializerContext"/>, returning <see langword="null"/> for a null source
    /// so the column stays NULL. The source is materialized into a concrete <see cref="Dictionary{TKey,TValue}"/>
    /// (when it is not already one) so the generated type info applies; with default options this yields the same
    /// JSON shape as the previous reflection-based path.
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
}
