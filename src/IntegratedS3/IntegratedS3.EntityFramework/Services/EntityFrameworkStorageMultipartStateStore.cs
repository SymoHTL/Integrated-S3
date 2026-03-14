using System.Runtime.CompilerServices;
using System.Text.Json;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Persistence;
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
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        var record = await dbContext.Set<MultipartUploadCatalogRecord>()
            .AsNoTracking()
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
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        var records = await dbContext.Set<MultipartUploadCatalogRecord>()
            .AsNoTracking()
            .Where(existing =>
                existing.ProviderName == providerName
                && existing.BucketName == bucketName)
            .ToListAsync(cancellationToken);

        foreach (var record in records
                     .Where(existing => string.IsNullOrWhiteSpace(prefix) || existing.Key.StartsWith(prefix, StringComparison.Ordinal))
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
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

        var states = dbContext.Set<MultipartUploadCatalogRecord>();
        var record = await states.SingleOrDefaultAsync(existing =>
            existing.ProviderName == providerName
            && existing.BucketName == state.BucketName
            && existing.Key == state.Key
            && existing.UploadId == state.UploadId,
            cancellationToken);

        if (record is null) {
            record = new MultipartUploadCatalogRecord
            {
                ProviderName = providerName,
                BucketName = state.BucketName,
                Key = state.Key,
                UploadId = state.UploadId
            };
            states.Add(record);
        }

        record.InitiatedAtUtc = state.InitiatedAtUtc;
        record.ContentType = state.ContentType;
        record.CacheControl = state.CacheControl;
        record.ContentDisposition = state.ContentDisposition;
        record.ContentEncoding = state.ContentEncoding;
        record.ContentLanguage = state.ContentLanguage;
        record.ExpiresUtc = state.ExpiresUtc;
        record.ChecksumAlgorithm = state.ChecksumAlgorithm;
        record.MetadataJson = state.Metadata is null ? null : JsonSerializer.Serialize(state.Metadata);
        record.TagsJson = state.Tags is null ? null : JsonSerializer.Serialize(state.Tags);
        record.LastSyncedAtUtc = DateTimeOffset.UtcNow;

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
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        ValidateModel(dbContext);

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
            Metadata = string.IsNullOrWhiteSpace(record.MetadataJson)
                ? null
                : JsonSerializer.Deserialize<Dictionary<string, string>>(record.MetadataJson),
            Tags = string.IsNullOrWhiteSpace(record.TagsJson)
                ? null
                : JsonSerializer.Deserialize<Dictionary<string, string>>(record.TagsJson)
        };
    }
}
