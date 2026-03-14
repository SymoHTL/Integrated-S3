using System.Text.Json;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Core.Persistence;
using IntegratedS3.Core.Services;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class ServerSideEncryptionContractTests
{
    [Fact]
    public void ObjectInfo_SerializesServerSideEncryptionUsingStringEnumNames()
    {
        var payload = new ObjectInfo
        {
            BucketName = "docs",
            Key = "encrypted.txt",
            ContentLength = 42,
            LastModifiedUtc = DateTimeOffset.Parse("2026-03-01T00:00:00Z"),
            ServerSideEncryption = new ObjectServerSideEncryptionInfo
            {
                Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
                KeyId = "key-123"
            }
        };

        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions(JsonSerializerDefaults.Web));

        Assert.Contains("\"serverSideEncryption\":{\"algorithm\":\"KmsDsse\",\"keyId\":\"key-123\"}", json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EntityFrameworkCatalogStore_PersistsServerSideEncryptionMetadata()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var services = new ServiceCollection();
        services.AddDbContext<TestCatalogDbContext>(options => options.UseSqlite(connection));
        services.AddIntegratedS3Core();
        services.AddEntityFrameworkStorageCatalog<TestCatalogDbContext>(options => options.EnsureCreated = true);

        await using var serviceProvider = services.BuildServiceProvider();
        var catalogStore = serviceProvider.GetRequiredService<IStorageCatalogStore>();

        var encryption = new ObjectServerSideEncryptionInfo
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.Aes256
        };

        await catalogStore.UpsertObjectAsync("catalog-disk", new ObjectInfo
        {
            BucketName = "catalog-bucket",
            Key = "docs/encrypted.txt",
            VersionId = "version-001",
            IsLatest = true,
            ContentLength = 128,
            LastModifiedUtc = DateTimeOffset.Parse("2026-03-01T00:00:00Z"),
            ServerSideEncryption = encryption
        });

        var objects = await catalogStore.ListObjectsAsync("catalog-disk", "catalog-bucket");
        var stored = Assert.Single(objects);

        Assert.NotNull(stored.ServerSideEncryption);
        Assert.Equal(ObjectServerSideEncryptionAlgorithm.Aes256, stored.ServerSideEncryption!.Algorithm);
    }

    [Fact]
    public void ObjectRequests_CanCarryServerSideEncryptionContracts()
    {
        var settings = new ObjectServerSideEncryptionSettings
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.KmsDsse,
            KeyId = "kms-key-001",
            Context = new Dictionary<string, string>
            {
                ["tenant"] = "alpha"
            }
        };

        var putRequest = new PutObjectRequest
        {
            BucketName = "docs",
            Key = "put.txt",
            Content = new MemoryStream(),
            ServerSideEncryption = settings
        };

        var copyRequest = new CopyObjectRequest
        {
            SourceBucketName = "docs",
            SourceKey = "source.txt",
            DestinationBucketName = "docs",
            DestinationKey = "copy.txt",
            SourceServerSideEncryption = settings,
            DestinationServerSideEncryption = settings
        };

        var getRequest = new GetObjectRequest
        {
            BucketName = "docs",
            Key = "get.txt",
            ServerSideEncryption = settings
        };

        var headRequest = new HeadObjectRequest
        {
            BucketName = "docs",
            Key = "head.txt",
            ServerSideEncryption = settings
        };

        var multipartRequest = new InitiateMultipartUploadRequest
        {
            BucketName = "docs",
            Key = "multipart.txt",
            ServerSideEncryption = settings
        };

        Assert.Same(settings, putRequest.ServerSideEncryption);
        Assert.Same(settings, copyRequest.SourceServerSideEncryption);
        Assert.Same(settings, copyRequest.DestinationServerSideEncryption);
        Assert.Same(settings, getRequest.ServerSideEncryption);
        Assert.Same(settings, headRequest.ServerSideEncryption);
        Assert.Same(settings, multipartRequest.ServerSideEncryption);
    }

    private sealed class TestCatalogDbContext(DbContextOptions<TestCatalogDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.MapIntegratedS3Catalog();
        }
    }
}
