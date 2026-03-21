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

        Assert.Contains("\"algorithm\":\"KmsDsse\"", json, StringComparison.Ordinal);
        Assert.Contains("\"keyId\":\"key-123\"", json, StringComparison.Ordinal);
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

    [Fact]
    public void BucketDefaultEncryptionContracts_CanCarryManagedRules()
    {
        var rule = new BucketDefaultEncryptionRule
        {
            Algorithm = ObjectServerSideEncryptionAlgorithm.Kms,
            KeyId = "kms-default-key"
        };

        var configuration = new BucketDefaultEncryptionConfiguration
        {
            BucketName = "docs",
            Rule = rule
        };

        var putRequest = new PutBucketDefaultEncryptionRequest
        {
            BucketName = "docs",
            Rule = rule
        };

        var deleteRequest = new DeleteBucketDefaultEncryptionRequest
        {
            BucketName = "docs"
        };

        Assert.Same(rule, configuration.Rule);
        Assert.Same(rule, putRequest.Rule);
        Assert.Equal("docs", deleteRequest.BucketName);
    }

    [Fact]
    public void ObjectRequests_CanCarryCustomerEncryptionContracts()
    {
        var settings = new ObjectCustomerEncryptionSettings
        {
            Algorithm = "AES256",
            Key = Convert.ToBase64String(new byte[32]),
            KeyMd5 = Convert.ToBase64String(new byte[16])
        };

        var putRequest = new PutObjectRequest
        {
            BucketName = "docs",
            Key = "put.txt",
            Content = new MemoryStream(),
            CustomerEncryption = settings
        };

        var getRequest = new GetObjectRequest
        {
            BucketName = "docs",
            Key = "get.txt",
            CustomerEncryption = settings
        };

        var headRequest = new HeadObjectRequest
        {
            BucketName = "docs",
            Key = "head.txt",
            CustomerEncryption = settings
        };

        var copyRequest = new CopyObjectRequest
        {
            SourceBucketName = "docs",
            SourceKey = "source.txt",
            DestinationBucketName = "docs",
            DestinationKey = "copy.txt",
            SourceCustomerEncryption = settings,
            DestinationCustomerEncryption = settings
        };

        var multipartRequest = new InitiateMultipartUploadRequest
        {
            BucketName = "docs",
            Key = "multipart.txt",
            CustomerEncryption = settings
        };

        var uploadPartRequest = new UploadMultipartPartRequest
        {
            BucketName = "docs",
            Key = "multipart.txt",
            UploadId = "upload-1",
            PartNumber = 1,
            Content = new MemoryStream(),
            CustomerEncryption = settings
        };

        var uploadPartCopyRequest = new UploadPartCopyRequest
        {
            BucketName = "docs",
            Key = "multipart.txt",
            UploadId = "upload-1",
            PartNumber = 1,
            SourceBucketName = "docs",
            SourceKey = "source.txt",
            SourceCustomerEncryption = settings,
            DestinationCustomerEncryption = settings
        };

        Assert.Same(settings, putRequest.CustomerEncryption);
        Assert.Same(settings, getRequest.CustomerEncryption);
        Assert.Same(settings, headRequest.CustomerEncryption);
        Assert.Same(settings, copyRequest.SourceCustomerEncryption);
        Assert.Same(settings, copyRequest.DestinationCustomerEncryption);
        Assert.Same(settings, multipartRequest.CustomerEncryption);
        Assert.Same(settings, uploadPartRequest.CustomerEncryption);
        Assert.Same(settings, uploadPartCopyRequest.SourceCustomerEncryption);
        Assert.Same(settings, uploadPartCopyRequest.DestinationCustomerEncryption);

        var info = new ObjectInfo
        {
            BucketName = "docs",
            Key = "encrypted.txt",
            ContentLength = 42,
            LastModifiedUtc = DateTimeOffset.UtcNow,
            CustomerEncryption = new ObjectCustomerEncryptionInfo
            {
                Algorithm = "AES256",
                KeyMd5 = settings.KeyMd5
            }
        };

        Assert.NotNull(info.CustomerEncryption);
        Assert.Equal("AES256", info.CustomerEncryption!.Algorithm);
        Assert.Equal(settings.KeyMd5, info.CustomerEncryption.KeyMd5);

        var multipartInfo = new MultipartUploadInfo
        {
            BucketName = "docs",
            Key = "multipart.txt",
            UploadId = "upload-1",
            CustomerEncryption = new ObjectCustomerEncryptionInfo
            {
                Algorithm = "AES256",
                KeyMd5 = settings.KeyMd5
            }
        };

        Assert.NotNull(multipartInfo.CustomerEncryption);
        Assert.Equal("AES256", multipartInfo.CustomerEncryption!.Algorithm);
    }

    private sealed class TestCatalogDbContext(DbContextOptions<TestCatalogDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.MapIntegratedS3Catalog();
        }
    }
}
