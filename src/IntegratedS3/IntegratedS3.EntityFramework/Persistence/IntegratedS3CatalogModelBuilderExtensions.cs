using Microsoft.EntityFrameworkCore;

namespace IntegratedS3.Core.Persistence;

/// <summary>
/// Extension methods for configuring EF Core entity mappings for IntegratedS3 catalog tables.
/// </summary>
public static class IntegratedS3CatalogModelBuilderExtensions
{
    /// <summary>
    /// Configures the EF Core entity mappings for <see cref="BucketCatalogRecord"/>,
    /// <see cref="ObjectCatalogRecord"/>, and <see cref="MultipartUploadCatalogRecord"/>
    /// used by the IntegratedS3 catalog persistence layer.
    /// </summary>
    /// <param name="modelBuilder">The <see cref="ModelBuilder"/> to apply the mappings to.</param>
    /// <returns>The same <see cref="ModelBuilder"/> for chaining.</returns>
    public static ModelBuilder MapIntegratedS3Catalog(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<BucketCatalogRecord>(entity => {
            entity.ToTable("IntegratedS3Buckets");
            entity.HasKey(static bucket => bucket.Id);
            entity.Property(static bucket => bucket.ProviderName).IsRequired().HasMaxLength(256);
            entity.Property(static bucket => bucket.BucketName).IsRequired().HasMaxLength(63);
            entity.HasIndex(static bucket => new { bucket.ProviderName, bucket.BucketName }).IsUnique();
            entity.HasMany(static bucket => bucket.Objects)
                .WithOne()
                .HasForeignKey(static @object => new { @object.ProviderName, @object.BucketName })
                .HasPrincipalKey(static bucket => new { bucket.ProviderName, bucket.BucketName })
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<ObjectCatalogRecord>(entity => {
            entity.ToTable("IntegratedS3Objects");
            entity.HasKey(static @object => @object.Id);
            entity.Property(static @object => @object.ProviderName).IsRequired().HasMaxLength(256);
            entity.Property(static @object => @object.BucketName).IsRequired().HasMaxLength(63);
            entity.Property(static @object => @object.Key).IsRequired().HasMaxLength(1024);
            entity.Property(static @object => @object.VersionId).HasMaxLength(128);
            entity.Property(static @object => @object.ContentType).HasMaxLength(256);
            entity.Property(static @object => @object.CacheControl).HasMaxLength(256);
            entity.Property(static @object => @object.ContentDisposition).HasMaxLength(512);
            entity.Property(static @object => @object.ContentEncoding).HasMaxLength(128);
            entity.Property(static @object => @object.ContentLanguage).HasMaxLength(128);
            entity.Property(static @object => @object.ETag).HasMaxLength(128);
            entity.Property(static @object => @object.MetadataJson).HasMaxLength(32_768);
            entity.Property(static @object => @object.TagsJson).HasMaxLength(32_768);
            entity.Property(static @object => @object.ChecksumsJson).HasMaxLength(4096);
            entity.Property(static @object => @object.ServerSideEncryptionKeyId).HasMaxLength(512);
            entity.Property(static @object => @object.Version).IsConcurrencyToken();
            entity.HasIndex(static @object => new { @object.ProviderName, @object.BucketName, @object.Key, @object.VersionId }).IsUnique();
            // Enforce the "at most one latest version per key" invariant as a hard database constraint so that
            // two concurrent writers cannot both commit an IsLatest = true row for the same key. A filtered unique
            // index is used (only rows with IsLatest = true participate) so historical, non-latest versions are
            // unaffected. The filter predicate targets the column name emitted for the boolean flag; it is valid
            // for the SQL Server, PostgreSQL, and SQLite providers used with this catalog.
            entity.HasIndex(static @object => new { @object.ProviderName, @object.BucketName, @object.Key })
                .IsUnique()
                .HasFilter("\"IsLatest\" = 1")
                .HasDatabaseName("IX_IntegratedS3Objects_SingleLatestPerKey");
            entity.HasIndex(static @object => new { @object.ProviderName, @object.BucketName, @object.Key, @object.IsLatest });
        });

        modelBuilder.Entity<MultipartUploadCatalogRecord>(entity => {
            entity.ToTable("IntegratedS3MultipartUploads");
            entity.HasKey(static upload => upload.Id);
            entity.Property(static upload => upload.ProviderName).IsRequired().HasMaxLength(256);
            entity.Property(static upload => upload.BucketName).IsRequired().HasMaxLength(63);
            entity.Property(static upload => upload.Key).IsRequired().HasMaxLength(1024);
            entity.Property(static upload => upload.UploadId).IsRequired().HasMaxLength(128);
            entity.Property(static upload => upload.ContentType).HasMaxLength(256);
            entity.Property(static upload => upload.CacheControl).HasMaxLength(256);
            entity.Property(static upload => upload.ContentDisposition).HasMaxLength(512);
            entity.Property(static upload => upload.ContentEncoding).HasMaxLength(128);
            entity.Property(static upload => upload.ContentLanguage).HasMaxLength(128);
            entity.Property(static upload => upload.MetadataJson).HasMaxLength(32_768);
            entity.Property(static upload => upload.TagsJson).HasMaxLength(32_768);
            entity.Property(static upload => upload.ChecksumAlgorithm).HasMaxLength(32);
            entity.HasIndex(static upload => new { upload.ProviderName, upload.BucketName, upload.Key, upload.UploadId }).IsUnique();
        });

        return modelBuilder;
    }
}