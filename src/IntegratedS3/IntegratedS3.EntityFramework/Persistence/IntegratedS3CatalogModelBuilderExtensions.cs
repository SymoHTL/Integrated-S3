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
            entity.Property(static bucket => bucket.ProviderName).IsRequired();
            entity.Property(static bucket => bucket.BucketName).IsRequired();
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
            entity.Property(static @object => @object.ProviderName).IsRequired();
            entity.Property(static @object => @object.BucketName).IsRequired();
            entity.Property(static @object => @object.Key).IsRequired();
            entity.HasIndex(static @object => new { @object.ProviderName, @object.BucketName, @object.Key, @object.VersionId }).IsUnique();
            entity.HasIndex(static @object => new { @object.ProviderName, @object.BucketName, @object.Key, @object.IsLatest });
        });

        modelBuilder.Entity<MultipartUploadCatalogRecord>(entity => {
            entity.ToTable("IntegratedS3MultipartUploads");
            entity.HasKey(static upload => upload.Id);
            entity.Property(static upload => upload.ProviderName).IsRequired();
            entity.Property(static upload => upload.BucketName).IsRequired();
            entity.Property(static upload => upload.Key).IsRequired();
            entity.Property(static upload => upload.UploadId).IsRequired();
            entity.HasIndex(static upload => new { upload.ProviderName, upload.BucketName, upload.Key, upload.UploadId }).IsUnique();
        });

        return modelBuilder;
    }
}