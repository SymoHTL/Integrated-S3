using Microsoft.EntityFrameworkCore;

namespace IntegratedS3.Core.Persistence;

public static class IntegratedS3CatalogModelBuilderExtensions
{
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
            entity.HasIndex(static @object => new { @object.ProviderName, @object.BucketName, @object.Key }).IsUnique();
        });

        return modelBuilder;
    }
}