using IntegratedS3.Abstractions.Services;
using IntegratedS3.EntityFramework.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace IntegratedS3.Core.DependencyInjection;

/// <summary>
/// DI helpers for replacing the default catalog services with EF Core-backed implementations.
/// </summary>
public static class EntityFrameworkStorageCatalogServiceCollectionExtensions
{
    /// <summary>
    /// Registers EF Core-backed catalog, object-state, and multipart-state services using default options.
    /// </summary>
    public static IServiceCollection AddEntityFrameworkStorageCatalog<TDbContext>(this IServiceCollection services)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddEntityFrameworkStorageCatalog<TDbContext>(static _ => { });
    }

    /// <summary>
    /// Registers EF Core-backed catalog, object-state, and multipart-state services and allows callers to configure the integration.
    /// </summary>
    public static IServiceCollection AddEntityFrameworkStorageCatalog<TDbContext>(this IServiceCollection services, Action<EntityFrameworkCatalogOptions> configure)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<EntityFrameworkCatalogOptions>()
            .Configure(configure);

        services.Replace(ServiceDescriptor.Singleton<IStorageCatalogStore, EntityFrameworkStorageCatalogStore<TDbContext>>());
        services.Replace(ServiceDescriptor.Singleton<IStorageObjectStateStore, CatalogStorageObjectStateStore>());
        services.Replace(ServiceDescriptor.Singleton<IStorageMultipartStateStore, EntityFrameworkStorageMultipartStateStore<TDbContext>>());

        return services;
    }
}
