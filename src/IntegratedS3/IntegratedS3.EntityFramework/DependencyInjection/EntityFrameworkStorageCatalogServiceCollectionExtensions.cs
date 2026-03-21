using IntegratedS3.Abstractions.Services;
using IntegratedS3.EntityFramework.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace IntegratedS3.Core.DependencyInjection;

/// <summary>
/// Extension methods for registering EF Core-backed <see cref="IStorageCatalogStore"/> implementations
/// in a <see cref="IServiceCollection"/>.
/// </summary>
public static class EntityFrameworkStorageCatalogServiceCollectionExtensions
{
    /// <summary>
    /// Registers an EF Core-backed <see cref="IStorageCatalogStore"/> using the specified <typeparamref name="TDbContext"/>
    /// with default <see cref="EntityFrameworkCatalogOptions"/>.
    /// </summary>
    /// <typeparam name="TDbContext">The <see cref="DbContext"/> type that contains the IntegratedS3 catalog entity mappings.</typeparam>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddEntityFrameworkStorageCatalog<TDbContext>(this IServiceCollection services)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddEntityFrameworkStorageCatalog<TDbContext>(static _ => { });
    }

    /// <summary>
    /// Registers an EF Core-backed <see cref="IStorageCatalogStore"/> using the specified <typeparamref name="TDbContext"/>
    /// and applies the given <paramref name="configure"/> callback to <see cref="EntityFrameworkCatalogOptions"/>.
    /// </summary>
    /// <typeparam name="TDbContext">The <see cref="DbContext"/> type that contains the IntegratedS3 catalog entity mappings.</typeparam>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <param name="configure">A delegate to configure <see cref="EntityFrameworkCatalogOptions"/>.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
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