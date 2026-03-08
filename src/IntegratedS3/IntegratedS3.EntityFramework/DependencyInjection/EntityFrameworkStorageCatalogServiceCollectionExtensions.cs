using IntegratedS3.Abstractions.Services;
using IntegratedS3.EntityFramework.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace IntegratedS3.Core.DependencyInjection;

public static class EntityFrameworkStorageCatalogServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkStorageCatalog<TDbContext>(this IServiceCollection services)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddEntityFrameworkStorageCatalog<TDbContext>(static _ => { });
    }

    public static IServiceCollection AddEntityFrameworkStorageCatalog<TDbContext>(this IServiceCollection services, Action<EntityFrameworkCatalogOptions> configure)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<EntityFrameworkCatalogOptions>()
            .Configure(configure);

        services.Replace(ServiceDescriptor.Singleton<IStorageCatalogStore, EntityFrameworkStorageCatalogStore<TDbContext>>());

        return services;
    }
}