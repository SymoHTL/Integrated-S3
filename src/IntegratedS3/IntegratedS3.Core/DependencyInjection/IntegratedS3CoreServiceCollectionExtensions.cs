using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace IntegratedS3.Core.DependencyInjection;

public static class IntegratedS3CoreServiceCollectionExtensions
{
    public static IServiceCollection AddIntegratedS3Core(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddIntegratedS3Core(static _ => { });
    }

    public static IServiceCollection AddIntegratedS3Core(this IServiceCollection services, Action<IntegratedS3CoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<IntegratedS3CoreOptions>()
            .Configure(configure);

        services.TryAddSingleton<IStorageCatalogStore, NullStorageCatalogStore>();
        services.TryAddSingleton<IIntegratedS3AuthorizationService, AllowAllIntegratedS3AuthorizationService>();
        services.TryAddSingleton<IIntegratedS3RequestContextAccessor, AsyncLocalIntegratedS3RequestContextAccessor>();
        services.TryAddSingleton<IStorageBackendHealthEvaluator, DefaultStorageBackendHealthEvaluator>();
        services.TryAddSingleton<OrchestratedStorageService>();
        services.TryAddSingleton<IStorageService, AuthorizingStorageService>();

        return services;
    }
}
