using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace IntegratedS3.Core.DependencyInjection;

/// <summary>
/// Extension methods for registering IntegratedS3 core orchestration services
/// into an <see cref="IServiceCollection"/>.
/// </summary>
public static class IntegratedS3CoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers the IntegratedS3 core orchestration services using default
    /// <see cref="IntegratedS3CoreOptions"/>. Services include
    /// <see cref="IStorageService"/>, health monitoring, replication, and authorization.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddIntegratedS3Core(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddIntegratedS3Core(static _ => { });
    }

    /// <summary>
    /// Registers the IntegratedS3 core orchestration services with the specified
    /// configuration. Services include <see cref="IStorageService"/>, health monitoring,
    /// replication, and authorization.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configure">A delegate to configure <see cref="IntegratedS3CoreOptions"/>.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddIntegratedS3Core(this IServiceCollection services, Action<IntegratedS3CoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddLogging();
        services.AddOptions<IntegratedS3CoreOptions>()
            .Configure(configure);

        services.TryAddSingleton<IStorageCatalogStore, NullStorageCatalogStore>();
        services.TryAddSingleton<IStorageObjectLocationResolver, NullStorageObjectLocationResolver>();
        services.TryAddSingleton<IIntegratedS3AuthorizationService, AllowAllIntegratedS3AuthorizationService>();
        services.TryAddSingleton<IStorageAuthorizationCompatibilityService, InMemoryStorageAuthorizationCompatibilityService>();
        services.TryAddSingleton<IIntegratedS3RequestContextAccessor, AsyncLocalIntegratedS3RequestContextAccessor>();
        services.TryAddSingleton<IStoragePresignStrategy, UnsupportedStoragePresignStrategy>();
        services.TryAddSingleton<IStorageBackendHealthEvaluator, DefaultStorageBackendHealthEvaluator>();
        services.TryAddSingleton<IStorageBackendHealthProbe, DefaultStorageBackendHealthProbe>();
        services.TryAddSingleton<IStorageReplicaRepairBacklog, InMemoryStorageReplicaRepairBacklog>();
        services.TryAddSingleton<IStorageReplicaRepairDispatcher, InProcessStorageReplicaRepairDispatcher>();
        services.TryAddSingleton<IStorageReplicaRepairService, StorageReplicaRepairService>();
        services.TryAddSingleton<IStorageAdminDiagnosticsProvider, StorageAdminDiagnosticsProvider>();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<StorageBackendHealthMonitor>();
        services.TryAddSingleton<OrchestratedStorageService>();
        services.TryAddSingleton<IStorageService, AuthorizingStorageService>();
        services.TryAddSingleton<IStoragePresignService, AuthorizingStoragePresignService>();

        return services;
    }

    /// <summary>
    /// Replaces the default allow-all authorization service with
    /// <see cref="ScopeBasedIntegratedS3AuthorizationService"/>, which enforces the <c>scope</c> claims carried by
    /// authenticated principals. This is opt-in: the default registered by <see cref="AddIntegratedS3Core(IServiceCollection)"/>
    /// is <see cref="AllowAllIntegratedS3AuthorizationService"/> (grants everything) for backward compatibility.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="configure">An optional delegate to configure <see cref="ScopeBasedIntegratedS3AuthorizationOptions"/> (e.g. the scope grammar and unscoped-principal behavior).</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    /// <remarks>
    /// Call this alongside <see cref="AddIntegratedS3Core(IServiceCollection)"/> (either order works). See
    /// <see cref="ScopeBasedIntegratedS3AuthorizationOptions"/> for the supported scope grammar
    /// (<c>admin</c>, <c>read</c>, <c>write</c>, <c>bucket:NAME[:read|write]</c>).
    /// </remarks>
    public static IServiceCollection AddIntegratedS3ScopeBasedAuthorization(
        this IServiceCollection services,
        Action<ScopeBasedIntegratedS3AuthorizationOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var optionsBuilder = services.AddOptions<ScopeBasedIntegratedS3AuthorizationOptions>();
        if (configure is not null) {
            optionsBuilder.Configure(configure);
        }

        services.Replace(ServiceDescriptor.Singleton<IIntegratedS3AuthorizationService, ScopeBasedIntegratedS3AuthorizationService>());
        return services;
    }
}
