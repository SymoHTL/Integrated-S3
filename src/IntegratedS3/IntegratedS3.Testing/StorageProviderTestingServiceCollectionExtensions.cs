using IntegratedS3.Abstractions.Services;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Testing;

/// <summary>
/// Registers reusable in-memory support-state stores for provider contract tests.
/// </summary>
public static class StorageProviderTestingServiceCollectionExtensions
{
    /// <summary>
    /// Registers an <see cref="InMemoryObjectStateStore"/> as the
    /// <see cref="IStorageObjectStateStore"/> singleton for the container.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="stateStore">
    /// An optional pre-existing instance. When <see langword="null"/>, a new instance is created as a singleton.
    /// </param>
    /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddInMemoryStorageObjectStateStore(
        this IServiceCollection services,
        InMemoryObjectStateStore? stateStore = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (stateStore is null) {
            services.AddSingleton<InMemoryObjectStateStore>();
        }
        else {
            services.AddSingleton(stateStore);
        }

        services.AddSingleton<IStorageObjectStateStore>(static serviceProvider => serviceProvider.GetRequiredService<InMemoryObjectStateStore>());
        return services;
    }

    /// <summary>
    /// Registers an <see cref="InMemoryMultipartStateStore"/> as the
    /// <see cref="IStorageMultipartStateStore"/> singleton for the container.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="stateStore">
    /// An optional pre-existing instance. When <see langword="null"/>, a new instance is created as a singleton.
    /// </param>
    /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddInMemoryStorageMultipartStateStore(
        this IServiceCollection services,
        InMemoryMultipartStateStore? stateStore = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (stateStore is null) {
            services.AddSingleton<InMemoryMultipartStateStore>();
        }
        else {
            services.AddSingleton(stateStore);
        }

        services.AddSingleton<IStorageMultipartStateStore>(static serviceProvider => serviceProvider.GetRequiredService<InMemoryMultipartStateStore>());
        return services;
    }
}
