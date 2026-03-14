using IntegratedS3.Abstractions.Services;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Testing;

/// <summary>
/// Registers reusable in-memory support-state stores for provider contract tests.
/// </summary>
public static class StorageProviderTestingServiceCollectionExtensions
{
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
