using IntegratedS3.Abstractions.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Provider.Disk.DependencyInjection;

/// <summary>
/// Extension methods for registering a disk-backed <see cref="IStorageBackend"/> with the dependency-injection container.
/// </summary>
public static class DiskStorageServiceCollectionExtensions
{
    /// <summary>
    /// Registers a disk-backed <see cref="IStorageBackend"/> that stores objects on the local filesystem
    /// using the supplied <paramref name="options"/>.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="options">Pre-configured disk storage options.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddDiskStorage(this IServiceCollection services, DiskStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        Normalize(options);

        services.AddSingleton<IStorageBackend>(serviceProvider => new DiskStorageService(
            options,
            serviceProvider.GetService<IStorageObjectStateStore>(),
            serviceProvider.GetService<IStorageMultipartStateStore>(),
            serviceProvider.GetService<ILoggerFactory>()?.CreateLogger<DiskStorageService>()));

        return services;
    }

    /// <summary>
    /// Registers a disk-backed <see cref="IStorageBackend"/> that stores objects on the local filesystem,
    /// using a delegate to configure <see cref="DiskStorageOptions"/>.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configure">A delegate that configures the <see cref="DiskStorageOptions"/>.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddDiskStorage(this IServiceCollection services, Action<DiskStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new DiskStorageOptions();
        configure(options);

        return services.AddDiskStorage(options);
    }

    private static void Normalize(DiskStorageOptions options)
    {
        options.ProviderName = string.IsNullOrWhiteSpace(options.ProviderName)
            ? "disk-primary"
            : options.ProviderName.Trim();

        options.RootPath = string.IsNullOrWhiteSpace(options.RootPath)
            ? "App_Data/IntegratedS3"
            : options.RootPath.Trim();
    }
}
