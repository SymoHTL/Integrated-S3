using IntegratedS3.Abstractions.Services;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Provider.Disk.DependencyInjection;

public static class DiskStorageServiceCollectionExtensions
{
    public static IServiceCollection AddDiskStorage(this IServiceCollection services, DiskStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        Normalize(options);

        services.AddSingleton<IStorageBackend>(_ => new DiskStorageService(options));

        return services;
    }

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
