using IntegratedS3.Abstractions.Blobs;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Engine.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Engine.DependencyInjection;

/// <summary>
/// Extension methods for registering the IntegratedS3 engine as an <see cref="IStorageBackend"/>.
/// </summary>
public static class IntegratedS3EngineServiceCollectionExtensions
{
    /// <summary>
    /// Registers the engine as an <see cref="IStorageBackend"/>. It keeps its metadata in SQLite and its blobs in the
    /// registered <see cref="IBlobStore"/>, or, when none is registered, in a <see cref="LocalDiskBlobStore"/> under
    /// <see cref="IntegratedS3EngineOptions.BlobRootPath"/>. It reads the time from the registered
    /// <see cref="TimeProvider"/>, or from <see cref="TimeProvider.System"/> when none is registered.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configure">A delegate that configures the <see cref="IntegratedS3EngineOptions"/>, or <see langword="null"/> for the defaults.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddIntegratedS3Engine(this IServiceCollection services, Action<IntegratedS3EngineOptions>? configure = null)
    {
        var options = new IntegratedS3EngineOptions();
        configure?.Invoke(options);
        return services.AddIntegratedS3Engine(options);
    }

    /// <summary>
    /// Registers the engine as an <see cref="IStorageBackend"/> with the given options; see the overload that takes a
    /// delegate.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="options">The engine's options.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddIntegratedS3Engine(this IServiceCollection services, IntegratedS3EngineOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        services.AddSingleton<IStorageBackend>(serviceProvider => new EngineStorageBackend(
            options,
            serviceProvider.GetService<IBlobStore>() ?? new LocalDiskBlobStore(options.BlobRootPath),
            serviceProvider.GetService<ILoggerFactory>()?.CreateLogger<EngineStorageBackend>(),
            serviceProvider.GetService<TimeProvider>()));

        return services;
    }
}
