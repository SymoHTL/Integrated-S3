using IntegratedS3.Abstractions.Services;
using IntegratedS3.Provider.S3.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Provider.S3.DependencyInjection;

/// <summary>
/// Extension methods for registering an AWS S3-backed <see cref="IStorageBackend"/> with the dependency-injection container.
/// </summary>
public static class S3StorageServiceCollectionExtensions
{
    /// <summary>
    /// Registers an AWS S3-backed <see cref="IStorageBackend"/> using the supplied <paramref name="options"/>.
    /// Also registers the internal <see cref="IS3StorageClient"/> and <see cref="IStorageObjectLocationResolver"/> services.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="options">Pre-configured S3 storage options.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddS3Storage(this IServiceCollection services, S3StorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        Normalize(options);

        services.AddSingleton<IS3StorageClient>(sp =>
            new AwsS3StorageClient(options, sp.GetRequiredService<ILoggerFactory>().CreateLogger<AwsS3StorageClient>()));
        services.AddSingleton<IStorageBackend>(sp =>
            new S3StorageService(options, sp.GetRequiredService<IS3StorageClient>(),
            sp.GetRequiredService<ILoggerFactory>().CreateLogger<S3StorageService>()));
        services.AddSingleton<IStorageObjectLocationResolver>(sp =>
            new S3StorageObjectLocationResolver(options, sp.GetRequiredService<IS3StorageClient>()));

        return services;
    }

    /// <summary>
    /// Registers an AWS S3-backed <see cref="IStorageBackend"/> using a delegate to configure <see cref="S3StorageOptions"/>.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configure">A delegate that configures the <see cref="S3StorageOptions"/>.</param>
    /// <returns>The same <see cref="IServiceCollection"/> for chaining.</returns>
    public static IServiceCollection AddS3Storage(this IServiceCollection services, Action<S3StorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new S3StorageOptions();
        configure(options);

        return services.AddS3Storage(options);
    }

    private static void Normalize(S3StorageOptions options)
    {
        options.ProviderName = string.IsNullOrWhiteSpace(options.ProviderName)
            ? "s3-primary"
            : options.ProviderName.Trim();

        options.Region = string.IsNullOrWhiteSpace(options.Region)
            ? "us-east-1"
            : options.Region.Trim();

        if (options.ServiceUrl is not null)
            options.ServiceUrl = string.IsNullOrWhiteSpace(options.ServiceUrl) ? null : options.ServiceUrl.Trim();

        if (options.AccessKey is not null)
            options.AccessKey = string.IsNullOrWhiteSpace(options.AccessKey) ? null : options.AccessKey.Trim();

        if (options.SecretKey is not null)
            options.SecretKey = string.IsNullOrWhiteSpace(options.SecretKey) ? null : options.SecretKey.Trim();
    }
}
