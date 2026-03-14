using IntegratedS3.Abstractions.Services;
using IntegratedS3.Provider.S3.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Provider.S3.DependencyInjection;

public static class S3StorageServiceCollectionExtensions
{
    public static IServiceCollection AddS3Storage(this IServiceCollection services, S3StorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        Normalize(options);

        services.AddSingleton<IS3StorageClient>(_ => new AwsS3StorageClient(options));
        services.AddSingleton<IStorageBackend>(sp =>
            new S3StorageService(options, sp.GetRequiredService<IS3StorageClient>()));
        services.AddSingleton<IStorageObjectLocationResolver>(sp =>
            new S3StorageObjectLocationResolver(options, sp.GetRequiredService<IS3StorageClient>()));

        return services;
    }

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
