using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Client.DependencyInjection;

/// <summary>
/// First-party dependency-injection helpers for <see cref="IntegratedS3Client"/>.
/// </summary>
public static class IntegratedS3ClientServiceCollectionExtensions
{
    /// <summary>
    /// The default configuration section used by <see cref="AddIntegratedS3Client(IServiceCollection, IConfiguration)"/>.
    /// </summary>
    public const string ConfigurationSectionName = "IntegratedS3:Client";

    /// <summary>
    /// The named <see cref="HttpClient"/> registration used by the first-party IntegratedS3 client.
    /// </summary>
    public const string HttpClientName = "IntegratedS3.Client";

    public static IHttpClientBuilder AddIntegratedS3Client(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        AddIntegratedS3ClientOptions(services);
        return AddIntegratedS3ClientCore(services);
    }

    public static IHttpClientBuilder AddIntegratedS3Client(this IServiceCollection services, Uri baseAddress)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(baseAddress);

        return services.AddIntegratedS3Client(options => {
            options.BaseAddress = IntegratedS3ClientPathUtilities.NormalizeBaseAddress(baseAddress);
        });
    }

    public static IHttpClientBuilder AddIntegratedS3Client(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        return services.AddIntegratedS3Client(configuration.GetSection(ConfigurationSectionName));
    }

    public static IHttpClientBuilder AddIntegratedS3Client(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<IntegratedS3ClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(configure);

        return services.AddIntegratedS3Client(configuration.GetSection(ConfigurationSectionName), configure);
    }

    public static IHttpClientBuilder AddIntegratedS3Client(this IServiceCollection services, IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);

        AddIntegratedS3ClientOptions(services);
        services.AddOptions<IntegratedS3ClientOptions>()
            .Bind(section);

        return AddIntegratedS3ClientCore(services);
    }

    public static IHttpClientBuilder AddIntegratedS3Client(
        this IServiceCollection services,
        IConfigurationSection section,
        Action<IntegratedS3ClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        ArgumentNullException.ThrowIfNull(configure);

        AddIntegratedS3ClientOptions(services);
        services.AddOptions<IntegratedS3ClientOptions>()
            .Bind(section)
            .Configure(configure);

        return AddIntegratedS3ClientCore(services);
    }

    public static IHttpClientBuilder AddIntegratedS3Client(
        this IServiceCollection services,
        Action<IntegratedS3ClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        AddIntegratedS3ClientOptions(services);
        services.AddOptions<IntegratedS3ClientOptions>()
            .Configure(configure);

        return AddIntegratedS3ClientCore(services);
    }

    private static void AddIntegratedS3ClientOptions(IServiceCollection services)
    {
        services.AddOptions<IntegratedS3ClientOptions>()
            .PostConfigure(static options => {
                options.RoutePrefix = IntegratedS3ClientPathUtilities.NormalizeRoutePrefix(options.RoutePrefix);
                if (options.BaseAddress is { IsAbsoluteUri: true }) {
                    options.BaseAddress = IntegratedS3ClientPathUtilities.NormalizeBaseAddress(options.BaseAddress);
                }
            })
            .Validate(
                static options => options.BaseAddress is null || options.BaseAddress.IsAbsoluteUri,
                "The IntegratedS3 client base address must be absolute when configured.");
    }

    private static IHttpClientBuilder AddIntegratedS3ClientCore(IServiceCollection services)
    {
        services.TryAddTransient<IntegratedS3Client>(static serviceProvider => {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var options = serviceProvider.GetRequiredService<IOptions<IntegratedS3ClientOptions>>().Value;
            var httpClient = httpClientFactory.CreateClient(HttpClientName);

            return new IntegratedS3Client(httpClient, options.RoutePrefix);
        });
        services.TryAddTransient<IIntegratedS3Client>(static serviceProvider => serviceProvider.GetRequiredService<IntegratedS3Client>());

        return services.AddHttpClient(HttpClientName)
            .ConfigureHttpClient(static (serviceProvider, httpClient) => {
                var options = serviceProvider.GetRequiredService<IOptions<IntegratedS3ClientOptions>>().Value;
                if (httpClient.BaseAddress is null && options.BaseAddress is not null) {
                    httpClient.BaseAddress = options.BaseAddress;
                }
            });
    }
}
