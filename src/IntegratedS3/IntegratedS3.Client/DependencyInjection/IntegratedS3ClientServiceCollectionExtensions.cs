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

    /// <summary>
    /// Registers <see cref="IntegratedS3Client"/> and <see cref="IIntegratedS3Client"/> with default options.
    /// </summary>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <returns>An <see cref="IHttpClientBuilder"/> for further configuration of the underlying <see cref="HttpClient"/>.</returns>
    public static IHttpClientBuilder AddIntegratedS3Client(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        AddIntegratedS3ClientOptions(services);
        return AddIntegratedS3ClientCore(services);
    }

    /// <summary>
    /// Registers <see cref="IntegratedS3Client"/> and <see cref="IIntegratedS3Client"/>,
    /// setting <see cref="IntegratedS3ClientOptions.BaseAddress"/> to <paramref name="baseAddress"/>.
    /// </summary>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <param name="baseAddress">The absolute base address of the IntegratedS3 host.</param>
    /// <returns>An <see cref="IHttpClientBuilder"/> for further configuration of the underlying <see cref="HttpClient"/>.</returns>
    public static IHttpClientBuilder AddIntegratedS3Client(this IServiceCollection services, Uri baseAddress)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(baseAddress);

        return services.AddIntegratedS3Client(options => {
            options.BaseAddress = IntegratedS3ClientPathUtilities.NormalizeBaseAddress(baseAddress);
        });
    }

    /// <summary>
    /// Registers <see cref="IntegratedS3Client"/> and <see cref="IIntegratedS3Client"/>,
    /// binding options from the <see cref="ConfigurationSectionName"/> section of <paramref name="configuration"/>.
    /// </summary>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <param name="configuration">The root <see cref="IConfiguration"/> containing the IntegratedS3 client section.</param>
    /// <returns>An <see cref="IHttpClientBuilder"/> for further configuration of the underlying <see cref="HttpClient"/>.</returns>
    public static IHttpClientBuilder AddIntegratedS3Client(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        return services.AddIntegratedS3Client(configuration.GetSection(ConfigurationSectionName));
    }

    /// <summary>
    /// Registers <see cref="IntegratedS3Client"/> and <see cref="IIntegratedS3Client"/>,
    /// binding options from the <see cref="ConfigurationSectionName"/> section of <paramref name="configuration"/>
    /// and then applying the <paramref name="configure"/> callback.
    /// </summary>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <param name="configuration">The root <see cref="IConfiguration"/> containing the IntegratedS3 client section.</param>
    /// <param name="configure">A delegate to further configure <see cref="IntegratedS3ClientOptions"/>.</param>
    /// <returns>An <see cref="IHttpClientBuilder"/> for further configuration of the underlying <see cref="HttpClient"/>.</returns>
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

    /// <summary>
    /// Registers <see cref="IntegratedS3Client"/> and <see cref="IIntegratedS3Client"/>,
    /// binding options from the provided <paramref name="section"/>.
    /// </summary>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <param name="section">The <see cref="IConfigurationSection"/> to bind <see cref="IntegratedS3ClientOptions"/> from.</param>
    /// <returns>An <see cref="IHttpClientBuilder"/> for further configuration of the underlying <see cref="HttpClient"/>.</returns>
    public static IHttpClientBuilder AddIntegratedS3Client(this IServiceCollection services, IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);

        AddIntegratedS3ClientOptions(services);
        services.AddOptions<IntegratedS3ClientOptions>()
            .Bind(section);

        return AddIntegratedS3ClientCore(services);
    }

    /// <summary>
    /// Registers <see cref="IntegratedS3Client"/> and <see cref="IIntegratedS3Client"/>,
    /// binding options from the provided <paramref name="section"/> and then applying the <paramref name="configure"/> callback.
    /// </summary>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <param name="section">The <see cref="IConfigurationSection"/> to bind <see cref="IntegratedS3ClientOptions"/> from.</param>
    /// <param name="configure">A delegate to further configure <see cref="IntegratedS3ClientOptions"/>.</param>
    /// <returns>An <see cref="IHttpClientBuilder"/> for further configuration of the underlying <see cref="HttpClient"/>.</returns>
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

    /// <summary>
    /// Registers <see cref="IntegratedS3Client"/> and <see cref="IIntegratedS3Client"/>,
    /// applying the <paramref name="configure"/> callback to <see cref="IntegratedS3ClientOptions"/>.
    /// </summary>
    /// <param name="services">The service collection to add registrations to.</param>
    /// <param name="configure">A delegate to configure <see cref="IntegratedS3ClientOptions"/>.</param>
    /// <returns>An <see cref="IHttpClientBuilder"/> for further configuration of the underlying <see cref="HttpClient"/>.</returns>
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
