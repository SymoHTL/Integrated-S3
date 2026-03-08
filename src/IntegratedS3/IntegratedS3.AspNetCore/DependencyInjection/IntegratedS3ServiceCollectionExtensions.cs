using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore.Services;
using IntegratedS3.AspNetCore.Serialization;
using IntegratedS3.Core.DependencyInjection;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace IntegratedS3.AspNetCore.DependencyInjection;

public static class IntegratedS3ServiceCollectionExtensions
{
    public static IServiceCollection AddIntegratedS3(this IServiceCollection services)
    {
        return services.AddIntegratedS3(static _ => { });
    }

    public static IServiceCollection AddIntegratedS3(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        return services.AddIntegratedS3(configuration.GetSection("IntegratedS3"));
    }

    public static IServiceCollection AddIntegratedS3(this IServiceCollection services, IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(section);

        services.AddOptions<IntegratedS3Options>()
            .Bind(section);

        return services.AddIntegratedS3CoreServices();
    }

    public static IServiceCollection AddIntegratedS3(this IServiceCollection services, Action<IntegratedS3Options> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<IntegratedS3Options>()
            .Configure(configure);

        return services.AddIntegratedS3CoreServices();
    }

    private static IServiceCollection AddIntegratedS3CoreServices(this IServiceCollection services)
    {
        if (!services.Any(static serviceDescriptor => serviceDescriptor.ServiceType == typeof(IStorageService))) {
            services.AddIntegratedS3Core();
        }

        services.PostConfigure<IntegratedS3Options>(options => {
            options.ServiceName = string.IsNullOrWhiteSpace(options.ServiceName)
                ? "Integrated S3"
                : options.ServiceName.Trim();

            options.RoutePrefix = NormalizeRoutePrefix(options.RoutePrefix);
            options.SignatureAuthenticationRegion = string.IsNullOrWhiteSpace(options.SignatureAuthenticationRegion)
                ? "us-east-1"
                : options.SignatureAuthenticationRegion.Trim();
            options.SignatureAuthenticationService = string.IsNullOrWhiteSpace(options.SignatureAuthenticationService)
                ? "s3"
                : options.SignatureAuthenticationService.Trim();
            options.AllowedSignatureClockSkewMinutes = options.AllowedSignatureClockSkewMinutes <= 0
                ? 5
                : options.AllowedSignatureClockSkewMinutes;
            options.MaximumPresignedUrlExpirySeconds = options.MaximumPresignedUrlExpirySeconds <= 0
                ? 3600
                : options.MaximumPresignedUrlExpirySeconds;
            options.AccessKeyCredentials = (options.AccessKeyCredentials ?? [])
                .Where(static credential => !string.IsNullOrWhiteSpace(credential.AccessKeyId) && !string.IsNullOrWhiteSpace(credential.SecretAccessKey))
                .Select(static credential => new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = credential.AccessKeyId.Trim(),
                    SecretAccessKey = credential.SecretAccessKey.Trim(),
                    DisplayName = string.IsNullOrWhiteSpace(credential.DisplayName) ? null : credential.DisplayName.Trim(),
                    Scopes = (credential.Scopes ?? [])
                        .Where(static scope => !string.IsNullOrWhiteSpace(scope))
                        .Select(static scope => scope.Trim())
                        .Distinct(StringComparer.Ordinal)
                        .ToList()
                })
                .GroupBy(static credential => credential.AccessKeyId, StringComparer.Ordinal)
                .Select(static group => group.First())
                .ToList();
            options.VirtualHostedStyleHostSuffixes = (options.VirtualHostedStyleHostSuffixes ?? [])
                .Select(static suffix => NormalizeHostSuffix(suffix))
                .Where(static suffix => !string.IsNullOrWhiteSpace(suffix))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            options.Providers ??= [];
            options.Capabilities ??= new();
        });

        services.TryAddSingleton<ConfiguredStorageDescriptorProvider>();
    services.TryAddSingleton<IIntegratedS3RequestAuthenticator, AwsSignatureV4RequestAuthenticator>();
        services.TryAddSingleton<IStorageCapabilityProvider>(static serviceProvider => serviceProvider.GetRequiredService<ConfiguredStorageDescriptorProvider>());
        services.TryAddSingleton<IStorageServiceDescriptorProvider>(static serviceProvider => serviceProvider.GetRequiredService<ConfiguredStorageDescriptorProvider>());

        services.ConfigureHttpJsonOptions(options => {
            if (!options.SerializerOptions.TypeInfoResolverChain.Contains(IntegratedS3AspNetCoreJsonSerializerContext.Default)) {
                options.SerializerOptions.TypeInfoResolverChain.Insert(0, IntegratedS3AspNetCoreJsonSerializerContext.Default);
            }
        });

        return services;
    }

    private static string NormalizeRoutePrefix(string? routePrefix)
    {
        if (string.IsNullOrWhiteSpace(routePrefix)) {
            return "/integrated-s3";
        }

        var trimmed = routePrefix.Trim();
        if (!trimmed.StartsWith('/')) {
            trimmed = $"/{trimmed}";
        }

        return trimmed.Length > 1
            ? trimmed.TrimEnd('/')
            : trimmed;
    }

    private static string NormalizeHostSuffix(string? hostSuffix)
    {
        if (string.IsNullOrWhiteSpace(hostSuffix)) {
            return string.Empty;
        }

        return hostSuffix.Trim().TrimStart('.').TrimEnd('.').ToLowerInvariant();
    }
}
