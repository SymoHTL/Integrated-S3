using IntegratedS3.Abstractions.Services;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.AspNetCore.Services;
using IntegratedS3.AspNetCore.Serialization;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Core.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace IntegratedS3.AspNetCore.DependencyInjection;

public static class IntegratedS3ServiceCollectionExtensions
{
    public static IServiceCollection AddIntegratedS3(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddIntegratedS3(static _ => { });
    }

    public static IServiceCollection AddIntegratedS3(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        return services.AddIntegratedS3(configuration.GetSection("IntegratedS3"));
    }

    public static IServiceCollection AddIntegratedS3(this IServiceCollection services, IConfiguration configuration, Action<IntegratedS3Options> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(configure);

        return services.AddIntegratedS3(configuration.GetSection("IntegratedS3"), configure);
    }

    public static IServiceCollection AddIntegratedS3(this IServiceCollection services, IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);

        services.AddOptions<IntegratedS3Options>()
            .Bind(section);
        services.AddOptions<IntegratedS3EndpointOptions>()
            .Bind(section.GetSection("Endpoints"));

        return services.AddIntegratedS3CoreServices();
    }

    public static IServiceCollection AddIntegratedS3(this IServiceCollection services, IConfigurationSection section, Action<IntegratedS3Options> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<IntegratedS3Options>()
            .Bind(section)
            .Configure(configure);
        services.AddOptions<IntegratedS3EndpointOptions>()
            .Bind(section.GetSection("Endpoints"));

        return services.AddIntegratedS3CoreServices();
    }

    public static IServiceCollection AddIntegratedS3(this IServiceCollection services, Action<IntegratedS3Options> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<IntegratedS3Options>()
            .Configure(configure);

        return services.AddIntegratedS3CoreServices();
    }

    public static IServiceCollection AddIntegratedS3Backend<TBackend>(this IServiceCollection services)
        where TBackend : class, IStorageBackend
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IStorageBackend, TBackend>();

        return services.AddIntegratedS3CoreServices();
    }

    public static IServiceCollection AddIntegratedS3Backend<TBackend>(
        this IServiceCollection services,
        Func<IServiceProvider, TBackend> implementationFactory)
        where TBackend : class, IStorageBackend
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(implementationFactory);

        services.AddSingleton<IStorageBackend>(serviceProvider => implementationFactory(serviceProvider));

        return services.AddIntegratedS3CoreServices();
    }

    public static IServiceCollection AddIntegratedS3Provider(this IServiceCollection services, string name, string kind, bool isPrimary = false, string? description = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (string.IsNullOrWhiteSpace(name)) {
            throw new ArgumentException("Provider name is required.", nameof(name));
        }

        if (string.IsNullOrWhiteSpace(kind)) {
            throw new ArgumentException("Provider kind is required.", nameof(kind));
        }

        return services.AddIntegratedS3Provider(new StorageProviderDescriptor
        {
            Name = name,
            Kind = kind,
            IsPrimary = isPrimary,
            Description = description
        });
    }

    public static IServiceCollection AddIntegratedS3Provider(this IServiceCollection services, Action<StorageProviderDescriptor> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var provider = new StorageProviderDescriptor();
        configure(provider);

        return services.AddIntegratedS3Provider(provider);
    }

    public static IServiceCollection AddIntegratedS3Provider(this IServiceCollection services, StorageProviderDescriptor provider)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(provider);

        var normalizedProvider = NormalizeProvider(provider);
        services.PostConfigure<IntegratedS3Options>(options => {
            options.Providers ??= [];
            options.Providers.Add(CloneProvider(normalizedProvider));
        });

        return services.AddIntegratedS3CoreServices();
    }

    private static IServiceCollection AddIntegratedS3CoreServices(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<IntegratedS3EndpointOptions>();

        if (!services.Any(static serviceDescriptor => serviceDescriptor.ServiceType == typeof(IStorageService))
            || !services.Any(static serviceDescriptor => serviceDescriptor.ServiceType == typeof(IStoragePresignService))) {
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
            options.PresignAccessKeyId = string.IsNullOrWhiteSpace(options.PresignAccessKeyId)
                ? null
                : options.PresignAccessKeyId.Trim();
            options.PresignPublicBaseUrl = string.IsNullOrWhiteSpace(options.PresignPublicBaseUrl)
                ? null
                : options.PresignPublicBaseUrl.Trim();
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
        services.TryAddSingleton<BucketCorsRuntimeService>();
        services.TryAddSingleton<IHttpContextAccessor, HttpContextAccessor>();
        services.TryAddSingleton<IIntegratedS3RequestAuthenticator, AwsSignatureV4RequestAuthenticator>();
        services.TryAddSingleton<IIntegratedS3PresignCredentialResolver, ConfiguredIntegratedS3PresignCredentialResolver>();
        services.TryAddSingleton<IStorageCapabilityProvider>(static serviceProvider => serviceProvider.GetRequiredService<ConfiguredStorageDescriptorProvider>());
        services.TryAddSingleton<IStorageServiceDescriptorProvider>(static serviceProvider => serviceProvider.GetRequiredService<ConfiguredStorageDescriptorProvider>());
        if (!HasCustomPresignStrategy(services)) {
            services.Replace(ServiceDescriptor.Singleton<IStoragePresignStrategy, IntegratedS3HttpPresignStrategy>());
        }

        services.ConfigureHttpJsonOptions(options => {
            if (!options.SerializerOptions.TypeInfoResolverChain.Contains(IntegratedS3AspNetCoreJsonSerializerContext.Default)) {
                options.SerializerOptions.TypeInfoResolverChain.Insert(0, IntegratedS3AspNetCoreJsonSerializerContext.Default);
            }
        });

        return services;
    }

    private static bool HasCustomPresignStrategy(IServiceCollection services)
    {
        return services.Any(static serviceDescriptor =>
            serviceDescriptor.ServiceType == typeof(IStoragePresignStrategy)
            && (serviceDescriptor.ImplementationFactory is not null
                || serviceDescriptor.ImplementationInstance is not null
                || !string.Equals(
                    serviceDescriptor.ImplementationType?.FullName,
                    "IntegratedS3.Core.Services.UnsupportedStoragePresignStrategy",
                    StringComparison.Ordinal)));
    }

    private static StorageProviderDescriptor NormalizeProvider(StorageProviderDescriptor provider)
    {
        var normalizedName = string.IsNullOrWhiteSpace(provider.Name)
            ? throw new ArgumentException("Provider name is required.", nameof(provider))
            : provider.Name.Trim();
        var normalizedKind = string.IsNullOrWhiteSpace(provider.Kind)
            ? throw new ArgumentException("Provider kind is required.", nameof(provider))
            : provider.Kind.Trim();

        return new StorageProviderDescriptor
        {
            Name = normalizedName,
            Kind = normalizedKind,
            IsPrimary = provider.IsPrimary,
            Description = string.IsNullOrWhiteSpace(provider.Description) ? null : provider.Description.Trim(),
            Mode = provider.Mode,
            Capabilities = CloneCapabilities(provider.Capabilities),
            ObjectLocation = CloneObjectLocation(provider.ObjectLocation),
            SupportState = CloneSupportState(provider.SupportState)
        };
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

    private static StorageProviderDescriptor CloneProvider(StorageProviderDescriptor provider)
    {
        return new StorageProviderDescriptor
        {
            Name = provider.Name,
            Kind = provider.Kind,
            IsPrimary = provider.IsPrimary,
            Description = provider.Description,
            Mode = provider.Mode,
            Capabilities = CloneCapabilities(provider.Capabilities),
            ObjectLocation = CloneObjectLocation(provider.ObjectLocation),
            SupportState = CloneSupportState(provider.SupportState)
        };
    }

    private static StorageObjectLocationDescriptor CloneObjectLocation(StorageObjectLocationDescriptor objectLocation)
    {
        ArgumentNullException.ThrowIfNull(objectLocation);

        List<StorageObjectAccessMode> supportedAccessModes = objectLocation.SupportedAccessModes.Count == 0
            ? [objectLocation.DefaultAccessMode]
            : [.. objectLocation.SupportedAccessModes];
        if (!supportedAccessModes.Contains(objectLocation.DefaultAccessMode)) {
            supportedAccessModes.Insert(0, objectLocation.DefaultAccessMode);
        }

        return new StorageObjectLocationDescriptor
        {
            DefaultAccessMode = objectLocation.DefaultAccessMode,
            SupportedAccessModes = supportedAccessModes
                .Distinct()
                .ToList()
        };
    }

    private static Abstractions.Capabilities.StorageSupportStateDescriptor CloneSupportState(Abstractions.Capabilities.StorageSupportStateDescriptor supportState)
    {
        return new Abstractions.Capabilities.StorageSupportStateDescriptor
        {
            ObjectMetadata = supportState.ObjectMetadata,
            ObjectTags = supportState.ObjectTags,
            MultipartState = supportState.MultipartState,
            Versioning = supportState.Versioning,
            Checksums = supportState.Checksums,
            AccessControl = supportState.AccessControl,
            Retention = supportState.Retention,
            ServerSideEncryption = supportState.ServerSideEncryption,
            RedirectLocations = supportState.RedirectLocations
        };
    }

    private static Abstractions.Capabilities.StorageCapabilities CloneCapabilities(Abstractions.Capabilities.StorageCapabilities capabilities)
    {
        return new Abstractions.Capabilities.StorageCapabilities
        {
            BucketOperations = capabilities.BucketOperations,
            ObjectCrud = capabilities.ObjectCrud,
            ObjectMetadata = capabilities.ObjectMetadata,
            ListObjects = capabilities.ListObjects,
            Pagination = capabilities.Pagination,
            RangeRequests = capabilities.RangeRequests,
            ConditionalRequests = capabilities.ConditionalRequests,
            MultipartUploads = capabilities.MultipartUploads,
            CopyOperations = capabilities.CopyOperations,
            PresignedUrls = capabilities.PresignedUrls,
            ObjectTags = capabilities.ObjectTags,
            Versioning = capabilities.Versioning,
            BatchDelete = capabilities.BatchDelete,
            AccessControl = capabilities.AccessControl,
            Cors = capabilities.Cors,
            ObjectLock = capabilities.ObjectLock,
            ServerSideEncryption = capabilities.ServerSideEncryption,
            ServerSideEncryptionDetails = CloneServerSideEncryptionDetails(capabilities.ServerSideEncryptionDetails),
            Checksums = capabilities.Checksums,
            XmlErrors = capabilities.XmlErrors,
            PathStyleAddressing = capabilities.PathStyleAddressing,
            VirtualHostedStyleAddressing = capabilities.VirtualHostedStyleAddressing
        };
    }

    private static Abstractions.Capabilities.StorageServerSideEncryptionDescriptor CloneServerSideEncryptionDetails(Abstractions.Capabilities.StorageServerSideEncryptionDescriptor serverSideEncryptionDetails)
    {
        ArgumentNullException.ThrowIfNull(serverSideEncryptionDetails);

        return new Abstractions.Capabilities.StorageServerSideEncryptionDescriptor
        {
            Variants = serverSideEncryptionDetails.Variants.Count == 0
                ? []
                : serverSideEncryptionDetails.Variants
                    .Select(static variant => new Abstractions.Capabilities.StorageServerSideEncryptionVariantDescriptor
                    {
                        Algorithm = variant.Algorithm,
                        RequestStyle = variant.RequestStyle,
                        SupportedRequestOperations = variant.SupportedRequestOperations.Count == 0
                            ? []
                            : [.. variant.SupportedRequestOperations],
                        SupportsResponseMetadata = variant.SupportsResponseMetadata,
                        SupportsKeyId = variant.SupportsKeyId,
                        SupportsContext = variant.SupportsContext
                    })
                    .ToArray()
        };
    }
}
