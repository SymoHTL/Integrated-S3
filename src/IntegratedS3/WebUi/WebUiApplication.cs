using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using IntegratedS3.Provider.S3;
using IntegratedS3.Provider.S3.DependencyInjection;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

public static class WebUiApplication
{
    private const string IntegratedS3SectionName = "IntegratedS3";
    private const string DiskSectionPath = $"{IntegratedS3SectionName}:Disk";
    private const string S3SectionPath = $"{IntegratedS3SectionName}:S3";
    private const string ReferenceHostSectionPath = $"{IntegratedS3SectionName}:ReferenceHost";

    /// <summary>
     /// Configures the reference host services for the IntegratedS3 sample application.
     /// </summary>
     /// <remarks>
     /// The sample intentionally uses the public ASP.NET integration entrypoint plus configurable disk/S3 backends
     /// so the host stays a lightweight onboarding/reference application instead of becoming the final architecture container.
     /// </remarks>
     /// <param name="builder">The application builder.</param>
    public static void ConfigureServices(WebApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var referenceHostOptions = ResolveReferenceHostOptions(builder.Configuration);

        builder.Services.AddOptions<WebUiReferenceHostOptions>()
            .Bind(builder.Configuration.GetSection(ReferenceHostSectionPath));
        builder.Services.PostConfigure<WebUiReferenceHostOptions>(NormalizeReferenceHostOptions);
        builder.Services.AddOpenApi();
        builder.Services.AddIntegratedS3(builder.Configuration, options => {
            options.ServiceName = string.IsNullOrWhiteSpace(options.ServiceName)
                ? "Integrated S3 Sample Host"
                : options.ServiceName;
            options.RoutePrefix = string.IsNullOrWhiteSpace(options.RoutePrefix)
                ? "/integrated-s3"
                : options.RoutePrefix;
        });

        if (HasConfiguredRoutePolicies(referenceHostOptions.RoutePolicies)) {
            builder.Services.AddAuthorization();
        }

        switch (referenceHostOptions.StorageProvider)
        {
            case WebUiStorageProvider.Disk:
                builder.Services.AddDiskStorage(ResolveDiskStorageOptions(builder));
                break;
            case WebUiStorageProvider.S3:
                builder.Services.AddS3Storage(ResolveS3StorageOptions(builder.Configuration));
                break;
            default:
                throw new InvalidOperationException(
                    $"Unsupported {ReferenceHostSectionPath}:StorageProvider value '{referenceHostOptions.StorageProvider}'.");
        }
        builder.Services.AddHealthChecks()
            .AddIntegratedS3BackendHealthCheck();
    }

    /// <summary>
    /// Configures the HTTP pipeline for the IntegratedS3 reference host.
    /// </summary>
    /// <param name="app">The application instance.</param>
    /// <param name="configureIntegratedS3Endpoints">
    /// Optional endpoint customization used by the sample host and isolated test hosts.
    /// </param>
    public static void ConfigurePipeline(WebApplication app, Action<IntegratedS3EndpointOptions>? configureIntegratedS3Endpoints = null)
    {
        ArgumentNullException.ThrowIfNull(app);

        if (app.Environment.IsDevelopment()) {
            app.MapOpenApi();
        }

        if (app.Services.GetService<IAuthenticationSchemeProvider>() is not null) {
            app.UseAuthentication();
        }

        if (app.Services.GetService<IAuthorizationHandlerProvider>() is not null) {
            app.UseAuthorization();
        }

        app.MapIntegratedS3HealthEndpoints();

        app.MapGet("/", (IOptions<IntegratedS3Options> options) => TypedResults.Redirect(options.Value.RoutePrefix))
            .ExcludeFromDescription();

        var referenceHostOptions = app.Services.GetRequiredService<IOptions<WebUiReferenceHostOptions>>().Value;
        app.MapIntegratedS3Endpoints(options => {
            ApplyConfiguredRoutePolicies(options, referenceHostOptions.RoutePolicies);
            configureIntegratedS3Endpoints?.Invoke(options);
        });
    }

    private static DiskStorageOptions ResolveDiskStorageOptions(WebApplicationBuilder builder)
    {
        var diskOptions = builder.Configuration.GetSection(DiskSectionPath).Get<DiskStorageOptions>() ?? new DiskStorageOptions();
        diskOptions.RootPath = Path.IsPathRooted(diskOptions.RootPath)
            ? Path.GetFullPath(diskOptions.RootPath)
            : Path.GetFullPath(Path.Combine(builder.Environment.ContentRootPath, diskOptions.RootPath));
        return diskOptions;
    }

    private static S3StorageOptions ResolveS3StorageOptions(IConfiguration configuration)
    {
        return configuration.GetSection(S3SectionPath).Get<S3StorageOptions>() ?? new S3StorageOptions();
    }

    private static WebUiReferenceHostOptions ResolveReferenceHostOptions(IConfiguration configuration)
    {
        var options = configuration.GetSection(ReferenceHostSectionPath).Get<WebUiReferenceHostOptions>() ?? new WebUiReferenceHostOptions();
        options.StorageProvider = ResolveStorageProvider(configuration[$"{ReferenceHostSectionPath}:StorageProvider"]);
        NormalizeReferenceHostOptions(options);
        return options;
    }

    private static WebUiStorageProvider ResolveStorageProvider(string? configuredValue)
    {
        if (string.IsNullOrWhiteSpace(configuredValue)) {
            return WebUiStorageProvider.Disk;
        }

        return Enum.TryParse<WebUiStorageProvider>(configuredValue.Trim(), ignoreCase: true, out var storageProvider)
            ? storageProvider
            : throw new InvalidOperationException(
                $"Unsupported {ReferenceHostSectionPath}:StorageProvider value '{configuredValue}'. Supported values are 'Disk' and 'S3'.");
    }

    private static void ApplyConfiguredRoutePolicies(IntegratedS3EndpointOptions endpointOptions, WebUiReferenceHostRoutePolicyOptions routePolicies)
    {
        ArgumentNullException.ThrowIfNull(endpointOptions);
        ArgumentNullException.ThrowIfNull(routePolicies);

        if (routePolicies.Route is { Length: > 0 } routePolicy) {
            endpointOptions.ConfigureRouteGroup = group => group.RequireAuthorization(routePolicy);
        }

        if (routePolicies.Root is { Length: > 0 } rootPolicy) {
            endpointOptions.ConfigureRootRouteGroup = group => group.RequireAuthorization(rootPolicy);
        }

        if (routePolicies.Compatibility is { Length: > 0 } compatibilityPolicy) {
            endpointOptions.ConfigureCompatibilityRouteGroup = group => group.RequireAuthorization(compatibilityPolicy);
        }

        if (routePolicies.Service is { Length: > 0 } servicePolicy) {
            endpointOptions.ConfigureServiceRouteGroup = group => group.RequireAuthorization(servicePolicy);
        }

        if (routePolicies.Bucket is { Length: > 0 } bucketPolicy) {
            endpointOptions.ConfigureBucketRouteGroup = group => group.RequireAuthorization(bucketPolicy);
        }

        if (routePolicies.Object is { Length: > 0 } objectPolicy) {
            endpointOptions.ConfigureObjectRouteGroup = group => group.RequireAuthorization(objectPolicy);
        }

        if (routePolicies.Multipart is { Length: > 0 } multipartPolicy) {
            endpointOptions.ConfigureMultipartRouteGroup = group => group.RequireAuthorization(multipartPolicy);
        }

        if (routePolicies.Admin is { Length: > 0 } adminPolicy) {
            endpointOptions.ConfigureAdminRouteGroup = group => group.RequireAuthorization(adminPolicy);
        }
    }

    private static bool HasConfiguredRoutePolicies(WebUiReferenceHostRoutePolicyOptions routePolicies)
    {
        ArgumentNullException.ThrowIfNull(routePolicies);

        return routePolicies.Route is { Length: > 0 }
            || routePolicies.Root is { Length: > 0 }
            || routePolicies.Compatibility is { Length: > 0 }
            || routePolicies.Service is { Length: > 0 }
            || routePolicies.Bucket is { Length: > 0 }
            || routePolicies.Object is { Length: > 0 }
            || routePolicies.Multipart is { Length: > 0 }
            || routePolicies.Admin is { Length: > 0 };
    }

    private static void NormalizeRoutePolicies(WebUiReferenceHostRoutePolicyOptions routePolicies)
    {
        routePolicies.Route = NormalizePolicyName(routePolicies.Route);
        routePolicies.Root = NormalizePolicyName(routePolicies.Root);
        routePolicies.Compatibility = NormalizePolicyName(routePolicies.Compatibility);
        routePolicies.Service = NormalizePolicyName(routePolicies.Service);
        routePolicies.Bucket = NormalizePolicyName(routePolicies.Bucket);
        routePolicies.Object = NormalizePolicyName(routePolicies.Object);
        routePolicies.Multipart = NormalizePolicyName(routePolicies.Multipart);
        routePolicies.Admin = NormalizePolicyName(routePolicies.Admin);
    }

    private static void NormalizeReferenceHostOptions(WebUiReferenceHostOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.RoutePolicies ??= new();
        NormalizeRoutePolicies(options.RoutePolicies);
    }

    private static string? NormalizePolicyName(string? policyName)
    {
        return string.IsNullOrWhiteSpace(policyName)
            ? null
            : policyName.Trim();
    }
}
