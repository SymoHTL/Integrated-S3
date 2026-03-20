using System.Diagnostics.CodeAnalysis;
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
        builder.Services.PostConfigure<IntegratedS3EndpointConfigurationOptions>(
            options => ApplyConfiguredRoutePolicies(options, referenceHostOptions.RoutePolicies));

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
     public static void ConfigurePipeline(WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        ConfigureSharedPipeline(app);

        var endpointConfiguration = app.Services.GetRequiredService<IOptions<IntegratedS3EndpointConfigurationOptions>>().Value;
        app.MapIntegratedS3Endpoints(endpointConfiguration);
    }

    /// <summary>
     /// Configures the HTTP pipeline for the IntegratedS3 reference host with additional endpoint customization.
     /// </summary>
     /// <param name="app">The application instance.</param>
     /// <param name="configureIntegratedS3Endpoints">
     /// Additional endpoint customization used by isolated test hosts or callback-driven consumers.
     /// </param>
    [RequiresUnreferencedCode("Endpoint customization callbacks may register additional Minimal API conventions or handlers that are not trimming-safe.")]
    [RequiresDynamicCode("Endpoint customization callbacks may register additional Minimal API conventions or handlers that are not AOT-safe.")]
    public static void ConfigurePipeline(WebApplication app, Action<IntegratedS3EndpointOptions> configureIntegratedS3Endpoints)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(configureIntegratedS3Endpoints);

        ConfigureSharedPipeline(app);

        app.MapIntegratedS3Endpoints(configureIntegratedS3Endpoints);
    }

    private static void ConfigureSharedPipeline(WebApplication app)
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

    private static void ApplyConfiguredRoutePolicies(
        IntegratedS3EndpointConfigurationOptions endpointOptions,
        WebUiReferenceHostRoutePolicyOptions routePolicies)
    {
        ArgumentNullException.ThrowIfNull(endpointOptions);
        ArgumentNullException.ThrowIfNull(routePolicies);

        endpointOptions.RouteAuthorization = AppendAuthorizationPolicy(endpointOptions.RouteAuthorization, routePolicies.Route);
        endpointOptions.RootRouteAuthorization = AppendAuthorizationPolicy(endpointOptions.RootRouteAuthorization, routePolicies.Root);
        endpointOptions.CompatibilityRouteAuthorization = AppendAuthorizationPolicy(endpointOptions.CompatibilityRouteAuthorization, routePolicies.Compatibility);
        endpointOptions.ServiceRouteAuthorization = AppendAuthorizationPolicy(endpointOptions.ServiceRouteAuthorization, routePolicies.Service);
        endpointOptions.BucketRouteAuthorization = AppendAuthorizationPolicy(endpointOptions.BucketRouteAuthorization, routePolicies.Bucket);
        endpointOptions.ObjectRouteAuthorization = AppendAuthorizationPolicy(endpointOptions.ObjectRouteAuthorization, routePolicies.Object);
        endpointOptions.MultipartRouteAuthorization = AppendAuthorizationPolicy(endpointOptions.MultipartRouteAuthorization, routePolicies.Multipart);
        endpointOptions.AdminRouteAuthorization = AppendAuthorizationPolicy(endpointOptions.AdminRouteAuthorization, routePolicies.Admin);
    }

    private static IntegratedS3EndpointAuthorizationOptions? AppendAuthorizationPolicy(
        IntegratedS3EndpointAuthorizationOptions? authorizationOptions,
        string? policyName)
    {
        if (string.IsNullOrWhiteSpace(policyName) || authorizationOptions?.AllowAnonymous == true) {
            return authorizationOptions;
        }

        authorizationOptions ??= new();
        authorizationOptions.PolicyNames = (authorizationOptions.PolicyNames ?? [])
            .Append(policyName)
            .Where(static configuredPolicyName => !string.IsNullOrWhiteSpace(configuredPolicyName))
            .Select(static configuredPolicyName => configuredPolicyName.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        return authorizationOptions;
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
