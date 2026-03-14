using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;

public static class WebUiApplication
{
    /// <summary>
    /// Configures the reference host services for the IntegratedS3 sample application.
    /// </summary>
    /// <remarks>
    /// The sample intentionally uses the public ASP.NET integration entrypoint plus the disk provider so the
    /// host stays a lightweight onboarding/reference application instead of becoming the final architecture container.
    /// </remarks>
    /// <param name="builder">The application builder.</param>
    public static void ConfigureServices(WebApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var diskOptions = ResolveDiskStorageOptions(builder);

        builder.Services.AddOpenApi();
        builder.Services.AddIntegratedS3(builder.Configuration, options => {
            options.ServiceName = string.IsNullOrWhiteSpace(options.ServiceName)
                ? "Integrated S3 Sample Host"
                : options.ServiceName;
            options.RoutePrefix = string.IsNullOrWhiteSpace(options.RoutePrefix)
                ? "/integrated-s3"
                : options.RoutePrefix;
        });
        builder.Services.AddDiskStorage(diskOptions);
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

        if (configureIntegratedS3Endpoints is null) {
            app.MapIntegratedS3Endpoints();
        }
        else {
            app.MapIntegratedS3Endpoints(configureIntegratedS3Endpoints);
        }
    }

    private static DiskStorageOptions ResolveDiskStorageOptions(WebApplicationBuilder builder)
    {
        var diskOptions = builder.Configuration.GetSection("IntegratedS3:Disk").Get<DiskStorageOptions>() ?? new DiskStorageOptions();
        diskOptions.RootPath = Path.IsPathRooted(diskOptions.RootPath)
            ? Path.GetFullPath(diskOptions.RootPath)
            : Path.GetFullPath(Path.Combine(builder.Environment.ContentRootPath, diskOptions.RootPath));
        return diskOptions;
    }
}
