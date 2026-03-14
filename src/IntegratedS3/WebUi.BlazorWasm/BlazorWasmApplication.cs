using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;

namespace WebUi.BlazorWasm;

public static class BlazorWasmApplication
{
    public static void ConfigureServices(WebApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var diskOptions = ResolveDiskStorageOptions(builder);

        builder.Services.AddOpenApi();
        builder.Services.AddIntegratedS3(builder.Configuration, options => {
            options.ServiceName = string.IsNullOrWhiteSpace(options.ServiceName)
                ? "Integrated S3 Blazor WebAssembly Sample"
                : options.ServiceName;
            options.RoutePrefix = string.IsNullOrWhiteSpace(options.RoutePrefix)
                ? "/integrated-s3"
                : options.RoutePrefix;
        });
        builder.Services.AddDiskStorage(diskOptions);
        builder.Services.AddHostedService<BlazorWasmSampleDataSeeder>();
    }

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

        app.UseBlazorFrameworkFiles();
        app.UseStaticFiles();

        if (configureIntegratedS3Endpoints is null) {
            app.MapIntegratedS3Endpoints();
        }
        else {
            app.MapIntegratedS3Endpoints(configureIntegratedS3Endpoints);
        }

        app.MapFallbackToFile("index.html");
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
