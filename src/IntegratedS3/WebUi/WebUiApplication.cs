using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

public static class WebUiApplication
{
    public static void ConfigureServices(WebApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var diskOptions = builder.Configuration.GetSection("IntegratedS3:Disk").Get<DiskStorageOptions>() ?? new DiskStorageOptions();
        diskOptions.RootPath = Path.IsPathRooted(diskOptions.RootPath)
            ? Path.GetFullPath(diskOptions.RootPath)
            : Path.GetFullPath(Path.Combine(builder.Environment.ContentRootPath, diskOptions.RootPath));

        builder.Services.AddOpenApi();
        builder.Services.AddIntegratedS3Core();
        builder.Services.AddIntegratedS3(options => {
            options.ServiceName = builder.Configuration["IntegratedS3:ServiceName"] ?? "Integrated S3 Sample Host";
            options.RoutePrefix = builder.Configuration["IntegratedS3:RoutePrefix"] ?? "/integrated-s3";
        });
        builder.Services.AddDiskStorage(diskOptions);
    }

    public static void ConfigurePipeline(WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        if (app.Environment.IsDevelopment()) {
            app.MapOpenApi();
        }

        if (app.Services.GetService<IAuthenticationSchemeProvider>() is not null) {
            app.UseAuthentication();
        }

        app.MapGet("/", (IOptions<IntegratedS3Options> options) => TypedResults.Redirect(options.Value.RoutePrefix))
            .ExcludeFromDescription();

        app.MapIntegratedS3Endpoints();
    }
}
