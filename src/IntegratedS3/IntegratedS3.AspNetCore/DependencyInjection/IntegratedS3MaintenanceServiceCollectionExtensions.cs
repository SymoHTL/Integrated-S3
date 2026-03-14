using IntegratedS3.AspNetCore.HostedServices;
using IntegratedS3.AspNetCore.Maintenance;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace IntegratedS3.AspNetCore.DependencyInjection;

public static class IntegratedS3MaintenanceServiceCollectionExtensions
{
    public static IServiceCollection AddIntegratedS3MaintenanceJob<TJob>(this IServiceCollection services, string name)
        where TJob : class, IIntegratedS3MaintenanceJob
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<TJob>();
        return services.AddIntegratedS3MaintenanceJob(
            name,
            static (serviceProvider, cancellationToken) => serviceProvider.GetRequiredService<TJob>().ExecuteAsync(cancellationToken));
    }

    public static IServiceCollection AddIntegratedS3MaintenanceJob<TJob>(
        this IServiceCollection services,
        string name,
        Action<IntegratedS3MaintenanceJobOptions> configure)
        where TJob : class, IIntegratedS3MaintenanceJob
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.TryAddScoped<TJob>();
        return services.AddIntegratedS3MaintenanceJob(
            name,
            static (serviceProvider, cancellationToken) => serviceProvider.GetRequiredService<TJob>().ExecuteAsync(cancellationToken),
            configure);
    }

    public static IServiceCollection AddIntegratedS3MaintenanceJob<TJob>(
        this IServiceCollection services,
        string name,
        IConfigurationSection section)
        where TJob : class, IIntegratedS3MaintenanceJob
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);

        services.TryAddScoped<TJob>();
        return services.AddIntegratedS3MaintenanceJob(
            name,
            section,
            static (serviceProvider, cancellationToken) => serviceProvider.GetRequiredService<TJob>().ExecuteAsync(cancellationToken));
    }

    public static IServiceCollection AddIntegratedS3MaintenanceJob<TJob>(
        this IServiceCollection services,
        string name,
        IConfigurationSection section,
        Action<IntegratedS3MaintenanceJobOptions> configure)
        where TJob : class, IIntegratedS3MaintenanceJob
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        ArgumentNullException.ThrowIfNull(configure);

        services.TryAddScoped<TJob>();
        return services.AddIntegratedS3MaintenanceJob(
            name,
            section,
            static (serviceProvider, cancellationToken) => serviceProvider.GetRequiredService<TJob>().ExecuteAsync(cancellationToken),
            configure);
    }

    public static IServiceCollection AddIntegratedS3MaintenanceJob(
        this IServiceCollection services,
        string name,
        IntegratedS3MaintenanceJobDelegate execute)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(execute);

        return AddIntegratedS3MaintenanceJobCore(services, name, section: null, execute, configure: null);
    }

    public static IServiceCollection AddIntegratedS3MaintenanceJob(
        this IServiceCollection services,
        string name,
        IntegratedS3MaintenanceJobDelegate execute,
        Action<IntegratedS3MaintenanceJobOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(execute);
        ArgumentNullException.ThrowIfNull(configure);

        return AddIntegratedS3MaintenanceJobCore(services, name, section: null, execute, configure);
    }

    public static IServiceCollection AddIntegratedS3MaintenanceJob(
        this IServiceCollection services,
        string name,
        IConfigurationSection section,
        IntegratedS3MaintenanceJobDelegate execute)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        ArgumentNullException.ThrowIfNull(execute);

        return AddIntegratedS3MaintenanceJobCore(services, name, section, execute, configure: null);
    }

    public static IServiceCollection AddIntegratedS3MaintenanceJob(
        this IServiceCollection services,
        string name,
        IConfigurationSection section,
        IntegratedS3MaintenanceJobDelegate execute,
        Action<IntegratedS3MaintenanceJobOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        ArgumentNullException.ThrowIfNull(execute);
        ArgumentNullException.ThrowIfNull(configure);

        return AddIntegratedS3MaintenanceJobCore(services, name, section, execute, configure);
    }

    private static IServiceCollection AddIntegratedS3MaintenanceJobCore(
        IServiceCollection services,
        string name,
        IConfigurationSection? section,
        IntegratedS3MaintenanceJobDelegate execute,
        Action<IntegratedS3MaintenanceJobOptions>? configure)
    {
        var normalizedName = NormalizeName(name);

        services.TryAddSingleton<TimeProvider>(TimeProvider.System);

        var optionsBuilder = services.AddOptions<IntegratedS3MaintenanceJobOptions>(normalizedName);
        if (section is not null) {
            optionsBuilder.Bind(section);
        }

        if (configure is not null) {
            optionsBuilder.Configure(configure);
        }

        optionsBuilder.Validate(
            static options => options.Interval > TimeSpan.Zero,
            "IntegratedS3 maintenance job intervals must be greater than zero.");

        services.AddSingleton<IHostedService>(serviceProvider =>
            ActivatorUtilities.CreateInstance<IntegratedS3ScheduledMaintenanceJobHostedService>(
                serviceProvider,
                normalizedName,
                execute));

        return services;
    }

    private static string NormalizeName(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) {
            throw new ArgumentException("Maintenance job name is required.", nameof(name));
        }

        return name.Trim();
    }
}
