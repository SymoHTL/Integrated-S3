using IntegratedS3.AspNetCore.HostedServices;
using IntegratedS3.AspNetCore.Maintenance;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace IntegratedS3.AspNetCore.DependencyInjection;

/// <summary>
/// Extension methods for registering IntegratedS3 scheduled maintenance jobs
/// with an <see cref="IServiceCollection"/>.
/// </summary>
public static class IntegratedS3MaintenanceServiceCollectionExtensions
{
    /// <summary>
    /// Registers a maintenance job implemented by <typeparamref name="TJob"/>.
    /// The job class is resolved from DI as a scoped service.
    /// </summary>
    /// <typeparam name="TJob">
    /// A class that implements <see cref="IIntegratedS3MaintenanceJob"/>.
    /// </typeparam>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the job to.</param>
    /// <param name="name">Unique name for this job instance, used in logging and options resolution.</param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
    public static IServiceCollection AddIntegratedS3MaintenanceJob<TJob>(this IServiceCollection services, string name)
        where TJob : class, IIntegratedS3MaintenanceJob
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<TJob>();
        return services.AddIntegratedS3MaintenanceJob(
            name,
            static (serviceProvider, cancellationToken) => serviceProvider.GetRequiredService<TJob>().ExecuteAsync(cancellationToken));
    }

    /// <summary>
    /// Registers a maintenance job implemented by <typeparamref name="TJob"/>
    /// with programmatic option configuration.
    /// The job class is resolved from DI as a scoped service.
    /// </summary>
    /// <typeparam name="TJob">
    /// A class that implements <see cref="IIntegratedS3MaintenanceJob"/>.
    /// </typeparam>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the job to.</param>
    /// <param name="name">Unique name for this job instance, used in logging and options resolution.</param>
    /// <param name="configure">
    /// A callback to configure <see cref="IntegratedS3MaintenanceJobOptions"/> programmatically.
    /// </param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
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

    /// <summary>
    /// Registers a maintenance job implemented by <typeparamref name="TJob"/>
    /// with options bound from a configuration section.
    /// The job class is resolved from DI as a scoped service.
    /// </summary>
    /// <typeparam name="TJob">
    /// A class that implements <see cref="IIntegratedS3MaintenanceJob"/>.
    /// </typeparam>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the job to.</param>
    /// <param name="name">Unique name for this job instance, used in logging and options resolution.</param>
    /// <param name="section">
    /// An <see cref="IConfigurationSection"/> to bind <see cref="IntegratedS3MaintenanceJobOptions"/> from.
    /// </param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
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

    /// <summary>
    /// Registers a maintenance job implemented by <typeparamref name="TJob"/>
    /// with options bound from a configuration section and additional programmatic configuration.
    /// The job class is resolved from DI as a scoped service.
    /// </summary>
    /// <typeparam name="TJob">
    /// A class that implements <see cref="IIntegratedS3MaintenanceJob"/>.
    /// </typeparam>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the job to.</param>
    /// <param name="name">Unique name for this job instance, used in logging and options resolution.</param>
    /// <param name="section">
    /// An <see cref="IConfigurationSection"/> to bind <see cref="IntegratedS3MaintenanceJobOptions"/> from.
    /// </param>
    /// <param name="configure">
    /// A callback to further configure <see cref="IntegratedS3MaintenanceJobOptions"/> after binding.
    /// </param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
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

    /// <summary>
    /// Registers a maintenance job using an <see cref="IntegratedS3MaintenanceJobDelegate"/>.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the job to.</param>
    /// <param name="name">Unique name for this job instance, used in logging and options resolution.</param>
    /// <param name="execute">The delegate that performs the maintenance work.</param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
    public static IServiceCollection AddIntegratedS3MaintenanceJob(
        this IServiceCollection services,
        string name,
        IntegratedS3MaintenanceJobDelegate execute)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(execute);

        return AddIntegratedS3MaintenanceJobCore(services, name, section: null, execute, configure: null);
    }

    /// <summary>
    /// Registers a maintenance job using an <see cref="IntegratedS3MaintenanceJobDelegate"/>
    /// with programmatic option configuration.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the job to.</param>
    /// <param name="name">Unique name for this job instance, used in logging and options resolution.</param>
    /// <param name="execute">The delegate that performs the maintenance work.</param>
    /// <param name="configure">
    /// A callback to configure <see cref="IntegratedS3MaintenanceJobOptions"/> programmatically.
    /// </param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
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

    /// <summary>
    /// Registers a maintenance job using an <see cref="IntegratedS3MaintenanceJobDelegate"/>
    /// with options bound from a configuration section.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the job to.</param>
    /// <param name="name">Unique name for this job instance, used in logging and options resolution.</param>
    /// <param name="section">
    /// An <see cref="IConfigurationSection"/> to bind <see cref="IntegratedS3MaintenanceJobOptions"/> from.
    /// </param>
    /// <param name="execute">The delegate that performs the maintenance work.</param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
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

    /// <summary>
    /// Registers a maintenance job using an <see cref="IntegratedS3MaintenanceJobDelegate"/>
    /// with options bound from a configuration section and additional programmatic configuration.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the job to.</param>
    /// <param name="name">Unique name for this job instance, used in logging and options resolution.</param>
    /// <param name="section">
    /// An <see cref="IConfigurationSection"/> to bind <see cref="IntegratedS3MaintenanceJobOptions"/> from.
    /// </param>
    /// <param name="execute">The delegate that performs the maintenance work.</param>
    /// <param name="configure">
    /// A callback to further configure <see cref="IntegratedS3MaintenanceJobOptions"/> after binding.
    /// </param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
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
