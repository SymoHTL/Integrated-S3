using System.Security.Claims;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.AspNetCore.Endpoints;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Core.Options;
using IntegratedS3.Core.Services;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Benchmarks;

internal sealed class DiskServiceBenchmarkEnvironment : IAsyncDisposable
{
    private readonly ServiceProvider _serviceProvider;
    private readonly string _scenarioRootPath;

    private DiskServiceBenchmarkEnvironment(ServiceProvider serviceProvider, string scenarioRootPath, IReadOnlyList<string> observedRoots)
    {
        _serviceProvider = serviceProvider;
        _scenarioRootPath = scenarioRootPath;
        ObservedRoots = observedRoots;
        StorageService = serviceProvider.GetRequiredService<IStorageService>();
        PresignService = serviceProvider.GetRequiredService<IStoragePresignService>();
    }

    public IStorageService StorageService { get; }

    public IStoragePresignService PresignService { get; }

    public IReadOnlyList<string> ObservedRoots { get; }

    public ClaimsPrincipal PresignPrincipal { get; } = new(new ClaimsIdentity());

    public static Task<DiskServiceBenchmarkEnvironment> CreateSingleAsync(CancellationToken cancellationToken)
        => CreateAsync(isMirrored: false, cancellationToken);

    public static Task<DiskServiceBenchmarkEnvironment> CreateMirrorAsync(CancellationToken cancellationToken)
        => CreateAsync(isMirrored: true, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _serviceProvider.DisposeAsync();
        if (Directory.Exists(_scenarioRootPath)) {
            Directory.Delete(_scenarioRootPath, recursive: true);
        }
    }

    private static Task<DiskServiceBenchmarkEnvironment> CreateAsync(bool isMirrored, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var scenarioRootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Benchmarks", Guid.NewGuid().ToString("N"));
        var primaryRootPath = Path.Combine(scenarioRootPath, "primary");
        Directory.CreateDirectory(primaryRootPath);

        var services = new ServiceCollection();
        services.AddIntegratedS3Core(options => {
            options.ConsistencyMode = isMirrored
                ? StorageConsistencyMode.WriteThroughAll
                : StorageConsistencyMode.PrimaryOnly;
            options.ReadRoutingMode = StorageReadRoutingMode.PrimaryOnly;
            options.Replication.RequireHealthyReplicasForWriteThrough = !isMirrored;
            options.Replication.RequireCurrentReplicasForWriteThrough = !isMirrored;
        });
        services.AddIntegratedS3(options => {
            options.RoutePrefix = BenchmarkDefaults.RoutePrefix;
            options.EnableAwsSignatureV4Authentication = true;
            options.AllowedSignatureClockSkewMinutes = 120;
            options.PresignPublicBaseUrl = "https://benchmarks.integrateds3.local";
            options.PresignAccessKeyId = BenchmarkDefaults.AccessKeyId;
            options.AccessKeyCredentials =
            [
                new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = BenchmarkDefaults.AccessKeyId,
                    SecretAccessKey = BenchmarkDefaults.SecretAccessKey,
                    DisplayName = "IntegratedS3 Benchmarks"
                }
            ];
        });
        services.AddDiskStorage(new DiskStorageOptions
        {
            ProviderName = "disk-primary",
            IsPrimary = true,
            RootPath = primaryRootPath,
            CreateRootDirectory = true
        });

        var observedRoots = new List<string> { primaryRootPath };
        if (isMirrored) {
            var replicaRootPath = Path.Combine(scenarioRootPath, "replica");
            Directory.CreateDirectory(replicaRootPath);
            services.AddDiskStorage(new DiskStorageOptions
            {
                ProviderName = "disk-replica",
                IsPrimary = false,
                RootPath = replicaRootPath,
                CreateRootDirectory = true
            });
            observedRoots.Add(replicaRootPath);
        }

        DecorateStorageBackends(services);
        var serviceProvider = services.BuildServiceProvider();
        return Task.FromResult(new DiskServiceBenchmarkEnvironment(serviceProvider, scenarioRootPath, observedRoots));
    }

    private static void DecorateStorageBackends(IServiceCollection services)
    {
        var backendRegistrations = services
            .Where(static descriptor => descriptor.ServiceType == typeof(IStorageBackend))
            .ToArray();

        foreach (var registration in backendRegistrations) {
            services.Remove(registration);
            services.AddSingleton<IStorageBackend>(serviceProvider => new ProfilingStorageBackend(CreateBackend(serviceProvider, registration)));
        }
    }

    private static IStorageBackend CreateBackend(IServiceProvider serviceProvider, ServiceDescriptor descriptor)
    {
        if (descriptor.ImplementationInstance is IStorageBackend instance) {
            return instance;
        }

        if (descriptor.ImplementationFactory is not null) {
            return (IStorageBackend)descriptor.ImplementationFactory(serviceProvider)!;
        }

        if (descriptor.ImplementationType is not null) {
            return (IStorageBackend)ActivatorUtilities.CreateInstance(serviceProvider, descriptor.ImplementationType);
        }

        throw new InvalidOperationException("The IStorageBackend registration could not be materialized.");
    }
}

internal sealed class LoopbackHttpBenchmarkEnvironment : IAsyncDisposable
{
    private readonly WebApplication _application;
    private readonly string _scenarioRootPath;

    private LoopbackHttpBenchmarkEnvironment(WebApplication application, HttpClient client, string scenarioRootPath, IReadOnlyList<string> observedRoots)
    {
        _application = application;
        Client = client;
        _scenarioRootPath = scenarioRootPath;
        ObservedRoots = observedRoots;
        StorageService = application.Services.GetRequiredService<IStorageService>();
    }

    public HttpClient Client { get; }

    public IStorageService StorageService { get; }

    public IReadOnlyList<string> ObservedRoots { get; }

    public static async Task<LoopbackHttpBenchmarkEnvironment> CreateAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var scenarioRootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Benchmarks", Guid.NewGuid().ToString("N"));
        var storageRootPath = Path.Combine(scenarioRootPath, "storage");
        Directory.CreateDirectory(storageRootPath);

        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development,
            ContentRootPath = scenarioRootPath
        });

        builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, "http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Services.AddIntegratedS3(options => {
            options.RoutePrefix = BenchmarkDefaults.RoutePrefix;
            options.EnableAwsSignatureV4Authentication = true;
            options.AllowedSignatureClockSkewMinutes = 120;
            options.AccessKeyCredentials =
            [
                new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = BenchmarkDefaults.AccessKeyId,
                    SecretAccessKey = BenchmarkDefaults.SecretAccessKey,
                    DisplayName = "IntegratedS3 Benchmarks"
                }
            ];
        });
        builder.Services.AddDiskStorage(new DiskStorageOptions
        {
            ProviderName = "http-disk-primary",
            IsPrimary = true,
            RootPath = storageRootPath,
            CreateRootDirectory = true
        });
        DecorateStorageBackends(builder.Services);

        var application = builder.Build();
        application.Use(async (httpContext, next) => {
            var latencyCollector = new ProviderLatencyCollector();
            httpContext.Response.OnStarting(static state => {
                var (context, collector) = ((HttpContext Context, ProviderLatencyCollector Collector))state;
                var snapshot = collector.SnapshotTicks();
                if (snapshot.Count > 0) {
                    context.Response.Headers[BenchmarkHttpProviderLatencyHeader.HeaderName] = BenchmarkHttpProviderLatencyHeader.Serialize(snapshot);
                }

                return Task.CompletedTask;
            }, (httpContext, latencyCollector));

            using (StorageBackendProfilingContext.Begin(latencyCollector)) {
                await next(httpContext);
            }
        });
        application.MapIntegratedS3Endpoints();
        await application.StartAsync(cancellationToken);

        var address = application.Urls.SingleOrDefault();
        if (string.IsNullOrWhiteSpace(address)) {
            address = application.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()?.Addresses.SingleOrDefault();
        }

        if (string.IsNullOrWhiteSpace(address)) {
            await application.DisposeAsync();
            throw new InvalidOperationException("The loopback benchmark host did not expose an address.");
        }

        var client = new HttpClient
        {
            BaseAddress = new Uri(address, UriKind.Absolute)
        };

        return new LoopbackHttpBenchmarkEnvironment(application, client, scenarioRootPath, [storageRootPath]);
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _application.DisposeAsync();
        if (Directory.Exists(_scenarioRootPath)) {
            Directory.Delete(_scenarioRootPath, recursive: true);
        }
    }

    private static void DecorateStorageBackends(IServiceCollection services)
    {
        var backendRegistrations = services
            .Where(static descriptor => descriptor.ServiceType == typeof(IStorageBackend))
            .ToArray();

        foreach (var registration in backendRegistrations) {
            services.Remove(registration);
            services.AddSingleton<IStorageBackend>(serviceProvider => new ProfilingStorageBackend(CreateBackend(serviceProvider, registration)));
        }
    }

    private static IStorageBackend CreateBackend(IServiceProvider serviceProvider, ServiceDescriptor descriptor)
    {
        if (descriptor.ImplementationInstance is IStorageBackend instance) {
            return instance;
        }

        if (descriptor.ImplementationFactory is not null) {
            return (IStorageBackend)descriptor.ImplementationFactory(serviceProvider)!;
        }

        if (descriptor.ImplementationType is not null) {
            return (IStorageBackend)ActivatorUtilities.CreateInstance(serviceProvider, descriptor.ImplementationType);
        }

        throw new InvalidOperationException("The IStorageBackend registration could not be materialized.");
    }
}
