using System.Net;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.Core.Services;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using IntegratedS3.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3HealthCheckTests : IClassFixture<WebUiApplicationFactory>
{
    private readonly WebUiApplicationFactory _factory;

    public IntegratedS3HealthCheckTests(WebUiApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task AddIntegratedS3BackendHealthCheck_ReportsHealthyReadinessForRegisteredBackends()
    {
        var rootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.HealthChecks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(rootPath);

        try {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddIntegratedS3();
            services.AddDiskStorage(new DiskStorageOptions
            {
                ProviderName = "disk-primary",
                RootPath = rootPath,
                CreateRootDirectory = true
            });
            services.AddHealthChecks()
                .AddIntegratedS3BackendHealthCheck();

            await using var serviceProvider = services.BuildServiceProvider();
            var healthCheckService = serviceProvider.GetRequiredService<HealthCheckService>();

            var report = await healthCheckService.CheckHealthAsync(static registration => registration.Tags.Contains(IntegratedS3HealthCheckTags.Readiness));

            Assert.Equal(HealthStatus.Healthy, report.Status);

            var entry = Assert.Single(report.Entries);
            Assert.Equal(HealthStatus.Healthy, entry.Value.Status);
            Assert.Contains(IntegratedS3HealthCheckTags.Readiness, entry.Value.Tags);
            Assert.Equal("Healthy", entry.Value.Data["backend:disk-primary"]);
            Assert.Equal("primary", entry.Value.Data["backend:disk-primary:role"]);
        }
        finally {
            if (Directory.Exists(rootPath)) {
                Directory.Delete(rootPath, recursive: true);
            }
        }
    }

    [Fact]
    public async Task AddIntegratedS3BackendHealthCheck_ReportsUnhealthyWhenBackendHealthIsUnhealthy()
    {
        var primaryRootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.HealthChecks", Guid.NewGuid().ToString("N"), "primary");
        var replicaRootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.HealthChecks", Guid.NewGuid().ToString("N"), "replica");
        Directory.CreateDirectory(primaryRootPath);
        Directory.CreateDirectory(replicaRootPath);

        try {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddIntegratedS3();
            services.AddDiskStorage(new DiskStorageOptions
            {
                ProviderName = "disk-primary",
                RootPath = primaryRootPath,
                CreateRootDirectory = true,
                IsPrimary = true
            });
            services.AddDiskStorage(new DiskStorageOptions
            {
                ProviderName = "disk-replica",
                RootPath = replicaRootPath,
                CreateRootDirectory = true,
                IsPrimary = false
            });
            services.Replace(ServiceDescriptor.Singleton<IStorageBackendHealthEvaluator>(
                new ConfigurableStorageBackendHealthEvaluator(new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
                {
                    ["disk-primary"] = StorageBackendHealthStatus.Healthy,
                    ["disk-replica"] = StorageBackendHealthStatus.Unhealthy
                })));
            services.AddHealthChecks()
                .AddIntegratedS3BackendHealthCheck();

            await using var serviceProvider = services.BuildServiceProvider();
            var healthCheckService = serviceProvider.GetRequiredService<HealthCheckService>();

            var report = await healthCheckService.CheckHealthAsync(static registration => registration.Tags.Contains(IntegratedS3HealthCheckTags.Readiness));

            Assert.Equal(HealthStatus.Unhealthy, report.Status);

            var entry = Assert.Single(report.Entries);
            Assert.Equal(HealthStatus.Unhealthy, entry.Value.Status);
            Assert.Equal("Healthy", entry.Value.Data["backend:disk-primary"]);
            Assert.Equal("Unhealthy", entry.Value.Data["backend:disk-replica"]);
        }
        finally {
            if (Directory.Exists(primaryRootPath)) {
                Directory.Delete(primaryRootPath, recursive: true);
            }

            if (Directory.Exists(replicaRootPath)) {
                Directory.Delete(replicaRootPath, recursive: true);
            }
        }
    }

    [Fact]
    public async Task WebUiApplication_MapsLiveAndReadyHealthEndpoints()
    {
        using var client = await _factory.CreateClientAsync();

        var liveResponse = await client.GetAsync("/health/live");
        Assert.Equal(HttpStatusCode.OK, liveResponse.StatusCode);
        Assert.Equal("Healthy", await liveResponse.Content.ReadAsStringAsync());

        var readyResponse = await client.GetAsync("/health/ready");
        Assert.Equal(HttpStatusCode.OK, readyResponse.StatusCode);
        Assert.Equal("Healthy", await readyResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task WebUiApplication_ReadinessEndpointReturnsServiceUnavailableWhenBackendIsUnhealthy()
    {
        await using var isolatedClient = await _factory.CreateIsolatedClientAsync(builder => {
            builder.Services.Replace(ServiceDescriptor.Singleton<IStorageBackendHealthEvaluator>(
                new ConfigurableStorageBackendHealthEvaluator(new Dictionary<string, StorageBackendHealthStatus>(StringComparer.Ordinal)
                {
                    ["test-disk"] = StorageBackendHealthStatus.Unhealthy
                })));
        });

        var liveResponse = await isolatedClient.Client.GetAsync("/health/live");
        Assert.Equal(HttpStatusCode.OK, liveResponse.StatusCode);
        Assert.Equal("Healthy", await liveResponse.Content.ReadAsStringAsync());

        var readyResponse = await isolatedClient.Client.GetAsync("/health/ready");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, readyResponse.StatusCode);
        Assert.Equal("Unhealthy", await readyResponse.Content.ReadAsStringAsync());
    }

    private sealed class ConfigurableStorageBackendHealthEvaluator(IReadOnlyDictionary<string, StorageBackendHealthStatus> statuses) : IStorageBackendHealthEvaluator
    {
        public ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(backend);
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult(
                statuses.TryGetValue(backend.Name, out var status)
                    ? status
                    : StorageBackendHealthStatus.Healthy);
        }
    }
}
