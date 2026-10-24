using IntegratedS3.Engine;
using IntegratedS3.Engine.DependencyInjection;
using IntegratedS3.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Tests.Infrastructure;

internal sealed class EngineStorageFixture : StorageProviderContractFixture
{
    public EngineStorageFixture(Action<IServiceCollection>? configureServices = null, Action<IntegratedS3EngineOptions>? configureEngine = null)
    {
        RootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Tests", "engine-" + Guid.NewGuid().ToString("N"));
        ConfigureEngine = configureEngine;
        SetServices(CreateServiceProvider(configureServices));
    }

    public string RootPath { get; }

    private Action<IntegratedS3EngineOptions>? ConfigureEngine { get; }

    protected override ValueTask<IServiceProvider> CreateServiceProviderAsync(
        Action<IServiceCollection>? configureServices,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult<IServiceProvider>(CreateServiceProvider(configureServices));
    }

    protected override ValueTask DisposeFixtureResourcesAsync()
    {
        if (Directory.Exists(RootPath)) {
            Directory.Delete(RootPath, recursive: true);
        }

        return ValueTask.CompletedTask;
    }

    private ServiceProvider CreateServiceProvider(Action<IServiceCollection>? configureServices)
    {
        var services = new ServiceCollection();
        configureServices?.Invoke(services);
        services.AddIntegratedS3Engine(options => {
            options.ProviderName = "test-engine";
            options.SqliteDatabasePath = Path.Combine(RootPath, "metadata.db");
            options.BlobRootPath = Path.Combine(RootPath, "blobs");
            options.MaintenanceInterval = TimeSpan.Zero;
            ConfigureEngine?.Invoke(options);
        });

        return services.BuildServiceProvider();
    }
}
