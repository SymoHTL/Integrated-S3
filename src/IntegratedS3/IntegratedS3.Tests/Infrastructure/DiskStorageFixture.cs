using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using IntegratedS3.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratedS3.Tests.Infrastructure;

internal sealed class DiskStorageFixture : StorageProviderContractFixture
{
    public DiskStorageFixture(Action<IServiceCollection>? configureServices = null)
    {
        RootPath = Path.Combine(Path.GetTempPath(), "IntegratedS3.Tests", Guid.NewGuid().ToString("N"));
        SetServices(CreateServiceProvider(configureServices));
    }

    public string RootPath { get; }

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
        services.AddDiskStorage(new DiskStorageOptions
        {
            ProviderName = "test-disk",
            RootPath = RootPath,
            CreateRootDirectory = true
        });

        return services.BuildServiceProvider();
    }
}
