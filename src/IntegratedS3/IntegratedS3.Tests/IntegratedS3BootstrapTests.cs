using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace IntegratedS3.Tests;

public sealed class IntegratedS3BootstrapTests
{
    [Fact]
    public async Task AddIntegratedS3_BindsConfigurationAndNormalizesRoutePrefix()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IntegratedS3:ServiceName"] = "Test Host",
                ["IntegratedS3:RoutePrefix"] = "storage",
                ["IntegratedS3:Providers:0:Name"] = "disk-primary",
                ["IntegratedS3:Providers:0:Kind"] = "disk",
                ["IntegratedS3:Providers:0:IsPrimary"] = "true"
            })
            .Build();

        var services = new ServiceCollection();
        services.AddIntegratedS3(configuration);

        await using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<IOptions<IntegratedS3Options>>().Value;
        var descriptorProvider = serviceProvider.GetRequiredService<IStorageServiceDescriptorProvider>();
        var descriptor = await descriptorProvider.GetServiceDescriptorAsync();

        Assert.Equal("Test Host", options.ServiceName);
        Assert.Equal("/storage", options.RoutePrefix);
        Assert.Single(descriptor.Providers);
        Assert.Equal("disk-primary", descriptor.Providers[0].Name);
        Assert.Equal("disk", descriptor.Providers[0].Kind);
        Assert.True(descriptor.Providers[0].IsPrimary);
    }

    [Fact]
    public async Task CapabilityProvider_ReturnsClonedSnapshots()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3(options => {
            options.Capabilities.ObjectCrud = StorageCapabilitySupport.Native;
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var provider = serviceProvider.GetRequiredService<IStorageCapabilityProvider>();

        var first = await provider.GetCapabilitiesAsync();
        first.ObjectCrud = StorageCapabilitySupport.Unsupported;

        var second = await provider.GetCapabilitiesAsync();

        Assert.Equal(StorageCapabilitySupport.Native, second.ObjectCrud);
    }

    [Fact]
    public async Task DescriptorProvider_PrefersRegisteredBackendsOverConfiguredProviderMetadata()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3Core();
        services.AddIntegratedS3(options => {
            options.ServiceName = "Backend Driven Host";
            options.Providers =
            [
                new()
                {
                    Name = "configured-provider",
                    Kind = "configured",
                    IsPrimary = false
                }
            ];
            options.Capabilities.ObjectCrud = StorageCapabilitySupport.Unsupported;
        });
        services.AddDiskStorage(new DiskStorageOptions
        {
            ProviderName = "disk-from-backend",
            RootPath = Path.Combine(Path.GetTempPath(), $"integrated-s3-bootstrap-{Guid.NewGuid():N}"),
            CreateRootDirectory = true
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var descriptorProvider = serviceProvider.GetRequiredService<IStorageServiceDescriptorProvider>();
        var capabilityProvider = serviceProvider.GetRequiredService<IStorageCapabilityProvider>();

        var descriptor = await descriptorProvider.GetServiceDescriptorAsync();
        var capabilities = await capabilityProvider.GetCapabilitiesAsync();

        var provider = Assert.Single(descriptor.Providers);
        Assert.Equal("disk-from-backend", provider.Name);
        Assert.Equal("disk", provider.Kind);
        Assert.True(provider.IsPrimary);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Native, capabilities.ObjectCrud);
    }

    [Fact]
    public async Task DescriptorProvider_ReportsMultipleRegisteredBackendsWithPrimaryFlag()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3Core();
        services.AddIntegratedS3();
        services.AddDiskStorage(new DiskStorageOptions
        {
            ProviderName = "disk-primary",
            RootPath = Path.Combine(Path.GetTempPath(), $"integrated-s3-primary-{Guid.NewGuid():N}"),
            CreateRootDirectory = true,
            IsPrimary = true
        });
        services.AddDiskStorage(new DiskStorageOptions
        {
            ProviderName = "disk-replica",
            RootPath = Path.Combine(Path.GetTempPath(), $"integrated-s3-replica-{Guid.NewGuid():N}"),
            CreateRootDirectory = true,
            IsPrimary = false
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var descriptorProvider = serviceProvider.GetRequiredService<IStorageServiceDescriptorProvider>();

        var descriptor = await descriptorProvider.GetServiceDescriptorAsync();

        Assert.Equal(2, descriptor.Providers.Count);
        Assert.Contains(descriptor.Providers, static provider => provider.Name == "disk-primary" && provider.IsPrimary);
        Assert.Contains(descriptor.Providers, static provider => provider.Name == "disk-replica" && !provider.IsPrimary);
    }
}
