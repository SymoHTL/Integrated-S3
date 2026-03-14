using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore;
using IntegratedS3.AspNetCore.DependencyInjection;
using IntegratedS3.Core.DependencyInjection;
using IntegratedS3.Provider.Disk;
using IntegratedS3.Provider.Disk.DependencyInjection;
using Microsoft.AspNetCore.Routing;
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
                ["IntegratedS3:Providers:0:IsPrimary"] = "true",
                ["IntegratedS3:Providers:0:Mode"] = "Passthrough",
                ["IntegratedS3:Providers:0:ObjectLocation:DefaultAccessMode"] = "Redirect",
                ["IntegratedS3:Providers:0:ObjectLocation:SupportedAccessModes:0"] = "Redirect",
                ["IntegratedS3:Providers:0:ObjectLocation:SupportedAccessModes:1"] = "Delegated",
                ["IntegratedS3:Providers:0:SupportState:AccessControl"] = "Delegated",
                ["IntegratedS3:Providers:0:SupportState:Retention"] = "Delegated",
                ["IntegratedS3:Providers:0:SupportState:ServerSideEncryption"] = "Delegated"
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
        Assert.Equal(StorageProviderMode.Passthrough, descriptor.Providers[0].Mode);
        Assert.Equal(StorageObjectAccessMode.Redirect, descriptor.Providers[0].ObjectLocation.DefaultAccessMode);
        Assert.Equal([StorageObjectAccessMode.Redirect, StorageObjectAccessMode.Delegated], descriptor.Providers[0].ObjectLocation.SupportedAccessModes);
        Assert.Equal(StorageSupportStateOwnership.Delegated, descriptor.Providers[0].SupportState.AccessControl);
        Assert.Equal(StorageSupportStateOwnership.Delegated, descriptor.Providers[0].SupportState.Retention);
        Assert.Equal(StorageSupportStateOwnership.Delegated, descriptor.Providers[0].SupportState.ServerSideEncryption);
    }

    [Fact]
    public void AddIntegratedS3_BindsEndpointOptionsFromConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IntegratedS3:Endpoints:EnableAdminEndpoints"] = "false",
                ["IntegratedS3:Endpoints:EnableObjectEndpoints"] = "false",
                ["IntegratedS3:Endpoints:EnableMultipartEndpoints"] = "false"
            })
            .Build();

        var services = new ServiceCollection();
        services.AddIntegratedS3(configuration);

        using var serviceProvider = services.BuildServiceProvider();
        var endpointOptions = serviceProvider.GetRequiredService<IOptions<IntegratedS3EndpointOptions>>().Value;

        Assert.True(endpointOptions.EnableServiceEndpoints);
        Assert.True(endpointOptions.EnableBucketEndpoints);
        Assert.False(endpointOptions.EnableAdminEndpoints);
        Assert.False(endpointOptions.EnableObjectEndpoints);
        Assert.False(endpointOptions.EnableMultipartEndpoints);
    }

    [Fact]
    public void EndpointOptions_FeatureRouteGroupConfiguration_CanRoundTripThroughFeatureRegistry()
    {
        var options = new IntegratedS3EndpointOptions();
        Action<RouteGroupBuilder> bucketConfiguration = static _ => { };
        Action<RouteGroupBuilder> multipartConfiguration = static _ => { };

        options.SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket, bucketConfiguration);
        options.ConfigureMultipartRouteGroup = multipartConfiguration;

        Assert.Same(bucketConfiguration, options.ConfigureBucketRouteGroup);
        Assert.Same(bucketConfiguration, options.GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Bucket));
        Assert.Same(multipartConfiguration, options.GetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Multipart));

        options.SetFeatureRouteGroupConfiguration(IntegratedS3EndpointFeature.Multipart, null);

        Assert.Null(options.ConfigureMultipartRouteGroup);
    }

    [Fact]
    public async Task AddIntegratedS3_CanCombineConfigurationBindingWithInlineConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IntegratedS3:ServiceName"] = "Configured Host",
                ["IntegratedS3:RoutePrefix"] = "/configured",
                ["IntegratedS3:Providers:0:Name"] = "configured-primary",
                ["IntegratedS3:Providers:0:Kind"] = "configured",
                ["IntegratedS3:Providers:0:IsPrimary"] = "true"
            })
            .Build();

        var services = new ServiceCollection();
        services.AddIntegratedS3(configuration, options => {
            options.RoutePrefix = "inline-storage";
            options.AccessKeyCredentials =
            [
                new IntegratedS3AccessKeyCredential
                {
                    AccessKeyId = "test-access",
                    SecretAccessKey = "test-secret",
                    DisplayName = " Test User ",
                    Scopes = [" storage.read ", "storage.read"]
                }
            ];
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<IOptions<IntegratedS3Options>>().Value;
        var descriptorProvider = serviceProvider.GetRequiredService<IStorageServiceDescriptorProvider>();
        var descriptor = await descriptorProvider.GetServiceDescriptorAsync();

        Assert.Equal("Configured Host", options.ServiceName);
        Assert.Equal("/inline-storage", options.RoutePrefix);
        var credential = Assert.Single(options.AccessKeyCredentials);
        Assert.Equal("test-access", credential.AccessKeyId);
        Assert.Equal("test-secret", credential.SecretAccessKey);
        Assert.Equal("Test User", credential.DisplayName);
        Assert.Equal(["storage.read"], credential.Scopes);

        var provider = Assert.Single(descriptor.Providers);
        Assert.Equal("configured-primary", provider.Name);
        Assert.Equal("configured", provider.Kind);
        Assert.True(provider.IsPrimary);
    }

    [Fact]
    public async Task AddIntegratedS3Provider_RegistersNamedProvidersWithoutBackends()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3(options => {
            options.ServiceName = "Configured Providers";
        });
        services.AddIntegratedS3Provider(" manual-primary ", " manual ", isPrimary: true, description: " Primary provider ");
        services.AddIntegratedS3Provider(provider => {
            provider.Name = "manual-replica";
            provider.Kind = "manual";
            provider.Description = "Replica provider";
            provider.Mode = StorageProviderMode.Passthrough;
            provider.Capabilities.ObjectCrud = StorageCapabilitySupport.Native;
            provider.ObjectLocation = new StorageObjectLocationDescriptor
            {
                DefaultAccessMode = StorageObjectAccessMode.Delegated,
                SupportedAccessModes = [StorageObjectAccessMode.Delegated, StorageObjectAccessMode.Redirect]
            };
            provider.SupportState.ObjectMetadata = StorageSupportStateOwnership.PlatformManaged;
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var descriptorProvider = serviceProvider.GetRequiredService<IStorageServiceDescriptorProvider>();
        var descriptor = await descriptorProvider.GetServiceDescriptorAsync();

        Assert.Collection(descriptor.Providers,
            provider => {
                Assert.Equal("manual-primary", provider.Name);
                Assert.Equal("manual", provider.Kind);
                Assert.True(provider.IsPrimary);
                Assert.Equal("Primary provider", provider.Description);
                Assert.Equal(StorageProviderMode.Managed, provider.Mode);
                Assert.Equal(StorageObjectAccessMode.ProxyStream, provider.ObjectLocation.DefaultAccessMode);
            },
            provider => {
                Assert.Equal("manual-replica", provider.Name);
                Assert.Equal("manual", provider.Kind);
                Assert.False(provider.IsPrimary);
                Assert.Equal("Replica provider", provider.Description);
                Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ObjectCrud);
                Assert.Equal(StorageProviderMode.Passthrough, provider.Mode);
                Assert.Equal(StorageObjectAccessMode.Delegated, provider.ObjectLocation.DefaultAccessMode);
                Assert.Equal([StorageObjectAccessMode.Delegated, StorageObjectAccessMode.Redirect], provider.ObjectLocation.SupportedAccessModes);
                Assert.Equal(StorageSupportStateOwnership.PlatformManaged, provider.SupportState.ObjectMetadata);
            });
    }

    [Fact]
    public async Task AddIntegratedS3Backend_ActivatesCustomBackendsViaDependencyInjection()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new TestBackendRegistrationOptions
        {
            Name = "custom-primary",
            Kind = "custom",
            IsPrimary = true,
            Description = "Dependency-injected custom backend",
            Mode = StorageProviderMode.Passthrough,
            DefaultAccessMode = StorageObjectAccessMode.Delegated,
            SupportedAccessModes = [StorageObjectAccessMode.Delegated, StorageObjectAccessMode.Redirect],
            ObjectCrud = StorageCapabilitySupport.Native,
            Checksums = StorageCapabilitySupport.Emulated,
            ObjectMetadata = StorageSupportStateOwnership.PlatformManaged
        });
        services.AddIntegratedS3Backend<DescriptorOnlyStorageBackend>();

        await using var serviceProvider = services.BuildServiceProvider();
        var descriptorProvider = serviceProvider.GetRequiredService<IStorageServiceDescriptorProvider>();
        var capabilityProvider = serviceProvider.GetRequiredService<IStorageCapabilityProvider>();
        var storageService = serviceProvider.GetRequiredService<IStorageService>();

        var descriptor = await descriptorProvider.GetServiceDescriptorAsync();
        var capabilities = await capabilityProvider.GetCapabilitiesAsync();

        Assert.NotNull(storageService);
        Assert.Equal("Integrated S3", descriptor.ServiceName);

        var provider = Assert.Single(descriptor.Providers);
        Assert.Equal("custom-primary", provider.Name);
        Assert.Equal("custom", provider.Kind);
        Assert.True(provider.IsPrimary);
        Assert.Equal("Dependency-injected custom backend", provider.Description);
        Assert.Equal(StorageProviderMode.Passthrough, provider.Mode);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Emulated, provider.Capabilities.Checksums);
        Assert.Equal(StorageObjectAccessMode.Delegated, provider.ObjectLocation.DefaultAccessMode);
        Assert.Equal([StorageObjectAccessMode.Delegated, StorageObjectAccessMode.Redirect], provider.ObjectLocation.SupportedAccessModes);
        Assert.Equal(StorageSupportStateOwnership.PlatformManaged, provider.SupportState.ObjectMetadata);
        Assert.Equal(StorageCapabilitySupport.Native, capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Emulated, capabilities.Checksums);
    }

    [Fact]
    public async Task AddIntegratedS3Backend_FactoryOverload_SupportsMultipleCustomBackends()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3(options => {
            options.ServiceName = "Factory Registered Backends";
        });
        services.AddIntegratedS3Backend(static _ => new DescriptorOnlyStorageBackend(new TestBackendRegistrationOptions
        {
            Name = "factory-primary",
            Kind = "factory",
            IsPrimary = true,
            Description = "Primary factory backend",
            ObjectCrud = StorageCapabilitySupport.Native
        }));
        services.AddIntegratedS3Backend(static _ => new DescriptorOnlyStorageBackend(new TestBackendRegistrationOptions
        {
            Name = "factory-replica",
            Kind = "factory",
            IsPrimary = false,
            Description = "Replica factory backend",
            Mode = StorageProviderMode.Passthrough,
            DefaultAccessMode = StorageObjectAccessMode.Redirect,
            SupportedAccessModes = [StorageObjectAccessMode.Redirect, StorageObjectAccessMode.Delegated]
        }));

        await using var serviceProvider = services.BuildServiceProvider();
        var descriptorProvider = serviceProvider.GetRequiredService<IStorageServiceDescriptorProvider>();

        var descriptor = await descriptorProvider.GetServiceDescriptorAsync();

        Assert.Equal(2, descriptor.Providers.Count);
        Assert.Contains(descriptor.Providers, static provider =>
            provider.Name == "factory-primary"
            && provider.Kind == "factory"
            && provider.IsPrimary
            && provider.Description == "Primary factory backend"
            && provider.Capabilities.ObjectCrud == StorageCapabilitySupport.Native);
        Assert.Contains(descriptor.Providers, static provider =>
            provider.Name == "factory-replica"
            && provider.Kind == "factory"
            && !provider.IsPrimary
            && provider.Mode == StorageProviderMode.Passthrough
            && provider.ObjectLocation.DefaultAccessMode == StorageObjectAccessMode.Redirect
            && provider.ObjectLocation.SupportedAccessModes.SequenceEqual([StorageObjectAccessMode.Redirect, StorageObjectAccessMode.Delegated]));
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
        Assert.Equal(StorageProviderMode.Managed, provider.Mode);
        Assert.Equal(StorageCapabilitySupport.Native, provider.Capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Emulated, provider.Capabilities.Checksums);
        Assert.Equal(StorageObjectAccessMode.ProxyStream, provider.ObjectLocation.DefaultAccessMode);
        Assert.Equal([StorageObjectAccessMode.ProxyStream], provider.ObjectLocation.SupportedAccessModes);
        Assert.Equal(StorageSupportStateOwnership.BackendOwned, provider.SupportState.ObjectMetadata);
        Assert.Equal(StorageSupportStateOwnership.BackendOwned, provider.SupportState.ObjectTags);
        Assert.Equal(StorageSupportStateOwnership.BackendOwned, provider.SupportState.Checksums);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, provider.SupportState.AccessControl);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, provider.SupportState.Retention);
        Assert.Equal(StorageSupportStateOwnership.NotApplicable, provider.SupportState.ServerSideEncryption);
        Assert.Equal(StorageCapabilitySupport.Native, capabilities.ObjectCrud);
        Assert.Equal(StorageCapabilitySupport.Emulated, capabilities.Checksums);
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

    [Fact]
    public async Task AddIntegratedS3Core_RegistersNullObjectLocationResolver()
    {
        var services = new ServiceCollection();
        services.AddIntegratedS3Core();

        await using var serviceProvider = services.BuildServiceProvider();
        var resolver = serviceProvider.GetRequiredService<IStorageObjectLocationResolver>();

        Assert.Equal(StorageSupportStateOwnership.NotApplicable, resolver.Ownership);

        var location = await resolver.ResolveReadLocationAsync(new ResolveObjectLocationRequest
        {
            ProviderName = "provider",
            BucketName = "bucket",
            Key = "docs/example.txt"
        });

        Assert.Null(location);
    }

    private sealed class TestBackendRegistrationOptions
    {
        public string Name { get; init; } = string.Empty;

        public string Kind { get; init; } = "custom";

        public bool IsPrimary { get; init; }

        public string? Description { get; init; }

        public StorageProviderMode Mode { get; init; } = StorageProviderMode.Managed;

        public StorageCapabilitySupport ObjectCrud { get; init; } = StorageCapabilitySupport.Unsupported;

        public StorageCapabilitySupport Checksums { get; init; } = StorageCapabilitySupport.Unsupported;

        public StorageObjectAccessMode DefaultAccessMode { get; init; } = StorageObjectAccessMode.ProxyStream;

        public IReadOnlyList<StorageObjectAccessMode> SupportedAccessModes { get; init; } = [StorageObjectAccessMode.ProxyStream];

        public StorageSupportStateOwnership ObjectMetadata { get; init; } = StorageSupportStateOwnership.NotApplicable;
    }

    private sealed class DescriptorOnlyStorageBackend(TestBackendRegistrationOptions options) : IStorageBackend
    {
        private readonly TestBackendRegistrationOptions _options = options ?? throw new ArgumentNullException(nameof(options));

        public string Name => _options.Name;

        public string Kind => _options.Kind;

        public bool IsPrimary => _options.IsPrimary;

        public string? Description => _options.Description;

        public ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageCapabilities
            {
                ObjectCrud = _options.ObjectCrud,
                Checksums = _options.Checksums
            });
        }

        public ValueTask<StorageSupportStateDescriptor> GetSupportStateDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageSupportStateDescriptor
            {
                ObjectMetadata = _options.ObjectMetadata
            });
        }

        public ValueTask<StorageProviderMode> GetProviderModeAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(_options.Mode);
        }

        public ValueTask<StorageObjectLocationDescriptor> GetObjectLocationDescriptorAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new StorageObjectLocationDescriptor
            {
                DefaultAccessMode = _options.DefaultAccessMode,
                SupportedAccessModes = [.. _options.SupportedAccessModes]
            });
        }

        public IAsyncEnumerable<BucketInfo> ListBucketsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
