using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Services;
using Microsoft.Extensions.Options;

namespace IntegratedS3.AspNetCore.Services;

internal sealed class ConfiguredStorageDescriptorProvider(
    IEnumerable<IStorageBackend> backends,
    IOptions<IntegratedS3Options> options)
    : IStorageCapabilityProvider, IStorageServiceDescriptorProvider
{
    private readonly IStorageBackend[] _backends = backends.ToArray();

    public async ValueTask<StorageCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
    {
        var primaryBackend = ResolvePrimaryBackend();
        if (primaryBackend is null) {
            return CloneCapabilities(options.Value.Capabilities);
        }

        return CloneCapabilities(await primaryBackend.GetCapabilitiesAsync(cancellationToken));
    }

    public async ValueTask<StorageServiceDescriptor> GetServiceDescriptorAsync(CancellationToken cancellationToken = default)
    {
        var value = options.Value;
        var providers = _backends.Length == 0
            ? value.Providers.Select(CloneProvider).ToArray()
            : await CreateProviderDescriptorsAsync(cancellationToken);

        var descriptor = new StorageServiceDescriptor
        {
            ServiceName = value.ServiceName,
            Providers = providers,
            Capabilities = await GetCapabilitiesAsync(cancellationToken)
        };

        return descriptor;
    }

    private async Task<StorageProviderDescriptor[]> CreateProviderDescriptorsAsync(CancellationToken cancellationToken)
    {
        var providers = new StorageProviderDescriptor[_backends.Length];
        for (var index = 0; index < _backends.Length; index++) {
            cancellationToken.ThrowIfCancellationRequested();

            var backend = _backends[index];
            providers[index] = new StorageProviderDescriptor
            {
                Name = backend.Name,
                Kind = backend.Kind,
                IsPrimary = backend.IsPrimary,
                Description = backend.Description,
                Capabilities = CloneCapabilities(await backend.GetCapabilitiesAsync(cancellationToken))
            };
        }

        return providers;
    }

    private IStorageBackend? ResolvePrimaryBackend()
    {
        return _backends.FirstOrDefault(static backend => backend.IsPrimary)
            ?? _backends.FirstOrDefault();
    }

    private static StorageProviderDescriptor CloneProvider(StorageProviderDescriptor provider)
    {
        return new StorageProviderDescriptor
        {
            Name = provider.Name,
            Kind = provider.Kind,
            IsPrimary = provider.IsPrimary,
            Description = provider.Description,
            Capabilities = CloneCapabilities(provider.Capabilities)
        };
    }

    private static StorageCapabilities CloneCapabilities(StorageCapabilities capabilities)
    {
        return new StorageCapabilities
        {
            BucketOperations = capabilities.BucketOperations,
            ObjectCrud = capabilities.ObjectCrud,
            ObjectMetadata = capabilities.ObjectMetadata,
            ListObjects = capabilities.ListObjects,
            Pagination = capabilities.Pagination,
            RangeRequests = capabilities.RangeRequests,
            ConditionalRequests = capabilities.ConditionalRequests,
            MultipartUploads = capabilities.MultipartUploads,
            CopyOperations = capabilities.CopyOperations,
            PresignedUrls = capabilities.PresignedUrls,
            ObjectTags = capabilities.ObjectTags,
            Versioning = capabilities.Versioning,
            BatchDelete = capabilities.BatchDelete,
            AccessControl = capabilities.AccessControl,
            Cors = capabilities.Cors,
            ObjectLock = capabilities.ObjectLock,
            ServerSideEncryption = capabilities.ServerSideEncryption,
            Checksums = capabilities.Checksums,
            XmlErrors = capabilities.XmlErrors,
            PathStyleAddressing = capabilities.PathStyleAddressing,
            VirtualHostedStyleAddressing = capabilities.VirtualHostedStyleAddressing
        };
    }
}
