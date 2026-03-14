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
                Mode = await backend.GetProviderModeAsync(cancellationToken),
                Capabilities = CloneCapabilities(await backend.GetCapabilitiesAsync(cancellationToken)),
                ObjectLocation = CloneObjectLocation(await backend.GetObjectLocationDescriptorAsync(cancellationToken)),
                SupportState = CloneSupportState(await backend.GetSupportStateDescriptorAsync(cancellationToken))
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
            Mode = provider.Mode,
            Capabilities = CloneCapabilities(provider.Capabilities),
            ObjectLocation = CloneObjectLocation(provider.ObjectLocation),
            SupportState = CloneSupportState(provider.SupportState)
        };
    }

    private static StorageObjectLocationDescriptor CloneObjectLocation(StorageObjectLocationDescriptor objectLocation)
    {
        ArgumentNullException.ThrowIfNull(objectLocation);

        List<StorageObjectAccessMode> supportedAccessModes = objectLocation.SupportedAccessModes.Count == 0
            ? [objectLocation.DefaultAccessMode]
            : [.. objectLocation.SupportedAccessModes];
        if (!supportedAccessModes.Contains(objectLocation.DefaultAccessMode)) {
            supportedAccessModes.Insert(0, objectLocation.DefaultAccessMode);
        }

        return new StorageObjectLocationDescriptor
        {
            DefaultAccessMode = objectLocation.DefaultAccessMode,
            SupportedAccessModes = supportedAccessModes
                .Distinct()
                .ToList()
        };
    }

    private static StorageSupportStateDescriptor CloneSupportState(StorageSupportStateDescriptor supportState)
    {
        return new StorageSupportStateDescriptor
        {
            ObjectMetadata = supportState.ObjectMetadata,
            ObjectTags = supportState.ObjectTags,
            MultipartState = supportState.MultipartState,
            Versioning = supportState.Versioning,
            Checksums = supportState.Checksums,
            AccessControl = supportState.AccessControl,
            Retention = supportState.Retention,
            ServerSideEncryption = supportState.ServerSideEncryption,
            RedirectLocations = supportState.RedirectLocations
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
            ServerSideEncryptionDetails = CloneServerSideEncryptionDetails(capabilities.ServerSideEncryptionDetails),
            Checksums = capabilities.Checksums,
            XmlErrors = capabilities.XmlErrors,
            PathStyleAddressing = capabilities.PathStyleAddressing,
            VirtualHostedStyleAddressing = capabilities.VirtualHostedStyleAddressing
        };
    }

    private static StorageServerSideEncryptionDescriptor CloneServerSideEncryptionDetails(StorageServerSideEncryptionDescriptor serverSideEncryptionDetails)
    {
        ArgumentNullException.ThrowIfNull(serverSideEncryptionDetails);

        return new StorageServerSideEncryptionDescriptor
        {
            Variants = serverSideEncryptionDetails.Variants.Count == 0
                ? []
                : serverSideEncryptionDetails.Variants
                    .Select(static variant => new StorageServerSideEncryptionVariantDescriptor
                    {
                        Algorithm = variant.Algorithm,
                        RequestStyle = variant.RequestStyle,
                        SupportedRequestOperations = variant.SupportedRequestOperations.Count == 0
                            ? []
                            : [.. variant.SupportedRequestOperations],
                        SupportsResponseMetadata = variant.SupportsResponseMetadata,
                        SupportsKeyId = variant.SupportsKeyId,
                        SupportsContext = variant.SupportsContext
                    })
                    .ToArray()
        };
    }
}
