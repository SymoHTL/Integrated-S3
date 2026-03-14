using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.S3;

public static class S3StorageCapabilities
{
    /// <summary>
    /// Builds a capability snapshot that reflects the runtime <paramref name="options"/>.
    /// Path-style vs. virtual-hosted-style addressing capability is derived from
    /// <see cref="S3StorageOptions.ForcePathStyle"/>.
    /// </summary>
    public static StorageCapabilities CreateDefault(S3StorageOptions options) => new StorageCapabilities
    {
        BucketOperations = StorageCapabilitySupport.Native,
        ObjectCrud = StorageCapabilitySupport.Native,
        ObjectMetadata = StorageCapabilitySupport.Native,
        ListObjects = StorageCapabilitySupport.Native,
        Pagination = StorageCapabilitySupport.Native,
        RangeRequests = StorageCapabilitySupport.Native,
        ConditionalRequests = StorageCapabilitySupport.Native,
        MultipartUploads = StorageCapabilitySupport.Native,
        CopyOperations = StorageCapabilitySupport.Native,
        PresignedUrls = StorageCapabilitySupport.Native,
        ObjectTags = StorageCapabilitySupport.Native,
        Versioning = StorageCapabilitySupport.Native,
        BatchDelete = StorageCapabilitySupport.Unsupported,
        AccessControl = StorageCapabilitySupport.Unsupported,
        Cors = StorageCapabilitySupport.Native,
        ObjectLock = StorageCapabilitySupport.Unsupported,
        ServerSideEncryption = StorageCapabilitySupport.Native,
        ServerSideEncryptionDetails = CreateServerSideEncryptionDescriptor(),
        Checksums = StorageCapabilitySupport.Native,
        XmlErrors = StorageCapabilitySupport.Unsupported,
        PathStyleAddressing = options.ForcePathStyle
            ? StorageCapabilitySupport.Native
            : StorageCapabilitySupport.Unsupported,
        VirtualHostedStyleAddressing = options.ForcePathStyle
            ? StorageCapabilitySupport.Unsupported
            : StorageCapabilitySupport.Native
    };

    private static StorageServerSideEncryptionDescriptor CreateServerSideEncryptionDescriptor() => new()
    {
        Variants =
        [
            CreateManagedVariant(ObjectServerSideEncryptionAlgorithm.Aes256),
            CreateManagedVariant(ObjectServerSideEncryptionAlgorithm.Kms, supportsKeyId: true, supportsContext: true),
            CreateManagedVariant(ObjectServerSideEncryptionAlgorithm.KmsDsse, supportsKeyId: true, supportsContext: true)
        ]
    };

    private static StorageServerSideEncryptionVariantDescriptor CreateManagedVariant(
        ObjectServerSideEncryptionAlgorithm algorithm,
        bool supportsKeyId = false,
        bool supportsContext = false) => new()
    {
        Algorithm = algorithm,
        RequestStyle = StorageServerSideEncryptionRequestStyle.Managed,
        SupportedRequestOperations =
        [
            StorageServerSideEncryptionRequestOperation.PutObject,
            StorageServerSideEncryptionRequestOperation.CopyDestination,
            StorageServerSideEncryptionRequestOperation.InitiateMultipartUpload
        ],
        SupportsResponseMetadata = true,
        SupportsKeyId = supportsKeyId,
        SupportsContext = supportsContext
    };
}
