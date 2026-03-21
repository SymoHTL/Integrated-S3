using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Provider.Disk;

/// <summary>
/// Defines the S3 capability profile for the disk storage provider.
/// </summary>
public static class DiskStorageCapabilities
{
    /// <summary>
    /// Creates a <see cref="StorageCapabilities"/> snapshot describing the features
    /// natively supported, emulated, or unsupported by the disk provider.
    /// </summary>
    /// <returns>A <see cref="StorageCapabilities"/> instance with disk-provider defaults.</returns>
    public static StorageCapabilities CreateDefault()
    {
        return new StorageCapabilities
        {
            BucketOperations = StorageCapabilitySupport.Native,
            ObjectCrud = StorageCapabilitySupport.Native,
            ObjectMetadata = StorageCapabilitySupport.Emulated,
            ListObjects = StorageCapabilitySupport.Native,
            Pagination = StorageCapabilitySupport.Native,
            RangeRequests = StorageCapabilitySupport.Native,
            ConditionalRequests = StorageCapabilitySupport.Native,
            MultipartUploads = StorageCapabilitySupport.Emulated,
            CopyOperations = StorageCapabilitySupport.Native,
            PresignedUrls = StorageCapabilitySupport.Unsupported,
            ObjectTags = StorageCapabilitySupport.Emulated,
            Versioning = StorageCapabilitySupport.Emulated,
            BatchDelete = StorageCapabilitySupport.Unsupported,
            AccessControl = StorageCapabilitySupport.Unsupported,
            Cors = StorageCapabilitySupport.Emulated,
            ObjectLock = StorageCapabilitySupport.Unsupported,
            ServerSideEncryption = StorageCapabilitySupport.Unsupported,
            Checksums = StorageCapabilitySupport.Emulated,
            XmlErrors = StorageCapabilitySupport.Unsupported,
            PathStyleAddressing = StorageCapabilitySupport.Unsupported,
            VirtualHostedStyleAddressing = StorageCapabilitySupport.Unsupported
        };
    }
}
