using IntegratedS3.Abstractions.Capabilities;

namespace IntegratedS3.Provider.Disk;

public static class DiskStorageCapabilities
{
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
