namespace IntegratedS3.Abstractions.Capabilities;

public sealed class StorageCapabilities
{
    public StorageCapabilitySupport BucketOperations { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport ObjectCrud { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport ObjectMetadata { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport ListObjects { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport Pagination { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport RangeRequests { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport ConditionalRequests { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport MultipartUploads { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport CopyOperations { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport PresignedUrls { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport ObjectTags { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport Versioning { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport BatchDelete { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport AccessControl { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport Cors { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport ObjectLock { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport ServerSideEncryption { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageServerSideEncryptionDescriptor ServerSideEncryptionDetails { get; set; } = new();

    public StorageCapabilitySupport Checksums { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport XmlErrors { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport PathStyleAddressing { get; set; } = StorageCapabilitySupport.Unsupported;

    public StorageCapabilitySupport VirtualHostedStyleAddressing { get; set; } = StorageCapabilitySupport.Unsupported;
}
