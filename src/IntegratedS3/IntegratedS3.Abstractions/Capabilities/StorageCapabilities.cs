namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Describes the set of S3-compatible capabilities a storage provider supports.
/// </summary>
public sealed class StorageCapabilities
{
    /// <summary>Support level for bucket create, delete, and list operations.</summary>
    public StorageCapabilitySupport BucketOperations { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for basic object create, read, update, and delete operations.</summary>
    public StorageCapabilitySupport ObjectCrud { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for reading and writing object metadata (user-defined and system headers).</summary>
    public StorageCapabilitySupport ObjectMetadata { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for listing objects within a bucket.</summary>
    public StorageCapabilitySupport ListObjects { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for paginated listing of objects.</summary>
    public StorageCapabilitySupport Pagination { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for HTTP range requests on object content.</summary>
    public StorageCapabilitySupport RangeRequests { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for conditional requests using ETags or timestamps.</summary>
    public StorageCapabilitySupport ConditionalRequests { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for multipart upload workflows.</summary>
    public StorageCapabilitySupport MultipartUploads { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for server-side object copy operations.</summary>
    public StorageCapabilitySupport CopyOperations { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for generating pre-signed URLs.</summary>
    public StorageCapabilitySupport PresignedUrls { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for object tagging.</summary>
    public StorageCapabilitySupport ObjectTags { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for object versioning.</summary>
    public StorageCapabilitySupport Versioning { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for batch (multi-object) delete operations.</summary>
    public StorageCapabilitySupport BatchDelete { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for access control lists or policies.</summary>
    public StorageCapabilitySupport AccessControl { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for cross-origin resource sharing (CORS) configuration.</summary>
    public StorageCapabilitySupport Cors { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for object lock (WORM) retention.</summary>
    public StorageCapabilitySupport ObjectLock { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for server-side encryption of stored objects.</summary>
    public StorageCapabilitySupport ServerSideEncryption { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Detailed descriptor of server-side encryption variants supported by the provider.</summary>
    public StorageServerSideEncryptionDescriptor ServerSideEncryptionDetails { get; set; } = new();

    /// <summary>Support level for object checksum verification.</summary>
    public StorageCapabilitySupport Checksums { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for returning S3-compatible XML error responses.</summary>
    public StorageCapabilitySupport XmlErrors { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for path-style bucket addressing.</summary>
    public StorageCapabilitySupport PathStyleAddressing { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Support level for virtual-hosted-style bucket addressing.</summary>
    public StorageCapabilitySupport VirtualHostedStyleAddressing { get; set; } = StorageCapabilitySupport.Unsupported;
}
