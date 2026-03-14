namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Describes which S3-style features a provider or deployment supports and whether support is native or emulated.
/// </summary>
public sealed class StorageCapabilities
{
    /// <summary>Bucket creation, lookup, listing, and deletion.</summary>
    public StorageCapabilitySupport BucketOperations { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Object create, read, head, and delete operations.</summary>
    public StorageCapabilitySupport ObjectCrud { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Provider-managed or platform-managed object metadata persistence.</summary>
    public StorageCapabilitySupport ObjectMetadata { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Bucket object listing.</summary>
    public StorageCapabilitySupport ListObjects { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Continuation-token or marker-based pagination behavior.</summary>
    public StorageCapabilitySupport Pagination { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Byte-range object reads.</summary>
    public StorageCapabilitySupport RangeRequests { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Conditional request handling such as ETag or last-modified preconditions.</summary>
    public StorageCapabilitySupport ConditionalRequests { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Multipart upload initiation, part upload, completion, abort, and listing behavior.</summary>
    public StorageCapabilitySupport MultipartUploads { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Provider-native or emulated copy-object support.</summary>
    public StorageCapabilitySupport CopyOperations { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Presigned URL or direct object-access grant support.</summary>
    public StorageCapabilitySupport PresignedUrls { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Object tag read/write/delete behavior.</summary>
    public StorageCapabilitySupport ObjectTags { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Bucket versioning and version-aware object behavior.</summary>
    public StorageCapabilitySupport Versioning { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Multi-object delete request handling.</summary>
    public StorageCapabilitySupport BatchDelete { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Access-control list or equivalent access-policy behavior.</summary>
    public StorageCapabilitySupport AccessControl { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Bucket-level CORS configuration and response handling.</summary>
    public StorageCapabilitySupport Cors { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Object-lock, retention, or related compliance behavior.</summary>
    public StorageCapabilitySupport ObjectLock { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Server-side encryption request and response behavior.</summary>
    public StorageCapabilitySupport ServerSideEncryption { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Supported server-side-encryption variants and request-style details.</summary>
    public StorageServerSideEncryptionDescriptor ServerSideEncryptionDetails { get; set; } = new();

    /// <summary>Checksum validation, persistence, or exposure.</summary>
    public StorageCapabilitySupport Checksums { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>S3-style XML error payload generation.</summary>
    public StorageCapabilitySupport XmlErrors { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Path-style bucket addressing.</summary>
    public StorageCapabilitySupport PathStyleAddressing { get; set; } = StorageCapabilitySupport.Unsupported;

    /// <summary>Virtual-hosted-style bucket addressing.</summary>
    public StorageCapabilitySupport VirtualHostedStyleAddressing { get; set; } = StorageCapabilitySupport.Unsupported;
}
