namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Describes which party owns each category of state for a storage provider.
/// </summary>
public sealed class StorageSupportStateDescriptor
{
    /// <summary>Ownership of object metadata state.</summary>
    public StorageSupportStateOwnership ObjectMetadata { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of object tag state.</summary>
    public StorageSupportStateOwnership ObjectTags { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of multipart upload state.</summary>
    public StorageSupportStateOwnership MultipartState { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of versioning state.</summary>
    public StorageSupportStateOwnership Versioning { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of checksum state.</summary>
    public StorageSupportStateOwnership Checksums { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of access control state.</summary>
    public StorageSupportStateOwnership AccessControl { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of retention and object lock state.</summary>
    public StorageSupportStateOwnership Retention { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of server-side encryption state.</summary>
    public StorageSupportStateOwnership ServerSideEncryption { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of redirect/location resolution state.</summary>
    public StorageSupportStateOwnership RedirectLocations { get; set; } = StorageSupportStateOwnership.NotApplicable;
}
