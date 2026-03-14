namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Describes which layer owns the persisted state behind advanced storage features.
/// </summary>
public sealed class StorageSupportStateDescriptor
{
    /// <summary>Ownership of persisted object metadata state.</summary>
    public StorageSupportStateOwnership ObjectMetadata { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of persisted object-tag state.</summary>
    public StorageSupportStateOwnership ObjectTags { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of multipart upload state.</summary>
    public StorageSupportStateOwnership MultipartState { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of version history and delete-marker state.</summary>
    public StorageSupportStateOwnership Versioning { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of persisted checksum state.</summary>
    public StorageSupportStateOwnership Checksums { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of access-control state.</summary>
    public StorageSupportStateOwnership AccessControl { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of retention or legal-hold state.</summary>
    public StorageSupportStateOwnership Retention { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of server-side encryption configuration or metadata.</summary>
    public StorageSupportStateOwnership ServerSideEncryption { get; set; } = StorageSupportStateOwnership.NotApplicable;

    /// <summary>Ownership of redirect-location or direct-object-location state.</summary>
    public StorageSupportStateOwnership RedirectLocations { get; set; } = StorageSupportStateOwnership.NotApplicable;
}
