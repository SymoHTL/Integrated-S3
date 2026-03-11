namespace IntegratedS3.Abstractions.Capabilities;

public sealed class StorageSupportStateDescriptor
{
    public StorageSupportStateOwnership ObjectMetadata { get; set; } = StorageSupportStateOwnership.NotApplicable;

    public StorageSupportStateOwnership ObjectTags { get; set; } = StorageSupportStateOwnership.NotApplicable;

    public StorageSupportStateOwnership MultipartState { get; set; } = StorageSupportStateOwnership.NotApplicable;

    public StorageSupportStateOwnership Versioning { get; set; } = StorageSupportStateOwnership.NotApplicable;

    public StorageSupportStateOwnership Checksums { get; set; } = StorageSupportStateOwnership.NotApplicable;

    public StorageSupportStateOwnership AccessControl { get; set; } = StorageSupportStateOwnership.NotApplicable;

    public StorageSupportStateOwnership Retention { get; set; } = StorageSupportStateOwnership.NotApplicable;

    public StorageSupportStateOwnership ServerSideEncryption { get; set; } = StorageSupportStateOwnership.NotApplicable;

    public StorageSupportStateOwnership RedirectLocations { get; set; } = StorageSupportStateOwnership.NotApplicable;
}
