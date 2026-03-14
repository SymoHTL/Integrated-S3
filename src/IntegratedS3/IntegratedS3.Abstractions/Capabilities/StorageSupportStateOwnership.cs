namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Identifies which layer owns the persisted state behind an advanced capability.
/// </summary>
public enum StorageSupportStateOwnership
{
    /// <summary>The capability does not require persisted state.</summary>
    NotApplicable,

    /// <summary>The backing provider owns the relevant state directly.</summary>
    BackendOwned,

    /// <summary>IntegratedS3 persists and manages the relevant state.</summary>
    PlatformManaged,

    /// <summary>The relevant state is delegated to another external system or workflow.</summary>
    Delegated
}
