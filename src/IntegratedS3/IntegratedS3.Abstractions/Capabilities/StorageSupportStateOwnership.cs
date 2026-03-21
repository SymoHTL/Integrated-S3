namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Indicates which party owns and manages a particular category of state for a storage provider.
/// </summary>
public enum StorageSupportStateOwnership
{
    /// <summary>The state category is not relevant for this provider.</summary>
    NotApplicable,

    /// <summary>The backend itself manages this state natively.</summary>
    BackendOwned,

    /// <summary>The IntegratedS3 platform manages this state on behalf of the backend.</summary>
    PlatformManaged,

    /// <summary>State management is delegated to an external system.</summary>
    Delegated
}