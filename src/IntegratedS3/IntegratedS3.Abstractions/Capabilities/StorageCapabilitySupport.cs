namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Indicates the level of support a storage provider has for a given capability.
/// </summary>
public enum StorageCapabilitySupport
{
    /// <summary>The capability is not available.</summary>
    Unsupported,

    /// <summary>The capability is emulated by the platform, not natively provided by the backend.</summary>
    Emulated,

    /// <summary>The capability is natively supported by the backend.</summary>
    Native
}
