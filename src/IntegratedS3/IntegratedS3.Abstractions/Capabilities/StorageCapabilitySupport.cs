namespace IntegratedS3.Abstractions.Capabilities;

/// <summary>
/// Indicates whether a feature is unavailable, emulated by IntegratedS3, or provided natively by the backend.
/// </summary>
public enum StorageCapabilitySupport
{
    /// <summary>The feature is not supported.</summary>
    Unsupported,

    /// <summary>The feature is supported through IntegratedS3 emulation or orchestration.</summary>
    Emulated,

    /// <summary>The feature is supported natively by the underlying provider.</summary>
    Native
}
