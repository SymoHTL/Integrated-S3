namespace IntegratedS3.Abstractions.Models;

/// <summary>
/// Describes how a provider participates in the overall IntegratedS3 deployment.
/// </summary>
public enum StorageProviderMode
{
    /// <summary>IntegratedS3 manages the provider directly.</summary>
    Managed,

    /// <summary>IntegratedS3 delegates most of the behavior to an external provider-managed flow.</summary>
    Delegated,

    /// <summary>IntegratedS3 primarily passes requests through to another system.</summary>
    Passthrough,

    /// <summary>IntegratedS3 combines managed behavior with direct or delegated provider behavior.</summary>
    Hybrid
}
