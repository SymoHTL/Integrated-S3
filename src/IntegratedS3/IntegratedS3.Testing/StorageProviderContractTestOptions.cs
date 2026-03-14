namespace IntegratedS3.Testing;

/// <summary>
/// Opt-in settings for contract tests that exercise optional platform-managed helper seams.
/// </summary>
public sealed class StorageProviderContractTestOptions
{
    /// <summary>
    /// Enables tests that verify the backend cooperates with a registered
    /// <see cref="Abstractions.Services.IStorageObjectStateStore" />.
    /// </summary>
    public bool SupportsPlatformObjectStateStore { get; init; }

    /// <summary>
    /// Enables tests that verify the backend cooperates with a registered
    /// <see cref="Abstractions.Services.IStorageMultipartStateStore" />.
    /// </summary>
    public bool SupportsPlatformMultipartStateStore { get; init; }
}
