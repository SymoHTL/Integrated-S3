namespace IntegratedS3.Provider.Disk;

/// <summary>
/// Configures the disk-backed storage provider.
/// </summary>
public sealed class DiskStorageOptions
{
    /// <summary>The provider name reported in service metadata.</summary>
    public string ProviderName { get; set; } = "disk-primary";

    /// <summary>Whether this provider should be treated as the primary backend.</summary>
    public bool IsPrimary { get; set; } = true;

    /// <summary>The root path used to store bucket and object data.</summary>
    public string RootPath { get; set; } = "App_Data/IntegratedS3";

    /// <summary>Whether the root path should be created automatically on startup.</summary>
    public bool CreateRootDirectory { get; set; } = true;
}
