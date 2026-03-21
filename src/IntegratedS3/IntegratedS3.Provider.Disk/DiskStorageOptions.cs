namespace IntegratedS3.Provider.Disk;

/// <summary>
/// Configuration options for the disk storage provider.
/// </summary>
public sealed class DiskStorageOptions
{
    /// <summary>
    /// Logical name that identifies this provider instance. Defaults to <c>"disk-primary"</c>.
    /// </summary>
    public string ProviderName { get; set; } = "disk-primary";

    /// <summary>
    /// Indicates whether this provider is the primary storage backend. Defaults to <see langword="true"/>.
    /// </summary>
    public bool IsPrimary { get; set; } = true;

    /// <summary>
    /// Root directory on the local filesystem where objects are stored. Defaults to <c>"App_Data/IntegratedS3"</c>.
    /// </summary>
    public string RootPath { get; set; } = "App_Data/IntegratedS3";

    /// <summary>
    /// When <see langword="true"/>, the provider creates <see cref="RootPath"/> on startup if it does not exist.
    /// Defaults to <see langword="true"/>.
    /// </summary>
    public bool CreateRootDirectory { get; set; } = true;
}
