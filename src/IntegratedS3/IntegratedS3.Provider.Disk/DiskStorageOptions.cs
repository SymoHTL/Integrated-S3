namespace IntegratedS3.Provider.Disk;

public sealed class DiskStorageOptions
{
    public string ProviderName { get; set; } = "disk-primary";

    public bool IsPrimary { get; set; } = true;

    public string RootPath { get; set; } = "App_Data/IntegratedS3";

    public bool CreateRootDirectory { get; set; } = true;
}
