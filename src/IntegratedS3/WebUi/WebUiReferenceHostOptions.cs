public sealed class WebUiReferenceHostOptions
{
    public WebUiStorageProvider StorageProvider { get; set; } = WebUiStorageProvider.Disk;

    public WebUiReferenceHostRoutePolicyOptions RoutePolicies { get; set; } = new();
}

public sealed class WebUiReferenceHostRoutePolicyOptions
{
    public string? Route { get; set; }

    public string? Root { get; set; }

    public string? Compatibility { get; set; }

    public string? Service { get; set; }

    public string? Bucket { get; set; }

    public string? Object { get; set; }

    public string? Multipart { get; set; }

    public string? Admin { get; set; }
}

public enum WebUiStorageProvider
{
    Disk,
    S3
}
