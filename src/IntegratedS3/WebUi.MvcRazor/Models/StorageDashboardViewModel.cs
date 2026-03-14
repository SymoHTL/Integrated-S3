namespace WebUi.MvcRazor.Models;

public sealed class StorageDashboardViewModel
{
    public string ServiceName { get; init; } = string.Empty;

    public string RoutePrefix { get; init; } = string.Empty;

    public IReadOnlyList<StorageBucketViewModel> Buckets { get; init; } = [];
}

public sealed class StorageBucketViewModel
{
    public string Name { get; init; } = string.Empty;

    public DateTimeOffset CreatedAtUtc { get; init; }

    public IReadOnlyList<StorageObjectViewModel> Objects { get; init; } = [];
}

public sealed class StorageObjectViewModel
{
    public string Key { get; init; } = string.Empty;

    public long ContentLength { get; init; }

    public string? ContentType { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }
}
