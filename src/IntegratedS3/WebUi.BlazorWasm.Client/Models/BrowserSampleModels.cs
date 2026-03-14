using IntegratedS3.Core.Models;

namespace WebUi.BlazorWasm.Client.Models;

public sealed class BrowserSampleServiceDocument
{
    public string ServiceName { get; init; } = string.Empty;

    public BrowserSampleProviderDocument[] Providers { get; init; } = [];
}

public sealed class BrowserSampleProviderDocument
{
    public string Name { get; init; } = string.Empty;

    public string Kind { get; init; } = string.Empty;

    public bool IsPrimary { get; init; }
}

public sealed class BrowserSampleDashboardModel
{
    public string ServiceName { get; init; } = string.Empty;

    public IReadOnlyList<string> ProviderNames { get; init; } = [];

    public IReadOnlyList<BrowserSampleBucketModel> Buckets { get; init; } = [];
}

public sealed class BrowserSampleBucketModel
{
    public string Name { get; init; } = string.Empty;

    public DateTimeOffset CreatedAtUtc { get; init; }

    public IReadOnlyList<BrowserSampleObjectModel> Objects { get; init; } = [];
}

public sealed class BrowserSampleObjectModel
{
    public string Key { get; init; } = string.Empty;

    public long ContentLength { get; init; }

    public string? ContentType { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }
}

public sealed record BrowserSampleTransferResult(string BucketName, string Key, StorageAccessMode AccessMode, Uri Url);

public sealed record BrowserSampleDownloadResult(string BucketName, string Key, string Content, StorageAccessMode AccessMode, Uri Url);
