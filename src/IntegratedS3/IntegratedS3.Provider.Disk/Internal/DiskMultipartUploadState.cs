namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskMultipartUploadState
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public DateTimeOffset InitiatedAtUtc { get; init; }

    public string? ContentType { get; init; }

    public Dictionary<string, string>? Metadata { get; init; }
}
