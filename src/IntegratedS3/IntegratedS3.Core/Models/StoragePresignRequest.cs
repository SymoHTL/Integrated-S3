namespace IntegratedS3.Core.Models;

public sealed class StoragePresignRequest
{
    public required StoragePresignOperation Operation { get; init; }

    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required int ExpiresInSeconds { get; init; }

    public string? VersionId { get; init; }

    public string? ContentType { get; init; }

    /// <summary>
    /// The caller's preferred access mode for the returned presigned grant.
    /// When <see langword="null" /> the server chooses the default mode (typically <see cref="StorageAccessMode.Proxy" />).
    /// Strategies may honour, downgrade, or ignore this preference depending on provider capabilities.
    /// </summary>
    public StorageAccessMode? PreferredAccessMode { get; init; }
}
