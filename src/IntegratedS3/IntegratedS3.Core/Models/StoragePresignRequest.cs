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
    /// When <see langword="null" />, the server keeps proxy-mode issuance as the stable default
    /// rather than inferring <see cref="StorageAccessMode.Direct" /> or
    /// <see cref="StorageAccessMode.Delegated" /> from provider discovery.
    /// Strategies may honour, downgrade, or ignore an explicit preference depending on provider capabilities.
    /// </summary>
    public StorageAccessMode? PreferredAccessMode { get; init; }
}
