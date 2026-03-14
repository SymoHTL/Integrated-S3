namespace IntegratedS3.Core.Models;

/// <summary>
/// Describes a concrete presigned HTTP request that can be executed by a client.
/// </summary>
public sealed class StoragePresignedRequest
{
    /// <summary>The object operation that was presigned.</summary>
    public required StoragePresignOperation Operation { get; init; }

    /// <summary>The access mode chosen for the returned grant.</summary>
    public required StorageAccessMode AccessMode { get; init; }

    /// <summary>The HTTP method to use for the request.</summary>
    public required string Method { get; init; }

    /// <summary>The presigned or redirected request URL.</summary>
    public required Uri Url { get; init; }

    /// <summary>The UTC timestamp when the grant expires.</summary>
    public required DateTimeOffset ExpiresAtUtc { get; init; }

    /// <summary>The bucket name bound into the request.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key bound into the request.</summary>
    public required string Key { get; init; }

    /// <summary>An optional version identifier bound into the request.</summary>
    public string? VersionId { get; init; }

    /// <summary>An optional content type that the caller must preserve when uploading.</summary>
    public string? ContentType { get; init; }

    /// <summary>The headers that must be applied when executing the request.</summary>
    public IReadOnlyList<StoragePresignedHeader> Headers { get; init; } = [];
}
