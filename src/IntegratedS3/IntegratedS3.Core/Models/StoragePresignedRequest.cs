namespace IntegratedS3.Core.Models;

/// <summary>
/// Represents the result of a presign operation, containing the presigned URL,
/// required headers, and associated metadata.
/// </summary>
public sealed class StoragePresignedRequest
{
    /// <summary>The presign operation that produced this grant (e.g. GET or PUT).</summary>
    public required StoragePresignOperation Operation { get; init; }

    /// <summary>The access mode used for the presigned grant (direct, delegated, or proxy).</summary>
    public required StorageAccessMode AccessMode { get; init; }

    /// <summary>The HTTP method the caller must use when executing the presigned request (e.g. <c>GET</c> or <c>PUT</c>).</summary>
    public required string Method { get; init; }

    /// <summary>The presigned URL the caller should issue the request against.</summary>
    public required Uri Url { get; init; }

    /// <summary>The UTC point in time after which the presigned URL is no longer valid.</summary>
    public required DateTimeOffset ExpiresAtUtc { get; init; }

    /// <summary>The name of the target bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key within the bucket.</summary>
    public required string Key { get; init; }

    /// <summary>The object version identifier, or <see langword="null"/> for the latest version.</summary>
    public string? VersionId { get; init; }

    /// <summary>The MIME content type of the object, when applicable.</summary>
    public string? ContentType { get; init; }

    /// <summary>Additional HTTP headers the caller must include when executing the presigned request.</summary>
    public IReadOnlyList<StoragePresignedHeader> Headers { get; init; } = [];
}
