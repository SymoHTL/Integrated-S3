using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for generating a presigned URL for direct object access.</summary>
public sealed class StorageDirectObjectAccessRequest
{
    /// <summary>The type of operation the presigned URL will authorize.</summary>
    public required StorageDirectObjectAccessOperation Operation { get; init; }

    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The number of seconds until the presigned URL expires.</summary>
    public required int ExpiresInSeconds { get; init; }

    /// <summary>The version identifier of the object.</summary>
    public string? VersionId { get; init; }

    /// <summary>The MIME type constraint for the presigned URL.</summary>
    public string? ContentType { get; init; }

    /// <summary>The checksum algorithm to include in the presigned URL.</summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>Checksum values to include in the presigned URL, keyed by algorithm name.</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }
}
