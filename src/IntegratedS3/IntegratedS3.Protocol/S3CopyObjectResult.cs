namespace IntegratedS3.Protocol;

/// <summary>
/// Represents the result of an S3 CopyObject operation.
/// </summary>
public sealed class S3CopyObjectResult
{
    /// <summary>The ETag of the newly copied object.</summary>
    public required string ETag { get; init; }

    /// <summary>The date and time the object was last modified, in UTC.</summary>
    public DateTimeOffset LastModifiedUtc { get; init; }

    /// <summary>The CRC-32 checksum of the copied object.</summary>
    public string? ChecksumCrc32 { get; init; }

    /// <summary>The CRC-32C checksum of the copied object.</summary>
    public string? ChecksumCrc32c { get; init; }

    /// <summary>The SHA-1 checksum of the copied object.</summary>
    public string? ChecksumSha1 { get; init; }

    /// <summary>The SHA-256 checksum of the copied object.</summary>
    public string? ChecksumSha256 { get; init; }

    /// <summary>The checksum type used for the copied object.</summary>
    public string? ChecksumType { get; init; }
}
