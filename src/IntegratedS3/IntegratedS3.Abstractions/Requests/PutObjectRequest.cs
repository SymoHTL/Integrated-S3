using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the PutObject operation.</summary>
public sealed class PutObjectRequest
{
    /// <summary>The name of the target bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The object data stream to upload.</summary>
    public required Stream Content { get; init; }

    /// <summary>The size of the object data in bytes, if known.</summary>
    public long? ContentLength { get; init; }

    /// <summary>The MIME type of the object.</summary>
    public string? ContentType { get; init; }

    /// <summary>The Cache-Control header value for the object.</summary>
    public string? CacheControl { get; init; }

    /// <summary>The Content-Disposition header value for the object.</summary>
    public string? ContentDisposition { get; init; }

    /// <summary>The Content-Encoding header value for the object.</summary>
    public string? ContentEncoding { get; init; }

    /// <summary>The Content-Language header value for the object.</summary>
    public string? ContentLanguage { get; init; }

    /// <summary>The date and time at which the object should expire, in UTC.</summary>
    public DateTimeOffset? ExpiresUtc { get; init; }

    /// <summary>User-defined metadata key-value pairs for the object.</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>Tag key-value pairs to associate with the object.</summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    /// <summary>Checksum values for the object keyed by algorithm name.</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>Server-side encryption settings for the object.</summary>
    public ObjectServerSideEncryptionSettings? ServerSideEncryption { get; init; }

    /// <summary>Customer-provided encryption settings for the object.</summary>
    public ObjectCustomerEncryptionSettings? CustomerEncryption { get; init; }

    /// <summary>The storage class for the object.</summary>
    public string? StorageClass { get; init; }

    /// <summary>Only upload if the existing object ETag matches this value.</summary>
    public string? IfMatchETag { get; init; }

    /// <summary>Only upload if the existing object ETag does not match this value.</summary>
    public string? IfNoneMatchETag { get; init; }

    /// <summary>When <see langword="true"/>, overwrites an existing object with the same key. Defaults to <see langword="true"/>.</summary>
    public bool OverwriteIfExists { get; init; } = true;
}
