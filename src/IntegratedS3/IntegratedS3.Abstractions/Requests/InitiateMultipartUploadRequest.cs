using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the InitiateMultipartUpload operation.</summary>
public sealed class InitiateMultipartUploadRequest
{
    /// <summary>The name of the target bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The MIME type of the object being uploaded.</summary>
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

    /// <summary>The checksum algorithm to use for part integrity verification.</summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>Server-side encryption settings for the object.</summary>
    public ObjectServerSideEncryptionSettings? ServerSideEncryption { get; init; }

    /// <summary>Customer-provided encryption settings for the object.</summary>
    public ObjectCustomerEncryptionSettings? CustomerEncryption { get; init; }

    /// <summary>The storage class for the object.</summary>
    public string? StorageClass { get; init; }
}
