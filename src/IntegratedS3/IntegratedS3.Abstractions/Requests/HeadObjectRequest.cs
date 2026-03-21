using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the HeadObject operation (metadata-only retrieval).</summary>
public sealed class HeadObjectRequest
{
    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The version identifier of the object.</summary>
    public string? VersionId { get; init; }

    /// <summary>Server-side encryption settings used to decrypt the object metadata.</summary>
    public ObjectServerSideEncryptionSettings? ServerSideEncryption { get; init; }

    /// <summary>Customer-provided encryption settings used to decrypt the object metadata.</summary>
    public ObjectCustomerEncryptionSettings? CustomerEncryption { get; init; }

    /// <summary>Return metadata only if the object ETag matches this value.</summary>
    public string? IfMatchETag { get; init; }

    /// <summary>Return metadata only if the object ETag does not match this value.</summary>
    public string? IfNoneMatchETag { get; init; }

    /// <summary>Return metadata only if the object was modified after this date (UTC).</summary>
    public DateTimeOffset? IfModifiedSinceUtc { get; init; }

    /// <summary>Return metadata only if the object was not modified after this date (UTC).</summary>
    public DateTimeOffset? IfUnmodifiedSinceUtc { get; init; }
}
