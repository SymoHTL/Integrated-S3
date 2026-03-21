using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the GetObject operation.</summary>
public sealed class GetObjectRequest
{
    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The version identifier of the object to retrieve.</summary>
    public string? VersionId { get; init; }

    /// <summary>The byte range to retrieve from the object.</summary>
    public ObjectRange? Range { get; init; }

    /// <summary>Server-side encryption settings used to decrypt the object.</summary>
    public ObjectServerSideEncryptionSettings? ServerSideEncryption { get; init; }

    /// <summary>Customer-provided encryption settings used to decrypt the object.</summary>
    public ObjectCustomerEncryptionSettings? CustomerEncryption { get; init; }

    /// <summary>Return the object only if its ETag matches this value.</summary>
    public string? IfMatchETag { get; init; }

    /// <summary>Return the object only if its ETag does not match this value.</summary>
    public string? IfNoneMatchETag { get; init; }

    /// <summary>Return the object only if it was modified after this date (UTC).</summary>
    public DateTimeOffset? IfModifiedSinceUtc { get; init; }

    /// <summary>Return the object only if it was not modified after this date (UTC).</summary>
    public DateTimeOffset? IfUnmodifiedSinceUtc { get; init; }
}
