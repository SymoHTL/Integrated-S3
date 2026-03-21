namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the POST-based object upload operation.</summary>
public sealed class PostObjectRequest
{
    /// <summary>The name of the target bucket.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The MIME type of the object.</summary>
    public string? ContentType { get; init; }

    /// <summary>The object data stream to upload.</summary>
    public Stream? Content { get; init; }

    /// <summary>User-defined metadata key-value pairs for the object.</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>The canned ACL to apply to the object.</summary>
    public string? Acl { get; init; }

    /// <summary>The HTTP status code to return on success. Defaults to 204.</summary>
    public int SuccessActionStatus { get; init; } = 204;

    /// <summary>The URL to redirect to on successful upload.</summary>
    public string? SuccessActionRedirect { get; init; }
}
