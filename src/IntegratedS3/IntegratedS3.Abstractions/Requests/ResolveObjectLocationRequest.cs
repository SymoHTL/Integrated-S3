namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for resolving the physical read location of an object.</summary>
public sealed class ResolveObjectLocationRequest
{
    /// <summary>The name of the storage provider to resolve against.</summary>
    public string ProviderName { get; set; } = string.Empty;

    /// <summary>The name of the bucket containing the object.</summary>
    public string BucketName { get; set; } = string.Empty;

    /// <summary>The object key.</summary>
    public string Key { get; set; } = string.Empty;

    /// <summary>The version identifier of the object.</summary>
    public string? VersionId { get; set; }

    /// <summary>The expiration time for the resolved location, in UTC.</summary>
    public DateTimeOffset? ExpiresAtUtc { get; set; }
}
