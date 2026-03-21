namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the GetObjectRetention operation.</summary>
public sealed class GetObjectRetentionRequest
{
    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The version identifier of the object.</summary>
    public string? VersionId { get; init; }
}
