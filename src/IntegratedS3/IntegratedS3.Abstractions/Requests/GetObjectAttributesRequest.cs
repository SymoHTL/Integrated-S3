namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the GetObjectAttributes operation.</summary>
public sealed class GetObjectAttributesRequest
{
    /// <summary>The name of the bucket containing the object.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key.</summary>
    public required string Key { get; init; }

    /// <summary>The version identifier of the object.</summary>
    public string? VersionId { get; init; }

    /// <summary>The list of attribute names to return for the object.</summary>
    public IReadOnlyList<string> ObjectAttributes { get; init; } = [];
}
