namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the ListObjectsV2 operation.</summary>
public sealed class ListObjectsRequest
{
    /// <summary>The name of the bucket to list objects from.</summary>
    public required string BucketName { get; init; }

    /// <summary>Limits results to objects whose keys begin with this prefix.</summary>
    public string? Prefix { get; init; }

    /// <summary>The continuation token from a previous listing to resume pagination.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>The maximum number of objects to return per page.</summary>
    public int? PageSize { get; init; }

    /// <summary>When <see langword="true"/>, includes all object versions in the listing.</summary>
    public bool IncludeVersions { get; init; }
}
