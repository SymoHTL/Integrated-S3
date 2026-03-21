namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the ListObjectVersions operation.</summary>
public sealed class ListObjectVersionsRequest
{
    /// <summary>The name of the bucket to list object versions from.</summary>
    public required string BucketName { get; init; }

    /// <summary>Limits results to objects whose keys begin with this prefix.</summary>
    public string? Prefix { get; init; }

    /// <summary>A delimiter character used to group keys into common prefixes.</summary>
    public string? Delimiter { get; init; }

    /// <summary>The key marker from which to continue a previous listing.</summary>
    public string? KeyMarker { get; init; }

    /// <summary>The version-id marker from which to continue a previous listing.</summary>
    public string? VersionIdMarker { get; init; }

    /// <summary>The maximum number of versions to return per page.</summary>
    public int? PageSize { get; init; }
}