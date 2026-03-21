namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the ListMultipartUploads operation.</summary>
public sealed class ListMultipartUploadsRequest
{
    /// <summary>The name of the bucket to list multipart uploads for.</summary>
    public required string BucketName { get; init; }

    /// <summary>Limits results to uploads whose keys begin with this prefix.</summary>
    public string? Prefix { get; init; }

    /// <summary>A delimiter character used to group keys into common prefixes.</summary>
    public string? Delimiter { get; init; }

    /// <summary>The key marker from which to continue a previous listing.</summary>
    public string? KeyMarker { get; init; }

    /// <summary>The upload-id marker from which to continue a previous listing.</summary>
    public string? UploadIdMarker { get; init; }

    /// <summary>The maximum number of uploads to return per page.</summary>
    public int? PageSize { get; init; }
}
