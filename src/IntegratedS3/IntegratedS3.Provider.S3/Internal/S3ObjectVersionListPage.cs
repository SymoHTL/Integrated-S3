namespace IntegratedS3.Provider.S3.Internal;

/// <summary>A single page of object version listing results from S3 ListObjectVersions.</summary>
internal sealed record S3ObjectVersionListPage(
    IReadOnlyList<S3ObjectEntry> Entries,
    string? NextKeyMarker,
    string? NextVersionIdMarker);
