namespace IntegratedS3.Provider.S3.Internal;

/// <summary>A single page of object listing results from S3 ListObjectsV2.</summary>
internal sealed record S3ObjectListPage(
    IReadOnlyList<S3ObjectEntry> Entries,
    string? NextContinuationToken);
