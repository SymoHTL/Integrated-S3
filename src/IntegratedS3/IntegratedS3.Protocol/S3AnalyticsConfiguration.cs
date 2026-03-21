namespace IntegratedS3.Protocol;

public sealed class S3AnalyticsConfiguration
{
    public string Id { get; init; } = string.Empty;

    public string? FilterPrefix { get; init; }

    public IReadOnlyList<S3AnalyticsFilterTag>? FilterTags { get; init; }

    public S3StorageClassAnalysis? StorageClassAnalysis { get; init; }
}

public sealed class S3AnalyticsFilterTag
{
    public string Key { get; init; } = string.Empty;

    public string Value { get; init; } = string.Empty;
}

public sealed class S3StorageClassAnalysis
{
    public S3StorageClassAnalysisDataExport? DataExport { get; init; }
}

public sealed class S3StorageClassAnalysisDataExport
{
    public string OutputSchemaVersion { get; init; } = "V_1";

    public S3AnalyticsS3BucketDestination? Destination { get; init; }
}

public sealed class S3AnalyticsS3BucketDestination
{
    public string Format { get; init; } = "CSV";

    public string? BucketAccountId { get; init; }

    public string Bucket { get; init; } = string.Empty;

    public string? Prefix { get; init; }
}

public sealed class S3ListAnalyticsConfigurationsResult
{
    public IReadOnlyList<S3AnalyticsConfiguration> AnalyticsConfigurations { get; init; } = [];

    public bool IsTruncated { get; init; }

    public string? ContinuationToken { get; init; }

    public string? NextContinuationToken { get; init; }
}
