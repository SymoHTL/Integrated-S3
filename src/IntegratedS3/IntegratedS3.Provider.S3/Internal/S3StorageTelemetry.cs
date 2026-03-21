using System.Diagnostics;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Observability;

namespace IntegratedS3.Provider.S3.Internal;

internal static class S3StorageTelemetry
{
    private const string ProviderKind = "s3";

    private static readonly Histogram<double> OperationDuration = IntegratedS3Observability.Meter.CreateHistogram<double>(
        "integrateds3.s3.operation.duration",
        unit: "ms",
        description: "Duration of S3 storage provider operations.");

    private static readonly Counter<long> OperationErrors = IntegratedS3Observability.Meter.CreateCounter<long>(
        "integrateds3.s3.operation.errors",
        unit: "{error}",
        description: "Count of S3 storage provider operation errors.");

    public static Activity? StartActivity(string operationName, string? bucketName = null, string? key = null)
    {
        if (!IntegratedS3Observability.ActivitySource.HasListeners())
            return null;

        var activity = IntegratedS3Observability.ActivitySource.StartActivity(
            $"IntegratedS3.S3.{operationName}",
            ActivityKind.Client);

        if (activity is null)
            return null;

        activity.SetTag(IntegratedS3Observability.Tags.Operation, operationName);
        activity.SetTag(IntegratedS3Observability.Tags.ProviderKind, ProviderKind);

        if (bucketName is not null)
            activity.SetTag("integrateds3.bucket_name", bucketName);
        if (key is not null)
            activity.SetTag("integrateds3.object_key", key);

        return activity;
    }

    public static void RecordSuccess(string operationName, TimeSpan duration)
    {
        var tags = new TagList
        {
            { IntegratedS3Observability.Tags.Operation, operationName },
            { IntegratedS3Observability.Tags.ProviderKind, ProviderKind },
            { IntegratedS3Observability.Tags.Result, "success" }
        };
        OperationDuration.Record(duration.TotalMilliseconds, tags);
    }

    public static void RecordFailure(string operationName, TimeSpan duration, string? errorCode = null)
    {
        var tags = new TagList
        {
            { IntegratedS3Observability.Tags.Operation, operationName },
            { IntegratedS3Observability.Tags.ProviderKind, ProviderKind },
            { IntegratedS3Observability.Tags.Result, "failure" }
        };
        if (errorCode is not null)
            tags.Add(IntegratedS3Observability.Tags.ErrorCode, errorCode);

        OperationDuration.Record(duration.TotalMilliseconds, tags);
        OperationErrors.Add(1, tags);
    }

    public static void MarkFailure(Activity? activity, string errorCode, string? description = null)
    {
        if (activity is null)
            return;

        activity.SetStatus(ActivityStatusCode.Error, description);
        activity.SetTag(IntegratedS3Observability.Tags.ErrorCode, errorCode);
    }
}
