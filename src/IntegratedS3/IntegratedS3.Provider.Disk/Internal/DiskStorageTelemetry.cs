using System.Diagnostics;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Observability;

namespace IntegratedS3.Provider.Disk.Internal;

internal static class DiskStorageTelemetry
{
    private static readonly Histogram<double> OperationDuration = IntegratedS3Observability.Meter.CreateHistogram<double>(
        "integrateds3.disk.operation.duration", "ms", "Duration of disk storage operations");

    private static readonly Counter<long> OperationErrors = IntegratedS3Observability.Meter.CreateCounter<long>(
        "integrateds3.disk.operation.errors", "{error}", "Count of disk storage operation errors");

    public static Activity? StartActivity(string operationName, string bucketName, string? key = null)
    {
        var activity = IntegratedS3Observability.ActivitySource.StartActivity($"IntegratedS3.Disk.{operationName}");
        if (activity is not null)
        {
            activity.SetTag(IntegratedS3Observability.Tags.Operation, operationName);
            activity.SetTag(IntegratedS3Observability.Tags.Provider, "disk");
            activity.SetTag(IntegratedS3Observability.Tags.ProviderKind, "disk");
            activity.SetTag("integrateds3.bucket_name", bucketName);
            if (key is not null)
                activity.SetTag("integrateds3.object_key", key);
        }
        return activity;
    }

    public static void RecordSuccess(Activity? activity, string operation, long elapsedMs)
    {
        activity?.SetTag(IntegratedS3Observability.Tags.Result, "success");
        OperationDuration.Record(elapsedMs,
            new KeyValuePair<string, object?>(IntegratedS3Observability.Tags.Operation, operation),
            new KeyValuePair<string, object?>(IntegratedS3Observability.Tags.Result, "success"));
    }

    public static void RecordFailure(Activity? activity, string operation, string? errorCode, long elapsedMs)
    {
        activity?.SetStatus(ActivityStatusCode.Error, errorCode);
        activity?.SetTag(IntegratedS3Observability.Tags.Result, "failure");
        activity?.SetTag(IntegratedS3Observability.Tags.ErrorCode, errorCode);

        var tags = new TagList
        {
            { IntegratedS3Observability.Tags.Operation, operation },
            { IntegratedS3Observability.Tags.Result, "failure" },
            { IntegratedS3Observability.Tags.ErrorCode, errorCode }
        };
        OperationDuration.Record(elapsedMs, tags);
        OperationErrors.Add(1, tags);
    }
}
