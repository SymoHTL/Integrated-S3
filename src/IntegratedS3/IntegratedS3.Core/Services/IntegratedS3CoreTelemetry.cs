using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Observability;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.Core.Services;

internal static class IntegratedS3CoreTelemetry
{
    private const string BucketTagName = "integrateds3.bucket_name";
    private const string ObjectKeyTagName = "integrateds3.object_key";
    private const string VersionIdTagName = "integrateds3.version_id";
    private const string PrincipalTagName = "integrateds3.principal_name";

    private static readonly Counter<long> StorageOperationCounter = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.StorageOperationCount,
        unit: "{operation}",
        description: "Count of storage operations executed through IntegratedS3.");

    private static readonly Histogram<double> StorageOperationDuration = IntegratedS3Observability.Meter.CreateHistogram<double>(
        IntegratedS3Observability.Metrics.StorageOperationDuration,
        unit: "ms",
        description: "Duration of storage operations executed through IntegratedS3.");

    private static readonly Counter<long> AuthorizationFailureCounter = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.StorageAuthorizationFailures,
        unit: "{failure}",
        description: "Count of storage authorization failures.");

    public static IDisposable? BeginOperationScope(
        ILogger logger,
        StorageAuthorizationRequest request,
        IntegratedS3RequestContext? requestContext)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(request);

        return logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = requestContext?.CorrelationId,
            ["RequestId"] = requestContext?.RequestId,
            ["StorageOperation"] = request.Operation.ToString(),
            ["BucketName"] = request.BucketName,
            ["ObjectKey"] = request.Key,
            ["VersionId"] = request.VersionId,
            ["PrincipalName"] = GetPrincipalName(requestContext?.Principal)
        });
    }

    public static Activity? StartOperationActivity(StorageAuthorizationRequest request, IntegratedS3RequestContext? requestContext)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (!IntegratedS3Observability.ActivitySource.HasListeners()) {
            return null;
        }

        var activity = IntegratedS3Observability.ActivitySource.StartActivity($"IntegratedS3.Storage.{request.Operation}", ActivityKind.Internal);
        if (activity is null) {
            return null;
        }

        activity.SetTag(IntegratedS3Observability.Tags.Operation, request.Operation.ToString());
        activity.SetTag(BucketTagName, request.BucketName);
        activity.SetTag(ObjectKeyTagName, request.Key);
        activity.SetTag(VersionIdTagName, request.VersionId);
        activity.SetTag(IntegratedS3Observability.Tags.CorrelationId, requestContext?.CorrelationId);
        activity.SetTag(IntegratedS3Observability.Tags.RequestId, requestContext?.RequestId);
        activity.SetTag(PrincipalTagName, GetPrincipalName(requestContext?.Principal));
        return activity;
    }

    public static void RecordStorageOperation(StorageAuthorizationRequest request, StorageResult result, TimeSpan duration)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(result);

        var tags = CreateCommonTags(request, result.IsSuccess ? "success" : "failure");
        if (!result.IsSuccess && result.Error is not null) {
            tags.Add(IntegratedS3Observability.Tags.ErrorCode, result.Error.Code.ToString());
        }

        StorageOperationCounter.Add(1, tags);
        StorageOperationDuration.Record(duration.TotalMilliseconds, tags);
    }

    public static void RecordStorageOperationFailure(StorageAuthorizationRequest request, string errorCode, TimeSpan duration)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(errorCode);

        var tags = CreateCommonTags(request, "failure");
        tags.Add(IntegratedS3Observability.Tags.ErrorCode, errorCode);

        StorageOperationCounter.Add(1, tags);
        StorageOperationDuration.Record(duration.TotalMilliseconds, tags);
    }

    public static void RecordAuthorizationFailure(StorageAuthorizationRequest request, StorageError error)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(error);

        var tags = new TagList
        {
            { IntegratedS3Observability.Tags.AuthStage, "authorization" },
            { IntegratedS3Observability.Tags.Operation, request.Operation.ToString() },
            { IntegratedS3Observability.Tags.ErrorCode, error.Code.ToString() }
        };

        AuthorizationFailureCounter.Add(1, tags);
    }

    public static void MarkFailure(Activity? activity, StorageError? error)
    {
        if (activity is null) {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, error?.Message);
        if (error is not null) {
            activity.SetTag(IntegratedS3Observability.Tags.ErrorCode, error.Code.ToString());
        }
    }

    public static void MarkFailure(Activity? activity, string errorCode, string? description = null)
    {
        if (activity is null) {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, description);
        activity.SetTag(IntegratedS3Observability.Tags.ErrorCode, errorCode);
    }

    public static void MarkCancelled(Activity? activity)
    {
        if (activity is null) {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, "Cancelled");
        activity.SetTag(IntegratedS3Observability.Tags.Result, "cancelled");
    }

    public static void SetProvider(Activity? activity, string providerName, string providerKind, bool isPrimary)
    {
        if (activity is null) {
            return;
        }

        activity.SetTag(IntegratedS3Observability.Tags.Provider, providerName);
        activity.SetTag(IntegratedS3Observability.Tags.ProviderKind, providerKind);
        if (isPrimary) {
            activity.SetTag(IntegratedS3Observability.Tags.PrimaryProvider, providerName);
        }
    }

    public static void AddReplicaEvent(
        Activity? activity,
        string eventName,
        StorageOperationType operation,
        string replicaBackendName,
        StorageReplicaRepairOrigin origin,
        StorageReplicaRepairStatus status,
        StorageError? error = null)
    {
        if (activity is null) {
            return;
        }

        var tags = new ActivityTagsCollection
        {
            { IntegratedS3Observability.Tags.Operation, operation.ToString() },
            { IntegratedS3Observability.Tags.ReplicaBackend, replicaBackendName },
            { IntegratedS3Observability.Tags.RepairOrigin, origin.ToString() },
            { IntegratedS3Observability.Tags.RepairStatus, status.ToString() }
        };

        if (error is not null) {
            tags.Add(IntegratedS3Observability.Tags.ErrorCode, error.Code.ToString());
        }

        activity.AddEvent(new ActivityEvent(eventName, tags: tags));
    }

    public static string? GetPrincipalName(ClaimsPrincipal? principal)
    {
        return principal?.Identity?.Name;
    }

    private static TagList CreateCommonTags(StorageAuthorizationRequest request, string result)
    {
        var tags = new TagList
        {
            { IntegratedS3Observability.Tags.Operation, request.Operation.ToString() },
            { IntegratedS3Observability.Tags.Result, result }
        };

        var activity = Activity.Current;
        if (activity is not null) {
            TryAddActivityTag(tags, IntegratedS3Observability.Tags.Provider, activity);
            TryAddActivityTag(tags, IntegratedS3Observability.Tags.ProviderKind, activity);
            TryAddActivityTag(tags, IntegratedS3Observability.Tags.PrimaryProvider, activity);
        }

        return tags;
    }

    private static void TryAddActivityTag(TagList tags, string key, Activity activity)
    {
        if (activity.GetTagItem(key) is string value && !string.IsNullOrWhiteSpace(value)) {
            tags.Add(key, value);
        }
    }
}
