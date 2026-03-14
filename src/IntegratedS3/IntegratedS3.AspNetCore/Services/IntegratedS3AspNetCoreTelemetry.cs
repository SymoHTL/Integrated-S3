using System.Diagnostics;
using System.Diagnostics.Metrics;
using IntegratedS3.Abstractions.Observability;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.AspNetCore.Services;

internal static class IntegratedS3AspNetCoreTelemetry
{
    private const string RequestPathTagName = "url.path";
    private const string HttpMethodTagName = "http.request.method";
    private const string RequestScopeItemKey = "IntegratedS3.Observability.CorrelationId";

    private static readonly Counter<long> AuthenticationFailureCounter = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.HttpAuthenticationFailures,
        unit: "{failure}",
        description: "Count of HTTP authentication failures handled by IntegratedS3.");

    public static string GetOrCreateCorrelationId(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        if (httpContext.Items.TryGetValue(RequestScopeItemKey, out var existingValue)
            && existingValue is string existingCorrelationId
            && !string.IsNullOrWhiteSpace(existingCorrelationId)) {
            httpContext.Response.Headers[IntegratedS3Observability.CorrelationIdHeaderName] = existingCorrelationId;
            return existingCorrelationId;
        }

        var inboundCorrelationId = httpContext.Request.Headers[IntegratedS3Observability.CorrelationIdHeaderName].ToString();
        var correlationId = !string.IsNullOrWhiteSpace(inboundCorrelationId)
            ? inboundCorrelationId.Trim()
            : Activity.Current?.TraceId.ToString() ?? httpContext.TraceIdentifier;

        httpContext.Items[RequestScopeItemKey] = correlationId;
        httpContext.Response.Headers[IntegratedS3Observability.CorrelationIdHeaderName] = correlationId;
        return correlationId;
    }

    public static IDisposable? BeginRequestScope(ILogger logger, HttpContext httpContext, string correlationId)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);

        return logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = correlationId,
            ["RequestId"] = httpContext.TraceIdentifier,
            ["RequestPath"] = httpContext.Request.Path.ToString(),
            ["RequestMethod"] = httpContext.Request.Method
        });
    }

    public static Activity? StartRequestActivity(HttpContext httpContext, string correlationId)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);

        var activity = IntegratedS3Observability.ActivitySource.StartActivity("IntegratedS3.HttpRequest", ActivityKind.Internal);
        if (activity is null) {
            return null;
        }

        activity.SetTag(HttpMethodTagName, httpContext.Request.Method);
        activity.SetTag(RequestPathTagName, httpContext.Request.Path.ToString());
        activity.SetTag(IntegratedS3Observability.Tags.CorrelationId, correlationId);
        activity.SetTag(IntegratedS3Observability.Tags.RequestId, httpContext.TraceIdentifier);
        return activity;
    }

    public static void RecordAuthenticationFailure(string authStage, string authType, string errorCode)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(authStage);
        ArgumentException.ThrowIfNullOrWhiteSpace(authType);
        ArgumentException.ThrowIfNullOrWhiteSpace(errorCode);

        var tags = new TagList
        {
            { IntegratedS3Observability.Tags.AuthStage, authStage },
            { IntegratedS3Observability.Tags.AuthType, authType },
            { IntegratedS3Observability.Tags.ErrorCode, errorCode }
        };

        AuthenticationFailureCounter.Add(1, tags);
    }

    public static void MarkFailure(Activity? activity, string authType, string errorCode, string? description = null)
    {
        if (activity is null) {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, description);
        activity.SetTag(IntegratedS3Observability.Tags.AuthType, authType);
        activity.SetTag(IntegratedS3Observability.Tags.Result, "failure");
        activity.SetTag(IntegratedS3Observability.Tags.ErrorCode, errorCode);
    }

    public static void MarkSuccess(Activity? activity, string authType)
    {
        if (activity is null) {
            return;
        }

        activity.SetTag(IntegratedS3Observability.Tags.AuthType, authType);
        activity.SetTag(IntegratedS3Observability.Tags.Result, "success");
    }
}
