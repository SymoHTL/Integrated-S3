using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.RegularExpressions;
using IntegratedS3.Abstractions.Observability;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace IntegratedS3.AspNetCore.Services;

internal static partial class IntegratedS3AspNetCoreTelemetry
{
    private const string RequestPathTagName = "url.path";
    private const string HttpMethodTagName = "http.request.method";
    private const string RequestScopeItemKey = "IntegratedS3.Observability.CorrelationId";

    /// <summary>
    /// Maximum accepted length for a client-supplied correlation id. Longer values are rejected in
    /// favour of a server-generated id to bound log/trace amplification.
    /// </summary>
    internal const int MaxCorrelationIdLength = 128;

    // Restrict inbound correlation ids to a safe, printable subset so an attacker cannot inject
    // control characters (e.g. CR/LF for log forging) into the log scope or activity tags.
    [GeneratedRegex("^[A-Za-z0-9._:-]{1,128}$", RegexOptions.CultureInvariant)]
    private static partial Regex CorrelationIdPattern();

    private static readonly Counter<long> AuthenticationFailureCounter = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.HttpAuthenticationFailures,
        unit: "{failure}",
        description: "Count of HTTP authentication failures handled by IntegratedS3.");

    private static readonly Counter<long> HttpRequestCounter = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.HttpRequestCount,
        unit: "{request}",
        description: "Count of HTTP requests to IntegratedS3 endpoints.");

    private static readonly Histogram<double> HttpRequestDuration = IntegratedS3Observability.Meter.CreateHistogram<double>(
        IntegratedS3Observability.Metrics.HttpRequestDuration,
        unit: "ms",
        description: "Duration of HTTP requests to IntegratedS3 endpoints.");

    private static readonly Counter<long> HttpBytesReceivedCounter = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.HttpBytesReceived,
        unit: "By",
        description: "Bytes received in HTTP request bodies (uploads) by IntegratedS3 endpoints.");

    private static readonly Counter<long> HttpBytesSentCounter = IntegratedS3Observability.Meter.CreateCounter<long>(
        IntegratedS3Observability.Metrics.HttpBytesSent,
        unit: "By",
        description: "Bytes sent in HTTP response bodies (downloads) by IntegratedS3 endpoints.");

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
        var correlationId = TryAcceptInboundCorrelationId(inboundCorrelationId, out var validated)
            ? validated
            : Activity.Current?.TraceId.ToString() ?? httpContext.TraceIdentifier;

        httpContext.Items[RequestScopeItemKey] = correlationId;
        httpContext.Response.Headers[IntegratedS3Observability.CorrelationIdHeaderName] = correlationId;
        return correlationId;
    }

    /// <summary>
    /// Validates a client-supplied correlation id. The value is accepted only when, after trimming
    /// surrounding whitespace, it is at most <see cref="MaxCorrelationIdLength"/> characters long and
    /// consists solely of the safe character set <c>[A-Za-z0-9._:-]</c>. This prevents log-injection /
    /// header-splitting via embedded control characters (e.g. CR/LF) and unbounded-length abuse. Any
    /// invalid value is discarded so the caller can fall back to a server-generated id.
    /// </summary>
    internal static bool TryAcceptInboundCorrelationId(string? inboundCorrelationId, out string correlationId)
    {
        correlationId = string.Empty;

        if (string.IsNullOrWhiteSpace(inboundCorrelationId)) {
            return false;
        }

        var trimmed = inboundCorrelationId.Trim();
        if (trimmed.Length > MaxCorrelationIdLength || !CorrelationIdPattern().IsMatch(trimmed)) {
            return false;
        }

        correlationId = trimmed;
        return true;
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

    public static void RecordHttpRequest(string method, string operation, int statusCode, double elapsedMs)
    {
        var tags = new TagList
        {
            { "http.request.method", method },
            { IntegratedS3Observability.Tags.Operation, operation },
            { "http.response.status_code", statusCode }
        };
        HttpRequestCounter.Add(1, tags);
        HttpRequestDuration.Record(elapsedMs, tags);
    }

    public static void RecordHttpBytesReceived(string operation, long bytes)
    {
        if (bytes <= 0) return;
        HttpBytesReceivedCounter.Add(bytes, new TagList { { IntegratedS3Observability.Tags.Operation, operation } });
    }

    public static void RecordHttpBytesSent(string operation, long bytes)
    {
        if (bytes <= 0) return;
        HttpBytesSentCounter.Add(bytes, new TagList { { IntegratedS3Observability.Tags.Operation, operation } });
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
