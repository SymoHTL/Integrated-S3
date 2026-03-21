using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace IntegratedS3.Abstractions.Observability;

/// <summary>
/// Canonical observability identifiers for logs, metrics, traces, and correlation headers emitted by IntegratedS3.
/// </summary>
public static class IntegratedS3Observability
{
    /// <summary>
    /// The name used for the OpenTelemetry <see cref="ActivitySource"/>.
    /// </summary>
    public const string ActivitySourceName = "IntegratedS3";

    /// <summary>
    /// The name used for the OpenTelemetry <see cref="Meter"/>.
    /// </summary>
    public const string MeterName = "IntegratedS3";

    /// <summary>
    /// The HTTP header name used to propagate correlation IDs across service boundaries.
    /// </summary>
    public const string CorrelationIdHeaderName = "x-integrateds3-correlation-id";

    /// <summary>
    /// Gets the shared <see cref="System.Diagnostics.ActivitySource"/> instance for distributed tracing.
    /// </summary>
    public static ActivitySource ActivitySource { get; } = new(ActivitySourceName);

    /// <summary>
    /// Gets the shared <see cref="System.Diagnostics.Metrics.Meter"/> instance for metrics collection.
    /// </summary>
    public static Meter Meter { get; } = new(MeterName);

    /// <summary>
    /// Contains well-known metric instrument names emitted by IntegratedS3.
    /// </summary>
    public static class Metrics
    {
        /// <summary>Counter of HTTP-level authentication failures.</summary>
        public const string HttpAuthenticationFailures = "integrateds3.http.authentication.failures";

        /// <summary>Counter of storage operation authorization denials.</summary>
        public const string StorageAuthorizationFailures = "integrateds3.storage.authorization.failures";

        /// <summary>Counter of storage operations executed.</summary>
        public const string StorageOperationCount = "integrateds3.storage.operation.count";

        /// <summary>Histogram of storage operation durations.</summary>
        public const string StorageOperationDuration = "integrateds3.storage.operation.duration";

        /// <summary>Histogram of replica repair operation durations.</summary>
        public const string ReplicaRepairDuration = "integrateds3.replication.repair.duration";

        /// <summary>Gauge of pending replica repair items.</summary>
        public const string ReplicaRepairBacklogSize = "integrateds3.replication.backlog.size";

        /// <summary>Gauge of the oldest pending repair item's age.</summary>
        public const string ReplicaRepairOldestAge = "integrateds3.replication.backlog.oldest_age";

        /// <summary>Gauge indicating backend health (1 = healthy, 0 = unhealthy).</summary>
        public const string BackendHealthStatus = "integrateds3.backend.health.status";

        /// <summary>Histogram of maintenance job durations.</summary>
        public const string MaintenanceJobDuration = "integrateds3.maintenance.job.duration";

        /// <summary>Counter of failed maintenance jobs.</summary>
        public const string MaintenanceJobFailures = "integrateds3.maintenance.job.failures";

        /// <summary>Counter of S3 XML protocol parsing errors.</summary>
        public const string ProtocolXmlParseErrors = "integrateds3.protocol.xml.parse_errors";

        /// <summary>Counter of S3 signature verification failures.</summary>
        public const string ProtocolSignatureErrors = "integrateds3.protocol.signature.errors";

        /// <summary>Counter of HTTP requests handled.</summary>
        public const string HttpRequestCount = "integrateds3.http.request.count";

        /// <summary>Histogram of HTTP request durations.</summary>
        public const string HttpRequestDuration = "integrateds3.http.request.duration";
    }

    /// <summary>
    /// Contains well-known tag/attribute keys for metrics and traces emitted by IntegratedS3.
    /// </summary>
    public static class Tags
    {
        /// <summary>The authentication stage at which the event occurred.</summary>
        public const string AuthStage = "integrateds3.auth_stage";

        /// <summary>The type of authentication used for the request.</summary>
        public const string AuthType = "integrateds3.auth_type";

        /// <summary>The correlation identifier for the request.</summary>
        public const string CorrelationId = "integrateds3.correlation_id";

        /// <summary>The error code associated with the event.</summary>
        public const string ErrorCode = "integrateds3.error_code";

        /// <summary>The storage operation being performed.</summary>
        public const string Operation = "integrateds3.operation";

        /// <summary>The primary storage provider handling the operation.</summary>
        public const string PrimaryProvider = "integrateds3.primary_provider";

        /// <summary>The storage provider associated with the event.</summary>
        public const string Provider = "integrateds3.provider";

        /// <summary>The kind or role of the provider (e.g., primary, replica).</summary>
        public const string ProviderKind = "integrateds3.provider_kind";

        /// <summary>The origin that triggered a replica repair operation.</summary>
        public const string RepairOrigin = "integrateds3.repair_origin";

        /// <summary>The current status of a replica repair operation.</summary>
        public const string RepairStatus = "integrateds3.repair_status";

        /// <summary>The replica backend involved in the operation.</summary>
        public const string ReplicaBackend = "integrateds3.replica_backend";

        /// <summary>The unique request identifier.</summary>
        public const string RequestId = "integrateds3.request_id";

        /// <summary>The result of the operation (e.g., success, failure).</summary>
        public const string Result = "integrateds3.result";
    }
}
