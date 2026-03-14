using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace IntegratedS3.Abstractions.Observability;

/// <summary>
/// Canonical observability identifiers for logs, metrics, traces, and correlation headers emitted by IntegratedS3.
/// </summary>
public static class IntegratedS3Observability
{
    public const string ActivitySourceName = "IntegratedS3";
    public const string MeterName = "IntegratedS3";
    public const string CorrelationIdHeaderName = "x-integrateds3-correlation-id";

    public static ActivitySource ActivitySource { get; } = new(ActivitySourceName);

    public static Meter Meter { get; } = new(MeterName);

    public static class Metrics
    {
        public const string HttpAuthenticationFailures = "integrateds3.http.authentication.failures";
        public const string StorageAuthorizationFailures = "integrateds3.storage.authorization.failures";
        public const string StorageOperationCount = "integrateds3.storage.operation.count";
        public const string StorageOperationDuration = "integrateds3.storage.operation.duration";
        public const string ReplicaRepairDuration = "integrateds3.replication.repair.duration";
        public const string ReplicaRepairBacklogSize = "integrateds3.replication.backlog.size";
        public const string ReplicaRepairOldestAge = "integrateds3.replication.backlog.oldest_age";
        public const string BackendHealthStatus = "integrateds3.backend.health.status";
    }

    public static class Tags
    {
        public const string AuthStage = "integrateds3.auth_stage";
        public const string AuthType = "integrateds3.auth_type";
        public const string CorrelationId = "integrateds3.correlation_id";
        public const string ErrorCode = "integrateds3.error_code";
        public const string Operation = "integrateds3.operation";
        public const string PrimaryProvider = "integrateds3.primary_provider";
        public const string Provider = "integrateds3.provider";
        public const string ProviderKind = "integrateds3.provider_kind";
        public const string RepairOrigin = "integrateds3.repair_origin";
        public const string RepairStatus = "integrateds3.repair_status";
        public const string ReplicaBackend = "integrateds3.replica_backend";
        public const string RequestId = "integrateds3.request_id";
        public const string Result = "integrateds3.result";
    }
}
