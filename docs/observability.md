# Observability

IntegratedS3 now emits structured logs, traces, and metrics by default through the standard .NET observability primitives it already depends on. The platform does not force a specific exporter stack, but hosts can subscribe to the shared `ActivitySource` and `Meter` names exposed by `IntegratedS3.Abstractions`.

## Shared identifiers

`IntegratedS3.Abstractions.Observability.IntegratedS3Observability` is the canonical public surface for host integrations:

- activity source: `IntegratedS3`
- meter: `IntegratedS3`
- correlation header: `x-integrateds3-correlation-id`

## Structured logs

IntegratedS3 uses `ILogger` for structured logs across:

- request authentication and authorization failures
- storage orchestration and provider failover decisions
- replica backlog transitions and repair execution
- backend health probe failures and health-state transitions

The emitted log state is correlation-aware. Depending on the code path, logs include fields such as `CorrelationId`, `RequestId`, `Operation`, `Provider`, `PrimaryProvider`, `ReplicaBackend`, `RepairStatus`, `AuthType`, and `ErrorCode`.

## Traces

IntegratedS3 emits activities from the shared `IntegratedS3` activity source for request/authentication, storage operations, and replica-repair work. Operation activities carry correlation and provider tags so hosts can tie an HTTP request, an authorization decision, and the resulting backend work together inside a single trace.

## Metrics

The shared `IntegratedS3` meter exposes the following metrics:

- HTTP/API:
  - `integrateds3.http.authentication.failures`
  - `integrateds3.http.bytes_received`
  - `integrateds3.http.bytes_sent`
  - `integrateds3.http.request.count`
  - `integrateds3.http.request.duration`
- storage orchestration:
  - `integrateds3.storage.authorization.failures`
  - `integrateds3.storage.operation.count`
  - `integrateds3.storage.operation.duration`
  - `integrateds3.storage.operation.bytes`
- replication and backend health:
  - `integrateds3.replication.repair.duration`
  - `integrateds3.replication.backlog.size`
  - `integrateds3.replication.backlog.oldest_age`
  - `integrateds3.backend.health.status`
- maintenance and protocol:
  - `integrateds3.maintenance.job.duration`
  - `integrateds3.maintenance.job.failures`
  - `integrateds3.protocol.xml.parse_errors`
  - `integrateds3.protocol.signature.errors`
- provider-specific internals:
  - `integrateds3.disk.operation.duration`
  - `integrateds3.disk.operation.errors`
  - `integrateds3.s3.operation.duration`
  - `integrateds3.s3.operation.errors`

Provider, provider-kind, primary-provider, replica-backend, repair-origin, repair-status, operation, result, auth-stage, auth-type, error-code, request-id, and correlation-id tags are attached where they are meaningful for the emitted signal. HTTP metrics also include the normalized request-method and response-status-code tags exported by the chosen telemetry pipeline.

Most latency histograms use milliseconds, backlog age uses seconds, and backend health reports `1` for healthy, `0` for unhealthy, and `-1` for unknown.

## Correlation IDs

For the HTTP surface, IntegratedS3 reuses an inbound `x-integrateds3-correlation-id` header when a caller provides one. If the caller does not provide a correlation header, IntegratedS3 derives one from the current trace or request identifier and echoes the header on the response.

The correlation identifier is also copied into `IntegratedS3RequestContext`, log scopes, activity tags, and metric tags so downstream orchestration and auth logs can be tied back to the original request.

## Replica lag and reconciliation visibility

Replica drift is surfaced in two complementary ways:

- `integrateds3.replication.backlog.size` and `integrateds3.replication.backlog.oldest_age` expose queue depth and oldest outstanding age for each replica backend
- `GET /integrated-s3/admin/repairs` exposes the current repair backlog on the admin HTTP surface

Backend health state is exported through `integrateds3.backend.health.status`, where healthy is `1`, unhealthy is `0`, and unknown is `-1`.

## Host integration

Hosts can wire the shared source and meter into their preferred telemetry stack. For example, a host using OpenTelemetry can subscribe like this:

```csharp
using IntegratedS3.Abstractions.Observability;

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddSource(IntegratedS3Observability.ActivitySourceName))
    .WithMetrics(metrics => metrics.AddMeter(IntegratedS3Observability.MeterName));
```

Choose exporters, processors, and sampling policies at the host level. IntegratedS3 intentionally emits the signals, but does not hardcode a collector or backend choice. The sample `WebUi` host also layers `AddAspNetCoreInstrumentation()` and `AddHttpClientInstrumentation()` on top so standard ASP.NET Core metrics and traces are exported alongside the IntegratedS3-specific signals.

## Grafana dashboard

IntegratedS3 now ships a ready-to-import Grafana dashboard at `docs\grafana\integrateds3-overview-dashboard.json`.

The dashboard is designed for Prometheus-backed OpenTelemetry exports and focuses on the custom IntegratedS3 signals:

- overview stats for request rate, HTTP 5xx rate, request/storage latency, backend health, and repair backlog
- HTTP/API throughput, latency percentiles, auth failures, and response status distribution
- throughput panels for upload/download bytes per second and total data transferred
- storage operation throughput, failure rate, authorization failures, and provider-kind latency
- provider-specific disk and S3 latency/error panels
- replication backlog, oldest outstanding age, repair latency, and backend health views
- maintenance job latency/failures plus protocol XML/signature error panels

Import flow:

1. Export the `IntegratedS3` meter to Prometheus directly or through an OpenTelemetry Collector.
2. Ensure Prometheus scraping adds the usual `job` and `instance` labels.
3. In Grafana, import `docs\grafana\integrateds3-overview-dashboard.json` and select your Prometheus datasource.
4. Use the built-in dashboard variables to filter by job, instance, operation, provider kind, and replica backend.

The dashboard queries intentionally use regex-based PromQL selectors so they tolerate common OpenTelemetry-to-Prometheus normalization differences, including dots becoming underscores, optional `_total` counter suffixes, and histogram/gauge unit suffixes such as `_milliseconds` and `_seconds`. If your telemetry pipeline applies a non-standard translation strategy, adjust the metric name regexes after import.

## Current limitation

IntegratedS3 does not currently map dedicated health endpoints for consumers. Hosts that want `/health` or richer health-check routing should layer that at the host level while reusing the emitted backend-health and repair-backlog telemetry.
