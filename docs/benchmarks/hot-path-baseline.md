# IntegratedS3 hot-path benchmark baselines

Generated: `2026-03-14T09:46:23.7416501+00:00`

Environment: `Microsoft Windows 10.0.26200` | `.NET 10.0.3` | `X64` | `32` logical processors | server GC: `False`

Configuration: warmup `2`, measured `12`

| Scenario | Hot path | Transport | Topology | p50 ms | p95 ms | p99 ms | Ops/s | MiB/s | Items/s | Alloc KB/op |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| http-sigv4-head-bucket-auth | request auth/signature validation | loopback HTTP | ASP.NET endpoint plus disk backend | 0.713 | 0.96 | 0.96 | 1332.697 | - | - | 21.11 |
| disk-head-object-metadata | metadata lookup | in-process service | single disk backend | 0.348 | 0.562 | 0.562 | 2702.824 | 168.927 | - | 8.125 |
| disk-put-object | object upload | in-process service | single disk backend | 16.021 | 16.993 | 16.993 | 62.867 | 62.867 | - | 108.163 |
| disk-get-object | object download | in-process service | single disk backend | 1.156 | 1.417 | 1.417 | 876.821 | 3507.285 | - | 31.461 |
| disk-upload-multipart-part | multipart part upload | in-process service | single disk backend | 15.495 | 16.055 | 16.055 | 64.82 | 129.639 | - | 94.424 |
| disk-complete-multipart-upload | multipart complete | in-process service | single disk backend | 27.449 | 29.226 | 29.226 | 36.548 | 109.645 | - | 128.932 |
| disk-mirror-put-object | mirrored writes | in-process service | write-through primary plus replica disk backends | 35.768 | 39.33 | 39.33 | 27.891 | 27.891 | - | 230.788 |
| disk-list-objects | list operations | in-process service | single disk backend | 24.234 | 32.166 | 32.166 | 38.927 | - | 9965.226 | 1352.487 |
| service-presign-get-object | presign generation | in-process service | first-party HTTP presign strategy | 0.238 | 0.375 | 0.375 | 4301.538 | - | - | 26.167 |
| http-put-object | object upload | loopback HTTP | ASP.NET endpoint plus disk backend | 17.352 | 33.076 | 33.076 | 50.497 | 50.497 | - | 230.243 |
| http-get-object | object download | loopback HTTP | ASP.NET endpoint plus disk backend | 3.508 | 4.315 | 4.315 | 269.819 | 1079.277 | - | 52.716 |
| http-list-objects | list operations | loopback HTTP | ASP.NET endpoint plus disk backend | 24.872 | 30.136 | 30.136 | 39.058 | - | 9998.838 | 1429.378 |

## Provider breakdown

### `http-sigv4-head-bucket-auth`

- Workload: Send a SigV4-signed HEAD bucket request through the loopback HTTP host.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `0.083` / `0.131` / `0.131`
  - `application-overhead` mean/p95/p99 ms: `0.668` / `0.854` / `0.854`

### `disk-head-object-metadata`

- Workload: Head an existing disk-backed object through IStorageService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `1` / `0`
  - `disk-primary` mean/p95/p99 ms: `0.336` / `0.515` / `0.515`
  - `application-overhead` mean/p95/p99 ms: `0.034` / `0.056` / `0.056`

### `disk-put-object`

- Workload: Overwrite a 1 MiB object through the disk-backed IStorageService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `2` / `2` / `2`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `15.863` / `16.959` / `16.959`
  - `application-overhead` mean/p95/p99 ms: `0.044` / `0.057` / `0.057`

### `disk-get-object`

- Workload: Stream a 4 MiB disk-backed object through IStorageService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `0.346` / `0.512` / `0.512`
  - `application-overhead` mean/p95/p99 ms: `0.795` / `1.06` / `1.06`

### `disk-upload-multipart-part`

- Workload: Upload a 2 MiB multipart part against an existing disk-backed upload.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `1` / `1` / `1`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `15.392` / `16.024` / `16.024`
  - `application-overhead` mean/p95/p99 ms: `0.035` / `0.043` / `0.043`

### `disk-complete-multipart-upload`

- Workload: Complete a three-part disk-backed multipart upload after the parts are staged.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `2` / `8` / `2`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `27.329` / `29.204` / `29.204`
  - `application-overhead` mean/p95/p99 ms: `0.032` / `0.05` / `0.05`

### `disk-mirror-put-object`

- Workload: Write-through overwrite of a 1 MiB object across primary and replica disk backends.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `4` / `4` / `4`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `15.332` / `16.339` / `16.339`
  - `disk-replica` mean/p95/p99 ms: `15.283` / `17.072` / `17.072`
  - `application-overhead` mean/p95/p99 ms: `5.239` / `5.918` / `5.918`

### `disk-list-objects`

- Workload: Enumerate 256 disk-backed objects through IStorageService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `25.656` / `32.137` / `32.137`
  - `application-overhead` mean/p95/p99 ms: `0.033` / `0.053` / `0.053`

### `service-presign-get-object`

- Workload: Generate a first-party GET-object presigned URL via IStoragePresignService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `application-overhead` mean/p95/p99 ms: `0.232` / `0.375` / `0.375`

### `http-put-object`

- Workload: Send a SigV4-signed 1 MiB PUT object request through the loopback HTTP host.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `2` / `2` / `2`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `18.882` / `32.124` / `32.124`
  - `application-overhead` mean/p95/p99 ms: `0.921` / `1.02` / `1.02`

### `http-get-object`

- Workload: Send a SigV4-signed GET object request through the loopback HTTP host and drain the response body.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `0.276` / `0.419` / `0.419`
  - `application-overhead` mean/p95/p99 ms: `3.43` / `4.086` / `4.086`

### `http-list-objects`

- Workload: Send a SigV4-signed HTTP list-objects request and drain the serialized response body.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `24.379` / `28.808` / `28.808`
  - `application-overhead` mean/p95/p99 ms: `1.224` / `1.66` / `1.66`

## Notes

- Latency, throughput, and allocation values are regression-oriented baselines from the configured iteration counts, not pass/fail thresholds.
- Temp-file churn is collected from FileSystemWatcher events scoped to benchmark-owned roots, so provider work outside those roots is intentionally excluded.
- Provider breakdown adds an 'application-overhead' bucket for non-provider time inside the measured path.
