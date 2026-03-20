# IntegratedS3 hot-path benchmark baselines

Generated: `2026-03-16T20:51:54.0293755+00:00`

Environment: `Microsoft Windows 10.0.26200` | `.NET 10.0.4` | `X64` | `32` logical processors | server GC: `False`

Configuration: warmup `2`, measured `12`

| Scenario | Hot path | Transport | Topology | p50 ms | p95 ms | p99 ms | Ops/s | MiB/s | Items/s | Alloc KB/op |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| http-sigv4-head-bucket-auth | request auth/signature validation | loopback HTTP | ASP.NET endpoint plus disk backend | 0.864 | 2.326 | 2.326 | 867.622 | - | - | 23.408 |
| disk-head-object-metadata | metadata lookup | in-process service | single disk backend | 0.492 | 1.406 | 1.406 | 1588.668 | 99.292 | - | 11.074 |
| disk-put-object | object upload | in-process service | single disk backend | 25.161 | 28.874 | 28.874 | 38.822 | 38.822 | - | 114.007 |
| disk-get-object | object download | in-process service | single disk backend | 1.708 | 2.878 | 2.878 | 542.689 | 2170.757 | - | 34.155 |
| disk-upload-multipart-part | multipart part upload | in-process service | single disk backend | 23.074 | 27.713 | 27.713 | 42.289 | 84.579 | - | 97.159 |
| disk-complete-multipart-upload | multipart complete | in-process service | single disk backend | 41.719 | 50.611 | 50.611 | 23.913 | 71.739 | - | 161.249 |
| disk-mirror-put-object | mirrored writes | in-process service | write-through primary plus replica disk backends | 57.248 | 63.872 | 63.872 | 17.204 | 17.204 | - | 258.167 |
| disk-list-objects | list operations | in-process service | single disk backend | 41.358 | 52.101 | 52.101 | 23.58 | - | 6036.432 | 1392.622 |
| service-presign-get-object | presign generation | in-process service | first-party HTTP presign strategy | 0.285 | 0.461 | 0.461 | 3241.754 | - | - | 27.455 |
| http-head-object-metadata | metadata lookup | loopback HTTP | ASP.NET endpoint plus disk backend | 1.466 | 2.664 | 2.664 | 622.901 | 38.931 | - | 36.665 |
| http-put-object | object upload | loopback HTTP | ASP.NET endpoint plus disk backend | 27.952 | 42.957 | 42.957 | 31.752 | 31.752 | - | 236.954 |
| http-upload-multipart-part | multipart part upload | loopback HTTP | ASP.NET endpoint plus disk backend | 35.036 | 48.992 | 48.992 | 27.388 | 54.775 | - | 247.846 |
| http-get-object | object download | loopback HTTP | ASP.NET endpoint plus disk backend | 6.128 | 8.314 | 8.314 | 162.647 | 650.587 | - | 76.115 |
| http-list-objects | list operations | loopback HTTP | ASP.NET endpoint plus disk backend | 43.165 | 52.848 | 52.848 | 23.35 | - | 5977.703 | 1474.525 |
| aws-sdk-path-get-object-metadata | metadata lookup | AWS SDK path-style loopback HTTP | ASP.NET S3-compatible endpoint plus disk backend | 1.614 | 2.236 | 2.236 | 602.219 | 37.639 | - | 106.489 |
| aws-sdk-path-put-object | object upload | AWS SDK path-style loopback HTTP | ASP.NET S3-compatible endpoint plus disk backend | 31.445 | 35.19 | 35.19 | 31.214 | 31.214 | - | 324.26 |
| aws-sdk-path-upload-multipart-part | multipart part upload | AWS SDK path-style loopback HTTP | ASP.NET S3-compatible endpoint plus disk backend | 36.632 | 47.955 | 47.955 | 26.803 | 53.606 | - | 365.043 |
| aws-sdk-path-get-object | object download | AWS SDK path-style loopback HTTP | ASP.NET S3-compatible endpoint plus disk backend | 18.208 | 52.387 | 52.387 | 44.6 | 178.4 | - | 184.568 |
| aws-sdk-path-list-objects-v2 | list operations | AWS SDK path-style loopback HTTP | ASP.NET S3-compatible endpoint plus disk backend | 49.962 | 59.637 | 59.637 | 19.709 | - | 5045.381 | 2612.629 |

## Provider breakdown

### `http-sigv4-head-bucket-auth`

- Workload: Send a SigV4-signed HEAD bucket request through the loopback HTTP host.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `0.1` / `0.156` / `0.156`
  - `application-overhead` mean/p95/p99 ms: `1.052` / `2.169` / `2.169`

### `disk-head-object-metadata`

- Workload: Head an existing disk-backed object through IStorageService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `0.548` / `1.321` / `1.321`
  - `application-overhead` mean/p95/p99 ms: `0.082` / `0.103` / `0.103`

### `disk-put-object`

- Workload: Overwrite a 1 MiB object through the disk-backed IStorageService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `2` / `2` / `2`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `25.645` / `28.785` / `28.785`
  - `application-overhead` mean/p95/p99 ms: `0.113` / `0.182` / `0.182`

### `disk-get-object`

- Workload: Stream a 4 MiB disk-backed object through IStorageService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `0.463` / `1.142` / `1.142`
  - `application-overhead` mean/p95/p99 ms: `1.38` / `2.311` / `2.311`

### `disk-upload-multipart-part`

- Workload: Upload a 2 MiB multipart part against an existing disk-backed upload.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `1` / `1` / `1`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `23.56` / `27.631` / `27.631`
  - `application-overhead` mean/p95/p99 ms: `0.087` / `0.156` / `0.156`

### `disk-complete-multipart-upload`

- Workload: Complete a three-part disk-backed multipart upload after the parts are staged.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `2` / `8` / `2`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `41.754` / `50.547` / `50.547`
  - `application-overhead` mean/p95/p99 ms: `0.065` / `0.088` / `0.088`

### `disk-mirror-put-object`

- Workload: Write-through overwrite of a 1 MiB object across primary and replica disk backends.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `4` / `4` / `4`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `24.244` / `27.224` / `27.224`
  - `disk-replica` mean/p95/p99 ms: `24.322` / `28.184` / `28.184`
  - `application-overhead` mean/p95/p99 ms: `9.56` / `11.123` / `11.123`

### `disk-list-objects`

- Workload: Enumerate 256 disk-backed objects through IStorageService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `disk-primary` mean/p95/p99 ms: `42.349` / `51.979` / `51.979`
  - `application-overhead` mean/p95/p99 ms: `0.06` / `0.122` / `0.122`

### `service-presign-get-object`

- Workload: Generate a first-party GET-object presigned URL via IStoragePresignService.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `application-overhead` mean/p95/p99 ms: `0.308` / `0.461` / `0.461`

### `http-head-object-metadata`

- Workload: Send a SigV4-signed HEAD object request through the loopback HTTP host.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `0.395` / `0.714` / `0.714`
  - `application-overhead` mean/p95/p99 ms: `1.211` / `2.301` / `2.301`

### `http-put-object`

- Workload: Send a SigV4-signed 1 MiB PUT object request through the loopback HTTP host.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `2` / `2` / `2`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `29.969` / `40.95` / `40.95`
  - `application-overhead` mean/p95/p99 ms: `1.525` / `2.757` / `2.757`

### `http-upload-multipart-part`

- Workload: Send a SigV4-signed 2 MiB UploadPart request through the loopback HTTP host.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `2` / `2` / `2`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `35.369` / `47.23` / `47.23`
  - `application-overhead` mean/p95/p99 ms: `1.144` / `1.763` / `1.763`

### `http-get-object`

- Workload: Send a SigV4-signed GET object request through the loopback HTTP host and drain the response body.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `0.416` / `0.604` / `0.604`
  - `application-overhead` mean/p95/p99 ms: `5.732` / `7.843` / `7.843`

### `http-list-objects`

- Workload: Send a SigV4-signed HTTP list-objects request and drain the serialized response body.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `41.339` / `51.66` / `51.66`
  - `application-overhead` mean/p95/p99 ms: `1.487` / `2.335` / `2.335`

### `aws-sdk-path-get-object-metadata`

- Workload: Fetch object metadata through AmazonS3Client.GetObjectMetadataAsync(...) against the loopback S3-compatible endpoint.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `0.331` / `0.477` / `0.477`
  - `application-overhead` mean/p95/p99 ms: `1.329` / `1.983` / `1.983`

### `aws-sdk-path-put-object`

- Workload: Upload a 1 MiB object through AmazonS3Client against the loopback S3-compatible endpoint.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `2` / `2` / `2`
- Thread-pool pressure (max pending/thread delta): `0` / `1`
  - `http-disk-primary` mean/p95/p99 ms: `25.929` / `28.751` / `28.751`
  - `application-overhead` mean/p95/p99 ms: `6.108` / `6.981` / `6.981`

### `aws-sdk-path-upload-multipart-part`

- Workload: Upload a 2 MiB multipart part through AmazonS3Client against the loopback S3-compatible endpoint.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `1` / `1` / `1`
- Thread-pool pressure (max pending/thread delta): `1` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `26.417` / `35.263` / `35.263`
  - `application-overhead` mean/p95/p99 ms: `10.892` / `12.691` / `12.691`

### `aws-sdk-path-get-object`

- Workload: Download and drain a 4 MiB object through AmazonS3Client against the loopback S3-compatible endpoint.
- LOH delta (mean/max bytes): `0` / `0`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `0.465` / `0.903` / `0.903`
  - `application-overhead` mean/p95/p99 ms: `21.956` / `52.05` / `52.05`

### `aws-sdk-path-list-objects-v2`

- Workload: List 256 objects through AmazonS3Client against the loopback S3-compatible endpoint.
- LOH delta (mean/max bytes): `25456` / `305472`
- Temp-file churn (created/deleted/renamed mean): `0` / `0` / `0`
- Thread-pool pressure (max pending/thread delta): `0` / `0`
  - `http-disk-primary` mean/p95/p99 ms: `46.838` / `54.134` / `54.134`
  - `application-overhead` mean/p95/p99 ms: `3.902` / `6.725` / `6.725`

## Notes

- Latency, throughput, and allocation values are regression-oriented baselines from the configured iteration counts, not pass/fail thresholds.
- Temp-file churn is collected from FileSystemWatcher events scoped to benchmark-owned roots, so provider work outside those roots is intentionally excluded.
- Provider breakdown adds an 'application-overhead' bucket for non-provider time inside the measured path.
