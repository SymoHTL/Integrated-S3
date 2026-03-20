# IntegratedS3 hot-path benchmarks

`src\IntegratedS3\IntegratedS3.Benchmarks` is the supported benchmark harness for the Track H hot paths called out in `docs\integrated-s3-implementation-plan.md`.

It is intentionally scenario-oriented instead of microbenchmark-oriented: the harness exercises the public service and ASP.NET integration surfaces so the reported baselines track the paths adopters actually ship.

## Covered scenarios

The current scenario catalog covers these hot paths:

| Scenario id | Hot path | Shape |
| --- | --- | --- |
| `http-sigv4-head-bucket-auth` | request auth/signature validation | loopback HTTP `HEAD` bucket request with SigV4 auth |
| `disk-head-object-metadata` | metadata lookup | `IStorageService.HeadObjectAsync(...)` against disk |
| `disk-put-object` | object upload | `IStorageService.PutObjectAsync(...)` against disk |
| `disk-get-object` | object download | `IStorageService.GetObjectAsync(...)` against disk |
| `disk-upload-multipart-part` | multipart part upload | `IStorageService.UploadMultipartPartAsync(...)` against disk |
| `disk-complete-multipart-upload` | multipart complete | `IStorageService.CompleteMultipartUploadAsync(...)` against disk |
| `disk-mirror-put-object` | mirrored writes | write-through `PutObjectAsync(...)` across primary + replica disk backends |
| `disk-list-objects` | list operations | `IStorageService.ListObjectsAsync(...)` against disk |
| `service-presign-get-object` | presign generation | `IStoragePresignService.PresignObjectAsync(...)` |
| `http-head-object-metadata` | metadata lookup | loopback HTTP `HEAD` object request with SigV4 auth |
| `http-put-object` | object upload | loopback HTTP `PUT` object request with SigV4 auth |
| `http-upload-multipart-part` | multipart part upload | loopback HTTP `UploadPart` request with SigV4 auth |
| `http-get-object` | object download | loopback HTTP `GET` object request with SigV4 auth |
| `http-list-objects` | list operations | loopback HTTP list request with SigV4 auth |
| `aws-sdk-path-get-object-metadata` | metadata lookup | `AmazonS3Client.GetObjectMetadataAsync(...)` path-style request against the loopback S3-compatible endpoint |
| `aws-sdk-path-put-object` | object upload | `AmazonS3Client.PutObjectAsync(...)` path-style request against the loopback S3-compatible endpoint |
| `aws-sdk-path-upload-multipart-part` | multipart part upload | `AmazonS3Client.UploadPartAsync(...)` path-style request against the loopback S3-compatible endpoint |
| `aws-sdk-path-get-object` | object download | `AmazonS3Client.GetObjectAsync(...)` path-style request against the loopback S3-compatible endpoint |
| `aws-sdk-path-list-objects-v2` | list operations | `AmazonS3Client.ListObjectsV2Async(...)` path-style request against the loopback S3-compatible endpoint |

The disk scenarios provide the representative provider baselines requested by Track H, the loopback HTTP scenarios add the representative host-level `HEAD` / `GET` / `PUT` / multipart `UploadPart` / `LIST` baselines called out in the implementation plan, and the AWS SDK path-style scenarios now provide a broader reproducible repo-local client-comparison slice that covers both metadata lookup and request/response body flows against the S3-compatible endpoint surface.

## Running the benchmarks

Use the checked-in PowerShell wrapper to refresh the committed baseline reports:

```powershell
pwsh -File eng\Invoke-HotPathBenchmarkBaselines.ps1
```

Useful overrides:

```powershell
pwsh -File eng\Invoke-HotPathBenchmarkBaselines.ps1 -WarmupIterations 3 -MeasuredIterations 20
pwsh -File eng\Invoke-HotPathBenchmarkBaselines.ps1 -Scenario http-get-object,disk-put-object
```

The wrapper runs the benchmark project in `Release` and writes:

- `docs\benchmarks\hot-path-baseline.json`
- `docs\benchmarks\hot-path-baseline.md`

## Reported metrics

Each scenario records:

- throughput (`ops/s`, plus MiB/s or logical-items/s when the workload has a stable size)
- latency (`mean`, `p50`, `p95`, `p99`)
- allocations (`mean` and `p95` bytes per operation)
- LOH delta
- temp-file churn (created, deleted, renamed, and net bytes delta inside benchmark-owned roots)
- thread-pool pressure snapshots
- provider breakdown

Provider-backed service scenarios record backend timing through `ProfilingStorageBackend`. Loopback HTTP scenarios bridge the server-side provider timings back to the client-side harness through a benchmark-only response header, and the AWS SDK path-style scenarios read that same header from the SDK response pipeline, so the resulting report can show both backend time and the remaining `application-overhead` time for metadata-only and body-bearing request shapes.

## Limitations

- The committed baselines are machine- and environment-specific snapshots. They are intended for regression tracking, not as hard pass/fail thresholds.
- `p95` and `p99` are only as stable as the configured iteration counts. Increase `-MeasuredIterations` when you need a stricter refresh.
- Temp-file churn only covers benchmark-owned roots. Provider activity outside those roots is intentionally excluded.
- The current provider-breakdown baselines focus on disk, mirrored disk, loopback HTTP over disk, and AWS SDK path-style loopback access over disk, including metadata lookup comparisons. Native-S3 benchmarking can be added later once a reproducible repo-local S3 environment is part of the supported validation story.
