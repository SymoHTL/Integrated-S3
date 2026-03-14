# Optional host maintenance jobs

IntegratedS3 now exposes an opt-in host integration for recurring maintenance work without forcing Core or providers into a mandatory always-on background-service architecture.

The entry points live in `IntegratedS3.AspNetCore`:

- `IIntegratedS3MaintenanceJob`
- `IntegratedS3MaintenanceJobNames`
- `AddIntegratedS3MaintenanceJob(...)`

## Register a maintenance job

Hosts can register either a DI-resolved job type or a delegate. Both options run as isolated `IHostedService` loops, create a fresh DI scope per execution, and bind per-job scheduling options from configuration when needed.

```csharp
builder.Services.AddIntegratedS3MaintenanceJob<ReplicaRepairReplayJob>(
    IntegratedS3MaintenanceJobNames.MirrorReplay,
    builder.Configuration.GetSection("IntegratedS3:Maintenance:MirrorReplay"),
    options => {
        options.RunOnStartup = true;
        options.Interval = TimeSpan.FromMinutes(1);
    });

builder.Services.AddIntegratedS3MaintenanceJob(
    IntegratedS3MaintenanceJobNames.MultipartCleanup,
    builder.Configuration.GetSection("IntegratedS3:Maintenance:MultipartCleanup"),
    async (serviceProvider, cancellationToken) => {
        var backends = serviceProvider.GetServices<IStorageBackend>();

        foreach (var backend in backends) {
            await foreach (var bucket in backend.ListBucketsAsync(cancellationToken)) {
                await foreach (var upload in backend.ListMultipartUploadsAsync(
                                   new ListMultipartUploadsRequest { BucketName = bucket.Name },
                                   cancellationToken)) {
                    if (upload.InitiatedAtUtc < DateTimeOffset.UtcNow.AddHours(-24)) {
                        await backend.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                        {
                            BucketName = upload.BucketName,
                            Key = upload.Key,
                            UploadId = upload.UploadId
                        }, cancellationToken);
                    }
                }
            }
        }
    });
```

Each named job uses `IntegratedS3MaintenanceJobOptions`:

- `Enabled`
- `RunOnStartup`
- `Interval`

## Recommended job mappings

The well-known names in `IntegratedS3MaintenanceJobNames` map to the Track F maintenance backlog row:

- `MirrorReplay`
  - build around `IStorageReplicaRepairBacklog`, provider descriptors, and any host-specific replay/remediation logic
  - the current backlog records divergence visibility, but it does **not** yet persist enough operation-specific intent to deliver a completely generic durable replay engine after process restarts
- `OrphanDetection`
  - compare `IStorageCatalogStore` snapshots with one or more `IStorageBackend` listings and emit findings or cleanup actions through host-owned policy
- `ChecksumVerification`
  - use catalog/object state plus backend `HeadObjectAsync` or object-version reads to compare checksum/etag drift
- `MultipartCleanup`
  - list stale uploads through `IStorageBackend.ListMultipartUploadsAsync(...)` or a platform-managed `IStorageMultipartStateStore`, then abort/remove old state explicitly
- `IndexCompaction`
  - schedule compaction on the consumer-owned catalog/index implementation (for example an EF-backed store or another persistence layer)
- `ExpiredArtifactCleanup`
  - schedule deletion of host-owned temp files, staging artifacts, export caches, or other operational scratch storage that lives outside Core/provider contracts

## Supported boundary

This hosting layer is intentionally about **composition**, scheduling, and DI scoping. It does not claim that every maintenance workflow is already fully solved inside Core:

- durable generic mirror replay still needs richer persisted repair intent than the current backlog metadata carries
- orphan detection, checksum verification, index compaction, and expired-artifact cleanup stay host-policy-driven because they depend on the consumer's chosen catalog, persistence, and artifact ownership model
- multipart cleanup is a good fit for the new job host because the public backend surface already exposes listing/abort hooks

Use these helpers when you want the app host to own maintenance execution. If you prefer Hangfire, Quartz, CronJobs, or another scheduler, reuse the same job classes/delegates there instead of relying on the built-in hosted-service loop.
