# SeaweedFS comparison, 2026-07-04

An evidence-based performance and stability comparison of SeaweedFS (shallow clone, read),
IntegratedS3 and PersonalS3, done on 2026-07-04: one finder per sub-dimension reading both
sides, each finding re-verified against the code at the time. This page keeps the verdict and
the measured numbers; the gaps it found are issues.

## Numbers

Measured locally on an AMD Ryzen 9 9950X3D (32 threads), .NET 10, BenchmarkDotNet 0.15.2; the
committed baselines are in each repo's `benchmarks/baseline/`.

| System | Measurement | Result |
| --- | --- | --- |
| IntegratedS3, disk provider, in-process, 1 thread | PUT 64 KiB | 5.35 ms |
| | GET 64 KiB | 0.23 ms (the >20x asymmetry is the fsync, #121) |
| | MultipartComplete 3 x 1 MiB | 56 ms (O(size) concatenation) |
| | List 1000 objects | 40 ms (filesystem walk) |
| PersonalS3, local, excluding the Discord network | SQLite list 1000 | 2.5 ms |
| | AES-GCM 8 MiB | about 10 GB/s |
| SeaweedFS, published figures only (i7 MacBook SSD, 1 KiB, concurrency 64) | write | about 5,700 req/s |
| | read | about 13,000 req/s |

PersonalS3's real PUT and GET are Discord-network-bound and are not in these numbers.
`weed benchmark` was not run (no Go toolchain on the box), so the SeaweedFS rows are not the
same hardware and only bound the comparison.

## Verdict

SeaweedFS wins both axes where it counts, mostly by design: small-file needle packing with an
in-RAM index, replication plus 10+4 erasure coding, a CRC on every read, fault-injection tests
and a production track record. IntegratedS3 is a solid embeddable single-node library; every
bug its issue tracker held was verified fixed in the code at the time. PersonalS3 is a
well-engineered novelty whose worst data-corruption bugs are fixed; the remaining gap is the
inherent Discord dependency.

## Filed

IntegratedS3: #238 stream the signed-PUT SHA-256 instead of buffering the whole body in a
`MemoryStream`, #239 manifest-based multipart complete instead of O(size) concatenation, #240
read-time checksum verification on disk GET, #241 startup data-to-catalog reconciliation (reap
orphan `.tmp` files), #242 persist the replica-repair backlog across restarts, #243 execute
lifecycle rules (wire-only today), #244 a fault-injection I/O test backend, #245 push `maxKeys`
and the continuation token into `IStorageCatalogStore.ListObjectsAsync` (the EF catalog still
materialises the whole prefix; not a duplicate of the closed #102 and #138).

PersonalS3: #62 `PutObjectAsync` has no per-key lock, so two `IsCurrent = 1` rows can exist (a
bug), #63 verify SHA-256 on unencrypted reads, #64 a bounded LRU chunk read cache, #65 a
SIGTERM pre-stop drain window, #66 fault injection in `DiscordApiStubServer`, #67 small-object
sub-attachment packing.

Not filed, by design trade-off or low value: a Reed-Solomon erasure-coding mode, synchronous
replica fan-out, a small-object packing backend and hot-object cache, a dual-write chunk
mirror, channel auto-provisioning, a BatchDelete cap, a streaming redistribution consumer.
