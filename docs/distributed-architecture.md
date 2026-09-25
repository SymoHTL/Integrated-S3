# Distributed architecture

**Status: proposal, 2026-09-25.** Open work and the owner's decisions are tracked in the epic
(#288). This page is the design; it holds no status. The section "Where the code stands" is a
snapshot of `main` at 707bcaf and is not maintained; everything after it is the design and changes
through PRs like any other doc.

## Goal

IntegratedS3 should run as a production S3 server that scales out: any number of stateless nodes
behind a load balancer, strongly consistent like AWS S3, fast on small and large objects, and
durable across node crashes. It stays an embeddable ASP.NET library, and it serves every consumer,
PersonalS3 included, without code, options or branches written for one consumer.

## Where the code stands (snapshot at 707bcaf)

Every invariant that makes S3 semantics correct is held in process memory today, or by nothing:

| Invariant | What holds it | With two processes |
|---|---|---|
| Per-key atomicity: `If-None-Match`/`If-Match`, version archiving, Complete vs Abort, same-part uploads | 256 in-process `SemaphoreSlim` stripes (`DiskStorageService.cs:53-58, 7314-7323`) | Not held: two creates both win, versions are lost (#84's class), one writer's metadata lands on the other's bytes |
| Which version is "latest" (Disk + EF catalog) | Catalog rows written by the provider, then again by the orchestrator after the call and on every HEAD and every listed key (`OrchestratedStorageService.cs:296, 305, 472, 642`) | Not held, and not reliably even in one process |
| Bucket configuration | Read-modify-write of the whole `.integrateds3.bucket.json` under a stripe | Concurrent changes to different settings lose one of them |
| ACLs and bucket policies | `ConcurrentDictionary` in a singleton (`InMemoryStorageAuthorizationCompatibilityService.cs:14-15`) | Different on every node, gone after a restart |
| Replica repair backlog | In memory, nothing replays it (#242, #275) | Each node sees only its own backlog |
| Maintenance jobs | A timer per job per process, no lease | Every node runs every job |
| Version order, Object Lock clock | UUIDv7 ids and the node's wall clock | Order and retention depend on clock skew |
| EF catalog schema | `EnsureCreated` (#272), tested on SQLite only | No migrations; other databases untested |

Only the S3 provider behaves correctly with several processes, because upstream S3 does the
atomicity. Making the Disk provider and the orchestrator distributed would mean rebuilding each of
these invariants around shared state, one at a time. This design moves all of them into one
transactional store instead.

## The design in one paragraph

A new engine implements S3 semantics once, on two contracts: a transactional **metadata store**
(SQLite for a single node, PostgreSQL for a cluster) and a write-once **blob store** (local disk, a
shared filesystem, any S3-compatible store, a consumer's own store, and later the nodes' own disks).
Object bytes are written first and never modified; an operation takes effect when one metadata
transaction commits. API nodes hold no state that matters, so they scale out behind any load
balancer. Background work runs from a job table with leases, so any node can do it and a crashed
node's work is picked up by another. Azure Storage, GFS/Colossus, Meta's Tectonic, Ceph RGW's bucket
index and the SeaweedFS filer share this split between a metadata layer and immutable data.

## Architecture

```
clients --> any load balancer (no sticky sessions)
              |
   node 1 ... node N        Kestrel, SigV4, the engine; caches only
      |---> metadata store  SQLite (single node) | PostgreSQL (cluster)
      '---> blob store      local disk | shared filesystem | S3-compatible | consumer store
                            | native volumes (phase 4)
```

### Contracts

- **`IMetadataStore`**: transactional operations shaped like S3, not generic CRUD. Commit a version,
  delete, list one page, create and complete and abort an upload, commit a part, buckets and their
  configuration, jobs, leases, and the garbage-collection queue. It is narrow on purpose, so each
  implementation can run each operation as one transaction, ideally in one round trip.
- **`IBlobStore`**: write-once blobs. Write a stream and get back an opaque locator, the size and
  the digests; open a locator for reading, optionally a byte range; delete (idempotent); enumerate
  from a resumable cursor, for the orphan sweep. It never overwrites, renames or edits a blob, so a
  shared filesystem needs no locking at all.
- **The engine**: versioning, preconditions, multipart, copy, tagging, Object Lock, lifecycle and
  garbage collection, written once on top of the two contracts.

### How it enters the code base

- The engine registers as one more `IStorageBackend` (`AddIntegratedS3Engine(...)`, like
  `AddDiskStorage`), and the orchestrator runs it in its default `PrimaryOnly` mode with no catalog.
  The endpoints, DI, and `StorageProviderContractTests` apply from the first day.
- Everything is added and nothing is removed, so no major version is needed until something is
  retired. A later step can register the engine as the `IStorageService` directly, skipping the
  orchestrator; that registration is already replaceable.
- **Proxy mode stays**: the S3 provider keeps working as a pure proxy in front of S3, with no
  metadata store, for hosts that want exactly that.
- **Packages**, as a proposal for the table in `LayeringConventionTests`: the two contracts go into
  `IntegratedS3.Abstractions`, so a third-party store needs only that package. The engine is a
  provider package (Abstractions and Protocol only) and carries the local-disk blob store. The S3
  blob store goes into `IntegratedS3.Provider.S3` to reuse its client. The metadata stores are
  `IntegratedS3.Metadata.Sqlite` and `IntegratedS3.Metadata.PostgreSql`, on raw ADO.NET (see
  "Metadata").

### Deployment shapes

1. **Single node**: SQLite and local disk. This replaces the Disk provider for new deployments.
2. **Cluster**: PostgreSQL and a shared blob store (an S3-compatible service, an NFS or SMB share,
   or a consumer store), with N stateless nodes behind a load balancer.
3. **Own storage cluster** (phase 4): PostgreSQL, plus volumes on the nodes' own disks, with no
   external object store.

## Consistency contract

This is what AWS S3 guarantees today, and what the gates below check.

1. **Objects are linearizable per key.** Every change takes effect at its metadata commit, and the
   client gets its response after the commit. Read-after-write, overwrite and delete are strongly
   consistent on every node.
2. **LIST is strongly consistent**: it reads the index, with no cache. A page reflects every write
   that committed before it started; pagination is not a snapshot, as in S3.
3. **Preconditions are atomic across the cluster.** `If-None-Match: *` and `If-Match` on PUT,
   Complete, Copy and DELETE are evaluated inside the commit transaction.
4. **A GET reads one snapshot.** It resolves the version once and streams bytes that never change,
   so a concurrent overwrite cannot mix two versions into one response.
5. **Version order within a key is commit order.** It comes from a per-key counter (`head.seq + 1`)
   taken while the key's row lock is held. Not a global sequence: in PostgreSQL, a `nextval` inside
   an `INSERT … VALUES` runs before the `ON CONFLICT` lock wait, so a writer that waited could get a
   smaller number than the one it waited for.
6. **Time comes from the database, inside SQL**: LastModified, Object Lock and lifecycle ages, for
   example `retain_until > now()` in the delete transaction. A node's clock is used only for the
   SigV4 clock-skew window, which needs NTP, as with AWS.
7. **Bucket configuration is stale for at most T seconds** (proposed T = 5): CORS, policy, ACLs,
   lifecycle and the rest are cached per node, invalidated by `LISTEN`/`NOTIFY`, with a TTL as the
   safety net and a full flush after any reconnect. AWS documents bucket configuration as eventually
   consistent too. Bucket existence and versioning state are read inside every object-write
   transaction, so writes never act on a stale value.

## Metadata

### Tables (PostgreSQL; SQLite mirrors them)

| Table | Holds |
|---|---|
| `buckets` | One row per bucket: name (unique), versioning state, Object Lock flag, owner, creation time |
| `bucket_configs(bucket_id, kind, doc)` | One row per configuration kind, ACLs and policy included, so changes to different kinds never overwrite each other |
| `object_heads(bucket_id, key, seq, version_id, delete_marker, size, etag, last_modified, …)` | One row per key: the LIST index and the per-key lock |
| `object_versions(bucket_id, key, seq, version_id, manifest or inline data, size, etag, checksums, headers, user metadata, tags, retention, legal hold)` | One row per version |
| `uploads`, `upload_parts(upload_id, part_no, blob_ref, size, md5, checksums)` | Multipart state |
| `gc_queue(blob_ref, not_before)` | Blobs to delete once no reader can still hold them |
| `jobs`, `leases(name, owner, fence, until)`, `nodes` | Background work, single-owner work, membership |

Keys are stored as `bytea`: byte order is S3's UTF-8 binary order with no collation involved, and a
NUL byte is allowed. A manifest is the ordered list of `(blob locator, offset, length)` extents that
make up an object.

### Operations

- **PUT**
  1. Stream the body to the blob store, computing the digests on the way. No lock is held.
  2. In one transaction: take a shared advisory lock on the bucket and read the bucket row; run
     `INSERT … ON CONFLICT (bucket_id, key) DO UPDATE … WHERE <precondition>`, which takes the
     key's row lock and checks the precondition against the latest committed row; insert the
     version row; in an unversioned or suspended bucket, delete the replaced null version and queue
     its blob for garbage collection.
  3. Commit, then respond.

  The key's lock is held for two or three round trips, never while the body is still arriving.
- **Racing `If-None-Match: *`** on two nodes: exactly one commits, the other gets 412.
- **DELETE** has the same shape: it writes a delete marker or removes a version, and queues freed
  blobs. Object Lock, per-object retention and legal hold included, is checked in SQL against the
  database's `now()`.
- **UploadPart**: write the blob, verify any client checksum, then upsert the part row. The last
  writer wins, as in S3, and the replaced blob goes to garbage collection.
- **CompleteMultipartUpload**, one transaction: lock the upload row (`FOR UPDATE`) and check it is
  still active; validate the parts from their rows; compute the ETag from the stored part MD5s;
  insert a version whose manifest lists the part blobs; mark the upload completed. No byte is
  copied, so the cost grows with the number of parts, not the object size (#239's goal). Complete
  racing Abort, or two Completes, serialize on the upload row, and exactly one wins.
- **CopyObject**: a physical copy first (server-side when the blob store offers it); later a shared
  manifest with a reference count updated in the same transaction.
- **ListObjectsV2**: a range scan over live heads in key order that stops after max-keys + 1. With
  a delimiter it skips ahead: at `photos/2024/` it emits that common prefix and seeks to the first key
  after the prefix. A recursive query produces a whole page in one round trip, so the cost grows with
  the page, not the bucket.
- **ListObjectVersions**: versions in key order, newest first, which is S3's order. A version-id
  marker is resolved to a position by one lookup.
- **DeleteBucket, versioning changes, Object Lock configuration**: take the bucket's advisory lock
  exclusively, then check emptiness (heads, versions, active uploads). No PUT can land in a bucket
  being deleted, and no PUT runs half under the old versioning mode. Advisory locks live in shared
  memory and write nothing to table rows. A foreign key or `FOR SHARE` on the bucket row would do the
  same job, but many concurrent transactions locking one hot parent row cause MultiXact contention in
  PostgreSQL.
- **CreateBucket**: the unique name decides, so exactly one create wins across the cluster.

### Small objects

Objects up to a threshold (about 8 KiB, to be set by benchmark) are stored inline in the version
row. A PUT is then one transaction and a GET one query, with no blob I/O at all. This is the main
lever for small-object throughput.

### Schema and access

- Versioned SQL migrations run at startup under an advisory lock, replacing `EnsureCreated`. Each
  change adds first and removes only in a later release (expand, then contract), so old and new
  nodes run side by side during a rolling upgrade.
- The stores use raw ADO.NET (Npgsql, Microsoft.Data.Sqlite) rather than EF: the locking SQL is
  exact, statements are batched into one round trip, and the packages stay AOT- and trim-clean.
- SQLite has one writer at a time (`BEGIN IMMEDIATE`), so it needs no advisory locks; it passes the
  same contract suite as PostgreSQL.

## Blob stores

A blob store declares what it can do, and the engine adapts; it never checks which store it has.

- **Maximum blob size.** The engine splits larger bodies into several blobs and lists them in the
  manifest, which multipart already needs anyway.
- **Range reads.** When a store cannot serve a byte range, the engine reads from the start of the
  blob and skips.
- **Locators** are opaque and assigned by the store at write time. A store whose read URLs expire
  resolves a fresh URL itself; the engine only keeps the locator.
- **Throttling.** A store that answers "slow down" makes the engine return 503 `SlowDown` to the
  client and back off in background jobs.
- **Enumeration** is resumable from a cursor, so an orphan sweep over a slow or rate-limited store
  can run in small steps.
- **Encryption at rest** is an engine-level decorator (envelope encryption: a data key per object,
  stored in the version row and wrapped by a master key or KMS), so every store gets it. A store may
  also encrypt on its own.

## Consumers

IntegratedS3 contains no code, option or branch for a particular consumer. What a consumer needs
becomes a property of the generic contracts.

**Gates.** The blob-store contract suite runs in CI against a constrained in-memory store: an
8 MiB maximum blob size, store-assigned locators, injected throttling, slow resumable enumeration
and rate-limited deletes. A convention test fails when a file under `src/` outside tests and samples
names a consumer. A consumer runs the same suite against its own store in its own CI.

**PersonalS3 as the worked example.** It already has this shape: metadata in SQLite, chunks in
Discord messages.
- Its Discord storage becomes an `IBlobStore`: messages hold the blobs, the store assigns the
  locators and refreshes its expiring URLs.
- Its metadata moves to the SQLite metadata store through a one-time import. The engine offers one
  generic bulk-import path, which the Disk-layout importer uses too.
- Multipart expiry and the orphan sweep become engine jobs. Store-specific upkeep, such as URL
  refresh and channel redistribution, registers through the same generic job API and gets leases
  for free.
- Its encryption either stays inside its store or moves to the engine's decorator.
- Its rate-limit state stays inside its store. Running it as a cluster would need that store to
  coordinate its own quotas across nodes; the engine knows nothing about them.

## Crash safety and garbage collection

- The blob is written before the commit. A crash in between leaves an unreferenced blob, never a
  row that points at a missing blob.
- An orphan sweep job deletes unreferenced blobs older than a grace period G. The commit refuses a
  blob whose write started more than G ago (by the database clock), so the sweep can never delete a
  blob that a slow upload is about to commit.
- Garbage collection deletes a queued blob only after its `not_before`, which is at least as long
  as the longest GET that may still be streaming it. A reader that resolved an old manifest never
  hits a deleted blob.
- A point-in-time restore of the metadata is safe only as far back as the garbage-collection grace
  period.

## Coordination

PostgreSQL is the only consensus in the system; everything else derives from it.

- **Jobs**: a `jobs` table. A node claims work with `FOR UPDATE SKIP LOCKED`, holds a lease with a
  fencing token, and retries with backoff. Jobs run garbage collection, the orphan sweep, multipart
  expiry, lifecycle rules (#243), bucket replication and scrubbing. Any node can take any job; when
  a node dies its lease expires and another node carries on. This replaces the in-memory repair
  backlog, its dispatcher, and the lease-less maintenance scheduler.
- **Single-owner work**: a lease row whose fencing number only grows. Every write checks the number,
  so a leader that was paused and resumes late cannot overwrite newer work.
- **Caches**: invalidated through `LISTEN`/`NOTIFY`, bounded by a TTL, flushed on reconnect, so a
  lost notification costs at most T seconds of staleness.
- **Connections**: advisory locks are only held for one transaction, so PgBouncer in transaction
  mode works. Each node keeps one dedicated connection for `LISTEN`.

## Node behaviour in cluster mode

- **Readiness** means "this node reaches the metadata store and the blob store". It does not depend
  on the health of other backends.
- **Identity**: the node id is part of `x-amz-id-2` and of every metric. Cluster-wide gauges, such as
  the job backlog and garbage-collection debt, come from the database.
- **Presigning** reads keys from the same reloadable source as authentication, and builds URLs from
  the configured public base URL or forwarded headers when behind a proxy.
- **Credentials** can live in the database, cached per node and invalidated by `NOTIFY`.
- **Kestrel limits** (minimum body data rate, connection limits, timeouts) are set explicitly.

## Performance

Targets are set as SLOs with the owner before phase 3; the levers, in order of production impact:

1. **Streaming uploads.** A signed SHA-256 is checked while the body streams, and the commit happens
   only after the check passes (#238). aws-chunked bodies are decoded as a stream instead of being
   spooled to a temporary file first.
2. **Hash once**: MD5 for the ETag, plus the checksum the client asked for or one default, using
   hardware CRC instructions.
3. **Zero-copy multipart**, with the part digests stored at UploadPart.
4. **LIST cost grows with the page**, because it reads the index.
5. **No per-request directory work**: keys are rows, not file paths.
6. **GET**: one indexed read (or the inline data), then the blob read straight into the response's
   `PipeWriter` buffers in large segments.
7. **Round trips**: one per GET or HEAD, one transaction per write, statements batched and prepared,
   and no catalog writes on read paths.
8. **CPU per request**: SigV4 signing keys cached per (secret, date, region, service), XML written
   straight to the response as UTF-8, source-generated logging.
9. **Durability without an fsync storm**: every blob is flushed before its commit; small inline
   objects ride PostgreSQL's group commit; phase-4 volumes group their flushes.
10. **Runtime**: the server image is benchmarked as JIT with dynamic PGO and ReadyToRun against Native
    AOT. The libraries stay AOT-compatible either way.

Reference points: a 64 KiB Disk PUT takes 5.35 ms today, about 187 PUTs per second per writer
(`benchmarks/baseline`). SeaweedFS publishes about 5,700 writes and 13,000 reads per second for
1 KiB objects at 64 concurrent clients (`seaweedfs-comparison-2026-07-04.md`). With this design,
large-object throughput is bound by network and disks, and small-object throughput by metadata
transactions per second.

## Own storage cluster (phases 4 and 5)

The unit of placement, replication and repair is a **volume**: an append-only file of about 1-4 GiB,
stored on three nodes in different failure domains. A blob locator is (volume, offset, length,
CRC32C). Large bodies are split into chunks of tens of MiB spread over volumes, which gives parallel
I/O and small retry units.

- **Placement lives in the metadata store** (`volumes(id, state, replicas, primary, epoch,
  sealed_length)`), cached on every node. A 20 TB node holds about 5,000 volumes of 4 GiB, so losing
  a node updates thousands of volume rows, not millions of object rows.
- **Appending**: the volume's primary holds a lease and an epoch from the database and assigns
  offsets. It copies each append to the other replicas, flushes in groups, and acknowledges only
  when all three copies are durable. The metadata commits only after that, so every blob the
  metadata points to is on three nodes.
- **When a replica fails**, the volume is not repaired while open. It is **sealed** at the shortest
  durable length among the survivors, which every acknowledged append lies below, and writing
  continues in a new volume on healthy nodes. Replicas never diverge at the tail, and the data path
  needs no consensus protocol; this is the technique of Azure Storage's stream layer. Writes keep
  working while three healthy nodes exist.
- **Repair** copies a sealed, under-replicated volume from a survivor, checks its CRC and updates the
  replica list. It waits about 15 minutes first, to tell a reboot from a dead node.
- **Reads** go to any copy, the local zone first, with a CRC check per blob. A mismatch reads another
  copy and queues a repair; a background scrub does the same checks proactively (#240's class).
- **Deletes** leave garbage in sealed volumes. Compaction rewrites the live blobs, swaps their
  locators with a compare-and-swap on the old value, and drops the old volume after the grace
  period. A new node receives sealed volumes, with a bandwidth cap.
- **Between nodes**: HTTP on Kestrel first; a binary protocol only if measurements ask for it.
- **Phase 5** erasure-codes sealed volumes. With Reed-Solomon 10+4 the storage overhead is 1.4x
  instead of 3x; reads go to the data shard, and decoding is needed only when a shard is lost.

## The metadata tier

- **Throughput.** One PostgreSQL primary handles on the order of 10^4 write transactions and 10^5
  point reads per second on good hardware (published figures, not measured here). AWS documents
  3,500 writes and 5,500 reads per second per partitioned prefix of a bucket.
- **High availability**: streaming replication with automatic failover (Patroni, or a managed
  service). Without a synchronous standby, an acknowledged PUT can be lost in a failover; a
  synchronous standby costs one round trip to it per write.
- **Read scaling that stays strongly consistent**: with `synchronous_commit = remote_apply`, the
  standbys a commit waited for see every acknowledged write, so reads can go to them.
- **Beyond one primary**: shard by bucket (for example with Citus), or move `IMetadataStore` onto a
  range-sharded, ordered, transactional key-value store (FoundationDB, TiKV). No operation needs a
  transaction across buckets, so the contract allows this.
- **If PostgreSQL is down, the cluster is down**, because every read needs metadata. The cluster's
  availability is PostgreSQL's; that is accepted through phase 4.

### How other systems store metadata

Checked on 2026-09-25 only for SeaweedFS (its wiki and `weed/command/scaffold/filer.toml`); the
rest is as each project describes itself.

- **SeaweedFS**: the filer keeps metadata in a pluggable filer store. The default is embedded LevelDB
  (`leveldb2`); the others include SQLite, MySQL, PostgreSQL, CockroachDB, YugabyteDB, TiDB, Redis,
  Cassandra, etcd, MongoDB, Elasticsearch, TiKV, FoundationDB and YDB, and its wiki marks the SQL
  stores and YDB as atomic. The masters run Raft for the cluster view and file-id assignment, and
  volume servers replicate per volume.
- **MinIO**: no database; a metadata file per object on every drive of its erasure set, with
  distributed locks.
- **Ceph RGW**: a sharded bucket index stored in RADOS objects.
- **Garage**: a Dynamo-style design with CRDT metadata and no consensus for object writes.

This design takes SeaweedFS's shape, an embedded default and an external store for shared
deployments, with one difference: the store must be transactional. Atomic conditional writes,
multipart Complete and DeleteBucket need several rows to change together, which a plain key-value
store would push into this project's own code.

## Gates

Each rule of this design ships with the check that goes red when it breaks.

| Rule | Gate |
|---|---|
| Every `IMetadataStore` behaves the same | A contract suite run on SQLite and on PostgreSQL (Testcontainers in CI), including races: N parallel `If-None-Match: *` give exactly one winner; Complete racing Abort gives one winner; per-key order only increases |
| Blob stores are write-once and serve ranges correctly | The blob-store contract suite, run against every store and the constrained test store |
| The engine behaves like S3 | The existing provider contract, HTTP and SDK-compatibility suites, run against the engine on SQLite and on PostgreSQL |
| The cluster is linearizable | An in-process harness: three Kestrel nodes, one PostgreSQL, one blob directory, random concurrent operations on random nodes, a per-key linearizability checker, and LIST checked against the writes that finished before it. Its self-test must fail on today's Disk provider with two nodes sharing a root |
| A crash never leaves a row pointing at a missing blob | Kill the process in the middle of PUT, Complete and DELETE, restart, scan the invariants, with the fault-injection store (#244) |
| Rolling upgrades work | CI runs the previous release's node against the new schema |
| No consumer-specific code | The convention test from "Consumers" |
| No performance regression | Macro-benchmarks (`warp`) on one and three nodes on a self-hosted Linux runner, against the agreed SLOs, next to the in-process BenchmarkDotNet gate |
| Phase 4 never loses an acknowledged write | Partition and kill tests plus the checker |

## Phases

The order of work; each phase ends when its gates are green. Status lives in the epic.

0. **Foundations**: the benchmark gate measures what it claims; the two contracts and their suites;
   a PostgreSQL CI lane; the multi-node harness and checker, proved red on today's code.
1. **Engine on SQLite and local disk**, matching today's single-node behaviour: streaming uploads,
   hashing on the way, manifest-based multipart, LIST by page, database time, importers for
   existing Disk data and other stores' metadata. Ships as an added package.
2. **Cluster mode**: the PostgreSQL store with migrations; shared-filesystem and S3 blob stores;
   bucket configuration, ACLs and policy in the database with invalidated caches; jobs and leases;
   the node behaviour above. Done when the linearizability and crash gates pass on three nodes.
3. **Performance**: inline small objects, shared-manifest copy, the CPU and I/O levers, and the
   macro-benchmark gate at the agreed SLOs.
4. **Own storage cluster**: volumes, sealing, repair, scrubbing, compaction, rebalancing. Done when
   the partition tests pass.
5. **Scale-out**: erasure coding, metadata sharding, asynchronous replication between sites, reads
   from `remote_apply` standbys.

Phases 0-3 alone give a production S3 server on top of any blob store. The Disk provider and the
orchestrator's replication modes would be retired once phase 2 matches them; that is when a major
version comes.

## Alternatives considered

- **Patch the Disk provider and the orchestrator**: every invariant in the table above would need its
  own cross-node mechanism (file locks that behave differently on NFS and SMB, a shared catalog that
  is still written after the fact), and none would be transactional with the others.
- **Embedded Raft** (one binary, no database): the most self-contained option, but a replicated state
  machine with snapshots, membership changes and sharding is years of correctness work, against
  PostgreSQL's well-proven replication. The `IMetadataStore` seam keeps it possible later.
- **MinIO's model** (metadata files next to the data, distributed locks): listing becomes a merge of
  directory walks, and every multi-object invariant needs quorum locking.
- **A Dynamo/CRDT model** (as in Garage): no linearizable conditional writes, so it cannot meet the
  consistency contract.
