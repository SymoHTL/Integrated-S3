# Distributed architecture

Proposed on 2026-09-25; acceptance and open work are tracked in the epic (#288). The section "Where
the code stands" is a snapshot of `main` at 707bcaf and is not maintained. Everything after it is the
design, and it changes through PRs like any other doc. As each phase lands, its rules move into
`CLAUDE.md` next to their gates, and this page keeps only the reasoning.

## Goal

IntegratedS3 should run as a production S3 server that scales out: any number of stateless nodes
behind a load balancer, strongly consistent, fast on small and large objects, and durable across node
crashes. It stays an embeddable ASP.NET library, and it serves every consumer, PersonalS3 included,
without code, options or branches written for one consumer.

## Where the code stands (snapshot at 707bcaf)

Every invariant that makes S3 semantics correct is held in process memory today, or by nothing:

| Invariant | What holds it | With two processes |
|---|---|---|
| Per-key atomicity: `If-None-Match`/`If-Match`, version archiving, Complete vs Abort, same-part uploads | 256 in-process `SemaphoreSlim` stripes (`DiskStorageService.cs:53-58, 7314-7323`) | Not held: two creates both win, versions are lost (#84's class), one writer's metadata lands on the other's bytes |
| Which version is "latest" (Disk + EF catalog) | Catalog rows written by the provider, then again by the orchestrator after the call and on every HEAD and every listed key (`OrchestratedStorageService.cs:296, 305, 472, 642`) | Not held, and not even in one process (#294) |
| Bucket configuration | Read-modify-write of the whole `.integrateds3.bucket.json` under a stripe | Concurrent changes to different settings lose one of them |
| ACLs and bucket policies | `ConcurrentDictionary` in a singleton (`InMemoryStorageAuthorizationCompatibilityService.cs:14-15`), for every provider | Different on every node, gone after a restart |
| Replica repair backlog | In memory, nothing replays it (#242, #275, #299) | Each node sees only its own backlog |
| Maintenance jobs | A timer per job per process, no lease | Every node runs every job |
| Version order, Object Lock clock | UUIDv7 ids and the node's wall clock | Order and retention depend on clock skew |
| EF catalog schema | `EnsureCreated` (#272), tested on SQLite only | No migrations; other databases untested |

With the S3 provider, object atomicity comes from upstream S3, but ACLs, bucket policies, maintenance
jobs and the replica backlog are still per process, so even a pure S3 proxy is correct on one node
only. Making the Disk provider and the orchestrator distributed would mean rebuilding each of these
invariants around shared state, one at a time. This design moves all of them into one transactional
store instead.

## The design in one paragraph

A new engine implements S3 semantics once, on two contracts: a transactional **metadata store**
(SQLite for a single node, PostgreSQL for a cluster) and a write-once **blob store** (local disk, a
shared filesystem, any S3-compatible store, a consumer's own store, and later the nodes' own disks).
Object bytes are written first and never modified; an operation takes effect when one metadata
transaction commits. API nodes hold no state that matters, so they scale out behind any load
balancer. Background work runs from a job table with leases, so any node can do it and a crashed
node's work is picked up by another. Azure Storage, Meta's Tectonic, Ceph RGW's bucket index and the
SeaweedFS filer share this split between a metadata layer and immutable data.

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

- **The metadata store** is internal to the engine: transactional primitives (lock a bucket, lock a
  key, read and write the rows below) that the engine combines into one transaction per S3
  operation. The engine writes S3 semantics once, in C#, against these primitives; each store only
  translates them into its own queries. The contract stays internal because it will change many
  times while the engine grows, and a public interface would make each change a major version. A
  third store (FoundationDB, a PostgreSQL-compatible database) would be written inside the engine.
- **`IBlobStore`** (`IntegratedS3.Abstractions.Blobs`, public): write-once blobs. Write a stream and
  get back a locator the store assigns, unique for every write; open a locator for reading,
  optionally a byte range; delete (idempotent); list from a resumable cursor, for the orphan sweep.
  It never overwrites, renames or edits a blob, so a shared filesystem needs no locking at all. The
  engine computes all digests itself by wrapping the stream, so a store only moves bytes. A store
  depends on nothing but `Abstractions`, and runs `BlobStoreContractTests` from `IntegratedS3.Testing`
  in its own CI.
- **The engine**: versioning, preconditions, multipart, copy, tagging, ACLs, Object Lock, lifecycle
  and garbage collection, written once on top of the two contracts. It also gives blob stores a
  narrow service for their own upkeep (see "Consumers"): replace one locator by another, report a
  blob as lost, and run a store's own jobs. That service and the job API are public in
  `Abstractions`, because a store depends on nothing else.

### How it enters the code base

- **Phase 1** registers the engine as one more `IStorageBackend` (`AddIntegratedS3Engine(...)`, like
  `AddDiskStorage`), and the orchestrator runs it in its default `PrimaryOnly` mode with no catalog.
  The endpoints, DI, authorization and `StorageProviderContractTests` apply from the first day.
- **Later**, the engine replaces the orchestrator but never the authorization layer.
  `AuthorizingStorageService` runs the `IIntegratedS3AuthorizationService` checks, the ACL and policy
  fallback and the telemetry, and today it is bound to the concrete `OrchestratedStorageService`. Its
  inner dependency becomes `IStorageService`, and the engine registers behind it.
- Everything is added and nothing is removed, so no major version is needed until something is
  retired.
- **Proxy mode stays**: the S3 provider keeps working as a pure proxy in front of S3, with no metadata
  store, for hosts that want exactly that. It is correct on a single node only, until ACL and policy
  state is shared (above).
- **Packages** (the rows go into the table in `LayeringConventionTests`):
  - `IBlobStore` and its types live in `IntegratedS3.Abstractions`, so a store needs only that
    package; its contract suite and a constrainable in-memory store live in `IntegratedS3.Testing`.
  - `IntegratedS3.Engine` holds the engine, the internal metadata contract, both metadata stores on
    raw ADO.NET (Microsoft.Data.Sqlite and Npgsql; the host picks one with `UseSqlite` or
    `UsePostgreSql`, and trimming drops the other's managed code), the local-disk blob store, and
    the database-backed implementation of Core's `IStorageAuthorizationCompatibilityService`. That
    interface addresses an object's ACL by bucket and key, with no version id, so the engine adds a
    versioned interface and implements the old one for the latest version only. Its row ends as
    Abstractions, Protocol and Core. It ships with a `-preview` version suffix until it matches the
    Disk provider.
  - The S3 blob store goes into `IntegratedS3.Provider.S3`, which reuses its client and needs only
    `Abstractions` for the contract.

### Deployment shapes

1. **Single node**: SQLite and local disk. Proposed to replace the Disk provider for new deployments
   once the engine matches it (a decision in #288).
2. **Cluster**: PostgreSQL and a shared blob store (an S3-compatible service, an NFS or SMB share, or a
   consumer store), with N stateless nodes behind a load balancer.
3. **Own storage cluster** (phase 4): PostgreSQL, plus volumes on the nodes' own disks, with no
   external object store.

## Consistency contract

Each item says how it compares with what AWS documents for S3 ("Amazon S3 data consistency model",
read on 2026-09-25): strong read-after-write for PUT and DELETE of objects, atomic updates of a single
key, "the request with the latest timestamp wins" between concurrent writers, strongly consistent
object metadata, tags and ACLs, and eventually consistent bucket configuration.

1. **Objects are linearizable per key.** Every change takes effect at its metadata commit, and the
   client gets its response after the commit. Read-after-write, overwrite and delete are strongly
   consistent on every node. *Stronger than AWS*, which promises read-after-write but not
   linearizability. In a cluster this holds only with a synchronous standby (see "The metadata
   tier").
2. **LIST is strongly consistent**: it reads the index, with no cache. A page reflects every write
   that committed before it started; pagination is not a snapshot. *Matches AWS.*
3. **Preconditions are atomic across the cluster.** `If-None-Match: *` on PUT, Complete and Copy,
   and `If-Match` on those and on DELETE, are evaluated under the key's lock, inside the commit
   transaction. *Matches AWS's conditional writes*, including their status codes: `If-Match` on a
   missing key or on a delete marker fails with 404, a mismatched ETag with 412.
4. **A GET reads one snapshot.** It resolves the version once and streams bytes that never change, so
   a concurrent overwrite cannot mix two versions into one response. *Matches AWS.*
5. **Version order within a key is commit order.** It comes from a per-key counter (`head.seq + 1`)
   taken while the key's row lock is held. Not a global sequence: in PostgreSQL, a `nextval` inside
   an `INSERT … VALUES` runs before the `ON CONFLICT` lock wait, so a writer that waited could get a
   smaller number than the one it waited for. *Stronger than AWS*, whose tie-break is the request
   timestamp.
6. **Time comes from the database, inside SQL**: LastModified, Object Lock and lifecycle ages, for
   example `retain_until > clock_timestamp()` in the delete transaction. LastModified is
   `clock_timestamp()` read after the key's lock is taken, and never earlier than the key's previous
   LastModified, so a newer version never looks older than a noncurrent one. A node's clock is used
   only for the SigV4 clock-skew window; database hosts and nodes both need NTP, as with AWS.
   *Differs from AWS* for multipart uploads, whose LastModified AWS sets to the upload's initiation
   date; here it is the commit time of CompleteMultipartUpload.
7. **Object ACLs and tags are read inside the request**, from the version row: *matches AWS*. The
   ACL a write carries is stored by that write's own transaction, never applied by a later call.
   **Bucket configuration** (CORS, policy, the bucket ACL, lifecycle and the rest) **is stale for at
   most T seconds** (proposed T = 5): it is cached per node, invalidated by `LISTEN`/`NOTIFY`, with a
   TTL as the safety net and a full flush after any reconnect. *Matches AWS* for bucket configuration.
   Bucket existence and versioning state are read inside every object-write transaction, so writes
   never act on a stale value.

## Metadata

### Tables (PostgreSQL; SQLite mirrors them)

| Table | Holds |
|---|---|
| `buckets` | One row per bucket: name (unique), versioning state, Object Lock flag, owner, creation time |
| `bucket_configs(bucket_id, kind, doc)` | One row per configuration kind, the bucket ACL and policy included, so changes to different kinds never overwrite each other |
| `object_heads(bucket_id, key, seq, is_delete_marker, …)` | One row per key: the LIST index and the per-key lock. A partial index covers the heads that are not delete markers, so a page after a mass delete does not scan the markers |
| `object_versions(bucket_id, key, seq, version_id, manifest or inline data, size, etag, checksums, headers, user metadata, tags, ACL, retention, legal hold, data key)` | One row per version |
| `uploads`, `upload_parts(upload_id, part_no, manifest, size, md5, checksums)` | Multipart state |
| `blob_refs(locator primary key, state, ref_count)` | Every blob a row references, and every blob the orphan sweep has claimed: the fence between a commit and the sweep, and the count for shared manifests (phase 3). A row stays until garbage collection has deleted its blob, and a `swept` row stays after that |
| `blob_holders(locator, bucket_id, key, version or upload and part)` | The reverse index for relocation and lost-blob reports |
| `gc_queue(locator, not_before)`, `orphan_candidates(locator, first_seen)`, `blob_read_leases(locator, node, until)` | Garbage collection and the orphan sweep |
| `jobs`, `leases(name, owner, fence, until)`, `nodes` | Background work, single-owner work, membership |

Keys are stored as `bytea`: byte order is S3's UTF-8 binary order with no collation involved, and a
NUL byte is allowed. A manifest is the ordered list of `(blob locator, offset, length)` extents that
make up an object.

### Operations

Every object write has the same shape. PUT is the example:

1. Stream the body to the blob store, computing the digests on the way. No lock is held.
2. In one transaction, which on PostgreSQL starts with `BEGIN ISOLATION LEVEL READ COMMITTED`
   (the reasoning below holds only there, and `default_transaction_isolation` can be set per
   database or role):
   1. Take a shared advisory lock keyed by the bucket's **name**, in a statement of its own. Under
      READ COMMITTED a statement's snapshot is taken before its own lock wait, so a read in the same
      statement would see the bucket as it was before a concurrent DeleteBucket or versioning change.
      Keying by name, not by a cached id, also covers a bucket deleted and re-created under the same
      name. The key is a 64-bit hash of the name in the two-key form with the engine's namespace, so
      it cannot collide with the migration lock or a store's own locks. Every write under a bucket
      takes this lock (CreateMultipartUpload and the object subresource writes included), and the
      transaction checks that the bucket's id is the one the request was authorized against.
   2. Read the bucket row: existence, versioning state, Object Lock.
   3. Lock the key: insert the head row if it is missing (`ON CONFLICT DO NOTHING`), then
      `SELECT … FOR UPDATE`, and repeat both until the select returns the row: under READ COMMITTED
      a waiter whose row was deleted by the transaction it waited for gets no row. From here on, the
      latest committed version is fixed. A transaction that leaves a key with no version deletes its
      head row.
   4. Check the precondition in code against that version. `If-Match` on a missing key, or on a key
      whose latest version is a delete marker, fails with 404 as AWS documents, and a mismatched
      ETag with 412, which a single `INSERT … ON CONFLICT … DO UPDATE … WHERE` could not express.
   5. Insert the version row and point the head at it. In an unversioned or suspended bucket, delete
      the replaced null version and queue its blobs for garbage collection.
   6. Record the new blobs in `blob_refs` (see "Crash safety").
3. Commit, then respond.

The key's lock is held for a few round trips, never while the body is still arriving. Racing
`If-None-Match: *` on two nodes: exactly one commits, the other gets 412.

- **DELETE, CopyObject and CompleteMultipartUpload** commit the same way: the same bucket lock, the
  same key lock, preconditions checked under it. DELETE writes a delete marker or removes a version,
  and queues freed blobs. Object Lock, per-object retention and legal hold included, is checked in
  SQL against the database clock.
- **UploadPart**: write the blob, verify any client checksum, then, in one transaction, take
  `FOR SHARE` on the upload row, fail with `NoSuchUpload` unless the upload is still active, lock the
  part row as PUT locks the key, replace it, and queue the blob it held. The last writer wins, as in
  S3. Because Complete
  takes `FOR UPDATE` on the same row, a part cannot commit after Complete has read the parts, and a
  retried UploadPart after a successful Complete gets `NoSuchUpload` instead of queuing a blob the
  object now references.
- **CompleteMultipartUpload**: lock the upload row (`FOR UPDATE`) and check it is still active;
  validate the parts from their rows; compute the ETag from the stored part MD5s; insert a version
  whose manifest lists the part blobs; mark the upload completed; queue the blobs of parts that were
  uploaded but not listed. No byte is copied, so the cost grows with the number of parts, not the
  object size (#239's goal). Complete racing Abort, or two Completes, serialize on the upload row, and
  exactly one wins.
- **CopyObject**: a physical copy (server-side once the blob store contract gains a copy capability,
  which it does not have yet). A copy within one bucket may later share the source's manifest, with
  a reference count in `blob_refs` updated in the same transaction; that transaction locks the
  source locators' `blob_refs` rows and fails if one is no longer referenced. Copies between buckets
  stay physical, so no transaction spans two buckets.
- **ListObjectsV2**: a range scan over live heads in key order that stops after max-keys + 1. With a
  delimiter it skips ahead: at `photos/2024/` it emits that common prefix and seeks to the first key
  after the prefix. This needs the delimiter to reach the storage layer (#297). The cost grows with
  the page, not the bucket.
- **ListObjectVersions**: versions in key order, newest first, which is S3's order. A version-id
  marker is resolved to a position by one lookup. The endpoints still read every entry for this
  listing and for ListMultipartUploads before paging (#277), which the engine phase fixes too.
- **DeleteBucket, versioning changes, Object Lock configuration**: take the bucket's advisory lock
  exclusively, then check emptiness (heads, versions, active uploads). No write can land in a bucket
  being deleted, and no write runs half under the old versioning mode. Advisory locks live in shared
  memory and write nothing to table rows. `FOR SHARE` on the bucket row would do the same job, but
  many concurrent transactions locking one hot parent row cause MultiXact contention in PostgreSQL.
  (A foreign key would not: its check takes `FOR KEY SHARE`, which does not conflict with the update
  that changes the versioning state.)
- **CreateBucket**: the unique name decides, so exactly one create wins across the cluster.

### Small objects

Objects up to a threshold (about 8 KiB, to be set by benchmark) are stored inline in the version row.
A PUT is then one transaction and a GET one query, with no blob I/O at all. This is the main lever for
small-object throughput. When encryption at rest is on, inline data is encrypted with the object's
data key before it is stored, like any blob.

### Schema and access

- Versioned SQL migrations run at startup under an advisory lock, replacing `EnsureCreated`. The lock
  is a session lock on a direct connection, because a migration can span several transactions
  (`CREATE INDEX CONCURRENTLY` cannot run inside one). The lock and the migration's DDL run on that
  one connection. Nodes that find the lock taken poll with `pg_try_advisory_lock` and hold no
  transaction open between polls: a waiter blocked in `pg_advisory_lock` holds a snapshot that a
  concurrent index build waits for, a deadlock. A migration drops an INVALID index left by a failed
  build before building it again. Each change adds first and removes only in a
  later release (expand, then contract), so old and new nodes run side by side during a rolling
  upgrade.
- The stores use raw ADO.NET rather than EF: the locking SQL is exact, statements are batched into
  one round trip, and the package stays AOT- and trim-clean.
- SQLite has one writer at a time (`BEGIN IMMEDIATE`), so its key and bucket locks are no-ops; it must
  pass the same contract suite as PostgreSQL.

## Blob stores

A blob store declares what it can do, and the engine adapts; it never checks which store it has.

- **Maximum blob size.** The engine splits larger bodies into several blobs and lists them in the
  manifest, which multipart already needs anyway.
- **Parallel writes.** A store declares how many blobs of one body it accepts at once, and the engine
  writes a body's blobs with at most that parallelism.
- **Range reads.** When a store cannot serve a byte range, the engine reads from the start of the
  blob and skips.
- **Locators** are opaque and assigned by the store at write time. A store whose read URLs expire
  resolves a fresh URL itself; the engine only keeps the locator.
- **Throttling.** A store that answers "slow down" makes the engine return 503 `SlowDown` to the
  client and back off in background jobs.
- **Listing** is resumable from a cursor, so an orphan sweep over a slow or rate-limited store can run
  in small steps.
- **Encryption at rest** is an engine-level decorator (envelope encryption: a data key per object,
  stored in the version row and wrapped by a master key or KMS), so every store gets it. A store may
  also encrypt on its own.

## Consumers

IntegratedS3 contains no code, option or branch for a particular consumer. What a consumer needs
becomes a property of the generic contracts.

A store's own upkeep uses three generic mechanisms:

- **Relocation.** A store that must move a blob (to another container, channel or disk) writes the
  copy, then asks the engine to replace the old locator by the new one. In one transaction the engine
  locks the old locator's `blob_refs` row, takes the key lock of each version and `FOR SHARE` on the
  upload row of each part that holds it (found through `blob_holders`), swaps the locator in those
  rows, moves the reference to the new locator, and queues the old blob for garbage collection. A
  transaction that copies a locator into a new row (CompleteMultipartUpload, a shared-manifest copy)
  locks the same `blob_refs` row and fails if the locator is no longer referenced, and garbage
  collection deletes a blob only in a transaction that finds it unreferenced. Repeating a swap that
  already committed reports success. A store never deletes its copy itself: after a failed swap the
  copy is unreferenced, and the orphan sweep collects it. Phase 4's compaction uses the same
  mechanism.
- **Lost blobs.** A store that finds a blob gone for good reports it. The engine records the loss and
  lists the versions and upload parts that reference it. A policy decides what those versions become:
  answering an error on read (the default), or delete markers. Under either policy the version's
  other blobs are kept.
- **Private state and containers.** A store may keep private state, such as a cache of expiring URLs
  or the live blobs of containers that hold several blobs, in its own tables. On a single node it
  keeps them in its own database file, because the engine's SQLite file has one writer; in a cluster
  that state must be shared, for example in the same PostgreSQL database. A store whose containers
  hold several blobs keeps the set of live blobs in each container, deletes a container when that
  set is empty, and treats removing a blob that is not in the set as a no-op, so a retried delete
  never takes a container's other blobs with it.

**Gates.** The blob-store contract suite runs in CI against a constrained in-memory store: an 8 MiB
maximum blob size, store-assigned locators, no range reads, injected throttling, short listing pages
and rate-limited deletes. The engine's provider contract and endpoint suites also run on a
constrained store, so the engine itself is held to the capabilities: bodies over the maximum blob
size, ranged GETs without range support, and throttled calls that answer 503 `SlowDown`. A
convention test fails when a file under `src/` outside tests and samples names a consumer. It cannot
see an option or branch written for a consumer under a neutral name; that half of the rule is a
`judgment step` in review (HAZARD, #270). A consumer runs the same suite against its own store in its
own CI.

**PersonalS3 as the worked example.** It already has this shape: metadata in SQLite, chunks in Discord
messages.
- Its Discord storage becomes an `IBlobStore` in which a blob is one message: `MaxBlobSize` is ten
  8 MiB attachments, and the store cuts a blob into attachments itself, so messages need no live-blob
  sets, and redistribution and deletes act on whole messages as they do today. The engine writes a
  body's messages with the parallelism the store declares. The locator is the store's own id, not
  Discord's attachment id, because the URL refresh rewrites attachment ids; the store refreshes its
  expiring URLs in its own table.
- Channel redistribution becomes relocation; a message Discord lost becomes a lost-blob report, with
  the delete-marker policy PersonalS3 uses today.
- Its metadata moves to the SQLite metadata store through a one-time import. The engine offers one
  generic bulk-import path, which the Disk-layout importer uses too.
- Multipart expiry and the orphan sweep become engine jobs. Store-specific upkeep, such as URL refresh
  and redistribution, registers through the same generic job API and gets leases for free.
- Its encryption stays inside its store: moving it to the engine's decorator would mean rewriting
  every stored chunk, or teaching the decorator PersonalS3's own format. Inline storage stays off
  when encryption at rest is left to the store, since inline objects never reach the store.
- Its rate-limit state stays inside its store. Running it as a cluster would need that store to
  coordinate its own quotas across nodes; the engine knows nothing about them.

## Crash safety and garbage collection

- **Order.** The blob is written, and flushed, before the commit. A crash in between leaves an
  unreferenced blob, never a row that points at a missing blob.
- **The orphan sweep** walks the blob store's listing. A blob with no `blob_refs` row gets an
  `orphan_candidates` row stamped with the database clock the first time it is seen. Only when that
  stamp is older than the grace period G does the sweep claim it: it inserts a `swept` row into
  `blob_refs` and deletes the blob after that commits. A commit inserts a `referenced` row for each of
  its blobs. Both inserts hit the same primary key, so the database serializes the two. Whichever
  comes second sees the first: a commit that finds its blob claimed fails and the client retries, and
  the sweep never deletes a referenced blob. The ages come only from the database clock, never from
  the store's timestamps. A candidate row is dropped when the sweep finds its blob referenced or gone;
  a `blob_refs` row stays until garbage collection has deleted its blob, so the sweep never claims a
  locator that is in `gc_queue`. A `swept` row is kept, and each pass deletes again the blobs whose
  row is `swept`, which finishes a claim whose delete a crash interrupted.
- **The sweep refuses to empty a store.** It claims nothing while `blob_refs` holds no referenced row
  but the store lists blobs (a node pointed at a fresh, wrong or partly restored database), it stays
  off while an import runs, and a store may decline a claim (for example in a container none of whose
  blobs is referenced); a declined claim is logged for an operator.
- **G bounds the longest upload**: a single body whose upload takes longer than G fails at its commit.
  With G = 24 hours that is a sustained rate below about 60 KiB/s for a 5 GiB part.
- **Garbage collection** deletes a queued blob only after its `not_before`. A reader that is still
  reading a few minutes after it started records a read lease on its blobs and renews it; garbage
  collection skips a blob with a live lease. Every reader of a blob leases it: GETs, the physical
  copies of CopyObject and UploadPartCopy, replication and scrubbing. The lease insert fails when
  garbage collection has claimed the blob, and a reader whose lease lapsed stops with an error instead
  of reading on. The `not_before` delay must exceed the time before the first lease plus the longest
  pause a node survives; compaction drops a volume only after every blob in it is collected.
- **Failover.** Garbage collection deletes a blob only after the standbys have replayed the
  transaction that queued it, so a promoted standby never references a deleted blob.
- A point-in-time restore of the metadata is safe only as far back as the `not_before` delay.

## Coordination

PostgreSQL is the only consensus in the system; everything else derives from it.

- **Jobs**: a `jobs` table. A node claims work with `FOR UPDATE SKIP LOCKED`, holds a lease with a
  fencing token, and retries with backoff. Jobs run garbage collection, the orphan sweep, multipart
  expiry, lifecycle rules (#243), bucket replication and scrubbing. Any node can take any job; when a
  node dies its lease expires and another node carries on. This replaces the in-memory repair
  backlog, its dispatcher, and the lease-less maintenance scheduler.
- **Single-owner work**: a lease row whose fencing number only grows. Every metadata write checks the
  number, so a leader that was paused and resumes late cannot overwrite newer work. Side effects in
  the blob store cannot check a fencing token, so they are idempotent: writes create new blobs, and
  deletes of an absent blob succeed.
- **Caches**: invalidated through `LISTEN`/`NOTIFY`, bounded by a TTL, flushed on reconnect, so a lost
  notification costs at most T seconds of staleness.
- **Connections**: advisory locks for requests are held for one transaction only, so PgBouncer in
  transaction mode works, with three exceptions that use direct connections or settings:
  - `LISTEN` is not supported under transaction pooling, so each node keeps one direct connection
    for it;
  - the migration lock is a session lock on a direct connection (see "Schema and access");
  - prepared statements need PgBouncer 1.21 or later with `max_prepared_statements` set.

## Node behaviour in cluster mode

- **Readiness** means "this node reaches the metadata store and the blob store". It does not depend
  on the health of other backends.
- **Identity**: the node id is part of `x-amz-id-2` and of every metric. Cluster-wide gauges, such as
  the job backlog and garbage-collection debt, come from the database.
- **Presigning** reads keys from the same reloadable source as authentication, and builds URLs from
  the configured public base URL or forwarded headers when behind a proxy.
- **Credentials** can live in the database, cached per node and invalidated by `NOTIFY`.
- **Kestrel limits** (minimum body data rate, connection limits, timeouts) are set explicitly.
- **Commits cannot be cancelled.** A commit runs with no cancellation token, on connections with
  `Command Timeout=0`. Npgsql's default of 30 seconds would send PostgreSQL a cancel, which ends the
  wait for a synchronous standby and reports success for a transaction that is committed locally,
  visible, and possibly lost in a failover. A commit that reports the warning "canceling wait for
  synchronous replication" is answered with a 500 and logged as a durability fault, never
  acknowledged.

## Performance

Targets are set as SLOs with the owner before phase 3; the levers, in order of production impact:

1. **Streaming uploads.** No request path buffers or spools a whole body; each body streams straight
   to the blob store (#238).
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

Two reference points, not comparable with each other:

- The BenchmarkDotNet baseline of 2026-07-04 (`benchmarks/baseline`, a Windows desktop, bound by
  fsync) has a mean of 5.35 ms for one 64 KiB Disk PUT from one writer.
- SeaweedFS published about 5,700 writes and 13,000 reads per second for 1 KiB objects with 64
  concurrent clients on a laptop (`seaweedfs-comparison-2026-07-04.md`, which says the figure only
  bounds the comparison).

With this design, large-object throughput is bound by network and disks, and small-object throughput
by metadata transactions per second.

## Own storage cluster (phases 4 and 5)

The unit of placement, replication and repair is a **volume**: an append-only file of about 1-4 GiB,
stored on three nodes in different failure domains. A blob locator is (volume, offset, length,
CRC32C). Large bodies are split into chunks of tens of MiB spread over volumes, which gives parallel
I/O and small retry units.

- **Placement lives in the metadata store** (`volumes(id, state, replicas, primary, epoch,
  sealed_length)`), cached on every node. A 20 TB node holds about 5,000 volumes of 4 GiB, so losing
  a node updates thousands of volume rows, not millions of object rows.
- **Appending**: the volume's primary holds a lease and an epoch from the database and assigns
  offsets. It copies each append to the other replicas, flushes in groups, and acknowledges only when
  all three copies are durable. The metadata commits only after that, so every blob the metadata
  points to is on three nodes.
- **When a replica fails**, the volume is not repaired while open. It is **sealed**, in this order:
  the sealer bumps the volume's epoch in the database; it tells every reachable replica to reject
  appends from older epochs; only then does it read their durable lengths, and it seals at the
  shortest one. A primary that the sealer counts as failed, but that still reaches the replicas, can
  then no longer acknowledge an append above the sealed length. Writing continues in a new volume on
  healthy nodes. Replicas never diverge at the tail, and the data path needs no consensus protocol;
  this is the technique of Azure Storage's stream layer. Writes keep working while three healthy
  nodes in different failure domains exist.
- **Repair** copies a sealed, under-replicated volume from a survivor, checks its CRC and updates the
  replica list. It waits about 15 minutes first, to tell a reboot from a dead node.
- **Reads** go to any copy, the local zone first, with a CRC check per blob. A mismatch reads another
  copy and queues a repair; a background scrub does the same checks proactively (#240's class).
- **Deletes** leave garbage in sealed volumes. Compaction rewrites the live blobs and swaps their
  locators through relocation (see "Consumers"), then drops the old volume after garbage collection's
  delay. A new node receives sealed volumes, with a bandwidth cap.
- **Between nodes**: HTTP on Kestrel first; a binary protocol only if measurements ask for it.
- **Phase 5** erasure-codes sealed volumes. With Reed-Solomon 10+4 the storage overhead is 1.4x
  instead of 3x; reads go to the data shard, and decoding is needed only when a shard is lost.

## The metadata tier

- **Throughput.** One PostgreSQL primary should handle on the order of 10^4 write transactions and
  10^5 point reads per second on good hardware; this is an estimate, to be replaced by the phase-3
  measurements. AWS documents 3,500 writes and 5,500 reads per second per partitioned prefix of a
  bucket.
- **High availability**: streaming replication with automatic failover (Patroni, or a managed
  service). Consistency item 1 holds only with a synchronous standby: without one, an acknowledged
  PUT can be lost in a failover. A synchronous standby costs one round trip to it per write. With
  Patroni this needs `synchronous_mode_strict`: plain `synchronous_mode` turns replication
  asynchronous when no synchronous standby is left.
- **Read scaling**: with `synchronous_commit = remote_apply`, reads routed to the current synchronous
  standbys see every write their client had acknowledged. That is read-your-own-writes, not
  linearizability: a standby applies a commit before the primary makes it visible, and an `ANY k`
  quorum does not say which standbys those are, and the set can change. A standby therefore serves a
  read only when its replay position is at or past the client's last commit position; otherwise the
  primary serves it. Standby reads are limited to requests that only need read-your-own-writes, and
  phase 5 runs the linearizability checker against them.
- **Beyond one primary**: shard by bucket (for example with Citus), or move the metadata store onto a
  range-sharded, ordered, transactional key-value store (FoundationDB, TiKV). Object operations stay
  within one bucket: copies between buckets are physical. `blob_refs`, `gc_queue` and the read leases
  are sharded with their bucket, since a locator is referenced from one bucket only. Bucket names,
  jobs and leases stay unsharded, and the orphan sweep checks every shard.
- **If PostgreSQL is down, the cluster is down**, because every read needs metadata. The cluster's
  availability is PostgreSQL's; that is accepted through phase 4.

### How other systems store metadata

Checked on 2026-09-25 only for SeaweedFS (its wiki and `weed/command/scaffold/filer.toml`); the rest
is as each project describes itself.

- **SeaweedFS**: the filer keeps metadata in a pluggable filer store. The default is embedded LevelDB
  (`leveldb2`); the others include SQLite, MySQL, PostgreSQL, CockroachDB, YugabyteDB, TiDB, Redis,
  Cassandra, etcd, MongoDB, Elasticsearch, TiKV, FoundationDB and YDB. Its wiki marks directory
  renaming as atomic on the SQL stores, YDB and TiKV. The masters run Raft for the cluster view and
  file-id assignment, and volume servers replicate per volume.
- **MinIO**: no database; a metadata file per object on every drive of its erasure set, with
  distributed locks.
- **Ceph RGW**: a sharded bucket index stored in RADOS objects.
- **Garage**: a Dynamo-style design with CRDT metadata and no consensus for object writes.

This design takes SeaweedFS's shape, an embedded default and an external store for shared
deployments, with one difference: the store must be transactional. Atomic conditional writes,
multipart Complete and DeleteBucket need several rows to change together, which a plain key-value
store would push into this project's own code.

## Gates

Each rule of this design names the check that goes red when it breaks, or says it has none yet.
The harness, suite and job names below are the ones the phase issues create.

| Rule | Gate |
|---|---|
| The metadata stores behave the same | The engine's store-level suite (row and bucket locks across connections, jobs and leases, `not_before`, the `blob_refs` fence) on SQLite and on PostgreSQL (#290's lane) |
| The engine behaves like S3 | `StorageProviderContractTests` for the engine on both stores, with new facts for the races: N parallel `If-None-Match: *` give one winner; `If-Match` on a missing key and on a delete marker give 404, and a mismatched ETag 412; preconditions on Complete, Copy and DELETE; Complete racing Abort gives one winner; UploadPart racing or following Complete never queues a referenced blob; no write lands in a deleted or re-created bucket. The harness calls 29 of 86 operations today and many facts return early (#268), so the engine lane also runs the endpoint and SDK suites |
| Blob stores are write-once and serve ranges correctly | `BlobStoreContractTests` against every store and the constrained in-memory store |
| The cluster is linearizable | #291: the checker's self-test (known bad histories rejected, known good ones accepted), and the harness proved red on today's Disk provider with two nodes sharing a root; then three engine nodes on one PostgreSQL must stay green |
| A crash never leaves a row pointing at a missing blob | Kill the process in the middle of PUT, Complete and DELETE, restart, scan the invariants, with a fault-injecting `IBlobStore` decorator |
| The sweep never deletes a referenced blob | A store-level fact that races a commit against the sweep's claim on the same locator |
| The sweep refuses to empty a store | Two store-level facts: an empty database over a populated store deletes nothing, and a store that declines a claim keeps its blob |
| Heads never outlive their last version | Two facts: a DELETE of a missing key followed by LIST and DeleteBucket; N racing `If-None-Match: *` writes against a delete of the key's last version give exactly one winner |
| Relocation never loses or leaks a blob | Store-level facts for a swap racing Complete, a copy and garbage collection, for a retried swap whose first commit succeeded, and for a swap racing a DELETE of a version that holds the locator; HAZARD until relocation exists (phase 2, #288) |
| Version order is commit order | A fact that holds one PUT's body mid-stream and requires a second PUT of the same key on another connection to commit first and get the older version |
| No write runs half under the old versioning mode | A fact that races PUTs against a versioning change and checks every committed version against the state it committed under |
| The authorization layer is never replaced | A DI fact that `IStorageService` resolves to `AuthorizingStorageService` with the engine registered |
| The engine adapts to store capabilities | The engine's provider contract and endpoint suites on the constrained store (see "Consumers") |
| Readers never hit a collected blob | HAZARD until the read-lease fact exists (phase 2, #288) |
| Bucket configuration is at most T seconds stale | HAZARD until the cache-invalidation fact exists (phase 2, #288) |
| Time comes only from the database | A banned-API entry for `DateTime.Now`, `DateTime.UtcNow`, `DateTimeOffset.Now` and `DateTimeOffset.UtcNow` in `IntegratedS3.Engine`, plus a fact that sets a node clock far off and checks LastModified and retention |
| The key lock is never held while a body arrives | The version-order fact above: a second PUT commits while the first body is held mid-stream |
| Commits cannot be cancelled | A fact on #290's lane where a synchronous standby's WAL receiver is paused longer than the command timeout, asserting the commit still waits; HAZARD until that lane has a standby (#290) |
| Garbage collection waits for standby replay | HAZARD until #290's lane has a standby (#290) |
| Blobs are flushed before their commit | A store-level fact that the local-disk store's write returns only after `Flush(true)`; a power-cut test on a filesystem that drops unflushed writes (LazyFS) is HAZARD (#288); other stores HAZARD (#288) |
| PgBouncer in transaction mode works | HAZARD until #290's lane runs through PgBouncer (#290) |
| Rolling upgrades work | CI runs the previous release's node against the new schema; HAZARD until the engine's first release (#288) |
| No consumer-specific code | The convention test from "Consumers" (names only); `judgment step` for neutral-looking branches (HAZARD, #270) |
| No performance regression | Macro-benchmarks (`warp`) on one and three nodes, against the agreed SLOs. The in-process BenchmarkDotNet gate has never run, because no self-hosted runner exists (#270) |
| Phase 4 never loses an acknowledged write | Partition and kill tests plus the checker, including a primary that is alive but cut off from the sealer |

## Phases

The order of work; each phase ends when its gates are green. Status lives in the epic.

0. **Foundations**: the benchmark gate measures what it claims; the blob-store contract and its
   suite; a PostgreSQL CI lane; the multi-node harness and checker, proved red on today's code.
1. **Engine on SQLite and local disk**, matching today's single-node behaviour: streaming uploads,
   hashing on the way, manifest-based multipart, LIST by page, database time, bucket configuration,
   ACLs and policy in the database, garbage collection and jobs, importers for existing Disk data and
   other stores' metadata. Ships as an added package.
2. **Cluster mode**: the PostgreSQL store with migrations; shared-filesystem and S3 blob stores;
   invalidated caches; leases; read leases; the node behaviour above. Done when the linearizability
   and crash gates pass on three nodes.
3. **Performance**: inline small objects, shared-manifest copy within a bucket, the CPU and I/O
   levers, and the macro-benchmark gate at the agreed SLOs.
4. **Own storage cluster**: volumes, sealing, repair, scrubbing, compaction, rebalancing. Done when
   the partition tests pass.
5. **Scale-out**: erasure coding, metadata sharding, reads from synchronous standbys.

Phases 0-3 alone give a production S3 server on top of any blob store. The Disk provider and the
orchestrator's replication modes would be retired once phase 2 matches them; that is when a major
version comes.

## Alternatives considered

- **Patch the Disk provider and the orchestrator**: every invariant in the table above would need its
  own cross-node mechanism (file locks that behave differently on NFS and SMB, a shared catalog that
  is still written after the fact), and none would be transactional with the others.
- **Embedded Raft** (one binary, no database): the most self-contained option, but a replicated state
  machine with snapshots, membership changes and sharding is years of correctness work, against
  PostgreSQL's well-proven replication. The internal metadata contract keeps it possible later.
- **FoundationDB or TiKV as the first cluster store**: ordered, transactional and sharded from the
  start, so they scale past one primary without resharding. But each is a second distributed system
  to deploy and operate, with no managed offering as common as PostgreSQL's, its transactions have a
  time limit (FoundationDB: five seconds), and a single node would still need SQLite. They remain the
  scale-out path (see "The metadata tier").
- **MinIO's model** (metadata files next to the data, distributed locks): listing becomes a merge of
  directory walks, and every multi-object invariant needs quorum locking.
- **A Dynamo/CRDT model** (as in Garage): no linearizable conditional writes, so it cannot meet the
  consistency contract.
