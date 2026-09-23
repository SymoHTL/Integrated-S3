---
name: same-key-race-two-latest-rows
description: Every store here with an "is latest" flag or a per-key file has shipped a same-key race that lost a version or left two latest rows (#84, #110, #111, #123, #124; PersonalS3 #62 still open). The fix has three parts - per-key serialization, a transaction, a database unique constraint - and a concurrent same-key test.
metadata:
  type: project
---

History:

- **#84.** `DiskStorageService` had no concurrency control, so concurrent writes to one key lost
  versions.
- **#110 and #111.** The EF catalog's demote-then-insert in `UpsertObjectAsync` was neither
  atomic nor guarded by a concurrency token, so two writers could each leave an `IsLatest = true`
  row.
- **#123.** `AbortMultipartUpload` raced `CompleteMultipartUpload`.
- **#124.** Concurrent uploads of the same part number returned an ETag for bytes the call had not
  written.
- **SymoHTL/PersonalS3#62 (open).** The Dapper catalog leaves two `IsCurrent = 1` rows.

What the fixes are made of:

- **Per-key serialization.** `DiskStorageService` holds 256 `SemaphoreSlim` stripes
  (`MutationLockStripeCount`).
- **A transaction** around the demote and the insert.
- **A database guard**: the filtered unique index `IX_IntegratedS3Objects_SingleLatestPerKey`
  (`HasFilter("\"IsLatest\" = 1")`, in `IntegratedS3CatalogModelBuilderExtensions`).

Known limits, not yet observed in production:

- The stripes belong to one `DiskStorageService` instance, so two instances or processes on the
  same root are not serialized against each other.
- `SemaphoreSlim` is not reentrant.
- The EF tests run on SQLite only, which runs writers one at a time and hides races. The filter
  literal is provider-specific.
- A database created before 11.0.0 never gets the index, because the store only calls
  `EnsureCreated` (#272).

**Why:** a same-key race needs two writers at once, which no sequential test has.

**How to apply:** a new store, or a new write path in an old one, ships all three parts and a test
that runs concurrent writes to one key and then asserts every version and exactly one latest row.

Gate:

- `DiskStorage_ConcurrentSameKeyPuts_PreserveEveryVersion`, plus the abort-versus-complete and
  same-part-number tests in `DiskStorageServiceTests`.
- `UpsertObjectAsync_ConcurrentWritesToSameKey_LeaveExactlyOneLatest`.

A new provider has no gate until the same-key test lives in `StorageProviderContractTests` (#268).
