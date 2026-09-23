---
name: public-interface-member-is-a-major
description: A new abstract member on a public interface, or a new EF column or index, breaks every consumer, and nothing in CI packs, API-diffs or migrates. 11.0.0 did both; EF databases created by 10.0.x fail on object reads and writes after the upgrade (#272).
metadata:
  type: project
---

11.0.0 (PR #259, 01a7a4c, 2026-09-13) is the reference case.

**Interfaces.** It added abstract members that every custom implementation must write, and the
major bump was right:

- four on `IStorageCatalogStore`;
- three on `IStorageService`;
- `RevertToPendingAsync` on `IStorageReplicaRepairBacklog`.

`IStorageBackend` members ship with `NotImplemented` default implementations instead, so backends
compile unchanged. SymoHTL/PersonalS3 PR #82 implemented the catalog members in its Dapper store.
It needed its own schema migration for the new `PrimaryVersionId` column, seen red as 5 tests
failing with `no such column: PrimaryVersionId`. It also found that its null-filter semantics had
drifted from the EF store's.

**EF schema.** The same release added four columns and two indexes to the EF catalog model (#272):

- `PrimaryVersionId`, `StorageClass` and a `Version` concurrency token on `IntegratedS3Objects`;
- `StorageClass` on `IntegratedS3MultipartUploads`;
- the single-latest-per-key unique index;
- an index on (`ProviderName`, `BucketName`, `Key`, `PrimaryVersionId`).

The EF stores create their schema with `EnsureCreated`, which does nothing on an existing database,
and the package ships no migrations. So a database created by 10.0.x fails every object and
multipart catalog operation after the upgrade, and the Breaking section never mentions it.

**Package dependencies are public API too.**

- #144 removed the `xunit` meta-package from the shipped `IntegratedS3.Testing`.
- Dependabot's grouped #187 (95d587a) then added a `Microsoft.Extensions.Logging` reference to it,
  which nothing in the project uses.

**Why:** consumers find these breaks, in production, after the release. The package on nuget.org
cannot be changed ([[nuget-release-postmortem]]).

**How to apply:**

- A break ships a major version, and its PR says so under Breaking in `CHANGELOG.md`, with the
  consumer's migration step. A break is any of:
  - a public member removed or changed;
  - an abstract member added to a public interface in any shipped package, Core and AspNetCore
    included;
  - a column or index added to the EF model;
  - a package dependency added, removed or moved to a new major;
  - a changed default behaviour.
- Before the release, build the consumer (PersonalS3) against a probe pack and see it fail first:
  step 2 of the `release-and-consume` skill.

Gate: none. #270 tracks package validation against the last release (`EnablePackageValidation`);
#272 adds the schema upgrade test and a schema snapshot.
