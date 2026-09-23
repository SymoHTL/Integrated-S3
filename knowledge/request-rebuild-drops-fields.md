---
name: request-rebuild-drops-fields
description: Code that rebuilds a request by listing its properties silently drops every property added later. Replica repair lost object tags (#107), and write-through PutObject copies 9 of PutObjectRequest's 20 properties, so SSE and If-None-Match are dropped (#273).
metadata:
  type: project
---

`PutObjectRequest` (`IntegratedS3.Abstractions/Requests`) is a `sealed class` with `init`
properties, not a record, so there is no `with`. Every copy is written out field by field, and a
property added later is missing from each copy that nobody updates.

History:

- **#107.** Async replica repair re-PUT the object without its tags, so tags diverged between
  primary and replica for good. PR #190 (c87c434).
- **#273 (open).** `OrchestratedStorageService.PutBufferedObjectAsync` copies 9 of the 20
  properties. It drops `CacheControl`, `ContentDisposition`, `ContentEncoding`,
  `ContentLanguage`, `ExpiresUtc`, `Expires`, `ServerSideEncryption`, `CustomerEncryption`,
  `StorageClass`, `IfMatchETag` and `IfNoneMatchETag`.
  - It is used for the primary write in `WriteThroughAll`, and for every replica write.
  - In `WriteThroughAll`, an SSE PUT is stored unencrypted, and `If-None-Match: *` overwrites an
    existing object.
  - On 2026-07-04, #165 added `Expires` and #155 started persisting `StorageClass`; neither
    touched this method.

**Why:** a compiler never flags a property that a copy site leaves out, and a test that sets only
the properties it knows passes.

**How to apply:**

- Prefer one copy helper per request type, shared by every copy site.
- A PR that adds a property to a request type greps every `new <Type>` and every positional
  rebuild of that type.
- The test sets every settable property by reflection and asserts that each one arrives.

Gate: `StorageReplicaRepairService_RepairReplicaObject_PreservesPrimaryObjectTags`, for tags only.
The reflection test proposed in #273 is the real gate; #270 tracks the general case.
