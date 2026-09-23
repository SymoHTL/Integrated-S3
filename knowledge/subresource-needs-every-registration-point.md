---
name: subresource-needs-every-registration-point
description: A new S3 subresource or query parameter has about ten registration points, and a missed one fails far from the change - a finished handler unreachable behind the Known*QueryParameters allow-list (#153), or a replicated operation the repair service cannot repair (#275).
metadata:
  type: project
---

A new subresource, like `?intelligent-tiering`, or a new query parameter touches all of these:

1. the `KnownBucketQueryParameters` / `KnownObjectQueryParameters` allow-lists in
   `IntegratedS3EndpointRouteBuilderExtensions`;
2. the dispatch arm and its handler;
3. the error mapping, including its `NoSuch*` code ([[s3-error-code-and-status-diverge]]);
4. `StorageOperationType`;
5. `AuthorizingStorageService`;
6. the replica write policy in `OrchestratedStorageService`;
7. the `StorageReplicaRepairService` switch;
8. both providers, or an explicit `NotImplemented`;
9. `docs/protocol-compatibility.md` and the reported `StorageCapabilities`.

History:

- **#153.** The Intelligent-Tiering handlers were finished, but the request
  validator rejected `?intelligent-tiering` before dispatch, so no client could reach them. PR #205
  (5216458) added it to the allow-list.
- **#148.** GET and HEAD rejected the `response-content-*` override parameters with 501, which broke
  presigned downloads. PR #202 (f33bf28).
- **#275.** 25 of the 39 operation types that `OrchestratedStorageService` hands to the replica
  write policy have no working repair arm. One failed replica write of such a type blocks
  write-through to that replica, or halts async replication.

**Why:** each registration point fails somewhere else: a 501 at the validator, a 403 at
authorization, a stuck repair backlog on a replica. Handler tests pass in all of these cases.

**How to apply:** walk the list above in the PR that adds the subresource, and add an HTTP test that
goes through the validator.

Gate: only the HTTP rows of each endpoint, and `GetObject_WithSingleResponseOverrideParam_DoesNotReturn501`.
Nothing cross-checks the points against each other. The scan in #275 and the item in #270 would.
