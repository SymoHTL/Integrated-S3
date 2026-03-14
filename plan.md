# IntegratedS3 last-20-percent execution plan

This file is for AI agents and contributors closing the remaining gap between the current implementation and a broadly functional, credible S3-compatible backend.

It intentionally does **not** restate the entire architecture guide. Use `docs/integrated-s3-implementation-plan.md` as the source of truth for full background, and use this file as the execution board for the remaining high-value work.

## Mission

Close the remaining protocol, capability, and conformance gaps that block a reasonable claim of broad S3 compatibility.

The current repository already has a strong vertical slice:

- bucket CRUD
- object CRUD
- range and conditional reads
- copy object
- multipart initiate/upload/complete/abort
- list uploads
- tags
- versioning and delete markers
- bucket CORS and location
- SigV4 header and presigned-query auth
- virtual-hosted-style routing
- managed SSE (`AES256`, `aws:kms`, `aws:kms:dsse`)
- meaningful AWS SDK compatibility coverage

The remaining work is the hard tail: advanced S3 semantics, stricter fidelity, and real-world client compatibility.

## Definition of done

Do **not** claim completion when a feature compiles. A slice is done only when all of the following are true:

1. The behavior is implemented in shared contracts, Core orchestration, relevant providers, and the ASP.NET S3-compatible surface where applicable.
2. Unsupported permutations fail explicitly with correct S3-style errors instead of silently degrading.
3. Disk and native S3 providers report capabilities/support-state honestly.
4. Tests cover unit, integration, and compatibility behavior.
5. `docs/integrated-s3-implementation-plan.md` is updated with the new status and next best step.
6. Validation passes:
   - `dotnet build src/IntegratedS3/IntegratedS3.slnx`
   - `dotnet test src/IntegratedS3/IntegratedS3.slnx`
   - `dotnet publish -c Release --self-contained src/IntegratedS3/WebUi/WebUi.csproj`

## Non-negotiable working rules

- Keep public contracts provider-agnostic.
- Preserve trimming and native AOT friendliness.
- Prefer streaming-first behavior; avoid buffering full objects unless the protocol forces it.
- Do not reopen package boundaries casually.
- Do not make disk-provider sidecars a shared abstraction requirement.
- Unsupported S3 features must return explicit `NotImplemented` or the canonical modeled error, not quiet fallback behavior.
- Keep changes small, testable, and issue-scoped.

## Priority order

### P0 — compliance-critical gaps

These are the biggest blockers to claiming broad S3 compatibility.

1. **Object Lock / Retention / Legal Hold**
2. **Advanced multipart parity** (`ListParts`, `UploadPartCopy`, remaining multipart edge cases)
3. **ACL / bucket policy parity beyond the current public-read/public-list slice**
4. **Encryption parity beyond the managed SSE slice**
5. **SigV4 and checksum edge-case parity**

### P1 — conformance and fidelity hardening

1. Versioning / tagging / delete-marker edge cases
2. XML namespace and response fidelity
3. Multi-delete integrity and limits
4. Broader bucket/object subresource coverage
5. Local S3-compatible endpoint hardening against MinIO / LocalStack / similar targets

### P2 — production hardening after parity slices land

1. Broader multi-provider fault injection
2. Multipart behavior under replicated write modes
3. Richer repair and reconciliation semantics
4. Benchmark and release-polish follow-through

## Agent workstreams

Run these as separate PR-sized slices. Avoid overlapping the same files unless the dependency order below requires it.

---

## Workstream A — Object Lock, Retention, and Legal Hold

### Why this matters

This is the largest missing compliance feature family. The capability matrix still marks this area as not started / unsupported.

### Scope

Implement provider-agnostic abstractions and current HTTP semantics for:

- object lock configuration model
- retention mode and retain-until date
- legal hold state
- required request/response headers and XML where applicable
- correct unsupported behavior where a backend cannot support the feature

### Expected files

- `src/IntegratedS3/IntegratedS3.Abstractions/**/*`
- `src/IntegratedS3/IntegratedS3.Core/**/*`
- `src/IntegratedS3/IntegratedS3.AspNetCore/**/*`
- `src/IntegratedS3/IntegratedS3.Provider.Disk/**/*`
- `src/IntegratedS3/IntegratedS3.Provider.S3/**/*`
- `src/IntegratedS3/IntegratedS3.Tests/**/*`
- `docs/integrated-s3-implementation-plan.md`

### Acceptance criteria

- capability and support-state reporting distinguish native, emulated, delegated, and unsupported status honestly
- object routes support the modeled retention/legal-hold subresources or return explicit S3-style unsupported responses
- delete and overwrite semantics respect retention rules where supported
- tests cover current object, versioned object, and explicitly unsupported backend flows

### Suggested issue mapping

- `#29` object lock, retention, and legal hold

---

## Workstream B — Multipart parity completion

### Why this matters

Multipart is good but not complete. This is one of the biggest SDK compatibility gaps.

### Scope

Implement:

- `ListParts`
- `UploadPartCopy`
- remaining multipart listing and marker edge cases
- stronger multipart XML fidelity
- explicit behavior for unsupported multipart combinations

### Expected files

- `src/IntegratedS3/IntegratedS3.Abstractions/Requests/**/*`
- `src/IntegratedS3/IntegratedS3.Abstractions/Models/**/*`
- `src/IntegratedS3/IntegratedS3.Abstractions/Services/**/*`
- `src/IntegratedS3/IntegratedS3.Protocol/**/*`
- `src/IntegratedS3/IntegratedS3.Core/**/*`
- `src/IntegratedS3/IntegratedS3.AspNetCore/**/*`
- `src/IntegratedS3/IntegratedS3.Provider.Disk/**/*`
- `src/IntegratedS3/IntegratedS3.Provider.S3/**/*`
- `src/IntegratedS3/IntegratedS3.Testing/**/*`
- `src/IntegratedS3/IntegratedS3.Tests/**/*`
- `docs/integrated-s3-implementation-plan.md`

### Acceptance criteria

- `GET ?uploadId=...` supports `ListParts`
- `PUT ?partNumber=...&uploadId=...` with copy-source semantics supports `UploadPartCopy`
- disk and native S3 providers both implement or explicitly reject unsupported permutations with correct errors
- AWS SDK compatibility tests or local conformance tests cover both paths

### Suggested issue mapping

- `#35` object and multipart list API parity
- `#36` `ListParts`
- `#37` `UploadPartCopy`

---

## Workstream C — ACL and bucket policy parity expansion

### Why this matters

The current ACL/policy implementation is intentionally narrow. That is useful, but not enough for broad S3 compatibility.

### Scope

Extend the current compatibility layer for:

- more grant-header semantics
- version-specific ACL decisions where modeled
- multipart-initiation ACL handling where appropriate
- richer bucket policy parsing/validation for supported safe subsets
- explicit rejection of still-unsupported IAM semantics

### Guardrails

Do **not** replace `ClaimsPrincipal`-based authorization with raw IAM emulation. Keep the current model: S3 compatibility layered over the storage authorization service.

### Acceptance criteria

- broader canned ACL and grant-header handling is implemented or rejected explicitly
- policy parsing is stricter and more S3-like
- tests cover public, private, version-aware, malformed, and unsupported-policy scenarios

### Suggested issue mapping

- follow-on to the current ACL/policy slice in Track G

---

## Workstream D — Encryption parity beyond current managed SSE

### Why this matters

Managed SSE is present, but full S3 encryption parity is not.

### Scope

Prioritize:

1. **SSE-C** (`x-amz-server-side-encryption-customer-*`)
2. bucket default encryption behavior if it is the next lowest-risk control-plane slice
3. any remaining response/header fidelity needed for copy/get/head/multipart paths

### Acceptance criteria

- request models remain provider-agnostic
- unsupported backends reject unsupported SSE shapes explicitly
- native S3 provider maps supported encryption flows cleanly
- tests cover put, get, head, copy, and multipart initiation/upload behavior as applicable

### Suggested issue mapping

- `#27` broader server-side-encryption variants
- `#40` next bucket-subresource surface expansion

---

## Workstream E — SigV4, checksum, and streaming edge-case hardening

### Why this matters

The remaining failures here are subtle but important for real client compatibility.

### Scope

Finish the high-friction edge cases:

- temporary session credentials / session token handling
- trailer-backed `aws-chunked` checksum/signature flows
- checksum override edge cases
- conditional precedence corner cases
- canonical-request and signed-query edge cases still not covered

### Acceptance criteria

- added coverage includes raw signed requests, presigned requests, and `aws-chunked` upload permutations
- incorrect combinations fail with correct S3-style errors
- no regression in existing path-style and virtual-hosted-style compatibility tests

### Suggested issue mapping

- `#23` conditional/checksum/canonical-request hardening
- `#24` `aws-chunked`, presigned-query, and virtual-hosted-style compatibility

---

## Workstream F — Versioning, tagging, and delete-marker hard tail

### Why this matters

This area is close, but the remaining odd cases are exactly the ones that break compatibility claims.

### Scope

- finish remaining `NoSuchVersion` fidelity decisions
- harden delete-marker behavior across single-object and batch-delete paths
- complete broader tag-character and validation parity
- verify historical-version edge cases across copy/delete/head/get/tagging flows

### Acceptance criteria

- explicit-version and current-version behavior are differentiated correctly
- delete-marker responses match expected status/header/XML semantics
- tag validation matches the documented supported rule set
- tests cover disk, HTTP, and AWS SDK compatibility paths

### Suggested issue mapping

- `#25` versioning/tagging/delete-marker parity

---

## Workstream G — XML, subresource, and multi-delete fidelity

### Why this matters

This is the polish layer that decides whether standard clients keep working when they drift beyond the happy path.

### Scope

- canonical XML namespace fidelity
- multi-delete integrity and limits
- broader bucket/object subresource recognition and rejection behavior
- response-shape parity for remaining implemented operations

### Acceptance criteria

- XML payloads include the expected namespaces where required
- multi-delete handles mixed versioned/non-versioned errors correctly
- unsupported mixed subresource combinations continue to fail consistently

### Suggested issue mapping

- `#31` bucket/object subresource matrix hardening
- `#32` canonical S3 XML namespace parity
- `#34` multi-delete integrity and object-count limits

---

## Workstream H — Real-endpoint conformance harness

### Why this matters

A feature is not done until it survives a real S3-compatible target and a real client.

### Scope

Expand the current conformance harness and local-endpoint validation against:

- MinIO
- LocalStack
- any compatible local test target already supported by `INTEGRATEDS3_S3COMPAT_*`

Focus on:

- multipart advanced flows
- SSE variants
- checksums
- versioning edge cases
- presigned behavior
- virtual-hosted-style routing

### Acceptance criteria

- conformance tests clearly label intentionally unsupported behavior vs regressions
- new failures found against local S3-compatible targets are either fixed or documented explicitly in the capability matrix

### Suggested issue mapping

- `#21` local S3-compatible endpoint hardening
- `#30` broader protocol/client conformance

## Dependency order

Use this order unless a smaller isolated slice is obviously safe to run earlier.

1. **Workstream E** — SigV4/checksum/canonical-request hardening
2. **Workstream B** — Multipart parity completion
3. **Workstream F** — Versioning/tagging/delete-marker tail
4. **Workstream G** — XML/subresource/multi-delete fidelity
5. **Workstream D** — Encryption expansion
6. **Workstream C** — ACL/policy expansion
7. **Workstream A** — Object lock/retention/legal hold
8. **Workstream H** — continuous conformance expansion in parallel with all of the above

Rationale:

- E, B, F, and G close the highest-probability client-compatibility gaps first.
- D and C expand advanced behavior after the protocol surface is tighter.
- A is the largest architectural feature and should land once the protocol and provider seams are calmer.
- H should run throughout as the regression net.

## Agent prompt template

Use this template when assigning a slice to an AI agent:

- **Goal:** implement `<workstream name>` in a PR-sized slice
- **Do not:** reopen unrelated abstractions, reformat unrelated files, or weaken AOT/trimming posture
- **Must update:** tests, `docs/integrated-s3-implementation-plan.md`, capability reporting if changed
- **Must validate:** build, test, publish
- **Return:**
  - files changed
  - behaviors added
  - unsupported behaviors kept explicit
  - tests added/updated
  - validation results
  - any follow-on issues discovered

## Suggested PR granularity

Keep PRs small enough that a reviewer can verify them quickly.

Good:

- `ListParts` only
- `UploadPartCopy` only
- SSE-C request model + HTTP parsing + S3 provider mapping
- session-token SigV4 support only
- XML namespace parity only

Bad:

- “finish S3 compatibility” mega-PRs
- mixing object lock, ACL overhaul, SSE-C, and multipart parity in one change

## What not to do

- Do not claim full S3 compliance before Workstreams A through G are substantially complete.
- Do not hide unsupported semantics behind silent success.
- Do not couple provider-native state to Core abstractions when support state can be delegated or platform-managed.
- Do not break existing disk-provider standalone behavior while improving native S3 parity.

## First recommended execution batch

If multiple agents are available now, start with:

1. **Agent 1:** Workstream E — SigV4/checksum/canonical-request hardening
2. **Agent 2:** Workstream B — Multipart parity completion
3. **Agent 3:** Workstream F — Versioning/tagging/delete-marker hard tail
4. **Agent 4:** Workstream H — real-endpoint conformance expansion

Then queue:

5. **Agent 5:** Workstream D — encryption expansion
6. **Agent 6:** Workstream C — ACL/policy expansion
7. **Agent 7:** Workstream A — object lock/retention/legal hold
8. **Agent 8:** Workstream G — XML/subresource/multi-delete fidelity

## End-state target

The repository can credibly describe itself as a broadly functional S3-compatible backend when:

- advanced multipart semantics are present
- object lock / retention / legal hold are modeled and enforced where supported
- ACL/policy compatibility is broader and explicit about limits
- encryption coverage extends beyond the current managed slice
- SigV4/checksum edge cases are hardened
- versioning/delete-marker/tagging edge cases are tightened
- XML and subresource behavior are more faithful
- real-endpoint conformance coverage is continuously exercised

Until then, describe the project as **strong S3-compatible coverage with explicit advanced-feature gaps**, not full parity.
