---
name: absent-state-treated-as-success
description: Seven times a missing input - credentials, an authorization policy, a signing context, a chunk-chain check, a body-hash check, a resolved version, health data - meant "nothing to check" and the request succeeded (#82, #86, #101, #114, #126, #127, #131); replica writes still do it (#274).
metadata:
  type: project
---

The shape: code checks a thing only if the thing is present, and treats its absence as a pass.
Every instance below was a security or data-integrity defect. All seven were filed 2026-07-02/03
and closed by 2026-07-04:

| Issue | What was missing | What happened |
|---|---|---|
| #82 | credentials | an unsigned request passed as anonymous |
| #86 | an authorization policy | everything was allowed |
| #114 | a signing context | signed streaming was accepted |
| #101 | a per-chunk signature check | the chunk bytes were not bound to the signature |
| #131 | a body-hash check | the signed `x-amz-content-sha256` was never compared with the body |
| #126 | a resolved version | a replica tag write got the raw request version |
| #127 | health data | a failure stamped the backend Healthy |

Still open: #274. Replica tag writes with an explicit `VersionId`, and replica deletes, return
success without changing the replica, and the repair backlog records nothing.

Allow-all is still the default. `AddIntegratedS3Core` registers
`AllowAllIntegratedS3AuthorizationService` "for backward compatibility". Scope enforcement is the
opt-in `AddIntegratedS3ScopeBasedAuthorization`, which no host in this repo calls. `CHANGELOG.md`
says authorization is scope-based (#86); see #269.

**Why:** a missing input looks like "nothing to do" to the code that should have failed. No test
fails, because the tests supply the input.

**How to apply:** for every check, write the test where the input is absent, and assert that the
request is rejected or that a failure is recorded, never only `IsSuccess`. "Not found" counts as
success only when the thing looked up is known to be the right one.

Gate:

- `UnsignedRequest_WithSigV4Enabled_IsRejectedWith403` (#82);
- `PutObject_WithTrailerBackedPayloadHashAndTrailerSignatureButNoSigningContext_ReturnsAccessDenied`
  (#114);
- `PutObject_WithSignedContentSha256NotMatchingBody_ReturnsXAmzContentSHA256Mismatch` (#131);
- the tampered aws-chunked tests from #208.

The replica paths have no gate; #274 adds them.
