---
name: sigv4-fix-needs-sigv4a-twin
description: SigV4 and SigV4a are twin code paths tested mostly by sign-then-verify round trips, so a fix can land in one twin only, and a mistake shared by signer and verifier passes both - #103's key derivation, and SigV4a signatures in P1363 where AWS uses DER (#276).
metadata:
  type: project
---

`AwsSignatureV4RequestAuthenticator` keeps a SigV4 block and a SigV4a block for header
authentication and for presigned URLs. `S3SigV4aSigner` both signs and verifies. A round trip
through the repo's own signer passes as long as signer and verifier agree, even when both are
wrong.

History:

- **#103.** The SigV4a ECDSA key derivation missed the `+1` scalar offset that aws-c-auth applies.
  Signer and verifier shared the mistake, so every SigV4a test was green while no real client could
  authenticate. PR #169 (a4520a9) fixed it and added the first external vector,
  `DeriveEcdsaKey_MatchesAwsCrtKnownAnswerVector`.
- **#132 (ce9a531), #133 (7f4c4e0), #161 (a6fe538).** Each needed the same edit in the SigV4 block
  and the SigV4a block of `AwsSignatureV4RequestAuthenticator`.
  - The SigV4a presign expiry test (`SigV4aPresignedQueryAuthentication_ExpiredUrl_ReturnsAccessDenied`)
    signs with a 1-second expiry, 20 minutes in the past. It passes with or without #132's fix.
  - The SigV4 twin, `SigV4PresignedQueryAuthentication_ExpiredWithinClockSkewGrace_ReturnsXmlError`,
    does pin the boundary.
- **#276 (open).** `S3SigV4aSigner` signs and verifies in IEEE P1363 format. AWS (aws-c-auth,
  aws-sdk-go-v2, smithy-typescript) uses ASN.1 DER, and pads chunk signatures with `*`. So a real
  SigV4a client gets 403. Two tests pin the wrong format: they assert a signature length of 128 hex
  characters.

**Why:** crypto that is only checked against itself proves consistency, not correctness. Twin code
paths drift one edit at a time.

**How to apply:**

- Pin every signing format with an externally produced vector, such as the aws-c-auth test suite
  (`tests/aws-signing-test-suite/v4a`).
- A SigV4 fix lands in the SigV4a block in the same PR, with the twin's own boundary test.

Gate: `DeriveEcdsaKey_MatchesAwsCrtKnownAnswerVector`, for the key only. #276 adds the signature
vectors; #270 tracks the twin boundary tests.
