---
name: in-process-tests-miss-real-server-and-sdk
description: TestServer enforces no request body limit and HttpClient validates no response checksums, so #81 (Kestrel answered 413 above 28.6 MiB) and #233 (AWS SDK v4 rejected every ranged GET) passed the in-process suite. Test wire behaviour on the loopback Kestrel host with AmazonS3Client.
metadata:
  type: project
---

`WebUiApplicationFactory` (`IntegratedS3.Tests/Infrastructure`) starts the real `WebUi` host in
one of two ways:

| Method | Server | Client |
|---|---|---|
| `CreateClientAsync`, `CreateIsolatedClientAsync` | TestServer | `HttpClient` |
| `CreateLoopbackIsolatedClientAsync` | Kestrel on 127.0.0.1 | any, including `AmazonS3Client` |

TestServer is not Kestrel: it applies no `MaxRequestBodySize`. `HttpClient` is not the AWS SDK:
it validates no response checksums. Most of the suite uses the first row.

History:

- **#81** (filed 2026-07-02). Uploads over about 28.6 MiB failed with 413. Kestrel's default
  `MaxRequestBodySize` is 30,000,000 bytes, and nothing lifted it. The whole in-process suite had
  passed. PR #93 (1ff547f) lifted the limit.
- **#115 and #116**, filed the same day #93 merged. There was no default upload cap, so an upload
  could fill the disk (#115), and an aws-chunked line could grow without bound (#116).
  - PR #213 (d67c75b) shipped a 5 GiB default cap.
  - PR #181 (1e8e0f0) capped the line length.
- **#233**. A ranged GET (206) carried the whole-object `x-amz-checksum-*` header. The AWS SDK for
  .NET v4 validates response checksums by default, so every ranged GET failed with "Expected hash
  not equal to calculated hash". PR #236 (35aeaf0) fixed it.

**Why:** a limit, a header or a checksum that only a real server or a real SDK enforces is
invisible to TestServer and HttpClient.

**How to apply:**

- Test behaviour on the wire (body size, headers, checksums, signing, framing) with
  `CreateLoopbackIsolatedClientAsync` and `AmazonS3Client`.
- A PR that lifts a server default ships the replacement bound too.

Gate:

- `KestrelHostedPutObjectAndUploadPart_LargerThanDefaultBodyLimit_Succeed`, 32 MiB through
  Kestrel.
- The upload-cap tests: `PutObject_AwsChunkedBodyExceedingMaxObjectSizeBytes_ReturnsEntityTooLarge`
  and `PutObject_AwsChunkedDeclaredDecodedLengthExceedingCap_ReturnsEntityTooLargeBeforeSpooling`.
- `AmazonS3Client_RangedGetObject_SucceedsWithoutChecksumValidationFailure`.

A new wire feature has no gate unless its PR adds an SDK loopback test.
