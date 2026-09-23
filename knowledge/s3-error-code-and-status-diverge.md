---
name: s3-error-code-and-status-diverge
description: An S3 error's HTTP status and its XML <Code> come from two independent hand-kept mappings, so a path that gets one right can ship a wrong but plausible error; seven wrong-error defects so far. Test both through the endpoint, with a fake that leaves SuggestedHttpStatusCode unset.
metadata:
  type: project
---

`ToErrorResult` in `IntegratedS3EndpointRouteBuilderExtensions` builds the S3 error response for a
`StorageError` (other call sites pass a literal code to its second overload):

- the status is `error.SuggestedHttpStatusCode ?? ToStatusCode(error.Code)`, and `ToStatusCode`
  ends in `_ => 500`;
- the `<Code>` is `ToS3ErrorCode(error.Code)`, which ends in `_ => "InternalError"`, except for
  a missing explicit version, which answers `NoSuchVersion`.

The two switches sit next to each other and are kept by hand. `SuggestedHttpStatusCode` overrides
only the status. So a service that sets the status right and the code wrong, or the reverse, sends
an error a client will act on. The S3 provider keeps a third copy, in reverse: `S3ErrorTranslator`
turns AWS error codes back into `StorageErrorCode`s. A 404 it does not recognise, on a request
without an object key, becomes `BucketNotFound`, which goes out as `NoSuchBucket` (#261).

History, all closed on 2026-07-04:

- #152: "bucket configuration not found" answered `InternalError`.
- #147: the multipart error family collapsed to 409 `InvalidRequest`.
- #139: every 409 was labelled `BucketAlreadyExists`.
- #164: `QuotaExceeded` answered 413 `EntityTooLarge`, and `BucketAlreadyOwnedByYou` was not
  distinguished.
- #150: an invalid `max-keys` answered 500 instead of 400 `InvalidArgument`.
- #157: an unhandled exception answered a non-S3 500 with no `<Error>` body.
- #118: a duplicate `Authorization` parameter answered 500 instead of 400.

Not all of them were the two maps disagreeing: #118, #150 and #157 were unhandled exceptions
that answered 500.

Still open: #261 (the reverse map, and `EntityTooSmall` going out as
`InvalidRequest`) and #278 (duplicate tag keys in bucket-configuration XML answer 500).

**Why:** a test that asserts only the `StorageErrorCode`, or only the status, stays green while the
client sees the wrong error.

**How to apply:**

- An error test goes through the HTTP endpoint and asserts both the XML `<Code>` and the status.
- Its fake service leaves `SuggestedHttpStatusCode` unset, so the mapping itself runs. Copy
  `AbsentBucketConfigStorageService` in `IntegratedS3HttpEndpointsTests`.
- A new `StorageErrorCode` gets its forward pair and its `S3ErrorTranslator` arm in the same PR.

Gate: only the `InlineData` rows of
`S3CompatibleBucketSubresource_WhenConfigAbsent_ReturnsNoSuchCodeWithNotFoundStatus`. Nothing
covers the whole enum; #261 proposes the theory that would.
