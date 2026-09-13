# S3 API compliance audit, 2026-07-04

A full AWS S3 REST API compliance audit of IntegratedS3 against `main` as of 2026-07-04: a
fan-out of scoped finders, every finding re-verified against the code at the time by a second
pass that read the current source, then deduplicated and filed. This page keeps the verdict and
the map; the gaps themselves are issues, and the issue tracker is the only place their state is
kept.

## Verdict

Not 100 %. Roughly **80 %** parity overall: high on the common happy paths, about 70 to 75 % on
strict wire, error and auth-integrity behaviour. That is consistent with what the project claims:
[`protocol-compatibility.md`](protocol-compatibility.md) disclaims full wire parity, and the
"config only" subresources on the disk provider (lifecycle, replication, website, logging,
notification, analytics, metrics, inventory) are intentional non-goals: wire-compliant and
behaviour-absent, not gaps.

## What was filed

108 confirmed gaps went into 21 issues, **#147 to #167** (all closed since). The parity
blockers among them, as found then:

- disk ETag was not an MD5 (#105, pre-existing)
- the multipart error family collapsed to `409 InvalidRequest` (#147)
- response-override query parameters answered `501` (#148)
- `max-keys` was unclamped, and a bad parameter answered `500` (#149, #150)
- the SigV4a `+1` offset was broken (#103)
- `x-amz-version-id` leaked on unversioned buckets (#151)
- systemic: config-not-found mapped to `InternalError` instead of the matching `NoSuch*` code,
  one fix in `ToS3ErrorCode` (#152)

## Where to start next time

The real S3 surface is the `/{**s3Path}` catch-all dispatch in
`src/IntegratedS3/IntegratedS3.AspNetCore/Endpoints/IntegratedS3EndpointRouteBuilderExtensions.cs`
(over 600 KB: grep it, do not read it whole). Compliance work starts from the issues in the
#101 to #167 range and that file, not from a new audit, and a new parity issue is deduplicated
against that range, open and closed, before it is filed.
