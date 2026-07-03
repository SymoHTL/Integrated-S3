# Security Policy

IntegratedS3 exposes S3-compatible HTTP endpoints, handles request signing (SigV4/SigV4a), presigned URLs, and authorization decisions. Security reports are taken seriously and are appreciated.

## Supported versions

Only the latest published package line receives security fixes:

| Version | Supported |
|---|---|
| 10.0.x (latest release) | Yes |
| Older versions | No — please upgrade |

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions, or pull requests.**

Use GitHub's private vulnerability reporting instead:

1. Go to the repository's Security tab: <https://github.com/SymoHTL/Integrated-S3/security>
2. Click **Report a vulnerability** (or open <https://github.com/SymoHTL/Integrated-S3/security/advisories/new> directly).
3. Include as much of the following as you can:
   - affected package(s) and version(s) (e.g. `IntegratedS3.AspNetCore 10.0.4`)
   - the provider and configuration in play (disk / S3 / custom backend, SigV4 on or off)
   - reproduction steps or a proof-of-concept request
   - the impact you believe the issue has (e.g. authentication bypass, signature forgery, path traversal, information disclosure)

If you cannot use private vulnerability reporting, you may contact the maintainer directly via the email address on the [@SymoHTL](https://github.com/SymoHTL) GitHub profile. Please use a subject line starting with `[SECURITY] Integrated-S3`.

## What to expect

- **Acknowledgement** of your report within 7 days.
- **Assessment and triage** — we may ask follow-up questions to reproduce the issue.
- **Fix and disclosure** — validated vulnerabilities are fixed in the latest release line, published to NuGet.org, and disclosed via a GitHub Security Advisory. You will be credited in the advisory unless you prefer otherwise.

Please give us a reasonable window to remediate before any public disclosure.

## Scope notes

Reports are especially valuable in these areas:

- SigV4 / SigV4a signature validation and presigned URL verification
- authorization and bucket-policy evaluation
- object-key normalization and path handling in the disk provider
- multipart upload state handling
- XML/request parsing on the S3-compatible surface

Vulnerabilities in third-party dependencies should be reported upstream first; open a report here if IntegratedS3's usage of the dependency is what creates the exposure.
