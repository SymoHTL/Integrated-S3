# NuGet Publishing

The release procedure, from picking the version to moving PersonalS3 onto it, is the
`release-and-consume` skill: [`.claude/skills/release-and-consume/SKILL.md`](../.claude/skills/release-and-consume/SKILL.md).
This page holds only the one-time setup it assumes.

## Prerequisites

- **`NUGET_API_KEY`**, a repository secret in **Settings → Secrets and variables → Actions**:
  - generate it at [nuget.org/account/apikeys](https://www.nuget.org/account/apikeys);
  - scope: push new packages and package versions;
  - glob pattern: `IntegratedS3.*`.
- Without the secret, a non-dry-run publish fails in its preflight, before any package is pushed.
  Add the secret, then rerun the workflow.

## Prereleases

The workflow's `version-suffix` input appends a prerelease label (`preview.1`, `rc.1`) to
`VersionPrefix`. Everything else follows the skill.
