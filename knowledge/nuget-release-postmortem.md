---
name: nuget-release-postmortem
description: How to release the 9 IntegratedS3 packages, and what went wrong before - the publish workflow goes green without shipping anything when VersionPrefix was not bumped (three green no-op runs on 2026-04-07), and nuget.org versions are immutable, so a mistake in a published package is permanent.
metadata:
  type: reference
---

**Recipe** (as used for 11.0.0):

1. Bump `VersionPrefix` in `src/IntegratedS3/Directory.Build.props` with `eng/Bump-Version.ps1`
   (`-Part Major|Minor|Patch`, or `-Version x.y.z`), in a release commit of its own. The script
   changes only that line.
2. In the same commit, move `CHANGELOG.md` `Unreleased` into the new version's section. Breaking
   changes include schema changes ([[public-interface-member-is-a-major]]).
3. Dispatch `nuget-publish.yml` with `dry-run` (the default). Its `validate` job runs the full
   solution tests and the AOT script; `pack-and-publish` packs and uploads the packages as an
   artifact.
4. Dispatch again with `push-to-nuget=true` and `dry-run=false`. The workflow pushes with
   `--skip-duplicate`, tags `v{version}` and creates the GitHub Release.
5. Move the consumers: SymoHTL/PersonalS3 bumps its pin in `Directory.Packages.props` (as in its
   PR #82).

**What went wrong before:**

- **Missing API key.** The first publish (run 23388247026, f23489c, 2026-03-21) failed because
  `NUGET_API_KEY` was empty. 6116b38 added the preflight that fails fast on a missing secret.
- **Three green runs that shipped nothing.** On 2026-04-07, publish runs at 3b0829c, f4a1a79 and
  e8fce9f all succeeded, but `VersionPrefix` was still 10.0.3, already on nuget.org since the
  2026-03-27 run. `--skip-duplicate` turned every push into a no-op. Only fd0f06f, which bumped to
  10.0.4, shipped those fixes.
- **A bump hidden in a refactor.** The 10.0.3 bump sat inside an EF refactor commit (8631da9).
- **A permanent typo.** The packages' project URL read "Intergrated-S3" in 10.0.0–10.0.3. 3b0829c
  fixed the repository, but published versions cannot be changed.
- **Reconstructed release notes.** The notes for 10.0.x were rebuilt from git history afterwards,
  and the 11.0.0 notes from 78 commits. Several of their claims do not match the code (#269).
- **One tag.** `v11.0.0` is the only release tag; earlier versions cannot be traced to a commit.

**Why:** a green publish run proves the workflow ran, not that a new version exists on nuget.org.

**How to apply:** after a publish, check nuget.org for the version
(`https://api.nuget.org/v3-flatcontainer/integrateds3.core/index.json`) before announcing it.

Gate:

- The API-key preflight.
- The tag-conflict step, which fails when `v{version}` already exists at another commit ("Bump
  VersionPrefix"). It runs after the push, so an unbumped run turns red only after a push that did
  nothing.
- CHANGELOG discipline has no gate (#270).
