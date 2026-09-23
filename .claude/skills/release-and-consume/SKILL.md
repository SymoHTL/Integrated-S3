---
name: release-and-consume
description: Release the IntegratedS3 NuGet packages and move PersonalS3 onto them. Covers picking the version, the VersionPrefix bump and CHANGELOG commit, the nuget-publish.yml dry run and real publish, checking nuget.org, the tag and the GitHub Release, then the PersonalS3 pin bump with its local-cache and AOT checks. Use when publishing or releasing IntegratedS3, bumping the version, running nuget-publish.yml, or updating PersonalS3 to a new IntegratedS3 version.
---

# Release IntegratedS3, then move PersonalS3 onto it

nuget.org versions are immutable, so every check below comes before the push. Why each step
exists, with the runs that went wrong: `knowledge/nuget-release-postmortem.md`.

## 0. Decide the version

1. What ships: `git fetch origin --tags && git log --oneline v<last>..origin/main`. `v11.0.0` is
   the only tag; older versions cannot be traced to a commit.
2. It is a major if, since the last tag, a public interface gained an abstract member or the EF
   stores gained a column or index (`knowledge/public-interface-member-is-a-major.md`). Read
   `git diff v<last>..origin/main -- src/IntegratedS3/IntegratedS3.Abstractions src/IntegratedS3/IntegratedS3.EntityFramework`:
   nothing in CI packs or API-diffs (HAZARD, #270).
3. CI on `origin/main`'s head sha finished green, not cancelled:
   `gh run list -R SymoHTL/Integrated-S3 --workflow ci.yml --branch main --limit 3`.

## 1. Release commit, in a PR of its own

1. A worktree off a fresh `origin/main`, branch `release/<version>`.
2. `pwsh -File eng/Bump-Version.ps1 -Version <version>` (or `-Part Major|Minor|Patch`). It
   changes only `VersionPrefix` in `src/IntegratedS3/Directory.Build.props`.
3. `CHANGELOG.md`: rename `## [Unreleased]` to `## [<version>] - <YYYY-MM-DD>` and open a new
   empty `## [Unreleased]` above it. A major lists what a consumer must do: new abstract members
   to implement, and schema changes with their migration (`EnsureCreated` alters nothing, #272).
4. Nothing else goes in this commit. PR titled `release: <version>`, CI green on its head sha,
   squash-merge.

## 2. Dry run

1. `gh workflow run nuget-publish.yml -R SymoHTL/Integrated-S3 --ref main` runs with the
   defaults `push-to-nuget=false` and `dry-run=true`. Its `validate` job runs the full solution
   tests and the AOT script.
2. `gh run download <run-id> -R SymoHTL/Integrated-S3 -n nuget-packages -D <scratch dir>` must
   hold 9 `.nupkg` files, each named `*.<version>.nupkg`. The 9 ids are the rows of
   `LayeringConventionTests`. Any other version means the bump is not on `main`.

## 3. Publish

1. On the sha the dry run packed (dry-run again if `main` moved):
   `gh workflow run nuget-publish.yml -R SymoHTL/Integrated-S3 --ref main -f push-to-nuget=true -f dry-run=false`.
   It pushes with `--skip-duplicate`, tags `v<version>` and creates the GitHub Release. A green
   run does not prove a release: three green runs on 2026-04-07 shipped nothing.
2. nuget.org lists the version for every id; indexing can take minutes:
   `curl -s https://api.nuget.org/v3-flatcontainer/integrateds3.<lowercase id suffix>/index.json`,
   for example `integrateds3.core`.
3. `git fetch origin --tags && git rev-parse "v<version>^{commit}"` is the released sha, and
   `gh release view v<version> -R SymoHTL/Integrated-S3` has 18 assets (9 `.nupkg`, 9 `.snupkg`).

## 4. Move PersonalS3 (`SymoHTL/PersonalS3`, branch `master`)

1. A worktree off a fresh `origin/master`, branch `chore/integrateds3-<version>`.
2. `grep -H '"source"' ~/.nuget/packages/integrateds3.*/*/.nupkg.metadata | grep -v api.nuget.org`
   must print nothing. A local pack under a released version shadows nuget.org, and restore never
   replaces it (PersonalS3 #98). Delete such a version folder before restoring.
3. Bump both pins in `Directory.Packages.props`, `IntegratedS3.Abstractions` and
   `IntegratedS3.AspNetCore`, to `<version>`. Restore without `--no-restore`.
4. A major: implement the new members (11.0.0 added four to `IStorageCatalogStore`, PersonalS3
   #82). A new column is a numbered schema migration, with a test seen red without it.
5. Build and test with the commands in PersonalS3's `CLAUDE.md`, including its warning ratchet.
   A package bump reaches the Native AOT image, so dispatch `heavy`
   (`gh workflow run ci.yml -R SymoHTL/PersonalS3 --ref <branch> -f run-heavy=true`) and run the
   image locally as PersonalS3's `knowledge/aot-only-failures.md` shows, with one request through
   a path the release changed.
6. `CHANGELOG.md` `Unreleased`: the behaviour inherited from the release, linking its notes.
7. PR, then CI green on its head sha. CI restores from nuget.org, which a local build may not
   have done.

## Done when

- nuget.org lists `<version>` for all 9 ids, and the tag and the Release point at the release sha.
- PersonalS3's pin bump is merged with CI green. Deploying is a separate step, and production has
  run ahead of `master` before (PersonalS3 `knowledge/production-runs-ahead-of-master.md`).
