---
name: release-and-consume
description: Release the IntegratedS3 NuGet packages and move PersonalS3 onto them. Covers picking the version, the VersionPrefix bump and CHANGELOG commit, a probe pack built into PersonalS3, the nuget-publish.yml dry run and real publish, checking nuget.org, the tag and the GitHub Release, then the PersonalS3 pin bump with its local-cache and AOT checks. Use when publishing or releasing IntegratedS3, bumping the version, running nuget-publish.yml, or updating PersonalS3 to a new IntegratedS3 version.
---

# Release IntegratedS3, then move PersonalS3 onto it

nuget.org versions are immutable, so every check that can stop a release runs before the push in
step 4. Why each step exists, with the runs that went wrong: `knowledge/nuget-release-postmortem.md`.
One-time setup (the `NUGET_API_KEY` secret): `docs/nuget-publishing.md`.

## 0. Decide the version

1. What ships: `git fetch origin --tags && git log --oneline v<last>..origin/main`. Tags start at
   `v11.0.0`.
2. It is a major if the diff holds a break from the list in
   `knowledge/public-interface-member-is-a-major.md`. Nothing in CI packs or API-diffs (HAZARD,
   #270), so read the diff of every shipped project and of the central props:
   `git diff v<last>..origin/main -- 'src/IntegratedS3/Directory.*.props' 'src/IntegratedS3/IntegratedS3.*' ':!src/IntegratedS3/IntegratedS3.Tests' ':!src/IntegratedS3/IntegratedS3.E2E.Tests' ':!src/IntegratedS3/IntegratedS3.Benchmarks'`.
   PersonalS3 implements interfaces from Core (`IStorageCatalogStore`) and AspNetCore
   (`IIntegratedS3CredentialResolver`), not only from Abstractions.
3. CI on `origin/main`'s head sha finished green:
   `gh run list -R SymoHTL/Integrated-S3 --workflow ci.yml --commit "$(git rev-parse origin/main)" --json databaseId,status,conclusion`.
   A cancelled run proves nothing; `gh run rerun <id> -R SymoHTL/Integrated-S3` and wait.

## 1. Release commit, in a PR of its own

1. A worktree off a fresh `origin/main`, branch `release/<version>`.
2. `pwsh -File eng/Bump-Version.ps1 -Version <version>` (or `-Part Major|Minor|Patch`). It
   changes only `VersionPrefix` in `src/IntegratedS3/Directory.Build.props`.
3. By hand (#269): the supported-versions table and the version example in `SECURITY.md`, and the
   version example in `.github/ISSUE_TEMPLATE/bug_report.yml`.
4. `CHANGELOG.md`: rename `## [Unreleased]` to `## [<version>] - <YYYY-MM-DD>` and open a new
   empty `## [Unreleased]` above it. A major lists what a consumer must do: new abstract members
   to implement, and schema changes with their migration (`EnsureCreated` alters nothing, #272).
5. Nothing else goes in this commit. PR titled `release: <version>`, CI green on its head sha. Do
   not merge it before step 2 passes.

## 2. Probe PersonalS3, before merging the release PR

Nothing else shows that a release breaks the consumer. Pack under a prerelease version nothing has
used, because restore never replaces a cached version (PersonalS3 #98):

1. Pick `<n>` so that `ls ~/.nuget/packages/integrateds3.core/` has no `<version>-probe.<n>`.
2. In the release worktree:
   `dotnet pack src/IntegratedS3/IntegratedS3.slnx -c Release -o <scratch>/probe --version-suffix probe.<n>`.
3. In a PersonalS3 worktree off a fresh `origin/master`, set both pins in
   `Directory.Packages.props` to `<version>-probe.<n>`, then build with
   `dotnet build PersonalS3.sln -c Release --source <scratch>/probe --source https://api.nuget.org/v3/index.json`
   and run the fast gate from PersonalS3's `CLAUDE.md`. A break found in step 0 fails here; a minor
   or patch passes.
4. Throw the probe away: `git checkout -- Directory.Packages.props` in that worktree, and
   `rm -rf ~/.nuget/packages/integrateds3.*/<version>-probe.<n>`.
5. Squash-merge the release PR.

## 3. Dry run

1. `gh workflow run nuget-publish.yml -R SymoHTL/Integrated-S3 --ref main -f push-to-nuget=false -f dry-run=true`.
   Its `validate` job runs the full solution tests and the AOT script.
2. `gh run download <run-id> -R SymoHTL/Integrated-S3 -n nuget-packages -D <scratch dir>` holds one
   `*.<version>.nupkg` per row of `LayeringConventionTests`. Any other version means the bump is not
   on `main`.
3. nuget.org does not list `<version>` for any id yet: a duplicate is skipped and the run still
   goes green. `curl -s https://api.nuget.org/v3-flatcontainer/integrateds3.<lowercase id suffix>/index.json`,
   for example `integrateds3.core`.

## 4. Publish

1. `main` has not moved since the dry run:
   `gh run view <dry-run-id> -R SymoHTL/Integrated-S3 --json headSha -q .headSha` equals the sha
   `git ls-remote origin refs/heads/main` prints. Otherwise dry-run again.
2. `gh workflow run nuget-publish.yml -R SymoHTL/Integrated-S3 --ref main -f push-to-nuget=true -f dry-run=false`.
   A dispatch cannot pin a sha, so check the new run's `headSha` the same way
   (`gh run list -R SymoHTL/Integrated-S3 --workflow nuget-publish.yml --limit 1 --json databaseId,headSha`)
   and cancel it during `validate` if it differs. It pushes with `--skip-duplicate`, tags
   `v<version>` and creates the GitHub Release. A green run does not prove a release: three green
   runs on 2026-04-07 shipped nothing.
3. If it fails after pushing some packages, rerun it on the same sha with
   `gh run rerun <run-id> -R SymoHTL/Integrated-S3 --failed`, never with a new dispatch.
4. nuget.org lists `<version>` for every id (the URL in step 3.3; indexing can take minutes).
5. `git fetch origin --tags && git rev-parse "v<version>^{commit}"` is the released sha, and
   `gh release view v<version> -R SymoHTL/Integrated-S3` has a `.nupkg` and a `.snupkg` per id.
6. The generated Release notes list PRs, not migrations. Replace them with the version's
   `CHANGELOG.md` section: `gh release edit v<version> -R SymoHTL/Integrated-S3 --notes-file <section>`.

## 5. Move PersonalS3 (`SymoHTL/PersonalS3`, branch `master`)

1. A worktree off a fresh `origin/master`, branch `chore/integrateds3-<version>`.
2. `grep -H '"source"' ~/.nuget/packages/integrateds3.*/*/.nupkg.metadata | grep -v api.nuget.org`
   must print nothing. A local pack under a released version shadows nuget.org, and restore never
   replaces it (PersonalS3 #98). Delete such a version folder before restoring.
3. Bump both pins in `Directory.Packages.props`, `IntegratedS3.Abstractions` and
   `IntegratedS3.AspNetCore`, to `<version>`. Restore without `--no-restore`.
4. A major: implement the new members (11.0.0 added four to `IStorageCatalogStore`, PersonalS3
   #82). A new column is a numbered schema migration, with a test seen red without it.
5. Build and test with the commands in PersonalS3's `CLAUDE.md`, including its warning ratchet.
   A package bump reaches the Native AOT binary, so dispatch `heavy`
   (`gh workflow run ci.yml -R SymoHTL/PersonalS3 --ref <branch> -f run-heavy=true`) and run the
   AOT binary as PersonalS3's `knowledge/aot-only-failures.md` shows, with one request through a
   path the release changed.
6. `CHANGELOG.md` `Unreleased`: the behaviour inherited from the release, linking the GitHub
   Release from step 4.6.
7. PR, then CI green on its head sha. CI restores from nuget.org, which a local build may not
   have done.

## Done when

- nuget.org lists `<version>` for every id, and the tag and the Release point at the release sha.
- PersonalS3's pin bump is merged with CI green. Deploying is a separate step, and production has
  run ahead of `master` before (PersonalS3 `knowledge/production-runs-ahead-of-master.md`).
