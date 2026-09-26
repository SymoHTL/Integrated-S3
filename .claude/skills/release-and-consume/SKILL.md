---
name: release-and-consume
description: Release the IntegratedS3 NuGet packages and move PersonalS3 onto them. Covers picking the version, the VersionPrefix bump and CHANGELOG commit, a probe pack built into PersonalS3, the nuget-publish.yml dry run and real publish, checking nuget.org, the tag and the GitHub Release, then the PersonalS3 pin bump with its local-cache and AOT checks. Use when publishing or releasing IntegratedS3, bumping the version, running nuget-publish.yml, or updating PersonalS3 to a new IntegratedS3 version.
---

# Release IntegratedS3, then move PersonalS3 onto it

nuget.org versions are immutable, so every check that can stop a release runs before the push in
step 4. Why each step exists, with the runs that went wrong: `knowledge/nuget-release-postmortem.md`.
One-time setup (the `NUGET_API_KEY` secret): `docs/nuget-publishing.md`. This covers a stable
release; a prerelease (the workflow's `version-suffix` input) has no written procedure.

From step 1 until step 4.5, nothing else merges into `main`: a PR merged in between ships in the
release without step 0's check or the probe. Nothing enforces that (HAZARD, #314), so steps 2.5,
3.1, 4.1 and 4.2 check it.

To start again at step 0, which several steps below ask for: close the open release PR, if any,
and use the branch `release/<version>-2` (the old branch stays: this repo deletes no branch on
merge, and closing a PR keeps its branch). If step 0 now calls a major, bump again and rename the unreleased `<version>` section to
the new version. Otherwise the new release PR moves the new `Unreleased` lines into the
`<version>` section, or is an empty commit `release: <version>` when there is nothing to move, so
that it still has a merge commit for steps 3.1 and 4.1.

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
5. Nothing else goes in this commit. PR titled `release: <version>`, CI green on its head sha,
   and its verification passes (`CLAUDE.md`, Git & PRs). They check step 0's major-or-minor call
   against the diff, and the `CHANGELOG.md` section against step 0.1's `git log`, because step 4.6
   makes that section the Release notes. Do not merge it before step 2 passes.

## 2. Probe PersonalS3, before merging the release PR

Nothing else shows that a release breaks the consumer. Pack under a prerelease version nothing has
used, because restore never replaces a cached version (PersonalS3 #98):

1. Pick `<n>` so that `ls ~/.nuget/packages/integrateds3.core/` has no `<version>-probe.<n>`.
2. In the release worktree, with `git status --porcelain` printing nothing:
   `dotnet pack src/IntegratedS3/IntegratedS3.slnx -c Release -o <scratch>/probe --version-suffix probe.<n>`.
   Record `git rev-parse HEAD^{tree}`: the probed tree, which steps 3.1, 4.1 and 4.2 compare with.
3. In a PersonalS3 worktree off a fresh `origin/master`, set both pins in
   `Directory.Packages.props` to `<version>-probe.<n>`. Then build, run the full suite, run the
   warning ratchet on that build's log, and run the AOT binary. nuget.org goes first among the
   sources: with the local folder first, the CLI mangles the URL into a local path and restore
   fails with NU1301.
   - `dotnet build PersonalS3.sln -c Release --no-incremental --source https://api.nuget.org/v3/index.json --source <scratch>/probe > <scratch>/probe-build.log 2>&1`
   - `dotnet test PersonalS3.sln -c Release --no-build`. A red `ObjectDisposedException` on
     `SQLitePCL.sqlite3` or a `GlobalRateLimitGateTests` timing failure is PersonalS3's known
     flake (its `CLAUDE.md`, phantom result 5): rerun once with
     `-- xUnit.ParallelizeTestCollections=false`.
   - `py -3 scripts/check_warnings.py <scratch>/probe-build.log`.
   - The AOT recipe in PersonalS3's `knowledge/aot-only-failures.md`, with the same two
     `--source` options on its `dotnet publish` and that command's output kept in
     `<scratch>/probe-publish.log`. `grep -c "Generating native code" <scratch>/probe-publish.log`
     prints 1; without it ILC did not run (`CLAUDE.md`, phantom result 4). Then
     `grep -E 'IL2104|IL3053' <scratch>/probe-publish.log` must name no `IntegratedS3` assembly:
     trim and AOT warnings from code inside the packages show only there (#140's class). It sees
     only package code the host reaches that is not itself marked `RequiresUnreferencedCode` or
     `RequiresDynamicCode`: a warning inside such a member folds into the IL2026/IL3050 at
     PersonalS3's call site, which its baseline already holds. PersonalS3's host reaches most
     package code through two such members, `AddIntegratedS3` and `MapIntegratedS3Endpoints`, and
     none of the 11 IL warnings #264 counts in the shipped packages shows in the probe's log. Until
     PersonalS3 #105 is fixed, the recipe's last two checks print 1 and 0 on the probe, as they do
     on `master`.

   A break in what PersonalS3 implements or calls fails here; PersonalS3 restores only
   Abstractions, AspNetCore, Core and Protocol, so a pass never downgrades step 0's call. For a
   major, the build fails on the members PersonalS3 has not implemented yet, and then the suite,
   the ratchet and the AOT binary have nothing to run. So for a major, or when the build fails on a
   planned minor (then it is a major: redo steps 1.2-1.4), implement the members and migrations in
   the probe worktree as step 5.4 will, keep that diff for step 5, and run the four checks on it.
   A failure outside what the `CHANGELOG.md` section lists stops the release. A `new warning`
   stops the release until a PR to `main` removes it (close the release PR first, so that the
   freeze ends with it, then start again at step 0), or the `CHANGELOG.md` section tells consumers
   what to change. A baseline line `no longer produced` is not a break: step 5 deletes it.
4. Throw the probe away: `git checkout -- Directory.Packages.props` in that worktree (keep a
   major's diff from step 2.3 elsewhere first), and
   `rm -rf ~/.nuget/packages/integrateds3.*/<version>-probe.<n>`.
5. Squash-merge the release PR, and only if it holds `main`'s head:
   `git fetch origin && git merge-base --is-ancestor origin/main HEAD` in the release worktree.
   Otherwise merge `origin/main` into it, move the merged `Unreleased` lines into the `<version>`
   section (a conflict at the `<version>` heading resolves the same way), push, and repeat steps
   0.1-0.3 and 2; the merge gets CI green on the new head sha and a verification pass over it
   (`CLAUDE.md`, Git & PRs). If step 2.3 changed the release PR after its passes, repeat steps
   2.1-2.4 with a new `<n>` on the new head, and the change gets CI green on that head sha and a
   verification pass over it.

## 3. Dry run

1. `gh workflow run nuget-publish.yml -R SymoHTL/Integrated-S3 --ref main -f push-to-nuget=false -f dry-run=true`.
   Its `validate` job runs the full solution tests and the AOT script. Take the run's id from the
   run URL that `gh workflow run` prints; if it prints none, from
   `gh run list -R SymoHTL/Integrated-S3 --workflow nuget-publish.yml --json databaseId,headSha,createdAt`,
   the entry created after your dispatch (wait until one exists; every run has the same title).
   The run's `headSha` (`gh run view <run-id> -R SymoHTL/Integrated-S3 --json headSha -q .headSha`)
   must be the release PR's merge commit
   (`gh pr view <release PR> -R SymoHTL/Integrated-S3 --json mergeCommit -q .mergeCommit.oid`), and
   after `git fetch origin`, `git rev-parse <headSha>^{tree}` must be the probed tree from step 2.2:
   a PR merged between step 2.5's check and the squash merge sits below the merge commit and
   changes its tree. If `main` has moved past the merge commit, or the tree differs, stop: the new
   commits missed step 0 and the probe. Start again at step 0 on `main`'s head.
2. `gh run download <run-id> -R SymoHTL/Integrated-S3 -n nuget-packages -D <scratch dir>` holds one
   `*.<version>.nupkg` per row of `LayeringConventionTests`. Any other version means the bump is not
   on `main`.
3. nuget.org does not list `<version>` for any id yet: a duplicate is skipped and the run still
   goes green. `curl -s https://api.nuget.org/v3-flatcontainer/integrateds3.<lowercase id suffix>/index.json`,
   for example `integrateds3.core`.

## 4. Publish

1. `main` has not moved since the dry run: the sha `git ls-remote origin refs/heads/main` prints
   is still the release PR's merge commit, which step 3.1 checked the dry run and the probed tree
   against. Otherwise stop, as in step 3.1.
2. `gh workflow run nuget-publish.yml -R SymoHTL/Integrated-S3 --ref main -f push-to-nuget=true -f dry-run=false`.
   A dispatch on `main` cannot pin a sha, so check the new run's `headSha` and its tree the same
   way and cancel it during `validate` if either differs. Take the new run's id as in step 3.1:
   from the run URL `gh workflow run` prints, or the entry created after this dispatch, never the
   newest entry before it appears. It pushes with `--skip-duplicate`, tags `v<version>` and
   creates the GitHub Release. A green run does not prove a release: three green runs on
   2026-04-07 shipped nothing.
3. If it fails after pushing some packages, rerun it on the same sha with
   `gh run rerun <run-id> -R SymoHTL/Integrated-S3 --failed`, never with a new dispatch.
4. nuget.org lists `<version>` for every id (the URL in step 3.3; indexing can take minutes).
5. `git fetch origin --tags && git rev-parse "v<version>^{commit}"` is the release PR's merge
   commit (step 3.1), and `gh release view v<version> -R SymoHTL/Integrated-S3` has a `.nupkg` and
   a `.snupkg` per id.
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
   #82), starting from the diff step 2.3 kept. A new column is a numbered schema migration, with a
   test seen red without it.
5. Build and test with the commands in PersonalS3's `CLAUDE.md`, including its warning ratchet.
   A package bump reaches the Native AOT binary. Push the branch, then dispatch `heavy`
   (`gh workflow run ci.yml -R SymoHTL/PersonalS3 --ref <branch> -f run-heavy=true`), which builds
   the image without starting it. Then run the AOT binary as PersonalS3's
   `knowledge/aot-only-failures.md` shows. With Discord disabled that run covers startup, the
   migrations and the access-key store, but maps no S3 route: a path the release changed needs a
   run with Discord enabled, against a throwaway guild or the Discord API stub (HAZARD, PersonalS3
   #96).
6. `CHANGELOG.md` `Unreleased`: the behaviour inherited from the release, linking the GitHub
   Release from step 4.6.
7. PR, then CI green on its head sha and its verification passes, as for any PR. CI restores from
   nuget.org, which a local build may not have done.

## Done when

- nuget.org lists `<version>` for every id, and the tag and the Release point at the release PR's
  merge commit.
- PersonalS3's pin bump is merged with CI green. Deploying is a separate step, and production has
  run ahead of `master` before (PersonalS3 `knowledge/production-runs-ahead-of-master.md`).
