---
name: gh-cli
description: 'Use when working with GitHub via gh CLI: auth checks, repo setup, branch and PR workflows, issue triage, release/tag tasks, and CI/check diagnostics. Trigger for gh, github cli, pull request, issue, branch, release, tag, checks, and actions workflows.'
argument-hint: 'What should be done with gh (issue, PR, release, checks, repo)?'
user-invocable: true
---

# GitHub CLI Workflow (`gh`)

A reusable, safety-first workflow for completing common GitHub tasks with the `gh` CLI.

## When to Use

Use this skill when the request involves one or more of these:
- Create/update issues, labels, milestones, or project triage
- Create/review/update pull requests and request reviewers
- Inspect CI checks, workflow runs, logs, or failing jobs
- Manage tags, releases, and release notes
- Clone/fork/repo metadata tasks from terminal
- Quick GitHub automation without writing custom scripts

## Inputs

Capture these first (ask only for what is missing):
- Owner/org and repository name
- Target branch (base/head) if a PR is involved
- Issue/PR number if modifying existing objects
- Desired outcome (create/update/close/merge/release/etc.)
- Constraints (draft PR, squash merge, labels, reviewer team, release type)

## Procedure

### 1) Preflight and context
1. Confirm the current working repository and branch.
2. Check `gh` availability and authentication status.
3. If repo context is ambiguous, resolve explicitly with `gh repo view` and/or full `owner/repo` notation.

Completion check:
- `gh` is authenticated and the target repository is unambiguous.

### 2) Select workflow lane
Choose the lane based on user intent:
- **Issue lane**: create, comment, label, assign, close/reopen issues
- **PR lane**: create/update PR, view diff/status, review, merge
- **Checks lane**: inspect checks/workflows, identify failing jobs/log links
- **Release lane**: tags, changelog notes, draft/publish releases
- **Repo lane**: metadata, forks, clone defaults, branch protections (informational)

Completion check:
- Exactly one primary lane selected (secondary lane allowed if needed).

### 3) Execute lane-specific actions

#### Issue lane
1. List/find relevant issues with filters (state, labels, assignee, author).
2. Create or update issue fields (title/body/labels/assignees/milestone).
3. Add comments with concise context and next actions.
4. If closing, include reason and resolution evidence.

Quality criteria:
- Issue text is actionable, scoped, and includes acceptance criteria when relevant.
- Labels and assignees reflect team conventions.

#### PR lane
1. Validate branch state and commit range.
2. Create or update PR with clear title/body and linked issues.
3. Verify checks/review status before merge operations.
4. Merge using the requested strategy (merge/squash/rebase) and confirm result.

Quality criteria:
- PR body explains change, risk, test evidence, and linked issue(s).
- Merge strategy matches repository policy.

#### Checks lane
1. Inspect latest workflow runs or PR checks.
2. Identify failing job(s), step name(s), and first actionable error.
3. Report concise remediation path and re-run guidance.

Quality criteria:
- Report includes exact failing workflow/job and actionable root-cause hint.

#### Release lane
1. Confirm tag/version intent and branch/source commit.
2. Create draft notes or release notes from merged PRs/issues.
3. Create draft release first unless user explicitly requests immediate publish.
4. Publish and verify release artifacts/notes.

Quality criteria:
- Version/tag follows project scheme.
- Notes summarize user-facing changes and breaking changes.

#### Repo lane
1. Validate repository identity and permissions.
2. Apply requested metadata/config updates or provide next actionable steps.
3. Confirm visibility/ownership implications for destructive or org-level actions.

Quality criteria:
- No destructive action without explicit user confirmation.

### 4) Safety and decision points

Use these branches while executing:
- **Auth failure** → stop and resolve login/session before continuing.
- **Permission denied** → report required role/scope; avoid repeated failing writes.
- **Ambiguous repo/branch** → ask one precise clarification.
- **Protected branch conflict** → pivot to PR-based path.
- **Potentially destructive operation** (delete/force/close-many) → require explicit confirmation.

### 5) Final verification
Before declaring done, verify:
- Target object exists and reflects requested state (issue/PR/release/check outcome).
- Links/IDs are captured (issue number, PR URL, release tag, run URL).
- Any follow-up actions are listed (reviewers, reruns, backports, docs updates).

## Output format

Return a compact summary with:
- What was changed
- Evidence/links/IDs
- Current status (done/blocked/waiting)
- Immediate next step (if any)

## Example prompts

- `/gh-cli Create a draft PR from my branch to main and request @team/backend.`
- `/gh-cli Find open bugs labeled regression in this repo and summarize top 5 by recency.`
- `/gh-cli Check why the latest CI run failed and point me to the first actionable error.`
- `/gh-cli Draft a v1.8.0 release with notes from merged PRs since v1.7.0.`
