---
name: audit-to-issues
description: How to turn a whole-repo audit into well-scoped GitHub issues without false positives or re-filed fixes - scoped finders in waves of about 4, one skeptic per finding that proves it with a test failing on the current sha, dedup against open and closed issues, idempotent filing, and security findings to a private advisory.
metadata:
  type: reference
---

**Recipe**, as used for #147–#167 (July 2026) and #261–#278 (September 2026):

1. **Scoped finders.** Run one agent per subsystem and lens. The lenses are correctness, security,
   concurrency, performance, resource leaks, testing and CI. This repo takes about 12 finders.
2. **Adversarial verification.** Give each finding to one skeptic that reads the current code.
   It rejects false positives and anything already fixed. It proves a surviving finding with a
   test that fails on the current sha, in a scratch copy of the repo, and where cheap it tries the
   fix in a second copy. In September this turned "read from code" leads into reproduced defects.
   Three of them turned out worse than first reported (#272, #274, #275).
3. **Dedup.**
   - Cluster by primary file, and merge findings with the same root cause.
   - Check open and closed issues, bodies included:
     `gh api --paginate "repos/<owner>/<repo>/issues?state=all&per_page=100"`.
   - A finding that extends an existing issue becomes a comment on it; the EntityTooSmall gap went
     onto #261 that way.
4. **File.** Use `gh issue create --body-file` through a loop that:
   - appends `key url` to a log and skips keys already in it;
   - sleeps 2.5 s between issues;
   - on "was submitted too quickly" (a secondary rate limit), backs off 45 s and retries.

   Each body says:
   - the problem;
   - where it was seen red;
   - the smallest fix;
   - the test that gates it.
5. **Security findings do not go to the public tracker.** `SECURITY.md` asks for private
   vulnerability reporting, so file a draft advisory instead:
   `gh api -X POST repos/<owner>/<repo>/security-advisories --input <json>`, with `summary`,
   `description`, `severity`, `cwe_ids` and `vulnerabilities`. Keep the details out of public issues,
   PRs, `CLAUDE.md` and this store until the fix ships.

**What went wrong before:**

- **Too many agents at once.** About 18 finders launched together tripped the server's rate
  limits. Run waves of about 4.
- **One synthesis agent for everything.** An agent writing about 50 issue bodies stalled
  mid-stream. Give each synthesis agent at most about 8 issues, and have it also return the raw
  verified findings as a fallback.
- **The low-value tail.** About a third of a full sweep's findings were low severity. Agree a
  severity floor, or confirm the scope, before filing dozens.

**When not to fan out:** for "the top few bugs", or for one file or subsystem, a single agent or a
3–5 agent workflow is faster and cheaper.

**Why:** an issue filed from an unverified lead costs a maintainer the verification anyway, and a
re-filed fixed bug costs trust in the tracker.

**How to apply:** follow the steps in order. A finding without a failing test stays a lead, and a
lead is not filed.
