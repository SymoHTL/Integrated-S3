---
name: audit-to-issues-postmortem
description: What went wrong in this repo's whole-repo audits, behind each step of the audit-to-issues skill - about 18 finders launched at once tripped the server's rate limits, one synthesis agent writing about 50 issue bodies stalled mid-stream, and a third of a full sweep's findings were low severity.
metadata:
  type: reference
---

The procedure is the `audit-to-issues` skill (`.claude/skills/audit-to-issues/SKILL.md`). This
entry keeps the history behind its steps, from the audits behind #147-#167 (July 2026) and
#261-#278 (September 2026).

**What went wrong before:**

- **Too many agents at once.** About 18 finders launched together tripped the server's rate
  limits. Waves of about 4 did not.
- **One synthesis agent for everything.** An agent writing about 50 issue bodies stalled
  mid-stream. Agents with at most about 8 issues each finished, and the raw verified findings
  they also returned were the fallback.
- **The low-value tail.** About a third of a full sweep's findings were low severity. A severity
  floor, or a confirmed scope, keeps them from being filed by the dozen.

**Why:** an issue filed from an unverified lead costs a maintainer the verification anyway, and a
re-filed fixed bug costs trust in the tracker.

**How to apply:** follow the skill; read this entry when a step of it looks skippable.
