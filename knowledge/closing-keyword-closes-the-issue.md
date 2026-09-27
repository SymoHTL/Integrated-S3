---
name: closing-keyword-closes-the-issue
description: A closing keyword before an issue number anywhere in a PR's title or body closes that issue when the PR merges, whatever the sentence means - "Fixed: #314 proposes the gate" in #282's body closed #314, and PersonalS3 #103's "Fixed: #99's item" closed PersonalS3 #99; both stayed closed for about ten hours. Write `Closes #<n>` for an issue the PR closes, and no other closing keyword before an issue number.
metadata:
  type: reference
---

**What happened.** #282's `## Verification` section answered a finding with "Fixed: #314 proposes
the gate". Its squash merge (07dd232) closed #314 at 21:55Z on 2026-09-26, and #314 stayed
closed until 07:33Z the next morning, after the first verification pass of PersonalS3 #104
reported it. An hour before #282 merged, PersonalS3 #103's "Fixed: #99's item" had closed
PersonalS3 #99 the same way. Both are HAZARD tickets, so for those hours the rules that cite them
pointed at closed issues.

**Why:** GitHub matches the keyword and the reference, not the sentence. Close, closes, closed,
fix, fixes, fixed, resolve, resolves and resolved, in any case and with or without a colon ("The
keywords can be followed by colons or in uppercase", GitHub's page on linking a pull request to an
issue), before `#<n>` or `<owner>/<repo>#<n>`, link the issue from a PR into the default branch,
and a commit message that lands on the default branch closes it too; the squash message is the PR
body here. A section that answers findings with "Fixed: ..." puts the keyword right before
whatever it names next.

**How to apply:** write `Closes #<n>` for an issue the PR closes, and put no other closing
keyword before an issue number: name the issue first ("#314 proposes the gate; fixed") or leave
the keyword out. After an accidental close, reopen the issue with a comment saying why. A stacked
PR is linked only once it targets `main`, so the check runs on every PR, whatever its base.

Gate: `pr-body.yml` `closing-references` (`scripts/check_closing_refs.py`), on the title and the
body of every PR.
