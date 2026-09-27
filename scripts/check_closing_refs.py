#!/usr/bin/env python3
"""Closing-reference check: the CI gate on a pull request's title and body.

GitHub closes an issue when a pull request into the default branch merges whose description
names it after a closing keyword, and again when a commit message that does lands there; the
squash message is the PR body (`CLAUDE.md`, Git & PRs). It matches the keyword and the
reference, not the sentence: "Fixed: #314 proposes the gate" closed #314 (#282).

Fails (exit 1) on every closing reference in the title or body except one that starts `Closes `,
spelled exactly so, with a capital C and no colon (`Closes #<n>`): that one spelling says which
issue the PR closes. A closing reference is close, closes, closed, fix, fixes, fixed,
resolve, resolves or resolved, in any case, then an optional colon, then the issue as `#<n>`,
`<owner>/<repo>#<n>` or an issue URL; `*` and `_` around them count as formatting. Each failure
names its line.

Reads the title from PR_TITLE and the body from PR_BODY (the pull_request event's fields), or
the files named as arguments instead.

Usage: python3 scripts/check_closing_refs.py [--self-test] [file ...]   (py -3 on Windows)
CI checks that the self-test prints "self-test OK", because a broken exit code would pass it.
"""
import contextlib
import io
import os
import re
import sys

REFERENCE = re.compile(
    r"(?<![\w])(?P<keyword>close[sd]?|fix(?:e[sd])?|resolve[sd]?)[*_]*(?P<colon>:?)[*_]*[ \t]+[*_]*"
    r"(?P<issue>(?:[\w.-]+/[\w.-]+)?#\d+|https?://github\.com/[\w.-]+/[\w.-]+/issues/\d+)",
    re.IGNORECASE)


def offenders(text):
    """The closing references in text that are not `Closes #<n>`, as (line number, match)."""
    found = []
    for number, line in enumerate(text.splitlines(), 1):
        for match in REFERENCE.finditer(line):
            if match.group("keyword") != "Closes" or match.group("colon"):
                found.append((number, match.group(0)))
    return found


def check(sources):
    failed = False
    for name, text in sources:
        for number, reference in offenders(text):
            print(f"{name} line {number}: '{reference}' closes that issue on merge; write "
                  "'Closes #<n>' for an issue this PR closes, and reword anything else")
            failed = True
    return 1 if failed else 0


def self_test():
    cases = [
        ("Fixed: #314 proposes the gate", 1),
        ("Fixed: #99's item now", 1),
        ("closes SymoHTL/PersonalS3#12", 1),
        ("Resolves #7 and fixes #8", 2),
        ("the fix #39012 lands", 1),
        ("Fix #5.", 1),
        ("Closed #21, close #22", 2),
        ("resolved: #23", 1),
        ("Closes: #265", 1),
        ("CLOSES #265", 1),
        ("**Fixed:** #12", 1),
        ("**Fixed**: #12", 1),
        ("Fixes https://github.com/SymoHTL/Integrated-S3/issues/12", 1),
        ("Closes #265", 0),
        ("- Closes #2, the second item", 0),
        ("Closes SymoHTL/PersonalS3#12", 0),
        ("fixed in 7cb26ee, see #313", 0),
        ("prefix #288 Refs #288", 0),
        ("unfixed #3", 0),
        ("Fixes the race in #311's second case", 0),
    ]
    failures = 0
    for text, want in cases:
        got = len(offenders(text))
        if got != want:
            print(f"FAIL: {text!r} gave {got} offenders, want {want}")
            failures += 1
    for title, body, want in [("Fix #12 crash", "Closes #13", 1), ("ci: a gate", "Closes #13\n\nRefs #288", 0),
                              ("ci: a gate", "Closes #13\n\nfixed: #14", 1)]:
        os.environ["PR_TITLE"], os.environ["PR_BODY"] = title, body
        with contextlib.redirect_stdout(io.StringIO()):
            got = main(["check_closing_refs.py"])
        if got != want:
            print(f"FAIL: title {title!r} and body {body!r} gave exit {got}, want {want}")
            failures += 1
    if failures:
        return 1
    print("self-test OK")
    return 0


def main(argv):
    if argv[1:] == ["--self-test"]:
        return self_test()
    if len(argv) > 1:
        sources = [(path, open(path, encoding="utf-8").read()) for path in argv[1:]]
    else:
        sources = [("title", os.environ.get("PR_TITLE", "")), ("body", os.environ.get("PR_BODY", ""))]
    return check(sources)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
