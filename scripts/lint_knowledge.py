#!/usr/bin/env python3
"""Knowledge lint: the CI gate for the in-repo knowledge store (INDEX.md + knowledge/).

Fails (exit 1) on:
  * an INDEX.md link into knowledge/ whose target file does not exist
  * a file under knowledge/ that no INDEX.md link points to, or that leads more than one INDEX.md
    list item (-, *, + or 1.), as a merge that kept both sides does. A sub-item that starts with
    a link counts as a line for that entry, so a see-also goes in the hook.
  * a knowledge/ path named in CLAUDE.md, INDEX.md or an entry that does not exist, and a missing
    CLAUDE.md, whose pointers would otherwise go unchecked. A path inside a URL or after another
    directory (team-knowledge/, ../other-repo/knowledge/) is another store's and is skipped: name
    another repo's entry that way, and write an example as knowledge/<slug>.md.
  * a file under knowledge/ that is not a .md entry directly in it: the store is flat
  * an entry without frontmatter (a --- block at the top with non-empty name: and description:,
    and a metadata: type: of user, feedback, project or reference), or whose name: is not its file
    name, so a [[slug]] link and the file it means cannot drift apart
  * a credential-shaped string in INDEX.md or any file under knowledge/ (secret VALUES are banned;
    variable names and flags are fine, and so are AWS's documented example keys). The error names
    the line and prints nothing of the value, because CI logs are public.
  * a merge-conflict marker in INDEX.md, CLAUDE.md or any entry: a half-resolved INDEX.md merge
    is otherwise a well-formed index with two extra lines

Text inside an HTML comment in INDEX.md is skipped, except by the credential and conflict checks.
Warns without failing on a [[wikilink]] that resolves to no entry: it marks an entry worth
writing, or one renamed without updating the links to it.

Usage: python3 scripts/lint_knowledge.py [--self-test]   (py -3 on Windows)
CI checks that the self-test prints "self-test OK", because a broken exit code would pass it.
"""
import collections
import os
import re
import shutil
import subprocess
import sys
import tempfile

SECRET_PATTERNS = [
    r"gh[pousr]_[A-Za-z0-9]{30,}",                                 # GitHub tokens
    r"github_pat_[A-Za-z0-9_]{30,}",
    r"sk-ant-[A-Za-z0-9_\-]{20,}",                                 # Anthropic API keys
    r"AKIA[A-Z0-9]{16}",                                           # AWS access key ids
    r"BEGIN [A-Z ]*PRIVATE KEY",
    r"(?i)bearer [A-Za-z0-9_\-\.]{25,}",
    r"[MNO][A-Za-z0-9_\-]{23,27}\.[A-Za-z0-9_\-]{6}\.[A-Za-z0-9_\-]{27,}",  # Discord bot tokens
    r"discord(?:app)?\.com/api/(?:v\d+/)?webhooks/\d+/[A-Za-z0-9_\-]{30,}",  # Discord webhook URLs
    # A value assigned to a secret-named key: S3 secret keys, env files, KeyBase64 settings.
    r"(?i)(?:secret|password|token|keybase64)[\w-]*[\"']?[ \t]*[:=][ \t]*[\"']?(?P<value>[A-Za-z0-9/+=_\-\.]{16,})",
]
ALLOWED_SECRETS = {"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}  # AWS docs examples
CONFLICT_MARKER = re.compile(r"^(<<<<<<< |>>>>>>> )", re.M)
# Link targets into knowledge/: inline [text](./knowledge/x.md#anchor "title") and reference [r]: knowledge/x.md
LINK_TARGET = re.compile(
    r"\]\([ \t]*<?(?:\./)?(knowledge/[^)\s>#]+)[^)]*\)|^[ \t]*\[[^\]]+\]:[ \t]*<?(?:\./)?(knowledge/[^\s>#]+)", re.M)
# The link that leads an INDEX.md list item: the entry the item is for.
LEADING_LINK = re.compile(
    r"^[ \t]*(?:[-*+]|\d+[.)])[ \t]+(?:\*\*|__)?\[[^\]]*\]\([ \t]*<?(?:\./)?(knowledge/[^)\s>#]+)", re.M)
# A path into this repo's knowledge/ named anywhere in prose, such as `knowledge/x.md` in CLAUDE.md.
# Not one inside a longer path or URL, which points into another repo, and not knowledge/x.md.bak.
ENTRY_MENTION = re.compile(r"(?<![\w/.-])(?:\./)?(knowledge/[\w./-]+?\.md)(?![\w-]|\.\w)", re.I)
FRONTMATTER_TYPE = re.compile(r"^metadata:[ \t]*\n(?:[ \t]+\S.*\n)*?[ \t]+type:[ \t]*(user|feedback|project|reference)[ \t]*$", re.M)


def read(path):
    with open(path, encoding="utf-8", errors="replace") as f:  # text mode reads CRLF as \n
        return f.read()


def lint(root):
    """(errors, warnings) for the store under root."""
    errors, warnings = [], []
    index = read(os.path.join(root, "INDEX.md"))
    index_text = re.sub(r"<!--.*?-->", "", index, flags=re.S)
    linked = {a or b for a, b in LINK_TARGET.findall(index_text)}
    files = set()
    for dirpath, _, filenames in os.walk(os.path.join(root, "knowledge")):
        files.update(os.path.relpath(os.path.join(dirpath, f), root).replace(os.sep, "/") for f in filenames)

    for t in sorted(linked - files):
        errors.append(f"INDEX.md links missing file: {t}")
    for f in sorted(files - linked):
        errors.append(f"{f} has no INDEX.md line")
    for t, count in sorted(collections.Counter(LEADING_LINK.findall(index_text)).items()):
        if count > 1:
            errors.append(f"INDEX.md has {count} lines for {t}")

    bodies = {"INDEX.md": index}
    names = set()
    for f in sorted(files):
        body = bodies[f] = read(os.path.join(root, f))
        stem = f[len("knowledge/"):]
        if "/" in stem or not stem.endswith(".md"):
            errors.append(f"{f}: not a .md entry directly in knowledge/")
            continue
        m = re.match(r"---\n(.*?\n)---\n", body, re.S)
        front = m.group(1) if m else ""
        name = re.search(r"^name:[ \t]*(\S+)", front, re.M)
        if not name or not re.search(r"^description:[ \t]*\S", front, re.M) or not FRONTMATTER_TYPE.search(front):
            errors.append(f"{f}: missing frontmatter (name:, description:, metadata type:)")
        elif name.group(1) != stem[:-3]:
            errors.append(f"{f}: name: {name.group(1)} is not the file name")
        else:
            names.add(name.group(1))

    mentions = {**bodies, "INDEX.md": index_text}
    if os.path.exists(os.path.join(root, "CLAUDE.md")):
        mentions["CLAUDE.md"] = read(os.path.join(root, "CLAUDE.md"))
        if CONFLICT_MARKER.search(mentions["CLAUDE.md"]):
            errors.append("CLAUDE.md: merge-conflict marker")
    else:
        errors.append("CLAUDE.md is missing, so the entry names it cites go unchecked")
    for where, body in mentions.items():
        reported = linked if where == "INDEX.md" else set()  # a dead INDEX.md link is reported above
        for t in sorted(set(ENTRY_MENTION.findall(body)) - files - reported):
            errors.append(f"{where} names missing file {t}")

    for where, body in bodies.items():
        if CONFLICT_MARKER.search(body):
            errors.append(f"{where}: merge-conflict marker")
        for pat in SECRET_PATTERNS:
            for hit in re.finditer(pat, body):
                if (hit.groupdict().get("value") or hit.group(0)) not in ALLOWED_SECRETS:
                    line = body.count("\n", 0, hit.start()) + 1
                    errors.append(f"{where}:{line}: credential-shaped string")
        for link in re.findall(r"\[\[([^\]]+)\]\]", body):
            if link not in names:
                warnings.append(f"{where}: [[{link}]] resolves to no entry (worth writing?)")
    return errors, warnings


def run(root):
    """Lints the store under root, prints the result and returns the process exit code."""
    errors, warnings = lint(root)
    for w in warnings:
        print(f"WARN  {w}")
    for e in errors:
        print(f"ERROR {e}")
    print(f"{len(errors)} error(s), {len(warnings)} warning(s)")
    return 1 if errors else 0


def self_test():
    """Each check fires on a store built to break it, and a clean store passes. No assert, so
    python -O cannot skip it. Returns the process exit code."""
    entry = "---\nname: {0}\ndescription: d\nmetadata:\n  type: project\n---\n\nBody [[{1}]].\n"
    index = "- [a](knowledge/a.md) - hook\n"
    token = "Ab_-" * 17
    alnum = "aB3dE5fG7hJ9kL1mN3pQ5rS7tU9vW1xY3zA5bC7dE9"
    # Fake credentials shaped like real ones, each matching one pattern only, and together every
    # alternative of every pattern and each pattern's minimum length: dropping or narrowing one fails.
    secrets = [f"gh{c}_" + alnum[:36] for c in "pousr"] + [
        "github_pat_11" + alnum[:20] + "_" + alnum[:30],
        "sk-ant-api03-" + alnum[:30] + "_x-" + alnum[:8],
        "AKIA2E0A8F3B244C9986",
        "-----BEGIN OPENSSH PRIVATE KEY-----",
        "-----BEGIN PRIVATE KEY-----",
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
        "BEARER " + alnum[:25],
        "MTIzNDU2Nzg5MDEyMzQ1Njc4OQ.Gx1-ab." + alnum[:38],
        "NzQ4MjU5OTk5OTk5OTk5OTk5OQ.Zz9_ef." + alnum[:38],
        "OTk5OTk5OTk5OTk5OTk5OTk5OQ.Yx_2cd." + alnum[:38],
        "https://discordapp.com/api/webhooks/123456789012345678/" + token,
        "https://discord.com/api/v10/webhooks/123456789012345678/" + token,
        "INTEGRATEDS3_S3COMPAT_SECRET_KEY=" + alnum[:40],
        '"KeyBase64": "q83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq80="',
        "password: hunter2hunter2hu",
        "api-token = " + alnum[:20],
        "secret-access-key: " + alnum[:20],
    ]
    cases = [  # (expected error substring, or None for a clean store; files to write over the clean store, None deletes)
        (None, []),
        (None, [("INDEX.md", "- [a](./knowledge/a.md) - hook\n")]),
        (None, [("INDEX.md", '- [a](knowledge/a.md "title") - hook\n')]),
        (None, [("INDEX.md", "- [a](knowledge/a.md#why) - hook\n")]),
        (None, [("INDEX.md", "- [a][r] - hook\n\n[r]: knowledge/a.md\n")]),
        (None, [("INDEX.md", "<!-- x -->\n" + index + "<!-- y -->\n")]),
        (None, [("INDEX.md", index + "- [b](knowledge/b.md) - hook, see [a](knowledge/a.md)\n"),
                ("knowledge/b.md", entry.format("b", "a"))]),
        (None, [("CLAUDE.md", "Rule; `knowledge/a.md` and ./knowledge/a.md#why.\n")]),
        (None, [("CLAUDE.md", "Elsewhere: https://github.com/o/r/blob/main/knowledge/zz.md, `team-knowledge/zz.md`,"
                              " ../other-repo/knowledge/zz.md, `knowledge/<slug>.md`.\n")]),
        (None, [("knowledge/a.md", entry.format("a", "a") + "AKIAIOSFODNN7EXAMPLE\n")]),
        (None, [("knowledge/a.md", entry.format("a", "a") + "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n")]),
        (None, [("knowledge/a.md", entry.format("a", "a").replace("  type: project\n", "  local_reason: x\n  type: user\n"))]),
        (None, [("knowledge/a.md", entry.format("a", "a").replace("\n", "\r\n"))]),
        (None, [("INDEX.md", index + "<!-- - [old](knowledge/old.md) - deleted, see `knowledge/old.md` -->\n")]),
        (None, [("CLAUDE.md", "A backup, knowledge/zz.md.bak, is not an entry name.\n")]),
        ("INDEX.md links missing file", [("INDEX.md", index + "- [b](knowledge/b.md)\n")]),
        ("INDEX.md links missing file", [("INDEX.md", index + "- [b](./knowledge/b.md)\n")]),
        ("INDEX.md links missing file", [("INDEX.md", index + '- [b](knowledge/b.md "t")\n')]),
        ("INDEX.md links missing file", [("INDEX.md", index + "\n[r]: knowledge/b.md\n")]),
        ("has no INDEX.md line", [("knowledge/c.md", entry.format("c", "a"))]),
        ("has no INDEX.md line", [("INDEX.md", "<!--\n- [a](knowledge/a.md) - hook\n-->\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + index)]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "* [a](./knowledge/a.md#why) - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "1. **[a](knowledge/a.md)** - hook\n")]),
        ("CLAUDE.md names missing file knowledge/b-c.md", [("CLAUDE.md", "Rule; `knowledge/b-c.md`.\n")]),
        ("CLAUDE.md names missing file knowledge/sigv4-b2.md", [("CLAUDE.md", "Rule; `knowledge/sigv4-b2.md`.\n")]),
        ("CLAUDE.md names missing file knowledge/b.md", [("CLAUDE.md", "Rule; see ./knowledge/b.md.\n")]),
        ("CLAUDE.md names missing file knowledge/sub/b.md", [("CLAUDE.md", "Rule; knowledge/sub/b.md.\n")]),
        ("CLAUDE.md names missing file knowledge/b.MD", [("CLAUDE.md", "Rule; knowledge/b.MD.\n")]),
        ("CLAUDE.md is missing", [("CLAUDE.md", None)]),
        ("INDEX.md names missing file knowledge/b.md", [("INDEX.md", index + "Also see `knowledge/b.md`.\n")]),
        ("knowledge/a.md names missing file knowledge/b.md", [("knowledge/a.md", entry.format("a", "a") + "See knowledge/b.md.\n")]),
        ("not a .md entry", [("INDEX.md", index + "- [x](knowledge/sub/x.md)\n"), ("knowledge/sub/x.md", entry.format("x", "a"))]),
        ("not a .md entry", [("INDEX.md", index + "- [x](knowledge/x.MD)\n"), ("knowledge/x.MD", entry.format("x", "a"))]),
        ("missing frontmatter", [("knowledge/a.md", "no frontmatter\n")]),
        ("missing frontmatter", [("knowledge/a.md", "Intro.\n" + entry.format("a", "a"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("description: d\n", ""))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("description: d\n", "description:\n"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("  type: project\n", ""))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("  type: project\n", "  type:\n"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("  type: project\n", "  type: lesson\n"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("name: a\n", "name:\n"))]),
        ("is not the file name", [("knowledge/a.md", entry.format("b", "a"))]),
        ("merge-conflict marker", [("INDEX.md", index + "<<<<<<< HEAD\n")]),
        ("merge-conflict marker", [("knowledge/a.md", entry.format("a", "a") + ">>>>>>> branch\n")]),
        ("CLAUDE.md: merge-conflict marker", [("CLAUDE.md", "Rules.\n<<<<<<< HEAD\n")]),
        ("INDEX.md:2: credential-shaped string", [("INDEX.md", index + secrets[0] + "\n")]),
        ("knowledge/sub/y.txt:1: credential-shaped string", [("knowledge/sub/y.txt", secrets[0] + "\n")]),
    ] + [("knowledge/a.md:9: credential-shaped string", [("knowledge/a.md", entry.format("a", "a") + s + "\n")])
         for s in secrets]

    failures = []
    for p in SECRET_PATTERNS:
        if not any(re.search(p, s) for s in secrets):
            failures.append(f"no secret sample for pattern {p[:20]}...")
    for s in secrets:
        matching = [p for p in SECRET_PATTERNS if re.search(p, s)]
        if len(matching) != 1:
            failures.append(f"secret sample {s[:12]}... matches {len(matching)} patterns, not 1")

    with tempfile.TemporaryDirectory() as root:
        def put(rel, text):
            path = os.path.join(root, rel)
            if text is None:
                os.remove(path)
                return
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(text)

        def reset(a_links):
            shutil.rmtree(os.path.join(root, "knowledge"), ignore_errors=True)
            put("INDEX.md", index)
            put("CLAUDE.md", "Rules.\n")
            put("knowledge/a.md", entry.format("a", a_links))

        for expected, files in cases:
            reset("a")
            for rel, text in files:
                put(rel, text)
            errors, warnings = lint(root)
            if expected is None and (errors or warnings):
                failures.append(f"clean store {files} gave {errors + warnings}")
            elif expected is not None and not any(expected in e for e in errors):
                failures.append(f"{files} gave {errors}, expected '{expected}'")
            elif any(s[4:12] in e for s in secrets for e in errors):
                failures.append(f"{files}: an error prints part of a credential")

        reset("later")
        if lint(root)[1] != ["knowledge/a.md: [[later]] resolves to no entry (worth writing?)"]:
            failures.append(f"dangling wikilink gave {lint(root)[1]}")
        # The script as CI runs it, from scripts/ under the store: exit 0 with only a warning, 1 with an error.
        script = os.path.join(root, "scripts", "lint_knowledge.py")
        os.makedirs(os.path.dirname(script), exist_ok=True)
        shutil.copyfile(os.path.abspath(__file__), script)
        codes = [subprocess.run([sys.executable, script], capture_output=True).returncode]
        put("knowledge/c.md", entry.format("c", "a"))
        codes.append(subprocess.run([sys.executable, script], capture_output=True).returncode)
        if codes != [0, 1]:
            failures.append(f"exit codes {codes} for a store with a warning, then with an error; expected [0, 1]")

    for f in failures:
        print(f"SELF-TEST FAIL {f}")
    print("self-test OK" if not failures else f"self-test FAILED: {len(failures)} case(s)")
    return 1 if failures else 0


def main():
    if "--self-test" in sys.argv:
        return self_test()
    return run(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


if __name__ == "__main__":
    sys.exit(main())
