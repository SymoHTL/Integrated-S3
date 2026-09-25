#!/usr/bin/env python3
"""Knowledge lint: the CI gate for the in-repo knowledge store (INDEX.md + knowledge/).

Fails (exit 1) on:
  * fewer than FLOOR entries in knowledge/: every other check passes on an empty or wrong tree
  * an INDEX.md link into knowledge/ whose target file does not exist
  * a file under knowledge/ that leads no INDEX.md list item, or more than one, as a merge that
    kept both sides does. An item starts with -, *, +, 1. or 1), then an inline link to its entry,
    closed on the same line, plain, bold or italic: - [Title](knowledge/x.md). A link later in an
    item, or a reference-style link, does not count as the entry's line. A sub-item that starts
    with a link does, so a see-also goes in the hook. So does an item inside a code block.
  * a knowledge/*.md path named in CLAUDE.md, INDEX.md or an entry that does not exist, and a
    missing CLAUDE.md, whose pointers would otherwise go unchecked. A path inside a URL or after
    another directory (team-knowledge/, ../other-repo/knowledge/) is another store's and is
    skipped: name another repo's entry that way, and write an example as knowledge/<slug>.md.
  * a file under knowledge/ that is not a .md entry directly in it: the store is flat
  * an entry without frontmatter (a --- block at the top with non-empty name: and description:,
    where "", '', ~ and null count as empty, and a metadata: type: of user, feedback, project or
    reference), or whose name: is not its file name, so a [[slug]] link and the file it means
    cannot drift apart
  * a credential-shaped string in INDEX.md or any file under knowledge/ (secret VALUES are banned;
    variable names and flags are fine, and so are AWS's documented example keys). The error names
    the line and prints nothing of the value, because CI logs are public.
  * a merge-conflict marker in INDEX.md, CLAUDE.md or any entry: a half-resolved INDEX.md merge
    is otherwise a well-formed index with two extra lines

Text inside an HTML comment in INDEX.md is skipped, except by the credential, conflict and
wikilink checks.
Warns without failing on a [[wikilink]] that resolves to no entry: it marks an entry worth
writing, or one renamed without updating the links to it.

Usage: python3 scripts/lint_knowledge.py [--self-test]   (py -3 on Windows)
CI checks that the self-test prints "self-test OK", because a broken exit code would pass it.
"""
import collections
import contextlib
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile

FLOOR = 5  # well below each store's entry count, well above zero; raise it as the stores grow
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
# The link that leads an INDEX.md list item: the entry the item is for. Its text may hold one level
# of brackets, and it must close on its line: [a](knowledge/a.md without ")" is plain text.
LEADING_LINK = re.compile(
    r"^[ \t]*(?:[-*+]|\d+[.)])[ \t]+(?:\*{1,3}|_{1,3})?\[(?:[^\[\]\n]|\[[^\[\]\n]*\])*\]"
    r"\([ \t]*<?(?:\./)?(knowledge/[^)\s>#]+)(?:#[^)\s>]*)?>?(?:[ \t]+(?:\"[^\"\n]*\"|'[^'\n]*'))?[ \t]*\)", re.M)
# A path into this repo's knowledge/ named anywhere in prose, such as `knowledge/x.md` in CLAUDE.md.
# Not one inside a longer path or URL, which points into another repo, and not the knowledge/x.md
# in knowledge/x.md.bak; a sentence's closing period or dash still ends the name.
ENTRY_MENTION = re.compile(r"(?<![\w/.-])(?:\./)?(knowledge/[\w./-]+?\.md)(?!\w|\.\w)", re.I)
EMPTY_SCALAR = r"(?!(?:\"\"|''|~|null)[ \t]*$)"  # YAML's empty values, which \S alone accepts
FRONTMATTER_TYPE = re.compile(r"^metadata:[ \t]*\n(?:[ \t]+\S.*\n)*?[ \t]+type:[ \t]*(user|feedback|project|reference)[ \t]*$", re.M)


def read(path):
    with open(path, encoding="utf-8", errors="replace") as f:  # text mode reads CRLF as \n
        return f.read()


def lint(root, floor=FLOOR):
    """(errors, warnings) for the store under root. Prints nothing: run() does."""
    errors, warnings = [], []
    index = read(os.path.join(root, "INDEX.md"))
    index_text = re.sub(r"<!--.*?-->", "", index, flags=re.S)
    linked = {a or b for a, b in LINK_TARGET.findall(index_text)}
    files = set()
    for dirpath, _, filenames in os.walk(os.path.join(root, "knowledge")):
        files.update(os.path.relpath(os.path.join(dirpath, f), root).replace(os.sep, "/") for f in filenames)

    entries = [f for f in files if f.count("/") == 1 and f.endswith(".md")]
    if len(entries) < floor:
        errors.append(f"knowledge/ holds fewer than {floor} entries ({len(entries)}): is this the right tree?")
    lines = collections.Counter(LEADING_LINK.findall(index_text))
    for t in sorted(linked - files):
        errors.append(f"INDEX.md links missing file: {t}")
    for f in sorted(files - set(lines)):
        errors.append(f"{f} has no INDEX.md line")
    for t, count in sorted(lines.items()):
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
        if not name or not re.search(r"^description:[ \t]*" + EMPTY_SCALAR + r"\S", front, re.M) or not FRONTMATTER_TYPE.search(front):
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
    token = "aB3_-" * 14
    alnum = "aB3dE5fG7hJ9kL1mN3pQ5rS7tU9vW1xY3zA5bC7dE9"
    # Fake credentials shaped like real ones, each matching one pattern only, so dropping a pattern
    # fails. They vary prefixes, separators and lengths the way real ones do; a narrowing that no
    # sample exercises still passes. Each is split across literals, so that no secret scanner sees a
    # whole one in this file; AWS's documented example keys, which the lint allows, stay whole.
    secrets = [f"gh{c}_" + alnum[:36] for c in "pousr"] + [
        "github_pat_11" + alnum[:20] + "_" + alnum[:30],
        "sk-ant-" "api03-q7_Lm2Xw9Rt4Yb8Nv1Zp6Dk3Hs5Jf0Gc2Ea7Uo4Ix9",
        "sk-ant-" "admin01-" + alnum[:30],
        "AKIA" "2E0A8F3B244C9986",
        "-----BEGIN OPENSSH " "PRIVATE KEY-----",
        "-----BEGIN " "PRIVATE KEY-----",
        "-----BEGIN RSA " "PRIVATE KEY-----",
        "-----BEGIN EC " "PRIVATE KEY-----",
        "Authorization: Bearer " "eyJhbGciOiJIUzI1NiJ9" ".eyJzdWIiOiIxIn0" ".dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
        "Authorization: Bearer " "kX3-9fQ2_Lm8vRt7YbN4wZ0pD6sH1jGc",
        "BEARER " + alnum[:25],
        "MTIzNDU2Nzg5MDEyMzQ1Njc4" ".Ab-9_Z." "q7_Lm2Xw9-t4Yb8Nv1Zp6Dk3Hs5",
        "NzQ4MjU5OTk5OTk5OTk5OTk5OQ.Zz9_ef." + alnum[:38],
        "OTk5OTk5OTk5OTk5OTk5OTk5OQ.Yx_2cd." + alnum[:38],
        "https://discordapp.com/api/webhooks/123456789012345678/" + token,
        "https://discord.com/api/v10/webhooks/1234567890123456789/" + token,
        "https://discord.com/api/v9/webhooks/123456789012345678/" + token,
        "https://discord.com/api/v8/webhooks/123456789012345678/" + token,
        "INTEGRATEDS3_S3COMPAT_SECRET_KEY=" + alnum[:40],
        '"KeyBase64": ' '"q83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq80="',
        "password: " "hunter2hunter2hu",
        "'SECRET_KEY': '" "django-insecure-q7x9Lm2Xw9Rt4Yb8'",
        "DB_PASSWORD\t\t= " "s3cr3tP4ssw0rd1234",
        "api-token = " "1f0c2d4e-8b7a-4c3d-9e2f-5a6b7c8d9e0f",
        "secret-access-key: " "aB3_dE5.fG7hJ9kL1mN3",
        "aws_secret_access_key = " "Kx9/q2Lm8+RtY7vBn3Wz" "0PdF6sHj1aGc5eUo4iXk",
    ]
    # (expected: None for a clean store, an error substring, or the exact list of errors, which also
    # allows no warning; files to write over the clean store, where None deletes a file)
    cases = [
        (None, []),
        (None, [("INDEX.md", "- [a](./knowledge/a.md) - hook\n")]),
        (None, [("INDEX.md", '- [a](knowledge/a.md "title") - hook\n')]),
        (None, [("INDEX.md", "- [a](knowledge/a.md#why) - hook\n")]),
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
        (None, [("INDEX.md", "- [`a[0]` is null](knowledge/a.md) - hook\n")]),
        (None, [("INDEX.md", "- *[a](knowledge/a.md)* - hook\n")]),
        ("INDEX.md links missing file", [("INDEX.md", index + "- [b](knowledge/b.md)\n")]),
        ("INDEX.md links missing file", [("INDEX.md", index + "- [b](./knowledge/b.md)\n")]),
        ("INDEX.md links missing file", [("INDEX.md", index + '- [b](knowledge/b.md "t")\n')]),
        ("INDEX.md links missing file", [("INDEX.md", index + "\n[r]: knowledge/b.md\n")]),
        ("has no INDEX.md line", [("knowledge/c.md", entry.format("c", "a"))]),
        ("has no INDEX.md line", [("INDEX.md", "<!--\n- [a](knowledge/a.md) - hook\n-->\n")]),
        (["knowledge/a.md has no INDEX.md line"], [("INDEX.md", "- [a](knowledge/a.md - hook\n")]),
        ("knowledge/a.md has no INDEX.md line", [("INDEX.md", "- [a][r] - hook\n\n[r]: knowledge/a.md\n")]),
        ("knowledge/a.md has no INDEX.md line", [("INDEX.md", "- [b](knowledge/b.md) - hook, see [a](knowledge/a.md)\n"),
                                                 ("knowledge/b.md", entry.format("b", "a"))]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + index)]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "* [a](./knowledge/a.md#why) - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "1. **[a](knowledge/a.md)** - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "+ [a](knowledge/a.md) - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "  - [a](knowledge/a.md) - see also\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "1) [a](knowledge/a.md) - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "10. [a](knowledge/a.md) - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "- __[a](knowledge/a.md)__ - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "- [a](<knowledge/a.md>) - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "1.  [a](knowledge/a.md) - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "- *[a](knowledge/a.md)* - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "- ***[a](knowledge/a.md)*** - hook\n")]),
        ("INDEX.md has 2 lines for knowledge/a.md", [("INDEX.md", index + "- [`a[0]`](knowledge/a.md) - hook\n")]),
        ("CLAUDE.md names missing file knowledge/b-c.md", [("CLAUDE.md", "Rule; `knowledge/b-c.md`.\n")]),
        ("CLAUDE.md names missing file knowledge/sigv4-b2.md", [("CLAUDE.md", "Rule; `knowledge/sigv4-b2.md`.\n")]),
        ("CLAUDE.md names missing file knowledge/b.md", [("CLAUDE.md", "Rule; see ./knowledge/b.md.\n")]),
        ("CLAUDE.md names missing file knowledge/b.md", [("CLAUDE.md", "See knowledge/b.md--it moved.\n")]),
        ("CLAUDE.md names missing file knowledge/sub/b.md", [("CLAUDE.md", "Rule; knowledge/sub/b.md.\n")]),
        ("CLAUDE.md names missing file knowledge/b.MD", [("CLAUDE.md", "Rule; knowledge/b.MD.\n")]),
        ("CLAUDE.md names missing file knowledge/a-typo.md", [("CLAUDE.md", "See `knowledge/a.md` and `knowledge/a-typo.md`.\n")]),
        ("CLAUDE.md is missing", [("CLAUDE.md", None)]),
        ("INDEX.md names missing file knowledge/b.md", [("INDEX.md", index + "Also see `knowledge/b.md`.\n")]),
        ("knowledge/a.md names missing file knowledge/b.md", [("knowledge/a.md", entry.format("a", "a") + "See knowledge/b.md.\n")]),
        ("not a .md entry", [("INDEX.md", index + "- [x](knowledge/sub/x.md)\n"), ("knowledge/sub/x.md", entry.format("x", "a"))]),
        ("not a .md entry", [("INDEX.md", index + "- [x](knowledge/x.MD)\n"), ("knowledge/x.MD", entry.format("x", "a"))]),
        ("missing frontmatter", [("knowledge/a.md", "no frontmatter\n")]),
        ("missing frontmatter", [("knowledge/a.md", "Intro.\n" + entry.format("a", "a"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("description: d\n", ""))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("description: d\n", "description:\n"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("description: d\n", 'description: ""\n'))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("description: d\n", "description: null\n"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("description: d\n", "")
                                  + "\n---\n\ndescription: later\n---\n")]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("  type: project\n", ""))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("  type: project\n", "  type:\n"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("  type: project\n", "  type: lesson\n"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("  type: project\n", "  type: projects\n"))]),
        ("missing frontmatter", [("knowledge/a.md", entry.format("a", "a").replace("name: a\n", "name:\n"))]),
        ("is not the file name", [("knowledge/a.md", entry.format("b", "a"))]),
        ("merge-conflict marker", [("INDEX.md", index + "<<<<<<< HEAD\n")]),
        ("INDEX.md: merge-conflict marker", [("INDEX.md", index + "<!--\n<<<<<<< HEAD\n-->\n")]),
        ("merge-conflict marker", [("knowledge/a.md", entry.format("a", "a") + ">>>>>>> branch\n")]),
        ("CLAUDE.md: merge-conflict marker", [("CLAUDE.md", "Rules.\n<<<<<<< HEAD\n")]),
        (["INDEX.md:2: credential-shaped string"], [("INDEX.md", index + secrets[0] + "\nMore.\n")]),
        (["INDEX.md:2: credential-shaped string"], [("INDEX.md", index + "<!-- old hook: " + [s for s in secrets if "/webhooks/" in s][0] + " -->\nMore.\n")]),
        (["knowledge/sub/y.txt has no INDEX.md line", "knowledge/sub/y.txt: not a .md entry directly in knowledge/",
          "knowledge/sub/y.txt:1: credential-shaped string"], [("knowledge/sub/y.txt", secrets[0] + "\nMore.\n")]),
    ] + [(["knowledge/a.md:9: credential-shaped string"], [("knowledge/a.md", entry.format("a", "a") + s + "\nMore.\n")])
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
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                errors, warnings = lint(root, floor=1)
            if out.getvalue():
                failures.append(f"{files}: lint() printed {len(out.getvalue())} characters")
            if expected is None and (errors or warnings):
                failures.append(f"clean store {files} gave {errors + warnings}")
            elif isinstance(expected, list) and (errors != expected or warnings):
                failures.append(f"{files} gave {len(errors)} error(s) and {len(warnings)} warning(s), expected exactly {expected}")
            elif isinstance(expected, str) and not any(expected in e for e in errors):
                failures.append(f"{files} gave {errors}, expected '{expected}'")

        reset("a")
        if lint(root, floor=2)[0] != ["knowledge/ holds fewer than 2 entries (1): is this the right tree?"]:
            failures.append(f"a store below the floor gave {lint(root, floor=2)[0]}")
        put("INDEX.md", index + "<!-- see [[later]] -->\n")
        if lint(root, floor=1)[1] != ["INDEX.md: [[later]] resolves to no entry (worth writing?)"]:
            failures.append(f"a wikilink in an INDEX.md comment gave {lint(root, floor=1)[1]}")

        reset("later")
        if lint(root, floor=1)[1] != ["knowledge/a.md: [[later]] resolves to no entry (worth writing?)"]:
            failures.append(f"dangling wikilink gave {lint(root, floor=1)[1]}")
        # The script as CI runs it, from scripts/ under the store, at its own FLOOR: exit 0 with FLOOR
        # entries and a warning, 1 with an unindexed entry, and 1 with one entry fewer than FLOOR.
        if FLOOR < 5:
            failures.append(f"FLOOR is {FLOOR}; below 5 it stops telling a real store from an empty one")
        script = os.path.join(root, "scripts", "lint_knowledge.py")
        os.makedirs(os.path.dirname(script), exist_ok=True)
        shutil.copyfile(os.path.abspath(__file__), script)
        more = [f"e{n}" for n in range(1, max(FLOOR, 2))]
        put("INDEX.md", index + "".join(f"- [{e}](knowledge/{e}.md) - hook\n" for e in more))
        for e in more:
            put(f"knowledge/{e}.md", entry.format(e, "a"))
        codes = [subprocess.run([sys.executable, script], capture_output=True).returncode]
        put("knowledge/c.md", entry.format("c", "a"))
        codes.append(subprocess.run([sys.executable, script], capture_output=True).returncode)
        put("knowledge/c.md", None)
        put(f"knowledge/{more[-1]}.md", None)
        put("INDEX.md", index + "".join(f"- [{e}](knowledge/{e}.md) - hook\n" for e in more[:-1]))
        codes.append(subprocess.run([sys.executable, script], capture_output=True).returncode)
        if codes != [0, 1, 1]:
            failures.append(f"exit codes {codes} for a store with a warning, with an error, and below FLOOR; expected [0, 1, 1]")

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
