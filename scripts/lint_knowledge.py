#!/usr/bin/env python3
"""Knowledge lint: the CI gate for the in-repo knowledge store (INDEX.md + knowledge/).

Fails (exit 1) on:
  * an INDEX.md link into knowledge/ whose target file does not exist
  * a knowledge/*.md file no INDEX.md line links to
  * an entry without frontmatter (a --- block with name: and description:), or whose name:
    is not its file name, so a [[slug]] link and the file it means cannot drift apart
  * a credential-shaped string in INDEX.md or any entry (secret VALUES are banned; variable
    names and flags are fine)
  * a merge-conflict marker in INDEX.md or any entry: a half-resolved INDEX.md merge is
    otherwise a well-formed index with two extra lines

Warns without failing on a [[wikilink]] that resolves to no entry: it marks an entry worth
writing.

Usage: python scripts/lint_knowledge.py [--self-test]
"""
import os
import re
import sys
import tempfile

SECRET_PATTERNS = [
    r"gh[pousr]_[A-Za-z0-9]{30,}",                                 # GitHub tokens
    r"github_pat_[A-Za-z0-9_]{30,}",
    r"sk-ant-[A-Za-z0-9_\-]{20,}",                                 # Anthropic API keys
    r"AKIA[A-Z0-9]{16}",                                           # AWS access key ids
    r"BEGIN [A-Z ]*PRIVATE KEY",
    r"Bearer [A-Za-z0-9_\-\.]{25,}",
    r"[MNO][A-Za-z0-9_\-]{23,27}\.[A-Za-z0-9_\-]{6}\.[A-Za-z0-9_\-]{27,}",  # Discord bot tokens
]
CONFLICT_MARKER = re.compile(r"^(<<<<<<< |>>>>>>> )", re.M)


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read().replace("\r\n", "\n")


def lint(root):
    """(errors, warnings) for the store under root."""
    errors, warnings = [], []
    kdir = os.path.join(root, "knowledge")
    index = read(os.path.join(root, "INDEX.md"))
    linked = set(re.findall(r"\]\(knowledge/([^)#]+\.md)\)", index))
    files = {f for f in os.listdir(kdir) if f.endswith(".md")}

    for t in sorted(linked - files):
        errors.append(f"INDEX.md links missing file: knowledge/{t}")
    for f in sorted(files - linked):
        errors.append(f"knowledge/{f} has no INDEX.md line")

    bodies = {"INDEX.md": index}
    names = set()
    for f in sorted(files):
        body = bodies[f"knowledge/{f}"] = read(os.path.join(kdir, f))
        m = re.match(r"---\n(.*?)\n---\n", body, re.S)
        name = m and re.search(r"^name:\s*(\S+)", m.group(1), re.M)
        if not name or not re.search(r"^description:\s*\S", m.group(1), re.M):
            errors.append(f"knowledge/{f}: missing frontmatter (name: + description:)")
        elif name.group(1) != f[:-3]:
            errors.append(f"knowledge/{f}: name: {name.group(1)} is not the file name")
        else:
            names.add(name.group(1))

    for where, body in bodies.items():
        if CONFLICT_MARKER.search(body):
            errors.append(f"{where}: merge-conflict marker")
        for pat in SECRET_PATTERNS:
            for hit in re.finditer(pat, body):
                errors.append(f"{where}: credential-shaped string {hit.group(0)[:12]}...")
        for link in re.findall(r"\[\[([^\]]+)\]\]", body):
            if link not in names:
                warnings.append(f"{where}: [[{link}]] resolves to no entry (worth writing?)")
    return errors, warnings


def self_test():
    """Every error class fires on a store built to have it, and a clean store passes."""
    entry = "---\nname: {0}\ndescription: d\nmetadata:\n  type: project\n---\n\nBody [[{1}]].\n"
    with tempfile.TemporaryDirectory() as root:
        os.mkdir(os.path.join(root, "knowledge"))

        def put(rel, text):
            with open(os.path.join(root, rel), "w", encoding="utf-8", newline="\n") as f:
                f.write(text)

        put("INDEX.md", "- [a](knowledge/a.md) - hook\n")
        put("knowledge/a.md", entry.format("a", "a"))
        assert lint(root) == ([], []), lint(root)

        put("knowledge/a.md", entry.format("a", "later"))
        assert lint(root)[1] == ["knowledge/a.md: [[later]] resolves to no entry (worth writing?)"]

        cases = {
            "INDEX.md links missing file": ("INDEX.md", "- [a](knowledge/a.md)\n- [b](knowledge/b.md)\n"),
            "has no INDEX.md line": ("knowledge/c.md", entry.format("c", "a")),
            "missing frontmatter": ("knowledge/a.md", "no frontmatter\n"),
            "is not the file name": ("knowledge/a.md", entry.format("b", "a")),
            "merge-conflict marker": ("INDEX.md", "- [a](knowledge/a.md)\n<<<<<<< HEAD\n"),
            "credential-shaped string": ("knowledge/a.md", entry.format("a", "a") + "ghp_" + "x" * 36 + "\n"),
        }
        for expected, (rel, text) in cases.items():
            put("INDEX.md", "- [a](knowledge/a.md) - hook\n")
            put("knowledge/a.md", entry.format("a", "a"))
            if os.path.exists(os.path.join(root, "knowledge/c.md")):
                os.remove(os.path.join(root, "knowledge/c.md"))
            put(rel, text)
            errors = lint(root)[0]
            assert any(expected in e for e in errors), (expected, errors)
    print("self-test OK")


def main():
    if "--self-test" in sys.argv:
        self_test()
        return 0
    errors, warnings = lint(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    for w in warnings:
        print(f"WARN  {w}")
    for e in errors:
        print(f"ERROR {e}")
    print(f"{len(errors)} error(s), {len(warnings)} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
