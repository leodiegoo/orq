#!/usr/bin/env python3
"""Looks for forbidden terms in the versioned files of the public repository.

The list lives outside the repository (ORQ_TERMOS, default ~/.claude/orquestrador-plan/termos-proibidos.txt): one term per line,
case-insensitive; `re:` at the start of a line makes it a regular expression; `#` comments. Without the list, it warns and exits 0.
Fails (1) with file:line. Usage: audiencia-check.py [repository-root]"""
import os
import re
import subprocess
import sys


def terms(path):
    defaults = []
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        defaults.append(re.compile(line[3:] if line.startswith("re:") else re.escape(line), re.I))
    return defaults


def findings(root, defaults):
    files_set = subprocess.run(["git", "-C", root, "ls-files", "-z"], capture_output=True, text=True, check=True).stdout.split("\0")
    for item_name in filter(None, files_set):
        try:
            text_value = open(os.path.join(root, item_name), encoding="utf-8").read()
        except (OSError, UnicodeDecodeError):
            continue  # binary (png) or deleted in the index
        for n, line in enumerate(text_value.splitlines(), 1):
            for p in defaults:
                if p.search(line):
                    yield f"{item_name}:{n}: termo proibido /{p.pattern}/"


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    listing = os.environ.get("ORQ_TERMOS") or os.path.expanduser("~/.claude/orquestrador-plan/termos-proibidos.txt")
    if not os.path.exists(listing):
        print(f"audiencia-check: sem {listing}, checagem pulada", file=sys.stderr)
        return 0
    failures = list(findings(root, terms(listing)))
    print("\n".join(failures))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
