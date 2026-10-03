#!/usr/bin/env python3
"""Anonymizes the real night log into the versioned replay fixture (ticket 349). English alias: scripts/anonymize-night.py.

  anonimizar-noite.py --in <dir with the real events.jsonl and cursor.json> --out fixtures/noite-2026-10-01

The repository is public, the real log is not: it holds the owner's absolute paths, the product's repository and PR URLs, terminal handles and project names.
The swap is deterministic (the same input gives the same bytes) and one mapping serves every file, so `events.jsonl`, `cursor.json`, and the `verdade.json` and
`ciclos.log` beside them (copied through the same swap when the input has them: they cite the same Run and task ids) stay consistent with each other:

  /Users/<user>/Developer/<org>/<repo>, /Users/<user>/orca/workspaces/<repo>  ->  /home/dev/<app-a>   (one fake per project, in order of appearance; the rest of the path stays)
  any other /Users/<user>/...                                                  ->  /home/dev/...
  https://github.com/<org>/<repo>/pull/N                                       ->  https://github.com/example-org/app/pull/N   (same N)
  term_<hex>, run_<hex>, task_<hex>                                            ->  term_001, run_001, task_001 (sequential, in order of appearance)
  project and group names, `prisma <id>` (a client), a repo or org name        ->  app-a, app-b, ... (`example-org` for an org), the same name always the same fake
  e-mail addresses                                                             ->  dev@example.com

Nothing is written when the result still holds something that must not be public: a `/Users/` path, a `/home/` path of someone else, an e-mail, a GitHub URL outside
example-org, a name the swap read from the input, or a term of the forbidden list (`ORQ_TERMOS`, default termos-proibidos.txt in the plan). Exits 1 with file:line."""
import argparse
import importlib.util
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import orqpaths  # noqa: E402

FILES = ("events.jsonl", "cursor.json")  # required
ALSO = ("verdade.json", "ciclos.log")  # copied through the same swap when the input has them
KEEP = {"orq"}  # the public project itself: never a fake name
SEEN = re.compile(
    r'/Users/[^/\s"\\]+/(?:Developer/[^/\s"\\]+/(?P<repo>[^/\s"\\]+)|orca/workspaces/(?P<repo2>[^/\s"\\]+))'
    r'|https://github\.com/(?P<org>[^/\s"\\]+)/(?P<urlrepo>[^/\s"\\]+)'
    r'|"(?P<field>project|group)": "(?P<value>[^"]*)"'
    r'|\bprisma[ -](?P<client>\d+)\b', re.I)
PROJECT_PATH = re.compile(r'/Users/[^/\s"\\]+/(?:Developer/[^/\s"\\]+/|orca/workspaces/)(?P<repo>[^/\s"\\]+)')
ANY_PATH = re.compile(r'/Users/[^/\s"\\]+/')
PR_URL = re.compile(r'https://github\.com/[^/\s"\\]+/[^/\s"\\]+')
HANDLE = re.compile(r'\b(?:term_[0-9a-f]{8}(?:-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})?|run_[0-9a-f]{12}|task_[0-9a-f]{12})\b')
CLIENT = re.compile(r'\bprisma[ -](\d+)\b', re.I)
GROUP = re.compile(r'("group": ")([^"]*)(")')
EMAIL = re.compile(r'[\w.+-]+@[\w-]+(?:\.[\w-]+)+')
LEFT = (("/Users/", re.compile(r"/Users/")), ("someone else's /home/ path", re.compile(r"/home/(?!dev\b)")), ("e-mail", re.compile(r"(?<!dev)@[\w-]+\.[a-z]{2,}", re.I)),
        ("GitHub URL outside example-org", re.compile(r"github\.com/(?!example-org/)")))


def _letters(n):
    """1 -> a, 26 -> z, 27 -> aa."""
    out = ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(97 + r) + out
    return out


def _word(name):
    return re.compile(rf"(?<!\w){re.escape(name)}(?!\w)", re.I)


class Swap:
    def __init__(self, texts):
        self.fake, self.ids, self.orgs = {}, {}, set()
        for text in texts:  # first pass in order of appearance, so the fakes come out the same wherever the swap runs
            for m in SEEN.finditer(text):
                if name := m["repo"] or m["repo2"] or m["urlrepo"] or (m["field"] == "project" and m["value"]):
                    self._name(name)
                if m["org"]:
                    self.orgs.add(m["org"])
                if m["field"] == "group" and m["value"]:
                    self._name(m["value"], group=True)
                if m["client"]:
                    self._name(f"prisma {m['client']}")
        self.names = sorted((n for n in self.fake if n not in KEEP and not n.startswith("group:")), key=len, reverse=True)

    def _name(self, name, group=False):
        if name not in KEEP:
            self.fake.setdefault(f"group:{name}" if group else name, f"app-{_letters(len(self.fake) + 1)}")

    def handle(self, m):
        kind = m[0].split("_")[0]
        table = self.ids.setdefault(kind, {})
        return f"{kind}_{table.setdefault(m[0], len(table) + 1):03d}"

    def __call__(self, text):
        text = PROJECT_PATH.sub(lambda m: "/home/dev/" + self.fake.get(m["repo"], m["repo"]), text)
        text = ANY_PATH.sub("/home/dev/", text)
        text = PR_URL.sub("https://github.com/example-org/app", text)
        text = HANDLE.sub(self.handle, text)
        text = CLIENT.sub(lambda m: self.fake[f"prisma {m[1]}"], text)
        text = GROUP.sub(lambda m: m[1] + self.fake.get(f"group:{m[2]}", m[2]) + m[3], text)
        for name in self.names:
            text = _word(name).sub(self.fake[name], text)
        for org in sorted(self.orgs, key=len, reverse=True):
            text = _word(org).sub("example-org", text)
        return EMAIL.sub("dev@example.com", text)

    def leftovers(self, text):
        """[(line, why)] of what must not survive in the swapped `text`."""
        checks = [*LEFT, *((f"the name {n!r} of the input", _word(n)) for n in [*self.names, *self.orgs])]
        return [(n, why) for n, line in enumerate(text.splitlines(), 1) for why, rx in checks if rx.search(line)]


def forbidden_terms():
    """The compiled forbidden list (ORQ_TERMOS, default termos-proibidos.txt in the plan), or [] with a warning when there is none."""
    listing = os.environ.get("ORQ_TERMOS") or os.path.join(orqpaths.PLAN, "termos-proibidos.txt")
    if not os.path.exists(listing):
        print(f"anonimizar-noite: no {listing}, forbidden-terms check skipped", file=sys.stderr)
        return []
    spec = importlib.util.spec_from_file_location("audiencia_check", os.path.join(os.path.dirname(os.path.realpath(__file__)), "audiencia-check.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.terms(listing)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Anonymizes the real night log into the replay fixture.")
    ap.add_argument("--in", dest="src", required=True, help="folder with the real events.jsonl and cursor.json")
    ap.add_argument("--out", required=True, help="fixture folder to write (fixtures/noite-2026-10-01)")
    a = ap.parse_args(argv)
    missing = [f for f in FILES if not os.path.isfile(os.path.join(a.src, f))]
    if missing:
        print(f"anonimizar-noite: {', '.join(missing)} not found in {a.src}", file=sys.stderr)
        return 1
    names = [*FILES, *(f for f in ALSO if os.path.isfile(os.path.join(a.src, f)))]
    texts = {f: open(os.path.join(a.src, f), encoding="utf-8", newline="").read() for f in names}
    swap, terms = Swap(texts.values()), forbidden_terms()
    out = {f: swap(t) for f, t in texts.items()}
    problems = [f"{f}:{n}: {why}" for f, t in out.items() for n, why in swap.leftovers(t)]
    problems += [f"{f}:{n}: forbidden term /{p.pattern}/" for f, t in out.items() for n, line in enumerate(t.splitlines(), 1) for p in terms if p.search(line)]
    if problems:
        print("anonimizar-noite: nothing written, the swap left:\n" + "\n".join(problems), file=sys.stderr)
        return 1
    os.makedirs(a.out, exist_ok=True)
    for f, t in out.items():
        with open(os.path.join(a.out, f), "w", encoding="utf-8", newline="") as fh:
            fh.write(t)
    print(f"anonimizar-noite: {len(out)} file(s) in {a.out}, {len(swap.fake)} name(s), {sum(map(len, swap.ids.values()))} id(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
