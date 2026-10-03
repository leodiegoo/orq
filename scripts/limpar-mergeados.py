#!/usr/bin/env python3
"""Removes already-merged Orca worktrees, local branches and remote branches (only from the user's PRs).

Usage: limpar-mergeados.py [--repo <path>] [--branch <name>] [--task <id>] [--dry-run] [--json] [--self-test]
Without --repo, uses the cwd's git root. Summary of the last run in ~/.claude/logs/limpar-mergeados.last.json.
With --branch, only that branch (worktree, local and remote) is considered; with --task, the result becomes a `pr`/`cleaned` event in orq's log.
A worktree with no merged PR also goes when orq proves its branch is in main (`orqlib.integration_proof`: a cycle of the integrator recorded the tip, or every commit is patch-equivalent in main), after a bundle of its commits; the 24 h window counts from the end of that cycle.
Never uses --force; an error on one item goes into the summary and not onto the others.
An untracked file that is an orq artifact (ORQ_ARTIFACTS) doesn't block removing the worktree: it is copied to RELATORIOS first.
"""
import shutil
import argparse
import fnmatch
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

HOME = os.path.expanduser("~")
KEEP_FILE = os.path.join(HOME, ".claude/scripts/limpar-mergeados.keep")
LAST = os.path.join(HOME, ".claude/logs/limpar-mergeados.last.json")
PROTECTED = set()  # the branches that are never deleted: main() fills this with the project's environments (`repo_flow`), the self-test with the example's
ORPHAN_IDLE_H = 24  # an orphan worktree only goes after this long without activity
ORQ_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # the clone this script belongs to (~/.claude/scripts/ links here)
ORQ = os.path.join(ORQ_DIR, "orq.py")
sys.path.insert(0, ORQ_DIR)
import orqpaths  # noqa: E402 - stdlib only, like this script
REPORTS = os.environ.get("ORQ_RELATORIOS") or os.path.join(orqpaths.PLAN, "relatorios")
# Written by the worker itself at orq's request: not the worker's work (paths relative to the worktree root).
ORQ_ARTIFACTS = ("PAUSE.md", "HANDOFF.md", "final-report*.md", ".scratch/*/final-report.md",
                 "PAUSA.md", "PASSAGEM.md", "relatorio*.md", ".scratch/*/relatorio-final.md")  # the pt names: worker that follows the old spec


def repo_flow(repo):
    """{producao, ambientes} of that repository's project, read from orq (`orq flow --repo`): in a promotion flow the same branch goes by PR to each environment and only
    the merge into production closes it. ORQ_FINAL_BASE and ORQ_PROTECTED_BRANCHES (comma-separated list) force one of the two. Without orq, the remote's default branch."""
    flow_info = None
    try:
        r = run([sys.executable, ORQ, "flow", "--repo", repo, "--json"])
        flow_info = json.loads(r.stdout) if r.returncode == 0 else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        pass
    if not flow_info:
        ref = run(["git", "symbolic-ref", "-q", "--short", "refs/remotes/origin/HEAD"], repo).stdout.strip().removeprefix("origin/")
        flow_info = {"producao": ref, "ambientes": [ref] if ref else []}
    if os.environ.get("ORQ_FINAL_BASE"):
        flow_info["producao"] = os.environ["ORQ_FINAL_BASE"]
    if os.environ.get("ORQ_PROTECTED_BRANCHES"):
        flow_info["ambientes"] = os.environ["ORQ_PROTECTED_BRANCHES"].split(",")
    return flow_info


def is_final(pr, branch, flow_info):
    """The merge that closes the branch: into the project's production, or a merge/<feature>-<environment> one into the environment itself."""
    return pr["baseRefName"] == flow_info["producao"] or (branch.startswith("merge/") and pr["baseRefName"] in flow_info["ambientes"])


def pr_final(prs, branch, flow_info):
    """The newest merged PR that closes the branch, or None."""
    final = [p for p in prs if is_final(p, branch, flow_info)]
    return max(final, key=lambda p: p["number"]) if final else None


def run(args, cwd=None):
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=120)


def load_keep(path=KEEP_FILE):
    try:
        lines = open(path).read().splitlines()
    except OSError:
        return []
    return [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]


def is_kept(branch, patterns):
    return any(fnmatch.fnmatchcase(branch, p) for p in patterns)


def dirt(where):
    """(blockers, artifacts) of the worktree: `git status` lines that hold back the removal, and paths of untracked orq artifacts.
    Only an untracked file can be an artifact; any other (modified, staged, deleted) or an untracked one with another name blocks."""
    blockers, artifacts = [], []
    for l in run(["git", "status", "--porcelain", "-uall"], where).stdout.splitlines():
        path = l[3:].strip('"')
        if l.startswith("?? ") and any(fnmatch.fnmatchcase(path, p) and (p.startswith(".scratch/") or "/" not in path) for p in ORQ_ARTIFACTS):
            artifacts.append(path)
        else:
            blockers.append(l)
    return blockers, artifacts


def store(where, artifacts, dest=None):
    """Copies the artifacts to RELATORIOS/<worktree>-<file> (the `/` in the path becomes `-`) and returns the destinations. Raises OSError if a copy fails."""
    dest = dest or REPORTS
    os.makedirs(dest, exist_ok=True)
    item_name = os.path.basename(where.rstrip("/"))
    out = []
    for c in artifacts:
        d = os.path.join(dest, f"{item_name}-{c.replace('/', '-')}")
        shutil.copy2(os.path.join(where, c), d)
        out.append(d)
    return out


def decide(f):
    """f: facts of an item. Returns (remove|skip, reason). 'remote' requires author and existence on the remote."""
    if f["branch"] in PROTECTED or f.get("is_main_current"):
        return "skip", "protected branch"
    if f["kept"]:
        return "skip", "listed in limpar-mergeados.keep"
    if not f["merged"]:
        return "skip", "no merged PR"
    if f["open_head"]:
        return "skip", "an open PR uses the branch as head"
    if f["kind"] == "remote":
        if not f["mine"]:
            return "skip", "merged PR is not the user's"
        if not f["on_remote"]:
            return "skip", "no longer on the remote"
        if f["open_base"]:
            return "skip", "an open PR uses the branch as base"
        if not f["tip_matches"]:
            return "skip", "remote has a commit after the merge"
        return "remove", "merged PR by the user"
    if f["kind"] == "worktree" and f["dirty"]:
        return "skip", "worktree has changes or untracked files"
    if f["ahead"]:
        return "skip", "commit outside the PR base"
    return "remove", "merged PR and no pending work"


def live_children(worktree, by_id):
    """Existing Orca child worktrees that must outlive this parent removal attempt."""
    child_ids = set(worktree.get("childWorktreeIds", []))
    child_ids.update(w.get("id") for w in by_id.values() if w.get("parentWorktreeId") == worktree.get("id"))
    return [by_id[c] for c in child_ids if c in by_id and os.path.isdir(by_id[c].get("path", ""))]


def decide_orphan(f):
    """Worktree with no merged PR. Only goes if nothing in it is lost: every commit is already in main (cherry with no "+", e.g. cherry-pick), clean tree,
    no live orq worker (`busy` None, no answer from orq, counts as live) and no recent activity (a worktree that was just born has no commit either)."""
    if f["branch"] in PROTECTED or f["branch"].startswith("prototype/") or f["kept"]:
        return "skip", "protected branch or listed in limpar-mergeados.keep"
    if f["open_head"]:
        return "skip", "an open PR uses the branch as head"
    if f["dirty"]:
        return "skip", "worktree has changes or untracked files"
    if f["ahead"] and not f.get("integrated"):
        return "skip", "commit outside main"
    if f["busy"] is not False:
        return "skip", "a live orq worker uses the worktree"
    if f["recente"]:
        return "skip", f"{'its integration cycle ended' if f.get('integrated') else 'activity'} in the last {ORPHAN_IDLE_H} h"
    return "remove", f"integrated by orq ({f['integrated']}), clean tree and no worker" if f.get("integrated") else "no commit outside main, clean tree and no worker"


def is_ahead(base, head, cwd, pr_head_oid=None):
    """True if `head` has work outside `base`. A git failure counts as ahead (when in doubt, don't delete).
    `head` is a revision (never the worktree path). Squash-merge: HEAD == the PR's headRefOid proves that
    everything the branch has went through the PR; any extra commit changes the oid and counts as ahead again."""
    if pr_head_oid and run(["git", "rev-parse", "--verify", "-q", head + "^{commit}"], cwd).stdout.strip() == pr_head_oid:
        return False
    ch = run(["git", "cherry", base, head], cwd)
    return ch.returncode != 0 or any(l.startswith("+") for l in ch.stdout.splitlines())


def self_test_git():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        def g(*a, cwd=d):
            r = run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], cwd)
            assert r.returncode == 0, r.stderr
            return r.stdout.strip()
        g("init", "-q", "-b", "development")
        for n in "ab":
            open(f"{d}/{n}", "w").write(n); g("add", n); g("commit", "-qm", n)
        g("update-ref", "refs/remotes/origin/development", "HEAD")
        wt = d + "-wt"
        g("worktree", "add", "-q", "-b", "feat/x", wt)
        open(f"{wt}/x", "w").write("x"); g("add", "x", cwd=wt); g("commit", "-qm", "x", cwd=wt)
        assert is_ahead("origin/development", "HEAD", wt)  # commit still outside the base
        g("merge", "--no-ff", "-qm", "merge", "feat/x")  # merge commit, like a PR
        g("update-ref", "refs/remotes/origin/development", "HEAD")
        assert not is_ahead("origin/development", "HEAD", wt)  # regression: cherry received the worktree path
        assert is_ahead("origin/development", wt, d)  # a path is not a revision: failure becomes ahead
        open(f"{wt}/y", "w").write("y"); g("add", "y", cwd=wt); g("commit", "-qm", "y", cwd=wt)
        assert is_ahead("origin/development", "HEAD", wt)  # new commit after the merge
        # squash: patch-id doesn't match; only HEAD == the PR's headRefOid proves nothing was left out
        tip = g("rev-parse", "feat/x")
        prs = [{"number": 5, "baseRefName": "development"}, {"number": 3, "baseRefName": "main"}, {"number": 4, "baseRefName": "main"}]
        flow_info = {"producao": "main", "ambientes": ["development", "staging", "main"]}
        assert pr_final(prs, "feat/x", flow_info)["number"] == 4 and pr_final(prs[:1], "feat/x", flow_info) is None  # development doesn't close it; the highest one in main wins
        assert pr_final(prs[:1], "merge/feat-development", flow_info)["number"] == 5 and pr_final(prs[:1], "feat/merge/x", flow_info) is None
        assert pr_final(prs, "feat/x", {"producao": "trunk", "ambientes": ["trunk"]}) is None  # production comes from the project, not from a fixed name
        assert not is_ahead("origin/nope", "feat/x", d, tip)  # HEAD == the PR's headRefOid
        assert is_ahead("origin/nope", "feat/x", d, "0" * 40)
        # cherry-pick: new sha, same patch-id; cherry doesn't see a "+" commit and the branch is orphaned
        g("checkout", "-q", "-b", "feat/cp", "origin/development")
        open(f"{d}/cp", "w").write("cp"); g("add", "cp"); g("commit", "-qm", "cp")
        g("checkout", "-q", "development")
        assert is_ahead("origin/development", "feat/cp", d)  # still outside the base
        g("cherry-pick", "feat/cp")
        g("update-ref", "refs/remotes/origin/development", "HEAD")
        assert not is_ahead("origin/development", "feat/cp", d)  # came in by cherry-pick
        g("worktree", "remove", "--force", wt)


def self_test_dirt():
    import tempfile
    with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as rel:
        def g(*a):
            r = run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], d)
            assert r.returncode == 0, r.stderr
        g("init", "-q", "-b", "main")
        open(f"{d}/a", "w").write("a"); g("add", "a"); g("commit", "-qm", "a")
        assert dirt(d) == ([], [])
        os.makedirs(f"{d}/.scratch/feat")
        for n in ("PAUSA.md", "relatorio-final.md", "PASSAGEM.md", ".scratch/feat/relatorio-final.md"):
            open(f"{d}/{n}", "w").write(n)
        bl, ar = dirt(d)  # worktree with only orq artifacts: nothing blocks
        assert bl == [] and sorted(ar) == [".scratch/feat/relatorio-final.md", "PASSAGEM.md", "PAUSA.md", "relatorio-final.md"], (bl, ar)
        assert decide(dict(branch="feat/a", kind="worktree", kept=False, merged=True, open_head=False, dirty=bool(bl), ahead=False))[0] == "remove"
        stored = store(d, ar, rel)  # copied before removing
        assert sorted(os.listdir(rel)) == sorted(f"{os.path.basename(d)}-{c.replace('/', '-')}" for c in ar) and len(stored) == 4
        assert open(f"{rel}/{os.path.basename(d)}-PAUSA.md").read() == "PAUSA.md"
        open(f"{d}/notas.md", "w").write("x")  # any other untracked file blocks
        bl, ar = dirt(d)
        assert bl == ["?? notas.md"] and len(ar) == 4
        assert decide(dict(branch="feat/a", kind="worktree", kept=False, merged=True, open_head=False, dirty=True, ahead=False))[0] == "skip"
        os.remove(f"{d}/notas.md")
        open(f"{d}/a", "w").write("mod")  # modified tracked file blocks; an artifact with the same name outside the pattern too
        assert dirt(d)[0] == [" M a"]
        g("checkout", "-q", "a")
        os.makedirs(f"{d}/src"); open(f"{d}/src/PAUSA.md", "w").write("x")
        assert dirt(d)[0] == ["?? src/PAUSA.md"]  # only the root counts; a subfolder is a worker file


def self_test():
    PROTECTED.update({"main", "development", "staging"})  # the example's environments; in real use they come from the project
    self_test_git()
    self_test_dirt()
    orphan = dict(branch="feat/o", kept=False, open_head=False, dirty=False, ahead=False, busy=False, recente=False)
    assert decide_orphan(orphan)[0] == "remove"  # cherry-pick: no "+" commit, clean tree, no worker
    for k, v in dict(branch="main", kept=True, open_head=True, dirty=True, ahead=True, busy=True, recente=True).items():
        assert decide_orphan({**orphan, k: v})[0] == "skip", k
    assert decide_orphan({**orphan, "branch": "prototype/2039-x"})[0] == "skip"  # prototype never, even outside .keep
    assert decide_orphan({**orphan, "ahead": True, "integrated": "registro"})[0] == "remove"  # a tip outside main that an integrator cycle recorded
    assert decide_orphan({**orphan, "ahead": True, "integrated": "registro", "dirty": True})[0] == "skip" and decide_orphan({**orphan, "ahead": True, "integrated": "cherry", "recente": True})[0] == "skip"
    assert decide_orphan({**orphan, "ahead": True})[0] == "skip"
    assert decide_orphan({**orphan, "busy": None})[0] == "skip"  # no answer from orq about workers, when in doubt it doesn't delete
    assert is_kept("prototype/2039-x", ["prototype/*"]) and not is_kept("feat/x", ["prototype/*"])
    assert is_kept("main_bkp_1", ["main_bkp_*"]) and is_kept("feat/plataform-metrics", ["feat/plataform-metrics"])
    parent = {"id": "parent", "childWorktreeIds": ["child"]}
    child = {"id": "child", "path": os.path.dirname(__file__)}
    assert live_children(parent, {"parent": parent, "child": child}) == [child]
    base = dict(branch="feat/a", kind="worktree", kept=False, merged=True, open_head=False, open_base=False,
                dirty=False, ahead=False, mine=True, on_remote=True, tip_matches=True)
    assert decide(base)[0] == "remove"
    for k, v in dict(kept=True, merged=False, open_head=True, dirty=True, ahead=True, branch="development").items():
        assert decide({**base, k: v})[0] == "skip", k
    assert decide({**base, "kind": "local", "dirty": True})[0] == "remove"  # dirt only counts in a worktree
    assert decide({**base, "is_main_current": True})[0] == "skip"
    r = {**base, "kind": "remote"}
    assert decide({**r, "dirty": True, "ahead": True})[0] == "remove"  # remoto ignora estado local
    for k, v in dict(mine=False, on_remote=False, open_base=True, tip_matches=False).items():
        assert decide({**r, k: v})[0] == "skip", k
    print("self-test ok")


def orq_proof(repo, branch):
    """orq's proof that `branch` is in main (`integration_proof`: contained, recorded by an integrator cycle, or every commit patch-equivalent): {via, since} or None.
    orq doesn't answer: None, and the branch stays as it was (ticket 327)."""
    try:
        from orqlib import integration_proof
        return integration_proof(repo, branch, "main")
    except Exception as e:  # noqa: BLE001 - without orq the hook decides only by the PR and by `git cherry`
        print(f"limpar-mergeados: orq proof: {e}", file=sys.stderr)
        return None


def busy_worktrees():
    """Paths of the worktrees with a live worker, via `orq busy`; None if orq doesn't answer."""
    p = run(["python3", ORQ, "busy"])
    return None if p.returncode else set(p.stdout.split("\n"))


def gh_json(args, cwd):
    p = run(["gh", *args], cwd)
    if p.returncode:
        raise RuntimeError(f"gh {' '.join(args[:3])}: {p.stderr.strip()[:200]}")
    return json.loads(p.stdout or "[]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo")
    ap.add_argument("--branch")
    ap.add_argument("--task")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()

    cwd = a.repo or os.getcwd()
    top = run(["git", "rev-parse", "--show-toplevel"], cwd)
    if top.returncode:
        sys.exit(f"not a git repository: {cwd}")
    cwd = top.stdout.strip()
    # the cwd may be a worktree; the main checkout is the first one in `git worktree list`
    main_path = run(["git", "worktree", "list", "--porcelain"], cwd).stdout.split("\n")[0].removeprefix("worktree ")
    flow_info = repo_flow(main_path)
    PROTECTED.update(flow_info["ambientes"], [flow_info["producao"]])
    items = []

    def add(kind, name, action, reason):
        items.append({"repo": main_path, "kind": kind, "name": name, "action": action, "reason": reason})

    def act(kind, name, verdict, reason, cmd, cwd_=None):
        if verdict == "skip":
            return add(kind, name, "skip", reason)
        if a.dry_run:
            return add(kind, name, "would-remove", reason)
        p = run(cmd, cwd_ or main_path)
        if p.returncode:
            add(kind, name, "error", (p.stderr or p.stdout).strip()[:300])
        else:
            add(kind, name, "removed", reason)

    try:
        keep = load_keep()
        me = run(["gh", "api", "user", "--jq", ".login"]).stdout.strip()
        f = run(["git", "fetch", "--prune", "origin"], main_path)
        if f.returncode:
            raise RuntimeError("git fetch: " + f.stderr.strip()[:200])
        cur = run(["git", "symbolic-ref", "--short", "-q", "HEAD"], main_path).stdout.strip()
        opened = gh_json(["pr", "list", "--state", "open", "--limit", "200", "--json", "headRefName,baseRefName"], main_path)
        open_heads = {p["headRefName"] for p in opened}
        open_bases = {p["baseRefName"] for p in opened}
        mine_merged = gh_json(["pr", "list", "--state", "merged", "--author", "@me", "--limit", "200",
                               "--json", "number,headRefName,headRefOid,baseRefName"], main_path)
    except Exception as e:  # without GitHub there is no way to decide anything
        add("repo", main_path, "error", str(e))
        return finish(items, a)

    merged_cache = {}

    def merged_pr(b):
        if b not in merged_cache:
            try:
                prs = gh_json(["pr", "list", "--head", b, "--state", "merged", "--json",
                               "number,baseRefName,headRefOid,author"], main_path)
                merged_cache[b] = pr_final(prs, b, flow_info)
            except Exception as e:
                merged_cache[b] = e
        return merged_cache[b]

    def facts(kind, b, where):
        pr = merged_pr(b)
        if isinstance(pr, Exception):
            raise pr
        fa = dict(branch=b, kind=kind, kept=is_kept(b, keep), merged=bool(pr), open_head=b in open_heads,
                  is_main_current=b == cur, dirty=False, ahead=False)
        if pr and kind != "remote":
            # worktree: HEAD inside it; local: the branch name from the main checkout
            fa["ahead"] = is_ahead(f"origin/{pr['baseRefName']}", "HEAD" if kind == "worktree" else b,
                                   where if kind == "worktree" else main_path, pr.get("headRefOid"))
            if kind == "worktree":
                blockers, fa["artefatos"] = dirt(where)
                fa["dirty"] = bool(blockers)
        return fa

    # 1. Orca worktrees
    try:
        wts = json.loads(run(["orca", "worktree", "list", "--repo", f"path:{main_path}", "--json"]).stdout)["result"]["worktrees"]
    except Exception as e:
        wts = []
        add("worktree", "(orca worktree list)", "error", str(e))
    busy = busy_worktrees()
    by_id = {w.get("id"): w for w in wts if w.get("id")}
    for w in wts:
        if w.get("isMainWorktree") or not w.get("branch"):
            continue
        b = w["branch"].removeprefix("refs/heads/")
        if a.branch and b != a.branch:
            continue
        try:
            fa = facts("worktree", b, w["path"])
            v, r = decide(fa)
            if not fa["merged"] and not fa["is_main_current"]:  # no merged PR: maybe orphaned (cherry-pick, research, stopped)
                proof = orq_proof(main_path, b)
                fa["integrated"] = proof and proof["via"]
                fa["ahead"] = False if proof else is_ahead("origin/main", "HEAD", w["path"])
                blockers, fa["artefatos"] = dirt(w["path"])
                fa["dirty"] = bool(blockers)
                fa["busy"] = None if busy is None else w["path"] in busy
                since = proof["since"] * 1000 if proof and proof["since"] else (w.get("lastActivityAt") or 0)  # the cycle's end, not the folder's activity (the hook itself and `orq agents` touch it)
                fa["recente"] = datetime.now(timezone.utc).timestamp() * 1000 - since < ORPHAN_IDLE_H * 3600 * 1000
                v, r = decide_orphan(fa)
                if v == "remove":
                    r += "; branch local apagada junto"
            child_rows = live_children(w, by_id)
            if v == "remove" and child_rows:
                names = ", ".join(c.get("displayName") or c.get("branch") or c["path"] for c in child_rows)
                v, r = "skip", f"live child worktree(s): {names}"
            stored = []
            if v == "remove" and fa.get("artefatos"):
                if a.dry_run:
                    stored = fa["artefatos"]
                else:
                    try:
                        stored = store(w["path"], fa["artefatos"])
                    except OSError as e:  # without the copy, removing would lose the report
                        v, r = "skip", f"could not save {', '.join(fa['artefatos'])}: {e}"
            if v == "remove" and not a.dry_run and fa.get("integrated") not in (None, False, "contida"):  # the tip is not in main: bundle first, then the branch is deleted with -D
                try:
                    from orqlib import PLAN, clean_bundle
                    if not clean_bundle(main_path, [b], os.path.join(PLAN, "backups"), time.time(), "main")[1]:
                        v, r = "skip", "backup bundle failed: nothing removed"
                except Exception as e:  # noqa: BLE001
                    v, r = "skip", f"no backup bundle ({e}): nothing removed"
            if v == "remove" and not a.dry_run:
                try:
                    from orqlib import terminate_worktree_processes
                    r = terminate_worktree_processes(w["path"])
                    if r and (r["nao_encerrados"] or r["recusado"]):
                        print(f"limpar-mergeados: {w['path']}: {r['nao_encerrados']} process(es) not terminated (not the worker's), {r['recusado']} refused over the limit; see the processes event", file=sys.stderr)
                except Exception as e:  # without orq the cleanup carries on; Orca removes the worktree the same way
                    print(f"limpar-mergeados: worktree processes not terminated: {e}", file=sys.stderr)
            act("worktree", b, v, r, ["orca", "worktree", "rm", "--worktree", f"path:{w['path']}", "--run-hooks"])
            parent_row = by_id.get(w.get("parentWorktreeId"))
            lineage = {"parent": (parent_row.get("displayName") or parent_row.get("branch") or parent_row.get("path")) if parent_row else None,
                       "children": [c.get("displayName") or c.get("branch") or c.get("path") for c in child_rows]}
            if lineage["parent"] or lineage["children"]:
                items[-1]["lineage"] = lineage
            if stored:
                items[-1]["guardados"] = stored
            if v == "remove" and not fa["merged"] and not a.dry_run and items[-1]["action"] == "removed":
                run(["git", "branch", "-D", b], main_path)  # the commit is already in main (cherry with no "+"), so -D loses no work
        except Exception as e:
            add("worktree", b, "error", str(e))

    # 2. local branches outside any worktree
    in_wt = {l.removeprefix("branch refs/heads/") for l in
             run(["git", "worktree", "list", "--porcelain"], main_path).stdout.splitlines() if l.startswith("branch ")}
    for b in run(["git", "for-each-ref", "--format=%(refname:short)", "refs/heads"], main_path).stdout.split():
        if a.branch and b != a.branch:
            continue
        if b in in_wt or b in PROTECTED:
            if b in in_wt and b not in PROTECTED:
                add("local", b, "skip", "branch in use by a worktree")
            continue
        try:
            v, r = decide(facts("local", b, b))
            act("local", b, v, r, ["git", "branch", "-D", b])
        except Exception as e:
            add("local", b, "error", str(e))

    # 3. remote branches of the user's PRs
    tips = {}
    for l in run(["git", "ls-remote", "--heads", "origin"], main_path).stdout.splitlines():
        sha, ref = l.split("\t")
        tips[ref.removeprefix("refs/heads/")] = sha
    seen = set()
    for pr in sorted(mine_merged, key=lambda p: -p["number"]):
        b = pr["headRefName"]
        if (a.branch and b != a.branch) or not is_final(pr, b, flow_info):
            continue
        if b in seen or b not in tips:
            continue
        seen.add(b)
        fa = dict(branch=b, kind="remote", kept=is_kept(b, keep), merged=True, open_head=b in open_heads,
                  open_base=b in open_bases, is_main_current=b == cur, mine=True, on_remote=True, tip_matches=tips[b] == pr["headRefOid"])
        v, r = decide(fa)
        act("remote", b, v, r, ["git", "push", "origin", "--delete", b])
    return finish(items, a)


def orq_event(task, branch, items):
    """Records in orq's log what the cleanup removed, kept and skipped. Orq missing or log inaccessible: carries on without the event."""
    try:
        from orqlib import append_event, cleaned_close
        ev = {"tipo": "pr", "op": "limpou", "task": task, "branch": branch,
                      "removidos": [f"{i['kind']}:{i['name']}" for i in items if i["action"] == "removed"],
                      "guardados": [g for i in items for g in i.get("guardados", [])],
                      "pulados": [f"{i['kind']}:{i['name']} ({i['reason']})" for i in items if i["action"] in ("skip", "error")]}
        append_event(ev)
        cleaned_close(ev)
    except Exception as e:  # noqa: BLE001 - the event is a record, it can't take the cleanup down
        print(f"event not recorded: {type(e).__name__}: {e}", file=sys.stderr)


def finish(items, a):
    summary = {"timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"), "dry_run": a.dry_run, "items": items}
    os.makedirs(os.path.dirname(LAST), exist_ok=True)
    with open(LAST, "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    if a.task:
        orq_event(a.task, a.branch, items)
    if a.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        for i in items:
            print(f"{i['action']:<12} {i['kind']:<9} {i['name']}  ({i['reason']})")
            if i.get("lineage", {}).get("parent"):
                print(f"  └─ parent: {i['lineage']['parent']}")
            for child in i.get("lineage", {}).get("children", []):
                print(f"  └─ child: {child}")
        if not items:
            print("nothing to do")


if __name__ == "__main__":
    main()
