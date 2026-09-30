#!/usr/bin/env python3
"""Remove worktrees do Orca, branches locais e branches remotas (só de PRs do usuário) já mergeadas.

Uso: limpar-mergeados.py [--repo <caminho>] [--dry-run] [--json] [--self-test]
Sem --repo, usa a raiz git do cwd. Resumo do último run em ~/.claude/logs/limpar-mergeados.last.json.
Nunca usa --force; erro em um item vai para o resumo e não para os outros.
"""
import argparse
import fnmatch
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

HOME = os.path.expanduser("~")
KEEP_FILE = os.path.join(HOME, ".claude/scripts/limpar-mergeados.keep")
LAST = os.path.join(HOME, ".claude/logs/limpar-mergeados.last.json")
# Fluxo em que a mesma branch é promovida por PR para cada ambiente (development, staging, main): só o merge na base final a encerra.
PROTECTED = set((os.environ.get("ORQ_PROTECTED_BRANCHES") or "main,development,staging").split(","))
FINAL_BASE = os.environ.get("ORQ_FINAL_BASE") or "main"


def is_final(pr, branch):
    """O merge que encerra a branch: em main, ou o de uma merge/<feature>-<ambiente> no próprio ambiente."""
    return pr["baseRefName"] == FINAL_BASE or (branch.startswith("merge/") and pr["baseRefName"] in PROTECTED)


def pr_final(prs, branch):
    """O PR mergeado mais novo que encerra a branch, ou None."""
    finais = [p for p in prs if is_final(p, branch)]
    return max(finais, key=lambda p: p["number"]) if finais else None


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


def decide(f):
    """f: fatos de um item. Devolve (remove|skip, motivo). 'remote' exige autor e existência no remoto."""
    if f["branch"] in PROTECTED or f.get("is_main_current"):
        return "skip", "branch protegida"
    if f["kept"]:
        return "skip", "listada em limpar-mergeados.keep"
    if not f["merged"]:
        return "skip", "sem PR mergeado"
    if f["open_head"]:
        return "skip", "PR aberto usa o branch como head"
    if f["kind"] == "remote":
        if not f["mine"]:
            return "skip", "PR mergeado não é do usuário"
        if not f["on_remote"]:
            return "skip", "já não existe no remoto"
        if f["open_base"]:
            return "skip", "PR aberto usa o branch como base"
        if not f["tip_matches"]:
            return "skip", "remoto tem commit depois do merge"
        return "remove", "PR mergeado do usuário"
    if f["kind"] == "worktree" and f["dirty"]:
        return "skip", "worktree com alterações ou arquivos não rastreados"
    if f["ahead"]:
        return "skip", "commit fora da base do PR"
    return "remove", "PR mergeado e sem trabalho pendente"


def is_ahead(base, head, cwd, pr_head_oid=None):
    """True se `head` tem trabalho fora de `base`. Falha de git conta como ahead (na dúvida, não apaga).
    `head` é uma revisão (nunca o caminho da worktree). Squash-merge: HEAD == headRefOid do PR prova que
    tudo o que o branch tem passou pelo PR; qualquer commit a mais muda o oid e volta a contar como ahead."""
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
        assert is_ahead("origin/development", "HEAD", wt)  # commit ainda fora da base
        g("merge", "--no-ff", "-qm", "merge", "feat/x")  # merge commit, como um PR
        g("update-ref", "refs/remotes/origin/development", "HEAD")
        assert not is_ahead("origin/development", "HEAD", wt)  # regressão: cherry recebia o caminho da worktree
        assert is_ahead("origin/development", wt, d)  # caminho não é revisão: falha vira ahead
        open(f"{wt}/y", "w").write("y"); g("add", "y", cwd=wt); g("commit", "-qm", "y", cwd=wt)
        assert is_ahead("origin/development", "HEAD", wt)  # commit novo depois do merge
        # squash: patch-id não casa; só HEAD == headRefOid do PR prova que nada ficou de fora
        tip = g("rev-parse", "feat/x")
        prs = [{"number": 5, "baseRefName": "development"}, {"number": 3, "baseRefName": "main"}, {"number": 4, "baseRefName": "main"}]
        assert pr_final(prs, "feat/x")["number"] == 4 and pr_final(prs[:1], "feat/x") is None  # development não encerra; o maior em main vence
        assert pr_final(prs[:1], "merge/feat-development")["number"] == 5 and pr_final(prs[:1], "feat/merge/x") is None
        assert not is_ahead("origin/nope", "feat/x", d, tip)  # HEAD == headRefOid do PR
        assert is_ahead("origin/nope", "feat/x", d, "0" * 40)
        g("worktree", "remove", "--force", wt)


def self_test():
    self_test_git()
    assert is_kept("prototype/2039-x", ["prototype/*"]) and not is_kept("feat/x", ["prototype/*"])
    assert is_kept("main_bkp_1", ["main_bkp_*"]) and is_kept("feat/plataform-metrics", ["feat/plataform-metrics"])
    base = dict(branch="feat/a", kind="worktree", kept=False, merged=True, open_head=False, open_base=False,
                dirty=False, ahead=False, mine=True, on_remote=True, tip_matches=True)
    assert decide(base)[0] == "remove"
    for k, v in dict(kept=True, merged=False, open_head=True, dirty=True, ahead=True, branch="development").items():
        assert decide({**base, k: v})[0] == "skip", k
    assert decide({**base, "kind": "local", "dirty": True})[0] == "remove"  # sujeira só conta em worktree
    assert decide({**base, "is_main_current": True})[0] == "skip"
    r = {**base, "kind": "remote"}
    assert decide({**r, "dirty": True, "ahead": True})[0] == "remove"  # remoto ignora estado local
    for k, v in dict(mine=False, on_remote=False, open_base=True, tip_matches=False).items():
        assert decide({**r, k: v})[0] == "skip", k
    print("self-test ok")


def gh_json(args, cwd):
    p = run(["gh", *args], cwd)
    if p.returncode:
        raise RuntimeError(f"gh {' '.join(args[:3])}: {p.stderr.strip()[:200]}")
    return json.loads(p.stdout or "[]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()

    cwd = a.repo or os.getcwd()
    top = run(["git", "rev-parse", "--show-toplevel"], cwd)
    if top.returncode:
        sys.exit(f"não é repositório git: {cwd}")
    cwd = top.stdout.strip()
    # o cwd pode ser uma worktree; o checkout principal é o primeiro de `git worktree list`
    main_path = run(["git", "worktree", "list", "--porcelain"], cwd).stdout.split("\n")[0].removeprefix("worktree ")
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
    except Exception as e:  # sem GitHub não há como decidir nada
        add("repo", main_path, "error", str(e))
        return finish(items, a)

    merged_cache = {}

    def merged_pr(b):
        if b not in merged_cache:
            try:
                prs = gh_json(["pr", "list", "--head", b, "--state", "merged", "--json",
                               "number,baseRefName,headRefOid,author"], main_path)
                merged_cache[b] = pr_final(prs, b)
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
            # worktree: HEAD dentro dela; local: o nome do branch a partir do checkout principal
            fa["ahead"] = is_ahead(f"origin/{pr['baseRefName']}", "HEAD" if kind == "worktree" else b,
                                   where if kind == "worktree" else main_path, pr.get("headRefOid"))
            if kind == "worktree":
                fa["dirty"] = bool(run(["git", "status", "--porcelain"], where).stdout.strip())
        return fa

    # 1. worktrees do Orca
    try:
        wts = json.loads(run(["orca", "worktree", "list", "--repo", f"path:{main_path}", "--json"]).stdout)["result"]["worktrees"]
    except Exception as e:
        wts = []
        add("worktree", "(orca worktree list)", "error", str(e))
    for w in wts:
        if w.get("isMainWorktree") or not w.get("branch"):
            continue
        b = w["branch"].removeprefix("refs/heads/")
        try:
            v, r = decide(facts("worktree", b, w["path"]))
            act("worktree", b, v, r, ["orca", "worktree", "rm", "--worktree", f"path:{w['path']}", "--run-hooks"])
        except Exception as e:
            add("worktree", b, "error", str(e))

    # 2. branches locais fora de qualquer worktree
    in_wt = {l.removeprefix("branch refs/heads/") for l in
             run(["git", "worktree", "list", "--porcelain"], main_path).stdout.splitlines() if l.startswith("branch ")}
    for b in run(["git", "for-each-ref", "--format=%(refname:short)", "refs/heads"], main_path).stdout.split():
        if b in in_wt or b in PROTECTED:
            if b in in_wt and b not in PROTECTED:
                add("local", b, "skip", "branch em uso por uma worktree")
            continue
        try:
            v, r = decide(facts("local", b, b))
            act("local", b, v, r, ["git", "branch", "-D", b])
        except Exception as e:
            add("local", b, "error", str(e))

    # 3. branches remotas de PRs do usuário
    tips = {}
    for l in run(["git", "ls-remote", "--heads", "origin"], main_path).stdout.splitlines():
        sha, ref = l.split("\t")
        tips[ref.removeprefix("refs/heads/")] = sha
    seen = set()
    for pr in sorted(mine_merged, key=lambda p: -p["number"]):
        b = pr["headRefName"]
        if not is_final(pr, b):
            continue
        if b in seen or b not in tips:
            continue
        seen.add(b)
        fa = dict(branch=b, kind="remote", kept=is_kept(b, keep), merged=True, open_head=b in open_heads,
                  open_base=b in open_bases, is_main_current=b == cur, mine=True, on_remote=True, tip_matches=tips[b] == pr["headRefOid"])
        v, r = decide(fa)
        act("remote", b, v, r, ["git", "push", "origin", "--delete", b])
    return finish(items, a)


def finish(items, a):
    summary = {"timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"), "dry_run": a.dry_run, "items": items}
    os.makedirs(os.path.dirname(LAST), exist_ok=True)
    with open(LAST, "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    if a.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        for i in items:
            print(f"{i['action']:<12} {i['kind']:<9} {i['name']}  ({i['reason']})")
        if not items:
            print("nada a fazer")


if __name__ == "__main__":
    main()
