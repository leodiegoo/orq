#!/usr/bin/env python3
"""Integrates orq branches in a worktree of its own; the live main (~/.claude/orq) only advances by fast-forward, with green tests.

  integrar.py <branch>...     creates ORQ_WT_DIR/integra-<branches> from main, merges each branch, runs the tests and advances main
  integrar.py --avancar <wt>  after resolving a conflict (and committing) in the worktree: checks, runs the tests and advances main

After the fast-forward it calls `orq integrate conclude`, which closes what the cycle integrated (queue, ticket, worker, cycle). The push stays manual.

~/.claude/orq is both the repository and the installation: hooks, orq and the panel run what is there. A merge with an open conflict in it leaves
markers in orqlib.py and takes everything down. Here the conflict only exists in the worktree.
Variables: ORQ_WT_DIR (default: ../orq-wt next to the installation), ORQ_TESTES (default: the README tests)."""
import os
import re
import shlex
import subprocess
import sys

TESTS = "python3 test_orq.py && python3 test_precompact.py"


def git(cwd, *args):
    return subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True)


def die(msg):
    print(f"integrar: {msg}", file=sys.stderr)
    sys.exit(1)


def alive():
    """A worktree principal do repositório: a instalação que roda."""
    r = git(os.path.dirname(os.path.realpath(__file__)), "worktree", "list", "--porcelain")
    if r.returncode:
        die(r.stderr.strip())
    return r.stdout.split("\n", 1)[0].removeprefix("worktree ")


def branches_file(wt):
    """Onde a worktree guarda as branches que o ciclo integra (dentro do gitdir dela, para o `--avancar` achá-las depois de um conflito)."""
    return os.path.join(git(wt, "rev-parse", "--absolute-git-dir").stdout.strip(), "orq-branches")


def conclude(viva, wt):
    """Avisa o orq que a main andou. A main já avançou: falha aqui vira aviso, com o comando para repetir à mão."""
    try:
        branches = open(branches_file(wt)).read().split()
    except OSError:
        branches = []
    if not branches:
        return
    hash_ = git(viva, "rev-parse", "--short", "HEAD").stdout.strip()
    cmd = [sys.executable, os.path.join(viva, "orq.py"), "integrate", "conclude", "--hash", hash_, *branches]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode:
        print(f"integrar: main advanced, but orq did not close the cycle ({r.stderr.strip()}); run `{shlex.join(cmd)}`", file=sys.stderr)
    else:
        print(f"integrar: {r.stdout.strip()}")
        if r.stderr.strip():
            print(r.stderr.strip(), file=sys.stderr)


def advance(wt):
    viva = alive()
    branch = git(wt, "symbolic-ref", "--short", "HEAD").stdout.strip()
    if not branch.startswith("integra/"):
        die(f"{wt} is not an integration worktree (branch {branch or 'detached'})")
    if git(wt, "status", "--porcelain").stdout.strip():
        die(f"{wt} has a conflict or uncommitted changes: resolve, commit and run `integrar.py --avancar {wt}`")
    markers = git(wt, "grep", "-nE", "^(<<<<<<<|>>>>>>>) ", "--", ".").stdout.strip()
    if markers:
        die(f"leftover conflict marker:\n{markers}")
    tests = os.environ.get("ORQ_TESTES") or TESTS
    if subprocess.run(tests, shell=True, cwd=wt).returncode:
        die(f"tests failing in {wt}; main did not advance")
    ff = git(viva, "merge", "--ff-only", branch)
    if ff.returncode:
        die(f"the live main cannot fast-forward (it moved, or has local changes):\n{ff.stderr.strip()}\n"
               f"bring main into the worktree (`git -C {shlex.quote(wt)} merge main`), resolve there and run `integrar.py --avancar {wt}`")
    conclude(viva, wt)  # antes de remover a worktree: o arquivo das branches vive no gitdir dela
    git(viva, "worktree", "remove", "--force", wt)
    git(viva, "branch", "-d", branch)
    print(f"integrar: main at {git(viva, 'rev-parse', '--short', 'HEAD').stdout.strip()}, worktree removed")


def clean(viva):
    """Começo do ciclo: se o push da main já saiu, remove as worktrees do orq-wt cujas branches a origin/main contém. Falha vira aviso."""
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}  # o orq.py roda dentro da main viva: nenhum __pycache__ suja a árvore
    if os.environ.get("ORQ_WT_DIR"):
        env["ORQ_WT_ROOT"] = os.environ["ORQ_WT_DIR"]  # a pasta de worktrees que este ciclo usa é a que se limpa
    r = subprocess.run([sys.executable, os.path.join(viva, "orq.py"), "worktrees", "clean"], capture_output=True, text=True, env=env)
    print(f"integrar: {r.stdout.splitlines()[0] if r.stdout else r.stderr.strip()}")


def integrate(branches):
    viva = alive()
    clean(viva)
    slug = re.sub(r"[^\w.-]+", "-", "-".join(branches))[:60]
    wt = os.path.join(os.environ.get("ORQ_WT_DIR") or os.path.join(os.path.dirname(viva), "orq-wt"), f"integra-{slug}")
    if os.path.exists(wt):
        die(f"{wt} already exists: finish with `integrar.py --avancar {wt}` or remove it with `git worktree remove --force {wt}`")
    base = git(viva, "symbolic-ref", "--short", "HEAD").stdout.strip()
    r = git(viva, "worktree", "add", "-b", f"integra/{slug}", wt, base)
    if r.returncode:
        die(r.stderr.strip())
    with open(branches_file(wt), "w") as f:
        f.write("\n".join(branches))
    for b in branches:
        r = git(wt, "merge", "--no-edit", b)
        if r.returncode:
            die(f"conflict integrating {b} in {wt} (the live main is untouched):\n{git(wt, 'status', '--short').stdout.strip()}\n"
                   f"resolve there, commit and run `integrar.py --avancar {wt}`")
    advance(wt)


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--avancar":
        advance(args[1])
    elif args and not args[0].startswith("-"):
        integrate(args)
    else:
        die(__doc__)
