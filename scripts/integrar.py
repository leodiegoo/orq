#!/usr/bin/env python3
"""Integrates orq branches in a worktree of its own; the live main (the clone that runs) only advances by fast-forward, with green tests.

  integrar.py <branch>... [--no-proof REASON]
                              refuses a queued branch whose tip is not the commit its delivery proved (`orq integrate check`), unless --no-proof (alias --sem-prova) gives the reason;
                              creates ORQ_WT_DIR/integra-<branches> from main, merges each branch, runs the tests and advances main
  integrar.py --avancar <wt>  after resolving a conflict (and committing) in the worktree: checks, runs the tests and advances main

Before the tests it runs the night replay (test_noite_replay.py, ticket 216) and prints its time; a red replay leaves main where it was and writes a
`[PENDENTE` line to ciclos.log, which the manager turns into a notice to the coordinator (ticket 137).
After the fast-forward it calls `orq integrate conclude`, which closes what the cycle integrated (queue, ticket, worker, cycle). The push stays manual.

The live clone is both the repository and the installation: hooks, orq and the panel run what is there. A merge with an open conflict in it leaves
markers in orqlib.py and takes everything down. Here the conflict only exists in the worktree.
The full suite runs once per cycle, on the final tree, and writes the test map of `orq test --affected` (plan/test-map.json). After a conflict resolved
only in test_orq.py, README.md or docs/design.md, `--avancar` runs `orq test --affected` against the live main instead; the full run stays for the next cycle.
Variables: ORQ_WT_DIR (default: ORQ_WT, the .worktrees/ folder of the clone), ORQ_TESTES (default: the README tests), ORQ_TESTES_AFETADOS (the run after a light conflict),
ORQ_REPLAY (default: the night replay; off when ORQ_TESTES replaces the tests), ORQ_CICLOS_LOG (default: <ORQ_WT_DIR>/integracao/ciclos.log)."""
import os
import re
import shlex
import subprocess
import sys
import time

sys.dont_write_bytecode = True  # this runs inside the live main: no __pycache__ dirties the tree
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import orqpaths  # noqa: E402

TEST_MAP = os.environ.get("ORQ_TEST_MAP") or os.path.join(orqpaths.PLAN, "test-map.json")
TESTS = f"python3 test_orq.py --map {shlex.quote(TEST_MAP)} && python3 test_precompact.py"
LIGHT = {"test_orq.py", "README.md", "docs/design.md"}  # a conflict only in these re-runs the affected tests, not the whole suite (ticket 328)
REPLAY = "python3 test_noite_replay.py"


def git(cwd, *args):
    return subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True)


def die(msg):
    print(f"integrar: {msg}", file=sys.stderr)
    sys.exit(1)


def alive():
    """The repository's main worktree: the installation that runs."""
    r = git(os.path.dirname(os.path.realpath(__file__)), "worktree", "list", "--porcelain")
    if r.returncode:
        die(r.stderr.strip())
    return r.stdout.split("\n", 1)[0].removeprefix("worktree ")


def branches_file(wt):
    """Where the worktree keeps the branches the cycle integrates (inside its gitdir, so `--advance` can find them after a conflict)."""
    return os.path.join(git(wt, "rev-parse", "--absolute-git-dir").stdout.strip(), "orq-branches")


def conflicts_file(wt):
    """Where the worktree keeps the files of the conflict that stopped the cycle, for `--avancar`."""
    return os.path.join(git(wt, "rev-parse", "--absolute-git-dir").stdout.strip(), "orq-conflicts")


def tests_for(viva, wt):
    """The cycle's test command: the full suite, or the affected tests when the conflict that stopped it touched only LIGHT files."""
    try:
        conflicts = open(conflicts_file(wt)).read().split()
    except OSError:
        conflicts = []
    if conflicts and set(conflicts) <= LIGHT:
        print(f"integrar: the conflict was only in {', '.join(conflicts)}: running the affected tests; the full suite runs in the next cycle")
        main = git(viva, "rev-parse", "HEAD").stdout.strip()
        return os.environ.get("ORQ_TESTES_AFETADOS") or f"{shlex.quote(sys.executable)} orq.py test --affected --base {main}"
    return os.environ.get("ORQ_TESTES") or TESTS


def conclude(viva, wt):
    """Tells orq that main moved. Main has already advanced: a failure here becomes a notice, with the command to repeat by hand."""
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
    replay = os.environ.get("ORQ_REPLAY", "" if os.environ.get("ORQ_TESTES") else REPLAY)
    if replay:
        started = time.time()
        if subprocess.run(replay, shell=True, cwd=wt).returncode:
            log_path = os.environ.get("ORQ_CICLOS_LOG") or os.path.join(os.environ.get("ORQ_WT_DIR") or orqpaths.WT, "integracao", "ciclos.log")
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"[PENDENTE: main did not advance, night replay failed] {time.strftime('%Y-%m-%d %H:%M')} {branch} in {wt}\n")
            die(f"night replay failing in {wt} ({time.time() - started:.1f} s); main did not advance")
        print(f"integrar: night replay ok in {time.time() - started:.1f} s")
    if subprocess.run(tests_for(viva, wt), shell=True, cwd=wt).returncode:
        die(f"tests failing in {wt}; main did not advance")
    ff = git(viva, "merge", "--ff-only", branch)
    if ff.returncode:
        die(f"the live main cannot fast-forward (it moved, or has local changes):\n{ff.stderr.strip()}\n"
               f"bring main into the worktree (`git -C {shlex.quote(wt)} merge main`), resolve there and run `integrar.py --avancar {wt}`")
    conclude(viva, wt)  # before removing the worktree: the branches file lives in its gitdir
    git(viva, "worktree", "remove", "--force", wt)
    git(viva, "branch", "-d", branch)
    print(f"integrar: main at {git(viva, 'rev-parse', '--short', 'HEAD').stdout.strip()}, worktree removed")


def clean(viva):
    """Start of the cycle: if the main push already went out, removes the ORQ_WT worktrees whose branches origin/main contains. A failure becomes a notice."""
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}  # orq.py runs inside the live main: no __pycache__ dirties the tree
    if os.environ.get("ORQ_WT_DIR"):
        env["ORQ_WT"] = env["ORQ_WT_ROOT"] = os.environ["ORQ_WT_DIR"]  # the worktrees folder this cycle uses is the one that gets cleaned
    r = subprocess.run([sys.executable, os.path.join(viva, "orq.py"), "worktrees", "clean"], capture_output=True, text=True, env=env)
    print(f"integrar: {r.stdout.splitlines()[0] if r.stdout else r.stderr.strip()}")


def check_proof(viva, branches, no_proof):
    """A queued branch with commits after its proof, or rewritten, does not enter the cycle (ticket 222)."""
    cmd = [sys.executable, os.path.join(viva, "orq.py"), "integrate", "check", *branches, *(["--no-proof", no_proof] if no_proof else [])]
    r = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})  # orq.py runs inside the live main: no __pycache__ dirties the tree
    if r.returncode:
        die(r.stderr.strip() or r.stdout.strip())
    if r.stderr.strip():
        print(r.stderr.strip(), file=sys.stderr)


def integrate(branches, no_proof=None):
    viva = alive()
    check_proof(viva, branches, no_proof)
    clean(viva)
    slug = re.sub(r"[^\w.-]+", "-", "-".join(branches))[:60]
    wt = os.path.join(os.environ.get("ORQ_WT_DIR") or orqpaths.WT, f"integra-{slug}")
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
            with open(conflicts_file(wt), "w") as f:
                f.write(git(wt, "diff", "--name-only", "--diff-filter=U").stdout)
            die(f"conflict integrating {b} in {wt} (the live main is untouched):\n{git(wt, 'status', '--short').stdout.strip()}\n"
                   f"resolve there, commit and run `integrar.py --avancar {wt}`")
    advance(wt)


if __name__ == "__main__":
    args = sys.argv[1:]
    no_proof = None
    for flag in ("--no-proof", "--sem-prova"):
        if flag in args:
            i = args.index(flag)
            no_proof = "".join(args[i + 1:i + 2]) or die(f"{flag} needs a reason")
            del args[i:i + 2]
    if len(args) == 2 and args[0] == "--avancar":
        advance(args[1])
    elif args and not args[0].startswith("-"):
        integrate(args, no_proof)
    else:
        die(__doc__)
