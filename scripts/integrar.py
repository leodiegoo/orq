#!/usr/bin/env python3
"""Integra branches do orq numa worktree própria; a main viva (~/.claude/orq) só avança por fast-forward, com os testes verdes.

  integrar.py <branch>...     cria ORQ_WT_DIR/integra-<branches> a partir da main, faz o merge de cada branch, roda os testes e avança a main
  integrar.py --avancar <wt>  depois de resolver um conflito (e commitar) na worktree: confere, roda os testes e avança a main

O ~/.claude/orq é repositório e instalação ao mesmo tempo: hooks, orq e painel executam o que está lá. Um merge com conflito aberto nele deixa
marcadores no orqlib.py e derruba tudo. Aqui o conflito só existe na worktree.
Variáveis: ORQ_WT_DIR (padrão: ../orq-wt ao lado da instalação), ORQ_TESTES (padrão: os testes do README)."""
import os
import re
import shlex
import subprocess
import sys

TESTES = "python3 test_orq.py && python3 test_precompact.py"


def git(cwd, *args):
    return subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True)


def morrer(msg):
    print(f"integrar: {msg}", file=sys.stderr)
    sys.exit(1)


def vivo():
    """A worktree principal do repositório: a instalação que roda."""
    r = git(os.path.dirname(os.path.realpath(__file__)), "worktree", "list", "--porcelain")
    if r.returncode:
        morrer(r.stderr.strip())
    return r.stdout.split("\n", 1)[0].removeprefix("worktree ")


def avancar(wt):
    viva = vivo()
    ramo = git(wt, "symbolic-ref", "--short", "HEAD").stdout.strip()
    if not ramo.startswith("integra/"):
        morrer(f"{wt} não é uma worktree de integração (branch {ramo or 'solto'})")
    if git(wt, "status", "--porcelain").stdout.strip():
        morrer(f"{wt} tem conflito ou mudança sem commit: resolva, commite e rode `integrar.py --avancar {wt}`")
    marcadores = git(wt, "grep", "-nE", "^(<<<<<<<|>>>>>>>) ", "--", ".").stdout.strip()
    if marcadores:
        morrer(f"marcador de conflito esquecido:\n{marcadores}")
    testes = os.environ.get("ORQ_TESTES") or TESTES
    if subprocess.run(testes, shell=True, cwd=wt).returncode:
        morrer(f"testes vermelhos em {wt}; a main não andou")
    ff = git(viva, "merge", "--ff-only", ramo)
    if ff.returncode:
        morrer(f"a main viva não avança por fast-forward (ela andou, ou tem mudança local):\n{ff.stderr.strip()}\n"
               f"traga a main para a worktree (`git -C {shlex.quote(wt)} merge main`), resolva lá e rode `integrar.py --avancar {wt}`")
    git(viva, "worktree", "remove", "--force", wt)
    git(viva, "branch", "-d", ramo)
    print(f"integrar: main em {git(viva, 'rev-parse', '--short', 'HEAD').stdout.strip()}, worktree removida")


def integrar(branches):
    viva = vivo()
    slug = re.sub(r"[^\w.-]+", "-", "-".join(branches))[:60]
    wt = os.path.join(os.environ.get("ORQ_WT_DIR") or os.path.join(os.path.dirname(viva), "orq-wt"), f"integra-{slug}")
    if os.path.exists(wt):
        morrer(f"{wt} já existe: termine com `integrar.py --avancar {wt}` ou remova com `git worktree remove --force {wt}`")
    base = git(viva, "symbolic-ref", "--short", "HEAD").stdout.strip()
    r = git(viva, "worktree", "add", "-b", f"integra/{slug}", wt, base)
    if r.returncode:
        morrer(r.stderr.strip())
    for b in branches:
        r = git(wt, "merge", "--no-edit", b)
        if r.returncode:
            morrer(f"conflito ao integrar {b} em {wt} (a main viva está intacta):\n{git(wt, 'status', '--short').stdout.strip()}\n"
                   f"resolva lá, commite e rode `integrar.py --avancar {wt}`")
    avancar(wt)


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--avancar":
        avancar(args[1])
    elif args and not args[0].startswith("-"):
        integrar(args)
    else:
        morrer(__doc__)
