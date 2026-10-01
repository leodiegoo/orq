#!/usr/bin/env python3
"""UserPromptSubmit: prompt do usuário com a palavra "merged" dispara limpar-mergeados.py destacado, com 20 s de atraso.

Só no coordenador, com a mesma detecção do orq (Run ligado e terminal que não é de worker). Notificação, comando, resumo de sessão
e preâmbulo de despacho não disparam (origem do orq), nem mensagem que termina com "?". Sem confirmar o papel, não limpa."""
import json
import os
import re
import signal
import subprocess
import sys

sys.path.insert(0, os.path.expanduser("~/.claude/orq"))
try:
    from orq import coordenador, origem
except Exception as e:  # noqa: BLE001 - orqlib quebrado: o hook sai mudo (ver falha_segura.py)
    import falha_segura
    falha_segura.sair("limpar-mergeados-hook.py", e)

try:
    event = json.load(sys.stdin)
except ValueError:
    sys.exit(0)
if not isinstance(event, dict):
    sys.exit(0)  # entrada ruim: sem prompt, nada a limpar
cwd = event.get("cwd") if isinstance(event.get("cwd"), str) and event.get("cwd") else os.getcwd()
prompt = event.get("prompt") if isinstance(event.get("prompt"), str) else ""
if origem(prompt) != "usuario" or prompt.rstrip().endswith("?"):
    sys.exit(0)
if not re.search(r"\bmerged\b", prompt, re.I):
    sys.exit(0)
if subprocess.run(["git", "rev-parse", "--git-dir"], cwd=cwd, capture_output=True).returncode:
    sys.exit(0)

signal.signal(signal.SIGALRM, lambda *_: sys.exit(0))
signal.alarm(3)  # o mesmo teto dos hooks do orq
try:
    if coordenador(event) is None:
        sys.exit(0)
except Exception:  # noqa: BLE001 - sem confirmar que é o coordenador, não limpa
    sys.exit(0)
signal.alarm(0)

home = os.path.expanduser("~")
log = open(f"{home}/.claude/logs/limpar-mergeados.log", "a")
# atraso para o GitHub marcar o PR como mergeado
subprocess.Popen(["sh", "-c", f'sleep 20; exec python3 "{home}/.claude/scripts/limpar-mergeados.py" --repo "$0"', cwd],
                 cwd=cwd, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
print(json.dumps({"hookSpecificOutput": {
    "hookEventName": "UserPromptSubmit",
    "additionalContext": "Limpeza de branches mergeadas começou em segundo plano (em ~20 s); resumo em ~/.claude/logs/limpar-mergeados.last.json.",
}}))
