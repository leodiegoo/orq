#!/usr/bin/env python3
"""UserPromptSubmit: a user prompt with the word "merged" triggers limpar-mergeados.py, detached, with a 20 s delay.

Only in the coordinator, with the same detection as orq (Run attached and a terminal that is not a worker's). Notification, command, session summary
and dispatch preamble do not trigger it (orq origin), nor does a message that ends with "?". Without confirming the role, it does not clean."""
import json
import os
import re
import signal
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))  # the clone of this hook (~/.claude/hooks/ links here)
try:
    from orq import coordinator, origin_name
except Exception as e:  # noqa: BLE001 - orqlib quebrado: o hook sai mudo (ver falha_segura.py)
    import fail_safe
    fail_safe.bail_out("limpar-mergeados-hook.py", e)

try:
    event = json.load(sys.stdin)
except ValueError:
    sys.exit(0)
if not isinstance(event, dict):
    sys.exit(0)  # bad input: no prompt, nothing to clean
cwd = event.get("cwd") if isinstance(event.get("cwd"), str) and event.get("cwd") else os.getcwd()
prompt = event.get("prompt") if isinstance(event.get("prompt"), str) else ""
if origin_name(prompt) != "usuario" or prompt.rstrip().endswith("?"):
    sys.exit(0)
if not re.search(r"\bmerged\b", prompt, re.I):
    sys.exit(0)
if subprocess.run(["git", "rev-parse", "--git-dir"], cwd=cwd, capture_output=True).returncode:
    sys.exit(0)

signal.signal(signal.SIGALRM, lambda *_: sys.exit(0))
signal.alarm(3)  # the same ceiling as the orq hooks
try:
    if coordinator(event) is None:
        sys.exit(0)
except Exception:  # noqa: BLE001 - without confirming it is the coordinator, it does not clean
    sys.exit(0)
signal.alarm(0)

home = os.path.expanduser("~")
log = open(f"{home}/.claude/logs/limpar-mergeados.log", "a")
# delay for GitHub to mark the PR as merged
subprocess.Popen(["sh", "-c", f'sleep 20; exec python3 "{home}/.claude/scripts/limpar-mergeados.py" --repo "$0"', cwd],
                 cwd=cwd, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
print(json.dumps({"hookSpecificOutput": {
    "hookEventName": "UserPromptSubmit",
    "additionalContext": "Cleanup of merged branches started in the background (in ~20 s); summary in ~/.claude/logs/limpar-mergeados.last.json.",
}}))
