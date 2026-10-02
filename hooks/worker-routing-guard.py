#!/usr/bin/env python3
"""PreToolUse: bloqueia despacho de worker sem modelo explícito (skill worker-routing)."""
import json
import re
import sys

REASON = (
    "Worker dispatch without an explicit model. Load the worker-routing skill, "
    "pick model and effort by the task's ambiguity and repeat the call with them{extra}."
)


def deny(extra):
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": REASON.format(extra=extra),
        }
    }))
    sys.exit(0)


try:
    event = json.load(sys.stdin)
except ValueError:
    sys.exit(0)  # entrada ruim: o hook não decide nada (o erro que não bloqueia só faz ruído)
if not isinstance(event, dict):
    sys.exit(0)
tool = event.get("tool_name")
params = event.get("tool_input") if isinstance(event.get("tool_input"), dict) else {}

if tool == "Bash":
    cmd = params.get("command") if isinstance(params.get("command"), str) else ""
    if re.search(r"orchestration\s+worker-start\b", cmd) and "--help" not in cmd:
        # --terminal reaproveita um terminal vivo, e o orca não aceita --model junto
        if "--terminal" not in cmd:
            missing = [f for f in ("--model", "--effort") if f not in cmd]
            if missing:
                deny(" (missing " + " and ".join(missing) + " in worker-start)")
    # o orq despachar chama o worker-start por dentro: sem --modelo e --effort ele nem sobe, mas a recusa vem aqui com o texto da skill
    # só em posição de comando (início, ou depois de ; & | ( , e então um python3 opcional): o mesmo texto dentro de aspas (um --body, um echo, um commit) não conta
    sem_aspas = re.sub(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'', '""', cmd)
    if re.search(r"(?:^|[;&|(]\s*)(?:python3?\s+)?(?:\S*/)?orq(?:\.py)?\s+(?:dispatch|despachar)\b", sem_aspas) and "--help" not in cmd:
        missing = [f for f in ("--model", "--effort") if f not in cmd]
        if missing:
            deny(" (missing " + " and ".join(missing) + " in orq dispatch)")
    # sem --run-hooks o archive do orca.yaml não roda, e o que ele devolve ao checkout principal (o .scratch, por exemplo) se perde
    if re.search(r"\borca\s+worktree\s+rm\b", cmd) and "--help" not in cmd and "--run-hooks" not in cmd:
        print(json.dumps({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": "orca worktree rm without --run-hooks skips the orca.yaml archive, and what it hands back to the main checkout (the .scratch, for example) is lost. Repeat with --run-hooks.",
            }
        }))
        sys.exit(0)
elif tool == "Agent":
    # fork herda o modelo do pai e ignora model
    if params.get("subagent_type") != "fork" and not params.get("model"):
        deny(" (pass model in the Agent call)")
    # subagente não aparece no Run do Orca sozinho; lembrete, sem bloquear
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": "A subagent from the Agent tool does not become a task in Orca. If this is a user task, register it in the stream's Run (task-create + task-update dispatched) and close it when it returns.",
        }
    }))

sys.exit(0)
