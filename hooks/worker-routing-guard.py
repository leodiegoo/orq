#!/usr/bin/env python3
"""PreToolUse: bloqueia despacho de worker sem modelo explícito (skill worker-routing)."""
import json
import re
import sys

REASON = (
    "Despacho de worker sem modelo explícito. Carregue a skill worker-routing, "
    "escolha modelo e effort pela ambiguidade da tarefa e repita a chamada com eles{extra}."
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
                deny(" (faltou " + " e ".join(missing) + " no worker-start)")
    # o orq despachar chama o worker-start por dentro: sem --modelo e --effort ele nem sobe, mas a recusa vem aqui com o texto da skill
    # só em posição de comando (início, ou depois de ; & | ( , e então um python3 opcional): o mesmo texto dentro de aspas (um --body, um echo, um commit) não conta
    sem_aspas = re.sub(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'', '""', cmd)
    if re.search(r"(?:^|[;&|(]\s*)(?:python3?\s+)?(?:\S*/)?orq(?:\.py)?\s+despachar\b", sem_aspas) and "--help" not in cmd:
        missing = [f for f in ("--modelo", "--effort") if f not in cmd]
        if missing:
            deny(" (faltou " + " e ".join(missing) + " no orq despachar)")
    # sem --run-hooks o archive do orca.yaml não roda, e o que ele devolve ao checkout principal (o .scratch, por exemplo) se perde
    if re.search(r"\borca\s+worktree\s+rm\b", cmd) and "--help" not in cmd and "--run-hooks" not in cmd:
        print(json.dumps({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": "orca worktree rm sem --run-hooks pula o archive do orca.yaml, e o que ele devolve ao checkout principal (o .scratch, por exemplo) se perde. Repita com --run-hooks.",
            }
        }))
        sys.exit(0)
elif tool == "Agent":
    # fork herda o modelo do pai e ignora model
    if params.get("subagent_type") != "fork" and not params.get("model"):
        deny(" (passe model no Agent)")
    # subagente não aparece no Run do Orca sozinho; lembrete, sem bloquear
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": "Subagente do Agent tool não vira task no Orca. Se esta é uma tarefa do usuário, registre no Run da frente (task-create + task-update dispatched) e feche quando ele voltar.",
        }
    }))

sys.exit(0)
