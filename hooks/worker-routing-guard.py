#!/usr/bin/env python3
"""PreToolUse: blocks a worker dispatch without an explicit model (worker-routing skill)."""
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
    sys.exit(0)  # bad input: the hook decides nothing (an error that does not block only makes noise)
if not isinstance(event, dict):
    sys.exit(0)
tool = event.get("tool_name")
params = event.get("tool_input") if isinstance(event.get("tool_input"), dict) else {}

if tool == "Bash":
    cmd = params.get("command") if isinstance(params.get("command"), str) else ""
    if re.search(r"orchestration\s+worker-start\b", cmd) and "--help" not in cmd:
        # --terminal reuses a live terminal, and orca does not accept --model together with it
        if "--terminal" not in cmd:
            missing = [f for f in ("--model", "--effort") if f not in cmd]
            if missing:
                deny(" (missing " + " and ".join(missing) + " in worker-start)")
    # orq despachar calls worker-start internally: without --modelo and --effort it does not even start, but the refusal comes from here with the skill's text
    # only in command position (start, or after ; & | ( , and then an optional python3): the same text inside quotes (a --body, an echo, a commit) does not count
    without_quotes = re.sub(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'', '""', cmd)
    if re.search(r"(?:^|[;&|(]\s*)(?:python3?\s+)?(?:\S*/)?orq(?:\.py)?\s+(?:dispatch|despachar)\b", without_quotes) and "--help" not in cmd:
        missing = [f for f in ("--model", "--effort") if f not in cmd]
        if missing:
            deny(" (missing " + " and ".join(missing) + " in orq dispatch)")
    # without --run-hooks the orca.yaml archive does not run, and what it returns to the main checkout (the .scratch, for example) is lost
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
    # fork inherits the parent's model and ignores model
    if params.get("subagent_type") != "fork" and not params.get("model"):
        deny(" (pass model in the Agent call)")
    # a subagent does not show up in the Orca Run by itself; reminder, without blocking
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": "A subagent from the Agent tool does not become a task in Orca. If this is a user task, register it in the stream's Run (task-create + task-update dispatched) and close it when it returns.",
        }
    }))

sys.exit(0)
