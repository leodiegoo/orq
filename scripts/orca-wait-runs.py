#!/usr/bin/env python3
"""Waits for an event across several Orca Runs, because the coordinator only consumes the mailbox of the Run it is attached to.

Usage: orca-wait-runs.py <run_id> [<run_id> ...]
Exits when a `dispatched` task of any Run changes status or when a message arrives in the attached Run (with the agent manager on, in any of its Runs). In the
other Runs of the list, it exits with whatever is not a heartbeat and has not yet been read in the inbox (question and escalation don't change the task status); consumes nothing.
Prints the Run and the tasks that changed; reading and acking requires `orca orchestration run-use --run <id>` first.

A worker heartbeat is confirmed without waking anyone, through the same routine as the `orq hook prompt` hook (`orq.confirm_batches`): Orca repeats the
current delivery when the same batch is confirmed twice, so hook and waiter together neither double-ack nor lose a message, and each
confirmed batch becomes a heartbeat_absorvido event on the panel. A batch that is not only heartbeat is printed and left open for the `check`.
"""
import contextlib
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))  # the clone of this script (~/.claude/scripts/ links here)
try:
    import orq
except Exception:  # noqa: BLE001 - without orq the waiter confirms the heartbeat on its own, without recording the event
    orq = None

POLL_S = float(os.environ.get("ORQ_WAIT_POLL") or 30)
MAX_S = float(os.environ.get("ORQ_WAIT_MAX") or 3600)
ORCA = os.environ.get("ORQ_ORCA") or "orca"  # orq's tests point to the fake Orca


def orca(*args):
    h = orq.handle_orca() if orq else None  # with the agent manager on, the Run is read through its terminal
    if orq and "--run" in args and not orq._is_manager_run(args[args.index("--run") + 1]):
        h = os.environ.get("ORCA_TERMINAL_HANDLE") or h  # Run that the coordinator holds outside the manager (B52)
    env = {**os.environ, "ORCA_TERMINAL_HANDLE": h} if h else None
    if orq and args[0] in orq.MUTA_RUN and "--run" in args and orq._is_manager_run(args[args.index("--run") + 1]):
        orq.orca("run-use", "--id", args[args.index("--run") + 1])  # the manager attaches one Run at a time; the caller holds the lock
    out = subprocess.run([ORCA, "orchestration", *args, "--json"], capture_output=True, text=True, timeout=30, env=env).stdout
    return json.loads(out or "{}")


def open_tasks(run):
    r = orca("task-list", "--run", run).get("result") or {}
    ts = r.get("tasks", r) if isinstance(r, dict) else r
    return {t["id"]: t["status"] for t in ts if t.get("status") not in ("completed",)}


runs = sys.argv[1:]
before = {run: open_tasks(run) for run in runs}
bound = (orca("run-current").get("result") or {}).get("run", {}).get("id")
watched = (orq.manager_runs() if orq else []) or ([bound] if bound else [])  # with the manager attached, each of its Runs' mailbox
watched += [r for r in runs if r not in watched and orq and orq.coordinator_run(r)]
start = time.time()
while time.time() - start < MAX_S:
    time.sleep(POLL_S)
    for bound in watched:
        with orq.manager_lock() if orq else contextlib.nullcontext():  # attach, read and confirm the Run in the same manager attachment
            res = orca("check", "--run", bound).get("result") or {}
            if orq and res.get("messages"):
                try:
                    res = orq.confirm_batches(bound, res, "waiter")[1]
                except Exception:  # noqa: BLE001 - a batch consumed and without ack is repeated on the next check: nothing is lost
                    res = {}
        msgs = res.get("messages") or []
        if not orq and msgs and all(m.get("type") == "heartbeat" for m in msgs):
            orca("check", "--run", bound, "--ack", res["deliveryId"])
            msgs = []
        elif orq and msgs and orq.only_heartbeats(msgs):  # heartbeat batch left over beyond the batch ceiling: stays for the next round
            msgs = []
        if msgs:
            print(json.dumps({"run": bound, "messages": msgs}, ensure_ascii=False))
            sys.exit(0)
    for run in runs:
        if run not in watched:  # Run that the coordinator doesn't read (the check gives consumer_fenced): question/escalation/worker_done only show up in the inbox
            msgs = [m for m in (orca("inbox", "--limit", "200").get("result") or {}).get("messages") or []
                    if isinstance(m, dict) and m.get("to_handle") == f"run:{run}" and not m.get("read") and m.get("type") != "heartbeat"]
            if msgs:  # nothing is consumed: the messages come out in one batch when the coordinator does run-use --id <run>
                print(json.dumps({"run": run, "messages": msgs}, ensure_ascii=False))
                sys.exit(0)
        now = open_tasks(run)
        changed = {t: s for t, s in before[run].items() if now.get(t, "completed") != s}
        if changed:
            print(json.dumps({"run": run, "changed": {t: now.get(t, "completed") for t in changed}}))
            sys.exit(0)
print(json.dumps({"timeout": True}))
