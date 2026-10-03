#!/usr/bin/env python3
"""Deterministic snapshot of the coordinator at PreCompact and resume at SessionStart(compact).

  precompact.py            (stdin: PreCompact JSON) writes handoff/<date>.md, updates ultimo.md and saves to engram
  precompact.py retomar    (stdin: SessionStart JSON) prints ultimo.md as additionalContext if source == compact
  precompact.py passagem --de H --para H   (`orq handoff coordinator`) writes the same snapshot without compacting and handoff/passagem.json (who wrote it and when);
                           the other harness's `orq hook session` injects it (ticket 93). Prints the record as JSON; with no Run attached, exits 1 with the cause on stderr
Coordinator only (orq.py's role rule, imported without changing it). Fail-open: any error becomes exit 0 and a line in orq.log.
Variables for testing: ORQ_HOME, ORQ_ORCA, ORQ_PENDENCIAS (orq.py's), ORQ_CLI, ORQ_ENGRAM, ORQ_GH, ORQ_DESENHO.
"""
import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import orq  # noqa: E402
except Exception as e:  # noqa: BLE001 - orqlib quebrado: o hook sai mudo (ver falha_segura.py)
    import fail_safe
    fail_safe.bail_out("precompact.py", e)

CAP_S = 20  # PreCompact: the hook has the settings timeout (30 s); the script stops collecting at 20 s
RESUME_LINES = 60  # the build respects per-section budgets that fit in here (M12); past that, the resume notice says it cut
OLD_S = 15 * 60  # ultimo.md older than this is not from the compact that just happened (B33)
DESIGN_PATH = os.environ.get("ORQ_DESENHO") or os.path.join(orq.PLAN, "desenho.md")
CLI = shlex.split(os.environ.get("ORQ_CLI") or f"python3 {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'orq.py')}")
ENGRAM = os.environ.get("ORQ_ENGRAM") or "engram"
GH = os.environ.get("ORQ_GH") or "gh"
WAITER = os.path.expanduser("~/.claude/scripts/orca-wait-runs.py")
HANDOFF = os.path.join(orq.HOME, "handoff")
MAX_ALIVE, MAX_DELIVERED, MAX_PENDING, MAX_LIST = 5, 4, 5, 4  # line budget per section (M12)


def run_it(cmd, timeout=5, cwd=None):
    """Output of a command, or '' if it failed: a section that fails doesn't take down the others. Whatever goes past the hook's CAP_S ceiling is left unrun (B33)."""
    remaining_pids = CAP_S - (time.monotonic() - T0)
    if remaining_pids <= 0.5:
        orq.log(f"precompact: {cmd[0]} skipped, {CAP_S} s budget exhausted")
        return ""
    timeout = min(timeout, remaining_pids)
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        return p.stdout.strip() if p.returncode == 0 else ""
    except Exception as e:
        orq.log(f"precompact: {cmd[0]} failed: {e}")
        return ""


def agents_section():
    txt = run_it(CLI + ["agents", "--json"])
    try:
        agent_rows = json.loads(txt)
    except ValueError:
        return "(orq agents unavailable)"
    live = [a for a in agent_rows if a.get("estado") not in ("liberado", "entregue")]
    delivered_items = [a for a in agent_rows if a.get("estado") == "entregue" and not a.get("retido")]
    line_list = [f"- {a.get('estado')} {a.get('task')} {a.get('titulo')} ({a.get('modelo')}, {a.get('terminal')}, phase: {a.get('fase') or '-'})"
              for a in live[:MAX_ALIVE]] or ["No agent running, stuck or asking."]
    if len(live) > MAX_ALIVE:
        line_list.append(f"+{len(live) - MAX_ALIVE} (orq agents)")
    if live:
        runs = sorted({a["run"] for a in live if a.get("run")})
        line_list.append(f"Waiter: `python3 {WAITER} {' '.join(runs)}`")
    line_list += [f"- delivered {a.get('task')} {a.get('titulo')}: orq release {a.get('dispatch')}" for a in delivered_items[:MAX_DELIVERED]]  # B32
    if len(delivered_items) > MAX_DELIVERED:
        line_list.append(f"+{len(delivered_items) - MAX_DELIVERED} delivered (orq agents)")
    return "\n".join(line_list)


def pending_section():
    try:
        item_list = orq._pending_ro()["itens"]  # pendencias.json, or the backlog with ORQ_BACKLOG
    except Exception:
        return "(pendencias.json unreadable)"
    if not item_list:
        return "None."
    line_list = [f"- {i.get('id')} [{i.get('tipo')}] {i.get('titulo')} (since {i.get('desde') or '?'}, waiting: {i.get('espera') or '-'})" for i in item_list[:MAX_PENDING]]
    return "\n".join(line_list + ([f"+{len(item_list) - MAX_PENDING} (orq pend)"] if len(item_list) > MAX_PENDING else []))


def cut(txt, n=MAX_LIST):
    """The first n lines of txt and '+N' with what was left out."""
    ls = txt.splitlines()
    return "\n".join(ls[:n] + ([f"+{len(ls) - n}"] if len(ls) > n else []))


def prs_section(cwd):
    if not run_it(["git", "rev-parse", "--git-dir"], cwd=cwd):
        return "(cwd outside a git repo)"
    txt = run_it([GH, "pr", "list", "--author", "@me", "--state", "open", "--json", "number,title,url"], timeout=8, cwd=cwd)
    try:
        prs = json.loads(txt)
    except ValueError:
        return "(gh unavailable)"
    return cut("\n".join(f"- #{p['number']} {p['title']} {p['url']}" for p in prs)) or "None."


def entries_section():
    try:
        ev = orq.read_events()
    except Exception:
        return "(events.jsonl unreadable)"
    effect = {e.get("entrada"): e for e in ev if e.get("tipo") == "intake"}
    us = [e for e in ev if e.get("tipo") == "entrada" and e.get("origem") == "usuario"][-10:]
    out = []
    for e in us:
        i = effect.get(e.get("id")) or {}
        out.append(f"- {e.get('id')} {e.get('ts')}: {(e.get('texto') or '')[:120]!r} -> {i.get('efeito') or 'no effect'}" + (f" {i['ref']}" if i.get("ref") else ""))
    return "\n".join(out) or "None."


def build(run, cwd):
    """The snapshot in markdown; each section is independent."""
    # what matters most comes first: the resume cuts the end (M12). No "orq status": `orq hook session` injects a newer one on compact (B29)
    sections = [
        ("Bound Run", run["id"]),
        *([("Away mandate", "\n".join(orq.away_readback(orq._cursor_ro())))] if orq.away_enabled() else []),
        ("Last 10 user entries", entries_section()),
        ("Map", DESIGN_PATH),
        ("Agents", agents_section()),
        ("User pending items", pending_section()),
        ("Open tickets", cut(run_it(CLI + ["ticket", "list"])) or "None (or orq unavailable)."),
        ("User's open PRs", prs_section(cwd)),
    ]
    return "\n\n".join(f"## {t}\n{c}" for t, c in sections)


def write_out(md, now_at):
    os.makedirs(HANDOFF, exist_ok=True)
    item_name = os.path.join(HANDOFF, now_at.strftime("%Y-%m-%dT%H-%M") + ".md")
    tmp = item_name + ".tmp"
    open(tmp, "w").write(f"# Handoff {now_at:%Y-%m-%d %H:%M}\n\n{md}\n")
    os.replace(tmp, item_name)
    last_item = os.path.join(HANDOFF, "ultimo.md")
    if os.path.lexists(last_item):
        os.remove(last_item)
    os.symlink(os.path.basename(item_name), last_item)
    return item_name


def engram(md, now_at, cwd):
    project = os.path.basename(run_it(["git", "rev-parse", "--show-toplevel"], cwd=cwd) or cwd)
    summary = "\n".join(md.splitlines()[:60])[:3500]
    run_it([ENGRAM, "save", f"Handoff {now_at:%Y-%m-%d %H:%M}", summary, "--project", project, "--type", "decision", "--topic", "sessao/handoff"], timeout=10)


def precompact(ev):
    sid = ev.get("session_id") or ""
    run = orq.coordinator({"session_id": sid})
    if not run:
        return
    cwd = ev.get("cwd") or os.getcwd()
    now_at = datetime.now()
    md = build(run, cwd)
    if time.monotonic() - T0 > CAP_S:
        orq.log("precompact: over the limit; snapshot written anyway")
    write_out(md, now_at)
    engram(md, now_at, cwd)


def resume(ev):
    if ev.get("source") != "compact" or not orq.coordinator({"session_id": ev.get("session_id") or ""}):
        return
    try:
        file_path = os.path.join(HANDOFF, "ultimo.md")
        all_listing, age = open(file_path).read().splitlines(), time.time() - os.path.getmtime(file_path)
    except OSError:
        return
    line_list = all_listing[:RESUME_LINES] + ([f"(… {len(all_listing) - RESUME_LINES} lines cut; read handoff/ultimo.md)"] if len(all_listing) > RESUME_LINES else [])
    title = ("Handoff saved before the compact (handoff/ultimo.md):" if age < OLD_S else
              f"STALE handoff from {datetime.fromtimestamp(os.path.getmtime(file_path)):%Y-%m-%d %H:%M}: PreCompact did not write a new one (handoff/ultimo.md):")  # B33
    ctx = title + "\n" + "\n".join(line_list)
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": ctx}}))


def handoff(from_, to_):
    """The PreCompact snapshot on demand, for the coordinator switching harness. The record (de, para, ts, Run) is what the other side's `orq hook session`
    checks: it only injects if `ts` is less than 15 min old and `from_` is not its own harness. No Engram: the server is the same in both harnesses."""
    run = orq.orca("run-current")["run"]
    if not run:
        print("orq: no Run bound to this terminal: there is no coordinator state to hand off (`orca orchestration run-use --id <run>`)", file=sys.stderr)
        return 1
    now_at = datetime.now()
    item_name = write_out(build(run, os.getcwd()), now_at)
    reg = {"arquivo": os.path.basename(item_name), "de": from_, "para": to_, "ts": time.time(), "run": run["id"], "aceita": None}
    orq._write_json(os.path.join(HANDOFF, "passagem.json"), reg)
    print(json.dumps(reg, ensure_ascii=False))
    return 0


def main(argv):
    global T0
    T0 = time.monotonic()
    if argv[1:2] == ["passagem"]:
        ap = argparse.ArgumentParser(prog="precompact.py passagem")
        ap.add_argument("--de", required=True, dest="from_")
        ap.add_argument("--para", required=True, dest="to_")
        a = ap.parse_args(argv[2:])
        try:
            return handoff(a.from_, a.to_)
        except Exception as e:  # noqa: BLE001 - command, not hook: the cause goes to stderr instead of vanishing in the log
            print(f"orq: coordinator handoff failed: {type(e).__name__}: {e}", file=sys.stderr)
            return 1
    try:
        raw = sys.stdin.read()
        ev = json.loads(raw) if raw.strip() else {}
        (resume if argv[1:2] == ["retomar"] else precompact)(ev)
    except Exception as e:
        orq.log(f"precompact: {type(e).__name__}: {e}")
    return 0


T0 = time.monotonic()
if __name__ == "__main__":
    sys.exit(main(sys.argv))
