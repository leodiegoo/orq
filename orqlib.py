#!/usr/bin/env python3
"""orq: orchestrator entry registry (slices 1, 3, 4, 6, 7, 8 and 9, and the review fixes). stdlib only. Design: docs/design.md

ORQ_PENDENCIAS replaces pendencias.json, ORQ_HOME replaces the data directory, ORQ_ORCA the Orca binary, ORQ_LOG the log, ORQ_TRANSCRITOS the coordinator transcripts folder, ORQ_PROJETOS the Claude Code `projects` folder (worker transcripts), ORQ_NO_BG=1 turns off the background refresh, ORQ_ISSUES the tickets folder and ORQ_MAPA the map (desenho.md).
"""
import argparse
import collections
import contextlib
import difflib
import fcntl
import glob
import hashlib
import importlib.util
import html
import json
import os
import pathlib
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timedelta, timezone

import backlog
import cmdnorm
import fail_safe
import orqpaths


def ThreadPoolExecutor(n):  # late import: concurrent.futures costs ~11 ms and only the panel and the ingest use it (ticket 49)
    from concurrent.futures import ThreadPoolExecutor as _T
    return _T(n)

HOME = orqpaths.HOME  # the clone (ORQ_HOME overrides); the plan and the integration worktrees live in it too (ticket 124)
PLAN = orqpaths.PLAN  # tickets, specs, reports and the design map (ORQ_PLAN)
WT_ROOT = orqpaths.WT  # the orq ticket worktrees and the integrator's (ORQ_WT)
ORCA = os.environ.get("ORQ_ORCA") or "orca"
GH = os.environ.get("ORQ_GH") or "gh"
GIT = os.environ.get("ORQ_GIT") or "git"
CLEAN_SCRIPT = os.environ.get("ORQ_LIMPAR") or os.path.join(orqpaths.HERE, "scripts", "limpar-mergeados.py")
CLEAN_DELAY_S = float(os.environ.get("ORQ_LIMPAR_ATRASO_S") or 20)  # the same delay as the "merged" hook
CLOSED_DAYS = float(os.environ.get("ORQ_FECHADO_DIAS") or 1)  # days between the task's last PR closed without merge and the automatic branch cleanup
REPORTS = os.environ.get("ORQ_RELATORIOS") or os.path.join(PLAN, "relatorios")
FINAL_BASE = os.environ.get("ORQ_FINAL_BASE")  # forces the base that closes out the branch; without it, the project's production applies (same as limpar-mergeados.py)
LAVISH = os.environ.get("ORQ_LAVISH") or "lavish-axi"
ASK_MIN = float(os.environ.get("ORQ_PERGUNTAR_MIN") or 30)  # how long `orq ask` waits for the answer before leaving the pending item open
LOG = os.environ.get("ORQ_LOG") or os.path.expanduser("~/.claude/logs/orq.log")
PENDING = os.environ.get("ORQ_PENDENCIAS") or os.path.expanduser("~/.claude/dashboard/data/pendencias.json")


def _machine_backlog():
    """The first line of ORQ_HOME/backlog.path: the backlog the machine binds for every process (ticket 167), already-open sessions included."""
    try:
        with open(os.path.join(HOME, "backlog.path"), encoding="utf-8") as f:
            return f.readline().strip() or None
    except OSError:
        return None


def _configured_backlog():
    """The bound backlog.md: the environment's `ORQ_BACKLOG` (empty turns it off) or, without the variable, the machine's."""
    if "ORQ_BACKLOG" in os.environ:
        return os.environ["ORQ_BACKLOG"] or None
    return _machine_backlog()


BACKLOG = _configured_backlog()  # tasks-axi's backlog.md (ticket 101): with it the pending items live there and pendencias.json becomes just the mirror the panel reads
BACKLOG_TICKETS = os.environ["ORQ_BACKLOG_TICKETS"] if "ORQ_BACKLOG_TICKETS" in os.environ else (os.path.exists(os.path.join(HOME, "backlog.tickets")) or None)  # tickets live in the backlog (M5); the file is the machine's switch, like backlog.path, and an empty variable turns it off
EFFECTS = ("tarefa", "steer", "pend", "decisao", "conversa", "descartado", "mate", "lembrete")
HOOK_TIMEOUT = int(os.environ.get("ORQ_HOOK_TIMEOUT") or 3)  # seconds of alarm per hook; the tests shorten it
WARN_KINDS = ("session", "prompt", "stop")  # the coordinator hooks whose failure warns the user (ticket 228); the per-tool ones fail in silence, as the log
MSG_NO_CONTEXT = "[orq] orq context did not load: run `orq status`"
PENDING_TYPES = ("acao", "decisao", "avisar")
PENDING_AGE_DAYS = 14  # a pending item untouched for more than 14 days leaves the view and goes to "Depois" (Later)
ID_HEADER = 12  # the AskUserQuestion header accepts up to 12 characters
SUSPECT_S = 3  # answer less than 3 s after a notification seen by the prompt hook
ORCA_WINDOW_S = 5  # message delivered by Orca within ±5 s of the answer (Orca inbox)
AUDIT_WINDOW_S = 2  # orq auditar-respostas: delivery within 2 s of the answer, in the transcript
TRANSCRIPTS = os.environ.get("ORQ_TRANSCRITOS")  # forces the coordinator's transcripts folder; without it they come from the projects (`transcript_dirs`)
PROJECTS = os.environ.get("ORQ_PROJETOS") or os.path.expanduser("~/.claude/projects")  # where Claude Code stores each session's transcript, including the worker's (M9)
TRANSCRIPT_DAYS = 3  # orq liberar only looks for the worker's transcript among the files touched in the last 3 days
INITIAL_READ = 200_000  # bytes from the start of the transcript where the dispatch prompt is
MAX_ATTEMPTS = 3  # transient ingest failure: the message/run is tried up to 3 times before being discarded
START = "2026-09-29T15:00:00Z"  # starting point of the ingest's first run: nothing earlier becomes an entry
MAX_ITEMS = 20  # action items per report; the rest becomes a single "ler" (read)
ALERT_H = 24  # the scout alert stays in the summary for 24 h
ASK_GUARD_TTL = 10  # seconds the active-dispatch list is valid for the guard hook (Orca takes ~130 ms per page)
ACTIVE_PAGES = 3  # worker-list comes from newest to oldest: 300 dispatches are enough to find an active one
NO_BINDING = "term_00000000-0000-0000-0000-000000000000"  # handle Orca does not know: with no Run attached, worker-list gives the all scope
STUCK_S = 15 * 60  # a running dispatch with no heartbeat for longer than this is stuck: it shows up in the summary and the panel with the suggested orq steer
WAIT_CAP_S = 60 * 60  # heartbeat `esperando: <reason>` without `até HH:MM` is valid for this long; after that the dispatch is stuck due to "espera vencida" (expired wait)
SCREEN_CAP_S = 45 * 60  # shell/monitor running on screen with no heartbeat for this long: stops being a wait and becomes stuck ("shell sem heartbeat", shell without heartbeat)
SCREEN_WAIT = re.compile(r"(?:\d+\s+)?(?:shell|monitor)s?\s+still\s+running", re.I)  # the Claude Code footer with the turn ended, waiting on a background process
SCREEN_LINES = 30  # lines from the end of the screen read per terminal (the footer is in the last ones; the permission prompt with the command and the notice goes past 15)
SCREEN_OPTION = re.compile(r"^\s*([❯>])?\s*(\d{1,2})\.\s+(\S.*?)\s*$")  # `❯ 1. Yes`: an option of a Claude Code menu, with the cursor on the chosen one
SCREEN_QUESTIONS = (("trust", re.compile(r"trust (?:this|the files in this) folder|Is this a project you (?:created|trust)", re.I)),
                  ("permissao", re.compile(r"Do you want to \w+|Yes, and don't ask again", re.I)),
                  ("pergunta", re.compile(r"Enter to select|Type something|Chat about this", re.I)))  # o AskUserQuestion aberto
SCREEN_FOOTER_MAX = 4  # non-empty lines after the options (menu footer) for the menu to still count as open
WAIT_PHASE = re.compile(r"^\s*(?:esperando:|waiting\b[:\s.…-]*)\s*(.*?)(?:\s+até\s+(\d{1,2}):(\d{2}))?\s*$", re.I)
STEER_READ_S = 90  # adjustment the dispatch has not read (`read` in the Orca inbox or the id in the worker's transcript) this long after sending, or after the last retype, is redelivered
STEER_ATTEMPTS = 3  # retypes of the notice to the idle worker; without a read STEER_READ_S after the third, it becomes the "steer não lido" (unread steer) alert
STEER_TRANSCRIPT_BYTES = 4_000_000  # the end of the worker's transcript where the steer message id is looked up
STEER_WINDOW_S = 30 * 60  # a steer older than this leaves the tracking: the 200-message inbox no longer reaches it
NOT_STARTED_S = 120  # a dispatch open with no turn recorded this long after the dispatch did not start (the worker-start that was left without Enter)
STOPPED_S = 60  # the turn ended this long ago and nothing more came: the worker stopped at the prompt (the threshold avoids calling the gap between two turns idle)
TURNS = "turns.json"  # {dispatch: {task, sessao, inicio, fim}}: what the worker's prompt and stop hooks write, without calling Orca
TURNS_DAYS = 7  # a turn older than this leaves turnos.json on the next write
CONTROL_LINES = 5  # entries of the control history that `orq agents` shows per dispatch (the newest ones)
AGENT_ORDER = {"travado": 0, "limite": 1, "sem_terminal": 2, "nao_comecou": 3, "parado": 4, "perguntando": 5, "rodando": 6, "aguardando_integracao": 7, "entregue": 8, "devolvida": 8, "servico": 9, "hibernado": 10, "encerrado": 11, "liberado": 12}
START_WAIT_S = float(os.environ.get("ORQ_INICIO_ESPERA_S") or 8)  # how long `orq dispatch_worker` waits for the spec prompt to enter the worker, before and after the Enter
ID_DISPATCH = re.compile(r"--dispatch-id (ctx_\w+)")  # in the Orca dispatch preamble, in the commands the worker runs
ID_TASK = re.compile(r"Your task ID is: (task_\w+)")
HB_WINDOW_S = 120  # a notice that arrives right after an absorbed batch of heartbeats finds the box empty: it is also blocked
HB_BATCHES = 4  # consecutive heartbeat batches that a single notice confirms (--ack returns the next batch)
NOTICE_RUN = re.compile(r"orchestration check --run (run_\w+)")
SUMMARY_REQUEST_S = 60  # a user entry newer than this is the request of `orq summary` itself
ANDA = ("rodando", "perguntando", "travado", "limite", "parado", "nao_comecou", "aguardando_integracao", "devolvida")  # states that appear in "Anda" (Moving) of `orq summary`
PANEL_ALIVE = "manager-alive"  # the agent manager panel touches this file on every round (painel-agent-manager.sh), outside orq
PANEL_STOPPED_S = 60  # a stamp older than this already warrants a "painel lento" (slow panel) notice; stopped is the limit of panel_limit_s
PANEL_LIMIT_MIN_S = 90  # the stamp only counts as a stopped panel after this long (or PANEL_ROUNDS_X average rounds, whichever is greater)
PANEL_ROUNDS_X = 3
PANEL_INTERVAL_S = 10  # the panel's `sleep` with the fast round
PANEL_INTERVAL_MAX_S = 30  # ceiling of the `sleep` when the round exceeds PANEL_INTERVAL_S
REMEMBERED_ROUNDS = 10  # round durations kept in gerente.json `voltas_s`
INTEGRATE_QUEUE_FILE = "integrate-queue.json"  # {itens: [{branch, ticket, ts}]}: the branches waiting for the integrator; the ticket's worker stays "aguardando integração" (awaiting integration)
PANEL_CHECK = "manager-check.json"  # {ts, terminal, morto}: what the last `orq manager checar` (outside the hook) saw in Orca; the hook only reads it
MANAGER = "manager.json"  # {coordenador, gerente, runs}: the coordinator talks to Orca through the agent manager terminal
RUN_STOPPED_MIN = float(os.environ.get("ORQ_RUN_PARADO_MIN") or 30)  # a Run with no open task or message for this long leaves the manager
RECENT_RUN_H = 24  # a Run with no open work only shows up in the summary until 24 h after the last activity; after that it goes to the archive (`orq runs --include_all`)
RUN_TESTE = re.compile(r"teste|descart[aá]vel", re.I)  # test Run objective: never shown by default
RUN_STOPPED_CACHE_S = float(os.environ.get("ORQ_RUN_PARADO_CACHE_S") or 300)  # a Run seen alive is not rechecked (run-show + task-list) before this; the release is delayed by at most this much
MANAGER_STUCK_S = float(os.environ.get("ORQ_GERENTE_PRESO_S") or 120)  # the panel stays on the notice's Run until the coordinator confirms, or for this period
_UNKNOWN = object()  # "I haven't asked Orca which Run is attached yet"
MUTA_RUN = {"worker-start", "send", "check", "reply", "task-create", "task-update"}  # Orca only accepts these from the terminal attached to the --run's Run
NOTICE_FINAL = re.compile(r"You have \d+ orchestration messages?\. Run `orca orchestration check --run run_\w+(?: --terminal [\w-]+)?`\.?\s*$")  # the Orca notice at the end of the prompt (B26)  # the Orca notice cites the Run: "Run `orca orchestration check --run <r>`."
WAIT_REPORT_S = 3600  # Orca marks the terminal automation as completed before the agent writes the report: waits up to 1 h for the file
CHOICE_LAVISH = ("escolha", "escolhida", "decidido", "manter", "trocar")  # disposicao of a Lavish item that closes the decision (with an answer); the rest is a free-form answer
ISSUES = os.environ.get("ORQ_ISSUES") or os.path.join(PLAN, "issues")  # one file per ticket: NN-<slug>.md
MAP = os.environ.get("ORQ_MAPA") or os.path.join(PLAN, "desenho.md")  # plan map (optional): Destino, Notes, Decisões até aqui, Não especificado; see docs/design.md
STATUS_NEW = "ready-for-agent"  # the triage role from docs/agents/triage-labels.md: complete ticket, ready for an agent
STATUS_IN_PROGRESS = "claimed"  # despachar --ticket sets this status: the ticket already has a worker (B24)
STATUS_CLOSED = "resolved"
SESSION_LINES = 12  # SessionStart injects orq status and the open tickets in up to 12 lines, counting the map path line
DONE_LAVISH = "feito"  # disposicao (or answer with `escolha`) of "já fiz" (already done): closes the pending item of any type
ALREADY_DONE = "ja-fez"  # id/header of the item that lists the already-done pending items, like AskUserQuestion's multiSelect
CHAT_LAVISH = "__conversar"  # the answer the page sends with the "conversar" (talk) disposicao: a postponement, not user text
MARK_LAVISH = "\n\nContext data:\n"  # lavish-axi poll puts queuePrompt's data after this, inside the prompt text
_STRING_JSON = re.compile(r'"(?:[^"\\\n]|\\.)*"')
# Orca's own detector (findOrcaDispatchPreambleStart): optional opening line, optional <pasted_content>, and the preamble;
# hosts without the opening line send the bare preamble
DISPATCH_WITHOUT_OPENING = re.compile(r"(?:<pasted_content\b[^>]*>\s*)?You are working inside Orca, a multi-agent IDE\.")
CLOSED_FILE = "limpar-fechados.json"  # {confirmado}: written by the first real `orq clean --closed_items`; before it, the automatic cleanup only shows the preview
PRS = "prs.json"  # {itens: [{task, url, numero, base, estado, ligado_em, avisado, ...}], ultimo_poll}: each feature's PRs, linked by `orq pr ligar`
PR_POLL_S = float(os.environ.get("ORQ_PR_POLL_S") or 120)  # minimum interval between two gh polls (outside the hooks); `orq pr poll --force` ignores it
PR_GH_S = 15  # time limit for each `gh pr view`
OLD_READ_MIN = float(os.environ.get("ORQ_LEITURA_VELHA_MIN") or 10)  # minutes: CI and conflict read longer ago than this show up as a stale reading
CHECK_FAILED = {"FAILURE", "TIMED_OUT", "CANCELLED", "ACTION_REQUIRED", "STARTUP_FAILURE", "ERROR"}
STOPPED_WT_D = 3  # a worktree with no worker and no activity for longer than this enters the `orq status` line
PR_VISIBLE_D = 7  # a feature with all PRs resolved for longer than this leaves `orq status`
FLOWS = ("promocao", "direto")  # promocao (promotion): the same feature branch opens one PR per environment, in order; direto (direct): a single PR, to the production branch
BRANCH_NO_REMOTE = "main"  # the default branch when the remote does not say which one it is (no origin/HEAD) and the project declares no environments
ACTION_TITLES = re.compile(r"^#{1,6}\s*(?:\d+\.\s*)?(?:Itens de ação|O que fazer hoje|O que precisa de ação)\s*$", re.I)
# the plan limit notice on a line at the end of the screen. Claude Code 2.1: `You've hit your session limit · resets 6:50pm (…)` and `Usage limit reached · continuing automatically at …`
# (the phrases it writes to the transcript); `Weekly limit reached ∙ resets Oct 5, 9am` is the weekly one. Codex 0.159 (binary strings): `You've hit your usage limit. …` and `You're out of credits`.
# Only valid at the start of the line (except the `⎿`/`●`/`■` decoration): the phrase quoted in the middle of a worker's text does not count.
SCREEN_LIMIT_CLAUDE = re.compile(r"^\W*(?:You[\'’]ve hit your [\w -]*limit\b|Usage limit reached\b|(?:Weekly|Session|Opus|Sonnet|\d+-hour) limit reached\b)", re.M)
SCREEN_LIMIT_CODEX = re.compile(r"^\W*(?:You[\'’]ve hit your usage limit\b|You[\'’]re out of credits\b|Usage limit reached\b)", re.M)
SCREEN_FAILURE = ("No conversation found", "command not found")  # claude --resume did not find the session, or the command does not even exist


# ---------- harness (claude | codex) ----------
# What changes from one agent to another, in a table: the resume command (the launch is Orca's, `worker-start --agent`) and the screen patterns.
# An agent outside the table has no orq hook and no screen read: its state stays `unknown`. Design: orq-claude-e-codex.md in the plan (ORQ_PLAN)
HARNESS = {
    "claude": {
        "resume": lambda session, model, effort, msg: ["claude", "--resume", session, *(["--model", model] if model else []),
                                                       "--dangerously-skip-permissions", msg],
        "tela": {"opcao": SCREEN_OPTION, "cursor": "❯", "perguntas": SCREEN_QUESTIONS, "espera": SCREEN_WAIT, "limite": SCREEN_LIMIT_CLAUDE, "falha": SCREEN_FAILURE,
                 "pronto": re.compile(r"bypass permissions|\? for shortcuts|esc to interrupt")},  # claude's box is on screen: it is possible to type
        "abrir": lambda model, effort, msg: ["claude", *(["--model", model] if model else []), "--dangerously-skip-permissions", msg],  # o mate (ticket 80)
        # the mate opens without a prompt and the text is typed (ticket 80). What made claude non-interactive was the `env ORQ_MATE=…` in front, not the prompt on the line (ticket 106)
        "digita_prompt": True,
        "efforts": ("low", "medium", "high", "xhigh", "max"),
        "filho": re.compile(r"/shell-snapshots/"),  # the Bash tool command (E2E, test, build, background shell) starts as `zsh -c source ~/.claude/shell-snapshots/…`, a child of claude
    },
}
# Codex numbers the options with `›` (or `>`) at the cursor; the folder trust and the untrusted-hooks modal are the menus that stop one of its workers
SCREEN_OPTION_CODEX = re.compile(r"^\s*([›>])?\s*(\d{1,2})\.\s+(\S.*?)\s*$")
HARNESS["codex"] = {
    "resume": lambda session, model, effort, msg: ["codex", "resume", session, *(["-m", model] if model else []),
                                                   *(["-c", f'model_reasoning_effort="{effort}"'] if effort else []),
                                                   "--dangerously-bypass-approvals-and-sandbox", msg],
    "abrir": lambda model, effort, msg: ["codex", *(["-m", model] if model else []), *(["-c", f'model_reasoning_effort="{effort}"'] if effort else []),
                                          "--dangerously-bypass-approvals-and-sandbox", msg],
    "tela": {"opcao": SCREEN_OPTION_CODEX, "cursor": "›", "enter_separado": True,  # the number with Enter in the same send does not confirm the menu (01/10)
             "perguntas": (("trust", re.compile(r"Trust this folder\?|Do you trust the contents of this directory", re.I)),
                           ("hooks", re.compile(r"Hooks? need review|hooks? (?:are|is) new or changed", re.I))),
             "limite": SCREEN_LIMIT_CODEX,
             "espera": re.compile(r"\d+\s+background terminals?\s+running", re.I),  # `• Working (9s • esc to interrupt) · 1 background terminal running`
             "falha": ("No saved session found", "command not found")},
    "efforts": ("low", "medium", "high", "xhigh", "max", "ultra"),  # ~/.codex/models_cache.json: ultra only on the models that have it (Orca refuses it on Luna)
}
HARNESSES = tuple(HARNESS)
CODEX_CONFIG = os.environ.get("ORQ_CODEX_CONFIG") or os.path.join(os.environ.get("CODEX_HOME") or os.path.expanduser("~/.codex"), "config.toml")
CODEX_HOOKS = os.environ.get("ORQ_CODEX_HOOKS") or os.path.join(os.path.dirname(CODEX_CONFIG), "hooks.json")
CLAUDE_SETTINGS = os.environ.get("ORQ_CLAUDE_SETTINGS") or os.path.expanduser("~/.claude/settings.json")
HOOKS_FILES = {"claude": CLAUDE_SETTINGS, "codex": CODEX_HOOKS}  # where each harness reads the hooks; each one's example sits next to orq.py
HOOKS_EXAMPLE = {"claude": "settings.hooks.example.json", "codex": "codex.hooks.example.json"}
ORQ_LINK = os.environ.get("ORQ_LINK") or os.path.expanduser("~/.local/bin/orq")  # what the manager loop calls (`orq`): a wrapper that pins the interpreter
HOOK_ORQ = re.compile(r"orq\.py hook \w+|precompact\.py(?: retomar)?")  # the call of an orq hook, without the path or the harness argument
PATCH_FILE = re.compile(r"^\*\*\* (?:Add|Update|Delete) File: (.+?)\s*$", re.M)  # the path of each file that Codex's apply_patch touches


# ---------- puras ----------

ORQ_NOTICES = ("orq hooks broken ", "orq: PR ", "orq: E2E queue", "orq: plan usage", "orq: worker ", "orq: coordinator stopped ", "orq ▸ request ", "orq ▸ mate ",
              "orq: Fila do E2E", "orq: uso do plano", "orq: coordenador parado ", "orq ▸ pedido ")  # what the panel types into the coordinator (notify_coordinator)


def origin_name(prompt):
    """usuario | notificacao | orca | comando | resumo | despacho | aviso_orq (the line the panel types when a linked PR is resolved)."""
    p = (prompt or "").lstrip()
    if p.startswith("<task-notification"):
        return "notificacao"
    if re.match(r"You have \d+ orchestration", p):
        return "orca"
    if p.startswith(("<command-", "<local-command", "/compact")):
        return "comando"
    if p.startswith("This session is being continued"):
        return "resumo"
    if p.startswith(ORQ_NOTICES):
        return "aviso_orq"
    if p.startswith("Please carry out this task from my Orca coordinator") or DISPATCH_WITHOUT_OPENING.match(p):
        return "despacho"
    return "usuario"


def split_notice(prompt):
    """(text without Orca's notice at the end, if there was one): Orca types the notice on the line the user was writing, and the rest may be half-typed input (B26)."""
    p = prompt or ""
    m = NOTICE_FINAL.search(p)
    return (p[:m.start()].rstrip(), True) if m else (p, False)


def open_entries(events, group_name=None):
    """Entries without any intake with the same id, in the order they came in. Only those of the reader: the ones of the mate for `group_name` (default: the environment's ORQ_MATE)
    or, without a group, the coordinator's. An entry typed into the mate carries `group_name`; one the mate sends up (mate origin) doesn't, and belongs to the coordinator (ticket 80)."""
    g = group_name or os.environ.get("ORQ_MATE") or None
    closed_ids = {e.get("entrada") for e in events if e.get("tipo") == "intake"}
    return [e for e in events if e.get("tipo") == "entrada" and e.get("id") and e["id"] not in closed_ids and e.get("grupo") == g]


def marked(answer_text, options):
    """Splits a multiSelect answer into (checked labels, free text). AskUserQuestion joins everything with ', '.

    ponytail: matches by substring, from the longest label to the shortest; free text containing an exact label counts as checked.
    """
    rest, found_labels = answer_text, []
    for o in sorted(options, key=lambda o: -len(o.get("label") or "")):
        label = o.get("label") or ""
        if label and label in rest:
            found_labels.append(label)
            rest = rest.replace(label, "", 1)
    return found_labels, rest.strip(' ,"')


def suspects(events, now_at):
    """Headers with resposta_suspeita still unconfirmed.

    Leaves with an answer for the same header, with a real answer from the same session on another question (the question redone
    with another header), with pend done for the same id, or after ALERT_H hours.
    """
    open_by_header = {}
    for e in events:
        t = e.get("tipo")
        if t == "resposta_suspeita":
            if not e.get("ts") or (now_at - _dt(e["ts"])).total_seconds() < ALERT_H * 3600:
                open_by_header[e.get("header")] = e
        elif t == "resposta":
            for h, s in list(open_by_header.items()):
                same_session = s.get("sessao") and s.get("sessao") == e.get("sessao") and (not s.get("ask") or s.get("ask") != e.get("ask"))
                if h == e.get("header") or same_session:
                    del open_by_header[h]
        elif t == "pend" and e.get("op") == "done":
            open_by_header.pop(e.get("pend"), None)
    return list(open_by_header)


def only_heartbeats(msgs):
    """There is a message and all are heartbeat. An unknown type, missing type, or item that isn't an object counts as non-heartbeat: when in doubt, it passes."""
    return bool(msgs) and all(isinstance(m, dict) and m.get("type") == "heartbeat" for m in msgs)


def liveness_signal(m):
    """{msg, task, dispatch, fase, ts} of an Orca heartbeat (the phase comes in the payload)."""
    p = _payload(m)
    return {"msg": m.get("id"), "task": p.get("taskId"), "dispatch": p.get("dispatchId"), "fase": p.get("phase"), "ts": m.get("created_at")}


def declared_wait(phase, ts):
    """(reason, deadline) of a heartbeat `esperando: <reason> [até HH:MM]`, or None for an ordinary phase.

    `HH:MM` is local time, the next one after the heartbeat; without it the deadline is the heartbeat plus WAIT_CAP_S.
    """
    m = WAIT_PHASE.match(phase or "")
    ts = _ts(ts) if isinstance(ts, str) else ts
    if not m or not ts:
        return None
    reason = m.group(1) or "esperando"
    if m.group(2) is None:
        return reason, ts + timedelta(seconds=WAIT_CAP_S)
    local = ts.astimezone()
    deadline = local.replace(hour=int(m.group(2)) % 24, minute=int(m.group(3)) % 60, second=0, microsecond=0)
    return reason, (deadline if deadline >= local else deadline + timedelta(days=1)).astimezone(timezone.utc)


def _alive_or_stuck(phase, ts, age, now_at, screen=None, interrupted=False):
    """(state, wait, reason) of a dispatch open by the last heartbeat: a declared wait within the deadline is not stuck; expired, it is "espera vencida".

    Without a declared wait: `interrupted` (the coordinator paused the worker) and `screen` (shell/monitor running in the footer) are also waits, this one only up to
    SCREEN_CAP_S without a heartbeat. `age` is that of the last heartbeat (or of the dispatch).
    """
    wait_info = declared_wait(phase, ts)
    if wait_info and now_at <= wait_info[1]:
        return "rodando", wait_info[0], None
    if wait_info:
        return "travado", None, "wait expired"
    if interrupted:
        return "rodando", "interrupted by the coordinator", None
    if screen:
        return ("travado", None, "shell without heartbeat") if age is not None and age > SCREEN_CAP_S else ("rodando", screen, None)
    return ("travado" if age is not None and age > STUCK_S else "rodando"), None, None


def interrupted_dispatches(events):
    """{dispatch: ts} of the last successful `orq interrupt` of each dispatch."""
    return {e["dispatch"]: e.get("ts") for e in events if e.get("tipo") == "controle" and e.get("acao") == "interromper" and e.get("resultado") == "ok" and e.get("dispatch")}


def _paused(ts_interrupt, last_hb, t):
    """The interrupt holds until the worker gives a sign after it: a heartbeat or new turn (prompt) newer than the interrupt ends it."""
    i = _ts(ts_interrupt)
    return bool(i) and not (last_hb and _ts(last_hb) > i) and not (_ts(_dict(t).get("inicio")) and _ts(t["inicio"]) > i)


def liveness_signals(events):
    """{dispatch: last heartbeat (msg, task, fase, ts, run)} from the heartbeat_absorvido (bound Run) and heartbeat_visto (other Run) events."""
    out = {}
    for e in events:
        if e.get("tipo") in ("heartbeat_absorvido", "heartbeat_visto"):
            for h in e.get("heartbeats") or []:
                if isinstance(h, dict) and h.get("dispatch"):
                    out[h["dispatch"]] = {**h, "run": e.get("run")}
    return out


def _ts(x):
    """datetime of a stamp (Orca or ISO); None if missing or unreadable."""
    try:
        return _dt(x) if x else None
    except (AttributeError, ValueError):
        return None


def _z(x):
    """The stamp in ISO with Z, or None."""
    d = _ts(x)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ") if d else None


def dispatch_turn(t, agent, since, last_hb, now_at):
    """(turn, since_when) of what the worker's hooks say about an open dispatch.

    `nao_comecou`: no turn recorded and no heartbeat NOT_STARTED_S after the dispatch (proof of life counts for more than the lack of a record, as with the
    worker that was already running when the hooks arrived); `stopped`: the last turn ended STOPPED_S or more ago and no
    heartbeat came after; `open_state`: turn in progress (or ended a moment ago). `unknown` is what can't be asserted (agent without the orq hook,
    agent Orca didn't report, still inside the window): never counts as idle. `t` is {inicio, fim} of turnos.json; `since` and `last_hb` are datetimes.
    """
    if agent not in HARNESS:
        return "unknown", None
    start_time, end = _ts(_dict(t).get("inicio")), _ts(_dict(t).get("fim"))
    if not start_time:
        return ("nao_comecou", since) if since and not last_hb and (now_at - since).total_seconds() >= NOT_STARTED_S else ("unknown", None)
    if end and end >= start_time and not (last_hb and last_hb > end):
        return ("parado", end) if (now_at - end).total_seconds() >= STOPPED_S else ("aberto", None)
    return "aberto", None


def _msg_dispatch(m):
    """The dispatch that sent the message: payload.dispatchId, else the from_handle `dispatch:<id>`."""
    d = _payload(m).get("dispatchId")
    h = str(m.get("from_handle") or "")
    return d or (h[len("dispatch:"):] if h.startswith("dispatch:") else None)


def open_questions(msgs):
    """{dispatch: message} of a worker question/escalation still unanswered.

    Answered is one that has, after it, a message from another sender in the same thread or to the dispatch (or to whoever asked).
    """
    ms = sorted((m for m in msgs if isinstance(m, dict) and isinstance(m.get("sequence"), int)), key=lambda m: m["sequence"])
    out = {}
    for i, q in enumerate(ms):
        d = _msg_dispatch(q)
        if q.get("type") not in ("question", "escalation") or not d:
            continue
        resp = any(_msg_dispatch(m) != d and (m.get("thread_id") == q.get("id") or m.get("to_handle") in (f"dispatch:{d}", q.get("from_handle")))
                   for m in ms[i + 1:])
        if resp:
            out.pop(d, None)
        else:
            out[d] = q
    return out


def last_signals(events, msgs):
    """{dispatch: {fase, ts}} of the newest heartbeat of each dispatch: the log's heartbeat_absorvido and the inbox heartbeats."""
    out = {d: {"fase": h.get("fase"), "ts": h.get("ts")} for d, h in liveness_signals(events).items()}
    for m in msgs:
        d = _msg_dispatch(m) if isinstance(m, dict) and m.get("type") == "heartbeat" else None
        if d:
            signal_name = liveness_signal(m)
            new, old_value = _ts(signal_name["ts"]), _ts((out.get(d) or {}).get("ts"))
            if new and (not old_value or new > old_value):
                out[d] = {"fase": signal_name["fase"], "ts": signal_name["ts"]}
    return out


def _no_terminal(w, released, live):
    """A dispatch that has already finished and has no terminal left to release: it went through `orq release` (`release` event) or the terminal isn't in `orca terminal list`.

    `live` None (Orca didn't answer) proves nothing: only the event counts.
    """
    return w.get("dispatchStatus") != "dispatched" and (w.get("dispatchId") in released or (live is not None and w.get("agentTerminalHandle") not in live))


def _lost_terminal(w, live, paused, hibernated):
    """A `dispatched` dispatch (no worker_done) whose terminal isn't in `orca terminal list` and that is neither paused nor hibernated: what the Orca crash left
    for `orq resume`. `live` None (Orca didn't answer or cut the list) proves nothing: False."""
    d = w.get("dispatchId")
    return live is not None and w.get("dispatchStatus") == "dispatched" and w.get("agentTerminalHandle") not in live and d not in (paused or {}) and d not in (hibernated or {})


def integration_queue():
    """{ticket: {branch, ticket, ts}} of the branches waiting for the integrator (integrar-fila.json, fed by `orq integrate queue add`)."""
    return {i["ticket"]: i for i in _dict(_read_json(_path(INTEGRATE_QUEUE_FILE))).get("itens") or [] if isinstance(i, dict) and i.get("ticket")}


def integrate_queue_add(branch, ticket):
    """Puts the ticket's branch on the integrator queue (repeating swaps the branch, doesn't duplicate). While it is there, the ticket's worker stays `aguardando integração`."""
    n = str(ticket).strip().zfill(2)
    if not branch or not branch.strip():
        raise ValueError("empty branch")
    new = {"branch": branch.strip(), "ticket": n, "ts": now()}
    with _lock("integrate-queue.lock"):
        item_list = [i for i in _dict(_read_json(_path(INTEGRATE_QUEUE_FILE))).get("itens") or [] if isinstance(i, dict)]
        pos = next((k for k, i in enumerate(item_list) if i.get("ticket") == n), len(item_list))
        item_list[pos:pos + 1] = [new]
        _write_json(_path(INTEGRATE_QUEUE_FILE), {"itens": item_list}, indent=2)
    return append_event({"tipo": "integrar_fila", "op": "add", "ticket": n, "branch": new["branch"]})


def audit_publication(revs, repo=None, checks=("author", "trailer", "terms", "readme")):
    """Reasons why `revs` (args of `git rev-list`, e.g. `base..head`) cannot go to the public main: author or committer outside the configured noreply
    (`ORQ_AUTOR` or `git config user.email`), Co-Authored-By trailer or generator footer, forbidden term in the diff or message, `orqlib.py`/`orq.py` without `README.md` in the range.
    Returns [] if clean (ticket 139). The pre-push and the integrator, before the FF, run this same check. `checks` narrows it: `orq pr open` on a product repo
    passes ("author", "trailer"), since the forbidden terms list holds the product's own name and the README rule is orq's (ticket 227)."""
    git = lambda *x: subprocess.run([GIT, *(["-C", repo] if repo else []), *x], capture_output=True, text=True, check=True).stdout  # noqa: E731
    expected = os.environ.get("ORQ_AUTOR") or git("config", "user.email").strip()
    terms = []
    listing = os.environ.get("ORQ_TERMOS") or os.path.join(PLAN, "termos-proibidos.txt")
    if "terms" not in checks:
        pass
    elif os.path.exists(listing):
        spec = importlib.util.spec_from_file_location("audiencia_check", os.path.join(os.path.dirname(os.path.realpath(__file__)), "scripts", "audiencia-check.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        terms = mod.terms(listing)
    else:
        print(f"audit-publication: no {listing}, forbidden-terms check skipped", file=sys.stderr)
    reasons, files_set, first_code = [], set(), None
    for sha in git("rev-list", "--reverse", *revs).split():
        an, ae, cn, ce, msg = git("show", "-s", "--format=%an%x00%ae%x00%cn%x00%ce%x00%B", sha).split("\0", 4)
        findings = [f"author {an} <{ae}> and committer {cn} <{ce}> must be <{expected}> (git commit --amend --reset-author, or git rebase --exec 'git commit --amend --no-edit --reset-author')"
                   for _ in [0] if "author" in checks and {ae, ce} != {expected}]
        if "trailer" in checks and re.search(r"^co-authored-by:|generated with|🤖", msg, re.I | re.M):
            findings.append("Co-Authored-By trailer or generator footer in the message (git commit --amend to remove the line)")
        added_text = "\n".join(l[1:] for l in git("show", "--format=", "--unified=0", sha).splitlines() if l.startswith("+") and not l.startswith("+++"))
        findings += [f"forbidden term /{t.pattern}/ in the diff or the message" for t in terms if t.search(msg) or t.search(added_text)]
        changed_files = set(git("diff-tree", "--no-commit-id", "--name-only", "-r", "--root", sha).split())
        if "readme" in checks and first_code is None and changed_files & {"orqlib.py", "orq.py"}:
            first_code = sha
        files_set |= changed_files
        reasons += [f"{sha[:7]} {msg.splitlines()[0] if msg.strip() else ''}: {m}" for m in findings]
    if "readme" in checks and first_code and "README.md" not in files_set:
        reasons.append(f"{first_code[:7]}: changes orqlib.py/orq.py and no commit in the range touches README.md (document the change)")
    return reasons


def integrate_queue_rm(ticket):
    """Removes the ticket from the integrator queue (the branch is already on main, or they gave up on it). ValueError if it wasn't there."""
    n = str(ticket).strip().zfill(2)
    with _lock("integrate-queue.lock"):
        item_list = [i for i in _dict(_read_json(_path(INTEGRATE_QUEUE_FILE))).get("itens") or [] if isinstance(i, dict)]
        rest = [i for i in item_list if i.get("ticket") != n]
        if len(rest) == len(item_list):
            raise ValueError(f"ticket {n} is not in the integrator queue (orq integrate queue list)")
        _write_json(_path(INTEGRATE_QUEUE_FILE), {"itens": rest}, indent=2)
    return append_event({"tipo": "integrar_fila", "op": "rm", "ticket": n})


def _dispatch_ticket(events):
    """{dispatch or task: ticket number} of the dispatches made with `orq dispatch_worker --ticket`: what links a worker to the integrator queue."""
    out = {}
    for e in events:
        if e.get("tipo") == "despacho" and e.get("ticket"):
            out.update({k: e["ticket"] for k in (e.get("dispatch"), e.get("task")) if k})
    return out


def _services(events):
    """{dispatch: last cycle {ts, hash, nota} or None} of the dispatches that `orq dispatch_worker --service` or `orq service marcar` marked (integrator, secondmate)."""
    out = {e["dispatch"]: None for e in events if (e.get("tipo") == "despacho" and e.get("servico") or e.get("tipo") == "servico_marcado") and e.get("dispatch")}
    for e in events:
        if e.get("tipo") == "ciclo" and e.get("dispatch") in out:
            out[e["dispatch"]] = {k: e.get(k) for k in ("ts", "hash", "nota")}
    return out


def mark_service(dispatch):
    """Turns an already dispatched dispatch (from before `--service` existed) into a service dispatch: records `servico_marcado`, which `_services` reads as the dispatch's `--service`.
    ValueError if the dispatch doesn't exist (neither a dispatch in the log nor an agent in aberto.json) or was already released."""
    events = read_events()
    if dispatch not in {e.get("dispatch") for e in events if e.get("tipo") == "despacho"} | {a.get("dispatch") for a in _dict(_read_json(_path("open.json"))).get("agentes") or [] if isinstance(a, dict)}:
        raise ValueError(f"{dispatch} is not a known dispatch (orq agents)")
    if dispatch in _released(events) | {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("estado") == "released"}:
        raise ValueError(f"{dispatch} was already released")
    return append_event({"tipo": "servico_marcado", "dispatch": dispatch})


def cycle_done(dispatch, hash_, note=None, extra=None):
    """The service worker finished a cycle: records the `cycle` event. Doesn't talk to Orca (after the first worker_done it no longer has a capability).
    A known, not-yet-released dispatch that wasn't a service becomes one here (`servico_marcado`): reporting a cycle already proves it is. ValueError if the
    dispatch is unknown or was already released. `extra` are extra event fields (`integrate conclude` records branches and tickets)."""
    if dispatch not in _services(read_events()):
        try:
            mark_service(dispatch)
        except ValueError:
            raise ValueError(f"{dispatch} is not a service dispatch (orq dispatch --service)") from None
    return append_event({"tipo": "ciclo", "dispatch": dispatch, "hash": hash_, **({"nota": note} if note else {}), **(extra or {})})


def _open_cwds():
    """The working directories of live processes (`lsof`); empty if lsof doesn't exist."""
    try:
        r = subprocess.run(["lsof", "-a", "-d", "cwd", "-Fn"], capture_output=True, text=True, timeout=30)
    except (subprocess.TimeoutExpired, OSError):
        return []
    return [l[1:] for l in r.stdout.splitlines() if l.startswith("n")]


def _birth(folder):
    """When the worktree was born (its `.git`; birthtime where the system has it, else mtime)."""
    st = os.stat(os.path.join(folder, ".git"))
    return getattr(st, "st_birthtime", st.st_mtime)


def clean_orq_worktrees(repo=None, root=None, ref="origin/main", dry_run=False, now_at=None, resolved_tickets=None, live=None, backups=None):
    """Removes orq's `<root>/<ticket>` worktrees already published in `ref` (tickets 176 and the decision that followed it). The integrator fast-forwards main and the push is
    manual, so nobody else removed them. Stays, with the reason: the integrator's worktree (`integra/*`, `integration`), a loose-branch one, a ticket one with a live
    dispatch (not released: running, delivered or returned), one created less than 24 h ago, one with an uncommitted change (new file included) and one with a process inside.
    Branch contained in `ref` (`merge-base --is-ancestor`): `git worktree remove` + `git branch -d`. Branch with a rewritten hash: only if the ticket is resolved AND every commit
    of it has one with the same subject in `ref`; before, the refs go to a dated bundle in `backups` (verified), and then `worktree remove` + `branch -D`.
    Never `--force` nor `rm -rf`. `resolved_tickets` and `live` are sets of ticket numbers (default: the tickets with Status resolved; the unreleased dispatches).
    Returns {removidas: [{pasta, branch, via}], ficaram: [{pasta, motivo}], bundle}; with `dry_run` nothing is removed or recorded."""
    repo = repo or HOME
    root = root or WT_ROOT
    backups = backups or os.path.join(PLAN, "backups")
    now_at = now_at if now_at is not None else time.time()
    if resolved_tickets is None:
        resolved_tickets = {t["num"] for t in tickets() if t["status"] == STATUS_CLOSED}
    if live is None:
        ev = read_events()
        lib = _released(ev) | {e.get("dispatch") for e in ev if e.get("tipo") == "liberar" and e.get("estado") in ("released", "already_released")}
        live = {str(e["ticket"]).zfill(2) for e in ev if e.get("tipo") == "despacho" and e.get("ticket") and e.get("dispatch") not in lib}  # `retained` and `release_unknown` count as alive
    subjects = set((_git(repo, "log", "--format=%s", ref) or "").splitlines())
    out, cwds, rewrites = {"removidas": [], "ficaram": [], "bundle": None}, None, []
    for item_name in sorted(os.listdir(root)) if os.path.isdir(root) else []:
        d = os.path.join(root, item_name)
        if not os.path.exists(os.path.join(d, ".git")):
            continue
        number = (item_name[1:] if item_name.startswith("t") else item_name).zfill(2)
        branch = (_git(d, "branch", "--show-current") or "").strip()
        reason = ("loose branch (detached HEAD)" if not branch else "integrator worktree" if item_name == "integracao" or branch.startswith("integra/")
                  else "main branch" if _environment_branch(branch) else "live dispatch for the ticket" if number in live
                  else "created less than 24 h ago" if now_at - _birth(d) < 86400 else None)
        contained = not reason and _git(repo, "merge-base", "--is-ancestor", branch, ref) is not None
        if not reason and not contained:
            subs = (_git(repo, "log", "--format=%s", f"{ref}..{branch}") or "?").splitlines()
            if number not in resolved_tickets:
                reason = f"{branch} has a commit outside {ref} and the ticket is not resolved"
            elif not all(x in subjects for x in subs):
                reason = f"{branch} has a commit with no matching subject in {ref}"
        if not reason and ((st := _git(d, "status", "--porcelain")) is None or st.strip()):
            reason = "uncommitted changes"
        if not reason:
            cwds = _open_cwds() if cwds is None else cwds
            real = os.path.realpath(d)
            reason = "process inside the folder" if any(c == real or c.startswith(real + os.sep) for c in cwds) else None
        if reason:
            out["ficaram"].append({"pasta": d, "motivo": reason})
        else:
            out["removidas"].append({"pasta": d, "branch": branch, "via": "contida" if contained else "assunto"})
    if dry_run:
        return out
    rewrites = [x for x in out["removidas"] if x["via"] == "assunto"]
    if rewrites:
        os.makedirs(backups, exist_ok=True)
        bundle = os.path.join(backups, f"orq-wt-{time.strftime('%Y-%m-%d', time.localtime(now_at))}.bundle")
        if os.path.exists(bundle):
            bundle = bundle[:-7] + time.strftime("-%H%M%S", time.localtime(now_at)) + ".bundle"
        ok = subprocess.run(["git", "-C", repo, "bundle", "create", bundle, *[x["branch"] for x in rewrites]], capture_output=True).returncode == 0 \
            and subprocess.run(["git", "-C", repo, "bundle", "verify", bundle], capture_output=True).returncode == 0
        out["bundle"] = bundle if ok else None
    for x in list(out["removidas"]):
        if x["via"] == "assunto" and not out["bundle"]:
            reason = "backup bundle failed: nothing removed"
        elif _git(repo, "worktree", "remove", x["pasta"]) is None:
            reason = "git worktree remove refused"
        elif _git(repo, "branch", "-D" if x["via"] == "assunto" else "-d", x["branch"]) is None:
            reason = f"folder removed, but git branch refused {x['branch']}"
        else:
            continue
        out["removidas"].remove(x)
        out["ficaram"].append({"pasta": x["pasta"], "motivo": reason})
    return out


def integrate_conclude(hash_, branches, dispatch=None):
    """`integrate.py` advanced main by fast-forward to `hash_`: closes what the cycle integrated (ticket 154). For each branch on the integrator queue:
    removes the ticket from the queue, `ticket_close` with the hash in the Answer and `release` the ticket's worker. A branch off the queue only enters the cycle. Records the integrator's `cycle`
    (`dispatch`, or the not-yet-released service titled "integrador"; without it the event stays without a dispatch, which the Stop still reads). Nothing here is a push: it stays manual.
    A failure of one step becomes a notice and doesn't stop the others. Returns {hash, branches, tickets, avisos}."""
    queue, event_list = integration_queue(), read_events()
    tickets_, notices = [], []
    for b in branches:
        item = next((i for i in queue.values() if i["branch"] == b), None)
        if not item:
            continue
        n = item["ticket"]
        tickets_.append(n)
        integrate_queue_rm(n)
        try:
            notices += [x for x in [ticket_close(n, f"integrated into main at {hash_}")["aviso"]] if x]
        except ValueError as e:
            notices.append(f"ticket {n}: {e}")
        released = _released(read_events())
        d = next((e["dispatch"] for e in reversed(event_list) if e.get("tipo") == "despacho" and e.get("ticket") == n and e.get("dispatch") and e["dispatch"] not in released), None)
        if not d:
            notices.append(f"ticket {n}: no worker to release (no dispatch --ticket, or already released)")
            continue
        try:
            notices += [x for x in [release(d).get("aviso")] if x]
        except (ValueError, RuntimeError, subprocess.TimeoutExpired, OSError) as e:
            notices.append(f"release {d}: {e}")
    d = dispatch or (_integrator_dispatch(event_list) or {}).get("dispatch")
    extra = {"branches": list(branches), "tickets": tickets_}
    if d:
        cycle_done(d, hash_, extra=extra)
    else:
        append_event({"tipo": "ciclo", "hash": hash_, **extra})
    return {"hash": hash_, "branches": list(branches), "tickets": tickets_, "avisos": notices}


def build_agents(workers, msgs, events, now_at, details=None, live=None, turns=None, screens=None, screen_questions=None, hibernated=None, integration=None, limits=None, paused=None):
    """Pure: one line per worker-list dispatch, with the state (rodando, travado, nao_comecou, parado, perguntando, entregue or liberado).

    Dispatched without an open question is `nao_comecou` or `stopped` when the turns from the worker hooks say so (dispatch_turn); otherwise `travado` when the
    last heartbeat (or, with none, the dispatch) is more than STUCK_S old; completed with the terminal released is `liberado`, the rest is `delivered`
    (worker_done given, terminal still open). `details` is {dispatch: titulo, modelo, desde, agente}; `live` are the handles from `orca terminal list`;
    `turns` is turnos.json (None: no data, the turn stays `unknown`); `screens` is {dispatch: reason} of what the terminal screen shows waiting;
    `screen_questions` is {dispatch: screen_question} of menus waiting for a human answer in the terminal (the worker becomes `perguntando`, with the `question` on the line);
    `hibernated` is cursor.json `hibernated` ({dispatch: {desde, motivo, …}}): the worker becomes `hibernado` (terminal closed on purpose, session stored).
    `integration` is {ticket: {branch, ticket}} from the integrator queue: the ticket's worker that would be `travado`, `nao_comecou` or `stopped` becomes `aguardando_integracao`
    (known wait, no steer suggestion). `limits` is {dispatch: line} of the plan-limit notice on screen (screen_limit): the worker becomes `limit`
    (no turn until the plan renews; an open menu counts for more). A service dispatch (`orq dispatch_worker --service`) delivered becomes `service`, with the last cycle.
    """
    signals, questions, details, humans = last_signals(events, msgs), open_questions(msgs), details or {}, _interaction_recorded(events)
    released, pauses, screens, not_started = _released(events), interrupted_dispatches(events), screens or {}, _not_started(events)
    delivery_notices = {e.get("dispatch"): e["avisos"] for e in events if e.get("tipo") == "entrega" and e.get("avisos")}
    integration, dispatch_tickets, services = integration_queue() if integration is None else integration, _dispatch_ticket(events), _services(events)
    controls = {}
    for e in events:
        if e.get("tipo") == "controle" and e.get("resultado") != "iniciado":
            c = {k: e[k] for k in ("ts", "acao", "resultado", "dispatch", "novo_dispatch", "motivo", "nota") if e.get(k)}
            for d in {e.get("dispatch"), e.get("novo_dispatch")} - {None}:
                controls.setdefault(d, []).append(c)
    out = []
    for w in workers:
        d = w.get("dispatchId")
        detail_entry, signal_name = details.get(d) or {}, signals.get(d) or {}
        ref = _ts(signal_name.get("ts")) or _ts(detail_entry.get("desde"))  # the last heartbeat; with none, the dispatch
        age = int((now_at - ref).total_seconds()) if ref else None
        t = _dict((turns or {}).get(d))
        turn, heartbeat_age = "unknown", age
        if w.get("dispatchStatus") == "dispatched":
            if turns is not None:
                turn, when = dispatch_turn(t, detail_entry.get("agente"), _ts(detail_entry.get("desde")), _ts(signal_name.get("ts")), now_at)
                if when:
                    age = int((now_at - when).total_seconds())
            pause = _paused(pauses.get(d), signal_name.get("ts"), t)
            state, waiting, reason = _alive_or_stuck(signal_name.get("fase"), signal_name.get("ts"), heartbeat_age, now_at, screens.get(d), pause)
            state = "perguntando" if d in questions or (screen_questions or {}).get(d) else turn if turn in ("nao_comecou", "parado") and not (waiting or reason) else state  # a declared wait (within the deadline or expired) outweighs the ended turn (M16)
            if d in not_started and not (t.get("inicio") or signal_name):  # despachar saw the prompt fail to enter: it does not wait NOT_STARTED_S
                state = "nao_comecou"
            if (limits or {}).get(d) and state != "perguntando":
                state = "limite"
            if _lost_terminal(w, live, paused, hibernated) and not (limits or {}).get(d):  # with no terminal no steer arrives: what is missing is `orq resume`; limit screen read = live terminal
                state, waiting, reason = "sem_terminal", None, None
        else:
            waiting = reason = None
            done = any(m.get("type") == "worker_done" and _payload(m).get("dispatchId") == d for m in msgs or [])
            state, age = ("liberado" if w.get("terminalState") == "released" or _no_terminal(w, released, live) else "entregue" if done else "encerrado"), None
            state = "servico" if state == "entregue" and d in services else state
            state = "devolvida" if state == "entregue" and d in _sent_back(events) else state  # the coordinator sent it back to redo: it is not a delivery to integrate until the new worker_done
        agent_row = {"dispatch": d, "task": w.get("taskId"), "run": w.get("runId"), "titulo": detail_entry.get("titulo"), "modelo": detail_entry.get("modelo"),
              "effort": detail_entry.get("effort"), "terminal": w.get("agentTerminalHandle"), "estado": state, "fase": signal_name.get("fase"), "ultimo_heartbeat": _z(signal_name.get("ts")),
              "desde": _z(detail_entry.get("desde")), "idade_s": age, "agente": detail_entry.get("agente"), "turno": turn,
              "turno_inicio": _z(t.get("inicio")), "turno_fim": _z(t.get("fim"))}
        if waiting:
            agent_row["espera"] = waiting
        _mark_integration_and_service(agent_row, integration, dispatch_tickets, services)
        if (screen_questions or {}).get(d) and w.get("dispatchStatus") == "dispatched" and state != "sem_terminal":
            agent_row["pergunta"] = screen_questions[d]
        if screens.get(d) and state != "sem_terminal":
            agent_row["tela"], agent_row["tela_ts"] = screens[d], now_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        if (limits or {}).get(d) and w.get("dispatchStatus") == "dispatched":
            agent_row["limite"], agent_row["limite_ts"] = limits[d], now_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        if reason and state == "travado":
            agent_row["motivo"] = reason
        if delivery_notices.get(d):
            agent_row["entrega"] = delivery_notices[d]
        if controls.get(d):
            agent_row["controle"] = controls[d][-CONTROL_LINES:]
        retained = _retention(w, humans)
        if state == "entregue" and retained:
            agent_row["retido"] = retained
        if d in (hibernated or {}):
            agent_row.update(estado="hibernado", idade_s=None, hibernado_desde=_dict(hibernated[d]).get("desde"), motivo_hibernado=_dict(hibernated[d]).get("motivo"))
            for k in ("espera", "pergunta", "tela", "tela_ts", "limite", "limite_ts", "motivo", "retido"):
                agent_row.pop(k, None)
        out.append(agent_row)
    return sorted(out, key=lambda a: AGENT_ORDER[a["estado"]])


def _sent_back(events):
    """{dispatch: ts} of the deliveries returned to the worker (`orq send_back`) that don't yet have a new worker_done in the log.
    # ponytail: the log order holds; an old worker_done ingested after the return undoes it (the ingest usually runs before)."""
    out = {}
    for e in events:
        d = e.get("dispatch")
        if e.get("tipo") == "devolver" and d:
            out[d] = e.get("ts")
        elif e.get("tipo") == "worker_done" and d:
            out.pop(d, None)
    return out


def _mark_integration_and_service(agent_row, integration, dispatch_tickets, services):
    """Puts into `agent_row` (an agent's line) what the integrator queue and the service dispatches say about it: the state `aguardando_integracao` in place of
    travado/nao_comecou/parado, and the service's `cycle`. The reason for travado goes away: the worker isn't stuck, it's waiting for main."""
    agent_row.pop("integracao", None)
    tk = dispatch_tickets.get(agent_row.get("dispatch")) or dispatch_tickets.get(agent_row.get("task"))
    if agent_row["estado"] in ("travado", "nao_comecou", "parado") and tk in integration:
        agent_row["estado"], agent_row["integracao"] = "aguardando_integracao", integration[tk]
        agent_row.pop("motivo", None)
    if agent_row["estado"] == "servico" and services.get(agent_row.get("dispatch")):
        agent_row["ciclo"] = services[agent_row["dispatch"]]


def _not_started(events):
    """The dispatches that `orq dispatch_worker` marked with `not_started` (the spec prompt didn't go in, not even after Enter)."""
    return {e.get("dispatch") for e in events if e.get("tipo") == "nao_iniciou"}


def reassess(agents_, events, now_at, turns=None, integration=None):
    """The agents from the aberto.json cache with rodando/travado/nao_comecou/parado redone by the newest heartbeat in the log and, with `turns`, by turnos.json (the cache lags up to one prompt).
    Also redoes `aguardando_integracao` (the integrator queue is read now, or comes in `integration`) and `service` (the newest cycle in the log)."""
    integration, dispatch_tickets, services = integration_queue() if integration is None else integration, _dispatch_ticket(events), _services(events)
    signals = liveness_signals(events)
    released = _released(events) | {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("estado") == "released"}
    pauses, out, sent_back_list = interrupted_dispatches(events), [], _sent_back(events)
    for agent_row in agents_:
        agent_row = dict(agent_row)
        if agent_row.get("estado") in ("entregue", "devolvida", "servico", "rodando", "travado", "limite", "nao_comecou", "parado", "aguardando_integracao") and agent_row.get("dispatch") in released:
            agent_row["estado"] = "liberado"  # orq liberar after the cache (M13); release only exists after worker_done, so it holds even with a cache older than it (B38)
        if agent_row.get("estado") in ("entregue", "servico") and agent_row.get("dispatch") in services:
            agent_row["estado"] = "servico"
        if agent_row.get("estado") in ("entregue", "devolvida"):
            agent_row["estado"] = "devolvida" if agent_row.get("dispatch") in sent_back_list else "entregue"  # the new worker_done brings the delivery back to integrate
        if agent_row.get("estado") in ("rodando", "travado", "limite", "nao_comecou", "parado", "aguardando_integracao"):
            h = signals.get(agent_row.get("dispatch")) or {}
            if _ts(h.get("ts")) and (not _ts(agent_row.get("ultimo_heartbeat")) or _ts(h["ts"]) > _ts(agent_row["ultimo_heartbeat"])):
                agent_row["fase"], agent_row["ultimo_heartbeat"] = h.get("fase"), _z(h["ts"])
            ref = _ts(agent_row.get("ultimo_heartbeat")) or _ts(agent_row.get("desde"))
            agent_row["idade_s"] = int((now_at - ref).total_seconds()) if ref else None
            t = _dict(turns.get(agent_row.get("dispatch"))) if turns is not None else {"inicio": agent_row.get("turno_inicio"), "fim": agent_row.get("turno_fim")}
            screen = agent_row.get("tela") if agent_row.get("tela") and _ts(agent_row.get("tela_ts")) and not (_ts(agent_row.get("ultimo_heartbeat")) and _ts(agent_row["ultimo_heartbeat"]) > _ts(agent_row["tela_ts"])) \
                and not (_ts(t.get("inicio")) and _ts(t["inicio"]) > _ts(agent_row["tela_ts"])) else None  # the screen read is only valid until the worker gives a sign after it
            agent_row["estado"], waiting, reason = _alive_or_stuck(agent_row.get("fase"), agent_row.get("ultimo_heartbeat"), agent_row["idade_s"], now_at, screen,
                                                            _paused(pauses.get(agent_row.get("dispatch")), agent_row.get("ultimo_heartbeat"), t))
            agent_row.pop("espera", None), agent_row.pop("motivo", None)
            if waiting:
                agent_row["espera"] = waiting
            if reason:
                agent_row["motivo"] = reason
            agent_row["turno"], when = dispatch_turn(t, agent_row.get("agente"), _ts(agent_row.get("desde")), _ts(agent_row.get("ultimo_heartbeat")), now_at)
            if agent_row["turno"] in ("nao_comecou", "parado") and not (waiting or reason):  # whoever waits on purpose (E2E queue, CI) ends the turn and is not idle; expired, it is stuck (M16)
                agent_row["estado"], agent_row["idade_s"] = agent_row["turno"], int((now_at - when).total_seconds())
            elif agent_row.get("dispatch") in _not_started(events) and not (agent_row.get("turno_inicio") or h):
                agent_row["estado"] = "nao_comecou"
            if agent_row.get("limite") and _ts(agent_row.get("limite_ts")) and not (_ts(agent_row.get("ultimo_heartbeat")) and _ts(agent_row["ultimo_heartbeat"]) > _ts(agent_row["limite_ts"])) \
                    and not (_ts(t.get("inicio")) and _ts(t["inicio"]) > _ts(agent_row["limite_ts"])):  # the notice read is valid until the worker gives a sign after it
                agent_row["estado"] = "limite"
            else:
                agent_row.pop("limite", None), agent_row.pop("limite_ts", None)
        _mark_integration_and_service(agent_row, integration, dispatch_tickets, services)
        out.append(agent_row)
    return out


def alive_line(events, open_state, now_at=None, turns=None):
    """'Vivos: task fase HH:MM, …' with the state of each dispatch rodando, travado, não começado, parado or perguntando, and the delivered ones without release; '' if there is nothing.

    Uses the cache agents (aberto.json, slice 8) with the state redone from the log; a cache without `agents` falls back to the `in_progress` with the last phase.
    """
    now_at = now_at or datetime.now(timezone.utc)
    if isinstance((open_state or {}).get("agentes"), list):
        agent_rows = reassess(open_state["agentes"], events, now_at, turns)
        live = sorted((a for a in agent_rows if a["estado"] in ("travado", "limite", "nao_comecou", "parado", "perguntando", "rodando", "aguardando_integracao")), key=lambda a: a.get("prioridade") or 2)
        without_release = sum(a["estado"] == "entregue" and not a.get("retido") for a in agent_rows)
        item_list = []
        for a in live[:3]:
            item_name = f"{'P' + str(a['prioridade']) + ' ' if a.get('prioridade') else ''}{(a.get('task') or '?')[:9]}… "
            item_list.append(item_name + (f"STUCK for {(a.get('idade_s') or 0) // 60} min" + (f" ({a['motivo']})" if a.get("motivo") else "") if a["estado"] == "travado"
                                 else "plan LIMIT" if a["estado"] == "limite"
                                 else f"NOT STARTED for {(a.get('idade_s') or 0) // 60} min" if a["estado"] == "nao_comecou"
                                 else f"stopped at the prompt for {(a.get('idade_s') or 0) // 60} min" if a["estado"] == "parado" else "question" if a["estado"] == "perguntando"
                                 else f"waiting for integration of {a['integracao']['branch']}" if a["estado"] == "aguardando_integracao"
                                 else f"{a.get('fase') or '?'} {_hora_local(a['ultimo_heartbeat'])}" if a.get("ultimo_heartbeat") else "no heartbeat"))
            if a["estado"] == "rodando" and a.get("espera") and a["espera"] not in (a.get("fase") or ""):
                item_list[-1] += f" (waiting: {a['espera']})"
        return ((" Live: " + ", ".join(item_list) + (f" +{len(live) - 3}" if len(live) > 3 else "") + ".") if live else "") + \
            (f" Delivered, not released: {without_release} (orq agents)." if without_release else "")
    in_progress = (open_state or {}).get("andamento") or []
    if not in_progress:
        return ""
    signals = liveness_signals(events)
    item_list = []
    for a in in_progress[:3]:
        h = signals.get(a.get("dispatch"))
        item_list.append(f"{(a.get('task') or '?')[:9]}… " + (f"{h.get('fase') or '?'} {_hora_local(h.get('ts'))}" if h else "no heartbeat"))
    return " Live: " + ", ".join(item_list) + (f" +{len(in_progress) - 3}" if len(in_progress) > 3 else "") + "."


def _quote(text_value, n=40):
    t = " ".join((text_value or "").split())
    return t if len(t) <= n else t[: n - 1] + "…"


def recent_alerts(events, now_at, agents_=None):
    """Alerts from the last ALERT_H hours, one per (task, type): scout without a report and unread steer.

    Goes away when something later handles it: `orq alert seen_item <task>` (alerta_visto), the `orq release` of the task's dispatch or an intake that cites the task (B30).
    `agents_` (already reevaluated): the task whose agent is `liberado` also goes away, even if released outside `orq release` (B39); the unread steer goes away
    also with the worker `delivered` (the adjustment lost its meaning).
    """
    out = {}

    def resolve(task, only_one=None):
        for k in [k for k in out if k[0] == task and only_one in (None, k[1])]:
            del out[k]

    for e in events:
        if e.get("tipo") == "alerta" and (not e.get("ts") or (now_at - _dt(e["ts"])).total_seconds() < ALERT_H * 3600):
            out[(e.get("task"), e.get("alerta"))] = e
        elif e.get("tipo") in ("alerta_visto", "liberar"):
            resolve(e.get("task"))
        elif e.get("tipo") == "intake":
            resolve(e.get("ref"))
    for a in agents_ or []:
        if a.get("estado") == "liberado":
            resolve(a.get("task"))
        elif a.get("estado") == "entregue":
            resolve(a.get("task"), "steer_nao_lido")
    return list(out.values())


def open_steers(events, now_at):
    """{msg_id: {steer, tentativas, ultima}} of the steers still without an end (read, closed or alert) and within STEER_WINDOW_S.

    `attempts` are the notice retypes (steer_reentrega); `last_one` is the instant of the steer or of the last retype, from which the STEER_READ_S run.
    """
    out = {}
    for e in events:
        m = e.get("msg_id")
        if e.get("tipo") == "steer" and m and _ts(e.get("ts")):
            out[m] = {"steer": e, "tentativas": 0, "ultima": _ts(e["ts"])}
        elif e.get("tipo") == "steer_reentrega" and m in out and _ts(e.get("ts")):
            out[m]["tentativas"] += 1
            out[m]["ultima"] = _ts(e["ts"])
        elif e.get("tipo") == "steer_fim" or (e.get("tipo") == "alerta" and e.get("alerta") == "steer_nao_lido"):
            out.pop(m, None)
    return {m: s for m, s in out.items() if (now_at - _ts(s["steer"]["ts"])).total_seconds() < STEER_WINDOW_S}


def _reports(open_by_header):
    """Open report entries grouped by source (all worker_done in a single group), in the order they came in."""
    groups = {}
    for e in open_by_header:
        if e.get("origem") in ("relatorio", "relatorio_worker"):
            groups.setdefault("worker_done with report" if e["origem"] == "relatorio_worker" else e.get("fonte") or e["id"], []).append(e)
    return groups


def free_items(events, pending_items):
    """Headers of a still-open decision whose last answer was free text (closes nothing): the user may not have decided."""
    open_by_header = {i.get("id") for i in (pending_items or {}).get("itens", []) if i.get("tipo") == "decisao"}
    last_by_header = {}
    for e in events:
        if e.get("tipo") in ("resposta", "resposta_lavish") and e.get("header") in open_by_header:
            last_by_header[e["header"]] = e.get("livre")
    return [h for h, free in last_by_header.items() if free]


def last_free(events, id_):
    """Text of the last free answer given to header `id_` (AskUserQuestion or Lavish) since its last `pending add`; None if there isn't one.

    It's what `pending done` without --resposta sends to the gate: without it the unblocked worker receives "fechada sem resposta" and loses what the user wrote.
    """
    txt = None
    for e in events:
        if e.get("tipo") == "pend" and e.get("op") == "add" and e.get("pend") == id_:
            txt = None
        elif e.get("tipo") in ("resposta", "resposta_lavish") and e.get("header") == id_ and e.get("livre") and e.get("resposta"):
            txt = e["resposta"] + (f" (nota: {e['nota']})" if e.get("nota") else "")
    return txt


def gate_notice(gate, run):
    return f"gate {gate} of Run {run} stays pending until the coordinator commands the Run: {bind_tip(run)}"


def pending_gates(events):
    """[(gate, Run)] of already-closed decisions whose gate Orca has not yet resolved (no `gate_resolvido` in the log).

    A gate refused MAX_ATTEMPTS times is abandoned (reconcile_gates gives up on it): it stays out, because the line would have no possible action.
    aberto.json still lists Orca's pending gate.
    """
    refusals = [e.get("gate") for e in events if e.get("tipo") == "gate_falha"]
    resolved_tickets = {e.get("gate") for e in events if e.get("tipo") == "gate_resolvido"} | {g for g in refusals if refusals.count(g) >= MAX_ATTEMPTS}
    return list({e["gate"]: e.get("gate_run") for e in events
                 if e.get("tipo") == "pend" and e.get("op") == "done" and e.get("gate") and e["gate"] not in resolved_tickets}.items())


def recovered_cursor(cursor, now_at):
    """The cursor.json recovery mark (ts + copy) if it is from the last ALERT_H hours; otherwise None."""
    r = (cursor or {}).get("recuperado")
    if isinstance(r, dict) and r.get("ts") and (now_at - _dt(r["ts"])).total_seconds() < ALERT_H * 3600:
        return r
    return None


def recovered_notice(r):
    return f"cursor.json was unreadable: rebuilt from events.jsonl (copy at {r.get('copia')}); roles and ingest restarted."


def _avg_rounds(g):
    """Average duration, in seconds, of the last panel rounds (gerente.json `voltas_s`); 0 if there are none."""
    loops = [v for v in g.get("voltas_s") or [] if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0]
    return sum(loops) / len(loops) if loops else 0


def panel_interval_s(g):
    """How long the panel sleeps before the next round: the duration of the last round (gerente.json `voltas_s`), between PANEL_INTERVAL_S and
    PANEL_INTERVAL_MAX_S. A slow round leaves the panel working at most half the time; a fast round goes back to 10 s."""
    loops = [v for v in (g or {}).get("voltas_s") or [] if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0]
    return min(PANEL_INTERVAL_MAX_S, max(PANEL_INTERVAL_S, round(loops[-1]))) if loops else PANEL_INTERVAL_S


def panel_limit_s(g):
    """How old the `manager-alive` stamp can get before the panel counts as stopped: the larger of PANEL_LIMIT_MIN_S and PANEL_ROUNDS_X times the average round."""
    return max(PANEL_LIMIT_MIN_S, PANEL_ROUNDS_X * _avg_rounds(g))


def panel_notice(now_at=None):
    """Notice that the agent manager panel has stopped, or None: with the manager attached to this coordinator, Orca's notices go to the manager's
    terminal and only the panel relays them; the `manager-alive` stamp belongs to the panel's shell, so it holds even with orq.py broken (M19)."""
    g = _manager_cfg()
    if not g or g.get("coordenador") != os.environ.get("ORCA_TERMINAL_HANDLE"):
        return None
    try:
        age = (now_at or time.time()) - os.path.getmtime(_path(PANEL_ALIVE))
    except OSError:
        age = None
    if age is not None and age <= PANEL_STOPPED_S:
        return None
    ck = _dict(_read_json(_path(PANEL_CHECK)))
    if ck.get("morto") and ck.get("terminal") == g.get("gerente"):
        return (f"the agent manager terminal ({g['gerente']}) vanished from Orca: no worker notice arrives and the worker_done messages stay in the inbox. "
                "Bring it back up with: orq manager spawn")
    if age is not None and age <= panel_limit_s(g):  # the process is alive, the round is just slow (high load)
        media = _avg_rounds(g)
        return (f"slow panel ({media:.0f} s per loop)" if media else f"slow panel ({int(age)} s without a stamp)") + ": the process is alive, worker notices are delayed"
    if age is None:
        return f"agent manager panel has no stamp ({PANEL_ALIVE}): it did not start or runs the old script; no worker notice arrives meanwhile"
    return f"agent manager panel stopped for {int(age // 60)} min: no worker notice arrives; restart painel-agent-manager.sh in its terminal"


def check_manager_bg(now_at=None):
    """In the coordinator prompt: `manager-alive` stamp old (or missing) and the last check more than PANEL_STOPPED_S ago, asks Orca in the background
    whether the manager's terminal still exists (`orq manager checar`). The hook does not wait for Orca: the notice comes out on the next prompt, via panel_notice."""
    g = _manager_cfg()
    if not g or g.get("coordenador") != os.environ.get("ORCA_TERMINAL_HANDLE"):
        return
    now_at = now_at or time.time()
    try:
        if now_at - os.path.getmtime(_path(PANEL_ALIVE)) <= panel_limit_s(g):
            return
    except OSError:
        pass
    try:
        if now_at - os.path.getmtime(_path(PANEL_CHECK)) <= PANEL_STOPPED_S:
            return
    except OSError:
        _write_json(_path(PANEL_CHECK), {"ts": now_at, "terminal": g["gerente"], "morto": False})
    os.utime(_path(PANEL_CHECK), (now_at, now_at))  # marks the check already: consecutive prompts do not trigger another
    _orq_cli("gerente", "checar")


def _extra(events, all_listing, now_at, pending_items=None, cursor=None, open_state=None, turns=None, panel=None):
    """The single line for whatever is not a user message: panel stopped, cursor recovered, suspicious or free answer, scout alert and untriaged reports."""
    parts = [panel] if panel else []
    rec = recovered_cursor(cursor, now_at)
    if rec:
        parts.append("[notice] " + recovered_notice(rec))
    sus = suspects(events, now_at)
    if sus:
        parts.append("; ".join(f"suspect answer in {h}: confirm" for h in sus[:2]) + ", ask the question again.")
    liv = free_items(events, pending_items)
    if liv:
        parts.append("; ".join(f'free-text answer in {h}: if decided, close it with orq pend done {shlex.quote(h)} --answer "<what was decided>"' for h in liv[:2]) + ".")
    pending_gate_list = pending_gates(events)
    if pending_gate_list:
        parts.append("Gates of closed decisions still pending: " + "; ".join(f"{g} (Run {r}: {bind_tip(r)})" if r else g for g, r in pending_gate_list[:2])
                      + (f" +{len(pending_gate_list) - 2}" if len(pending_gate_list) > 2 else "") + ".")
    agent_rows = reassess((open_state or {}).get("agentes") or [], events, now_at, turns)
    for state, label in (("travado", "Stuck"), ("nao_comecou", "Not started"), ("parado", "Stopped at prompt")):
        parados = [a for a in agent_rows if a["estado"] == state]
        for a in parados[:2]:
            min_ = (a.get("idade_s") or 0) // 60
            detail = (f"{a.get('fase') or 'no phase'}, no heartbeat for {min_} min" + (f", {a['motivo']}" if a.get("motivo") else "") if state == "travado" else f"no turn {min_} min after the dispatch"
                       if state == "nao_comecou" else f"for {min_} min")
            parts.append(f"{label}: {a.get('task')} ({detail}): " + f'orq steer {a.get("task")} "<ajuste>" --run {a.get("run")}.')
        if len(parados) > 2:
            parts.append(f"+{len(parados) - 2} {label.lower()}.")
    for a in [a for a in agent_rows if a["estado"] == "limite"][:2]:
        parts.append(f"Plan limit: {a.get('task')} ({a.get('limite')}): no turn until the plan renews, a steer does not arrive.")
    alerts = recent_alerts(events, now_at, agent_rows)
    for a in alerts[:2]:
        if a.get("alerta") == "steer_nao_lido":
            parts.append(f"Alert: steer not read in {a.get('task')} after {STEER_ATTEMPTS} notices to the stopped terminal: check the worker (orq agents).")
            continue
        title = re.sub(r"^\s*\[scout\]\s*", "", a.get("titulo") or "", flags=re.I)
        parts.append(f"Alert: scout {_quote(title)!r} finished without reportPath ({(a.get('task') or '')[:14]}).")
    if len(alerts) > 2:
        parts.append(f"+{len(alerts) - 2} alerts.")
    prs = [e for e in all_listing if e.get("origem") == "pr"]
    if prs:
        parts.append("PR: " + "; ".join(f"{e['texto']} [{e['id']}]" for e in prs[:2]) + (f" +{len(prs) - 2}" if len(prs) > 2 else "") + ".")
    groups = _reports(all_listing)
    if groups:
        def group_name(source, es):
            ids = ",".join(e["id"] for e in es) if len(es) <= 3 else f"{es[0]['id']}-{es[-1]['id']}"
            paths = list(dict.fromkeys(e["caminho"] for e in es if e.get("caminho")))
            where = "/".join((paths[-1] if paths else "").split("/")[-2:])  # the one from the newest report
            return (f"{_quote(source, 38)} ×{len(es)} [{ids}]" + (f" {len(paths)} reports, the newest {where}" if len(paths) > 1
                                                                 else f" {where}" if where else ""))
        listing = [group_name(f, es) for f, es in list(groups.items())[:3]]
        parts.append("Reports not triaged: " + "; ".join(listing) + (f" +{len(groups) - 3} fontes" if len(groups) > 3 else "") + ".")
    line = " ".join(parts)
    line = line if len(line) <= 560 else line[:559] + "…"
    obligation_part = obligations_line(events)  # outside the 560 ceiling: it does not cut the earlier notices nor is it cut by them
    return " ".join(filter(None, [line, obligation_part]))


def _runs_line(open_state):
    """" Runs: <objetivo> (N abertas), ..." for the visible Runs in aberto.json; empty on the old cache, without the key."""
    rs = open_state.get("runs") or []
    return " Runs: " + ", ".join(f"{_quote(x['objetivo'], 30)} ({x['abertas']} open)" for x in rs[:3]) + (f" +{len(rs) - 3}" if len(rs) > 3 else "") + "." if rs else ""


NO_EFFECT_H = 24  # the hook's "Sem efeito" (No effect) line only brings entries from the last 24 h; the older ones show up in `orq status` (ticket 150)


def summary(events, open_state, pending_items, entry=None, now_at=None, cursor=None, turns=None, panel=None, include_old=False):
    """At most 5 lines: entry and what has no effect, one extra line (suspicion, alert, reports), open in Orca, pending items, how to give effect."""
    all_listing = open_entries(events)
    now_at = now_at or datetime.now(timezone.utc)
    without = [e for e in all_listing if e["id"] != (entry or {}).get("id") and e.get("origem", "usuario") == "usuario"]
    old_entries = [e for e in without if (t := _ts(e.get("ts"))) and (now_at - t).total_seconds() > NO_EFFECT_H * 3600]
    without = [e for e in without if e not in old_entries]
    who = f"entry {entry['id']} (user). " if entry else ""
    listing = ", ".join(f"{e['id']} ({_quote(e.get('texto'))!r})" for e in without[:3]) + (f" +{len(without) - 3}" if len(without) > 3 else "")
    l1 = f"[orq] {who}No effect: {listing or 'none'}."
    if include_old and old_entries:
        l1 += f" Over {NO_EFFECT_H} h old: " + ", ".join(f"{e['id']} ({_quote(e.get('texto'))!r})" for e in old_entries[:3]) + (f" +{len(old_entries) - 3}" if len(old_entries) > 3 else "") + "."
    extra = _extra(events, all_listing, now_at, pending_items, cursor, open_state, turns, panel)
    if open_state:
        bl = open_state["backlog"]
        old_value = f" ({bl[0]['id'][:9]}… {_quote(bl[0]['titulo'])!r}, {bl[0]['dias']} d)" if bl else ""
        failures = open_state.get("falhas") or []
        unread = f" {len(failures)} Run{'s' if len(failures) > 1 else ''} unread." if failures else ""
        l2 = (f"Open (cache from {_hora_local(open_state.get('ts'))}): backlog {len(bl)}{old_value}, running {open_state['rodando']}, "
              f"blocked {len(open_state['bloqueado'])}, gates {len(open_state['gates'])}.{unread}{alive_line(events, open_state, now_at, turns)}"
              f"{_runs_line(open_state)}")
    else:
        l2 = "Open: cache does not exist yet (refresh in progress)."
    include_all = (pending_items or {}).get("itens", [])
    item_list = [i for i in include_all if not pending_after(i, now_at.astimezone().date())]
    l3 = (f"With you: {len(item_list)} ({sum(i.get('tipo') == 'decisao' for i in item_list)} decisions)."
          + (f" Later: {len(include_all) - len(item_list)}." if len(include_all) > len(item_list) else ""))
    l4 = "Effect: orq intake <e> task <task>|steer <task>|pend <id>|decision <id>|conversation|discarded --note <reason>"
    return "\n".join([l1, *([extra] if extra else []), l2, l3, l4])


def _lim(item_list, n, fmt):
    """Up to n formatted items, plus `+k` for what is left over."""
    return [fmt(i) for i in item_list[:n]] + ([f"+{len(item_list) - n}"] if len(item_list) > n else [])


def last_from_user(events, now_at):
    """The stamp of the user's last message, not counting the request for the summary or digest itself (made less than SUMMARY_REQUEST_S ago); "" if there is none."""
    users = [e["ts"] for e in events if e.get("tipo") == "entrada" and e.get("origem", "usuario") == "usuario" and e.get("ts")]
    if users and 0 <= (now_at - _dt(users[-1])).total_seconds() < SUMMARY_REQUEST_S:
        users.pop()  # the prompt hook writes the request before the command runs: the window starts at the previous message (M17)
    return users[-1] if users else ""


def summary_four(events, open_state, pending_items, ts, since=None, now_at=None, panel=None):
    """`orq summary`: the four parts since `since` (default: the user's last message, not counting the request for the summary itself) and the decisions, up to ~20 lines.

    Com você (With you) = pending items outside Depois (Later); Entrou (Came in) = window entries and the effect of each; Anda (Moving) = running workers and their phase;
    Vem (Coming) = ready tickets (Blocked by all resolved) and blocked ones. `ts` are the tickets already read.
    """
    now_at = now_at or datetime.now(timezone.utc)
    since = since or last_from_user(events, now_at)
    in_window = [e for e in events if (e.get("ts") or "") >= since]
    effect = {e["entrada"]: e for e in events if e.get("tipo") == "intake"}
    item_list = [i for i in (pending_items or {}).get("itens", []) if not pending_after(i, now_at.astimezone().date())]
    has_entered = [e for e in in_window if e.get("tipo") == "entrada" and e.get("id")]
    running = [a for a in reassess((open_state or {}).get("agentes") or [], events, now_at) if a.get("estado") in ANDA]  # perguntando (asking) also moves (M17)
    resolved_tickets = {t["num"] for t in ts if t["status"] == STATUS_CLOSED}
    open_items = [t for t in ts if t["status"] not in (STATUS_CLOSED, STATUS_IN_PROGRESS)]
    ready = [t for t in open_items if set(t["blocked_by"]) <= resolved_tickets]
    stuck = [t for t in open_items if t not in ready]
    decision_lines = []
    for e in in_window:
        if e.get("tipo") in ("resposta", "resposta_lavish") and e.get("resposta"):
            decision_lines.append(f"{e.get('header') or e.get('item')}: {_quote(e['resposta'], 60)}")
        elif e.get("tipo") == "pend" and e.get("op") == "done" and e.get("resposta"):
            decision_lines.append(f"{e['pend']}: {_quote(e['resposta'], 60)}")

    def effect_of(e):
        i = effect.get(e["id"])
        return f"{e['id']} ({e.get('origem', 'usuario')}) {_quote(e.get('texto'), 40)!r} -> " + (f"{i['efeito']}{' ' + i['ref'] if i.get('ref') else ''}" if i else "no effect")

    def blocker_text(t):
        missing = [n for n in t["blocked_by"] if n not in resolved_tickets]
        return f"{t['num']} {_quote(t['titulo'], 40)} (waits on {', '.join(missing)})"

    def part(item_name, lst, empty, fmt, n=5):
        return [f"{item_name} ({len(lst)}):", *("  " + x for x in _lim(lst, n, fmt))] if lst else [f"{item_name}: {empty}"]

    deliveries = [f"{_quote(e.get('task'), 14)}: {a}" for e in in_window if e.get("tipo") == "entrega" for a in e.get("avisos") or []]
    upcoming = [f"ready: {t['num']} {_quote(t['titulo'], 40)}" for t in ready[:5]] + [f"blocked: {blocker_text(t)}" for t in stuck[:5]]
    return "\n".join([
        f"[orq summary] since {_hora_local(since) if since else 'the start'}",
        *([f"[notice] {panel}"] if panel else []),
        *part("With you", item_list, "nothing", lambda i: f"{i['id']}  {i.get('tipo')}  {_quote(i.get('titulo'), 50)}"),
        *part("In", has_entered, "nothing", effect_of, 6),
        *(part("Delivery without proof", deliveries, "", lambda x: x, 4) if deliveries else []),
        *part("Moving", running, "nobody running", lambda a: f"{_quote(a.get('titulo') or a.get('task'), 40)} [{(a.get('fase') or 'no phase') if a['estado'] == 'rodando' else a['estado']}]"),
        *(["Next:", *("  " + x for x in upcoming)] if upcoming else ["Next: no open ticket"]),
        *part("Decisions", decision_lines, "none", lambda d: d, 6),
    ])


def action_items(md):
    """Numbered items (list or table) from the report's action section; None if there is no section or it has no items.

    Accepts "Itens de ação" and the old titles "O que fazer hoje" and "O que precisa de ação".
    """
    item_list, inside, in_fence = [], False, False
    for line in md.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if re.match(r"#{1,6}(\s|$)", line):  # "#101" without a space is not a heading in Markdown
            if inside:
                break
            inside = bool(ACTION_TITLES.match(line.strip()))
            continue
        m = inside and (re.match(r"\s*\d+[.)]\s+(.+)", line) or re.match(r"\s*\|\s*\d+\s*\|\s*([^|]+?)\s*\|", line))
        if m:
            item_list.append(" ".join(re.sub(r"[*`]", "", m.group(1)).split())[:300])
    return item_list or None


def report_path(content, base, when_ms=None):
    """The .scratch/….md cited in the automation text, absolute (relative to `base`, the run's repo).

    With several, the one with the run's date (UTC or local) in its name wins; with no matching date, the last one cited.
    """
    cands = list(dict.fromkeys(re.findall(r"([^\s`'\"()]*\.scratch/[^\s`'\"()]+?\.md)", content or "")))
    if not cands:
        return None
    c = cands[-1]
    if when_ms:
        t = datetime.fromtimestamp(when_ms / 1000, timezone.utc)
        dates = {t.strftime("%Y-%m-%d"), t.astimezone().strftime("%Y-%m-%d")}
        c = next((x for x in reversed(cands) if any(d in os.path.basename(x) for d in dates)), c)
    return c if c.startswith("/") or not base else os.path.join(base, c)


# ---------- E/S ----------

def log(msg):
    try:
        os.makedirs(os.path.dirname(LOG), exist_ok=True)
        with open(LOG, "a") as f:
            f.write(f"{now()} {msg}\n")
    except OSError:
        pass


def _dt(ts):
    """Orca date ("2026-09-29 12:47:56", UTC with no timezone; accepts ISO with Z) as a timezone-aware datetime."""
    d = datetime.fromisoformat(ts.replace("Z", "+00:00").replace(" ", "T"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _hora_local(ts):
    """Local HH:MM of a UTC stamp; "?" if there is none."""
    try:
        return _dt(ts).astimezone().strftime("%H:%M")
    except (AttributeError, ValueError):
        return "?"


def days_since(ts, now_at):
    """Age in days of an Orca created_at."""
    return (now_at - _dt(ts)).days


def now():
    return os.environ.get("ORQ_AGORA") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # ORQ_AGORA: the tests' simulated clock


TIMEOUT_ORCA = float(os.environ.get("ORQ_ORCA_TIMEOUT") or 2.5)  # the tests start one Python process per call and use more (B31)


def orca(*args, timeout=TIMEOUT_ORCA, area="orchestration", without_terminal=False, acting_as=None, run=None, env_extra=None):
    """The only point that touches Orca (`orca <area> ...`). Returns `result` or raises RuntimeError.

    With without_terminal, Orca calls it from an unbound terminal (NO_BINDING): `worker-list` stops being scoped to a Run and lists all.
    Removing the variable does not work: without it Orca falls back to the active coordinator's Run (`scope.source` bound, checked on 29/09 against the real Orca).

    With the agent manager holding the command's Run (`run`, or the --run of a MUTA_RUN command), it attaches the manager to it first, under the lock:
    Orca binds one Run per terminal. `run-use` on the already-bound Run changes nothing. A Run the manager does not hold goes through the
    coordinator's own handle, the Run's owner outside the manager (M15)."""
    target = None if without_terminal or acting_as else run or _command_run(area, args)
    bound_run = target if target and _is_manager_run(target) else None
    if target and not bound_run and manager_runs():
        acting_as = os.environ.get("ORCA_TERMINAL_HANDLE")
    if bound_run:
        with manager_lock():
            _orca("run-use", "--id", bound_run, timeout=timeout, h=handle_orca())
            return _orca(*args, timeout=timeout, area=area, h=handle_orca(), env_extra=env_extra)
    return _orca(*args, timeout=timeout, area=area, h=NO_BINDING if without_terminal else acting_as or handle_orca(), env_extra=env_extra)


def _command_run(area, args):
    """The Run from the --run of a command that requires the bound terminal; otherwise None."""
    if area != "orchestration" or not args or args[0] not in MUTA_RUN or "--run" not in args:
        return None
    return args[args.index("--run") + 1]


def manager_runs():
    """The Runs of the agent manager attached by THIS coordinator; [] with no manager (the waiter reads everyone's inbox)."""
    g = _manager_cfg()
    return g["runs"] if g and g.get("coordenador") == os.environ.get("ORCA_TERMINAL_HANDLE") else []


def _is_manager_run(run):
    """Does the Run belong to an agent manager attached by THIS coordinator?"""
    g = _manager_cfg()
    return bool(g) and g.get("coordenador") == os.environ.get("ORCA_TERMINAL_HANDLE") and run in g["runs"]


def _own_run():
    """The Run bound to the coordinator's own terminal (with the manager attached, the `run-current` without `acting_as` is where the panel stopped, M15)."""
    return _current_run_id(acting_as=os.environ.get("ORCA_TERMINAL_HANDLE") if manager_runs() else None)


def coordinator_run(run, own=_UNKNOWN):
    """The coordinator commands the Run: its agent manager holds it, or it is the Run bound to the coordinator's own terminal (`own`, if the
    caller already asked Orca). The manager's `run-current` does not work, it changes on every panel round (M15)."""
    return _is_manager_run(run) or (_own_run() if own is _UNKNOWN else own) == run


def default_run(run=None):
    """The target Run of a command: `run`, otherwise the one bound to the coordinator (the manager's only one, if the own terminal holds none).
    A manager on more than one Run has no default Run: its `run-current` is picked by the panel's rotation (M15)."""
    if run:
        return run
    g = manager_runs()
    if len(g) > 1:
        raise ValueError(f"the agent manager holds {len(g)} Runs ({', '.join(g)}): pass --run")
    own = _own_run()
    if own and g and own not in g:  # the coordinator commands two Runs: without --run there is no obvious target (B50)
        raise ValueError(f"the coordinator commands {len(g) + 1} Runs ({', '.join([*g, own])}): pass --run")
    return own or (g[0] if g else None)


def bind_tip(run):
    """What to run so the coordinator commands the Run again: rebind the manager to it, or run-use when there is no manager."""
    g = _manager_cfg()
    if g and g.get("coordenador") == os.environ.get("ORCA_TERMINAL_HANDLE"):
        return f"run orq manager bind --terminal {g['gerente']} --run {run}"
    return f"run run-use --id {run}"


@contextlib.contextmanager
def _no_run(target):
    """The coordinator commands `target` inside the block: if it did not, orq does its `run-use` and, on exit, rebinds the Run that was bound (Orca binds
    one Run per terminal). With the agent manager attached nothing is switched: the coordinator's terminal owns the loose Runs and `run-use` would take it off the Run the
    manager holds; the error applies, with the hint of `orq manager ligar`, which the caller keeps. With no prior Run, `target` stays bound, as with a manual run-use."""
    if not target or coordinator_run(target) or manager_runs():
        yield
        return
    mine = os.environ.get("ORCA_TERMINAL_HANDLE")
    before = _own_run()
    orca("run-use", "--id", target, acting_as=mine)
    try:
        yield
    finally:
        if before and before != target:
            try:
                orca("run-use", "--id", before, acting_as=mine)
            except (RuntimeError, subprocess.TimeoutExpired) as e:
                log(f"run-use back to Run {before}: {type(e).__name__}: {e}")


INBOX_BODY = 160  # characters of a message body in `orq inbox`
INBOX_BATCHES = 20  # consecutive batches that an `orq inbox --ack` confirms per Run
INBOX_BODY_HOOK = 1500  # characters of a message body that the prompt hook injects into the context (ticket 182)


def _box_line(m):
    """An Orca message on one line: id, type, from whom, subject, truncated body and the summarized payload (k=v)."""
    p = _payload(m)
    summary = " ".join(f"{k}={v}" for k, v in p.items() if isinstance(v, (str, int, float, bool)))
    body_text = re.sub(r"\s+", " ", str(m.get("body") or ""))
    return " | ".join(x for x in (f"{m.get('id')} {m.get('type')} de {m.get('from_handle') or '?'}", m.get("subject") or "",
                                  body_text[:INBOX_BODY] + ("…" if len(body_text) > INBOX_BODY else ""), summary[:INBOX_BODY]) if x)


def _done_by_orq(msg_id, events):
    """What orq already did on its own with the message (ingest, tickets 171/141): the report entry, the integrator queue, the notice to it and the alerts."""
    done = []
    for e in events:
        if e.get("ref") == msg_id and e.get("origem") == "relatorio_worker":
            done.append("report entry created")
        if e.get("msg") != msg_id:
            continue
        if e.get("tipo") == "entrega_orq":
            done.append(f"ticket {e.get('ticket')} entered the integrator queue (branch {e.get('branch')}); notice to the integrator: {e.get('aviso')}")
        elif e.get("tipo") == "entrega" and e.get("avisos"):
            done.append("; ".join(e["avisos"]))
        elif e.get("tipo") == "alerta":
            done.append(f"alert {e.get('alerta')}")
    return done


def _box_block(m, events):
    """A whole message for the coordinator's context: header, body up to INBOX_BODY_HOOK characters, summarized payload, what orq already did and,
    on a worker question, the reply command ready to run."""
    p = _payload(m)
    summary = " ".join(f"{k}={v}" for k, v in p.items() if isinstance(v, (str, int, float, bool)))
    body_text = str(m.get("body") or "").strip()
    ln = [f"{m.get('id')} {m.get('type')} de {m.get('from_handle') or '?'}" + (f" | {m['subject']}" if m.get("subject") else "")]
    if body_text:
        ln.append("  " + body_text[:INBOX_BODY_HOOK].replace("\n", "\n  ") + ("…" if len(body_text) > INBOX_BODY_HOOK else ""))
    if summary:
        ln.append(f"  payload: {summary[:INBOX_BODY_HOOK // 5]}")
    ln += [f"  orq already did: {x}" for x in _done_by_orq(m.get("id"), events)]
    if m.get("type") == "question":
        ln.append(f'  reply with: orq reply {m.get("id")} "<text>"')
    return ln


def _run_box(run, coord_handle, ack, detail=False):
    """Reads (and with `ack` confirms) a Run's inbox in the same generation: binds the coordinator (its `--from`) to the Run, then check and ack.

    `detail` (the prompt hook, ticket 182): each message that is not a heartbeat comes whole (`_box_block`), after the ingest."""
    line_list, hb, n = [], 0, 0
    if _is_manager_run(run):  # o gerente segura o Run: o orca() o liga sob a trava
        acting_as = None
    else:
        acting_as = coord_handle
        orca("run-use", "--id", run, acting_as=acting_as)
    res = orca("check", "--run", run, acting_as=acting_as)
    for _ in range(INBOX_BATCHES):
        msgs = res.get("messages") or []
        if not res.get("deliveryId") or not msgs:
            break
        hb += sum(1 for m in msgs if m.get("type") == "heartbeat")
        if ack:
            ingest_mailbox(msgs)
        if detail:
            events = read_events()
            line_list += [l for m in msgs if m.get("type") != "heartbeat" for l in _box_block(m, events)]
        else:
            line_list += [_box_line(m) for m in msgs if m.get("type") != "heartbeat"]
        n += len(msgs)
        if not ack:
            break
        res = orca("check", "--run", run, "--ack", res["deliveryId"], acting_as=acting_as)
    cab = f"{run}: {n} mensagem(ns)" + (f", {hb} heartbeat(s)" if hb else "") + (", confirmadas" if ack and n else "")
    return [cab, *("  " + l for l in line_list)]


def inbox(run=None, ack=False, all_listing=False, detail=False):
    """`orq inbox [<run>] [--ack] [--all_listing]`: reads Orca's inbox, and with `ack` confirms it, in the consumer's same generation.

    The coordinator's terminal comes from orq's state (gerente.json) and only then from the env. `--all_listing` walks the Runs with unread messages (the inbox
    covers all). At the end the binding goes back to the Run the coordinator was on."""
    coord_handle = (_manager_cfg() or {}).get("coordenador") or os.environ.get("ORCA_TERMINAL_HANDLE")
    before = _current_run_id(acting_as=coord_handle)
    if all_listing:
        targets = list(dict.fromkeys(x["to_handle"][4:] for x in orca("inbox", "--limit", "200", acting_as=coord_handle)["messages"]
                                   if isinstance(x, dict) and str(x.get("to_handle")).startswith("run:") and not x.get("read")))
    else:
        targets = [run or before]
    if not all(targets):
        raise ValueError("no Run bound: pass the Run (`orq inbox <run>`)")
    output = []
    try:
        for r in targets:
            output += _run_box(r, coord_handle, ack, detail)
    finally:
        if before and any(r != before and not _is_manager_run(r) for r in targets):
            try:
                orca("run-use", "--id", before, acting_as=coord_handle)
            except (RuntimeError, subprocess.TimeoutExpired) as e:
                log(f"inbox: run-use back to Run {before}: {type(e).__name__}: {e}")
    return output or ["inbox empty"]


def _manager_cfg():
    """gerente.json as {coordenador, gerente, runs}; {} if missing or the wrong shape. The ticket 17 format (`run`) counts as runs=[run]."""
    g = _dict(_read_json(_path(MANAGER)))
    if not isinstance(g.get("gerente"), str):
        return {}
    runs = g["runs"] if isinstance(g.get("runs"), list) else [g["run"]] if g.get("run") else []
    return {**g, "runs": [r for r in runs if isinstance(r, str)]}


_MANAGER_LOCK = [0]  # lock depth in this process


@contextlib.contextmanager
def manager_lock():
    """Binding the manager to a Run, reading and confirming its inbox is a single section, across processes (panel, hooks, waiter, orq): Orca binds one Run per
    terminal and cancels the delivery (consumer_fenced) of whoever loses the binding between check and ack. Reentrant; without gerente.json it does not lock."""
    if _MANAGER_LOCK[0] or not os.path.exists(_path(MANAGER)):
        yield
        return
    with _lock("manager.lock"):
        _MANAGER_LOCK[0] = 1
        try:
            yield
        finally:
            _MANAGER_LOCK[0] = 0


def _orca(*args, timeout, h, area="orchestration", env_extra=None):
    """A call to the Orca binary with handle `h` in the ORCA_TERMINAL_HANDLE variable."""
    env = {**os.environ, "ORCA_TERMINAL_HANDLE": h} if h and h != os.environ.get("ORCA_TERMINAL_HANDLE") else None
    if env_extra:
        env = {**(env or os.environ), **env_extra}
    p = subprocess.run([ORCA, area, *args, "--json"], capture_output=True, text=True, timeout=timeout, env=env)
    out = json.loads(p.stdout)
    if not out.get("ok"):
        raise RuntimeError((out.get("error") or {}).get("message") or "orca failed")
    return out["result"]


def handle_orca():
    """The handle this terminal uses to talk to Orca: the agent manager's when this is the coordinator that attached it (`orq manager ligar`),
    otherwise its own. Orca identifies the caller by the ORCA_TERMINAL_HANDLE variable and only notifies (types "You have N orchestration message")
    the terminal bound to the Run; a terminal with no agent gets no notice at all."""
    mine = os.environ.get("ORCA_TERMINAL_HANDLE")
    g = _read_json(_path(MANAGER))
    return g["gerente"] if isinstance(g, dict) and mine and g.get("coordenador") == mine and g.get("gerente") else mine


# ---------- state in English on disk (phase 2 of the migration; plan in orq-ingles-plano.md, in the plan) ----------
# The code keeps reading and building the keys in pt; the ORQ_HOME disk stays in English. Every write goes through to_en and every read through
# to_pt, which accepts both formats: a stray pt event (a worker or an old branch) is still read. The same map serves scripts/migrar-ingles.py.
# A new key written in English cannot be a destination of this map (the read would swap it for the pt key); the map's test checks the fixture's keys.
KEYS_EN = {
    "conformidade": "conformance", "entrada_real": "real_entry", "esquecidos": "forgotten", "faltando": "missing", "plano": "plan",
    "abertas": "open", "aberto": "is_open", "aberto_em": "opened_at", "abertos": "open_list", "acao": "action", "aceita": "accepted",
    "achados": "findings", "acordado": "woken", "agente": "agent", "agente_de": "agent_from", "agente_para": "agent_to", "agentes": "agents",
    "alerta": "alert", "ambiente": "environment", "ambientes": "environments", "andamento": "in_progress", "anterior": "previous", "antigos": "old",
    "arquivo": "file", "arquivo_novo": "new_file", "ate": "until", "ativos": "active", "ausencia": "absence", "ausente": "away",
    "auto_desde": "auto_since", "avisa": "warn_at", "avisado": "notified", "aviso": "notice", "aviso_terminal": "terminal_notice", "avisos": "notices",
    "blocos": "blocks", "bloqueado": "blocked", "bloqueios": "blockers", "cache_criado": "cache_created", "cache_lido": "cache_read",
    "caixa": "inbox", "caminho": "path", "campos": "fields", "carga": "load", "carga_max": "max_load", "caros": "expensive", "max_caros": "max_expensive", "casas": "houses",
    "casos": "cases", "causa": "cause", "chave": "key", "checkout_em_uso": "checkout_in_use", "checkout_em_uso_do_orq": "orq_checkout_in_use",
    "chegada": "arrival", "ciclo": "cycle", "cinco_h": "five_h", "cinco_h_reset": "five_h_reset", "com_aviso": "with_notice", "comando": "command",
    "completadas": "finished", "concluidas": "completed", "confiadas": "trusted", "confirmado": "confirmed", "conjunto": "set",
    "conserto": "fix", "contexto": "context", "controle": "control", "controle_falhou": "control_failed", "coordenador": "coordinator",
    "copia": "copy", "corpo": "body", "correcao_do_usuario": "user_correction", "cortes": "cuts", "de": "from", "decisao": "decision",
    "declarada": "declared_as", "declarado": "declared", "depois": "after", "deps_faltando": "missing_deps", "desde": "since", "desfeito": "undone",
    "desistiu": "gave_up", "despacho": "dispatch_info", "despachos": "dispatches", "destino": "dest", "detalhe": "detail", "devolvida": "sent_back",
    "dia": "day", "dias": "days", "digita_prompt": "types_prompt", "disposicao": "disposition", "dormindo": "sleeping", "dormiu": "slept",
    "duracao_s": "duration_s", "efeito": "effect", "em": "at", "encerrado": "ended", "encerrados": "ended_list", "enter_separado": "separate_enter",
    "entrada": "entry", "entrada_sem_tratamento": "untreated_entry", "entrega": "delivery", "entrega_com_aviso": "delivery_with_notice",
    "entregas": "deliveries", "entregue": "delivered", "enviado": "sent", "erro": "error", "escrito_por": "written_by", "espera": "wait",
    "espera_motivo": "wait_reason", "esperam": "waiting", "estado": "state", "eventos": "events", "externa_min": "external_min", "falha": "failure",
    "falhas": "failures", "falhos": "failed_list", "falhou": "failed", "fase": "phase", "fechado": "closed", "fechado_em": "closed_at",
    "fechou": "closed_it", "feito": "done", "ficam": "stay", "ficaram": "stayed", "fila": "queue", "fila_e2e": "e2e_queue", "fila_tipo": "queue_type",
    "filho": "child", "filhos": "children", "fim": "end", "fim_dispatch": "dispatch_end", "fluxo": "flow", "fonte": "source", "fora": "outside",
    "fora_cpu": "out_cpu", "fora_por_cpu": "out_by_cpu", "fora_por_mem": "out_by_mem", "fora_rss_mb": "out_rss_mb", "frente": "stream",
    "gerado": "generated", "gerente": "manager", "gerente_volta": "manager_round", "grupo": "group", "grupos": "groups", "guardados": "kept",
    "hb_absorvido": "hb_absorbed", "hibernado": "hibernated", "hibernados": "hibernated_list", "hibernar_volta": "hibernate_round",
    "idade": "age", "idade_s": "age_s", "ignora_term": "ignores_term", "ini": "start_at", "iniciado": "started_at", "inicio": "begin",
    "instalado": "installed", "intake_descartado": "intake_discarded", "integracao": "integration", "interacao": "interaction",
    "intervalo": "interval_s", "intervencao": "intervention", "itens": "items", "leitura": "reading", "liberado": "released",
    "liberado_sem_push": "released_unpushed", "liberado_sujo": "released_dirty", "liberados": "released_list", "lido_em": "read_at",
    "ligada_em": "bound_at", "ligado": "on", "ligado_em": "linked_at", "limite": "limit", "limite_ts": "limit_ts", "limpeza": "cleanup",
    "limpeza_em_dias": "cleanup_days", "linha": "line", "linha_antes": "line_before", "linhas": "lines", "livre": "free", "livre_pct": "free_pct",
    "livre_pct_min": "free_pct_min", "livres": "free_slots", "longa": "long", "lote": "batch", "mantido": "kept_it", "maquina": "machine",
    "marcas": "marks", "mate_avisada_ate": "mate_notified_until", "max_despachos": "max_dispatches", "max_falhas": "max_failures",
    "mem_livre_mb": "mem_free_mb", "mem_livre_min_mb": "mem_free_min_mb", "mem_piso_mb": "mem_floor_mb", "mergeado": "merged",
    "metricas": "metrics", "minutos": "minutes", "modelo": "model", "modelos_caros": "expensive_models", "morto": "dead", "motivo": "reason",
    "motivo_hibernado": "hibernated_reason", "na_fila": "queued", "nao_antes": "not_before", "nao_comecou": "not_started", "nivel": "level",
    "noite": "night", "nome": "name", "nota": "note", "notificar_macos": "notify_macos", "novo": "new", "novo_dispatch": "new_dispatch",
    "numero": "number", "objetivo": "objective", "ocupado": "busy", "ocupado_digitado": "busy_typed", "ocupadas": "busy_list",
    "onde": "where", "opcao": "option", "opcoes": "options", "orca_yaml_estado": "orca_yaml_state", "orcamento": "budget", "origem": "origin",
    "outro_ambiente": "other_environment", "pacote": "package", "pagina": "page", "papeis": "roles", "papel": "role", "para": "to",
    "parada": "stopped_by", "parado": "stopped", "passagem": "handoff", "passo": "step", "passos": "steps", "pasta": "folder", "pausa": "pause",
    "pausados": "paused", "pausar_sob_pressao": "pause_under_pressure", "pedido": "request", "pedidos": "requests", "pend": "pending",
    "pend_tipo": "pending_type", "pendencias": "pendings", "pergunta": "question", "pergunta_de_worker": "worker_question",
    "perguntando": "asking", "perguntas": "questions", "ponta_remota": "remote_tip", "ponteiro": "pointer", "pontos": "points", "por": "why",
    "por_modelo": "by_model", "posicao": "position", "pr_ci_vermelho": "pr_ci_red", "pr_pediu_mudanca": "pr_changes_requested", "prazo": "deadline",
    "prefixos": "prefixes", "presa": "stuck_lock", "preservados": "preserved", "prioridade": "priority", "problema": "problem",
    "problemas": "problems", "processos": "processes", "producao": "production", "projeto": "project", "projeto_mate": "mate_project",
    "projetos": "projects", "pronto": "ready", "prontos": "ready_list", "prontos_avisados": "ready_notified", "prova": "proof",
    "proximo": "next", "pulados": "skipped", "recuperado": "recovered", "recusado": "refused", "registrado_no_orca": "registered_in_orca",
    "regra": "rule", "regra_violada": "broken_rule", "regras": "rules", "removidas": "removed_list", "removidos": "removed", "responde": "answers",
    "resolvido_em": "resolved_at", "resolvidos": "resolved_list", "resposta": "answer", "resposta_id": "answer_id", "resultado": "result",
    "resumo": "summary", "retido": "retained_it", "retomado": "resumed", "revisao": "review", "rodando": "running", "rotulo": "label",
    "rss_antes_mb": "rss_before_mb", "rss_depois_mb": "rss_after_mb", "rss_liberado_mb": "rss_freed_mb", "runs_isentos": "exempt_runs",
    "saida": "output", "segura": "holds", "sem_entrega": "no_delivery", "sem_push": "unpushed", "sem_task": "no_task",
    "sem_terminal": "no_terminal", "sem_ticket": "no_ticket", "semana": "week", "semana_avisa": "week_warn", "semana_pausa": "week_pause",
    "semana_reset": "week_reset", "servico": "service", "sessao": "session", "setup_fechados": "setup_closed", "sinais": "signals",
    "sinal": "signal", "spec_arquivo": "spec_file", "steer_nao_lido": "steer_unread", "steer_reentregue": "steer_redelivered",
    "steer_sem_leitura": "steer_no_read", "stop_bloqueia": "stop_blocks", "sugerido": "suggested", "sujo": "dirty", "task_fechada": "task_closed",
    "tela": "screen", "tela_ts": "screen_ts", "tentativa": "attempt", "tentativas": "attempts", "texto": "text", "tickets_orq": "orq_tickets",
    "tipo": "type", "tipo_mate": "mate_type", "titulo": "title", "transcrito": "transcript", "transcritos": "transcripts", "travado": "stuck",
    "turno": "turn", "turno_fim": "turn_end", "turno_inicio": "turn_begin", "turnos": "turns", "ultima": "last", "ultima_volta": "last_round",
    "ultimo_heartbeat": "last_heartbeat", "ultimo_poll": "last_poll", "uso": "usage", "usuario": "user", "valor": "value", "velha": "stale",
    "versao": "version", "visto": "seen", "vistos": "seen_list", "vivo": "alive", "vivos": "alive_list", "voltas_s": "rounds_s",
    "worktree_intacta": "worktree_intact",
}
TYPES_EN = {  # the event types; those already in English (pr, ok, info, intake, worker_done, ticket, steer, mate, doctor, backlog) stay
    "conformidade": "conformance", "fase_declarada": "phase_declared",
    "entrada": "entry", "obrigacao": "obligation", "steer_fim": "steer_end", "steer_reentrega": "steer_redelivered",
    "steer_digitado_ocupado": "steer_typed_busy", "retomada": "resumed", "pergunta_tela": "screen_question", "pergunta_tela_fim": "screen_question_end",
    "pend": "pending", "liberar": "release", "mate_entregue": "mate_delivered", "mate_pedido": "mate_request", "mate_reenvio": "mate_resent",
    "mate_escalado": "mate_escalated", "mate_dormiu": "mate_slept", "mate_acordou": "mate_woke", "integrar_fila": "integrate_queue",
    "despacho": "dispatch", "despacho_fila": "dispatch_queued", "alerta": "alert", "alerta_visto": "alert_seen", "uso_aviso": "usage_notice",
    "uso_parou": "usage_stopped", "run_projeto": "run_project", "resposta": "answer", "resposta_worker": "worker_answer",
    "resposta_suspeita": "suspect_answer", "resposta_lavish": "lavish_answer", "resposta_coordenador": "coordinator_answer", "prioridade": "priority",
    "pausa_plano": "plan_pause", "pausa_fim": "pause_end", "passagem": "handoff", "noite_ligar": "night_on", "noite_desligar": "night_off",
    "noite_parou": "night_stopped", "nao_iniciou": "not_started", "maquina_aviso": "machine_notice", "hibernar": "hibernate", "acordar": "wake",
    "heartbeat_visto": "heartbeat_seen", "heartbeat_absorvido": "heartbeat_absorbed", "gate_aviso": "gate_notice", "gate_falha": "gate_failed",
    "gate_falhou": "gate_blocked", "gate_resolvido": "gate_resolved", "fim_dispatch": "dispatch_end", "entrega": "delivery",
    "entrega_orq": "orq_delivery", "controle": "control", "ciclo": "cycle", "binding_perdido": "binding_lost", "ausente_ligar": "away_on",
    "ausente_desligar": "away_off", "coordenador_parou": "coordinator_stopped", "coordenador_retomou": "coordinator_resumed", "away_bloqueio": "away_block", "fila": "queue", "gerente": "manager", "servico_marcado": "service_marked",
    "processos": "processes", "resumo_add": "summary_add", "devolver": "send_back", "limite_tela": "screen_limit",
    "pendente_avisado": "pending_notified", "revisao_nm": "nm_review",
}
PENDING_EN = {"acao": "action", "decisao": "decision", "avisar": "notify"}
ESCALATION_EN = {"resposta": "answer", "decisao": "decision", "pr": "pr", "bloqueio": "blocker", "resumo": "summary"}
VALUES_EN = {  # key (in pt) -> the stored values that change; the rest (free text, ids, gh and Orca states) passes as is
    "tipo": {**TYPES_EN, **PENDING_EN, **ESCALATION_EN},
    "pend_tipo": PENDING_EN, "tipo_mate": ESCALATION_EN, "fila_tipo": {"despacho": "dispatch"},
    "efeito": {"tarefa": "task", "decisao": "decision", "conversa": "conversation", "descartado": "discarded", "pend": "pending", "lembrete": "reminder"},
    "parada": {"orcamento": "budget", "decisao": "decision", "limite": "limit"},  # `end --stopped-by`
    "motivo": {"entregue": "delivered", "falhou": "failed", "parou: orçamento": "stopped: budget", "parou: decisão pendente": "stopped: pending decision",  # the release's end_reason
               "parou: limite de uso": "stopped: usage limit", "sem worker_done": "no worker_done", "motivo desconhecido": "unknown reason"},
    # agents (AGENT_ORDER) and PRs (prs.json). `liberado` becomes `freed`: the liberar event writes Orca's `released` in `state`
    "estado": {"rodando": "running", "travado": "stuck", "perguntando": "asking", "entregue": "delivered", "liberado": "freed", "limite": "limit",
               "sem_terminal": "no_terminal", "nao_comecou": "not_started", "parado": "stopped", "aguardando_integracao": "awaiting_integration",
               "devolvida": "sent_back", "servico": "service", "hibernado": "hibernated", "encerrado": "ended",
               "aberto": "open", "mergeado": "merged", "fechado": "closed"},
}
KEYS_PT = {en: pt for pt, en in KEYS_EN.items()}
VALUES_PT = {k: {en: pt for pt, en in m.items()} for k, m in VALUES_EN.items()}
OLD_FILE = {  # new name -> pt name: _path uses the old one while the new one does not exist (before the migration). The locks change name without fallback
    "open.json": "aberto.json", "merge-queue.json": "fila.json", "dispatch-queue.json": "fila-despacho.json", "integrate-queue.json": "integrar-fila.json",
    "turns.json": "turnos.json", "manager.json": "gerente.json", "manager-alive": "gerente-vivo", "manager-notice.json": "gerente-aviso.json",
    "manager-check.json": "gerente-checagem.json", "manager-state.json": "gerente-estado.json", "active.json": "ativos.json",
    "machine.json": "maquina.json", "usage.json": "uso.json", "hibernate.json": "hibernar.json", "e2e-notice.json": "e2e-aviso.json",
    "worktrees-notice.json": "worktrees-aviso.json", "ask": "perguntar",
}
WORKER_FILES = {"HANDOFF.md": "PASSAGEM.md", "PAUSE.md": "PAUSA.md", "final-report.md": "relatorio-final.md"}  # in the worktree: orq reads both names


def _troca(obj, keys, new_values):
    if isinstance(obj, list):
        return [_troca(x, keys, new_values) for x in obj]
    if not isinstance(obj, dict):
        return obj
    out = {}
    for k, v in obj.items():
        vs = new_values.get(KEYS_PT.get(k, k))
        out[keys.get(k, k)] = vs.get(v, v) if vs and isinstance(v, str) else _troca(v, keys, new_values)
    return out


def to_en(obj):
    """Keys and values pt -> English (what goes to disk). What is already in English passes through unchanged."""
    return _troca(obj, KEYS_EN, VALUES_EN)


def to_pt(obj):
    """The inverse, for the code, which reads the keys in pt: accepts the line in English, in pt or mixed."""
    return _troca(obj, KEYS_PT, VALUES_PT)


def _is_orq_path(path):
    """Is the file orq state in ORQ_HOME, which goes to disk in English? The digest (digest-v1 contract, in pt) and the hook examples do not."""
    rel = os.path.relpath(os.path.abspath(path), os.path.abspath(HOME))
    return not rel.startswith(("..", "digest" + os.sep)) and not rel.endswith(".example.json")


def _path(item_name):
    p = os.path.join(HOME, item_name)
    old_name = OLD_FILE.get(item_name)
    if old_name and not os.path.exists(p) and os.path.exists(q := os.path.join(HOME, old_name)):
        return q
    return p


def worker_file(folder, item_name):
    """The file the worker writes in the worktree (HANDOFF.md, PAUSE.md, final-report.md), or the pt name an old worker still writes."""
    p = os.path.join(folder, item_name)
    old_name = os.path.join(folder, WORKER_FILES.get(item_name, item_name))
    return old_name if not os.path.exists(p) and os.path.exists(old_name) else p


def _read_json(path, default=None):
    try:
        with open(path) as f:
            d = json.load(f)
    except (OSError, ValueError):
        return default
    return to_pt(d) if _is_orq_path(path) else d


def _dict(x):
    """Hand-read JSON may have the wrong shape (null, list): whatever is not an object counts as missing."""
    return x if isinstance(x, dict) else {}


def _cursor_ro():
    """cursor.json for readers only (hooks, summary): always a dict, even with a wrong root. Whoever writes recovers the file in _read_cursor."""
    return _dict(_read_json(_path("cursor.json")))


def _sub(c, key_name):
    """c[key] as a writable dict: if it has another shape, it is started over."""
    if not isinstance(c.get(key_name), dict):
        c[key_name] = {}
    return c[key_name]


def _read_cursor():
    """cursor.json for whoever will rewrite it (with cursor.lock): missing is {}; unreadable becomes a copy and a restart (_recover_cursor)."""
    try:
        with open(_path("cursor.json")) as f:
            d = to_pt(json.load(f))
    except FileNotFoundError:
        return {}
    except ValueError as e:
        return _recover_cursor(str(e))
    return d if isinstance(d, dict) else _recover_cursor("the root is not an object")


def _recover_cursor(reason):
    """Keeps the unreadable cursor.json aside (cursor.json.corrompido-<hora>) and returns a new cursor with the `recuperado` mark.

    Without the `entry` counter, _write_event rebuilds the ids from events.jsonl (largest eN + 1); the ingest does not repeat what the log already has.
    Roles and Runs per session are lost: a worker session with a bound Run counts as a coordinator again until the next dispatch.
    """
    copy_file = "cursor.json.corrompido-" + re.sub(r"\D", "", now())
    os.replace(_path("cursor.json"), _path(copy_file))
    log(f"cursor.json unreadable ({reason}): copy at {copy_file}, cursor rebuilt from events.jsonl")
    return {"recuperado": {"ts": now(), "copia": copy_file}}


def _write_json(path, data, indent=None):
    """tmp + rename in the same directory: a reader (the panel's fs.watch) never sees a half-written file, and a failure leaves no tmp."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    import tempfile
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(to_en(data) if _is_orq_path(path) else data, f, ensure_ascii=False, indent=indent)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


@contextlib.contextmanager
def _lock(item_name):
    """Exclusive flock on HOME/<nome>. Fixed order when there are two: pend.lock, then cursor.lock."""
    os.makedirs(HOME, exist_ok=True)
    with open(_path(item_name), "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


@contextlib.contextmanager
def _no_alarm():
    """Holds off the hook ceiling's SIGALRM until the end of a two-step write (file and event); the alarm arrives afterwards.

    Covers only the write: the locks are taken before, outside here, so a stuck lock can still be cut by the ceiling.
    """
    before = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, before)


def read_events():
    out = []
    try:
        with open(_path("events.jsonl")) as f:
            for line in f:
                try:
                    out.append(to_pt(json.loads(line)))
                except ValueError:
                    pass
    except OSError:
        pass
    return out


def _write_event(ev, new_id=False):
    """Appends a line; cursor.lock already taken. With new_id, the next eN is max(cursor, largest id in the log) + 1:
    if cursor.json is lost (deleted or recovered), the ids do not restart and do not collide with old intakes."""
    if new_id:
        cur = _read_cursor()
        n = cur["entrada"] if isinstance(cur.get("entrada"), int) else None
        # the log is only scanned when the counter was lost (cursor erased): scanning 50 thousand lines on every entry would cost ~90 ms
        largest = 0 if n is not None else max(
            (int(m.group(1)) for e in read_events() if (m := re.fullmatch(r"e(\d+)", str(e.get("id") or "")))), default=0)
        cur["entrada"] = max(n or 0, largest) + 1
        _write_json(_path("cursor.json"), cur)
        ev["id"] = f"e{cur['entrada']}"
    ev = {"ts": now(), **ev}
    with open(_path("events.jsonl"), "a") as f:
        f.write(json.dumps(to_en(ev), ensure_ascii=False) + "\n")
    return ev


def append_event(ev, new_id=False):
    """Appends a line to events.jsonl under cursor.lock (sequential ids with new_id)."""
    with _lock("cursor.lock"):
        return _write_event(ev, new_id)


# ---------- aberto.json ----------

OPEN_STATUSES = ("ready", "pending", "dispatched", "blocked")


def run_summary(r, tasks, manager=()):
    """A Run as `orq runs` and aberto.json show it. `last_one` (last) is the most recent task activity (creation or completion), or the Run's
    creation: Orca's updated_at does not work, the panel itself touches it on every run-use."""
    dates = [d for d in (_ts(r.get("created_at")), *(_ts(t.get(k)) for t in tasks for k in ("created_at", "completed_at"))) if d]
    return {"id": r["id"], "objetivo": r.get("objective") or "", "abertas": sum(t.get("status") in OPEN_STATUSES for t in tasks),
            "concluidas": sum(t.get("status") == "completed" for t in tasks), "gerente": r["id"] in manager,
            "ultima": max(dates).strftime("%Y-%m-%dT%H:%M:%SZ") if dates else None}


def visible_runs(runs, now_at, include_all=False):
    """Runs with open work or with activity in the last RECENT_RUN_H h; test ones only with `include_all`."""
    def worth(x):
        last_one = _ts(x.get("ultima"))
        return x["abertas"] or (last_one and now_at - last_one < timedelta(hours=RECENT_RUN_H))
    return [x for x in runs if include_all or (worth(x) and not RUN_TESTE.search(x["objetivo"]))]


def build_open(data, now_at):
    """Pure: [(run, tasks, pending_gates)] of all Runs -> the open view (backlog, running, blocked, gates, failures).

    Cancelled (completed with result.cancelado) is completed and appears in nothing. deps_faltando are the deps not yet completed.
    A Run with broken data goes to `failures` and to the log; the others carry on.
    """
    ab = {"ts": now(), "backlog": [], "rodando": 0, "andamento": [], "bloqueado": [], "gates": [], "falhas": [], "runs": []}
    for r, tasks, gates in data:
        try:
            backlog, running, in_progress, is_blocked = [], 0, [], []
            done_ids = {t["id"] for t in tasks if t["status"] == "completed"}
            for t in tasks:
                item = {"id": t["id"], "run": r["id"], "objetivo": r.get("objective") or "",
                        "titulo": t.get("task_title") or _quote(t.get("spec"), 50), "dias": days_since(t["created_at"], now_at)}
                if t["status"] in ("ready", "pending"):
                    deps = t.get("deps") or []
                    deps = json.loads(deps) if isinstance(deps, str) else deps
                    backlog.append({**item, "deps_faltando": [d for d in deps if d not in done_ids]})
                elif t["status"] == "dispatched":
                    running += 1
                    in_progress.append({"task": t["id"], "dispatch": t.get("dispatch_id"), "run": r["id"], "titulo": item["titulo"]})
                elif t["status"] == "blocked":
                    is_blocked.append(item)
            gates_ = [{"id": g.get("id"), "run": r["id"], "task": g.get("task_id"), "objetivo": r.get("objective") or "",
                       "pergunta": g.get("question") or "", "dias": days_since(g["created_at"], now_at) if g.get("created_at") else 0}
                      for g in gates]
        except Exception as e:  # noqa: BLE001 - one broken Run does not take down the other Runs' cache
            log(f"refresh: Run {r.get('id')}: {type(e).__name__}: {e}")
            ab["falhas"].append(r.get("id"))
            continue
        ab["runs"].append(run_summary(r, tasks, manager_runs()))
        teste = RUN_TESTE.search(r.get("objective") or "")  # test Run, or task "[teste] ..." in a real Run: out of the backlog and the blocked list (B54)
        backlog = [i for i in backlog if not teste and not RUN_TESTE.search(i["titulo"])]
        is_blocked = [i for i in is_blocked if not teste and not RUN_TESTE.search(i["titulo"])]
        ab["backlog"] += backlog
        ab["rodando"] += running
        ab["andamento"] += in_progress
        ab["bloqueado"] += is_blocked
        ab["gates"] += gates_
    ab["backlog"].sort(key=lambda i: -i["dias"])
    ab["runs"] = visible_runs(ab["runs"], now_at)  # the rest stays in the archive: orq runs --todos
    return ab


def _all_runs():
    """run-list following nextCursor (Orca paginates); ponytail: cap of 50 pages of 100."""
    runs, cursor = [], None
    for _ in range(50):
        res = orca("run-list", "--limit", "100", *(["--cursor", cursor] if cursor else []), timeout=20)
        runs += res["runs"]
        cursor = res.get("nextCursor")
        if not cursor:
            break
    return runs


def runs_list(include_all=False):
    """`orq runs`: all of Orca's Runs (run-list + task-list), with the end-of-life filter, most recent first."""
    def by_run(r):
        return run_summary(r, orca("task-list", "--run", r["id"], timeout=20)["tasks"], manager_runs())
    with ThreadPoolExecutor(8) as ex:
        rs = list(ex.map(by_run, _all_runs()))
    return sorted(visible_runs(rs, datetime.now(timezone.utc), include_all), key=lambda x: x["ultima"] or "", reverse=True)


def runs_text(rs):
    """One line per Run: id, objective, open/completed, manager and last activity."""
    return "\n".join(f"{x['id']}  {_quote(x['objetivo'], 50)}  {x['abertas']} open/{x['concluidas']} done  "
                     f"{'in the manager' if x['gerente'] else 'outside the manager'}  last {_hora_local(x['ultima'])}" for x in rs) or "no Run with open work"


def refresh_open():
    """Reads all Runs (task-list + pending gates) and writes the cache. One refresh at a time; a Run that fails goes to `failures`."""
    os.makedirs(HOME, exist_ok=True)
    with open(_path("refresh.lock"), "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return None

        def by_run(r):
            try:
                tasks = orca("task-list", "--run", r["id"], timeout=20)["tasks"]
                gates = orca("gate-list", "--run", r["id"], "--status", "pending", timeout=20)["gates"]
                return r, tasks, gates
            except Exception as e:  # noqa: BLE001
                log(f"refresh: Run {r.get('id') if isinstance(r, dict) else r}: {type(e).__name__}: {e}")
                return r, None, None

        runs = _all_runs()
        with ThreadPoolExecutor(8) as ex:
            data = list(ex.map(by_run, runs))
        ab = build_open([d for d in data if d[1] is not None], datetime.now(timezone.utc))
        ab["falhas"] += [d[0].get("id") if isinstance(d[0], dict) else str(d[0]) for d in data if d[1] is None]
        try:
            ab["agentes"] = agents()  # without the released ones; if Orca fails, the cache is left without the key and the summary falls back to `in_progress`
        except Exception as e:  # noqa: BLE001
            log(f"refresh: agentes: {type(e).__name__}: {e}")
        ab["maquina"] = machine_panel(ab.get("agentes"))
        _write_json(_path("open.json"), ab)
        return ab


def refresh_bg(refresh=True):
    """ingest in the background; with refresh, also rebuilds aberto.json (150 Orca calls): the Orca notice only needs the inbox."""
    if os.environ.get("ORQ_NO_BG"):
        return
    subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "orq.py"), "ingest", *(["--refresh"] if refresh else [])], stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


# ---------- ingest: automation reports and worker_done with report ----------

def _cursor_mut(fn):
    """Reads cursor.json, applies fn(cursor) and writes, under the same flock as the entry ids."""
    with _lock("cursor.lock"):
        cur = _read_cursor()
        fn(cur)
        _write_json(_path("cursor.json"), cur)


def _turns_ro():
    """turnos.json for readers only: always a dict."""
    return _dict(_read_json(_path(TURNS)))


def _turns_mut(fn):
    """Reads turnos.json, applies fn(turnos) and writes under turnos.lock, unless fn returns False (nothing changed)."""
    with _lock("turns.lock"):
        turns = _turns_ro()
        if fn(turns) is not False:
            _write_json(_path(TURNS), turns)


def record_turn(kind, ev, harness="claude"):
    """Prompt and stop hooks of a worker session: records the start or end of that session's dispatch turn in turnos.json. Does not call Orca.

    The dispatch preamble carries the dispatch and the task and opens the record; the following prompts (steer, notice) reopen the session's newest one, and Stop closes it.
    A slash command is not a turn. With no known dispatch for the session nothing is written: its state stays `unknown`.
    """
    sid, now_at, prompt = ev.get("session_id") or "", now(), ev.get("prompt") or ""
    org = origin_name(prompt)
    if kind == "prompt" and org == "comando":
        return
    dispatch = task = None
    if kind == "prompt" and org == "despacho":
        d, t = ID_DISPATCH.search(prompt), ID_TASK.search(prompt)
        dispatch, task = d and d.group(1), t and t.group(1)
        if not dispatch:
            return  # preamble without a dispatch (pasted by hand, new format): the state stays unknown

    def write(turns):
        d = dispatch or next((k for k in reversed(turns) if _dict(turns[k]).get("sessao") == sid), None)  # the newest is the last in insertion order
        if d is None:
            return False
        if kind == "prompt":
            old_name = _dict(turns.pop(d, None))  # the pop moves the reopened dispatch to the end of the order
            turns[d] = {"task": task or old_name.get("task"), "sessao": sid, "inicio": now_at, "fim": None, "harness": harness,
                         **({"cwd": old_name.get("cwd") or ev.get("cwd")} if old_name.get("cwd") or ev.get("cwd") else {}),
                         **({"transcrito": ev["transcript_path"]} if ev.get("transcript_path") else {})}  # the cwd is the launch one: `claude --resume` only finds the session in it (ticket 48)
        else:
            turns[d]["fim"] = now_at
        cutoff = _ts(now_at) - timedelta(days=TURNS_DAYS)
        for k in [k for k, v in turns.items() if (_ts(_dict(v).get("inicio")) or cutoff) < cutoff]:
            del turns[k]

    _turns_mut(write)


def daily_report(base, created_ms):
    """The only .scratch/*/*.md with the run's date in its name and written between the run start and WAIT_REPORT_S after; None with zero or several.

    It is what is left when the automation text does not cite the file. ponytail: several candidates from the same day are not tie-broken.
    """
    if not base or not created_ms:
        return None
    t = datetime.fromtimestamp(created_ms / 1000, timezone.utc)
    dates = {t.strftime("%Y-%m-%d"), t.astimezone().strftime("%Y-%m-%d")}
    findings = []
    for file_path in glob.glob(os.path.join(glob.escape(base), ".scratch", "*", "*.md")):
        try:
            written = os.path.getmtime(file_path)
        except OSError:
            continue
        if any(d in os.path.basename(file_path) for d in dates) and created_ms / 1000 - 60 <= written <= created_ms / 1000 + WAIT_REPORT_S:
            findings.append(file_path)
    return findings[0] if len(findings) == 1 else None


def run_path(r):
    """The run's report file: the one cited in the snapshot (the one with the run's date, if several) or, with a snapshot that does not cite it, the day's."""
    snap = r.get("outputSnapshot") or {}
    base = (r.get("runContext") or {}).get("path")
    return report_path(snap.get("content"), base, snap.get("capturedAt") or r.get("createdAt")) \
        or (daily_report(base, r.get("createdAt")) if snap else None)


def run_entries(r):
    """One entry per action item in the run's report; with no section (or no file) it becomes an item "ler <arquivo>" (read <file>).

    None while the report does not exist: the terminal automation becomes `completed` before the agent writes the file (the snapshot arrives 6 to
    13 min later). With no snapshot, a snapshot that cites no file or a cited file that does not exist yet, the run waits WAIT_REPORT_S since
    createdAt; after that whatever exists counts, and the worst case is the item "ler o resultado do run" (read the run result).
    """
    source = r.get("title") or r["id"]
    path = run_path(r)
    too_early = time.time() * 1000 - (r.get("createdAt") or 0) < WAIT_REPORT_S * 1000
    item_list = None
    if path:
        try:
            with open(path) as f:
                item_list = action_items(f.read())
        except FileNotFoundError as e:
            if too_early:
                return None
            log(f"ingest: unreadable report {path}: {type(e).__name__}: {e}")
        except (OSError, ValueError) as e:  # ValueError inclui UnicodeDecodeError
            log(f"ingest: unreadable report {path}: {type(e).__name__}: {e}")
    elif too_early:
        return None
    read_value = f"read {os.path.basename(path)}" if path else "read the run result (no report file)"
    item_list = item_list or [read_value]
    if len(item_list) > MAX_ITEMS:
        item_list = item_list[:MAX_ITEMS] + [f"{read_value} ({len(item_list) - MAX_ITEMS} more items)"]
    return [{"tipo": "entrada", "origem": "relatorio", "texto": t, "fonte": source, "ref": r["id"], "item": i,
             **({"caminho": path} if path else {})} for i, t in enumerate(item_list, 1)]


def complete_reports(runs, event_list):
    """Run recorded with only the "ler o resultado" (read the result) item (the ingest saw it before the file existed): when the path shows up, the report's
    items come in and the old entry, if still open, is dropped. Returns how many runs were completed."""
    without, with_ref = {}, set()
    for e in event_list:
        if e.get("tipo") == "entrada" and e.get("origem") == "relatorio" and e.get("ref"):
            if e.get("caminho"):
                with_ref.add(e["ref"])
            else:
                without.setdefault(e["ref"], []).append(e.get("id"))
    closed_ids = {e.get("entrada") for e in event_list if e.get("tipo") == "intake"}
    n = 0
    for r in runs:
        try:
            if r.get("id") not in without or r["id"] in with_ref or r.get("status") != "completed":
                continue
            path = run_path(r)
            fresh_entries = run_entries(r) if path and os.path.isfile(path) else None
            if not fresh_entries:
                continue
            for e in fresh_entries:
                append_event(e, new_id=True)
            for id_ in without[r["id"]]:
                if id_ not in closed_ids:
                    append_event({"tipo": "intake", "entrada": id_, "efeito": "descartado", "nota": f"replaced by the items of {os.path.basename(path)}"})
            n += 1
        except Exception as e:  # noqa: BLE001 - a poisonous run does not lock the others
            log(f"ingest automations: completing {r.get('id') if isinstance(r, dict) else r}: {type(e).__name__}: {e}")
    return n


def ingest_automations():
    """New completed Run from any automation -> entries with origin relatorio.

    Does not repeat what the cursor has already seen or what events.jsonl itself already has (if the cursor is lost); `auto_desde` advances to the
    largest ingested stamp, so the list of seen ids needs no cap to hold.
    A Run whose report does not exist yet (run_entries returns None) is left for the next round and holds `auto_desde` back, like a failure.
    A Run already recorded with no path gets its items when the file appears (complete_reports).
    """
    ing = _read_cursor()["ingest"]
    since = _dt(ing.get("auto_desde") or ing["desde"]).timestamp()  # auto_desde moves; `since` is the fixed starting point (the inbox uses it too)
    seen = set(ing["runs"]) | {e.get("ref") for e in read_events() if e.get("tipo") == "entrada" and e.get("origem") == "relatorio"}
    fresh, largest, failed, tent = 0, since, False, dict(ing.get("tentativas") or {})
    include_all = sorted(orca("runs", area="automations", timeout=20)["runs"], key=lambda r: r.get("createdAt") or 0)
    event_list = read_events()
    for r in include_all:
        try:
            when = ((r.get("outputSnapshot") or {}).get("capturedAt") or r.get("createdAt") or 0) / 1000
            if r["id"] in seen or r.get("status") != "completed" or when <= since:
                continue
            entries = run_entries(r)
            if entries is None:
                failed = True
                continue
            for e in entries:
                append_event(e, new_id=True)
            _cursor_mut(lambda c, i=r["id"]: c["ingest"].__setitem__("runs", (c["ingest"]["runs"] + [i])[-200:]))
        except Exception as e:  # noqa: BLE001 - a poisonous run cannot lock the following ones
            key_name = f"auto:{r.get('id') or r.get('createdAt') if isinstance(r, dict) else r}"
            tent[key_name] = tent.get(key_name, 0) + 1
            log(f"ingest automations: run {key_name[5:]} ({tent[key_name]}/{MAX_ATTEMPTS}): {type(e).__name__}: {e}")
            failed = failed or tent[key_name] < MAX_ATTEMPTS  # auto_desde does not go past a run that is still going to be tried
            continue
        fresh += 1
        if not failed:
            largest = max(largest, when)
    if largest > since or tent != (ing.get("tentativas") or {}):
        def write(c):
            if largest > since:
                c["ingest"]["auto_desde"] = datetime.fromtimestamp(largest, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if tent:
                c["ingest"]["tentativas"] = {**(c["ingest"].get("tentativas") or {}), **tent}
        _cursor_mut(write)
    return fresh + complete_reports(include_all, event_list)


def _payload(m):
    p = m.get("payload")
    try:
        p = json.loads(p) if isinstance(p, str) else p
    except ValueError:
        p = None
    return p if isinstance(p, dict) else {}


class _Transient(Exception):
    """Orca did not respond (timeout, refusal, unreadable output): there is nothing wrong with the message and it comes back on the next round."""


SHA_RE = re.compile(r"\b(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])[0-9a-f]{7,40}\b")
PR_RE = re.compile(r"https://github\.com/[\w.-]+/[\w.-]+/pull/\d+")


def _git(repo, *args, timeout=15):
    """Output of git in `repo`, or None if it fails, does not exist or takes too long (no network: local git only)."""
    try:
        r = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        return None
    return r.stdout if r.returncode == 0 else None


def check_delivery(text_value, repos, pr_commits=None):
    """Warnings from a worker_done's delivery proof: cited commits that exist in none of `repos` and a dirty tree where the commit is.

    With no sha in the text there is nothing to prove (empty list). `pr_commits(url)` returns the PR's oids (None: no gh or no answer, and then it does not warn).
    The warning does not block the worker; it only marks the delivery.
    """
    shas = list(dict.fromkeys(SHA_RE.findall(text_value or "")))
    notices, dirty_list = [], []
    for sha in shas:
        where = next((r for r in repos if _git(r, "cat-file", "-e", sha + "^{commit}") is not None), None)
        if not where:
            notices.append(f"delivery without commit: {sha} does not exist in the worker repository")
        elif where not in dirty_list and _git(where, "status", "--porcelain").strip():
            dirty_list.append(where)
            notices.append(f"delivery without commit: dirty tree in {where}")
    for url in dict.fromkeys(PR_RE.findall(text_value or "")):
        oids = pr_commits(url) if pr_commits else None
        still_missing = [s for s in shas if oids is not None and not any(o.startswith(s) for o in oids)]
        if still_missing:
            notices.append(f"delivery without commit: PR {url} does not have {', '.join(still_missing)}")
    return notices


def _pr_commits(url):
    """The PR's commit oids via gh, or None if gh does not exist, fails or there is no network."""
    try:
        r = subprocess.run([GH, "pr", "view", url, "--json", "commits", "-q", ".commits[].oid"], capture_output=True, text=True, timeout=20)
    except (subprocess.TimeoutExpired, OSError):
        return None
    return r.stdout.split() if r.returncode == 0 else None


def _worker_repos(m, p):
    """The dispatch's worktree (worker-list) and then the ORQ_REPOS repositories (default: the orq clone)."""
    repos = []
    try:
        for w in _all_workers(m["run_id"]):
            if w.get("dispatchId") == p.get("dispatchId"):
                wt = ((w.get("resource") or {}).get("worktreeId") or "").split("::", 1)[-1]
                repos += [wt] if wt.startswith("/") else []
    except Exception as e:  # noqa: BLE001
        log(f"delivery: worker-list failed ({type(e).__name__}: {e}); ORQ_REPOS only")
    return repos + [r for r in os.environ.get("ORQ_REPOS", orqpaths.CODE).split(":") if r]


def _already_has_event(type_name, msg):
    """Does events.jsonl already have an event `type_name` for this message? It is the dedup for when `orq inbox` and the manager ingest the same worker_done (ticket 171)."""
    return any(e.get("tipo") == type_name and e.get("msg") == msg for e in read_events())


def _delivery_proof(m, p):
    """worker_done with a sha in the text -> `delivery` event with the warnings, if any."""
    if _already_has_event("entrega", m["id"]):
        return
    text_value = f"{m.get('subject') or ''}\n{m.get('body') or ''}"
    if not SHA_RE.search(text_value):
        return
    notices = check_delivery(text_value, _worker_repos(m, p), _pr_commits)
    if notices:
        append_event({"tipo": "entrega", "dispatch": p.get("dispatchId"), "task": p.get("taskId"), "run": m["run_id"], "msg": m["id"], "avisos": notices})


BRANCH_RE = re.compile(r"\b(?:feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)/[\w./-]*\w")


def _dispatch_worktree(run, dispatch):
    """The path of the dispatch's worktree in worker-list, or None (no row, no path or Orca failed)."""
    try:
        w = next((w for w in _all_workers(run) if w.get("dispatchId") == dispatch), None)
    except (RuntimeError, KeyError):
        return None
    wt = (((w or {}).get("resource") or {}).get("worktreeId") or "").split("::", 1)[-1]
    return wt if wt.startswith("/") else None


def _text_branch(text_value):
    """The first branch cited in the text that exists in an ORQ_REPOS repository (`git rev-parse --verify`), or None: `docs/design.md` is not a branch."""
    repos = [r for r in os.environ.get("ORQ_REPOS", orqpaths.CODE).split(":") if r]
    return next((b for b in dict.fromkeys(BRANCH_RE.findall(text_value)) if any(_git(r, "rev-parse", "--verify", "--quiet", f"refs/heads/{b}") is not None for r in repos)), None)


def _environment_branch(b):
    """`b` is the default branch with no remote or an environment branch declared in some project (`environments` of projects/<nome>.json): never a ticket's delivery."""
    return b == BRANCH_NO_REMOTE or any(b in (p.get("ambientes") or []) for p in projects().values())


def _orq_branch(b):
    """`b` if it is an orq working branch (exists in an ORQ_REPOS repository and is not an environment branch), otherwise None: a product's worktree does not enter the queue."""
    repos = [r for r in os.environ.get("ORQ_REPOS", orqpaths.CODE).split(":") if r]
    ok = b and not _environment_branch(b) and any(_git(r, "rev-parse", "--verify", "--quiet", f"refs/heads/{b}") is not None for r in repos)
    return b if ok else None


def _payload_branch(b):
    """The branch the worker declared in worker_done counts without checking the repository, except an environment one."""
    return b if b and not _environment_branch(b) else None


def _orq_wt_branch(number):
    """The branch of the `<ORQ_WT>/<ticket>` convention worktree (or `t<ticket>`), or None. An orq ticket is dispatched with `--worktree
    current`: the dispatch's worktree is the product's, and the one holding the delivery branch is this one."""
    root = WT_ROOT
    for item_name in dict.fromkeys((number, number.lstrip("0"), f"t{number}", f"t{number.lstrip('0')}")):
        d = os.path.join(root, item_name)
        if os.path.isdir(d) and (b := _orq_branch((_git(d, "branch", "--show-current") or "").strip())):
            return b
    return None


def _integrator_dispatch(events):
    """The `dispatch_mode` event of the service called integrador (the last one not released), or None."""
    services, released = _services(events), _released(events)
    return next((e for e in reversed(events) if e.get("tipo") == "despacho" and e.get("dispatch") in services and e["dispatch"] not in released
                 and "integrador" in (e.get("titulo") or "").lower()), None)


def _integrator_terminal(events):
    """The terminal of the dispatch service called integrador (the last one not released), or None."""
    d = _integrator_dispatch(events)
    return _dispatch_terminal(d.get("run"), d["dispatch"]) if d else None


def _orq_delivery(m, p):
    """worker_done `succeeded` of an orq ticket (the task is the `Task:` of an ISSUES ticket; the product's do not enter) with a branch in the payload or the text ->
    `integrate queue add` and a short notice typed into the integrator (branch, worktree and commit). The branch comes from the payload, the `<ORQ_WT>/<ticket>` worktree, the dispatch worktree's current branch and only
    last from the text; an environment branch and a branch outside ORQ_REPOS never enter, if it exists in the orq repository. With no branch: log and `delivery` event with a warning, the coordinator adds it by hand.
    The notice is typed once (busy: tries to queue it on the turn; if still not, only the queue remains, which the integrator reads in its cycle). Returns the ticket or None."""
    if p.get("outcome") != "succeeded" or not p.get("taskId") or _already_has_event("entrega_orq", m["id"]):
        return None
    t = next((t for t in tickets() if t.get("task") == p["taskId"]), None)
    if not t:
        return None
    text_value = f"{m.get('subject') or ''}\n{m.get('body') or ''}"
    wt = _dispatch_worktree(m.get("run_id"), p.get("dispatchId"))
    branch = (_payload_branch(p.get("branch")) or _orq_wt_branch(t["num"]) or (wt and _orq_branch((_git(wt, "branch", "--show-current") or "").strip()))
              or _text_branch(text_value))
    if not branch:
        log(f"orq delivery: ticket {t['num']} has no branch in worker_done {m['id']}; it stays out of the integrator queue")
        append_event({"tipo": "entrega", "dispatch": p.get("dispatchId"), "task": p.get("taskId"), "run": m["run_id"], "msg": m["id"],
                      "avisos": [f"delivery of ticket {t['num']} has no branch: it did not enter the integrator queue; `orq integrate queue add <branch> {t['num']}`"]})
        return None
    commit = p.get("commit") or next(iter(SHA_RE.findall(text_value)), None)
    ev = integrate_queue_add(branch, t["num"])
    notice = f"orq: ticket {t['num']} entered the queue. Branch {branch}" + (f", worktree {wt}" if wt else "") + (f", commit {commit[:8]}" if commit else "") + "."
    h = _integrator_terminal(read_events())
    r = (type_text(h, notice) if h else "sem_integrador")
    if r == "ocupado":
        r = type_text_busy(h, notice)
    append_event({"tipo": "entrega_orq", "ticket": ev["ticket"], "branch": branch, "worktree": wt, "commit": commit, "msg": m["id"], "dispatch": p.get("dispatchId"), "aviso": r})
    return ev["ticket"]


def _ingest_msg(m, since, already, titles, send=True):
    """One inbox message -> 1 if it became an entry. Raises whatever is wrong with the message; the caller isolates it.

    _Transient is an Orca failure (the task-list of the scout title), which merits a retry; the rest belongs to the message and is discarded.
    """
    if m.get("type") != "worker_done" or _dt(m["created_at"]) <= since or m["id"] in already:
        return 0
    p = _payload(m)
    _delivery_proof(m, p)
    try:
        if _delivery_conformance(m, p, send):  # an incomplete one went back to the worker: it does not enter the integrator queue (ticket 201)
            _orq_delivery(m, p)
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:  # the queue is an extra: the message's report is not lost because of it
        log(f"orq delivery: worker_done {m['id']}: {type(e).__name__}: {e}")
    if p.get("reportPath"):
        append_event({"tipo": "entrada", "origem": "relatorio_worker", "texto": m.get("subject") or "", "fonte": f"worker {m.get('subject') or ''}",
                      "caminho": p["reportPath"], "ref": m["id"], "run": m["run_id"], "task": p.get("taskId"), **_run_group(m["run_id"])}, new_id=True)
        return 1
    log(f"ingest: worker_done {m['id']} without reportPath (task {p.get('taskId')}, {m.get('subject')})")
    if p.get("outcome") != "succeeded":  # a scout that failed has no report and is not an alert
        return 0
    if m["run_id"] not in titles:
        try:
            tasks = orca("task-list", "--run", m["run_id"], timeout=20)["tasks"]
        except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:
            raise _Transient(f"{type(e).__name__}: {e}")
        titles[m["run_id"]] = {t["id"]: t.get("task_title") or "" for t in tasks}
    title = titles[m["run_id"]].get(p.get("taskId"), "")
    if "[scout]" in title.lower():
        append_event({"tipo": "alerta", "alerta": "scout_sem_relatorio", "task": p.get("taskId"), "run": m["run_id"], "msg": m["id"], "titulo": title})
    return 0


def _record_worker_done(m):
    """One `worker_done` event per message: who delivered, with what result and the subject. It is what the digest reads, without calling Orca."""
    p = _payload(m)
    append_event({"tipo": "worker_done", "msg": m["id"], "run": m.get("run_id"), "task": p.get("taskId"), "dispatch": p.get("dispatchId"),
                  "outcome": p.get("outcome"), "subject": m.get("subject") or ""})


def ingest_mailbox(msgs):
    """worker_done read by `orq inbox --ack` -> the same record and delivery as the manager's ingest, before the ack takes the message out of the inbox (ticket 171).

    Same lock as the ingest (waits, does not skip); the dedup by `msg` (worker_done, entrada, alerta, entrega, entrega_orq) lets the manager ingest again without duplicating.
    A failure on one message goes to the log and does not stop the ack."""
    os.makedirs(HOME, exist_ok=True)
    with open(_path("ingest.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        since = _dt(_dict(_read_cursor().get("ingest")).get("desde") or START)
        event_list = read_events()
        already = {e.get("ref") for e in event_list if e.get("origem") == "relatorio_worker"} | {e.get("msg") for e in event_list if e.get("tipo") == "alerta"}
        done_items = {e.get("msg") for e in event_list if e.get("tipo") == "worker_done"}
        for m in msgs:
            if m.get("type") != "worker_done" or not m.get("id") or not m.get("created_at"):
                continue
            try:
                if m["id"] not in done_items and _dt(m["created_at"]) > since:
                    _record_worker_done(m)
                _ingest_msg(m, since, already, {}, send=False)  # inside the prompt hook: the manager's ingest does the send-back
            except Exception as e:  # noqa: BLE001
                log(f"inbox: ingest of message {m.get('id')}: {type(e).__name__}: {e}")


def ingest_inbox():
    """New worker_done with reportPath -> `relatorio_worker` entry; scout without reportPath -> alert; the rest only in the log.

    Each message is isolated (a poisonous one goes to the log and the cursor moves past it); what is already in events.jsonl is not repeated.
    An Orca failure (_Transient) stops the round before the message, which comes back on the next; after MAX_ATTEMPTS it is discarded.
    """
    ing = _read_cursor()["ingest"]
    since, last_item, fresh, titles = _dt(ing["desde"]), ing["inbox_seq"], 0, {}
    advanced, tent = last_item, dict(ing.get("tentativas") or {})
    event_list = read_events()
    already = {e.get("ref") for e in event_list if e.get("origem") == "relatorio_worker"} | {e.get("msg") for e in event_list if e.get("tipo") == "alerta"}
    done_items = {e.get("msg") for e in event_list if e.get("tipo") == "worker_done"}  # the digest reads from here what each worker delivered
    reserves = {e.get("dispatch") for e in event_list if e.get("origem") == "relatorio-final"} - set(_sent_back(event_list))  # relatorio-final.md already delivered: the late worker_done does not repeat
    msgs = sorted((m for m in orca("inbox", "--limit", "200", timeout=20)["messages"] if isinstance(m.get("sequence"), int)),
                  key=lambda m: m["sequence"])
    if last_item and msgs and msgs[0]["sequence"] > last_item + 1:
        log(f"ingest inbox: window lost, messages {last_item + 1} to {msgs[0]['sequence'] - 1} dropped out of the last 200")
    for m in msgs:
        if m["sequence"] <= last_item:
            continue
        try:
            if m.get("type") == "worker_done" and _payload(m).get("dispatchId") in reserves:
                advanced = m["sequence"]
                continue
            if m.get("type") == "worker_done" and _dt(m["created_at"]) > since and m["id"] not in done_items:
                _record_worker_done(m)
                done_items.add(m["id"])
            fresh += _ingest_msg(m, since, already, titles)
        except _Transient as e:
            key_name = f"msg:{m.get('id')}"
            tent[key_name] = tent.get(key_name, 0) + 1
            if tent[key_name] < MAX_ATTEMPTS:
                log(f"ingest inbox: message {m.get('id')} postponed ({tent[key_name]}/{MAX_ATTEMPTS}): {e}")
                break
            log(f"ingest inbox: message {m.get('id')} discarded after {MAX_ATTEMPTS} attempts: {e}")
        except Exception as e:  # noqa: BLE001
            log(f"ingest inbox: message {m.get('id')} discarded: {type(e).__name__}: {e}")
        advanced = m["sequence"]
    if advanced > last_item or tent != (ing.get("tentativas") or {}):
        def write(c):
            c["ingest"]["inbox_seq"] = max(c["ingest"].get("inbox_seq", 0), advanced)
            if tent:
                c["ingest"]["tentativas"] = {**(c["ingest"].get("tentativas") or {}), **tent}
        _cursor_mut(write)
    return fresh


FINAL_REPORT = "final-report.md"


def ingest_final_reports():
    """A worker that wrote `final-report.md` in the worktree and has no worker_done (Orca refused the new handle) -> fallback worker_done + entry.

    Only counts with the worker's turn closed (Stop), the cwd recorded by the hook and the file written after the turn start and the ingest's starting point:
    one from an earlier dispatch in the same worktree does not count. The outcome is `succeeded` because the worker only writes the file when it finishes. Runs after the inbox,
    so a real worker_done, if it exists, wins. Returns the number of new entries."""
    event_list = read_events()
    sent_back_list = _sent_back(event_list)
    done_items = {e.get("dispatch") for e in event_list if e.get("tipo") == "worker_done"} - set(sent_back_list)  # sent back: the newest report counts as a new delivery
    runs = {e.get("dispatch"): e.get("run") for e in event_list if e.get("tipo") == "despacho"}
    since, fresh = _dt(_read_cursor()["ingest"]["desde"]), 0
    for dispatch, t in _turns_ro().items():
        t = _dict(t)
        if dispatch in done_items or not t.get("cwd") or not t.get("fim") or not _ts(t.get("inicio")):
            continue
        file_path = worker_file(t["cwd"], FINAL_REPORT)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(file_path), timezone.utc)
            title = next((l.lstrip("# ").strip() for l in open(file_path, encoding="utf-8", errors="replace").read().splitlines() if l.strip()), "")
        except OSError:
            continue
        if mtime <= max(since, _ts(t["inicio"]), _ts(sent_back_list.get(dispatch)) or since):
            continue
        run, msg = runs.get(dispatch), f"relatorio-final:{dispatch}" + (f":{int(mtime.timestamp())}" if dispatch in sent_back_list else "")
        subject = title or f"final report of worker {dispatch}"
        append_event({"tipo": "worker_done", "msg": msg, "run": run, "task": t.get("task"), "dispatch": dispatch, "outcome": "succeeded", "subject": subject, "origem": "relatorio-final"})
        append_event({"tipo": "entrada", "origem": "relatorio_worker", "texto": subject, "fonte": f"worker {subject}", "caminho": file_path, "ref": msg, "run": run,
                      "task": t.get("task"), **_run_group(run)}, new_id=True)
        with contextlib.suppress(Exception):  # the same check as a real worker_done (ticket 201); a failure only skips it
            _delivery_conformance({"id": msg, "subject": subject, "body": "", "run_id": run}, {"dispatchId": dispatch, "taskId": t.get("task"), "outcome": "succeeded", "reportPath": file_path})
        fresh += 1
    return fresh


def ingest():
    """Automations and inbox -> entries, and gates of already-closed pending items -> resolved. Returns (new entries, resolved gates), or None if
    another ingest is running; each source fails on its own and goes to the log."""
    os.makedirs(HOME, exist_ok=True)
    with open(_path("ingest.lock"), "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return None
        # first run: records the starting point, and only what comes after it enters
        _cursor_mut(lambda c: c.__setitem__("ingest", {"desde": START, "inbox_seq": 0, "runs": [], **_dict(c.get("ingest"))}))
        total = gates = 0
        for item_name, fn in (("automations", ingest_automations), ("inbox", ingest_inbox), ("relatorios finais", ingest_final_reports), ("gates", reconcile_gates)):
            try:
                if item_name == "gates":
                    gates = fn()
                else:
                    total += fn()
            except Exception as e:  # noqa: BLE001
                log(f"ingest {item_name}: {type(e).__name__}: {e}")
        return total, gates


# ---------- pendencias.json (sole writer) ----------

def _pending_from_backlog():
    """The live pending items of the backlog (repo `pending`, outside Done) in the format pendencias.json used to hold, in file order."""
    return [backlog.pending_from_item(i) for i in backlog.read_value(BACKLOG) if i["repo"] == "pend" and i["estado"] != "done"]


def _load_pending():
    """Items from pendencias.json (or from the backlog, with ORQ_BACKLOG); a missing file is empty, a broken file raises ValueError (is never overwritten)."""
    if BACKLOG:
        try:
            return {"itens": _pending_from_backlog()}
        except OSError as e:
            raise ValueError(f"backlog unreadable: {e}")
    try:
        with open(PENDING) as f:
            d = json.load(f)
    except FileNotFoundError:
        return {"itens": []}
    except ValueError as e:
        raise ValueError(f"pendencias.json unreadable: {e}")
    if not isinstance(d, dict) or not isinstance(d.get("itens"), list):
        raise ValueError("pendencias.json without the itens list")
    return d


def _pending_ro():
    """Like `_read_json(PENDING)` for readers only (summary, digest, night card): None if the source does not open."""
    if BACKLOG:
        try:
            return {"itens": _pending_from_backlog()}
        except OSError:
            return None
    return _read_json(PENDING)


def _pending_to_backlog(item):
    """(title, body, (reason, kind, until)) with which a pending item enters the backlog. Refuses a title the grammar would read as a tag and an id tasks-axi does not accept."""
    i = backlog.pending_to_item(item)
    if (p := backlog.problem_title(i["titulo"])) and not i["titulo"].startswith("-"):
        raise ValueError(p)
    if not backlog.ID_RE.match(i["id"]):
        raise ValueError(f"pending-item id in the backlog: letters, digits, . _ - ({i['id']!r})")
    if p:
        raise ValueError(p)
    return i["titulo"], i["corpo"], (i["hold"]["motivo"], i["hold"]["kind"], i["hold"]["until"])


def _backlog_add(item):
    """`add` + `hold` of a pending item. Without the hold it would be born without `until_at`: if it fails, the `add` is undone."""
    title, body_text, (reason, kind, until_at) = _pending_to_backlog(item)
    backlog.cli(BACKLOG, "add", item["id"], title, "--kind", item["tipo"], "--repo", "pend", *(["--body", body_text] if body_text else []))
    try:
        backlog.cli(BACKLOG, "hold", item["id"], "--reason", reason, "--kind", kind, *(["--until", until_at] if until_at else []))
    except backlog.BacklogError:
        backlog.cli(BACKLOG, "rm", item["id"])
        raise


def _backlog_recreate(item, old_name):
    """Replaces the id's record with a new one (`rm` + `add` + `hold`): it is the path for an id that was once a closed pending item and for an edit that empties the body,
    because `update` does not accept an empty `--body`. If the new record fails, the old one comes back (ponytail: `since` restarts today)."""
    _pending_to_backlog(item)  # refuses an invalid title and id before deleting anything
    backlog.cli(BACKLOG, "rm", item["id"])
    try:
        _backlog_add(item)
    except Exception:
        with contextlib.suppress(backlog.BacklogError, ValueError):
            _backlog_add(old_name)
        raise


def _write_backlog(before, after, note=None):
    """Applies through the CLI the difference between the live pending items before and after: vanished = `done` (with `note`), new = `add` + `hold`, changed = `update` + `hold`.

    A repeated `add` the CLI would accept silently, so the id is checked here: the id of a closed pending item restarts (`rm` + `add`), a ticket id or
    live pending item id is refused.
    """
    a, d = {i["id"]: i for i in before}, {i["id"]: i for i in after}
    include_all = {i["id"]: i for i in backlog.read_value(BACKLOG)}
    for id_ in a:
        if id_ not in d:
            backlog.cli(BACKLOG, "done", id_, "--no-prune", *(["--note", note] if note else []))
    for id_, item in d.items():
        old_value = include_all.get(id_)
        if id_ not in a:
            if old_value and not (old_value["repo"] == "pend" and old_value["estado"] == "done"):
                raise ValueError(f"pending item {id_} already exists")
            _backlog_recreate(item, item) if old_value else _backlog_add(item)
        elif item != a[id_]:
            title, body_text, (reason, kind, until_at) = _pending_to_backlog(item)
            if not body_text and old_value and old_value["corpo"]:
                _backlog_recreate(item, a[id_])
                continue
            backlog.cli(BACKLOG, "update", id_, "--title", title, "--kind", item["tipo"], *(["--body", body_text] if body_text else []))
            backlog.cli(BACKLOG, "hold", id_, "--reason", reason, "--kind", kind, *(["--until", until_at] if until_at else []))  # o hold novo troca o anterior


def _mutate_pending(fn, event=None):
    """Reads, applies fn(items) and writes with tmp + rename, all under flock: two orq at the same time do not lose an item.

    With `event(result)`, the event enters in the same step: both writes stay out of reach of the hook's alarm.
    With ORQ_BACKLOG the write is the difference applied by the tasks-axi CLI (the event's `answer_text` goes as a note on the `done` of what vanished) and pendencias.json is regenerated
    from the backlog for the panel; the write-and-event pair is no longer atomic (the ingest reconciles a gate left without `gate_resolvido`).
    """
    with contextlib.ExitStack() as stack:
        stack.enter_context(_lock("pending.lock"))
        if event:
            stack.enter_context(_lock("cursor.lock"))
        d = _load_pending()
        before = json.loads(json.dumps(d["itens"]))
        out = fn(d["itens"])
        ev = event(out) if event else None
        with _no_alarm():
            if BACKLOG:
                _write_backlog(before, d["itens"], (ev or {}).get("resposta"))
                d = {"itens": _pending_from_backlog()}
            d["atualizadoEm"] = datetime.now().astimezone().isoformat(timespec="seconds")
            _write_json(PENDING, d, indent=2)
            if ev:
                _write_event(ev)
    return out


def _resolve_gate(gate, resolution, run=None):
    """Resolves the Orca gate tied to a decision and records `gate_resolvido`; True if resolved. `run` is the gate's Run (`gate_run`): orca()
    binds the manager to it, or uses the handle of the coordinator that holds it, and the check that the coordinator commands it fits in the same lock.

    The failure goes to the log and does not undo the closing of the pending item. Without `gate_resolvido` in the log, the next ingest tries again
    (reconcile_gates), and that is what covers the 3 s alarm that fires after the write. An Orca refusal (gate already resolved, outside the
    bound Run) records `gate_falha`; after MAX_ATTEMPTS refusals the gate is no longer tried.
    """
    try:
        with manager_lock():
            res = orca("gate-resolve", "--id", gate, "--resolution", resolution, run=run)
    except RuntimeError as e:
        log(f"gate-resolve {gate}: refused: {e}")
        append_event({"tipo": "gate_falha", "gate": gate, "erro": str(e)})
        return False
    except Exception as e:  # noqa: BLE001
        log(f"gate-resolve {gate}: {type(e).__name__}: {e}")
        return False
    # the destination confirms receipt with status resolved; anything else is an ambiguous answer and fails closed (lesson #6169)
    status = ((res.get("gate") or res) if isinstance(res, dict) else {}).get("status")
    if status != "resolved":
        log(f"gate-resolve {gate}: ambiguous Orca response (status {status!r}), gate treated as not delivered")
        append_event({"tipo": "gate_falha", "gate": gate, "erro": f"status {status!r}, esperado 'resolved'"})
        return False
    append_event({"tipo": "gate_resolvido", "gate": gate})
    return True


def reconcile_gates():
    """`pending done` of a decision with a gate left without `gate_resolvido` (3 s alarm, Orca down): resolves now. Returns how many."""
    event_list = read_events()
    done_items = {e.get("gate") for e in event_list if e.get("tipo") == "gate_resolvido"}
    refusals = [e.get("gate") for e in event_list if e.get("tipo") == "gate_falha"]
    done_items |= {g for g in refusals if refusals.count(g) >= MAX_ATTEMPTS}
    n = 0
    for e in event_list:
        g = e.get("gate")
        if e.get("tipo") == "pend" and e.get("op") == "done" and g and g not in done_items:
            run = e.get("gate_run")
            with manager_lock():  # the check and the gate-resolve in the same connection: the panel switching Run in the middle would burn the attempts (M15)
                if run and not coordinator_run(run):  # Orca only resolves the gate of the Run the coordinator commands; the refusal would burn the attempts
                    continue
                done_items.add(g)
                n += _resolve_gate(g, e.get("resposta") or "closed without an answer", run)
    return n


def _current_run_id(acting_as=None):
    """Id of the Run bound to the calling terminal (or to handle `acting_as`), or None."""
    return (orca("run-current", acting_as=acting_as)["run"] or {}).get("id")


def pending_after(item, today=None):
    """Reason a pending item is in "Depois" (Later) (future date, waiting, aged) or None if it is live.

    A future `until_at` hides it; an `until_at` that is due or today brings the pending item back into view, even if waiting on someone or aged. A decision with a
    gate locks a task and never ages out of view.
    """
    today = today or datetime.now().date()
    if item.get("ate"):
        return f"until {item['ate']}" if item["ate"] > today.isoformat() else None
    if item.get("espera"):
        return f"waiting on {item['espera']}"
    if item.get("gate"):
        return None
    days = (today - datetime.fromisoformat(item["desde"]).date()).days if item.get("desde") else 0
    return f"stale for {days} d" if days > PENDING_AGE_DAYS else None


def pending_list(all_listing=False, today=None):
    """Lines of `orq pending listing`: the live ones, and with `all_listing` also those in Depois with the reason."""
    item_list = _load_pending()["itens"]
    line_list = []
    for after in ((False, True) if all_listing else (False,)):
        for i in item_list:
            reason = pending_after(i, today)
            if bool(reason) == after:
                line_list.append(f"{i['id']}  {i.get('tipo')}  {i.get('titulo')}" + (f"  [Later: {reason}]" if reason else ""))
    return line_list


def _validate_pending(id_, type_name, title, waiting=None, until_at=None, task=None):
    """The refusals of `pending add` and `pending edit` that do not depend on Orca."""
    if not id_ or not title:
        raise ValueError("pend add needs --id and --title")
    if type_name not in PENDING_TYPES:
        raise ValueError(f"invalid type: {type_name} (use {'|'.join(PENDING_TYPES)})")
    if type_name == "decisao" and len(id_) > ID_HEADER:
        raise ValueError(f"decision id has at most {ID_HEADER} characters to fit the AskUserQuestion header ({id_!r} has {len(id_)})")
    if type_name == "decisao" and waiting:
        raise ValueError("a decision has no waiting: waiting is a pending item that waits on a third party and does not become a question")
    if until_at:
        try:
            datetime.strptime(until_at, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"--until needs YYYY-MM-DD ({until_at!r})")
    if task and type_name != "decisao":
        raise ValueError("--task only applies to a decision (the gate blocks the task until the answer)")


def pending_add(id_, type_name, title, detail=None, workstream=None, link=None, command=None, waiting=None, task=None, until_at=None, run=None):
    """Appends a user pending item (panel format; `waiting` optional) and records the event.

    With `task`, the decision locks the task: creates the gate in Orca, in the task's Run (`run`, otherwise the coordinator's; default_run refuses the manager with
    several Runs), and stores the id in the pending item (the ask hook resolves it).
    """
    id_, title = (id_ or "").strip(), (title or "").strip()
    _validate_pending(id_, type_name, title, waiting, until_at, task)
    gate = gate_run = None
    if task:
        run = default_run(run)
        if not coordinator_run(run):  # gate-create takes no --run: outside what the coordinator commands it would be born in the wrong Run (B51)
            raise ValueError(f"the coordinator does not command Run {run}: {bind_tip(run)}")
        try:
            res = orca("gate-create", "--task", task, "--question", title, run=run)
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            raise ValueError(f"gate-create failed, the pending item was not created: {e}")
        g = res.get("gate") or res
        gate = g.get("id")
        if not gate:
            raise ValueError("gate-create did not return the gate id, the pending item was not created")
        gate_run = g.get("run_id") or g.get("runId") or res.get("run_id") or run  # the gate is born in the requested Run

    def add(item_list):
        if any(i.get("id") == id_ for i in item_list):
            raise ValueError(f"pending item {id_} already exists")
        item = {"id": id_, "tipo": type_name, "titulo": title}
        for k, v in (("detalhe", detail), ("frente", workstream)):
            if v:
                item[k] = v
        item["desde"] = datetime.now().date().isoformat()
        for k, v in (("link", link), ("comando", command), ("espera", waiting), ("ate", until_at), ("gate", gate), ("gate_run", gate_run), ("task", task)):
            if v:
                item[k] = v
        item_list.append(item)
        return item

    try:
        return _mutate_pending(add, lambda item: {"tipo": "pend", "op": "add", "pend": id_, "pend_tipo": type_name, "titulo": title,
                                               **({"gate": gate} if gate else {}), **({"gate_run": gate_run} if gate and gate_run else {})})
    except Exception:
        if gate:
            _resolve_gate(gate, "canceled: the pending item was not created", gate_run)
        raise


def backlog_state():
    """`orq backlog`: where the backlog is, whether the CLI is the version orq requires and how many items there are. Without ORQ_BACKLOG it says the pending items still live in pendencias.json."""
    if not BACKLOG:
        return {"backlog": "off (ORQ_BACKLOG empty): pending items stay in " + PENDING}
    item_list = backlog.read_value(BACKLOG)
    try:
        backlog.check_version(backlog.binary())
        cli = f"{backlog.VERSION} ok"
    except backlog.BacklogError as e:
        cli = f"refused: {e}"
    counts = {e: sum(1 for i in item_list if i["estado"] == e) for e in ("queued", "in_flight", "done")}
    return {"backlog": BACKLOG, "tasks-axi": cli, "itens": len(item_list), **counts, "live pending items": len(_pending_from_backlog()),
            "prontos": len([i for i in backlog.ready(item_list) if i["repo"] != "pend"]), "tickets read from the backlog": bool(BACKLOG_TICKETS)}


def backlog_mover(ticket_numbers, group_name):
    """`orq backlog mover NN... --group_name G` (M7): moves a linked set of tickets to the group's backlog through `tasks-axi mv`, which moves all or nothing and refuses to leave
    a dependency dangling (include the whole set). Only a ticket still in the queue (Queued) leaves; In flight and Done stay. The already-Done blocker of one that leaves loses the edge
    before the `mv` (the CLI would treat it as a dangling dependency), and the edge comes back if the `mv` fails. Returns {grupo, destino, tickets}."""
    cfg = groups().get(group_name)
    if cfg is None:
        raise ValueError(f"group {group_name} does not exist in {GROUPS_DIR}/")
    if not (BACKLOG and _tickets_in_backlog()):
        raise ValueError("the per-group backlog needs the tickets in the backlog (ORQ_BACKLOG and ORQ_BACKLOG_TICKETS)")
    destination = backlog_group(group_name, cfg)
    if os.path.abspath(destination) == os.path.abspath(BACKLOG):
        raise ValueError(f"the backlog of group {group_name} is this one: {BACKLOG}")
    item_list = {i["id"]: i for i in backlog.read_value(BACKLOG)}
    ids = []
    for n in dict.fromkeys(str(x).strip().zfill(2) for x in ticket_numbers):
        i = _item_of_ticket(n)
        if not i:
            raise ValueError(f"ticket {n} is not in this backlog ({BACKLOG})")
        if i["estado"] != "queued":
            raise ValueError(f"ticket {n} is {STATUS_IN_PROGRESS if i['estado'] == 'in_flight' else STATUS_CLOSED}: only what is still queued moves; In flight and Done stay")
        ids.append(i["id"])
    resolved_blocks = [(i, b) for i in ids for b in item_list[i]["bloqueios"] if b not in ids and item_list.get(b, {}).get("estado") == "done"]
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    toml = os.path.join(os.path.dirname(destination), ".tasks.toml")
    if not os.path.exists(toml):
        with open(toml, "w", encoding="utf-8") as f:
            f.write(backlog.TOML)
    for i, b in resolved_blocks:
        backlog.cli(BACKLOG, "unblock", i, "--by", b)
    try:
        backlog.cli(BACKLOG, "mv", *ids, "--to", destination)
    except backlog.BacklogError:
        for i, b in resolved_blocks:
            with contextlib.suppress(backlog.BacklogError):
                backlog.cli(BACKLOG, "block", i, "--by", b)
        raise
    append_event({"tipo": "backlog", "op": "mover", "grupo": group_name, "tickets": ids, "destino": destination})
    return {"grupo": group_name, "destino": destination, "tickets": ids}


def pending_edit(id_, **fields):
    """Fixes a live pending item: `title`, `detail`, `workstream`, `link`, `command`, `waiting` and `until_at` (empty text clears the field). The id, the type and the gate do not change.

    Records the `pending`/`edit` event with the field names. In the backlog, `update` swaps the title and body and `hold` carries the new `until_at`.
    """
    fields = {k: v for k, v in fields.items() if v is not None}
    if not fields:
        raise ValueError("pend edit needs at least one field (--title, --detail, --stream, --link, --command, --waiting, --until)")

    def edit(item_list):
        item = next((i for i in item_list if i.get("id") == id_), None)
        if not item:
            raise ValueError(f"pending item {id_} does not exist among the live ones")
        for k, v in fields.items():
            v = " ".join(v.split()) if k == "titulo" else v.strip()
            item.pop(k, None)
            if v:
                item[k] = v
        _validate_pending(id_, item.get("tipo"), item.get("titulo"), item.get("espera"), item.get("ate"))
        return item

    return _mutate_pending(edit, lambda item: {"tipo": "pend", "op": "edit", "pend": id_, "campos": sorted(fields)})


class DeliveryNotConfirmed(ValueError):
    """The Orca gate did not confirm receipt of the reply: the pending item stays open."""


def pending_done(id_, answer_text=None, current=_UNKNOWN, confirm=False):
    """Closes (removes) an open pending item and resolves its gate, if there is one. `answer_text` stays in the event and in the resolution.

    Without `answer_text`, the last free-form answer given to the header (AskUserQuestion or Lavish) applies: the user's text reaches the unblocked worker.
    The gate belongs to the Run it was born in (`gate_run`): if the coordinator does not command that Run, Orca would refuse it, so it does not try, and the returned
    item carries `notice`; the next ingest resolves it when the coordinator commands that Run again. `current` is the Run linked to the own terminal, if the caller already knows it.

    With `confirm`, the pending item with a gate only closes after Orca confirms the gate resolved (lesson #6169): a coordinator that does not command the
    Run, a refused gate or an ambiguous reply raise EntregaNaoConfirmada and nothing is removed or written, so running again retries.
    """
    answer_text = answer_text or last_free(read_events(), id_)
    if confirm:
        item = next((i for i in _load_pending()["itens"] if i.get("id") == id_), None)
        if item is None:
            raise ValueError(f"pending item {id_} does not exist in pendencias.json")
        if item.get("gate"):
            run = item.get("gate_run")
            with manager_lock():
                if run and not coordinator_run(run, current):
                    raise DeliveryNotConfirmed(gate_notice(item["gate"], run))
                if not _resolve_gate(item["gate"], answer_text or "closed without an answer", run):
                    raise DeliveryNotConfirmed(f"Orca did not confirm gate {item['gate']} of {id_}: the pending item stays open")

    def rm(item_list):
        for i, item in enumerate(item_list):
            if item.get("id") == id_:
                return item_list.pop(i)
        raise ValueError(f"pending item {id_} does not exist in {'the backlog' if BACKLOG else 'pendencias.json'}")

    item = _mutate_pending(rm, lambda it: {"tipo": "pend", "op": "done", "pend": id_, **({"resposta": answer_text} if answer_text else {}), **({"task": it["task"]} if it.get("task") else {}),
                                       **({"gate": it["gate"]} if it.get("gate") else {}),
                                       **({"gate_run": it["gate_run"]} if it.get("gate") and it.get("gate_run") else {})})
    if item.get("gate") and not confirm:
        run = item.get("gate_run")
        with manager_lock():  # check and resolve in the same call (M15)
            if run and not coordinator_run(run, current):
                log(f"pend done {id_}: {gate_notice(item['gate'], run)}")
                return {**item, "aviso": gate_notice(item["gate"], run)}
            _resolve_gate(item["gate"], answer_text or "closed without an answer", run)
    return item


# ---------- PR linked to the task ----------

_PR_STATE = {"aberto": "open", "mergeado": "✓", "fechado": "closed"}


def _pr_state(url):
    """{state, mergedAt, baseRefName} of the PR through gh, or None if gh does not exist, fails, times out or there is no network."""
    try:
        r = subprocess.run([GH, "pr", "view", url, "--json", "state,mergedAt,baseRefName,headRefName,title,body"], capture_output=True, text=True, timeout=PR_GH_S)
        d = json.loads(r.stdout) if r.returncode == 0 else None
    except (subprocess.TimeoutExpired, OSError, ValueError):
        return None
    return d if isinstance(d, dict) else None


def _pr_gh_list(urls):
    """{url: gh data} of the PRs in `urls`, with one `gh pr list` call per repository (never one per PR). A PR that the list does not return (older than the
    limit) falls back to `gh pr view`. gh down: the whole repository is left unread, without calling gh again per PR."""
    by_repo = {}
    for u in urls:
        by_repo.setdefault(u.rsplit("/pull/", 1)[0].removeprefix("https://github.com/"), []).append(u)
    seen = {}
    for repo, us in by_repo.items():
        try:
            r = subprocess.run([GH, "pr", "list", "--repo", repo, "--state", "all", "--limit", "300", "--json",
                                "url,state,mergedAt,mergeCommit,baseRefName,headRefName,title,mergeable,statusCheckRollup"], capture_output=True, text=True, timeout=PR_GH_S)
            listing = json.loads(r.stdout) if r.returncode == 0 else None
        except (subprocess.TimeoutExpired, OSError, ValueError):
            listing = None
        if not isinstance(listing, list):
            continue
        findings = {x.get("url"): x for x in listing if isinstance(x, dict)}
        for u in us:
            seen[u] = findings.get(u) or _pr_state(u)
    return seen


def _workflow_of_other_environment(workflow, base, flow_info):
    """True if the workflow name is the deploy of one of the project's environments (`flow_info`, from `project_flow`) that is not the PR's base. GitHub ties the check to the commit,
    and the same branch opens one PR per environment. The environment name in the workflow counts for that environment; "production" counts for the project's production branch."""
    item_name = (workflow or "").lower()
    targets = {a for a in flow_info["ambientes"] if a.lower() in item_name} | ({flow_info["producao"]} if "production" in item_name else set())
    return bool(base and targets and base not in targets)


def _gh_ci(seen_item, now_at, flow_info):
    """{mergeable, falhas, rodando, outro_ambiente, lido_em} of what gh saw of a PR, or None if the response carries neither CI nor mergeable (gh with no response, `pr view`).
    A workflow check from another environment (e.g. the test environment's on a PR to production) counts neither in falhas nor in rodando: the workflow becomes `outro_ambiente`, only if it failed."""
    if "mergeable" not in seen_item and "statusCheckRollup" not in seen_item:
        return None
    failures, running, other_item = [], [], []
    for c in seen_item.get("statusCheckRollup") or []:
        if not isinstance(c, dict):
            continue
        item_name = c.get("name") or c.get("context") or "?"
        if _workflow_of_other_environment(c.get("workflowName"), seen_item.get("baseRefName"), flow_info):
            if c.get("conclusion") in CHECK_FAILED and c["workflowName"] not in other_item:
                other_item.append(c["workflowName"])
            continue
        if c.get("__typename") == "StatusContext" or "context" in c:  # old status: only `state`
            check_state = c.get("state")
            failures += [item_name] if check_state in CHECK_FAILED else []
            running += [item_name] if check_state in ("PENDING", "EXPECTED") else []
        elif c.get("status") != "COMPLETED":
            running.append(item_name)
        elif c.get("conclusion") in CHECK_FAILED:
            failures.append(item_name)
    return {"mergeable": seen_item.get("mergeable") or "UNKNOWN", "falhas": failures, "rodando": running, "outro_ambiente": other_item, "lido_em": now_at}


def _gh_state(s):
    """`mergeado`, `closed` or None (open, or no response) for what gh returned."""
    return "mergeado" if s.get("state") == "MERGED" or s.get("mergedAt") else "fechado" if s.get("state") == "CLOSED" else None


def _prs_ro():
    d = _dict(_read_json(_path(PRS)))
    return {**d, "itens": [i for i in d.get("itens") or [] if isinstance(i, dict) and i.get("task") and i.get("url")]}


def _mutate_prs(fn):
    """Reads prs.json, applies fn(data) and writes with tmp + rename, under pr.lock. Only commands and the panel write: the hooks never do."""
    with _lock("pr.lock"):
        d = _prs_ro()
        out = fn(d)
        _write_json(_path(PRS), d, indent=2)
    return out


def pr_next(item_list, flow_info):
    """The feature's next environment, only as a suggestion (`ready for <environment>`, or `in <production>` at the end), or None.

    `flow_info` is the flow of the feature's project (`project_flow`). It comes from the merged PR furthest along the environments up to production. With a PR of the feature
    still open, the next one is already on its way: None. With every environment before production merged and no PR open or in production, the notice says
    that only the production one is missing. A direct flow only counts production: it never asks for a PR for another environment. It never opens the PR.
    The `merge/<feature>-<environment>` PR has the environment as its base and counts as an entry in it."""
    if any(i["estado"] == "aberto" for i in item_list):
        return None
    prod = flow_info["producao"]
    envs = [prod] if flow_info["fluxo"] == "direto" else flow_info["ambientes"][:flow_info["ambientes"].index(prod) + 1]
    has_entered = {i["base"] for i in item_list if i["estado"] == "mergeado" and i.get("base") in envs}
    if not has_entered:
        return None
    if prod in has_entered:
        return f"in {prod}"
    last_item = max(envs.index(b) for b in has_entered)
    if last_item == len(envs) - 2 and envs[0] in has_entered:
        return f"ready for {prod} ({' and '.join(envs[:-1])} entered: open the one for {prod})"
    return f"ready for {envs[last_item + 1]}"


def _pr_segment(i):
    return f"#{i.get('numero')} {i.get('base') or '?'} {_PR_STATE.get(i.get('estado'), i.get('estado'))}"


def pr_link(task, url, issue=None, tag=None, note=None, head=None):
    """Links a PR to the task (the same feature has one per environment). Queries gh once, without insisting: with no response the PR enters `open_state` and without a base,
    and the poll completes it. A PR already merged or closed enters resolved and notified: whoever links it already knows. It does not check the task in Orca (no network here)."""
    if not re.fullmatch(r"task_\w+", task or ""):
        raise ValueError(f"invalid task: {task!r} (use the id, task_…)")
    if not PR_RE.fullmatch(url or ""):
        raise ValueError(f"invalid PR URL: {url!r} (https://github.com/<org>/<repo>/pull/<n>)")
    seen_item = _pr_state(url) or {}

    def add(d):
        already = next((i for i in d["itens"] if i["url"] == url), None)
        if already:
            raise ValueError(f"PR #{already.get('numero')} is already linked to task {already['task']}")
        state = _gh_state(seen_item) or "aberto"
        item = {"task": task, "url": url, "numero": int(url.rsplit("/", 1)[1]), "base": seen_item.get("baseRefName"), "estado": state,
                **({"head": head or seen_item["headRefName"]} if head or seen_item.get("headRefName") else {}), "ligado_em": now(), "avisado": state != "aberto", **({"resolvido_em": now()} if state != "aberto" else {}),
                **({"issue": int(issue)} if issue else {}), **({"titulo": seen_item["title"]} if seen_item.get("title") else {}),
                **({"tag": tag} if tag else {}), **({"nota": note} if note else {}),
                **({"por": _first_paragraph(seen_item["body"])} if (seen_item.get("body") or "").strip() else {})}
        d["itens"].append(item)
        d["sem_task"] = [x for x in d.get("sem_task") or [] if x.get("url") != url]
        append_event({"tipo": "pr", "op": "ligar", "task": task, "url": url, "numero": item["numero"], "base": item["base"], "estado": state,
                      **({"issue": item["issue"]} if issue else {})})
        return item

    item = _mutate_prs(add)
    _close_next(item)
    return item


def _first_paragraph(text_value, limit=120):
    """The first paragraph of the PR body on one line, cut at `limit` characters."""
    p = " ".join((text_value or "").strip().split("\n\n", 1)[0].split())
    return p if len(p) <= limit else p[: limit - 1].rstrip() + "…"


def queue_auto(item):
    """Puts the newly linked PR in the merge queue. Task with an open step (some PR of hers still open, of the same group: the project's production or the other environments):
    the PR joins it. With no step, opens one: the dispatch name (or the PR title), and the "por" (why) is the PR body or, in production, whoever already entered."""
    task = item["task"]
    flow_info = task_flow(task)
    main = item.get("base") == flow_info["producao"]
    dispatch_events = [e for e in read_events() if e.get("tipo") == "despacho" and e.get("task") == task and e.get("titulo")]
    siblings = [i for i in _prs_ro()["itens"] if i["task"] == task and i["url"] != item["url"] and (i.get("base") == flow_info["producao"]) == main]

    def poe(d):
        own_items = {i["numero"] for i in siblings}
        found_item = next((p for p in d["passos"] if not p["feito"] and own_items & set(p["prs"])), None)
        if found_item:
            found_item["prs"] = list(dict.fromkeys([*found_item["prs"], item["numero"]]))
            append_event({"tipo": "fila", "op": "auto", "passo": found_item["passo"], "prs": found_item["prs"]})
            return found_item
        item_name = dispatch_events[-1]["titulo"] if dispatch_events else item.get("titulo") or f"PR #{item['numero']}"
        previous = [i for i in _prs_ro()["itens"] if i["task"] == task and i.get("base") in flow_info["ambientes"] and i.get("base") != flow_info["producao"] and i["estado"] != "aberto"]
        did_enter = [a for a in flow_info["ambientes"] if a in {i["base"] for i in previous}]
        by = f"{' and '.join(did_enter)} already entered ({', '.join('#' + str(i['numero']) for i in previous)})" if main and previous else item.get("por", "")
        new = {"passo": max([p["passo"] for p in d["passos"]], default=0) + 1, "nome": f"{item_name} for {flow_info['producao']}" if main else item_name, "por": by, "prs": [item["numero"]], "feito": False}
        d["passos"].append(new)
        append_event({"tipo": "fila", "op": "auto", "passo": new["passo"], "nome": new["nome"], "prs": new["prs"]})
        return new

    return _mutate_queue(poe)


def orphan_pr(url, head=None):
    """Stores the PR whose branch has no known task in the `sem_task` list (`orq status` shows it until someone links it). PR already known: None."""
    if not PR_RE.fullmatch(url or ""):
        raise ValueError(f"invalid PR URL: {url!r} (https://github.com/<org>/<repo>/pull/<n>)")

    def add(d):
        if any(i["url"] == url for i in d["itens"]) or any(x.get("url") == url for x in d.get("sem_task") or []):
            return None
        item = {"url": url, "numero": int(url.rsplit("/", 1)[1]), "head": head, "em": now()}
        d["sem_task"] = [*(d.get("sem_task") or []), item]
        append_event({"tipo": "pr", "op": "sem_task", "url": url, "numero": item["numero"], "head": head})
        return item

    return _mutate_prs(add)


PR_DISPATCHES = 12  # how many recent dispatches pr_auto checks by worktree (one worker-show each)


def _worker_path(res):
    """The worktree path from a `worker-show`: the terminal's or, without it, that of the `worktreeId` (`<repo>::<path>`)."""
    wid = _deep_get(res, "worker", "worktreeId")
    return _deep_get(res, "terminal", "worktreePath") or (wid.split("::", 1)[1] if isinstance(wid, str) and "::" in wid else None)


def named_task(head, events, prs):
    """The task that owns branch `head` without asking Orca or the network, or None: by the `--name` of the new worktree (with or without the user prefix Orca adds,
    on either side), or by a PR of the same branch that is already linked (the PR to production after the ones to the other environments). Several tasks: the newest."""
    if not head:
        return None
    mine = _no_user_prefix(head)
    for e in (e for e in reversed(events) if e.get("tipo") == "despacho" and e.get("task") and e.get("nome")):
        if mine == _no_user_prefix(e["nome"]) or head.endswith("/" + e["nome"]):
            return e["task"]
    return next((i["task"] for i in reversed((prs or {}).get("itens") or []) if i.get("head") and _no_user_prefix(i["head"]) == mine), None)


def branch_task(head, wt, events, prs=None):
    """The dispatch task that owns branch `head`, or None. First by name or by a PR of the same branch (named_task, no network); then by the worktree where the
    branch is (`wt`), which Orca reports in worker-show, among the PR_DISPATCHES most recent dispatches. Two tasks in the same worktree (`current`): the newest."""
    dispatch_events = [e for e in reversed(events) if e.get("tipo") == "despacho" and e.get("task")]
    if task := named_task(head, events, prs):
        return task
    target = os.path.realpath(wt) if wt else None
    for e in dispatch_events[:PR_DISPATCHES] if target else []:
        try:
            path = _worker_path(orca("worker-show", "--dispatch", e["dispatch"], timeout=10))
        except Exception:  # noqa: BLE001 - a dispatch Orca no longer knows does not block the others
            continue
        if path and os.path.realpath(path) == target:
            return e["task"]
    return None


def _pr_head(url):
    """The PR's source branch through `gh pr view`, or None (gh down, PR that does not exist)."""
    try:
        r = subprocess.run([GH, "pr", "view", url, "--json", "headRefName"], capture_output=True, text=True, timeout=PR_GH_S)
        return (json.loads(r.stdout).get("headRefName") or None) if r.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired, ValueError, AttributeError):
        return None


def pr_auto(url, head=None, wt=None, cwd=None):
    """Links the PR to the task that owns the branch (branch_task) or, with no owner, puts it in `sem_task`. Returns the item; PR already known: None.

    Without `head` (the hook did not know this PR's branch, as in the `gh pr create` loop with a variable) it comes from `gh pr view`, and its worktree from `cwd`."""
    d = _prs_ro()
    if any(i["url"] == url for i in d["itens"]) or any(x.get("url") == url for x in d.get("sem_task") or []):
        return None
    if not head:
        head = _pr_head(url)
        wt = wt or (_worktrees_by_branch(cwd).get(head) if head and cwd else None)
    task = branch_task(head, wt, read_events(), _prs_ro())
    if not task and head and head.startswith("merge/"):  # conflict branch: merge/<feature>-<ambiente> belongs to the feature's task
        task = branch_task(re.sub(rf"-({'|'.join(map(re.escape, _known_environments()))})$", "", head[len("merge/"):]), wt, read_events(), _prs_ro())
    item = pr_link(task, url, head=head) if task else orphan_pr(url, head)
    if task:
        queue_auto(item)
    return item


def pr_unlink(task, url):
    def rm(d):
        for n, i in enumerate(d["itens"]):
            if i["task"] == task and i["url"] == url:
                append_event({"tipo": "pr", "op": "desligar", "task": task, "url": url, "numero": i.get("numero")})
                return d["itens"].pop(n)
        raise ValueError(f"PR {url} is not linked to task {task}")

    return _mutate_prs(rm)


COMMIT_TYPES = "feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert"
COMMIT_TITLE_RE = re.compile(rf"(?:{COMMIT_TYPES})(?:\([^)]+\))?!?: \S.*")
GENERATOR_FOOTER_RE = re.compile(r"Generated with|Co-Authored-By|claude\.com/claude-code|\U0001F916", re.I)
PR_SECTIONS = ("Summary", "Evidence", "Merge Danger")


def _no_user_prefix(branch):
    """`leodiegoo/feat/x` becomes `feat/x`: Orca prefixes the user, and the git flow rule is `<type>/<description>`. It only strips the 1st segment when the 2nd is a type."""
    topo, _, rest = branch.partition("/")
    return rest if rest and re.match(rf"(?:{COMMIT_TYPES})/", rest) and not re.fullmatch(COMMIT_TYPES, topo) else branch


def _git_wt(wt, *args, timeout=60):
    return subprocess.run([GIT, "-C", wt, *args], capture_output=True, text=True, timeout=timeout)


def branch_guard(wt, branch, prod, previous):
    """Reasons why `branch` must not go out as a feature PR (ticket 227, git flow: born from `origin/<prod>`, receives only code from it): a commit that is only on the
    local `<prod>` (unpublished work of another subject), or the tip of `origin/<previous>` (the environment before production) inside it without being in `origin/<prod>`.
    A `merge/<feature>-<env>` branch is exempt from the second rule: it exists to carry the environment."""
    g = lambda *x: _git_wt(wt, *x)  # noqa: E731
    reasons = []
    mine = set(g("rev-list", f"origin/{prod}..{branch}").stdout.split())
    if only_local := sorted(mine & set(g("rev-list", f"origin/{prod}..{prod}").stdout.split())):
        reasons.append(f"carries {len(only_local)} commit(s) only on the local {prod}, never pushed to origin/{prod} (e.g. {only_local[0][:7]}): push {prod} or rebase the branch onto origin/{prod}")
    if previous and not _no_user_prefix(branch).startswith("merge/") and g("merge-base", "--is-ancestor", f"origin/{previous}", branch).returncode == 0 \
            and g("merge-base", "--is-ancestor", f"origin/{previous}", f"origin/{prod}").returncode != 0:
        reasons.append(f"carries origin/{previous}, which is not in origin/{prod}: a feature branch gets code from {prod} only, rebase it onto origin/{prod}")
    return reasons


def pr_open(target, title, body_text, environments=None, cwd=None):
    """Publishes the delivery: strips the user prefix from the branch, checks `git merge-tree` against each environment (a conflict stops before the push), pushes, opens one
    PR per environment in the project's order and links each one to the task. `target` is the dispatch (ctx_…) or the branch. Returns (urls, notices); ValueError before touching anything."""
    text_value = open(body_text).read() if os.path.isfile(body_text) else None
    if not (text_value or "").strip():
        raise ValueError(f"empty or missing body: {body_text}")
    for item_name, t in (("title", title), ("body", text_value)):
        if m := GENERATOR_FOOTER_RE.search(t):
            raise ValueError(f"{item_name} with generator footer or trailer ({m.group(0)!r}): remove it before publishing")
    if not COMMIT_TITLE_RE.fullmatch(title.strip()):
        raise ValueError(f"title is not Conventional Commits: {title!r} (<type>(<scope>): <description in English>)")
    notices = [f"body without the {sec} section (skill /pr)" for sec in PR_SECTIONS if not re.search(rf"^#+\s*{sec}\b", text_value, re.M | re.I)]
    event_list = read_events()
    if target.startswith("ctx_"):
        dispatch_events = next((e for e in reversed(event_list) if e.get("tipo") == "despacho" and e.get("dispatch") == target), None)
        if not dispatch_events:
            raise ValueError(f"unknown dispatch: {target}")
        verdict = next((e for e in reversed(event_list) if e.get("tipo") == "conformidade" and e.get("dispatch") == target), None)
        if verdict and not verdict.get("ok"):  # ticket 201: the delivery went back to the worker; the PR waits for the new worker_done
            raise ValueError(f"the delivery of {target} is incomplete (## Conformance): {'; '.join(verdict.get('faltando') or [])}")
        task, wt = dispatch_events.get("task"), _worker_path(orca("worker-show", "--dispatch", target, timeout=10))
        if not wt:
            raise ValueError(f"Orca does not report the worktree of {target}")
        branch = _git_wt(wt, "branch", "--show-current").stdout.strip()
    else:
        branch, wt = target, _worktrees_by_branch(cwd or os.getcwd()).get(target)
        if not wt:
            raise ValueError(f"no worktree with branch {target} from {cwd or os.getcwd()}")
        task = branch_task(branch, wt, event_list)
    if not branch:
        raise ValueError(f"no branch in worktree {wt}")
    phases = phase_check(f"{title}\n{text_value}", scratch_roots([wt]), event_list)  # ticket 201: a PR that says "phase N" carries all of it
    if refusal := phase_refusal(phases):
        raise ValueError(f"{refusal}. Nothing was pushed: finish them, or name the tickets the PR carries without calling it the phase")
    flow_info = task_flow(task, event_list) if task else repo_flow(wt)
    envs = environments or ([a for a in flow_info["ambientes"] if a != flow_info["producao"]] or [flow_info["producao"]] if flow_info["fluxo"] == "promocao" else [flow_info["producao"]])
    envs = sorted(dict.fromkeys(envs), key=lambda a: flow_info["ambientes"].index(a) if a in flow_info["ambientes"] else len(flow_info["ambientes"]))
    if flow_info["producao"] in envs:
        entered = {i["base"] for i in _prs_ro()["itens"] if i.get("task") == task and i["estado"] != "aberto"}
        still_missing = [a for a in flow_info["ambientes"][: flow_info["ambientes"].index(flow_info["producao"])] if a not in entered]
        if still_missing:
            raise ValueError(f"{flow_info['producao']} only after {' and '.join(still_missing)} enter(s): no merged PR of {task} there")
    new = _no_user_prefix(branch)
    prod, ambs = flow_info["producao"], flow_info["ambientes"]
    previous = ambs[ambs.index(prod) - 1] if prod in ambs and ambs.index(prod) > 0 else None
    for a in dict.fromkeys([envs[0], prod, *([previous] if previous else [])]):
        subprocess.run([GIT, "-C", wt, "fetch", "origin", a], capture_output=True, text=True, timeout=60)
    reasons = branch_guard(wt, branch, prod, previous)
    try:
        reasons += audit_publication([f"origin/{envs[0]}..{branch}"], wt, checks=("author", "trailer"))
    except subprocess.CalledProcessError as e:
        raise ValueError(f"did not audit the commits of {branch}: {(e.stderr or '').strip()[-200:]}")
    if reasons:
        raise ValueError("; ".join(reasons) + ". Nothing was pushed")
    subjects = _git_wt(wt, "log", "--reverse", "--format=%h %s", f"origin/{envs[0]}..{branch}").stdout.splitlines()
    print(f"{len(subjects)} commit(s) go out from {branch} over origin/{envs[0]}:" + "".join(f"\n  {l}" for l in subjects), file=sys.stderr)
    for a in envs:
        subprocess.run([GIT, "-C", wt, "fetch", "origin", a], capture_output=True, text=True, timeout=60)
        r = _git_wt(wt, "merge-tree", "--write-tree", "--name-only", "--no-messages", f"origin/{a}", branch)
        if r.returncode != 0:
            raise ValueError(f"conflict with {a}" + (f" in: {', '.join(x for x in r.stdout.splitlines()[1:] if x.strip())}" if r.returncode == 1 else f" (merge-tree failed: {r.stderr.strip()[-200:]})") +
                             f". Nothing was pushed: create merge/{new.split('/', 1)[-1]}-{a} from {a}")
    if new != branch:
        r = _git_wt(wt, "branch", "-m", branch, new)
        if r.returncode:
            raise ValueError(f"did not rename {branch} to {new}: {r.stderr.strip()[-200:]}")
    r = _git_wt(wt, "push", "-u", "origin", new)
    if r.returncode:
        raise ValueError(f"push failed: {r.stderr.strip()[-300:]}")
    urls = []
    for a in envs:
        r = subprocess.run([GH, "pr", "create", "--base", a, "--head", new, "--title", title, "--body-file", body_text], cwd=wt, capture_output=True, text=True, timeout=120)
        found_matches = PR_RE.findall(r.stdout)
        if r.returncode or not found_matches:
            raise ValueError(f"gh pr create for {a} failed ({'; '.join(urls) or 'no PR opened before'}): {(r.stderr or r.stdout).strip()[-300:]}")
        urls.append(found_matches[-1])
        already = any(i["url"] == found_matches[-1] for i in _prs_ro()["itens"])
        if task and not already:
            queue_auto(pr_link(task, found_matches[-1]))
        elif not task:
            pr_auto(found_matches[-1], head=new, wt=wt)
    if phases:
        append_event({"tipo": "fase_declarada", "texto": f"{title}\n{text_value}"[:2000], "urls": urls, "task": task})
    return urls, notices


def _task_folder(task, event_list):
    """The repository folder of the project of the task's dispatch, otherwise the cwd."""
    run = next((e.get("run") for e in reversed(event_list) if e.get("tipo") == "despacho" and e.get("task") == task), None)
    try:
        p = projects().get(dispatch_project(None, run)) or {}
    except ValueError:
        p = {}
    return (repo_folder(p["repo"]) if p.get("repo") else None) or os.getcwd()


def _join_and(names):
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


PRODUCTION_OPS = ("production_opened", "production_failed")  # what ends the attempt to open a task's production PR: once only


def _open_production(task, prod, before, item_list, event_list):
    """Opens the production PR of `task`: title and body of the first environment's PR, with the line of who already entered on top, through `pr_open` (merge-tree
    against production, push, `gh pr create`, link to the task). Any failure (conflict, gh, a worktree that vanished) becomes the `production_failed` event, the
    notice to the coordinator and no new attempt: no push and PR repeated every lap. Returns the panel line."""
    with _lock("pr-production.lock"):
        if any(e.get("tipo") == "pr" and e.get("op") in PRODUCTION_OPS and e.get("task") == task for e in read_events()):
            return None
        first = item_list[0]
        body_file = None
        try:
            r = subprocess.run([GH, "pr", "view", first["url"], "--json", "title,body,headRefName"], capture_output=True, text=True, timeout=PR_GH_S)
            seen = json.loads(r.stdout) if r.returncode == 0 else None
            if not isinstance(seen, dict) or not seen.get("title"):
                raise ValueError(f"gh did not bring the title and body of PR #{first['numero']}")
            line = f"{_join_and(before)} already entered ({', '.join('#' + str(i['numero']) for i in item_list)})"
            import tempfile
            with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as f:
                f.write(f"{line}\n\n{(seen.get('body') or '').strip()}\n")
            body_file = f.name
            urls, _ = pr_open(seen.get("headRefName") or first.get("head") or "", seen["title"], body_file, [prod], _task_folder(task, event_list))
            append_event({"tipo": "pr", "op": "production_opened", "task": task, "url": urls[0], "inherited_from": first["url"]})
            text, panel_line = f"orq: opened the {prod} PR of {task} ({urls[0]}): {line}.", f"{task}: {prod} PR opened by itself ({urls[0]})"
        except (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired) as e:
            append_event({"tipo": "pr", "op": "production_failed", "task": task, "motivo": str(e)[:300]})
            text = f"orq: did not open the {prod} PR of {task}: {e}. Fix it and open it with orq pr open."
            panel_line = f"{task}: {prod} PR not opened ({e})"
        finally:
            if body_file:
                os.remove(body_file)
    g = _manager_cfg()
    if g and g.get("coordenador"):
        notify_coordinator(g["coordenador"], text, context=False)
    return panel_line


def pr_production_open():
    """One panel lap (ticket 184): a feature of a `promocao` flow with a merged PR in every environment before production, no open PR and no PR into production
    (not even one closed without merge: that is a human call) gets its production PR opened by itself (`_open_production`). It reads only prs.json
    and the log until it finds a candidate. Returns the panel lines."""
    event_list = read_events()
    handled = {e.get("task") for e in event_list if e.get("tipo") == "pr" and e.get("op") in PRODUCTION_OPS}
    by_task, lines = {}, []
    for i in _prs_ro()["itens"]:
        by_task.setdefault(i["task"], []).append(i)
    for task, item_list in by_task.items():
        fx = task_flow(task, event_list)
        prod = fx["producao"]
        before = fx["ambientes"][:fx["ambientes"].index(prod)] if prod in fx["ambientes"] else []
        if task in handled or fx["fluxo"] != "promocao" or not before or any(i["estado"] == "aberto" or i.get("base") == prod for i in item_list):
            continue
        entered = {i["base"]: i for i in item_list if i["estado"] == "mergeado" and i.get("base") in before}
        if set(entered) == set(before):
            lines.append(_open_production(task, prod, before, [entered[a] for a in before], event_list))
    return [l for l in lines if l]


def pr_list(task=None):
    """Lines of `orq pr listing`: task, PR with base and state, issue and URL."""
    return [f"{i['task']}  {_pr_segment(i)}" + (f"  (issue #{i['issue']})" if i.get("issue") else "") + f"  {i['url']}"
            for i in _prs_ro()["itens"] if not task or i["task"] == task]


def _old_pr(item_list, now_at):
    """All of the feature's PRs resolved more than PR_VISIBLE_D days ago: it leaves `orq status` and the digest."""
    return all(i["estado"] != "aberto" and i.get("resolvido_em") and (now_at - _dt(i["resolvido_em"])).days > PR_VISIBLE_D for i in item_list)


def pr_lines(now_at=None):
    """One line per feature for `orq status`: the PRs with each one's environment and the suggestion of the next. A feature with everything resolved more
    than PR_VISIBLE_D days ago leaves. Only reads prs.json."""
    now_at = now_at or datetime.now(timezone.utc)
    event_list = read_events()
    by_task = {}
    for i in _prs_ro()["itens"]:
        by_task.setdefault(i["task"], []).append(i)
    line_list = []
    for task, item_list in by_task.items():
        if _old_pr(item_list, now_at):
            continue
        next_item = pr_next(item_list, task_flow(task, event_list))
        line_list.append(f"PR {task}: " + " · ".join(_pr_segment(i) for i in item_list) + (f" → {next_item}" if next_item else ""))
    line_list += [f"PR without task: #{x.get('numero')} ({x.get('head') or '?'}) {x['url']}: `orq pr link <task> {x['url']}`"
               for x in _prs_ro().get("sem_task") or [] if isinstance(x, dict) and x.get("url")]
    return line_list


def stopped_worktrees(wts, busy_paths, now_at, outside):
    """Pure: the worktrees (list from `orca worktree list`) with no live worker (path outside `busy_paths`) and no activity for more than STOPPED_WT_D days,
    oldest first, with `outside(path)` = commits outside origin/main. The main one and the archived ones are excluded."""
    out = []
    for w in wts:
        last_by_header = w.get("lastActivityAt")
        if w.get("isMainWorktree") or w.get("isArchived") or w["path"] in busy_paths or not last_by_header:
            continue
        days = (now_at - datetime.fromtimestamp(last_by_header / 1000, timezone.utc)).days
        if days > STOPPED_WT_D:
            out.append({"caminho": w["path"], "branch": (w.get("branch") or "").removeprefix("refs/heads/") or os.path.basename(w["path"]),
                        "dias": days, "fora": outside(w["path"])})
    return sorted(out, key=lambda x: -x["dias"])


def busy_worktrees():
    """The paths of the worktrees with a worker not yet released (dispatched, or terminal not released). When in doubt the worker counts as alive."""
    return {p for w in _all_workers() if w.get("dispatchStatus") == "dispatched" or w.get("terminalState") != "released"
            for p in [((w.get("resource") or {}).get("worktreeId") or "").split("::", 1)[-1]] if p.startswith("/")}


def _outside_commits(path):
    n = _git(path, "rev-list", "--count", "origin/main..HEAD")
    return int(n) if n and n.strip().isdigit() else 0


def worktree_lines(now_at=None, wts=None, busy_paths=None, outside=None):
    """The `orq status` line with the stopped worktrees, once a day (worktrees-aviso.json stores the day). Orca down: no line."""
    now_at = now_at or datetime.now(timezone.utc)
    day, file_path = now_at.astimezone().strftime("%Y-%m-%d"), _path("worktrees-notice.json")
    if _dict(_read_json(file_path)).get("dia") == day:
        return []
    try:
        ps = stopped_worktrees(wts if wts is not None else orca("list", "--limit", "1000", area="worktree", timeout=15)["worktrees"],
                               busy_worktrees() if busy_paths is None else busy_paths, now_at, outside or _outside_commits)
    except Exception as e:  # noqa: BLE001
        log(f"status: stopped worktrees: {type(e).__name__}: {e}")
        return []
    if not ps:
        return []
    _write_json(file_path, {"dia": day})
    item = lambda x: f"{x['branch']} ({x['dias']} days, {x['fora']} commit{'s' if x['fora'] != 1 else ''} outside main)"
    return [f"Stopped worktrees ({len(ps)}): " + "; ".join(_lim(ps, 3, item))]


E2E_SESSION_MIN = 15  # minutes an `e2e-infra.sh start` session may stay without a live test process before the queue counts as stuck


def _pid_alive(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, belongs to another user
    except (ValueError, OverflowError):
        return False
    return True


def e2e_queue(queue=None, now_at=None, limit_min=E2E_SESSION_MIN):
    """The global E2E queue of the product repository (scripts/e2e-lock.sh: one `<order>-<pid>` ticket per arrival in the project's `e2e_queue` folder), read-only.

    Returns None with an empty queue, otherwise {ticket, worktree, projeto, comando, min, esperam, presa}. The owner is the first ticket; `stuck_lock` is the
    reason when it is not moving: no live pid of the ticket and no session (dead owner, the next one waiting would clear it), or a session opened by
    `start` with no live test process for more than `limit_min` minutes. Limit: it does not look at Docker, so a stack opened on purpose
    for more than `limit_min` without a test also shows up as stuck.

    Without `queue` or E2E_LOCK_DIR, reads the `e2e_queue` of each project file (a project without the field has no queue) and returns the first stuck one, otherwise the
    first one that is not empty. Limit: only one queue shows up; two stuck at the same time show the one from the project with the smaller name until it is released."""
    queue = queue or os.environ.get("E2E_LOCK_DIR")
    if not queue:
        found_labels = [f for f in (e2e_queue(os.path.expanduser(d["fila_e2e"]), now_at, limit_min) for d in projects().values() if isinstance(d.get("fila_e2e"), str)) if f]
        return next((f for f in found_labels if f["presa"]), found_labels[0] if found_labels else None)
    now_at = time.time() if now_at is None else now_at
    tickets = []
    for item_name in sorted(os.listdir(queue)) if os.path.isdir(queue) else []:
        d = os.path.join(queue, item_name)
        try:
            owner = dict(l.rstrip("\n").split("=", 1) for l in open(os.path.join(d, "owner")) if "=" in l)
            pids = os.listdir(os.path.join(d, "pids")) if os.path.isdir(os.path.join(d, "pids")) else []
            start_at = int(open(os.path.join(d, "acquired")).read().strip()) if os.path.exists(os.path.join(d, "acquired")) else int(owner.get("started") or now_at)
        except (OSError, ValueError):
            continue  # ticket still being written or unreadable
        tickets.append({"nome": item_name, "owner": owner, "vivo": any(_pid_alive(x) for x in pids), "sessao": os.path.exists(os.path.join(d, "session")), "ini": start_at})
    if not tickets:
        return None
    owner_name, rest = tickets[0], tickets[1:]
    minutes_elapsed = max(0, int((now_at - owner_name["ini"]) // 60))
    stuck_lock = None
    if not owner_name["vivo"] and not owner_name["sessao"]:
        stuck_lock = f"the owner (pid {owner_name['owner'].get('pid')}) died and the ticket stayed"
    elif not owner_name["vivo"] and minutes_elapsed > limit_min:
        stuck_lock = f"session opened by start with no live test process for {minutes_elapsed} min"
    o = owner_name["owner"]
    return {"ticket": owner_name["nome"], "worktree": os.path.basename(o.get("worktree", "")), "projeto": o.get("project"), "comando": o.get("command"),
            "min": minutes_elapsed, "esperam": sum(1 for t in rest if t["vivo"] or t["sessao"]), "presa": stuck_lock}


def e2e_line(f):
    """The `orq status` line for the E2E queue (`e2e_queue`); empty with an empty queue."""
    if not f:
        return ""
    txt = f"E2E queue: {f['worktree']} ({f['projeto']}) held for {f['min']} min, {f['esperam']} waiting"
    return txt + (f". STUCK: {f['presa']}; `scripts/e2e-infra.sh lock-release` in the product repository frees it" if f["presa"] else "")


def notify_e2e_queue(f=None):
    """Types into the coordinator, once per ticket, that the E2E queue is stuck (e2e-aviso.json stores the ticket). Coordinator busy: the next round tries again."""
    g, f = _manager_cfg(), f or e2e_queue()
    if not g or not g.get("coordenador") or not f or not f["presa"]:
        return []
    file_path = _path("e2e-notice.json")
    if _dict(_read_json(file_path)).get("ticket") == f["ticket"]:
        return []
    if notify_coordinator(g["coordenador"], f"orq: {e2e_line(f)}.") not in ("enviado", "adiado"):
        return []
    _write_json(file_path, {"ticket": f["ticket"]})
    return [f"E2E queue stuck ({f['ticket']}): notice typed in the coordinator"]


def hooks_broken_round():
    """The manager round's line when a hook is failing (the marker fail_safe.py leaves), and a notice typed in the coordinator once per marker: its own hooks are the ones that went quiet.
    The line goes away when the hook that failed imports or runs again."""
    line = fail_safe.broken_line()
    if not line:
        return []
    g, marker = _manager_cfg(), fail_safe.read_marker()
    if g and g.get("coordenador") and marker.get("told") != marker.get("first") and notify_coordinator(g["coordenador"], line) in ("enviado", "adiado"):
        fail_safe.write_marker({**fail_safe.read_marker(), "told": marker.get("first")})
        return [line, "broken hooks: notice typed in the coordinator"]
    return [line]


def _clean_post_merge(i, branch):
    """PR merged into the final base: fires that branch's limpar-mergeados.py in the background, with the delay of the "merged" hook (GitHub takes a few
    seconds to mark the PR). The repository comes from the task's dispatch project, otherwise from cwd. Without the branch there is nothing to clean. A failure to
    fire becomes just a log: the cleanup never takes the poll down."""
    if not branch:
        return
    dispatch_events = next((e for e in reversed(read_events()) if e.get("tipo") == "despacho" and e.get("task") == i["task"]), {})
    try:
        repo = repo_folder(projects()[dispatch_events["projeto"]]["repo"]) if dispatch_events.get("projeto") in projects() else None
        repo = repo or os.getcwd()
        subprocess.Popen(["sh", "-c", f'sleep {CLEAN_DELAY_S}; exec python3 "$0" --repo "$1" --branch "$2" --task "$3"', CLEAN_SCRIPT, repo, branch, i["task"]],
                         stdin=subprocess.DEVNULL, stdout=open(LOG, "a"), stderr=subprocess.STDOUT, start_new_session=True)
    except (OSError, ValueError) as e:
        log(f"_limpar_pos_merge: {i['url']}: {type(e).__name__}: {e}")
        return
    append_event({"tipo": "pr", "op": "limpeza", "task": i["task"], "url": i["url"], "branch": branch, "repo": repo})


def _store_reports(wt):
    """Copies the worktree's relatorio-final.md (`.scratch/*/report-final.md` and `report*.md` at the root) to RELATORIOS/<worktree>-<path with - in place of />. Returns the destinations."""
    os.makedirs(REPORTS, exist_ok=True)
    item_name, out = os.path.basename(wt.rstrip("/")), []
    for c in sorted({*glob.glob(os.path.join(wt, ".scratch/*/relatorio-final.md")), *glob.glob(os.path.join(wt, ".scratch/*/final-report.md")),
                     *glob.glob(os.path.join(wt, "relatorio*.md")), *glob.glob(os.path.join(wt, "final-report*.md"))}):
        d = os.path.join(REPORTS, f"{item_name}-{os.path.relpath(c, wt).replace('/', '-')}")
        shutil.copy2(c, d)
        out.append(d)
    return out


def _clean_closed_branch(task, repo, b, item_list, flow_info, dry):
    """Cleans up a branch of a PR closed without merge: keeps the report, removes the worktree through Orca, deletes the local one and the remote one. Returns (line, ok).
    It never touches an environment branch or a branch with an open PR (head or base). When in doubt (gh or Orca with no response) it does not delete."""
    if b in flow_info["ambientes"] or b == flow_info["producao"]:
        return f"{task}: {b} is an environment branch, not cleaned", True
    slug = item_list[0]["url"].rsplit("/pull/", 1)[0].removeprefix("https://github.com/")
    try:
        r = subprocess.run([GH, "pr", "list", "--repo", slug, "--state", "open", "--limit", "200", "--json", "headRefName,baseRefName"], capture_output=True, text=True, timeout=PR_GH_S)
        open_items = json.loads(r.stdout) if r.returncode == 0 else None
        wts = orca("list", "--repo", f"path:{repo}", area="worktree", timeout=15)["worktrees"]
    except (subprocess.TimeoutExpired, OSError, ValueError, RuntimeError, KeyError) as e:
        return f"{task}: {b} not cleaned, no answer from gh or Orca ({type(e).__name__})", False
    if not isinstance(open_items, list) or any(b in (p.get("headRefName"), p.get("baseRefName")) for p in open_items if isinstance(p, dict)):
        return f"{task}: {b} has an open PR, not cleaned", True
    wt = next((w for w in wts if (w.get("branch") or "").removeprefix("refs/heads/") == b and not w.get("isMainWorktree")), None)
    ticket_numbers = ", ".join(f"#{i['numero']}" for i in item_list)
    what = ", ".join(x for x in (f"worktree {wt['path']}" if wt else "", "local branch", "remote branch") if x)
    if dry:
        return f"{task}: would clean {b} ({what}); PR {ticket_numbers} closed without merge", True
    stored = []
    try:
        if wt:
            stored = _store_reports(wt["path"])
            terminate_worktree_processes(wt["path"])
            orca("rm", "--worktree", f"path:{wt['path']}", "--run-hooks", area="worktree", timeout=120)
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:  # without the copy or without removing the worktree, deleting the branch would lose work
        return f"{task}: {b} not cleaned, the worktree stayed ({e})", False
    topo = (_git(repo, "ls-remote", "--heads", "origin", b) or "").split("\t")[0]
    deleted = [_git(repo, "branch", "-D", b) is not None and "local", bool(topo) and _git(repo, "push", "origin", "--delete", b, timeout=60) is not None and "remota"]
    append_event({"tipo": "pr", "op": "limpou_fechado", "task": task, "branch": b, "removidos": [x for x in deleted if x], "guardados": stored, "ponta_remota": topo,
                  "restaurar": f"GitHub restores the remote branch with the Restore branch button on PR {ticket_numbers}"})
    return f"{task}: {b} cleaned ({what}); PR {ticket_numbers} closed without merge. GitHub restores the remote branch with the Restore branch button on the PR", True


def clean_closed(now_at=None, days=None, dry=False, auto=False):
    """Cleans up the branches of tasks whose PRs are all closed without merge (none open or merged). `days`: only those closed that long ago (None: right away,
    `orq clean --closed_items`). `auto` (the poll): respects CLOSED_DAYS and, until the first real `orq clean --closed_items` (limpar-fechados.json), only shows the
    preview, once per task. The repository comes from the dispatch project, otherwise from cwd. Returns the lines of what it did."""
    now_at = time.time() if now_at is None else now_at
    if auto and not _read_json(_path(CLOSED_FILE)):
        dry = True
    allowed = (None,) if auto else (None, "previa", "erro")
    line_list, event_list, by = [], read_events(), {}
    for i in _prs_ro()["itens"]:
        by.setdefault(i["task"], []).append(i)
    for task, xs in by.items():
        closing = max((_dt(x.get("resolvido_em") or x["ligado_em"]).timestamp() for x in xs if x.get("resolvido_em") or x.get("ligado_em")), default=now_at)
        waiting = CLOSED_DAYS if auto else days
        if not all(x["estado"] == "fechado" for x in xs) or any(x.get("limpeza") not in allowed for x in xs) or (waiting is not None and now_at - closing < waiting * 86400):
            continue
        dispatch_events = next((e for e in reversed(event_list) if e.get("tipo") == "despacho" and e.get("task") == task), {})
        repo = (repo_folder(projects()[dispatch_events["projeto"]]["repo"]) if dispatch_events.get("projeto") in projects() else None) or os.getcwd()
        res = [_clean_closed_branch(task, repo, b, xs, task_flow(task, event_list), dry) for b in sorted({x["head"] for x in xs if x.get("head")})]
        line_list += [l for l, _ in res]
        mark = "previa" if dry else "feita" if all(ok for _, ok in res) else "erro"
        _mutate_prs(lambda d, t=task, m=mark: [x.update(limpeza=m) for x in d["itens"] if x["task"] == t])
    if not dry and not auto and line_list:
        _write_json(_path(CLOSED_FILE), {"confirmado": now()})
    return line_list


def _apply_prs(d, seen, now_at):
    """Moves each open PR that gh saw merged or closed to the new state: one `pr` event and one `pr` entry per PR, only once (the entry's `ref` is the URL, so repeating the
    handoff does not duplicate it). Returns the lines of what changed."""
    d["ultimo_poll"] = now_at
    event_list = read_events()
    already = {e.get("ref") for e in event_list if e.get("origem") == "pr"}
    line_list = []
    for i in d["itens"]:
        seen_item = _dict(seen.get(i["url"]))
        new = _gh_state(seen_item)
        if i["estado"] == "aberto" and not new:
            ci = _gh_ci(seen_item, now_at, task_flow(i["task"], event_list))
            i.update(**({"ci": ci} if ci else {}), **({"head": seen_item["headRefName"]} if seen_item.get("headRefName") else {}))
        if i["estado"] != "aberto" or not new:
            i["base"] = seen_item.get("baseRefName") or i.get("base") if i["estado"] == "aberto" else i.get("base")
            continue
        i.update(estado=new, base=seen_item.get("baseRefName") or i.get("base"), resolvido_em=now(), **({"head": seen_item["headRefName"]} if seen_item.get("headRefName") else {}),
                 **({"sha": seen_item["mergeCommit"]["oid"]} if isinstance(seen_item.get("mergeCommit"), dict) and seen_item["mergeCommit"].get("oid") else {}))
        next_item = pr_next([x for x in d["itens"] if x["task"] == i["task"]], task_flow(i["task"]))
        where = f"{i['task']}" + (f", issue #{i['issue']}" if i.get("issue") else "")
        text_value = f"PR #{i['numero']} entered {i['base']} ({where})" + (f": {next_item}" if next_item else "") if new == "mergeado" \
            else f"PR #{i['numero']} closed without merge (base {i['base']}, {where})"
        append_event({"tipo": "pr", "op": "entrou" if new == "mergeado" else "fechou", "task": i["task"], "url": i["url"], "numero": i["numero"],
                      "base": i["base"], **({"proximo": next_item} if next_item else {})})
        if new == "fechado" and all(x["estado"] == "fechado" for x in d["itens"] if x["task"] == i["task"]):  # all closed without merge: the branch is a cleanup candidate
            append_event({"tipo": "pr", "op": "fechada", "task": i["task"], "branch": i.get("head"), "limpeza_em_dias": CLOSED_DAYS})
        if new == "mergeado" and i["base"] == (FINAL_BASE or task_flow(i["task"], event_list)["producao"]):
            _clean_post_merge(i, seen_item.get("headRefName") or i.get("head"))
        if i["url"] in already:
            i["avisado"] = True
            continue
        entry_event = append_event({"tipo": "entrada", "origem": "pr", "texto": text_value, "fonte": f"PR #{i['numero']}", "ref": i["url"], "task": i["task"]}, new_id=True)
        i.update(entrada=entry_event["id"], texto=text_value, avisado=False)
        if new == "mergeado":
            tk = next((t for t in tickets() if t["task"] == i["task"] and t["status"] != STATUS_CLOSED), None)
            for key_name, txt in merge_obligations(i, next_item, tk, task_flow(i['task'], event_list)):
                append_event({"tipo": "obrigacao", "op": "nova", "entrada": entry_event["id"], "chave": key_name, "texto": txt, "task": i["task"],
                              **({"ticket": tk["num"]} if key_name == "ticket" else {}), **({"base": i["base"], "sha": i.get("sha") or ""} if key_name == "deploy" else {})})
        line_list.append(f"{i['task']}: {text_value}")
    return line_list


def pr_poll(now_at=None, force=False):
    """Asks gh for the open PRs and records the ones that entered or were closed (_apply_prs). Returns the lines of what changed.

    Runs outside the hooks (the manager panel and `orq pr poll`), at most every PR_POLL_S (`force` ignores it), with no open PR it does not even call gh, and
    one poll at a time (non-blocking lock). One `gh pr list` call per repository brings the state, the `mergeable` and the checks of all linked PRs
    (stored in `ci`). gh with no response leaves the PR open for the next round. Limit: the panel's round waits for gh, PR_GH_S per repository."""
    now_at = time.time() if now_at is None else now_at
    os.makedirs(HOME, exist_ok=True)
    with open(_path("pr-poll.lock"), "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return []
        d = _prs_ro()
        urls = [i["url"] for i in d["itens"] if i["estado"] == "aberto"]
        if not urls:
            return clean_closed(now_at, auto=True)
        if not force and now_at - (d.get("ultimo_poll") or 0) < PR_POLL_S:
            return []
        seen = _pr_gh_list(urls)
        return _mutate_prs(lambda d: _apply_prs(d, seen, now_at)) + clean_closed(now_at, auto=True)


def pr_notify():
    """Types into the coordinator a line `orq: PR #N has_entered em <base> …` per resolved PR not yet notified, once (the manager panel calls it on
    every round). Coordinator busy or with a draft: nothing is typed and the next round tries again. With no manager running there is no one to type: the
    entry is already in the log and shows up in the next prompt. Returns the panel lines."""
    g = _manager_cfg()
    if not g or not g.get("coordenador"):
        return []
    line_list = []
    for i in [x for x in _prs_ro()["itens"] if x["estado"] != "aberto" and not x.get("avisado") and x.get("entrada")]:
        def reserve(d, url=i["url"], value=True):
            """Sets/clears the notice under pr.lock; returns False if it was already set (another panel or a restart got there first)."""
            for x in d["itens"]:
                if x["url"] == url:
                    if value and (x.get("avisado") or any(e.get("op") == "avisado" and e.get("url") == url for e in read_events())):
                        x["avisado"] = True
                        return False
                    x["avisado"] = value
            return True

        # reserve before typing: two panels (or a restart mid-round) do not type the same notice twice
        if not _mutate_prs(reserve):
            continue
        if notify_coordinator(g["coordenador"], f"orq: {i['texto']}. Entry {i['entrada']}.", context=False) not in ("enviado", "adiado"):  # the "PR: …" in the summary already shows it
            _mutate_prs(lambda d, f=reserve: f(d, value=False))  # nothing was typed: the next round tries
            break
        append_event({"tipo": "pr", "op": "avisado", "task": i["task"], "url": i["url"], "numero": i["numero"]})
        line_list.append(f"{i['task']}: PR #{i['numero']} notice typed in the coordinator")
    return line_list


# ---------- notice obligations (ticket 114) ----------

OBLIGATION_MIN = float(os.environ.get("ORQ_OBRIGACAO_MIN") or 10)  # obligation open for longer than this: the coordinator's Stop blocks on it, within the GATE_BLOCKERS budget
# what a PR merge asks of the coordinator, by base (README, "Obrigações dos avisos"). An obligation that cites a field with no value is not created:
# no issue, no comment; no open ticket for the task, nothing to close; no suggested next environment, no PR to open.
_NEXT_ONE = ("proximo", "open the PR for {proximo}, or defer with the reason to hold")
PRODUCTION_OBLIGATIONS = (("deploy", "check the production deploy (quave-one)"), ("comentario", "update the comment on #{issue}"),
                       ("limpeza", "check that the branch and the worktree are gone"), ("ticket", "close ticket {ticket}"))
ENVIRONMENT_OBLIGATIONS = (("deploy", "check the {base} deploy"), _NEXT_ONE)  # the environments before production


def merge_obligations(i, next_item, tk=None, flow_info=None):
    """[(key, text)] of what the merge of PR `i` asks for: the production list when the base is the project's production (`flow_info`, from `task_flow`), that of the other
    environments when it is one of them, nothing for another base. The issue comes from `orq pr ligar --issue` or from the `issue:` of the task's ticket `tk`."""
    flow_info = flow_info or task_flow(i.get("task"))
    base = i.get("base")
    table = PRODUCTION_OBLIGATIONS if base == flow_info["producao"] else ENVIRONMENT_OBLIGATIONS if base in flow_info["ambientes"] else ()
    issue = i.get("issue") or (tk or {}).get("issue")
    m = re.match(r"(?:pronto para|ready for) (\S+)", next_item or "")
    vals = {"issue": issue, "ticket": (tk or {}).get("num"), "proximo": m.group(1) if m else None, "base": base}
    return [(k, t.format(**vals)) for k, t in table
            if all(vals.get(c) for c in re.findall(r"\{(\w+)\}", t))]


def open_obligations(events, entry=None):
    """The `obligation nova` events with no `done` or `adiada` after them, in the order they were born (of a single entry, with `entry`)."""
    closed_ids = {(e.get("entrada"), e.get("chave")) for e in events if e.get("tipo") == "obrigacao" and e.get("op") in ("feito", "adiada")}
    return [e for e in events if e.get("tipo") == "obrigacao" and e.get("op") == "nova" and (e.get("entrada"), e.get("chave")) not in closed_ids
            and entry in (None, e.get("entrada"))]


def _by_entry(obligations_open):
    by = {}
    for o in obligations_open:
        by.setdefault(o["entrada"], []).append(o)
    return by


def obligations_line(events):
    """"A fazer por você: e484 → comentario (…), deploy (…)." from the coordinator's preamble, or empty. The mate does not carry the coordinator's.
    ponytail: no cap of its own; dozens of open obligations make a long line, which is the sign they are being forgotten."""
    obligations_open = [] if os.environ.get("ORQ_MATE") else open_obligations(events)
    if not obligations_open:
        return ""
    return ("To do by you: " + "; ".join(f"{e} → " + ", ".join(f"{o['chave']} ({o['texto']}" + (f"; {m}" if o["chave"] == "limpeza" and (m := _cleanup_reason(events, o.get("task"))) else "") + ")" for o in os_) for e, os_ in _by_entry(obligations_open).items())
            + '. Close: orq fulfill <e> <obligation> --proof "<url, version, hash>" | orq defer <e> <obligation> --reason "…".')


def _obligation(e, key_name):
    obligations_open = open_obligations(read_events(), e)
    o = next((o for o in obligations_open if o["chave"] == key_name), None)
    if not o:
        raise ValueError(f"entry {e} has no open obligation {key_name!r}" + (f" (open: {', '.join(o['chave'] for o in obligations_open)})" if obligations_open else ""))
    return o


def _close_obligation(o, op, **fields):
    """Records the closing; the last obligation of the entry closes the entry too, if it had no effect yet."""
    ev = append_event({"tipo": "obrigacao", "op": op, "entrada": o["entrada"], "chave": o["chave"], **fields})
    event_list = read_events()
    if not open_obligations(event_list, o["entrada"]) and not any(x.get("tipo") == "intake" and x.get("entrada") == o["entrada"] for x in event_list):
        append_event({"tipo": "intake", "entrada": o["entrada"], "efeito": "conversa", "nota": "obligations fulfilled"})
    return ev


def _close_auto(key_name, task, proof, when=lambda o: True):
    """Closes with `done` the open obligations of `key_name` of the `task` for which `when(o)` holds, with the proof that orq itself saw. Returns how many."""
    found_labels = [o for o in open_obligations(read_events()) if o["chave"] == key_name and o.get("task") == task and when(o)]
    for o in found_labels:
        _close_obligation(o, "feito", prova=proof, auto=True)
    return len(found_labels)


def cleaned_close(ev):
    """The `pr`/`cleaned` event from limpar-mergeados closes the task's `cleanup` when it removed something and nothing was kept or skipped; otherwise the obligation
    stays open and its line shows the reason (`_cleanup_reason`)."""
    if ev.get("removidos") and not ev.get("pulados") and not ev.get("guardados"):
        return _close_auto("limpeza", ev.get("task"), "cleaned: " + ", ".join(ev["removidos"]))
    return 0


def _cleanup_reason(events, task):
    """Why the task's last cleanup did not close the obligation (what it skipped or kept), or ''."""
    ev = next((e for e in reversed(events) if e.get("tipo") == "pr" and e.get("op") == "limpou" and e.get("task") == task), None)
    return "; ".join([*(ev or {}).get("pulados", []), *(f"kept {g}" for g in (ev or {}).get("guardados", []))])


def _close_next(item):
    """PR linked to the task whose base is the environment the `next_one` obligation asks for (`open the PR for <environment>, …`): closes with its URL."""
    if item.get("base"):
        _close_auto("proximo", item["task"], item["url"], lambda o: (re.match(r"(?:abrir o PR de|open the PR for) (\S+?),", o["texto"]) or [None, None])[1] == item["base"])


DEPLOY_ERROR_S = 60  # a `deploy_check` that exceeds this counts as a failure


def _deploy_of(o):
    """(command, folder) of the `deploy_check` of the project of the task of obligation `o`, or None (no project or no key)."""
    run = next((e.get("run") for e in reversed(read_events()) if e.get("tipo") == "despacho" and e.get("task") == o.get("task")), None)
    try:
        item_name = dispatch_project(None, run)
    except ValueError:
        return None
    p = projects().get(item_name) or {}
    return (p["deploy_check"], repo_folder(p["repo"]) or os.getcwd()) if p.get("deploy_check") else None


def deploy_verify(now_at=None):
    """Runs the project's `deploy_check` for each open `deploy` obligation, at most every PR_POLL_S per obligation. Exit 0: closes with the first
    line of stdout; 2: still building, stays open; other (or timeout): notifies the coordinator once (event `obligation failed`). Without the key, nothing runs.
    Returns the panel lines."""
    now_at = time.time() if now_at is None else now_at
    file_path, line_list = _path("deploy-check.json"), []
    seen = _dict(_read_json(file_path))
    for o in [o for o in open_obligations(read_events()) if o["chave"] == "deploy"]:
        key_name = f"{o['entrada']}/{o['chave']}"
        dep = _deploy_of(o)
        if not dep or now_at - seen.get(key_name, 0) < PR_POLL_S:
            continue
        seen[key_name] = now_at
        cmd = (dep[0].replace("{base}", shlex.quote(o.get("base") or "")).replace("{sha}", shlex.quote(o.get("sha") or ""))
               .replace("{orq}", shlex.quote(os.path.dirname(os.path.abspath(__file__)))))
        try:
            r = subprocess.run(cmd, shell=True, cwd=dep[1], capture_output=True, text=True, timeout=DEPLOY_ERROR_S)
            rc, output = r.returncode, (r.stdout or "").strip().splitlines()
        except (OSError, subprocess.TimeoutExpired) as e:
            rc, output = -1, [type(e).__name__]
        if rc == 0 and output:
            _close_obligation(o, "feito", prova=output[0], auto=True)
            line_list.append(f"{o['task']}: {o.get('base')} deploy checked ({output[0]})")
        elif rc not in (0, 2) and not any(e.get("tipo") == "obrigacao" and e.get("op") == "falhou" and (e.get("entrada"), e.get("chave")) == (o["entrada"], o["chave"]) for e in read_events()):
            g = _manager_cfg()
            if g and g.get("coordenador") and notify_coordinator(g["coordenador"], f"orq: deploy_check for {o.get('base')} exited {rc} ({o['task']}, entry {o['entrada']}): check by hand and close with orq fulfill.",
                                                                   context=False) in ("enviado", "adiado"):
                append_event({"tipo": "obrigacao", "op": "falhou", "entrada": o["entrada"], "chave": o["chave"], "saida": rc})
    _write_json(file_path, seen)
    return line_list


def obligation_done(e, key_name, proof):
    if not (proof or "").strip():
        raise ValueError("--proof is empty: the comment URL, the deploy version or the hash")
    return _close_obligation(_obligation(e, key_name), "feito", prova=proof.strip())


def defer_obligation(e, key_name, reason, run=None):
    """Postpones with a "a fazer depois" ticket that carries the reason: nothing is lost. Without the ticket (no Run, Orca down) the obligation stays open."""
    if not (reason or "").strip():
        raise ValueError("--reason is empty: say why it is postponed")
    o = _obligation(e, key_name)
    entry_event = next((x for x in read_events() if x.get("tipo") == "entrada" and x.get("id") == e), {})
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as f:
        f.write(f"## What to build\n\nObligation `{key_name}` of entry {e} ({entry_event.get('texto') or '?'}), deferred: {o['texto']}.\n\n"
                f"Reason: {reason.strip()}\n\n## Acceptance criteria\n\n- [ ] {o['texto']}, with the proof (URL, version or hash)\n")
    try:
        tk = ticket_new(f"to do later: {o['texto']} ({entry_event.get('fonte') or e})", f.name, run=run)
    finally:
        os.remove(f.name)
    _close_obligation(o, "adiada", motivo=reason.strip(), ticket=tk["ticket"])
    return {"entrada": e, "chave": key_name, **tk}


def obligations_to_chase(events, now_at, minutes_elapsed=None):
    """The obligations open for at least `minutes_elapsed` (OBLIGATION_MIN): the coordinator's Stop blocks them (`hook_stop`)."""
    minutes_elapsed = OBLIGATION_MIN if minutes_elapsed is None else minutes_elapsed
    return [o for o in open_obligations(events) if (now_at - _dt(o["ts"])).total_seconds() >= minutes_elapsed * 60]


def _open_question(events):
    """{dispatch: pergunta_tela event} of the on-screen menus that the manager already notified and have not disappeared yet (the `pergunta_tela_fim` after it closes it)."""
    open_entries = {}
    for e in events:
        if e.get("tipo") == "pergunta_tela" and e.get("dispatch"):
            open_entries[e["dispatch"]] = e
        elif e.get("tipo") == "pergunta_tela_fim":
            open_entries.pop(e.get("dispatch"), None)
    return open_entries


def notify_screens():
    """One round of the manager over the workers' screens: a permission prompt, AskUserQuestion or "trust this folder" stuck in a worker's terminal
    becomes a line typed into the coordinator, once per menu (event `pergunta_tela`, with the question and the options), with `orq reply_to-screen` to run.
    The plan limit notice on the screen (screen_limit) becomes a single line per dispatch (event `limite_tela`): the worker has no turn, and no steer reaches it.
    Coordinator busy or with a draft: nothing is typed and the next round tries again. A menu that disappeared from the screen closes the event (`pergunta_tela_fim`),
    so the same text is notified again if it reappears. Only reads the screens of those with a turn in the worker hooks (Claude Code). Returns the panel lines."""
    g = _manager_cfg()
    if not g or not g.get("coordenador"):
        return []
    turns, ws, hib = _turns_ro(), [], _hibernated()
    for r in g["runs"]:
        ws += [w for w in _all_workers(r) if w.get("dispatchId") in turns and w.get("dispatchId") not in hib]
    screens_read = _read_screens(ws, {w["dispatchId"]: {"agente": _dict(turns[w["dispatchId"]]).get("harness") or "claude"} for w in ws})
    open_entries, line_list = _open_question(read_events()), []
    for w in ws:
        d, p = w["dispatchId"], (screens_read.get(w["dispatchId"]) or {}).get("pergunta")
        if not p and d in open_entries:
            append_event({"tipo": "pergunta_tela_fim", "dispatch": d})
        if not p or (d in open_entries and open_entries[d].get("texto") == p["texto"] and open_entries[d].get("opcoes") == p["opcoes"]):
            continue
        ops = " ".join(f"{n}) {r}" for n, r in p["opcoes"])
        notice = f"orq: worker {w.get('taskId')} asks on screen ({p['tipo']}): {p['texto']} Options: {ops}. Reply with: orq answer-screen {w.get('taskId')} <option>."
        if notify_coordinator(g["coordenador"], re.sub(r"\s+", " ", notice)) not in ("enviado", "adiado"):
            break
        append_event({"tipo": "pergunta_tela", "dispatch": d, "task": w.get("taskId"), "run": w.get("runId"), "terminal": w.get("agentTerminalHandle"), "menu": p["tipo"], "texto": p["texto"], "opcoes": p["opcoes"]})
        line_list.append(f"{w.get('taskId')}: screen question ({p['tipo']}) reported to the coordinator")
    notified_items = {e.get("dispatch") for e in read_events() if e.get("tipo") == "limite_tela"}
    for w in ws:
        d, limit = w["dispatchId"], (screens_read.get(w["dispatchId"]) or {}).get("limite")
        if not limit or d in notified_items:
            continue
        notice = f"orq: worker {w.get('taskId')} stopped at the plan limit ({limit}). No turn until the plan renews: a steer does not arrive. Check with: orq agents."
        if notify_coordinator(g["coordenador"], notice) not in ("enviado", "adiado"):
            break
        append_event({"tipo": "limite_tela", "dispatch": d, "task": w.get("taskId"), "run": w.get("runId"), "terminal": w.get("agentTerminalHandle"), "texto": limit})
        line_list.append(f"{w.get('taskId')}: plan limit reported to the coordinator")
    return line_list


def no_terminal_line(open_state):
    """"N worker(s) perderam o terminal sem worker_done: orq retomar --dry-run" from the aberto.json cache (no call to Orca), or empty."""
    n = sum(a.get("estado") == "sem_terminal" for a in _dict(open_state).get("agentes") or [] if isinstance(a, dict))
    return f"{n} worker(s) lost the terminal without worker_done: orq resume --dry-run" if n else ""


def state(entry=None, include_old=False):
    events, cur, open_state = read_events(), _cursor_ro(), _read_json(_path("open.json"))
    txt = summary(events, open_state, _pending_ro(), entry, cursor=cur, turns=_turns_ro(), panel=panel_notice(), include_old=include_old)
    return "\n".join([*filter(None, [codex_hooks_notice()]), txt, *filter(None, [no_terminal_line(open_state)]), *night_lines(cur, events), *filter(None, [released_line(events, tickets()), dispatch_wait_line(tickets(), events)])])


# ---------- digest e modo ausente ----------

DIGEST = "digest"  # ORQ_HOME/digest/<YYYY-MM-DD>.html: one file per day, always in the same place
DIGEST_LINES = 100  # `line` entries; the older ones stay only in the log (the page says "+N antes")
MERGE_QUEUE_FILE = "merge-queue.json"  # {passos: [{passo, nome, por, prs: [numbers], feito}]}: the merge order the coordinator declares with `orq queue`
GH_STATE = {"aberto": "OPEN", "mergeado": "MERGED", "fechado": "CLOSED"}  # the PR state in the digest contract
TRANSCRIPT_END = 400_000  # bytes from the end of the transcript where the Stop looks for the coordinator's last reply
ANSWER_MAX = 4000  # characters of the coordinator's reply that go to the log
DIGEST_STATE = {"MERGED": ("m", "merged"), "CLOSED": ("x", "closed"), "OPEN": ("o", "open")}
DIGEST_CSS = """:root{--bg:#f7f7f5;--card:#fff;--ink:#1d1d1f;--mute:#6b6b70;--line:#e4e4e0;--dev:#2f6fdb;--stg:#b7791f;--ok:#2e8b57;--warn:#c2410c;--sec:#b91c1c;--chip:#f0efeb}
@media (prefers-color-scheme:dark){:root{--bg:#141416;--card:#1d1d20;--ink:#ededed;--mute:#9a9aa2;--line:#2c2c31;--chip:#26262b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif}
main{max-width:1000px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:24px;margin:0 0 4px}h2{font-size:17px;margin:28px 0 10px}.sub{color:var(--mute);margin:0 0 8px}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(min(100%,300px),1fr))}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;min-width:0}
.card h3{margin:0 0 6px;font-size:15px}.card p{margin:6px 0 0;color:var(--mute);font-size:13.5px}.card.dec{border-left:4px solid var(--warn)}
.prs{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.pr{display:inline-flex;align-items:center;gap:6px;text-decoration:none;color:var(--ink);background:var(--chip);border-radius:999px;padding:3px 10px;font-size:13px;border:1px solid var(--line)}
.pr em{font-style:normal;font-size:11px;color:var(--mute)}.pr.m{opacity:.6}.pr.m em{color:var(--ok)}
.dot{width:8px;height:8px;border-radius:50%}.d{background:var(--dev)}.s{background:var(--stg)}.p{background:var(--ok)}
.legend{font-size:12.5px;color:var(--mute);display:flex;gap:12px;align-items:center;flex-wrap:wrap}.legend .dot{display:inline-block}
.fila{list-style:none;padding:0;margin:0;display:grid;gap:10px}.fila li{display:flex;gap:12px;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px;min-width:0}
.fila li>div{min-width:0}.fila .num{flex:0 0 28px;height:28px;border-radius:50%;background:var(--ink);color:var(--bg);display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px}
.fila li.feito{opacity:.5}.fila li.feito .num{background:var(--ok)}.fila p{margin:2px 0 0;color:var(--mute);font-size:13.5px}.fila .aviso{color:var(--warn)}
ol.tl{list-style:none;padding:0;margin:0;border-left:2px solid var(--line)}
ol.tl li{position:relative;padding:0 0 14px 18px}ol.tl li::before{content:"";position:absolute;left:-7px;top:6px;width:12px;height:12px;border-radius:50%;background:var(--card);border:2px solid var(--mute)}
ol.tl li.sec::before{border-color:var(--sec)}ol.tl li.fo::before{border-color:var(--dev)}ol.tl li.ok::before{border-color:var(--ok)}ol.tl li.warn::before{border-color:var(--warn)}
ol.tl b{font-weight:600}ol.tl p{margin:2px 0 0;color:var(--mute);font-size:13.5px;overflow-wrap:anywhere}
.run{display:flex;gap:8px;flex-wrap:wrap}.run span{background:var(--chip);border-radius:8px;padding:6px 10px;font-size:13px}
code{font-size:12.5px;background:var(--chip);padding:1px 5px;border-radius:5px;overflow-wrap:anywhere}"""


def _env_pos(flow_info, base):
    """The base's position among the flow's environments (the promotion order); an unknown base goes to the end."""
    return flow_info["ambientes"].index(base) if base in flow_info["ambientes"] else len(flow_info["ambientes"])


def _points(flows):
    """{branch: point class} of the flows' environments, in the order they appear: production `p`, the first environment `d`, the middle ones `s`."""
    points = {}
    for flow_info in flows:
        for b in flow_info["ambientes"]:
            points.setdefault(b, "p" if b == flow_info["producao"] else "d" if b == flow_info["ambientes"][0] else "s")
    return points


def _step_done(item_list, flow_info):
    """The feature is over: it has a PR, none is open and nothing is left to promote (it entered production, or only closed PRs remain)."""
    return bool(item_list) and all(i["estado"] != "aberto" for i in item_list) and pr_next(item_list, flow_info) in (None, f"in {flow_info['producao']}")


def merge_order(groups, ts):
    """The PR groups (`{task, ligado_em, item_list}`, one per feature) in the order they should enter, by the tickets' `Blocked by`.

    The feature of ticket N waits for the features of the tickets N depends on, directly or through another ticket (the middle ticket may not even have a PR).
    With no declared dependency, or no ticket, the link order applies. Each group comes back with `waiting` (the tasks it depends on) and, in a cycle
    or behind one, `cycle`: the rest comes out in link order instead of locking up the page."""
    by_num = {t["num"]: t for t in ts}
    task_number = {t["task"]: t["num"] for t in ts if t.get("task")}

    def ancestors(number):
        seen, stack = set(), list(_dict(by_num.get(number)).get("blocked_by") or [])
        while stack:
            n = stack.pop()
            if n not in seen:
                seen.add(n)
                stack += _dict(by_num.get(n)).get("blocked_by") or []
        return seen

    deps = {}
    for g in groups:
        above = ancestors(task_number[g["task"]]) if g["task"] in task_number else set()
        deps[g["task"]] = {h["task"] for h in groups if h["task"] != g["task"] and task_number.get(h["task"]) in above}
    missing = sorted(groups, key=lambda g: (g.get("ligado_em") or "", g["task"]))
    order, placed = [], set()
    while missing:
        g = next((g for g in missing if deps[g["task"]] <= placed), None)
        if g is None:
            order += [{**x, "espera": sorted(deps[x["task"]]), "ciclo": True} for x in missing]
            break
        missing.remove(g)
        order.append({**g, "espera": sorted(deps[g["task"]]), "ciclo": False})
        placed.add(g["task"])
    return order


def _pr_reading(i, now_at=None):
    """The CI and the conflict that the poll stored for an open PR: {marcas: [(icon, text)], pronto, velha, idade}. With no reading or with an old reading
    (more than OLD_READ_MIN minutes) the PR does not count as ready. PR already resolved: None."""
    if i.get("estado") != "aberto":
        return None
    ci = _dict(i.get("ci"))
    if not ci:
        return {"marcas": [("?", "no CI reading")], "pronto": False, "velha": False, "idade": None}
    now_at = time.time() if now_at is None else now_at
    age = (now_at - (ci.get("lido_em") or 0)) / 60
    marks = ([("✗", ", ".join(ci["falhas"]))] if ci.get("falhas") else []) + ([("⚠", "conflict")] if ci.get("mergeable") == "CONFLICTING" else []) \
        + ([("⏳", "CI running")] if ci.get("rodando") else []) + ([("?", "conflict not computed yet")] if ci.get("mergeable") == "UNKNOWN" and not ci.get("falhas") and not ci.get("rodando") else [])
    old = age > OLD_READ_MIN
    note = [("ℹ", "failure in another environment: " + ", ".join(ci["outro_ambiente"]))] if ci.get("outro_ambiente") else []  # informs, does not block
    return {"marcas": (marks or [("✓", "ready")]) + note, "pronto": not marks and not old, "velha": old, "idade": age}


def _pr_contract(i, now_at=None):
    """The PR as the digest contract (contratos/digest-v1.md) asks for it: state in uppercase and the title, or `PR #N` if gh never gave it.
    An open PR also carries what the poll read from the CI: mergeable, falhas, rodando, lidoEm, velha and pronto."""
    out = {"numero": i.get("numero"), "url": i["url"], "base": i.get("base"), "estado": GH_STATE.get(i["estado"], "OPEN"),
           "titulo": i.get("titulo") or f"PR #{i.get('numero')}"}
    l = _pr_reading(i, now_at)
    if l:
        ci = _dict(i.get("ci"))
        out.update(mergeable=ci.get("mergeable"), falhas=ci.get("falhas") or [], rodando=ci.get("rodando") or [], lidoEm=ci.get("lido_em"), velha=l["velha"], pronto=l["pronto"])
    return out


def _merge_tree(a, b):
    """The files in conflict when merging branches `a` and `b` (`git merge-tree`, touching nothing), [] if they merge cleanly, None if it could not be determined
    (branch outside the clone, old git). Searches the repositories in ORQ_REPOS, by `origin/<branch>` and then by the local branch."""
    for repo in [r for r in os.environ.get("ORQ_REPOS", orqpaths.CODE).split(":") if r]:
        refs = [next((r for r in (f"origin/{h}", h) if _git(repo, "rev-parse", "--verify", "-q", r + "^{commit}")), None) for h in (a, b)]
        if not all(refs):
            continue
        try:
            r = subprocess.run(["git", "-C", repo, "merge-tree", "--write-tree", "--name-only", "--no-messages", *refs], capture_output=True, text=True, timeout=30)
        except (subprocess.TimeoutExpired, OSError):
            return None
        if r.returncode == 0:
            return []
        return [x for x in r.stdout.splitlines()[1:] if x.strip()] if r.returncode == 1 else None
    return None


def _mark_steps(steps, prs, now_at=None):
    """Puts on each step (format of `_declared_steps`) `pronto` (to do and all open PRs ready) and `notices` (a PR conflicts with a PR of an earlier step in the queue:
    `git merge-tree` between the branches). Returns the number of the next step to merge: the first to do with everything ready, or None."""
    heads = {i.get("numero"): i.get("head") for i in prs.get("itens") or []}
    for n, p in enumerate(steps):
        open_items = [i for i in p["prs"] if i["estado"] == "OPEN"]
        p["pronto"] = not p["feito"] and bool(open_items) and all(i.get("pronto") for i in open_items)
        p["avisos"] = []
        for b in open_items:
            for q in steps[:n]:
                for a in (x for x in q["prs"] if x["estado"] == "OPEN" and heads.get(x["numero"]) and heads.get(b["numero"])):
                    files = _merge_tree(heads[a["numero"]], heads[b["numero"]])
                    if files:
                        p["avisos"].append(f"#{b['numero']} conflicts with #{a['numero']} (step {q['passo']}): {', '.join(files[:5])}" + (f" and {len(files) - 5} more" if len(files) > 5 else ""))
    return next((p["passo"] for p in steps if p["pronto"]), None)


def _queue_ro():
    d = _dict(_read_json(_path(MERGE_QUEUE_FILE)))
    return {"passos": [p for p in d.get("passos") or [] if isinstance(p, dict) and isinstance(p.get("passo"), int)]}


def _mutate_queue(fn):
    """Reads fila.json, applies fn(data) and writes with tmp + rename, under fila.lock. Only the coordinator writes (`orq queue`)."""
    with _lock("merge-queue.lock"):
        d = _queue_ro()
        out = fn(d)
        d["passos"].sort(key=lambda p: p["passo"])
        _write_json(_path(MERGE_QUEUE_FILE), d, indent=2)
    return out


def queue_add(step, item_name, by, ticket_numbers):
    """Declares (or replaces) step `step` of the merge order: name, why and the PRs, which must already be linked (`orq pr ligar`)."""
    if step < 1 or not ticket_numbers:
        raise ValueError("step needs a number from 1 and at least one PR")
    bound_items = {i.get("numero") for i in _prs_ro()["itens"]}
    still_missing = [n for n in ticket_numbers if n not in bound_items]
    if still_missing:
        raise ValueError(f"PR {', '.join('#' + str(n) for n in still_missing)} is not linked to any task (orq pr link <task> <url>)")

    def write(d):
        d["passos"] = [p for p in d["passos"] if p["passo"] != step] + [{"passo": step, "nome": item_name, "por": by, "prs": list(dict.fromkeys(ticket_numbers)), "feito": False}]
        append_event({"tipo": "fila", "op": "add", "passo": step, "nome": item_name, "prs": ticket_numbers})
        return d["passos"][-1]

    return _mutate_queue(write)


def queue_mark(step, op):
    """`done` marks the step as done by hand (holds even with an open PR); `rm` removes the step from the queue."""
    def change(d):
        found_item = next((p for p in d["passos"] if p["passo"] == step), None)
        if not found_item:
            raise ValueError(f"step {step} does not exist in the queue (orq queue list)")
        if op == "rm":
            d["passos"].remove(found_item)
        else:
            found_item["feito"] = True
        append_event({"tipo": "fila", "op": op, "passo": step})

    _mutate_queue(change)


def _txt_pr(i, raw):
    """`#N state` plus what the poll read from the CI: ✓, ✗ with the checks' names, ⚠ conflict, ⏳ CI running, and `(reading old, N min)`."""
    l = _pr_reading(raw.get(i["numero"]) or {})
    if not l:
        return f"#{i['numero']} {i['estado'].lower()}"
    return f"#{i['numero']} {i['estado'].lower()} " + " ".join(f"{ic} {tx}" if ic != "✓" else ic for ic, tx in l["marcas"]) + (f" (stale reading, {l['idade']:.0f} min)" if l["velha"] else "")


def queue_list():
    """Lines of `orq queue listing`: the step, the name, whether it is done and each PR with the state, the CI and the conflict that the poll stored; the conflict notices
    between steps; and the next step to merge (the first to do with all PRs ready). This is where "can I merge?" gets answered."""
    prs = _prs_ro()
    steps, raw = _declared_steps(_queue_ro(), prs), {i.get("numero"): i for i in prs["itens"]}
    next_item = _mark_steps(steps, prs)
    line_list = []
    for p in steps:
        line_list.append(f"{p['passo']}  {p['nome']}  [{'done' if p['feito'] else 'to do'}]  " + ", ".join(_txt_pr(i, raw) for i in p["prs"]) + (f"  — {p['por']}" if p["por"] else ""))
        line_list += [f"   ⚠ {a}" for a in p["avisos"]]
    if steps:
        target = next((p for p in steps if p["passo"] == next_item), None)
        line_list.append(f"Next to merge: step {next_item} ({target['nome']})" if target else "Next to merge: no step ready")
    return line_list


def _declared_steps(queue, prs):
    """The `orq queue` steps in the contract's format, with each PR's state coming from prs.json. `done` = marked by hand, or the leftover PRs all
    MERGED or CLOSED (a PR unlinked later disappears from the step)."""
    by_num = {i.get("numero"): i for i in prs.get("itens") or []}
    out = []
    for p in queue["passos"]:
        item_list = [_pr_contract(by_num[n]) for n in p.get("prs") or [] if n in by_num]
        out.append({"passo": p["passo"], "nome": p.get("nome") or "", "por": p.get("por") or "", "prs": item_list,
                    "feito": bool(p.get("feito")) or (bool(item_list) and all(i["estado"] != "OPEN" for i in item_list))})
    return out


def _agent_state(a):
    """The state of a live worker in human text: the phase it declared, or the state without orq's jargon."""
    if a["estado"] == "rodando":
        return a.get("fase") or "running"
    return {"travado": "stuck", "limite": "stopped at the plan limit", "parado": "stopped at the prompt", "perguntando": "waiting for your answer", "nao_comecou": "not started", "aguardando_integracao": "waiting for integration",
            "sem_terminal": "no terminal", "hibernado": f"hibernated since {_hora_local(a.get('hibernado_desde'))}"}.get(a["estado"], a["estado"])


def _log_line(e, title):
    """An entry of the contract's `line` from a log event, or None if the event does not count. type: ok, sec (failure), info."""
    type_name = e.get("tipo")
    if type_name == "worker_done":
        ok = e.get("outcome") == "succeeded"
        return {"tipo": "ok" if ok else "sec", "titulo": ("Worker delivered" if ok else "Worker failed") + f": {_quote(e.get('subject'), 100)}",
                "detalhe": title.get(e.get("task")) or ""}
    if type_name == "pr" and e.get("op") in ("entrou", "fechou"):
        has_entered = e["op"] == "entrou"
        return {"tipo": "ok" if has_entered else "sec", "titulo": f"PR #{e.get('numero')} " + (f"entered {e.get('base')}" if has_entered else "closed without merge"),
                "detalhe": title.get(e.get("task")) or ""}
    if type_name == "resposta_coordenador" and e.get("texto"):
        return {"tipo": "info", "titulo": _quote(e["texto"].strip().splitlines()[0], 90), "detalhe": e["texto"]}
    if type_name == "resumo_add" and e.get("texto"):
        return {"tipo": "info", "titulo": _quote(e["texto"].strip().splitlines()[0], 90), "detalhe": e["texto"]}
    if type_name == "resposta_worker" and e.get("texto"):
        return {"tipo": "info", "titulo": "Coordinator answered a worker", "detalhe": e["texto"]}
    if type_name in ("resposta", "resposta_lavish") and e.get("resposta"):
        return {"tipo": "ok", "titulo": f"Decision: {e.get('header') or e.get('item') or ''}", "detalhe": e["resposta"]}
    if type_name == "pend" and e.get("op") == "done" and e.get("resposta"):
        return {"tipo": "ok", "titulo": f"Pending item {e.get('pend')} closed", "detalhe": e["resposta"]}
    return None


def panel_tickets(ts, open_state):
    """The digest's `tickets_orq`: the open tickets (not resolved or wontfix) with group (ready, blocked, in progress), the blockers still
    open, the task and the state of its live worker; plus the 5 most recent resolved ones, with the closing date (the backlog's, or the file's)."""
    live = {a.get("task"): _agent_state(a) for a in _dict(open_state).get("agentes") or [] if a.get("estado") in ANDA}
    closed = ("resolved", "wontfix")
    open_items = {t["num"] for t in ts if t["status"] not in closed}
    output, projects = [], groups()
    for t in ts:
        if t["status"] in closed:
            continue
        blockers = [n for n in t["blocked_by"] if n in open_items]
        group_name = "bloqueado" if blockers else "andamento" if t["status"] == "claimed" else "pronto"
        output.append({"num": t["num"], "titulo": t["titulo"], "status": t["status"], "grupo": group_name, "bloqueios": blockers,
                      "task": t["task"], "worker": live.get(t["task"]), "arquivo": t["arquivo"],
                      "projeto": group_of(projects, title=t["titulo"])[0]})
    done_items = []
    for t in ts:
        if t["status"] == "resolved":
            with contextlib.suppress(OSError, TypeError):  # with no closing date (backlog) and no file, the ticket stays out of the resolved ones
                done_items.append({"num": t["num"], "titulo": t["titulo"], "em": t.get("fechado_em") or datetime.fromtimestamp(os.path.getmtime(t["arquivo"])).strftime("%Y-%m-%d"),
                               "arquivo": t["arquivo"]})
    return {"abertos": output, "resolvidos": sorted(done_items, key=lambda x: (x["em"], x["num"]), reverse=True)[:5]}


def build_digest(events, prs, pending_items, open_state, ts, queue, since, now_at, turns=None, away_alias=None, e2e=None, machine=None):
    """The digest as data in the contract's format (contratos/digest-v1.md) plus what only the page uses. Only orq files: no gh and no Orca.

    fila = the order the coordinator declared (`orq queue`); with no declared step, it comes from the order by the tickets' `Blocked by`, one step per
    feature. features = one group per task with a linked PR. linha = what happened since `since` (the page always shows it; the contract
    file only carries it with away mode on)."""
    by_task = {t["task"]: t for t in ts if t.get("task")}
    by_task_prs = {}
    for i in prs.get("itens") or []:
        by_task_prs.setdefault(i["task"], []).append(i)
    flow_info = {task: task_flow(task, events) for task in by_task_prs}
    groups = [{"task": task, "ligado_em": min(i.get("ligado_em") or "" for i in item_list),
               "itens": sorted(item_list, key=lambda i: (_env_pos(flow_info[task], i.get("base")), i.get("numero") or 0))}
              for task, item_list in by_task_prs.items() if not _old_pr(item_list, now_at)]
    title = {g["task"]: _dict(by_task.get(g["task"])).get("titulo") or g["task"] for g in groups}
    done = {g["task"]: _step_done(g["itens"], flow_info[g["task"]]) for g in groups}
    order = merge_order(groups, ts)
    derived = [{"passo": n, "nome": title[g["task"]], "por": (f"Waits for: {', '.join(title[t] for t in g['espera'] if not done[t])}." if [t for t in g["espera"] if not done[t]] else ""),
                 "prs": [_pr_contract(i) for i in g["itens"]], "feito": done[g["task"]], "ticket": _dict(by_task.get(g["task"])).get("num"),
                 "proximo": None if done[g["task"]] else pr_next(g["itens"], flow_info[g["task"]]), "ciclo": g["ciclo"]} for n, g in enumerate(order, 1)]
    declared = _declared_steps(queue, prs)
    next_step = _mark_steps(declared or derived, prs)
    features = []
    for g in order:
        tagged_items = [i for i in g["itens"] if i.get("tag") or i.get("nota")]
        features.append({"tag": next((i["tag"] for i in tagged_items if i.get("tag")), None), "nome": title[g["task"]],
                         "nota": next((i["nota"] for i in tagged_items if i.get("nota")), ""), "prs": [_pr_contract(i) for i in g["itens"]]})
    today = now_at.astimezone().date()
    pending = [{**i, "depois": bool(pending_after(i, today))} for i in _dict(pending_items).get("itens", []) if isinstance(i, dict)]
    running = sorted(({"titulo": a.get("titulo") or "untitled worker", "estado": _agent_state(a), "desde": a.get("desde"),
                       **({"prioridade": a["prioridade"]} if a.get("prioridade") else {})}
                      for a in reassess(_dict(open_state).get("agentes") or [], events, now_at, turns) if a.get("estado") in (*ANDA, "hibernado")), key=lambda r: r.get("prioridade") or 2)  # highest first
    if machine:  # the slots and the dispatch queue (ticket 79): one extra key, `running` still holds only workers and the E2E queue
        machine = {**machine, "ocupadas": sum(not r["estado"].startswith("hibernated") for r in running), "livres": max(machine["max_workers"] - sum(not r["estado"].startswith("hibernated") for r in running), 0)}
    if e2e:  # the E2E queue is one extra line in `running`: `stuck_lock` when it does not move
        running.append({"titulo": e2e_line(e2e).split(". PRESA")[0], "estado": "presa" if e2e["presa"] else "rodando", "desde": None})
    line = [{"ts": e["ts"], **x} for e in events if (e.get("ts") or "") >= since and (x := _log_line(e, title))]
    return {"versao": 1, "geradoEm": now_at.strftime("%Y-%m-%dT%H:%M:%SZ"), "ausente": {"ligado": bool(away_alias), "desde": _dict(away_alias).get("ligada_em")},
            "fila": declared or derived, "proximoPasso": next_step, "features": features, "pendencias": pending, "linha": line[-DIGEST_LINES:], "rodando": running, "maquina": machine,
            "tickets_orq": panel_tickets(ts, open_state),
            "pagina": {"data": now_at.astimezone().strftime("%Y-%m-%d"), "gerado": now_at.astimezone().strftime("%H:%M"), "desde": since, "poll": prs.get("ultimo_poll"),
                       "linha_antes": max(0, len(line) - DIGEST_LINES), "declarada": bool(declared),
                       "pontos": _points(list(flow_info.values()) or [task_flow(None, events)])}}


def digest_json(d):
    """What goes into atual.json: the contract's keys, each step only with its own, and an empty `line` with away mode off."""
    return {"versao": d["versao"], "geradoEm": d["geradoEm"], "ausente": d["ausente"],
            "fila": [{k: p[k] for k in ("passo", "nome", "por", "prs", "feito", "pronto", "avisos")} for p in d["fila"]], "proximoPasso": d["proximoPasso"],
            "features": d["features"], "pendencias": d["pendencias"], "linha": d["linha"] if d["ausente"]["ligado"] else [], "rodando": d["rodando"],
            "tickets_orq": d["tickets_orq"],
            **({"ausencia": d["ausencia"]} if d.get("ausencia") else {}),
            **({"retro": d["retro"]} if d.get("retro") else {})}  # additive: with no recorded round the v1 contract stays as it was


def html_digest(d):
    """The digest page (a single file, with the CSS inside and light/dark by the system). All outside text goes through html.escape."""
    e, pg = html.escape, d["pagina"]

    def chip(i):
        cls, item_name = DIGEST_STATE.get(i["estado"], ("o", i["estado"]))
        return (f'<a class="pr {cls}" href="{e(i["url"])}" title="{e(i.get("base") or "base not known yet")}">'
                f'<span class="dot {pg["pontos"].get(i.get("base"), "d")}"></span>#{e(str(i.get("numero")))}<em>{item_name}</em></a>')

    steps = []
    for g in d["fila"]:
        notes = ([f"Ticket {g['ticket']}."] if g.get("ticket") else []) + ([g["por"]] if g["por"] else []) + ([f"Next: {g['proximo']}."] if g.get("proximo") else [])
        notice = '<p class="aviso">Circular dependency between tickets: the order is the link order.</p>' if g.get("ciclo") else ""
        steps.append(f'<li class="{"feito" if g["feito"] else ""}"><span class="num">{"✓" if g["feito"] else g["passo"]}</span><div><b>{e(g["nome"])}</b>'
                      f'<p>{e(" ".join(notes))}</p>{notice}<div class="prs">{"".join(chip(i) for i in g["prs"])}</div></div></li>')
    queue = f'<ol class="fila">{"".join(steps)}</ol>' if steps else '<p class="sub">No PR linked: nothing to merge.</p>'
    origin_note = "Order declared with `orq queue`." if pg["declarada"] else "By the tickets' Blocked by, since no step was declared with `orq queue`."
    pending = "".join(f'<div class="card dec"><h3>{e(str(p.get("titulo") or p.get("id")))}</h3><p>{e(str(p.get("tipo") or ""))} · <code>{e(str(p.get("id") or ""))}</code>'
                   f'{" · Later" if p["depois"] else ""}</p></div>' for p in d["pendencias"])
    rod = "".join(f'<span>{e(r["titulo"])} [{e(r["estado"])}]</span>' for r in d["rodando"])
    m = d.get("maquina")
    vagas = (f'<p class="sub">Machine: {m["ocupadas"]}/{m["max_workers"]:g} slots busy, {m["livres"]:g} free, {m["fila"]} in the dispatch queue.</p>' if m else "")
    line = "".join(f'<li class="{ {"sec": "sec", "info": "fo"}.get(x["tipo"], "ok") }"><b>{e(x["titulo"])}</b><p>{_hora_local(x["ts"])} {e(x["detalhe"])}</p></li>' for x in d["linha"])
    before = f'<p class="sub">+{pg["linha_antes"]} before these.</p>' if pg["linha_antes"] else ""
    poll = (f"The PR state is from the poll at {datetime.fromtimestamp(pg['poll']).strftime('%H:%M')}; this page does not query GitHub."
            if isinstance(pg.get("poll"), (int, float)) else "The PR state has not been read by any poll yet (`orq pr poll`); this page does not query GitHub.")
    since = f"Since {_hora_local(pg['desde'])}." if pg["desde"] else "Since the start of the log."
    retro = ('<h2>Failures per retro round</h2><ul class="sub">' + "".join(f'<li>until {e(r["ate"][:10])}: {r["falhas"]} failure(s)</li>' for r in d["retro"]) + "</ul>") if d.get("retro") else ""
    absence = f'<h2>Away report</h2><pre>{e(chr(10).join(d["ausencia"]))}</pre>' if d.get("ausencia") else ""
    legenda = "".join(f'<span><span class="dot {c}"></span> {e(n)}</span>' for n, c in pg["pontos"].items())
    before_of = [n for n, c in pg["pontos"].items() if c != "p"]  # the order of the environments before production, within each step
    after = f" Within each step, {' before '.join(before_of)}." if len(before_of) > 1 else ""
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>Digest {e(pg["data"])}</title><style>{DIGEST_CSS}</style></head><body><main>'
            f'<h1>Waiting on you</h1><p class="sub">{e(pg["data"])}. {e(since)} Generated at {e(pg["gerado"])}. {e(poll)}</p>'
            f'<div class="legend">{legenda}</div>'
            f'<h2>Merge order</h2><p class="sub">{e(origin_note)}{after}</p>'
            f'{queue}<h2>With you</h2>{f"<div class=grid>{pending}</div>" if pending else "<p class=sub>Nothing waiting on you.</p>"}'
            f'<h2>Running now</h2>{f"<div class=run>{rod}</div>" if rod else "<p class=sub>No worker running: nothing alive.</p>"}{vagas}'
            f'<h1 style="margin-top:36px">What happened</h1>{before}'
            f'{f"<ol class=tl>{line}</ol>" if line else "<p class=sub>Nothing since then.</p>"}{absence}{retro}</main></body></html>')


def digest_generate(now_at=None, since=None, with_html=False):
    """Builds the digest from the local files and writes ORQ_HOME/digest/atual.json (the contract the panel reads) and, with `with_html`, digest/<date>.html.
    Each file is swapped in one go. Returns (data, json path, html path or None).

    The window of `line` and of the page: `since`, otherwise the moment away mode turned on, otherwise the user's last message. No network: the
    PRs' state is whatever the poll (`orq pr poll`, the manager panel) left in prs.json."""
    now_at = now_at or datetime.now(timezone.utc)
    events, away_alias = read_events(), _dict(_cursor_ro().get("ausente")) or None
    window = since if since is not None else (away_alias or {}).get("ligada_em") or last_from_user(events, now_at)
    d = build_digest(events, _prs_ro(), _pending_ro(), _read_json(_path("open.json")), tickets(), _queue_ro(), window, now_at, _turns_ro(), away_alias, e2e_queue(),
                    {"max_workers": machine_cfg()["max_workers"], "fila": len(dispatch_queue_items())})
    d["retro"] = [{"ate": r["ate"], "falhas": r["falhas"], "metricas": r["metricas"]} for r in _retro_rounds()[-4:]]  # the latest rounds of `orq retro --write_out`
    with contextlib.suppress(OSError), open(_path(os.path.join(DIGEST, "ausencia.md")), encoding="utf-8") as f:  # the last `orq away off`
        d["ausencia"] = f.read().rstrip().splitlines()
    os.makedirs(_path(DIGEST), exist_ok=True)
    _write_json(_path(os.path.join(DIGEST, "atual.json")), digest_json(d), indent=2)
    page_data = None
    if with_html:
        page_data = _path(os.path.join(DIGEST, d["pagina"]["data"] + ".html"))
        _write(page_data, html_digest(d))
    return d, _path(os.path.join(DIGEST, "atual.json")), page_data


def digest_open(path):
    """Opens the digest page in an Orca tab (`orca tab create --url file://…`, in the worktree of the calling terminal)."""
    orca("create", "--url", pathlib.Path(path).as_uri(), area="tab", timeout=10)


def _away_marker(hora):
    """The `state/away` marker that statusline.sh reads without Python: the local time when the mode turned on, or nothing (erased). A disk failure does not take orq down."""
    path = os.path.join(HOME, "estado", "away")
    try:
        if hora is None:
            os.remove(path)
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(hora)
    except OSError:
        pass


NIGHT_FAILURES = 3  # consecutive failures that close the dispatch (night and away)
AWAY_UNTIL = "08:00"  # away on without --until: the budget ends at the next 08:00 local


def away_on(until_at=AWAY_UNTIL, max_dispatches=None, max_failures=NIGHT_FAILURES):
    """Turns on away mode: the coordinator's Stop updates the digest on every reply (`hook_stop`). It also arms the night budget (`noite` in cursor.json,
    marked `via: away` so `orq hook external` stays inert): the end time, the dispatch cap and the failure breaker. Already on, it keeps the original
    `ligada_em` and only re-arms the budget. Returns the stored state."""
    _night_state(until_at, max_dispatches, max_failures)  # refuses a bad flag before anything is written
    was = _dict(_cursor_ro().get("ausente"))
    state_ = was or {"ligada_em": now()}
    if not was:
        _cursor_mut(lambda c: c.__setitem__("ausente", state_))
        _away_marker(_hora_local(state_["ligada_em"]))
        append_event({"tipo": "ausente_ligar"})
    night_on(until_at, max_dispatches, max_failures, via="away")
    return state_


def away_off():
    """Turns off away mode; returns whether it was on."""
    bound = bool(_dict(_cursor_ro().get("ausente")))
    _cursor_mut(lambda c: c.pop("ausente", None))
    _away_marker(None)
    if bound:
        append_event({"tipo": "ausente_desligar"})
        night_off()  # away on armed the night budget; off disarms both
    return bound


def away_enabled():
    """Is away mode on? cursor.json decides (the `state/away` marker is only the mirror the statusline reads)."""
    return bool(_dict(_cursor_ro().get("ausente")))


def away_lines(cur):
    """The away mode state for `orq away_alias`: one line, plus the notice that no one runs the PR poll without the manager."""
    a = _dict(_dict(cur).get("ausente"))
    if not a:
        return ["away mode off"]
    return [f"away mode on since {_hora_local(a.get('ligada_em'))} (the HUD shows away since ... via statusline.sh): the Stop of each reply updates {_path(os.path.join(DIGEST, 'atual.json'))}",
            *([] if _manager_cfg() else ["warning: with no agent manager bound nobody runs the PR poll; run `orq pr poll` (the Stop does not call gh)"])]


PANEL_URL = "http://localhost:8765/"


COORDINATOR_TURN = ("entrada", "coordenador_retomou", "coordenador_parou")  # the events that start a coordinator turn or end the previous stop


def coordinator_stops(evs, end_ts):
    """[(start, end, motivo)] of the coordinator's stops longer than LACUNA_S: from a `coordenador_parou` to the next event of COORDINATOR_TURN (the user's prompt,
    a notice typed by the manager or the next Stop), or to `end_ts` when none came."""
    out, open_stop = [], None
    for e in evs:
        if e.get("tipo") in COORDINATOR_TURN:
            if open_stop:
                out.append((open_stop["ts"], e["ts"], open_stop.get("motivo")))
            open_stop = e if e["tipo"] == "coordenador_parou" else None
    if open_stop:
        out.append((open_stop["ts"], end_ts, open_stop.get("motivo")))
    return [x for x in out if (_dt(x[1]) - _dt(x[0])).total_seconds() > LACUNA_S]


def away_report(since, live=None, now_at=None):
    """The absence report in pt-BR lines, only from what orq stores (events.jsonl and the pending item list), no LLM, only with what was born after
    `since`. Sections in order: decisions, problems, coordinator stopped, summaries, deliveries, PRs, tickets, then the morning card's (dispatch stops, dirty
    worktrees, not pushed, manager, log gap, commands to paste); an empty section does not appear. `live` is `_live_states` for the dispatches still running."""
    now_at = now_at or datetime.now(timezone.utc)
    all_events = read_events()
    evs = [e for e in all_events if (e.get("ts") or "") >= since]
    stops = [(a, b, why, int((_dt(b) - _dt(a)).total_seconds() // 60)) for a, b, why in coordinator_stops(evs, now_at.strftime("%Y-%m-%dT%H:%M:%SZ"))]
    stop_line = lambda x: f"{_hora_local(x[0])} to {_hora_local(x[1])} ({x[3]} min): {x[2]}"  # noqa: E731
    card = _card_parts(evs, since, now_at, True, _cursor_ro(), live)
    born = {e["pend"] for e in evs if e.get("tipo") == "pend" and e.get("op") == "add"}
    open_entries = [i for i in _load_pending()["itens"] if i.get("id") in born]
    pending_line = lambda i: f"{i['id']}: {_quote(i.get('titulo'), 90)} — {i.get('link') or 'no link'}"  # noqa: E731
    decisions = [pending_line(i) for i in open_entries if i.get("tipo") == "decisao"]
    ok = lambda e: e.get("outcome") == "succeeded"  # noqa: E731
    sections = [
        ("Decisions left for you", decisions),
        ("Other open pending items", [f"{i.get('tipo')} {pending_line(i)}" for i in open_entries if i.get("tipo") != "decisao"]),
        ("Problems", [f"worker failed: {_quote(e.get('subject'), 100)}" for e in evs if e.get("tipo") == "worker_done" and not ok(e)]
         + [f"alert: {_quote(e.get('texto') or e.get('alerta') or e.get('tipo'), 100)}" for e in evs if e.get("tipo") == "alerta"]
         + [f"gate refused: {e.get('gate')}" for e in evs if e.get("tipo") == "gate_falha"]
         + [f"PR closed without merge: {e.get('url')}" for e in evs if e.get("tipo") == "pr" and e.get("op") == "fechou"]
         + [f"coordinator stopped with work waiting, {stop_line(x)}" for x in stops if x[2] == "trabalho_esperando"]),
        ("Coordinator stopped", [stop_line(x) for x in stops]),
        ("Summaries", [f"{_hora_local(e['ts'])} {_quote(e.get('texto'), 400)}" for e in evs if e.get("tipo") == "resumo" and e.get("texto")]),
        ("Worker deliveries", [_quote(e.get("subject"), 100) for e in evs if e.get("tipo") == "worker_done" and ok(e)]),
        ("PRs", [f"{'opened' if e['op'] == 'ligar' else 'merged into ' + str(e.get('base'))}: {e.get('url')}" for e in evs if e.get("tipo") == "pr" and e.get("op") in ("ligar", "entrou")]),
        ("Tickets", [f"{'opened' if e['op'] == 'novo' else 'closed'} {e.get('ticket')}" + (f": {_quote(e.get('titulo'), 80)}" if e.get("titulo") else "")
                     for e in evs if e.get("tipo") == "ticket" and e.get("op") in ("novo", "fechar")]),
        ("Dispatch stops", card["lines"]),
        ("Dirty worktrees", card["dirty"][1]),
        ("Not pushed", card["without_push"][1]),
        ("Manager", [card["manager"]] if card["dispatches"] else []),
        ("Log gap", [card["gap"]] if card["gap"] else []),
        ("To paste", card["cmds"]),
    ]
    line_list = [f"Away report (since {_hora_local(since)})"]
    for title, item_list in sections:
        if item_list:
            line_list += ["", f"{title} ({len(item_list)}):", *[f"- {i}" for i in item_list]]
    return line_list if len(line_list) > 1 else [*line_list, "", "Nothing happened in the window."]


def write_away_report(line_list):
    """Writes the report to `digest/absence.md` (the digest shows it) and to `<ORQ_RESUMOS|./.scratch/resumos>/<date>-absence.md`. Returns the path of the second."""
    txt = "\n".join(line_list) + "\n"
    os.makedirs(_path(DIGEST), exist_ok=True)
    _write(_path(os.path.join(DIGEST, "ausencia.md")), txt)
    folder = os.environ.get("ORQ_RESUMOS") or os.path.join(os.getcwd(), ".scratch", "resumos")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{datetime.now().strftime('%Y-%m-%d')}-ausencia.md")
    _write(path, txt)
    return path


def away(op=None, until_at=None, max_dispatches=None, max_failures=NIGHT_FAILURES):
    """`orq away` / `/away`: turns away mode on, off or shows it; without op it toggles. Returns the lines to print.
    On, the flags set the night budget (`--until` default 08:00, `--max-dispatches` default none, `--max-failures` default 3); on again with no flag keeps the budget as it is.
    On turning off it shows the link to the 8765 panel, the count and the absence report (written to a file; the path goes last); it does not open a tab."""
    cur = _dict(_cursor_ro().get("ausente"))
    op = {"ligar": "on", "desligar": "off"}.get(op, op) or ("off" if cur else "on")
    if op == "status":
        return away_lines(_cursor_ro())[:1]
    if op == "on":
        if not cur or not night_active(_cursor_ro()) or until_at or max_dispatches is not None or max_failures != NIGHT_FAILURES:
            away_on(until_at or AWAY_UNTIL, max_dispatches, max_failures)
        since = _hora_local(_dict(_cursor_ro().get("ausente")).get("ligada_em"))
        return [f"away mode on since {since}; the digest records each reply", *night_lines(_cursor_ro(), read_events())[1:]]
    n = len(digest_generate()[0]["linha"]) if cur else 0
    away_off()
    if not cur:
        return [f"away mode off; {n} timeline entries, see the dashboard {PANEL_URL}"]
    rel = away_report(cur["ligada_em"], _live_states(read_events(), cur["ligada_em"]))
    file_path = write_away_report(rel)
    digest_generate()  # atual.json already carries the report
    return [f"away mode off; {n} timeline entries, see the dashboard {PANEL_URL}", "", *rel, "", f"Report saved to {file_path}; hand it to the user in the first reply."]


def _last_answer(ev):
    """The text of the coordinator's last reply: the Stop's `last_assistant_message`, otherwise the end of the transcript (`transcript_path`); "" with neither."""
    text_value = ev.get("last_assistant_message")
    if isinstance(text_value, str) and text_value.strip():
        return text_value
    try:
        with open(ev["transcript_path"], "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - TRANSCRIPT_END))
            line_list = f.read().decode("utf-8", "replace").splitlines()
    except (KeyError, OSError, TypeError):
        return ""
    for line in reversed(line_list):
        try:
            m = json.loads(line)
        except ValueError:
            continue
        if isinstance(m, dict) and m.get("type") == "assistant":
            parts = _dict(m.get("message")).get("content")
            texts = [c.get("text", "") for c in parts if isinstance(c, dict) and c.get("type") == "text"] if isinstance(parts, list) else []
            if "".join(texts).strip():
                return "\n".join(texts)
    return ""


SUMMARY_STOP_MIN = 300  # a coordinator reply with more letters than this is "longa" (long): with no summary recorded in the turn, the Stop records one
SUMMARY_STOP_MAX = 400  # letters of the automatic summary


def add_summary(text_value, project=None, cwd=None, auto=False):
    """`orq summary add`: appends the summary to <project repo>/.scratch/resumos/<YYYY-MM-DD>.md (day and time local to orq's clock) and to the log, from where the
    digest takes it. The first line is the title (`## HH:MM — …`), the rest the body. Returns the path. ValueError without text or without a project with repo `path:`."""
    text_value = (text_value or "").strip()
    if not text_value:
        raise ValueError("summary add: no text")
    ps = projects()
    item_name = dispatch_project(project) if project or not cwd else (project_by_folder(ps, os.path.realpath(cwd)) or dispatch_project())
    repo = (ps.get(item_name) or {}).get("repo") or ""
    if not repo.startswith("path:"):
        raise ValueError("summary add: no project with `repo: path:` (use --project <name>; orq projects lists the ones available)")
    ts = now()
    local = _dt(ts).astimezone()
    folder = os.path.join(os.path.expanduser(repo[5:]), ".scratch", "resumos")
    os.makedirs(folder, exist_ok=True)
    title, _, body_text = text_value.partition("\n")
    with open(os.path.join(folder, local.strftime("%Y-%m-%d") + ".md"), "a", encoding="utf-8") as f:
        f.write(f"## {local.strftime('%H:%M')} — {title.strip()}\n" + (body_text.strip() + "\n" if body_text.strip() else "") + "\n")
    append_event({"tipo": "resumo_add", "texto": text_value, "projeto": item_name, **({"auto": True} if auto else {})})
    return os.path.join(folder, local.strftime("%Y-%m-%d") + ".md")


def _stop_summary(ev, text_value):
    """Long reply without `add_summary` since the previous Stop: records the first line (up to SUMMARY_STOP_MAX letters) as the day's summary. No project in the Stop's cwd, nothing."""
    if len(text_value) <= SUMMARY_STOP_MIN:
        return
    event_list = read_events()
    last_item = next((i for i in range(len(event_list) - 1, -1, -1) if event_list[i].get("tipo") == "resposta_coordenador"), -1)
    if any(e.get("tipo") == "resumo_add" for e in event_list[last_item + 1:]):
        return
    line = next((x.strip() for x in text_value.splitlines() if x.strip()), "")
    cwd = ev.get("cwd") or os.getcwd()
    if project_by_folder(projects(), os.path.realpath(cwd)):
        add_summary(line[:SUMMARY_STOP_MAX], cwd=cwd, auto=True)


def digest_no_stop(ev):
    """On the coordinator's Stop, with away mode on: records the reply as a `resposta_coordenador` event and updates the digest. Fail-open: the
    failure goes to the log and the Stop proceeds."""
    if not _dict(_cursor_ro().get("ausente")):
        return
    try:
        text_value = _last_answer(ev)
        if text_value:
            _stop_summary(ev, text_value)  # before the reply enters the log: the turn runs from the previous Stop to this one
            append_event({"tipo": "resposta_coordenador", "texto": text_value[:ANSWER_MAX], "sessao": (ev.get("session_id") or "")[:8]})
        digest_generate()
    except TimeoutError:
        raise  # the hook's 3 s ceiling applies to the whole hook
    except Exception as e:  # noqa: BLE001
        log(f"digest: {type(e).__name__}: {e}")


# ---------- night mode: budget and circuit breaker ----------



def night_active(cur):
    """The night mode state in cursor.json ({ate, ligada_em, max_dispatches, max_failures}) or None if off."""
    n = _dict(cur).get("noite")
    return n if isinstance(n, dict) and n.get("ate") else None


def _since_night(events, night, type_name):
    return [e for e in events if e.get("tipo") == type_name and (e.get("ts") or "") >= night["ligada_em"]]


def consecutive_failures(events, msgs, night):
    """Consecutive failures at the end of the dispatches since `night.ligada_em`, from the inbox's worker_done and the log's `release`.

    Counts: worker_done `failed` without reportPath, or a dispatch released without worker_done (the worker died). Resets: `succeeded`, or `failed`
    with reportPath (the worker explained it cannot be done: its decision, not the environment's). A dispatch still running neither counts nor resets.
    """
    done = {}
    for m in msgs:
        p = _payload(m)
        if m.get("type") == "worker_done" and p.get("dispatchId"):
            done[p["dispatchId"]] = p
    released = {e.get("dispatch") for e in events if e.get("tipo") == "liberar"}
    n = 0
    for e in _since_night(events, night, "despacho"):
        p = done.get(e.get("dispatch"))
        if p is None:
            n += e.get("dispatch") in released
        elif p.get("outcome") == "failed" and not p.get("reportPath"):
            n += 1
        else:
            n = 0
    return n


def night_reason(night, events, msgs, now_at):
    """Why `orq dispatch_worker` should refuse (schedule, dispatch cap or consecutive failures), or None."""
    if now_at >= _dt(night["ate"]):
        return f"past the end of the night ({_hora_local(night['ate'])})"
    cap = night.get("max_despachos")
    if cap is not None and len(_since_night(events, night, "despacho")) >= cap:
        return f"cap of {cap} dispatches for the night"
    failures = night.get("max_falhas") or NIGHT_FAILURES
    if msgs is not None and consecutive_failures(events, msgs, night) >= failures:
        return f"{failures} consecutive worker failures"
    return None


def night_check(now_at=None):
    """Refuses the dispatch with ValueError and records `noite_parou` if night mode went over budget. When off: does nothing."""
    night = night_active(_cursor_ro())
    if not night:
        return
    events = read_events()
    try:
        msgs = orca("inbox", "--limit", "200", timeout=20)["messages"]
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:  # without the inbox only the consecutive failures are left out: schedule and ceiling apply
        log(f"noite: inbox falhou ({type(e).__name__}: {e}); falhas seguidas não conferidas")
        msgs = None
    reason = night_reason(night, events, msgs, now_at or datetime.now(timezone.utc))
    if reason:
        first = not _since_night(events, night, "noite_parou")
        append_event({"tipo": "noite_parou", "motivo": reason})
        if night.get("via") == "away":
            if first:  # one pending item per arming, so `away off` lists it with the decisions
                try:
                    pending_add("away-parou", "decisao", f"away: stopped dispatching: {reason}; re-arm with `orq away on --until HH:MM`")
                except (ValueError, OSError) as e:  # an open item of an earlier stop, or a disk failure: the refusal below still holds
                    log(f"away: pending item of the stop not created ({type(e).__name__}: {e})")
            raise ValueError(f"away mode: {reason}; nothing was dispatched. Leave the decision in orq pend add or re-arm with orq away on --until HH:MM")
        raise ValueError(f"night mode: {reason}; nothing was dispatched. Leave the decision in orq pend add or run orq night off")


def night_lines(cur, events):
    """The two lines the coordinator reads with night mode on (rules; budget or reason for the stop). Empty with the mode off."""
    night = night_active(cur)
    if not night:
        return []
    has_stopped = next((e for e in reversed(_since_night(events, night, "noite_parou"))), None)
    cap = night.get("max_despachos")
    spent = f"{len(_since_night(events, night, 'despacho'))}/{cap if cap is not None else '∞'} dispatches, until {_hora_local(night['ate'])}"
    if night.get("via") == "away":  # away keeps its own rules (ticket 126) and pushes after the audit: only the budget is added
        return ["[orq away] Rules: park decisions with orq pend add (no AskUserQuestion), stop dispatching when the budget runs out.",
                ("[orq away] Stopped dispatching: " + has_stopped["motivo"] + " (" + str(spent) + "); orq away on --until HH:MM re-arms it.") if has_stopped else ("[orq away] Budget: " + str(spent) + ", " + str(night.get("max_falhas") or NIGHT_FAILURES) + " consecutive failures close it.")]
    return ["[orq night] Rules: no AskUserQuestion (park the decision with orq pend add and carry on with what is independent), no push or merge, "
            "stop dispatching when the budget runs out.",
            ("[orq night] Stopped dispatching: " + has_stopped["motivo"] + " (" + str(spent) + "); orq night off lifts it.") if has_stopped else ("[orq night] Budget: " + str(spent) + ", " + str(night.get("max_falhas") or NIGHT_FAILURES) + " consecutive failures close it.")]


def _night_state(until_at, max_dispatches=None, max_failures=NIGHT_FAILURES, now_at=None, via=None):
    """The night state to store (validated, nothing written): the end as the next local HH:MM, the cap and the breaker; `via: away` when away mode armed it."""
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", until_at or "")
    if not m or int(m[1]) > 23 or int(m[2]) > 59:
        raise ValueError(f"--until expects HH:MM (got {until_at!r})")
    if (max_dispatches is not None and max_dispatches < 1) or max_failures < 1:
        raise ValueError("--max-dispatches and --max-failures need a number greater than 0")
    now_at = (now_at or datetime.now(timezone.utc)).astimezone()
    end = now_at.replace(hour=int(m[1]), minute=int(m[2]), second=0, microsecond=0)
    end += timedelta(days=1) if end <= now_at else timedelta(0)
    return {"ate": end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "ligada_em": now(), "max_despachos": max_dispatches, "max_falhas": max_failures, **({"via": via} if via else {})}


def night_on(until_at, max_dispatches=None, max_failures=NIGHT_FAILURES, now_at=None, via=None):
    """Turns night mode on until the next local HH:MM. It is the same `noite` key that away mode arms, so a second call replaces the state, never adds one.
    Returns the recorded state."""
    night = _night_state(until_at, max_dispatches, max_failures, now_at, via)
    _cursor_mut(lambda c: c.__setitem__("noite", night))
    append_event({"tipo": "noite_ligar", **{k: v for k, v in night.items() if k != "ligada_em"}})
    return night


def night_off():
    """Turns night mode off; returns whether it was on."""
    bound = bool(night_active(_cursor_ro()))
    _cursor_mut(lambda c: c.pop("noite", None))
    if bound:
        append_event({"tipo": "noite_desligar"})
    return bound


# ---- night mode: stop reason per dispatch and morning card ----

MANAGER_ALIVE_S = 300  # the last absorb round up to 5 min before the end of the night counts as a live manager
LACUNA_S = 600  # log with no event for longer than this: the machine may have slept
CARD_LINES = 40
CARD_SESSION_H = 12  # the SessionStart injects the card's first line up to 12 h after the end of the night
STOPS = {"orcamento": "parou: orçamento", "decisao": "parou: decisão pendente", "limite": "parou: limite de uso"}  # orq encerrar --parada


def end_reason(dispatch, events, msgs):
    """The named final state of the dispatch that `release` records: entregue (delivered), falhou (failed), parou: <orçamento|decisão pendente|limite de uso> (budget | pending decision | usage limit) (the `terminate --stopped_by`),
    without worker_done. `msgs` is the inbox; None (Orca did not respond) gives "motivo desconhecido" instead of guessing "sem worker_done"."""
    if msgs is None:
        return "motivo desconhecido"
    done = next((_payload(m) for m in msgs if m.get("type") == "worker_done" and _payload(m).get("dispatchId") == dispatch), None)
    if done:
        return "entregue" if done.get("outcome") == "succeeded" else "falhou"
    end_event = next((e for e in reversed(events) if e.get("tipo") == "controle" and e.get("acao") == "encerrar" and e.get("dispatch") == dispatch and e.get("parada")), None)
    return STOPS.get(end_event["parada"], "sem worker_done") if end_event else "sem worker_done"


def worktree_state(path):
    """{caminho, sujo (files with uncommitted changes), without_push (commits not on origin/main)}; whatever git does not report is left out."""
    if not path or not os.path.isdir(path):
        return {"caminho": path} if path else {}
    dirty, n = _git(path, "status", "--porcelain"), _git(path, "rev-list", "--count", "origin/main..HEAD")
    return {"caminho": path, **({"sujo": len(dirty.splitlines())} if dirty is not None else {}), "sem_push": int(n) if n and n.strip().isdigit() else 0}


def _night_window(events):
    """(most recent night_on event, ts of the following night_off or None). No night in the log: (None, None)."""
    night_on_event = next((e for e in reversed(events) if e.get("tipo") == "noite_ligar"), None)
    if not night_on_event:
        return None, None
    return night_on_event, next((e["ts"] for e in events if e.get("tipo") == "noite_desligar" and (e.get("ts") or "") >= night_on_event["ts"]), None)


def _night_end(night_on_event, off_time):
    return _dt(off_time or night_on_event["ate"])


def _card_parts(events, start_at, end, finished, cur, live):
    """What the morning card and the away report share, computed over the events since `start_at` up to `end` (a datetime). Returns a dict: `dispatches` (the
    despacho events), `lines` (one formatted line per dispatch, up to 8), `stopped` (the noite_parou event or None), `dirty` and `without_push` ((count, lines)),
    `manager` (one line), `gap` (one line or None; a gap that opens on a `coordenador_parou` is the coordinator's stop, not a sleeping machine) and `cmds`.
    `live` = {dispatch: worktree_state} for the dispatches still without fim_dispatch."""
    live = live or {}
    end_s = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    in_scope = [e for e in events if (e.get("ts") or "") >= start_at]
    ends = {e["dispatch"]: e for e in in_scope if e.get("tipo") == "fim_dispatch"}
    dispatch_events = [e for e in in_scope if e.get("tipo") == "despacho"]
    line = lambda d: f"{_quote(d.get('titulo') or d['dispatch'], 36)}: {ends[d['dispatch']]['motivo'] if d['dispatch'] in ends else 'running'}"
    states = [(d, {**live.get(d["dispatch"], {}), **ends.get(d["dispatch"], {})}) for d in dispatch_events]
    dirty_rows = [(_quote(d.get("titulo") or d["dispatch"], 30), w) for d, w in states if (w.get("sujo") or 0) > 0]
    without_push = [(_quote(d.get("titulo") or d["dispatch"], 30), w) for d, w in states if (w.get("sem_push") or 0) > 0]
    lap = (cur or {}).get("gerente_volta")
    manager = ("Manager: no absorb round during the night" if not lap or lap < start_at else
               f"Manager: alive (last round {_hora_local(lap)})" if (end - _dt(lap)).total_seconds() <= MANAGER_ALIVE_S else
               f"Manager: stopped at {_hora_local(lap)} (the night ran until {_hora_local(end_s)})")
    marks = [(start_at, ""), *((e["ts"], e.get("tipo")) for e in in_scope if e.get("ts") and e["ts"] <= end_s), *([(end_s, "")] if finished else [])]
    gaps = sorted(((_dt(b) - _dt(a)).total_seconds(), a, b) for (a, kind), (b, _) in zip(marks, marks[1:]) if kind != "coordenador_parou")
    gaps = [x for x in gaps if x[0] > LACUNA_S]
    gap = None
    if gaps:
        seg, a, b = gaps[-1]
        gap = (f"Gap: no event in the log from {_hora_local(a)} to {_hora_local(b)} ({int(seg // 60)} min): the machine may have slept at {_hora_local(a)}."
               + (f" +{len(gaps) - 1} smaller gap{'s' if len(gaps) > 2 else ''}." if len(gaps) > 1 else ""))
    cmds = [f"git -C {w['caminho']} status --short" for _, w in dirty_rows if w.get("caminho")] + [f"git -C {w['caminho']} log --oneline origin/main..HEAD" for _, w in without_push if w.get("caminho")]
    return {
        "dispatches": dispatch_events, "lines": _lim(dispatch_events, 8, line),
        "stopped": next((e for e in reversed(in_scope) if e.get("tipo") == "noite_parou"), None),
        "dirty": (len(dirty_rows), _lim(dirty_rows, 3, lambda t: f"{t[0]}: {t[1]['sujo']} file{'s' if t[1]['sujo'] != 1 else ''} ({t[1].get('caminho') or '?'})")),
        "without_push": (len(without_push), _lim(without_push, 3, lambda t: f"{t[0]}: {t[1]['sem_push']} commit{'s' if t[1]['sem_push'] != 1 else ''} ({t[1].get('caminho') or '?'})")),
        "manager": manager, "gap": gap, "cmds": _lim(cmds, 6, lambda c: c),
    }


def night_card(events, cur, pending, now_at, live=None):
    """The morning card (up to CARD_LINES lines): a pure function of the last night's log. `live` = {dispatch: worktree_state} for the dispatches still without fim_dispatch."""
    night_on_event, off_time = _night_window(events)
    if not night_on_event:
        return ["No night in the log: run orq night on --until HH:MM"]
    start_at, finished = night_on_event["ts"], bool(off_time) or _dt(night_on_event["ate"]) <= now_at
    end = min(_night_end(night_on_event, off_time), now_at)
    c = _card_parts(events, start_at, end, finished, cur, live)
    n = len(c["dispatches"])
    ls = [f"[orq night] Morning card: {_hora_local(start_at)} to {_hora_local(end.strftime('%Y-%m-%dT%H:%M:%SZ'))}, {n} dispatch{'es' if n != 1 else ''}."]
    ls += [f"Dispatches ({n}):", *("  " + x for x in c["lines"])] if n else ["Dispatches: none"]
    if c["stopped"]:
        ls.append(f"Stopped dispatching at {_hora_local(c['stopped']['ts'])}: {c['stopped']['motivo']}.")
    if c["dirty"][0]:
        ls += [f"Dirty worktrees ({c['dirty'][0]}):", *("  " + x for x in c["dirty"][1])]
    if c["without_push"][0]:
        ls += [f"Not pushed ({c['without_push'][0]}):", *("  " + x for x in c["without_push"][1])]
    ids = {e["pend"] for e in events if (e.get("ts") or "") >= start_at and e.get("tipo") == "pend" and e.get("op") == "add"}
    decision_lines = [i for i in (pending or {}).get("itens", []) if i.get("id") in ids and i.get("tipo") == "decisao"]
    if decision_lines:
        ls += [f"Parked decisions ({len(decision_lines)}):", *("  " + x for x in _lim(decision_lines, 3, lambda i: f"{i['id']}  {_quote(i.get('titulo'), 50)}"))]
    ls.append(c["manager"])
    if c["gap"]:
        ls.append(c["gap"])
    if c["cmds"]:
        ls += ["To paste:", *("  " + x for x in c["cmds"])]
    return ls[:CARD_LINES]


def card_first_line(events, now_at):
    """The card line that SessionStart injects: only when the night has ended (turned off or expired) less than CARD_SESSION_H h ago, otherwise None."""
    night_on_event, off_time = _night_window(events)
    if not night_on_event:
        return None
    end = _night_end(night_on_event, off_time)
    if end > now_at or now_at - end > timedelta(hours=CARD_SESSION_H):
        return None
    ends = {e["dispatch"]: e["motivo"] for e in events if e.get("tipo") == "fim_dispatch"}
    dispatch_events = [e["dispatch"] for e in events if e.get("tipo") == "despacho" and (e.get("ts") or "") >= night_on_event["ts"]]
    counts = {}
    for d in dispatch_events:
        m = ends.get(d, "rodando")
        counts[m] = counts.get(m, 0) + 1
    return (f"[orq night] Morning card: the night ended at {_hora_local(off_time or night_on_event['ate'])}, {len(dispatch_events)} dispatch{'es' if len(dispatch_events) != 1 else ''}"
            f"{' (' + ', '.join(f'{n} {m}' for m, n in counts.items()) + ')' if counts else ''}; orq summary --night has the rest.")


def _live_states(events, start_at):
    """{dispatch: worktree_state} for the dispatches since `start_at` still without fim_dispatch, as worker-show sees them now (up to 10; whatever fails is left out)."""
    ends = {e.get("dispatch") for e in events if e.get("tipo") == "fim_dispatch"}
    live = {}
    for e in [e for e in events if start_at and e.get("tipo") == "despacho" and e["ts"] >= start_at and e.get("dispatch") not in ends][:10]:
        try:
            live[e["dispatch"]] = worktree_state(_checkpoint(e["dispatch"]).get("caminho"))
        except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as x:
            log(f"cartão: worker-show {e['dispatch']}: {x}")
    return live


def morning_card(now_at=None):
    """`orq summary --night`: the card from the log and, for the dispatches still without fim_dispatch, the worktree as seen now (worker-show; whatever fails is left out)."""
    events, now_at = read_events(), now_at or datetime.now(timezone.utc)
    night_on_event, _ = _night_window(events)
    return "\n".join(night_card(events, _cursor_ro(), _pending_ro(), now_at, _live_states(events, night_on_event and night_on_event["ts"])))


# ---- night mode: external actions and worker environment ----

NIGHT_GIT_CONFIG = ("commit.gpgsign", "false")  # no signing: pinentry has nobody to answer in the small hours


def night_environment(base=None):
    """The worker's extra environment at night: git with no credential prompt and no signing. Adds to the GIT_CONFIG_COUNT that already exists, does not
    overwrite it. Returns {variable: value}."""
    base = os.environ if base is None else base
    n = int(base.get("GIT_CONFIG_COUNT") or 0) if str(base.get("GIT_CONFIG_COUNT") or "0").isdigit() else 0
    return {"GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_COUNT": str(n + 1), f"GIT_CONFIG_KEY_{n}": NIGHT_GIT_CONFIG[0], f"GIT_CONFIG_VALUE_{n}": NIGHT_GIT_CONFIG[1]}


_EXT_GIT = r"^git((?:\s+-\S+(?:\s+\S+)?)*)\s+"  # the regexes run on one cmdnorm segment: command position, no rtk/env/VAR= prefix
_EXT_GH = r"^gh(?:\s+-\S+(?:\s+\S+)?)*\s+"
NIGHT_EXTERNAL = [  # (what is denied, a regex over a segment of the command)
    ("git push", re.compile(_EXT_GIT + r"push(?![-\w])")),
    ("gh pr merge", re.compile(_EXT_GH + r"pr\s+merge(?![-\w])")),
    ("gh workflow run (deploy)", re.compile(_EXT_GH + r"workflow\s+run(?![-\w])")),
    ("git commit --no-verify", re.compile(_EXT_GIT + r"commit(?![-\w])[^;&|\n]*?\s(?:--no-verify|-[aeiopqsuvz]*n[a-zA-Z]*)(?=\s|$)")),
    ("orca worktree rm --force", re.compile(r"^orca\s+worktree\s+rm(?![-\w])[^;&|\n]*\s(?:--force|-f)(?![-\w])")),
]
_EXT_RESET = re.compile(_EXT_GIT + r"reset(?![-\w])[^;&|\n]*\s--hard(?![-\w])")


def _external_denied(ev, cur):
    """The name of the external action that the Bash of `ev` would perform with night mode on in `cur`, or None. `git reset --hard` only counts inside a
    linked worktree (a worker's); on the main checkout it loses everyone's work. Only reads the cursor and, for reset, the local git."""
    if (ev.get("tool_name") != "Bash" or not isinstance(ev.get("tool_input"), dict) or not isinstance(ev["tool_input"].get("command"), str)
            or "--help" in ev["tool_input"]["command"]):
        return None
    segs = cmdnorm.segments(ev["tool_input"]["command"])
    found_item = next((item_name for seg in segs for item_name, rx in NIGHT_EXTERNAL if rx.search(seg)), None)
    m = None if found_item else next(filter(None, map(_EXT_RESET.search, segs)), None)
    night = night_active(cur)
    if not (found_item or m) or not night or night.get("via") == "away":  # away pushes after the audit (126, 139): only the budget is armed
        return None
    if found_item:
        return found_item
    d = ev.get("cwd") or os.getcwd()
    c = GIT_C_PLACE.search(m.group(1) or "")
    if c:
        d = os.path.join(d, os.path.expanduser(c.group(1).strip("'\"")))
    gd, common = ((_git(d, "rev-parse", "--absolute-git-dir", "--git-common-dir") or "").split() + ["", ""])[:2]
    if not gd or os.path.realpath(gd) != os.path.realpath(os.path.join(d, common)):
        return None  # outside a repository or in a linked worktree
    return "git reset --hard in the main checkout"


def hook_external(ev, run):
    """PreToolUse of Bash, in every session (workers included): with night mode on it denies push, PR merge, deploy, commit without hook,
    `orca worktree rm --force` and `git reset --hard` on the main checkout. The message says to park the work and how to turn it off. When off, nothing changes."""
    item_name = _external_denied(ev, _cursor_ro())
    if not item_name:
        return None
    reason = (f"{MARK} night mode: `{item_name}` is an external action or bypasses a hook, and nobody is watching. Park the decision with `orq pend add` and carry on with what "
              "is independent. If the user is back and authorized it, `orq night off` lifts it. A commit that fails pre-commit is not bypassed: fix what the hook pointed at.")
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": reason}}


# ---------- coordenador x worker ----------

def _roles():
    return _dict(_cursor_ro().get("papeis"))


def coordinator(ev):
    """The Run of this session if it coordinates (Run linked and session that is not a worker's); otherwise None. Nothing is recorded for a worker without a dispatch.

    The worker role comes from the dispatch preamble, which is the first prompt of every worker (origin `dispatch_mode`): it holds before any
    Run is linked and is kept in cursor.json (papeis), so a worker that runs run-create stays a worker. In a session that already has a registered Run
    (coordinator) the preamble does not turn it into a worker. Orca's `worker-list` is not usable
    as a signal: without --run it lists only the Run linked to the terminal, and the Run the worker created never has it. A session with no preamble and no stored role
    is a coordinator when there is a linked Run.
    """
    if not os.environ.get("ORCA_TERMINAL_HANDLE"):
        return None
    sid = ev.get("session_id") or ""
    if origin_name(ev.get("prompt")) == "despacho":
        if _roles().get(sid) == "worker":
            return None
        if not _dict(_cursor_ro().get("runs")).get(sid):
            def write(c):
                _sub(c, "papeis")[sid] = "worker"

            _cursor_mut(write)
            return None
        # the preamble is the first prompt of every worker: in a session that was already coordinating, the user pasted it (to ask something)
        log(f"preâmbulo de despacho numa sessão que já coordena ({sid[:8]}): segue como coordenador")
    if _roles().get(sid) == "worker":
        return None
    try:
        run = orca("run-current")["run"]
    except Exception as e:  # noqa: BLE001 - Orca slow or down (12 failures and 3 timeouts in 3 days): the session that already coordinated keeps its Run, as the guard does
        log(f"hook {HOOK_STATE.get('kind')}: {type(e).__name__}: {e} (run-current)")
        hook_failed(e)
        remembered = _dict(_cursor_ro().get("runs")).get(sid)
        return {"id": remembered} if remembered else None
    if run is None:
        return None
    remember_run(sid, run["id"], ev.get("cwd"), ev.get("_harness_orq") or "claude")
    if (g := os.environ.get("ORQ_MATE")) and run["id"] not in (_dict(_mates().get(g)).get("runs") or []):
        _mate_mut(g, run=run["id"])  # the mate's Run: its entries (worker report) stay in the mate's world
    return run


# ---------- hooks ----------

def remember_run(sid, run_id, cwd=None, harness="claude"):
    """Stores in cursor.json the last Run seen per session_id (coordinator only) and its home, the cwd of the first prompt (Codex has no
    CLAUDE_PROJECT_DIR); writes only when it changes."""
    cur = _cursor_ro()
    if _dict(cur.get("runs")).get(sid) == run_id and (not cwd or _dict(cur.get("casas")).get(sid)) and (harness == "claude" or _dict(cur.get("harnesses")).get(sid)):
        return

    def write(c):
        _sub(c, "runs")[sid] = run_id
        if cwd:
            _sub(c, "casas").setdefault(sid, cwd)
        if harness != "claude":
            _sub(c, "harnesses")[sid] = harness

    _cursor_mut(write)


def lost_binding(ev, last_item):
    """A session that already had a Run and now arrives with run-current null (hibernation or resume): records it and gives a notice."""
    append_event({"tipo": "binding_perdido", "run": last_item, "sessao": (ev.get("session_id") or "")[:8]})
    log(f"binding_perdido: sessão {(ev.get('session_id') or '')[:8]} sem Run, último {last_item}")
    ctx = f"[orq] binding lost: run run-use --id {last_item}"
    return {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit", "additionalContext": ctx}}


def mark_arrival(org):
    """Stores in cursor.json the last system message (orca | notificacao) and the time: the ask hook compares it with the answer."""
    _cursor_mut(lambda c: c.__setitem__("chegada", {"origem": org, "t": time.time()}))


def confirm_batches(run_id, res):
    """`res` is the response of a consuming `check`. Acknowledges (--ack) the consecutive batches that are only heartbeat, up to HB_BATCHES, and records
    a heartbeat_absorvido event. Returns (event or None, response of the first batch that is not only heartbeat or of the last).

    The --ack confirms the batch and already brings the next one. A batch that is not only heartbeat is not acknowledged: it stays in the open delivery, which Orca
    repeats on the coordinator's check. Acknowledging the same batch again is harmless in Orca (it repeats the current delivery, checked in a
    test Run), so the hook and the waiter (orca-wait-runs.py) can call this at the same time without double-ack or losing a message; at most the
    event comes out twice, and the panel keeps only the last signal of each dispatch.
    """
    signals, deliveries = [], []
    for _ in range(HB_BATCHES):
        msgs = res.get("messages") or []
        if not res.get("deliveryId") or not only_heartbeats(msgs):
            break
        signals += [liveness_signal(m) for m in msgs]
        deliveries.append(res["deliveryId"])
        res = orca("check", "--run", run_id, "--ack", res["deliveryId"])
    if not deliveries:
        return None, res
    with _lock("cursor.lock"), _no_alarm():  # already confirmed: the event and the batch mark go in together, out of the alarm's reach
        ev = _write_event({"tipo": "heartbeat_absorvido", "run": run_id, "entregas": deliveries, "heartbeats": signals})
        cur = _read_cursor()
        _sub(cur, "hb_absorvido")[run_id] = time.time()
        _write_json(_path("cursor.json"), cur)
    return ev, res


def absorb_heartbeats(run_id, pending_messages=None):
    """If ALL the Run's unacknowledged messages are heartbeat, consumes, acknowledges and returns the recorded event; otherwise None.

    It only reads with --peek beforehand: with any message that is not heartbeat (or of unknown type) nothing is consumed or acknowledged.
    `pending_messages` are the messages the caller already read with --peek.
    """
    if pending_messages is None:
        pending_messages = orca("check", "--run", run_id, "--peek")["messages"]
    if not only_heartbeats(pending_messages):
        return None
    with manager_lock():  # the check and the ack in the same manager-to-Run call
        return confirm_batches(run_id, orca("check", "--run", run_id))[0]


def late_notice(run_id):
    """Was the Run's last batch of heartbeats absorbed less than HB_WINDOW_S ago? Then a notice with an empty box belongs to the batch that already went out."""
    t = _dict(_cursor_ro().get("hb_absorvido")).get(run_id)
    return isinstance(t, (int, float)) and 0 <= time.time() - t < HB_WINDOW_S


# Fixed mark at the start of everything Claude Code shows the user (reason, systemMessage, permissionDecisionReason): the user
# tells at once what came from orq. Emoji, not ANSI color: see docs/design.md. additionalContext (only the coordinator reads it) carries no mark.
MARK = "🟣 orq ·"


def _short_run(run_id):
    return run_id if len(run_id) <= 15 else run_id[:10] + "…"


def other_run_blocker(run_id):
    """Notice for a Run this coordinator does not coordinate: check, --peek and --ack give consumer_fenced, so the signal is read in the inbox (which covers
    all Runs) and nothing is consumed or acknowledged. Blocks if the still-unread messages (`read` 0) for the Run are all heartbeat, with no
    time limit (M11: a new heartbeat cannot hide an old worker_done without ack); records heartbeat_visto. Any other
    unread message, or none, lets it through. The messages come out in one batch when the coordinator does run-use.
    """
    unread_messages = [x for x in orca("inbox", "--limit", "200")["messages"]
                 if isinstance(x, dict) and x.get("to_handle") == f"run:{run_id}" and not x.get("read")]
    if not only_heartbeats(unread_messages):
        return None
    append_event({"tipo": "heartbeat_visto", "run": run_id, "heartbeats": [liveness_signal(x) for x in unread_messages]})
    return {"decision": "block", "reason": f"{MARK} {len(unread_messages)} heartbeat(s) from Run {_short_run(run_id)} seen, not for the coordinator"}


def heartbeat_blocker(ev, run):
    """Orca notice ("You have N orchestration message. Run `orca orchestration check --run <r>`") that carries only heartbeat: absorbs and blocks.

    Returns the UserPromptSubmit output that stops processing, or None (the prompt goes through). It goes through when the notice names no Run, names a
    Run different from the linked one (the check would give consumer_fenced), there is any message that is not heartbeat or the box is empty with no recent
    batch. An Orca error propagates to the caller, which also lets it through.
    """
    m = NOTICE_RUN.search(ev.get("prompt") or "")
    if not m:
        return None
    target = m.group(1)
    if target != run["id"] and not coordinator_run(target):
        return other_run_blocker(target)
    pending_messages = orca("check", "--run", target, "--peek")["messages"]  # agent manager Run: orca() links the manager to it, and the coordinator's raw check counts
    if not pending_messages:
        if not late_notice(target):
            return None
        return {"decision": "block", "reason": f"{MARK} notice of heartbeats already absorbed ({_short_run(target)})"}
    ev = absorb_heartbeats(target, pending_messages)
    if not ev:
        return None
    return {"decision": "block", "reason": f"{MARK} {len(ev['heartbeats'])} heartbeats absorbed ({_short_run(target)})"}


def inbox_in_prompt(run_id):
    """The context lines for the Orca notice of `run_id` (ticket 182): the hook itself does the `orq inbox --ack` (with the ingest of 171, binding the
    coordinator to the Run and restoring the link) and brings each message whole. If the read fails, only the command hint comes back, for the coordinator to run."""
    tip = [f"orq: read and confirm the inbox with `orq inbox {run_id} --ack` (binds the coordinator to the Run and restores the link)"]
    try:
        header, *msgs = inbox(run_id, ack=True, detail=True)
    except TimeoutError:
        raise  # the hook's 3 s ceiling applies to the whole hook
    except Exception as e:  # noqa: BLE001 - fail-open: without reading, the coordinator gets the hint
        log(f"inbox in prompt: {type(e).__name__}: {e}")
        return tip
    return [f"orq: the hook read and confirmed the inbox (nothing to run). {header}", *msgs] if msgs else tip  # nothing to show (empty, or only heartbeats): the old hint


ONLY_ORQ_COMMAND = re.compile(r"\s*/away(\s+\w+)?\s*$")


def hook_prompt(ev, run):
    org = origin_name(ev.get("prompt"))
    text_value, with_notice = split_notice(ev.get("prompt")) if org == "usuario" else (ev.get("prompt") or "", False)
    if with_notice and not text_value.strip():
        org = "orca"
    if org == "orca":
        try:
            blocker = heartbeat_blocker(ev, run)
        except TimeoutError:
            raise  # the hook's 3 s ceiling applies to the whole hook
        except Exception as e:  # noqa: BLE001 - fail-open: not knowing, the notice goes through and the coordinator is woken
            log(f"heartbeat: {type(e).__name__}: {e}")
            blocker = None
        if blocker:
            return blocker
    if org in ("orca", "notificacao"):
        mark_arrival(org)
    elif with_notice:
        mark_arrival("orca")
    if org == "orca":
        refresh_bg(refresh=False)  # the Orca notice is the new-message signal: ingest the inbox now, without redoing aberto.json (one heartbeat per 100 s)
    if org != "usuario":
        if not os.environ.get("ORQ_MATE"):
            with contextlib.suppress(Exception):
                record_coordinator_resume()
        ln = night_lines(_cursor_ro(), read_events()) if org in ("orca", "notificacao") else []  # the night wakes the coordinator by notice, not by user
        if org == "orca" and (r := NOTICE_RUN.search(ev.get("prompt") or "")):
            ln = [*inbox_in_prompt(r.group(1)), *ln]
        if org == "aviso_orq" and text_value.lstrip().startswith("orq: PR ") and (obligation_part := obligations_line(read_events())):
            ln = [*ln, obligation_part]  # the merge notice arrives already carrying what it asks for
        return {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit", "additionalContext": "\n".join(ln)}} if ln else None
    entry = append_event({"tipo": "entrada", "origem": "usuario", "texto": text_value[:2000], "sessao": (ev.get("session_id") or "")[:8],
                            "terminal": os.environ["ORCA_TERMINAL_HANDLE"],
                            **({"com_aviso": True} if with_notice else {}), **({"grupo": os.environ["ORQ_MATE"]} if os.environ.get("ORQ_MATE") else {})}, new_id=True)
    if ONLY_ORQ_COMMAND.match(text_value):  # `/away` and `/away status` ask for no effect: they close on their own
        intake(entry["id"], "conversa", note="orq command")
    check_manager_bg()
    ctx = state(entry)
    if not os.environ.get("ORQ_MATE") and (deferred := context_notices()):  # the notice queue belongs to the coordinator
        ctx += "\n" + deferred
    refresh_bg()
    return {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit", "additionalContext": ctx}}


AWAY_BLOCKERS = 3  # the coordinator's Stop with away on blocks the same reason up to 3 times within AWAY_BLOCKER_MIN; then lets it stop (the coordinator may really be stuck)
AWAY_BLOCKER_MIN = 30


def _no_push():
    """Commits of the orq installation that upstream does not have yet; None if git does not respond. Always applies, without depending on a `cycle` event: the integrator advances
    main by hand (`git merge --ff-only` + `orq integrate queue rm`) and never records it (ticket 180). `ORQ_SEM_PUSH` fixes the number (tests)."""
    if os.environ.get("ORQ_SEM_PUSH"):
        return int(os.environ["ORQ_SEM_PUSH"])
    try:
        r = subprocess.run(["git", "-C", os.path.dirname(os.path.realpath(__file__)), "rev-list", "--count", "@{u}..HEAD"], capture_output=True, text=True, timeout=2)
        return int(r.stdout) if r.returncode == 0 else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


CYCLES_LOG = os.environ.get("ORQ_CICLOS_LOG") or os.path.join(WT_ROOT, "integracao", "ciclos.log")


def _integrator_pending(events):
    """The first `[PENDENTE` line of the integrator's ciclos.log that the coordinator has not received yet, with the dirty files of the live checkout; None if there is none.
    Only the pending item newer than the last completed cycle counts (any non-empty line without `[PENDENTE`). Log missing or unreadable: None."""
    try:
        with open(CYCLES_LOG, encoding="utf-8") as f:
            line_list = [x.strip() for x in f if x.strip()]
    except OSError:
        return None
    pending = []
    for x in line_list:
        pending = pending + [x] if x.startswith("[PENDENTE") else []
    notified_lines = {e.get("linha") for e in events if e.get("tipo") == "pendente_avisado"}
    line = next((x[:300] for x in pending if x[:300] not in notified_lines), None)
    if not line:
        return None
    try:
        dirty = subprocess.run(["git", "-C", ORQ_INSTALL, "status", "--short"], capture_output=True, text=True, timeout=2).stdout.splitlines()
    except (OSError, subprocess.TimeoutExpired):
        dirty = []
    return {"linha": line, "motivo": f"orq: integrator stopped: main did not advance, live tree dirty ({_quote(line, 160)})" + (f"; dirty files: {'; '.join(dirty[:10])}" if dirty else "")}


def next_without_user(tks, agent_rows, integration, queue, events, cfg, without_push, pending_item=None, mate_to_open=None):
    """The coordinator's next step that does not depend on the user, or None (pure; the Stop with away on blocks the end of the turn while there is one).

    In order: delivery (`delivered` worker of an open ticket) outside the integrator queue; integrator cycle with commits without push (`without_push` > 0); `ready-for-agent`
    ticket with no blocker, P1 or P2, with Modelo/Effort, outside the dispatch queue and with a free slot (a hibernated worker does not occupy a slot); at the end, the proposal to
    open the mate of a group with no mate that gathered ready tickets (`mate_to_open`: (group, [numbers]) without `mate_auto`, which the manager opens by itself)."""
    by_num, dispatch_index = {t["num"]: t for t in tks}, _dispatch_ticket(events)
    if without := sum(a.get("estado") == "sem_terminal" for a in agent_rows):
        return f"{without} worker(s) lost the terminal without worker_done: `orq resume --dry-run`, then `orq resume`"
    for a in agent_rows:
        n = dispatch_index.get(a.get("dispatch")) or dispatch_index.get(a.get("task"))
        if a.get("estado") == "entregue" and n and n not in integration and (by_num.get(n) or {}).get("status") != STATUS_CLOSED:
            return f"worker {a['dispatch']} delivered ticket {n} and the delivery was not integrated: `orq integrate queue add <branch> {n}`, then release the worker"
    if pending_item:
        return pending_item
    abandoned = away_abandoned(tks, events)
    if abandoned:
        return abandoned
    if without_push:
        cycle = next((e for e in reversed(events) if e.get("tipo") == "ciclo"), None)
        hash_ = f" ({str(cycle.get('hash'))[:8]})" if cycle else ""
        return f"the integrator cycle{hash_} left {without_push} commit(s) unpushed: audit the diff and push"
    occupancy = {"vivos": {a["dispatch"]: a.get("modelo") for a in agent_rows if a.get("estado") in ANDA}}
    in_queue = {i.get("ticket") for i in queue}
    for t in sorted(tks, key=lambda t: (priority_of(events, t["task"], None, t["titulo"]), t["num"])):
        if t["status"] != STATUS_NEW or t["num"] in in_queue or any((by_num.get(b) or {}).get("status") != STATUS_CLOSED for b in t["blocked_by"]):
            continue
        if dispatch_wait(t, integration, events, without_push):
            continue
        if priority_of(events, t["task"], None, t["titulo"]) < 3 and t["modelo"] and t["effort"] in HARNESS["claude"]["efforts"] and not machine_slot(t["modelo"], occupancy, cfg):
            return f"ticket {t['num']} ({_quote(t['titulo'], 50)}) is ready, unblocked and a slot is free: `orq dispatch --ticket {t['num']}`"
    if mate_to_open:
        return f"open the mate of group {mate_to_open[0]} ({len(mate_to_open[1])} ready tickets: {', '.join(mate_to_open[1])}): `orq mate open {mate_to_open[0]}`"
    return None


def _work_without_user(events, now_at):
    """(next step that does not depend on the user, integrator pending item) read only from orq's files; (None, None) if something fails (the Stop fails open, the manager tries again on the next round)."""
    try:
        agent_rows = reassess(_dict(_read_json(_path("open.json"))).get("agentes") or [], events, now_at, _turns_ro())
        without_push = _no_push()
        pending = _integrator_pending(events)
        tks, group_map, queue, machine = tickets(), groups(), dispatch_queue_items(), machine_cfg()
        mate_to_open = next(((n, nums) for n, nums in mate_proposals(tks, group_map, _mates(), queue, machine) if group_map[n].get("mate_auto") is not True), None)
        return next_without_user(tks, agent_rows, integration_queue(), queue, events, machine, without_push, pending and pending["motivo"], mate_to_open), pending
    except Exception as e:  # noqa: BLE001 - hook falha aberto
        log(f"trabalho_sem_usuario: {type(e).__name__}: {e}")
        return None, None


def away_blocker(events, now_at):
    """The reason for the Stop to block the end of the turn (away on and work that does not depend on the user), or None. Reads only orq's files, never Orca;
    any failure lets it stop. Records `away_blocker`: the same reason blocks at most AWAY_BLOCKERS times in AWAY_BLOCKER_MIN."""
    if not away_enabled():
        return None
    next_one, pending = _work_without_user(events, now_at)
    cutoff = now_at - timedelta(minutes=AWAY_BLOCKER_MIN)
    if not next_one or sum(e.get("tipo") == "away_bloqueio" and e.get("motivo") == next_one and (_ts(e.get("ts")) or cutoff) > cutoff for e in events) >= AWAY_BLOCKERS:
        return None
    append_event({"tipo": "away_bloqueio", "motivo": next_one})
    if pending and next_one == pending["motivo"]:  # the same log line warns once
        append_event({"tipo": "pendente_avisado", "linha": pending["linha"]})
    return f"{MARK} away is on and there is still work that does not depend on the user: {next_one}. Do it before ending the turn."


INTAKE_OLD_MIN = 30  # an entry from another session with no intake for longer than this also blocks the Stop
GATE_BLOCKERS = 2  # the Stop blocks the same set of open entries, in one session, up to 2 times in a row; then releases with a systemMessage and `gate_falhou`


def _gate_blocks(session, ids):
    """Should the Stop block now? Counts in cursor.json per session and per set of open entries: a new set restarts the counter, and the
    GATE_BLOCKERS+1-th Stop of the same set goes through. The payload's `stop_hook_active` is not used: another hook (OMC, engram) turns it on without it being our block."""
    r = []

    def count_from(c):
        s = _sub(_sub(c, "stop_gate"), session)
        key_name = ",".join(sorted(ids))
        if s.get("conjunto") != key_name:
            s.clear()
            s.update(conjunto=key_name, n=0)
        if s["n"] < GATE_BLOCKERS:
            s["n"] += 1
            r.append(True)

    _cursor_mut(count_from)
    return bool(r)


def _stop_motive(events, now_at):
    """Why the coordinator ends the turn, computed from orq's files: `trabalho_esperando` (a next step that does not depend on the user), `esperando_usuario`
    (nothing else to do and a decision open) or `sem_trabalho`."""
    next_one, _ = _work_without_user(events, now_at)
    return "trabalho_esperando" if next_one else "esperando_usuario" if any(i.get("tipo") == "decisao" for i in _load_pending()["itens"]) else "sem_trabalho"


def _last_turn_event(events):
    return next((e for e in reversed(events) if e.get("tipo") in COORDINATOR_TURN), None)


def record_coordinator_stop():
    """Away on: the Stop that ends the turn records `coordenador_parou` with the motive, once per turn (the previous turn event is not another stop)."""
    if not away_enabled():
        return
    events = read_events()
    last_item = _last_turn_event(events)
    if not last_item or last_item["tipo"] != "coordenador_parou":
        append_event({"tipo": "coordenador_parou", "motivo": _stop_motive(events, datetime.now(timezone.utc))})


def record_coordinator_resume():
    """Away on: a prompt that is not the user's (notice typed by the manager, Orca, notification) closes the open `coordenador_parou` with `coordenador_retomou`."""
    if away_enabled() and (last_item := _last_turn_event(read_events())) and last_item["tipo"] == "coordenador_parou":
        append_event({"tipo": "coordenador_retomou"})


def hook_stop(ev, run):
    out = _hook_stop(ev, run)
    if not os.environ.get("ORQ_MATE") and not (out or {}).get("decision") == "block":
        try:
            record_coordinator_stop()
        except TimeoutError:
            raise
        except Exception as e:  # noqa: BLE001 - fail-open like the digest
            log(f"coordenador_parou: {type(e).__name__}: {e}")
    return out


def _hook_stop(ev, run):
    # an entry with no intake in the turn (same session) or open for more than INTAKE_OLD_MIN always blocks, via _gate_blocks; the others only with `stop_bloqueia`
    if not os.environ.get("ORQ_MATE"):  # the mate's end of turn is not the coordinator's reply to the absent user
        digest_no_stop(ev)
    events, now_at = read_events(), datetime.now(timezone.utc)
    without = open_entries(events)
    old_entries = [] if os.environ.get("ORQ_MATE") else obligations_to_chase(events, now_at)
    blocker = None if os.environ.get("ORQ_MATE") else away_blocker(events, now_at)
    if not without and not old_entries and not blocker:
        return None
    msg = MARK
    if without:
        ids = [e["id"] for e in without]
        append_event({"tipo": "gate_aviso", "abertas": ids, "sessao": (ev.get("session_id") or "")[:8]})
        rec = recovered_cursor(_cursor_ro(), now_at)
        quoted = ", ".join(f"{e['id']} ({_quote(e.get('texto'))!r})" for e in without[:3]) + (f" +{len(without) - 3}" if len(without) > 3 else "")
        msg += f" {len(ids)} entry(ies) without effect: {quoted}. Use: orq intake <e> task|steer|pend|decision|conversation|discarded [ref]" + (f" [notice] {recovered_notice(rec)}" if rec else "")
        session = (ev.get("session_id") or "")[:8]
        if not os.environ.get("ORQ_MATE"):
            if not machine_cfg()["stop_bloqueia"]:  # only what the user said in this turn, or what has been open for a long time
                ids = [e["id"] for e in without if e.get("origem", "usuario") == "usuario" and (
                    (session and e.get("sessao") == session) or ((t := _ts(e.get("ts"))) and (now_at - t).total_seconds() > INTAKE_OLD_MIN * 60))]
            if ids:
                if _gate_blocks(session, ids):
                    return {"decision": "block", "reason": msg}
                append_event({"tipo": "gate_falhou", "abertas": ids, "sessao": session})
    if old_entries:
        msg += (f" Obligation open for more than {OBLIGATION_MIN:g} min: " + ", ".join(f"{o['entrada']} {o['chave']} ({o['texto']})" for o in old_entries[:4])
                + (f" +{len(old_entries) - 4}" if len(old_entries) > 4 else "") + ': orq fulfill <e> <obligation> --proof "…" or orq defer <e> <obligation> --reason "…" (a deploy that is still building can be deferred).')
        ids, session = [f"{o['entrada']}:{o['chave']}" for o in old_entries], (ev.get("session_id") or "")[:8]
        if _gate_blocks(session, ids):
            return {"decision": "block", "reason": msg}
        append_event({"tipo": "gate_falhou", "abertas": ids, "sessao": session})
    return {**({"systemMessage": msg} if without or old_entries else {}), **({"decision": "block", "reason": blocker} if blocker else {})}


def _ask_data(ev):
    """(questions, answers) of the AskUserQuestion PostToolUse. The answers are indexed by the question text,
    in tool_response (the transcript's toolUseResult) or, if the harness puts them there, in tool_input."""
    ti = ev.get("tool_input") if isinstance(ev.get("tool_input"), dict) else {}
    tr = ev.get("tool_response") if isinstance(ev.get("tool_response"), dict) else {}
    return ti.get("questions") or tr.get("questions") or [], tr.get("answers") or ti.get("answers") or {}


def _ask_notes(ev):
    """User notes per question (annotations[question].notes), in tool_response or, if the harness puts them there, in tool_input."""
    ti = ev.get("tool_input") if isinstance(ev.get("tool_input"), dict) else {}
    tr = ev.get("tool_response") if isinstance(ev.get("tool_response"), dict) else {}
    return _dict(tr.get("annotations") or ti.get("annotations"))


def _recent_arrival():
    c = _dict(_cursor_ro().get("chegada"))
    return c if isinstance(c.get("t"), (int, float)) and 0 <= time.time() - c["t"] < SUSPECT_S else None


def _to_coordinator(m):
    """Is the message addressed to a Run's mailbox (`run:<id>`)? That is the one Orca announces in the coordinator's terminal; those for
    `dispatch:` and `term_` go to a worker."""
    return str(m.get("to_handle") or "run:").startswith("run:")


def _recent_message():
    """Message that Orca delivered to the coordinator within ±ORCA_WINDOW_S of now (Orca's inbox, not our hook's), or None.

    It is the signal that the "You have N orchestration message" notice was typed into the terminal near the answer. The one from any Run counts:
    Orca also notifies the terminal of Runs it is no longer linked to, and the user works with one Run per front.
    """
    now_at = time.time()
    for m in orca("inbox", "--limit", "20")["messages"]:
        if _to_coordinator(m) and abs(now_at - _dt(m.get("delivered_at") or m["created_at"]).timestamp()) <= ORCA_WINDOW_S:
            return m
    return None


def _recommended(answer_text, q):
    """Is the answer only the recommended option: the first one, or the one carrying "(Recomendado)"/"(Recommended)" in the label?"""
    ops = q.get("options") or []
    found_labels, rest = marked(answer_text, ops)
    if not ops or rest or len(found_labels) != 1:
        return False
    return found_labels[0] == ops[0].get("label") or bool(re.search(r"\((?:recomendad[oa]|recommended)\b[^)]*\)", found_labels[0], re.I))


def hook_ask(ev, run):
    """Records each answered question and closes the header's pending item (or, in `already-fez`, the checked ones).

    Only a checked option closes: free text ("Other", or an option with text attached) is recorded with `free` and the pending item stays open,
    with the notice "resposta livre em <header>: feche com orq pend done se decidiu".

    Suspicious answer (closes nothing): the prompt hook saw an Orca notice pasted into it, its text is the notice itself, or
    Orca delivered a message to the Run within ±5 s and the answer is the recommended option, which is what Enter on the widget chooses.
    """
    qs, answers = _ask_data(ev)
    notes = _ask_notes(ev)
    if not answers:
        # no answers: the user dismissed the question, or the payload format changed; the log tells the two cases apart
        log(f"ask sem answers: tool_input={sorted((ev.get('tool_input') or {}))} tool_response={sorted(ev['tool_response']) if isinstance(ev.get('tool_response'), dict) else type(ev.get('tool_response')).__name__}")
        return None
    arrival = _recent_arrival()
    recent = None if arrival else _recent_message()  # Orca failure here: nothing was recorded yet, the hook only logs
    session, ask = (ev.get("session_id") or "")[:8], ev.get("tool_use_id")
    suspects_, free_items_, notices = [], [], []
    for q in qs:
        answer_text = answers.get(q.get("question"))
        if answer_text is None:
            continue
        header, base = q.get("header") or "", {"header": q.get("header") or "", "pergunta": q.get("question"), "resposta": answer_text,
                                                "sessao": session, "ask": ask}
        text_value = origin_name(answer_text) in ("orca", "notificacao")
        if arrival or text_value or (recent and _recommended(answer_text, q)):
            reason = arrival["origem"] if arrival else "texto" if text_value else "inbox"
            append_event({"tipo": "resposta_suspeita", **base, "chegada": reason, **({"msg": recent["id"]} if reason == "inbox" else {})})
            suspects_.append(header)
            continue
        item_list = {i.get("id"): i for i in _load_pending()["itens"]}
        marked_, rest = marked(answer_text, q.get("options") or [])
        note = str(_dict(notes.get(q.get("question"))).get("notes") or "").strip()  # an option marked with a note ("só se X", i.e. "only if X") is not a clean option
        free = bool(rest) or not marked_ or bool(note)
        if header == "ja-fez":
            ids = [m.group(1) for o in q.get("options") or [] if (o.get("label") in marked_)
                   for m in [re.match(r"\[([^\]]+)\]", o.get("description") or "")] if m]
        else:  # only a decision closes by header, and only with one option marked
            decision = (item_list.get(header) or {}).get("tipo") == "decisao"
            ids = [header] if decision and not free else []
            if decision and free:
                free_items_.append(header)
        did_close = [i for i in ids if i in item_list]
        append_event({"tipo": "resposta", **base, **({"livre": True} if free else {}), **({"nota": note} if note else {}),
                      **({"fechou": did_close} if did_close else {})})
        for i in did_close:
            try:
                done = pending_done(i, answer_text if header != "ja-fez" else None, _UNKNOWN if manager_runs() else run["id"])
            except ValueError as e:  # another orq closed in the middle: the next questions go on
                log(f"ask: {e}")
            except backlog.BacklogError as e:  # tasks-axi refused: the answer is already in the log and the decision stays open
                log(f"ask: {e}")
                notices.append(f"could not close {i} in the backlog ({e}): close it with orq pend done {shlex.quote(i)}")
            else:
                if done.get("aviso"):
                    notices.append(done["aviso"])
    ctx = []
    if suspects_:
        ctx.append("; ".join(f"[orq] suspect answer in {h}: confirm" for h in suspects_) +
                   " (it arrived together with an Orca message; the pending item stays open). Ask the question again before acting.")
    if free_items_:
        ctx.append("; ".join(f'[orq] free-text answer in {h}: if decided, close it with orq pend done {shlex.quote(h)} --answer "<what was decided>"' for h in free_items_) +
                   " (the pending item and the gate stay open).")
    if notices:
        ctx.append("; ".join(f"[orq] {a}" for a in notices) + ".")
    return {"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": " ".join(ctx)}} if ctx else None


def active_dispatches():
    """[{run, task, dispatch}] of the dispatches with dispatchStatus `dispatched` in any Run, cached for ASK_GUARD_TTL seconds.

    A call without the terminal linked lists all Runs (`scope.source` all), newest first, instead of run-list + one worker-list per
    Run (about 70 calls). If Orca returns the scoped list, it raises: the guard fails open instead of looking at only one Run.
    A dispatch of a Run that another live terminal coordinates does not count (_from_other_coordinator); the cache holds the already filtered list.
    ponytail: only the newest ACTIVE_PAGES pages; a `dispatched` dispatch older than 300 others is dead, and ask-guard.off releases it.
    """
    cache = _read_json(_path("active.json"))
    if isinstance(cache, dict) and isinstance(cache.get("ativos"), list) and isinstance(cache.get("t"), (int, float)) \
            and 0 <= time.time() - cache["t"] < ASK_GUARD_TTL:
        return cache["ativos"]
    active_items = [{"run": w.get("runId"), "task": w.get("taskId"), "dispatch": w.get("dispatchId")}
              for w in _all_workers() if w.get("dispatchStatus") == "dispatched"]
    active_items = _from_other_coordinator(active_items)
    _write_json(_path("active.json"), {"t": time.time(), "ativos": active_items})
    return active_items


def _from_other_coordinator(active_items):
    """Removes the dispatches of a Run coordinated by another terminal that still exists: Orca notifies the coordinator linked to the Run, so they
    do not belong to this terminal. A closed coordinator (terminal_handle_stale) notifies no one: the dispatch still counts. Orca error propagates."""
    mine = {os.environ.get("ORCA_TERMINAL_HANDLE"), handle_orca()}  # with the agent manager on, its Run and what the coordinator holds belong to this coordinator (M14)
    owner_name = {}
    for run in {a["run"] for a in active_items if a.get("run")}:
        h = (orca("run-show", "--id", run)["run"] or {}).get("coordinator_handle")
        if h and h not in mine:
            owner_name[run] = h
    live = {}
    for h in set(owner_name.values()):
        try:
            orca("show", "--terminal", h, area="terminal")
            live[h] = True
        except RuntimeError as e:
            if "terminal_handle_stale" not in str(e):
                raise
            live[h] = False
    return [a for a in active_items if not live.get(owner_name.get(a.get("run")))]


def hook_guard(ev, run):
    """PreToolUse of AskUserQuestion, coordinator only: with an active dispatch in any Run the box is refused and the decision goes through Lavish.

    Fail-open (run_hook): if Orca does not respond, the box goes through and the error goes to the log.
    """
    if ev.get("tool_name") != "AskUserQuestion" or os.path.exists(_path("ask-guard.off")):
        return None
    if away_enabled():  # the user is away: the decision waits as a pending item and the coordinator goes on with what does not depend on it
        reason = (f"{MARK} away is on: record it with `orq pend add --type decision --id <id> --title ...` and carry on with what does not depend on it "
                  "(Lavish still applies: `orq ask` leaves the page open and does not wait for the answer; a worker stopped on it takes `--task <id>`).")
        return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": reason}}
    active_items = active_dispatches()
    if not active_items:
        return None
    listing = ", ".join(f"{(a.get('task') or '?')[:18]} ({a.get('run')}, {a.get('dispatch')})" for a in active_items[:3]) + (f" +{len(active_items) - 3}" if len(active_items) > 3 else "")
    reason = (f"{MARK} {len(active_items)} active dispatch{'es' if len(active_items) > 1 else ''} ({listing}). With a worker running, the user's decision "
              "goes through Lavish, not AskUserQuestion: use `orq ask --id <pend> --question ... --option ... --option ...` (builds the page, opens it in Orca, waits and closes the pending item; run it in the background) or build the page by hand with lavish-axi (batch with items id, header equal to the pending id, "
              'resposta (the answer) and disposicao; only the disposicao "escolha" (or "manter"/"trocar") with a resposta closes the decision, "livre", "adiar" and "conversar" '
              "leave it open), run `lavish-axi poll <file.html>` and record the poll output, raw, with `orq lavish-answer <file>` "
              "(`-` reads from stdin). AskUserQuestion only applies with no active worker. Stuck dispatch? "
              f"`orca orchestration worker-stop --dispatch <id>` or `touch {shlex.quote(_path('ask-guard.off'))}`.")
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": reason}}


WRITE_PLACE = re.compile(r"^git((?:\s+-\S+(?:\s+\S+)?)*)\s+(commit|push)\b")  # on a cmdnorm segment
GIT_C_PLACE = re.compile(r"\s-C\s+(\S+)")


def hook_place(ev, run):
    """PreToolUse of Bash/Edit/Write, coordinator only: WARNS (does not block) about a write in the wrong place, on the main checkout outside the
    default branch or with the cwd in a worktree that is not the coordinator's (CLAUDE_PROJECT_DIR). Only looks at write commands: git commit/push and file edits."""
    tool_name, ti = ev.get("tool_name"), ev.get("tool_input") or {}
    if tool_name == "Bash":
        m = next(filter(None, map(WRITE_PLACE.search, cmdnorm.segments(ti.get("command") or ""))), None)
        if not m:
            return None
        d = ev.get("cwd") or os.getcwd()
        c = GIT_C_PLACE.search(m.group(1) or "")  # `git -C <dir> commit` writes to <dir>, not to the cwd
        if c:
            d = os.path.join(d, os.path.expanduser(c.group(1).strip("'\"")))
            d = d if os.path.isdir(d) else ev.get("cwd") or os.getcwd()
    elif tool_name in ("Edit", "Write", "MultiEdit", "NotebookEdit", "apply_patch"):
        file_path = ti.get("file_path") or ti.get("notebook_path") or next(iter(PATCH_FILE.findall(ti.get("command") or "")), "")
        d = os.path.dirname(os.path.join(ev.get("cwd") or os.getcwd(), file_path)) if file_path else ""
        d = d if os.path.isdir(d) else ev.get("cwd") or os.getcwd()
    else:
        return None
    gd, common, topo = ((_git(d, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel") or "").split() + ["", "", ""])[:3]
    if not gd:
        return None
    common = os.path.realpath(os.path.join(d, common))
    if os.path.realpath(gd) != common:  # linked worktree: only a mistake if it is not where the coordinator lives
        home_dir = os.environ.get("CLAUDE_PROJECT_DIR") or _dict(_cursor_ro().get("casas")).get(ev.get("session_id") or "")
        if home_dir and os.path.realpath(home_dir) == os.path.realpath(topo):
            return None
        notice = f"the cwd is in worktree {topo}, which looks like a worker's"
    else:
        branch = (_git(d, "rev-parse", "--abbrev-ref", "HEAD") or "").strip()
        default = default_branch(d)
        if branch in (default, BRANCH_NO_REMOTE, "master"):
            return None
        notice = f"the main checkout is on {branch}, not on {default}"
    msg = f"{MARK} wrong place? {notice}. Check `git status --short --branch` and the cwd before writing (notice, nothing was blocked)."
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": msg}}


def hook_session(ev, run):
    """SessionStart: injects the state and the open tickets so the new session resumes without anyone telling it anything."""
    if not os.path.exists(_path("open.json")):
        refresh_bg()  # with no cache the summary says "refresh em andamento" (refresh in progress): it asks for the refresh (B27)
    ctx = session_context()
    if other_handoff := _coordinator_handoff_to(ev):
        ctx += "\n\n" + other_handoff
    return {"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": ctx}}


PR_CREATE = re.compile(r"^gh(?:-axi)?\s+pr\s+create\b")  # on a cmdnorm segment
PR_HEAD = re.compile(r"(?<!\S)(?:--head[=\s]+|-H[=\s]*)['\"]?([^\s'\"]+)")
PR_CD = re.compile(r"(?<![\w-])cd\s+(\S+)\s*&&")


def _orq_cli(*args):
    """Runs an orq subcommand outside the hook (the hook only has HOOK_TIMEOUT s and Orca is slow): in the background; with ORQ_NO_BG, until the end."""
    cmd = [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "orq.py"), *args]
    if os.environ.get("ORQ_NO_BG"):
        signal.alarm(0)  # only tests get here: with the machine loaded the hook's alarm would cut the child off midway
        subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=True, timeout=30)
    else:
        return subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


def _worktrees_by_branch(cwd):
    """{branch: worktree path} from `git worktree list` of `cwd`."""
    wts = {}
    for block in (_git(cwd, "worktree", "list", "--porcelain") or "").split("\n\n"):
        fields = dict(l.split(" ", 1) for l in block.splitlines() if " " in l)
        if fields.get("branch", "").startswith("refs/heads/"):
            wts[fields["branch"][len("refs/heads/"):]] = fields.get("worktree")
    return wts


def hook_pr_link(ev, run):
    """PostToolUse of Bash, coordinator only: the output of `gh pr create` carries the PR URL; orq links it to the task that owns the branch (`orq pr auto`,
    outside the hook) or puts it under "PR sem tarefa". The branch is the one from `--head` or from the cwd (with `cd <dir> &&` in front, that one's). Only reads prs.json."""
    ti, resp = ev.get("tool_input") or {}, ev.get("tool_response")
    cmd = ti.get("command") or ""
    if ev.get("tool_name") != "Bash" or not any(map(PR_CREATE.search, cmdnorm.segments(cmd))):
        return None
    urls = PR_RE.findall((resp.get("stdout") or "") if isinstance(resp, dict) else str(resp or ""))
    if not urls:
        return None
    d = _prs_ro()
    urls = [u for u in dict.fromkeys(urls) if not any(i["url"] == u for i in d["itens"]) and not any(x.get("url") == u for x in d.get("sem_task") or [])]
    if not urls:
        return None
    cwd = ev.get("cwd") or os.getcwd()
    cd = PR_CD.search(cmd)
    if cd and os.path.isdir(os.path.join(cwd, os.path.expanduser(cd.group(1).strip("'\"")))):
        cwd = os.path.join(cwd, os.path.expanduser(cd.group(1).strip("'\"")))
    heads = [h.split(":")[-1] for h in PR_HEAD.findall(cmd)]
    if not heads and (h := (_git(cwd, "rev-parse", "--abbrev-ref", "HEAD") or "").strip()):
        heads = [h]
    # one --head per URL (loop with different branches) matches in order; a literal --head applies to all; a variable or a count that does not add up: each one's branch
    # URL comes from `gh pr view` in `orq pr auto`, outside the hook (None)
    if len(heads) == len(urls) > 1:
        by_url = dict(zip(urls, heads))
    elif len(set(heads)) == 1 and "$" not in heads[0] and "`" not in heads[0]:
        by_url = {u: heads[0] for u in urls}
    else:
        by_url = {u: None for u in urls}
    wts = _worktrees_by_branch(cwd)
    for url, head in by_url.items():
        wt = wts.get(head)
        _orq_cli("pr", "auto", url, *(["--head", head] if head else ["--cwd", cwd]), *(["--wt", wt] if wt else []))
    return {"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": _pr_link_notice(by_url)}}


PR_LINK_WAIT_S = 1.5  # how long the hook waits for the background `orq pr auto` before it says "still resolving": the hook has HOOK_TIMEOUT s in all


def _pr_link_notice(by_url):
    """What really happened to each PR of the hook, read from prs.json after a short wait: linked to a task, under "PR without a task" (with the command that links it)
    or still resolving. It never says "linked" for what is not."""
    deadline = time.time() + PR_LINK_WAIT_S
    while True:
        d = _prs_ro()
        done_state = {u: next((("task", i["task"]) for i in d["itens"] if i["url"] == u), None) or next((("orphan", None) for x in d.get("sem_task") or [] if x.get("url") == u), None)
                      for u in by_url}
        if all(done_state.values()) or time.time() >= deadline:
            break
        time.sleep(0.1)
    lines = []
    for url, head in by_url.items():
        n, branch = url.rsplit("/", 1)[1], head or "unknown branch"
        kind, task = done_state[url] or (None, None)
        if kind == "task":
            lines.append(f"PR #{n} ({branch}) linked to task {task}.")
        elif kind == "orphan":
            lines.append(f"PR #{n} ({branch}) is without task: no dispatch owns this branch, so it sits under \"PR without task\" in `orq status`. Link it: `orq pr link <task> {url}`.")
        else:
            lines.append(f"PR #{n} ({branch}) is not linked yet: orq is still looking for its task. Check `orq pr list`; if it ends under \"PR without task\", `orq pr link <task> {url}`.")
    return f"{MARK} " + " ".join(lines)


def guard_worker():
    """PreToolUse of AskUserQuestion in a worker: refuses the box, which would be stuck in the terminal, and tells it to escalate through Orca."""
    reason = (f"{MARK} a worker does not open a question in the terminal: the coordinator cannot see this screen. Escalate through Orca with the dispatch preamble data: "
              "`orca orchestration ask --from <your terminal> --dispatch-capability <cap> --question \"<question>\" --options \"a,b\"` (waits for the answer) or "
              "`orca orchestration send ... --type escalation --subject \"Blocked: <reason>\" --body \"<details>\"`. Without the preamble, use the handle of the "
              "coordinator that came in the continuation message.")
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": reason}}


HOOKS = {"prompt": hook_prompt, "stop": hook_stop, "ask": hook_ask, "guard": hook_guard, "session": hook_session, "lugar": hook_place, "externas": hook_external, "prligar": hook_pr_link}
HOOK_STATE = {}  # the hook running in this process: kind, failed (it raised or lost Orca), notice (the systemMessage to print), printed


def hook_failed(exc):
    """A coordinator hook (session, prompt, stop) failed or lost Orca: marks it, leaves the marker and keeps the warning to print with the hook's answer. The other kinds only log."""
    if HOOK_STATE.get("kind") not in WARN_KINDS:
        return
    HOOK_STATE["failed"] = True
    if not HOOK_STATE.get("notice"):
        HOOK_STATE["notice"] = fail_safe.warn(f"run:{HOOK_STATE['kind']}", exc)


def emit_hook(out=None):
    """Prints the hook's answer once, with the failure warning (systemMessage) and, in SessionStart, the short note that the context did not load."""
    out = dict(out or {})
    if HOOK_STATE.get("failed") and HOOK_STATE["kind"] == "session" and "hookSpecificOutput" not in out:
        out["hookSpecificOutput"] = {"hookEventName": "SessionStart", "additionalContext": MSG_NO_CONTEXT}
    if HOOK_STATE.get("notice"):
        out["systemMessage"] = "\n".join(filter(None, [HOOK_STATE["notice"], out.get("systemMessage")]))  # the hook's own systemMessage (the stop's block notice) stays
    if out:
        print(json.dumps(out, ensure_ascii=False))
        HOOK_STATE["printed"] = True


def run_hook(kind, harness="claude"):
    """Coordinator only (Run linked and terminal that is not a worker's). Fail-open: any exception becomes exit 0, a line in the log and, in session, prompt and stop, a warning to the user."""
    HOOK_STATE.clear()
    HOOK_STATE["kind"] = kind

    def overflow(*_):
        raise TimeoutError(f"hook {kind} passou de {HOOK_TIMEOUT}s")
    try:
        signal.signal(signal.SIGALRM, overflow)
        signal.alarm(HOOK_TIMEOUT)
        if not os.environ.get("ORCA_TERMINAL_HANDLE"):
            return 0  # outside Orca there is no Run or terminal: it neither calls Orca nor fills the log
        ev = json.load(sys.stdin)
        ev["_harness_orq"] = harness  # which agent the hook came from: the coordinator keeps its own
        if os.environ.get("ORQ_MATE"):
            if kind in ("prompt", "stop"):
                mate_turn(kind, ev)  # without Orca: the deadline for requests to the mate counts from the end of its turn
            elif kind == "guard" and ev.get("tool_name") == "AskUserQuestion":
                print(json.dumps(guard_mate(), ensure_ascii=False))
                return 0
        if kind == "externas":  # every Bash, from any session: only reads the cursor, no Orca
            out = hook_external(ev, None)
            if out:
                print(json.dumps(out, ensure_ascii=False))
            return 0
        if kind == "lugar":  # on every Bash/Edit: without Orca, the coordinator is the session that already has a Run stored and is not a worker
            sid = ev.get("session_id") or ""
            if _dict(_cursor_ro().get("runs")).get(sid) and _roles().get(sid) != "worker":
                out = hook_place(ev, None)
                if out:
                    print(json.dumps(out, ensure_ascii=False))
            return 0
        if kind == "prligar":  # on every Bash: without Orca, the coordinator is the session that already has a Run stored and is not a worker
            sid = ev.get("session_id") or ""
            if _dict(_cursor_ro().get("runs")).get(sid) and _roles().get(sid) != "worker":
                out = hook_pr_link(ev, None)
                if out:
                    print(json.dumps(out, ensure_ascii=False))
            return 0
        run = coordinator(ev)
        if run is None:
            sid = ev.get("session_id") or ""
            if kind in ("prompt", "stop") and _roles().get(sid) == "worker":
                record_turn(kind, ev, harness)  # the worker only records the turn: no Orca, no Run
            if kind == "guard" and ev.get("tool_name") == "AskUserQuestion" and _roles().get(sid) == "worker":
                print(json.dumps(guard_worker(), ensure_ascii=False))  # without Orca: the box opens in the terminal, where only whoever is watching sees it
                return 0
            last_item = _dict(_cursor_ro().get("runs")).get(sid)
            if last_item and kind == "guard" and _roles().get(sid) != "worker":
                run = {"id": last_item}  # lost binding (hibernation, resume): the session that already coordinated keeps the box locked
            else:
                if last_item and kind == "prompt" and origin_name(ev.get("prompt")) == "usuario":
                    print(json.dumps(lost_binding(ev, last_item), ensure_ascii=False))
                return 0
        emit_hook(HOOKS[kind](ev, run))
    except Exception as e:
        signal.alarm(0)  # the warning (macOS notification) may take a second: the alarm has already done its job
        log(f"hook {kind}: {type(e).__name__}: {e}")
        hook_failed(e)
    finally:
        signal.alarm(0)
        if HOOK_STATE.get("failed") and not HOOK_STATE.get("printed"):
            emit_hook()  # the hook died or lost Orca before answering: the warning still reaches the user
        elif kind in WARN_KINDS and not HOOK_STATE.get("failed"):
            fail_safe.recovered(f"run:{kind}")  # it ran clean: a marker left by this hook goes away
    return 0


# ---------- grupos e secondmates (ticket 80) ----------
# Design: secondmate-por-grupo.md in the plan. A group (ORQ_HOME/groups/<name>.json) gathers the projects of one domain; the group's mate is
# a coordinator session with ORQ_MATE=<name> in the environment, which `orq mate open_page` brings up in a terminal (not via worker-start: without a dispatch, there is no
# capability for Orca to revoke after the first worker_done). The channel is events.jsonl: the coordinator asks (`mate_pedido`, id pN, deadline), the mate
# comes up (`entry` origin mate, with `corr` when answering a request), and the manager enforces the deadline (mate_lap).

GROUPS_DIR = "groups"
REQUEST_DEADLINE_S = int(os.environ.get("ORQ_PRAZO_PEDIDO_S") or 120)  # the firstmate's, counted from the end of the turn that received the request
ESCALATION_TYPES = ("resposta", "decisao", "pr", "bloqueio", "resumo")
MATE_TURNS = 20  # turns kept per mate: the deadline counts from the first one that started after the delivery, not the last
MATE_WAIT_S = float(os.environ.get("ORQ_MATE_ESPERA_S") or 90)  # the mate's claude box on screen: the resume took over 20 s with the machine loaded (01/10)
MATE_IDLE_MIN = float(os.environ.get("ORQ_MATE_OCIOSO_MIN") or 10)  # minutes with the mate's turn closed until it counts as idle (ticket 127)
MATE_SLEEP_MIN = float(os.environ.get("ORQ_MATE_DORMIR_MIN") or 20)  # minutes idle until the manager puts it to sleep (hibernate the mate)
OPEN_TURN_CAP_S = 1800  # a turn with no end (Esc, API error: the Stop did not run) counts as finished at the start after this
DELIVERED = ("enviado", "adiado")  # the notify_coordinator (ticket 82): deferred already counts as delivered, it goes out in the context of the coordinator's next prompt
CHARTER_MATE = """You are the secondmate of group {grupo} in orq. The user talks only to the coordinator; you coordinate this group's workers and do not talk to the user.
Group projects: {projetos}.{regras}
1. Run `orq groups`: if mate {grupo} already has a Run there, bind to it with `orca orchestration run-use --id <run>`; otherwise create yours, once: `orca orchestration run-create --objective "{grupo}: secondmate"`.
2. Dispatch and follow the workers as the coordinator does (`orq dispatch`, `orq agents`, `orq steer`, `orq release`), with the worker-routing skill.
3. The coordinator's request arrives typed as `orq ▸ request pN ...`. Always answer with `orq mate raise --corr pN --type answer --text "<answer>"`: nobody reads the answer in the chat.
4. Raise to the coordinator with `orq mate raise --type <decision|pr|blocker|summary> --text "..."`: a decision only the user can make, a PR or branch that is ready, a blocker, and a summary when a ticket closes. The rest stays with you.
5. Never use AskUserQuestion, do not push or open a PR: the coordinator does that, after the `pr` raise."""
MSG_MATE_RESUME = ("You are the secondmate of group {grupo} and your terminal went down. Check `orq groups` (your Run), `orq agents --run <run>` and the unanswered requests "
                  "in `orq mate requests`; answer each one with `orq mate raise --corr pN`.")
MSG_MATE_WAKE = ("You are the secondmate of group {grupo} and fell asleep from idleness; the coordinator just sent a request. It arrives typed as `orq ▸ request pN ...`; "
                   "also check `orq groups` (your Run), `orq agents --run <run>` and `orq mate requests`, and answer each request with `orq mate raise --corr pN`.")


def groups():
    """{name: cfg} from ORQ_HOME/groups/*.json. A file that is unreadable, or has `projects`/`prefixos` that are not lists, is left out with a line in the log."""
    out = {}
    for file_path in sorted(glob.glob(os.path.join(_path(GROUPS_DIR), "*.json"))):
        cfg = _read_json(file_path)
        if not isinstance(cfg, dict) or not all(isinstance(cfg.get(k, []), list) for k in ("projetos", "prefixos")):
            log(f"grupos: {file_path} ilegível ou fora do formato, ignorado")
            continue
        out[os.path.basename(file_path)[:-len(".json")]] = cfg
    return out


def _inside(cwd, folder):
    c, p = os.path.abspath(os.path.expanduser(cwd)), os.path.abspath(os.path.expanduser(folder))
    if c == p or c.startswith(p.rstrip("/") + "/"):
        return True
    if not os.path.exists(p):  # folder that no longer exists: only the string compares
        return False
    while os.path.exists(c):  # file identity: symlink, /tmp vs /private/tmp, case on a volume that does not distinguish it
        if os.path.samefile(c, p):
            return True
        parent = os.path.dirname(c)
        if parent == c:
            return False
        c = parent
    return False


def group_of(groups_, title=None, cwd=None, group_name=None):
    """(name, reason) of a request's group; (None, reason) when it stays with the coordinator. Order: explicit `group_name`, title prefix (case-insensitive),
    cwd inside a project of the group. Two groups on the same criterion is ambiguous and does not route: the coordinator decides or asks."""
    if group_name:
        if group_name not in groups_:
            raise ValueError(f"group {group_name} does not exist in {GROUPS_DIR}/ (available: {', '.join(groups_) or 'none'})")
        return group_name, "explicit"
    t = (title or "").strip().lower()
    criteria = (("title", lambda g: t and any(p and t.startswith(p.lower()) for p in g.get("prefixos") or [])),
                 ("cwd", lambda g: cwd and any(_inside(cwd, p) for p in g.get("projetos") or [])))
    for item_name, home_dir in criteria:
        findings = [n for n, g in groups_.items() if home_dir(g)]
        if len(findings) == 1:
            return findings[0], f"by {item_name}"
        if findings:
            return None, f"ambiguous by {item_name} ({', '.join(findings)}): stays with the coordinator"
    return None, "no group fits: stays with the coordinator"


def _mates():
    return _dict(_cursor_ro().get("mates"))


def _mate_mut(group_name, **fields):
    def write(c):
        m = _sub(_sub(c, "mates"), group_name)
        for k, v in fields.items():
            if k == "run":
                m["runs"] = [*[r for r in m.get("runs") or [] if r != v], v]
            else:
                m[k] = v

    _cursor_mut(write)


def mate_turn(kind, ev):
    """Prompt and stop hooks of a session with ORQ_MATE: the start and end of the mate's turn (request deadlines count from the end), the session and the cwd for resume."""
    if kind == "prompt" and origin_name(ev.get("prompt")) == "comando":
        return
    g, now_at = os.environ["ORQ_MATE"], now()

    def write(c):
        m = _sub(_sub(c, "mates"), g)
        ts = [t for t in m.get("turnos") or [] if isinstance(t, list) and len(t) == 2]
        if kind == "prompt":
            ts.append([now_at, None])
        elif ts and ts[-1][1] is None:
            ts[-1][1] = now_at
        m["turnos"] = ts[-MATE_TURNS:]
        m["terminal"] = os.environ.get("ORCA_TERMINAL_HANDLE")
        if ev.get("session_id"):
            m["sessao"] = ev["session_id"]
        if kind == "prompt" and ev.get("cwd") and not m.get("cwd"):
            m["cwd"] = ev["cwd"]

    _cursor_mut(write)


def _entry_number(e):
    m = re.fullmatch(r"e(\d+)", str(e.get("id") or ""))
    return int(m.group(1)) if m else 0


def _run_group(run):
    """{"grupo": g} when the Run belongs to a mate (its hook registered it), so the Run's entry stays in the mate's world; otherwise {}."""
    return next(({"grupo": g} for g, m in _mates().items() if run in (_dict(m).get("runs") or [])), {})


def mate_pending(event_list, mates, now_at):
    """Requests to the mate without a correlated answer, with the state: a_entregar (to deliver), aguardando (waiting), reenviar (resend), escalar (escalate) or escalado (escalated). Pure.

    The deadline counts from the end of the mate's first turn that started after the delivery (or the repost): a long turn does not blow it, and the following turns (Orca notices, heartbeats)
    do not push the deadline back. With no turn started after it, counts from the delivery itself. A turn open for more than OPEN_TURN_CAP_S counts from its start. Only an `entry` with mate origin and the same `corr` resolves. One repost, one escalation, and nothing more: never in a loop."""
    replied = {e.get("corr") for e in event_list if e.get("tipo") == "entrada" and e.get("origem") == "mate" and e.get("corr")}
    marks = {}
    for e in event_list:
        if e.get("tipo") in ("mate_entregue", "mate_reenvio", "mate_escalado"):
            marks.setdefault(e.get("corr"), {})[e["tipo"]] = _ts(e.get("ts"))
    out = []
    for p in (e for e in event_list if e.get("tipo") == "mate_pedido" and e.get("corr") not in replied):
        m, mt = marks.get(p["corr"], {}), _dict(mates.get(p.get("grupo")))
        base = {"corr": p["corr"], "grupo": p.get("grupo"), "texto": p.get("texto"), "prazo": p.get("prazo"), "ts": p.get("ts")}
        if "mate_entregue" not in m:
            out.append({**base, "estado": "a_entregar"})
            continue
        if not p.get("prazo"):
            continue
        if "mate_escalado" in m:
            out.append({**base, "estado": "escalado"})
            continue
        since = m.get("mate_reenvio") or m["mate_entregue"]
        turn = next(((_ts(i), _ts(f)) for i, f in (t for t in mt.get("turnos") or [] if isinstance(t, list) and len(t) == 2)
                      if _ts(i) and _ts(i) >= since), None)
        if turn and not turn[1] and (now_at - turn[0]).total_seconds() <= OPEN_TURN_CAP_S:
            out.append({**base, "estado": "aguardando"})
            continue
        count_from = (turn[1] or turn[0]) if turn else since
        if (now_at - count_from).total_seconds() <= p["prazo"]:
            out.append({**base, "estado": "aguardando"})
        else:
            out.append({**base, "estado": "escalar" if "mate_reenvio" in m else "reenviar"})
    return out


def _request_text(corr, text_value, deadline, again=False):
    answer_text = f" Reply with `orq mate raise --corr {corr} --type answer --text \"...\"`." if deadline else ""
    text_value = " ".join((text_value or "").split())  # the line break would submit the request halfway; the event keeps the whole text
    return f"orq ▸ request {corr}{' again, no reply on the channel' if again else ''} from the coordinator: {text_value}{answer_text}"


def mate_request(group_name, text_value, deadline=REQUEST_DEADLINE_S, responde=None):
    """Records the request (`mate_pedido`, corr pN) before typing it into the mate's terminal; if the mate is in the middle of a turn, the manager delivers it later.
    `responde`: the entry the mate raised and this request answers (the decision coming back); it closes with the `mate` effect."""
    if group_name not in groups():
        raise ValueError(f"group {group_name} does not exist in {GROUPS_DIR}/")
    m0 = _dict(_mates().get(group_name))
    terminal, sleeping = m0.get("terminal"), bool(m0.get("dormiu")) and not m0.get("terminal")
    if not terminal and not sleeping:
        raise ValueError(f"group {group_name} has no open mate: orq mate open {group_name}")
    if responde:
        event_list = read_events()
        target = next((e for e in event_list if e.get("tipo") == "entrada" and e.get("id") == responde), None)
        if not target or target.get("origem") != "mate" or target.get("mate") != group_name:
            raise ValueError(f"--answers {responde}: not an entry that mate {group_name} raised; nothing was written")
        if any(e.get("tipo") == "intake" and e.get("entrada") == responde for e in event_list):
            raise ValueError(f"--answers {responde}: the entry is already closed; nothing was written")
    if sleeping:
        terminal = mate_open(group_name)["terminal"]  # the request wakes the mate: resumes the session before recording and typing the request
    with _lock("cursor.lock"):
        n = 1 + max((int(e["corr"][1:]) for e in read_events() if e.get("tipo") == "mate_pedido" and re.fullmatch(r"p\d+", str(e.get("corr")))), default=0)
        corr = f"p{n}"
        _write_event({"tipo": "mate_pedido", "corr": corr, "grupo": group_name, "texto": text_value[:2000], "prazo": deadline, **({"responde": responde} if responde else {})})
    if responde:
        intake(responde, "mate", corr)
    before = now()  # the mate's hook records the turn start before the typing returns: the delivery counts from before it
    delivery = type_text(terminal, _request_text(corr, text_value, deadline))
    if delivery == "enviado":
        append_event({"tipo": "mate_entregue", "corr": corr, "ts": before})
    return {"corr": corr, "grupo": group_name, "entrega": delivery}


def mate_raise(type_name, text_value, corr=None, link=None, group_name=None):
    """The mate escalates to the coordinator: an entry with mate origin (the coordinator treats it like the others, with orq intake), with `corr` when it answers a request."""
    g = group_name or os.environ.get("ORQ_MATE")
    if not g:
        raise ValueError("orq mate raise runs in the mate's terminal (ORQ_MATE) or with --group")
    if type_name not in ESCALATION_TYPES:
        raise ValueError(f"--type {type_name}: use {'|'.join(ESCALATION_TYPES)}")
    if corr and not any(e.get("tipo") == "mate_pedido" and e.get("corr") == corr and e.get("grupo") == g for e in read_events()):
        raise ValueError(f"request {corr} does not exist for group {g}: the answer was not written")
    if type_name == "resposta" and not corr:
        raise ValueError("--type answer needs --corr <request>")
    return append_event({"tipo": "entrada", "origem": "mate", "mate": g, "tipo_mate": type_name, "texto": text_value[:2000], "fonte": f"mate {g}",
                         **({"corr": corr} if corr else {}), **({"link": link} if link else {})}, new_id=True)


def _mate_command(group_name, cfg, session, cwd=None, was_sleeping=False):
    """(terminal command, text to type after the agent starts, or None if the text goes on the command line)."""
    agent, model = cfg.get("harness") or "claude", cfg.get("modelo")
    if agent not in HARNESS:
        raise ValueError(f"harness {agent} of group {group_name}: orq only opens {', '.join(HARNESSES)}")
    if session:
        text_value = (MSG_MATE_WAKE if was_sleeping else MSG_MATE_RESUME).format(grupo=group_name)
    else:
        rules = f"\nFirst read the group rules at {cfg['regras']}." if cfg.get("regras") else ""
        text_value = CHARTER_MATE.format(grupo=group_name, projetos=", ".join(cfg.get("projetos") or []) or "none", regras=rules)
    typed = HARNESS[agent].get("digita_prompt")
    msg = None if typed else text_value
    cmd = HARNESS[agent]["resume"](session, model, cfg.get("effort"), msg) if session else HARNESS[agent]["abrir"](model, cfg.get("effort"), msg)
    # `ORQ_MATE=x claude` and not `env ORQ_MATE=x claude`: in an Orca terminal (fish) a claude launched via `env` runs non-interactive (sdk-cli, or the --print error
    # without prompt), with or without a prompt on the line. The direct assignment works in fish, zsh and bash and leaves claude interactive (ticket 106)
    mine = backlog_group(group_name, cfg)  # the group that already received tickets (`orq backlog mover`) reads and writes its own backlog, not the machine's
    environment = f"ORQ_MATE={shlex.quote(group_name)} " + (f"ORQ_BACKLOG={shlex.quote(mine)} " if mine and os.path.exists(mine) else "")
    command = environment + shlex.join([x for x in cmd if x is not None])
    # Orca only creates a terminal in a worktree it knows, and the group's folder (the orq clone) is not one: the terminal opens in the current checkout and cds into it.
    # `cd x; y` works in fish, zsh and bash; claude's resume only finds the session in the cwd where it was born
    return (f"cd {shlex.quote(cwd)}; {command}" if cwd else command), (" ".join(text_value.split()) if typed else None)  # the line break would submit in the middle


def _agent_ready(handle, agent, waiting=None):
    """Waits, up to `waiting` (RESUME_WAIT_S), for the agent's box to appear in the new terminal (HARNESS ready screen). False with the failure screen (session that does not exist) or without
    the box by the deadline: typing earlier would land in the shell."""
    end, screen_ = time.time() + (RESUME_WAIT_S if waiting is None else waiting), HARNESS[agent]["tela"]
    while True:
        screen = "\n".join(orca("read", "--terminal", handle, "--screen", area="terminal")["terminal"].get("tail") or [])
        if any(f in screen for f in screen_["falha"]):
            return False
        if screen_.get("pronto") and screen_["pronto"].search(screen):
            return True
        if time.time() >= end:
            return False
        time.sleep(1)


def mate_open(group_name):
    """Opens the group's mate in a new terminal, or resumes it (`--resume` of the session its hooks recorded) if it went down. One live mate per group."""
    cfg = groups().get(group_name)
    if cfg is None:
        raise ValueError(f"group {group_name} does not exist in {GROUPS_DIR}/")
    m, live = _dict(_mates().get(group_name)), _alive_terminals()
    if live is None:
        raise ValueError("Orca did not list the terminals: without knowing whether the mate is alive, nothing was opened")
    if m.get("terminal") in live:
        raise ValueError(f"mate {group_name} is already open in terminal {m['terminal']}")
    cwd = m.get("cwd") or cfg.get("cwd") or next(iter(cfg.get("projetos") or []), None)
    cwd = cwd and os.path.expanduser(cwd)
    agent = cfg.get("harness") or "claude"
    command, text_value = _mate_command(group_name, cfg, m.get("sessao"), cwd, was_sleeping=bool(m.get("dormiu")))
    # the terminal opens in the group's project, if Orca knows it (it groups the mate with the project on screen): `mate_project`, otherwise the first of the `projects`
    mate_folder = os.path.expanduser(cfg.get("projeto_mate") or next(iter(cfg.get("projetos") or []), "")) or None
    new = _new_terminal(f"mate {group_name}{' (resumed)' if m.get('sessao') else ''}", command, mate_folder and _repo_in_orca(mate_folder))
    ok = _agent_ready(new, agent, MATE_WAIT_S) and type_text(new, text_value) == "enviado" if text_value else not m.get("sessao") or _came_back(new, agent)
    if not ok:
        # a session that does not come back, or an agent that did not come up, leaves a shell: the manager would type the request into it. Close it and, if it was a resume, forget the session
        with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired):
            orca("close", "--terminal", new, area="terminal")
        _mate_mut(group_name, terminal=None, **({"sessao": None} if m.get("sessao") else {}))
        reason = "the session did not come back" if m.get("sessao") else "the agent did not reach the prompt"
        append_event({"tipo": "mate", "op": "abrir", "grupo": group_name, "terminal": new, "retomado": False, "falhou": reason})
        raise ValueError(f"mate {group_name}: {reason} within {MATE_WAIT_S:.0f} s; terminal closed. Run orq mate open {group_name} again"
                         + (" to open with the charter" if m.get("sessao") else ""))
    _mate_mut(group_name, terminal=new, morto=None, dormiu=None, aberto_em=now(), **({"cwd": cwd} if cwd else {}))  # opened_at: the idleness clock starts here
    append_event({"tipo": "mate", "op": "abrir", "grupo": group_name, "terminal": new, "retomado": bool(m.get("sessao")), "anterior": m.get("terminal")})
    if m.get("dormiu"):
        append_event({"tipo": "mate_acordou", "grupo": group_name, "terminal": new, "sessao": m.get("sessao")})
    return {"grupo": group_name, "terminal": new, "retomado": bool(m.get("sessao"))}


def mate_lap():
    """One round of the manager over the mates: delivers the request that was waiting for the mate to be free, resends once what blew the deadline, escalates once what blew
    it again, notifies the coordinator of each new startup and of the mate that went down (once per terminal). Returns one line per action."""
    mates, line_list = _mates(), []
    if not mates:
        return line_list
    coord_handle = (_manager_cfg() or {}).get("coordenador")
    event_list, now_at = read_events(), datetime.now(timezone.utc)
    for p in mate_pending(event_list, mates, now_at):
        terminal, before = _dict(mates.get(p["grupo"])).get("terminal"), now()
        if p["estado"] == "a_entregar" and terminal and type_text(terminal, _request_text(p["corr"], p["texto"], p["prazo"])) == "enviado":
            append_event({"tipo": "mate_entregue", "corr": p["corr"], "ts": before})
            line_list.append(f"mate {p['grupo']}: {p['corr']} delivered")
        elif p["estado"] == "reenviar" and terminal and type_text(terminal, _request_text(p["corr"], p["texto"], p["prazo"], again=True)) == "enviado":
            append_event({"tipo": "mate_reenvio", "corr": p["corr"], "ts": before})
            line_list.append(f"mate {p['grupo']}: {p['corr']} resent")
        elif p["estado"] == "escalar" and coord_handle and notify_coordinator(coord_handle, f"orq ▸ mate {p['grupo']} did not answer request {p['corr']} ({_quote(p['texto'])!r}) "
                                                                               f"even after the repost. See terminal {terminal}.") in DELIVERED:
            append_event({"tipo": "mate_escalado", "corr": p["corr"]})
            line_list.append(f"mate {p['grupo']}: {p['corr']} escalated to the coordinator")
    until_at = _cursor_ro().get("mate_avisada_ate")
    until_at = until_at if isinstance(until_at, int) else 0  # the high-water mark: the highest startup eN already notified (a capped list would re-notify the old ones)
    for e in (x for x in event_list if x.get("tipo") == "entrada" and x.get("origem") == "mate" and _entry_number(x) > until_at):
        if not coord_handle or notify_coordinator(coord_handle, f"orq ▸ mate {e['mate']} raised {e['id']} ({e.get('tipo_mate')}): {_quote(e.get('texto'), 200)}. Handle with orq intake {e['id']} "
                                                 f"<effect>; to reply to the mate, orq mate request {e['mate']} --answers {e['id']} --text \"...\"") not in DELIVERED:
            break
        _cursor_mut(lambda c, n=_entry_number(e): c.__setitem__("mate_avisada_ate", n))
        line_list.append(f"mate {e['mate']}: raise {e['id']} reported to the coordinator")
    live = _alive_terminals()
    for g, m in mates.items():
        t = _dict(m).get("terminal")
        if live is None or not t or t in live or _dict(m).get("morto") == t:
            continue
        if coord_handle and notify_coordinator(coord_handle, f"orq ▸ mate {g} went down (terminal {t} vanished from Orca). Bring it back with: orq mate open {g}") in DELIVERED:
            _mate_mut(g, morto=t)
            line_list.append(f"mate {g}: went down, coordinator notified")
    return line_list


def _idle_min(m, now_at, pending):
    """Minutes the mate has been idle, or None if not: turn closed (or open beyond the cap, which counts from the start), no open request. Pure.
    The clock starts from the later of the end of the last turn and `opened_at` (the resume): the old turn of the mate that just woke up does not count."""
    if pending or not m.get("terminal"):
        return None
    ts = [t for t in m.get("turnos") or [] if isinstance(t, list) and len(t) == 2]
    start_at, end = (_ts(ts[-1][0]), _ts(ts[-1][1])) if ts else (None, None)
    if ts and not end:
        if not start_at or (now_at - start_at).total_seconds() <= OPEN_TURN_CAP_S:
            return None
        end = start_at
    since = max(filter(None, [end, _ts(m.get("aberto_em"))]), default=None)
    return (now_at - since).total_seconds() / 60 if since else None


def mate_status(m, now_at, pending, has_worker, minimum=None):
    """"dormindo" (sleeping), "ocioso há N min" (idle for N min) or "trabalhando" (working). Idle is the turn closed for `minimum` min (ORQ_MATE_OCIOSO_MIN), with no open request and no live worker of its
    Run (`has_worker`, only called with the rest idle). Pure, apart from `has_worker`."""
    if m.get("dormiu") and not m.get("terminal"):
        return "sleeping"
    idle = _idle_min(m, now_at, pending)
    if idle is None or idle < (MATE_IDLE_MIN if minimum is None else minimum) or has_worker():
        return "working"
    return f"idle for {int(idle)} min"


def _mate_has_worker(m, agent_rows=None):
    """Is there a worker of a mate's Run that was not released (rodando (running), hibernated, entregue (delivered) without release, in the integration queue)? Unreadable Orca counts as yes."""
    try:
        agent_rows = agents() if agent_rows is None else agent_rows
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        log(f"mate: agentes ilegível ({e}); sem prova de que não há worker")
        return True
    return any(a.get("run") in (m.get("runs") or []) and a.get("estado") not in ("liberado", "encerrado") for a in agent_rows)


def _group_mate(item_name, mates, live, event_list, now_at):
    """(state, terminal, Runs, unanswered requests) of a group's mate; state "sem mate" (no mate), "caiu" (down), "dormindo" (sleeping), "ocioso há N min" (idle for N min) or "trabalhando" (working)."""
    m = _dict(mates.get(item_name))
    pending = [p for p in mate_pending(event_list, mates, now_at) if p["grupo"] == item_name]
    if m.get("dormiu") and not m.get("terminal"):
        state = "sleeping"
    elif not m.get("terminal"):
        state = "no mate"
    elif live is None:
        state = "working"  # without Orca's list there is no proof of idleness
    elif m["terminal"] not in live:
        state = "down"
    else:
        state = mate_status(m, now_at, pending, lambda: _mate_has_worker(m))
    return state, m.get("terminal"), m.get("runs") or [], pending


def groups_text(group_map, mates, live, event_list, now_at):
    line_list = []
    for item_name, cfg in group_map.items():
        state, terminal, runs, pending = _group_mate(item_name, mates, live, event_list, now_at)
        line_list.append(f"{item_name}: {', '.join(cfg.get('projetos') or [])} | prefixes {', '.join(cfg.get('prefixos') or []) or '-'} | mate {state}"
                      + (f" ({terminal}, Runs {', '.join(runs) or '-'})" if terminal else "")
                      + (f" | requests: {', '.join(p['corr'] + ' ' + p['estado'] for p in pending)}" if pending else ""))
    return "\n".join(line_list) or f"no group in {_path(GROUPS_DIR)}"


def mate_ready(tks, group_map, queue):
    """{group: [ticket numbers]} of the ready tickets that match each group's title prefix: `ready-for-agent`, every blocker closed, outside the dispatch queue."""
    by_num, queued = {t["num"]: t for t in tks}, {i.get("ticket") for i in queue}
    found = {n: [] for n in group_map}
    for t in tks:
        if t["status"] != STATUS_NEW or t["num"] in queued or any((by_num.get(b) or {}).get("status") != STATUS_CLOSED for b in t["blocked_by"]):
            continue
        item_name, _ = group_of(group_map, title=t["titulo"])
        if item_name:
            found[item_name].append(t["num"])
    return found


def _ready_min(cfg, machine):
    """Ready tickets a group without a mate gathers before orq proposes opening it: the group's `mate_ready_min`, otherwise the machine's (default 3)."""
    v = cfg.get("mate_ready_min")
    return v if isinstance(v, int) and not isinstance(v, bool) and v > 0 else machine["mate_ready_min"]


def mate_proposals(tks, group_map, mates, queue, machine):
    """[(group, [ticket numbers])] of the groups with no mate (neither open nor asleep) that gather `_ready_min` or more ready tickets. Pure."""
    ready = mate_ready(tks, group_map, queue)
    return [(n, ready[n]) for n, cfg in group_map.items()
            if not _dict(mates.get(n)).get("terminal") and not _dict(mates.get(n)).get("dormiu") and len(ready[n]) >= _ready_min(cfg, machine)]


def mate_proposal_lap():
    """The manager's round over the proposals: tells the coordinator once per proposal that a group gathered ready tickets with no mate, and with `"mate_auto": true` in the
    group opens the mate itself (also once: a failure goes to the coordinator, it does not repeat every round). A group that stops qualifying forgets the mark."""
    group_map = groups()
    if not group_map:
        return []
    proposals = mate_proposals(tickets(), group_map, _mates(), dispatch_queue_items(), machine_cfg())
    noticed = _cursor_ro().get("mate_proposal_notified")
    noticed = noticed if isinstance(noticed, list) else []
    coord_handle, line_list, done = (_manager_cfg() or {}).get("coordenador"), [], []
    for item_name, nums in proposals:
        if item_name in noticed:
            done.append(item_name)
            continue
        if group_map[item_name].get("mate_auto") is True:
            try:
                mate_open(item_name)
                line_list.append(f"mate {item_name}: opened by mate_auto ({len(nums)} ready tickets)")
                done.append(item_name)
                continue
            except (ValueError, RuntimeError, subprocess.TimeoutExpired) as e:
                text = f"orq ▸ mate_auto could not open the mate of group {item_name} ({_quote(str(e), 160)}). Run: orq mate open {item_name}"
        else:
            text = (f"orq ▸ group {item_name} has {len(nums)} ready tickets ({', '.join(nums)}) and no mate. Open it with: orq mate open {item_name}"
                    f" (or set \"mate_auto\": true in groups/{item_name}.json)")
        if coord_handle and notify_coordinator(coord_handle, text) in DELIVERED:
            done.append(item_name)
            line_list.append(f"mate {item_name}: proposal reported to the coordinator")
    if sorted(done) != sorted(noticed):
        _cursor_mut(lambda c: c.__setitem__("mate_proposal_notified", done))
    return line_list


def mate_lines():
    """The `orq status` lines: `mate <group>: <state>` per group with a recorded mate (trabalhando (working), ocioso há N min (idle for N min), dormindo (sleeping) or caiu (down),
    terminal, unanswered requests), and `group <name>: N ready (…) | mate alive|sleeping|down|absent` per group, with the proposal to open the mate when it gathers enough."""
    group_map = groups()
    if not group_map:
        return []
    mates = _mates()
    live, event_list, now_at = (_alive_terminals() if mates else None), read_events(), datetime.now(timezone.utc)
    machine, ready = machine_cfg(), mate_ready(tickets(), group_map, dispatch_queue_items())
    line_list = []
    for item_name, cfg in group_map.items():
        state, terminal, _, pending = _group_mate(item_name, mates, live, event_list, now_at)
        if terminal or state == "sleeping":
            line_list.append(f"mate {item_name}: {state}" + (f" ({terminal})" if terminal else "") + (f" | requests: {', '.join(p['corr'] + ' ' + p['estado'] for p in pending)}" if pending else ""))
        kind = {"no mate": "absent", "sleeping": "sleeping", "down": "down"}.get(state, "alive")
        nums = ready[item_name]
        line_list.append(f"group {item_name}: {len(nums)} ready" + (f" ({', '.join(nums)})" if nums else "") + f" | mate {kind}"
                         + (f" | propose: orq mate open {item_name}" if kind == "absent" and len(nums) >= _ready_min(cfg, machine) else ""))
    return line_list


def dispatch_group(title, ticket=None, project=None, run=None):
    """(group, reason) of a dispatch that belongs to a group with a configured mate (open or asleep), by title prefix or by the project's folder; (None, reason) otherwise.
    The mate's own dispatches (ORQ_MATE) never go back up to it."""
    if os.environ.get("ORQ_MATE"):
        return None, "dispatch made by a mate"
    if ticket and not title:
        tk = next((t for t in tickets() if t["num"] == str(ticket).strip().zfill(2)), None)
        title = tk and tk["titulo"]
    project = dispatch_project(project, run)
    item_name, reason = group_of(groups(), title=title, cwd=project and repo_folder(projects()[project]["repo"]))
    if not item_name:
        return None, reason
    m, live = _dict(_mates().get(item_name)), _alive_terminals()
    if (m.get("dormiu") and not m.get("terminal")) or (m.get("terminal") and (live is None or m["terminal"] in live)):
        return item_name, reason
    return None, f"group {item_name} has no mate open or asleep: the coordinator dispatches"


def dispatch_to_mate(item_name, reason, run, title, ticket, spec_file, model, effort):
    """`orq dispatch` of a group's ticket: asks the group's mate to dispatch it (`orq mate request`, which wakes a sleeping one) instead of starting the worker."""
    what = f"ticket {str(ticket).zfill(2)}" if ticket else f"{title!r} (spec {spec_file})"
    r = mate_request(item_name, f"dispatch {what} in your Run: model {model}, effort {effort} (the coordinator's Run is {run}). Answer with the worker's dispatch id.")
    append_event({"tipo": "mate_dispatch", "grupo": item_name, "corr": r["corr"], "run": run, "motivo": reason, "modelo": model, "effort": effort,
                  **({"ticket": str(ticket).zfill(2)} if ticket else {"titulo": title})})
    return {"estado": "mate", "grupo": item_name, "corr": r["corr"], "entrega": r["entrega"], "motivo": reason}


def mate_sleep(group_name, reason="manual", agent_rows=None):
    """Hibernates the group's mate: keeps the session (already recorded by its hooks), marks `slept` and closes the terminal; `orq mate pedir` wakes it with `--resume`. Refuses, with ValueError,
    what worker hibernation also refuses: terminal of the coordinator, of the manager or of this process, busy screen or one with a draft, and what the mate cannot drop: open request
    and unreleased worker of its Run (the delivery notice would land in a closed terminal)."""
    cfg, m = groups().get(group_name), _dict(_mates().get(group_name))
    if cfg is None:
        raise ValueError(f"group {group_name} does not exist in {GROUPS_DIR}/")
    t, agent = m.get("terminal"), cfg.get("harness") or "claude"
    if not t:
        raise ValueError(f"mate {group_name} is not open" + (" (already asleep)" if m.get("dormiu") else ""))
    live = _alive_terminals()
    if live is None or t not in live:
        raise ValueError(f"mate {group_name}: Orca does not list terminal {t}; nothing was closed")
    g = _manager_cfg()
    if t in {os.environ.get("ORCA_TERMINAL_HANDLE"), g.get("coordenador"), g.get("gerente")}:
        raise ValueError(f"terminal {t} is the coordinator, the manager or the caller's own: it never sleeps")
    if not m.get("sessao") or agent not in HARNESS:
        raise ValueError(f"mate {group_name} has no session_id recorded by the hooks: there is no way back")
    if pending := [p["corr"] for p in mate_pending(read_events(), _mates(), datetime.now(timezone.utc)) if p["grupo"] == group_name]:
        raise ValueError(f"mate {group_name} has an open request ({', '.join(pending)})")
    if _mate_has_worker(m, agent_rows):
        raise ValueError(f"mate {group_name} has a worker in its Run that was not released")
    try:
        busy = _busy_screen(t, agent) or free_terminal(t)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        raise ValueError(f"mate {group_name}: screen unreadable ({e})")
    if busy:
        raise ValueError(f"mate {group_name} is not free: {busy}")
    _mate_mut(group_name, terminal=None, dormiu=now())  # before the close: a crash in the middle leaves neither the manager able to warn "caiu" (went down) nor the resume able to bring the mate up
    try:
        orca("close", "--terminal", t, area="terminal")
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _mate_mut(group_name, terminal=t, dormiu=None)
        raise ValueError(f"mate {group_name}: terminal close failed ({e})")
    append_event({"tipo": "mate_dormiu", "grupo": group_name, "terminal": t, "sessao": m["sessao"], "motivo": reason})
    return {"grupo": group_name, "terminal": t, "sessao": m["sessao"], "motivo": reason}


def _list_and(item_list):
    return item_list[0] if len(item_list) == 1 else ", ".join(item_list[:-1]) + " and " + item_list[-1]


def mates_sleep(now_at=None):
    """One round of the manager: a mate idle for ORQ_MATE_DORMIR_MIN min sleeps (`mate_sleep`), unless the group has a `ready` ticket and the machine has a free slot: then it notifies the coordinator,
    once per idle period, and does not hibernate. Refusal on the screen or on the requests is silent: the next round checks again. Returns the panel lines."""
    mates = _mates()
    if not mates:
        return []
    now_at, group_map, event_list, live, line_list = now_at or datetime.now(timezone.utc), groups(), read_events(), _alive_terminals(), []
    coord_handle = (_manager_cfg() or {}).get("coordenador")
    for g, m in ((g, _dict(m)) for g, m in mates.items()):
        if live is None or g not in group_map or m.get("terminal") not in live:
            continue
        idle = _idle_min(m, now_at, [p for p in mate_pending(event_list, mates, now_at) if p["grupo"] == g])
        if idle is None or idle < MATE_SLEEP_MIN:
            continue
        agent_rows = agents()
        if _mate_has_worker(m, agent_rows):
            continue
        ready = [t["num"].lstrip("0") or "0" for t in tickets() if t["status"] == "ready" and group_of(group_map, title=t["titulo"])[0] == g]
        if ready and machine_panel(agent_rows)["livres"] > 0:
            key_name = json.dumps([m.get("aberto_em"), (m.get("turnos") or [])[-1:], ready])
            if coord_handle and m.get("prontos_avisados") != key_name and notify_coordinator(
                    coord_handle, f"orq ▸ mate {g} idle, {_list_and(ready)} ready. Ask with orq mate request {g} --text \"...\"") in DELIVERED:
                _mate_mut(g, prontos_avisados=key_name)
                line_list.append(f"mate {g}: idle for {int(idle)} min, coordinator notified ({', '.join(ready)} ready)")
            continue
        try:
            mate_sleep(g, f"idle for {int(idle)} min", agent_rows)
        except ValueError as e:
            log(f"mate dormir {g}: recusado ({e})")
            continue
        line_list.append(f"mate {g}: slept (idle for {int(idle)} min)")
    return line_list


def guard_mate():
    """PreToolUse of AskUserQuestion in a mate: the user does not look at its terminal; the decision goes up to the coordinator."""
    reason = (f"{MARK} the secondmate does not ask the user: raise the decision with `orq mate raise --type decision --text \"<question and options, with the recommended one>\"`; "
              "the answer comes back as `orq ▸ request pN`.")
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": reason}}


# ---------- comandos ----------

def intake(e, effect, ref=None, run=None, note=None):
    """Records the effect of an entry. Raises ValueError when the reference does not exist."""
    effect = VALUES_PT["efeito"].get(effect, effect)  # task|decision|conversation|discarded count the same as the pt names
    if effect not in EFFECTS:
        raise ValueError(f"invalid effect: {effect} (use {'|'.join(VALUES_EN['efeito'].get(x, x) for x in EFFECTS)})")
    event_list = read_events()
    target_entry = next((x for x in event_list if x.get("id") == e and x.get("tipo") == "entrada"), None)
    if not target_entry:
        raise ValueError(f"entry {e} does not exist")
    if effect == "conversa" and target_entry.get("origem") in ("relatorio", "relatorio_worker"):
        raise ValueError(f"entry {e} is a report item ({_quote(target_entry.get('texto'))!r}, {target_entry.get('fonte')}): conversation does not handle it; "
                         "use task, steer, pend, decision or discarded --note <reason>")
    if effect in ("conversa", "descartado") and (obligations_open := open_obligations(event_list, e)):
        raise ValueError(f"entry {e} has an open obligation: " + ", ".join(f"{o['chave']} ({o['texto']})" for o in obligations_open)
                         + f'; close each one with orq fulfill {e} <obligation> --proof "<url, version, hash>" or orq defer {e} <obligation> --reason "…"')
    ev = {"tipo": "intake", "entrada": e, "efeito": effect}
    if effect in ("tarefa", "steer"):
        if not ref:
            raise ValueError(f"{effect} needs the task id")
        target = default_run(run)
        if not target:
            raise ValueError("no Run bound: pass --run")
        if effect == "tarefa" and not coordinator_run(target):
            print(f"warning: task-create --run {target} is refused (consumer_fenced) because the coordinator does not command that Run; "
                  f"to create a task in it, {bind_tip(target)} first (run-create and run-use take the coordinator off the previous Run).",
                  file=sys.stderr)
        if not any(t["id"] == ref for t in orca("task-list", "--run", target, timeout=20)["tasks"]):
            raise ValueError(f"task {ref} does not exist in Run {target}")
        ev.update(ref=ref, run=target)
    elif effect == "mate":
        if not any(x.get("tipo") == "mate_pedido" and x.get("corr") == ref for x in event_list):
            raise ValueError(f"mate needs the request that answered the entry (pN), and {ref} does not exist")
        ev["ref"] = ref
    elif effect in ("pend", "decisao"):
        if not ref:
            raise ValueError(f"{effect} needs the pending item id")
        # an answered decision has already left the file: the pending item the registry saw being created counts too
        in_file = any(i.get("id") == ref for i in (_pending_ro() or {}).get("itens", []))
        no_log = any(x.get("tipo") == "pend" and x.get("op") == "add" and x.get("pend") == ref for x in event_list)
        if not (in_file or no_log):
            raise ValueError(f"pending item {ref} does not exist in pendencias.json or in the event log")
        ev["ref"] = ref
    if note:
        ev["nota"] = note
    append_event(ev)
    # the echo says which entry was just closed: whoever closes by the wrong id sees the text
    return {**ev, "origem": target_entry.get("origem") or "usuario", "texto": _quote(target_entry.get("texto")), **({"fonte": target_entry["fonte"]} if target_entry.get("fonte") else {})}


def implicit_intake(effect, ref=None, run=None):
    """The intake that the coordinator's command already implies (ticket 157). Only counts when there is exactly one open user entry, typed in this terminal: a
    worker session has no entry with its handle, so it never records. With two or more open it does not guess. Returns the line for stderr, or None."""
    handle = os.environ.get("ORCA_TERMINAL_HANDLE")
    ds = [e["id"] for e in open_entries(read_events()) if e.get("origem", "usuario") == "usuario" and handle and e.get("terminal") == handle]
    if not ds:
        return None
    target = f"{effect} {ref}" if ref else effect
    if len(ds) > 1:
        return f"warning: {len(ds)} open entries ({', '.join(ds)}), I will not guess the effect: run orq intake <e> {target}" + (f" --run {run}" if run else "")
    try:
        intake(ds[0], effect, ref, run)
    except ValueError as e:
        return f"warning: implicit intake of {ds[0]} not recorded ({e}): run orq intake {ds[0]} {target}"
    return f"intake {ds[0]} → {target} (implicit)"


def _lavish_items(doc):
    """Items (dicts with id) from any `items` list in what `lavish-axi poll` returned: loose data.items, inside a list of prompts,
    nested or inside a prompt's text, after "Context data:" (that is how queuePrompt delivers the `data`).

    A repeated id counts by its last occurrence (the user sent it again)."""
    findings = {}

    def anda(x):
        if isinstance(x, dict):
            item_list = x.get("items")
            if isinstance(item_list, list):
                for i in item_list:
                    if isinstance(i, dict) and i.get("id") is not None:
                        findings.pop(str(i["id"]), None)
                        findings[str(i["id"])] = i
            for v in x.values():
                if not isinstance(v, list) or v is not item_list:
                    anda(v)
        elif isinstance(x, list):
            for v in x:
                anda(v)
        elif isinstance(x, str) and MARK_LAVISH in x:
            with contextlib.suppress(ValueError):
                anda(json.JSONDecoder().raw_decode(x.split(MARK_LAVISH, 1)[1].lstrip())[0])
    anda(doc)
    return list(findings.values())


def _poll_items(text_value):
    """Items of the batch from the file: JSON (with data.items or prompts) or the raw output of `lavish-axi poll`, which is TOON.

    In TOON the prompt text is a quoted string in JSON format, with the `data` as escaped JSON after "Context data:"; each
    quoted string is decoded and the ones carrying the marker are read."""
    try:
        doc = json.loads(text_value)
    except ValueError:
        doc = []
        for m in _STRING_JSON.finditer(text_value):
            with contextlib.suppress(ValueError):
                doc.append(json.loads(m.group(0)))
    return _lavish_items(doc)


def lavish_answer(path):
    """Records the user's answer coming from Lavish: the output of `lavish-axi poll` (raw, or its JSON; `-` reads standard input), with the batch
    of items id, header, resposta, disposicao.

    One `resposta_lavish` event per item; the same batch twice does not duplicate (hash of the items, so the raw output and the JSON extracted from it are
    the same batch). The `header` closes the decision pending item when the disposicao is an explicit choice (CHOICE_LAVISH) and there is an answer; free
    text, deferral (`adiar`, `conversar`) and action or notice pending items stay open, as in AskUserQuestion (review 2, M3). The linked Run is
    queried and the pending item closed before the event is recorded: if Orca fails, nothing was registered and running again redoes it (review 4, M8).
    Returns {"lote", "itens": [{"item", "efeito"}], "avisos": [...]}.
    """
    if path == "-":
        raw = sys.stdin.buffer.read()
    else:
        with open(path, "rb") as f:
            raw = f.read()
    item_list = _poll_items(raw.decode("utf-8", "replace"))
    if not item_list:
        raise ValueError("no item in Context data or in data.items: this is not the output of lavish-axi poll")
    import hashlib
    lote = hashlib.sha1(json.dumps(item_list, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:10]
    already = {(e.get("lote"), e.get("item")) for e in read_events() if e.get("tipo") == "resposta_lavish"}
    output, notices, current = [], [], _UNKNOWN
    for it in item_list:
        id_, header = str(it["id"]), str(it.get("header") or "")
        answer_text, disposition = str(it.get("resposta") or "").strip(), str(it.get("disposicao") or it.get("disposition") or "").strip()
        if disposition == "conversar" or answer_text == CHAT_LAVISH:  # "vamos conversar" (let's talk) is a postponement: the marker is not a user answer
            answer_text, disposition = "", "conversar"
        if (lote, id_) in already:
            output.append({"item": id_, "efeito": "repetido"})
            continue
        pending_by_id = {i.get("id"): i for i in _load_pending()["itens"]}
        pending = pending_by_id.get(header)
        aggregated = ALREADY_DONE in (id_, header)
        # "já fiz" (already done): the `done` disposition, or an explicit choice with the answer "feito" (the 09/29 batch) or on item `already-fez`; free text, `adiar` and `conversar` do not
        done = disposition == DONE_LAVISH or (disposition in CHOICE_LAVISH and bool(answer_text) and (aggregated or answer_text.lower() == DONE_LAVISH))
        explicit_choice = done or (disposition in CHOICE_LAVISH and bool(answer_text))
        without_ids = done and aggregated and not it.get("ids")
        if without_ids:  # B22: free text does not say which to close ("feito o A, menos o B"); only the structured `ids` list counts
            done, explicit_choice = False, False
            notices.append(f'{header}: no `ids` (structured list), nothing was closed; close with orq pend done <id> --answer "feito"')
        if done and aggregated:  # the ids of the finished pending items, in the page's `ids` list
            targets = [i for i in pending_by_id if i in {str(x) for x in it.get("ids") or []}]
        elif done:
            targets = [header] if pending is not None else []
        else:
            targets = [header] if explicit_choice and pending is not None and pending.get("tipo") == "decisao" else []
        did_close, not_delivered = [], False
        for target in targets:
            gr = pending_by_id[target].get("gate_run")
            if gr and current is _UNKNOWN and not _is_manager_run(gr):
                current = _own_run()  # before recording: with Orca down, the pending item is not left closed without the event
            try:
                done_ = pending_done(target, None if done else answer_text, current, confirm=True)
            except DeliveryNotConfirmed as e:  # the destination did not confirm: nothing closes and nothing is recorded, running again redoes it
                notices.append(str(e))
                not_delivered = True
                continue
            except ValueError as e:  # another orq closed in the middle
                log(f"lavish-answer: {e}")
                continue
            did_close.append(target)
            if done_.get("aviso"):
                notices.append(done_["aviso"])
        if not_delivered and not did_close:
            output.append({"item": id_, "efeito": "nao entregue"})
            continue
        append_event({"tipo": "resposta_lavish", "item": id_, "header": header, "resposta": answer_text, "disposicao": disposition, "lote": lote,
                      **({} if explicit_choice else {"livre": True}), **({"fechou": did_close} if did_close else {})})
        if did_close:
            output.append({"item": id_, "efeito": "fechou", "pend": did_close[0] if len(did_close) == 1 else did_close})
        elif pending is not None and pending.get("tipo") == "decisao":
            notices.append(f'free-text answer in {header}: if decided, close with orq pend done {shlex.quote(header)} --answer "<what was decided>"')
            output.append({"item": id_, "efeito": "aberta"})
        else:
            output.append({"item": id_, "efeito": "so registrada"})
    return {"lote": lote, "itens": output, "avisos": notices}


_QUESTION_PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITULO__</title><style>
:root{color-scheme:light dark;--bg:#fafafa;--fg:#1a1a1a;--card:#fff;--bd:#d4d4d8;--ac:#6d28d9}
@media(prefers-color-scheme:dark){:root{--bg:#18181b;--fg:#f4f4f5;--card:#27272a;--bd:#3f3f46;--ac:#a78bfa}}
html,body{background:var(--bg);color:var(--fg);margin:0;font:16px/1.5 system-ui,sans-serif}
main{max-width:42rem;margin:0 auto;padding:24px 16px}
label.op{display:block;background:var(--card);border:1px solid var(--bd);border-radius:8px;padding:12px;margin:8px 0;cursor:pointer}
label.op:has(input:checked){border-color:var(--ac)}.rec{color:var(--ac);font-size:.85em;margin-left:.5em}
textarea{width:100%;box-sizing:border-box;min-height:5rem;background:var(--card);color:var(--fg);border:1px solid var(--bd);border-radius:8px;padding:8px}
button{margin:12px 8px 0 0;padding:8px 16px;border-radius:8px;border:1px solid var(--bd);background:var(--card);color:var(--fg);cursor:pointer}
button.ok{background:var(--ac);color:#fff;border-color:var(--ac)}#erro{color:#dc2626;min-height:1.5em}
</style></head><body><main>
<h1>__PERGUNTA__</h1>__DETALHE__
<form data-lavish-question="__ID__" id="f">__OPCOES__
<p><label for="nota">Note or free-text answer (optional)</label><textarea id="nota"></textarea></p>
<div id="erro"></div>
<button type="submit" class="ok">Send answer</button>
<button type="button" data-d="adiar">Decide later</button>
<button type="button" data-d="conversar">I want to talk</button>
</form></main><script>
const H=__HEADER__,F=document.getElementById("f");
function envia(disp){
  const r=F.querySelector("input[name=op]:checked"),nota=document.getElementById("nota").value.trim();
  let resposta="",d=disp;
  if(!disp){
    if(!r&&!nota){document.getElementById("erro").textContent="Pick an option or write the answer.";return}
    resposta=r?(nota?r.value+": "+nota:r.value):nota;
    d=r&&!nota?"escolha":"livre";
  }
  window.lavish.queuePrompt("Answer to decision "+H,{tag:"tracked-batch",text:"Decision "+H+": "+(resposta||d),element:F,data:{items:[{id:"perg-"+H,header:H,resposta:resposta,disposicao:d}]}});
  window.lavish.sendQueuedPrompts&&window.lavish.sendQueuedPrompts();
}
F.addEventListener("submit",e=>{e.preventDefault();envia("")});
F.querySelectorAll("button[data-d]").forEach(b=>b.addEventListener("click",()=>envia(b.dataset.d)));
</script></body></html>"""


def question_page(id_, question, options, recommended=1, detail=None):
    """HTML of the decision page: one option per radio (the recommended one marked in the text, never preselected), free field, defer and talk.

    Sends a `data.items` batch with id, header (the pending item's id), resposta and disposicao, the format that `orq lavish-answer_text` reads."""
    ops = "".join(f'<label class="op"><input type="radio" name="op" value="{html.escape(o, quote=True)}"> {html.escape(o)}'
                  + ('<span class="rec">recommended</span>' if n == recommended else "") + "</label>" for n, o in enumerate(options, 1))
    detail_entry = f"<p>{html.escape(detail)}</p>" if detail else ""
    return (_QUESTION_PAGE.replace("__TITULO__", html.escape(id_)).replace("__PERGUNTA__", html.escape(question)).replace("__DETALHE__", detail_entry)
            .replace("__ID__", html.escape(id_, quote=True)).replace("__OPCOES__", ops)
            .replace("__HEADER__", json.dumps(id_).replace("</", "<\\/")))


def ask(id_, question, options, recommended=1, detail=None, wait_min=None, poll=True):
    """User decision that works in both harnesses (Codex has no AskUserQuestion): builds the page, opens it in Orca's browser, waits for the
    answer with `lavish-axi poll` and records it as `orq lavish-answer_text`, which closes the pending item (only an explicit choice closes it).

    The pending item `id_` is born as a decision if it does not exist. Without an answer (timeout, session ended, empty answer) it stays open and the
    result carries the notice. With `poll=False` it only builds and opens; the poll output goes later to `orq lavish-answer_text`. Blocks until the answer:
    run it as a harness background job. Returns {"pagina", "url", "efeito", "avisos", ...}."""
    id_, question = (id_ or "").strip(), (question or "").strip()
    options = [o.strip() for o in options or [] if o.strip()]
    if not id_ or not question or len(options) < 2:
        raise ValueError("ask needs --id, --question and at least two --option")
    if not 1 <= recommended <= len(options):
        raise ValueError(f"--recommended goes from 1 to {len(options)}")
    current = {i.get("id"): i for i in _load_pending()["itens"]}.get(id_)
    if current is None:
        pending_add(id_, "decisao", question, detail=detail)
    elif current.get("tipo") != "decisao":
        raise ValueError(f"pending item {id_} is {current.get('tipo')}, not a decision")
    os.makedirs(_path("ask"), exist_ok=True)
    page_data = os.path.join(_path("ask"), f"{id_}.html")
    with open(page_data, "w") as f:
        f.write(question_page(id_, question, options, recommended, detail))
    open_result = subprocess.run([LAVISH, page_data], capture_output=True, text=True, timeout=30)
    url = (re.search(r'url:\s*"?(http\S+?)"?\s*$', open_result.stdout, re.M) or [None, None])[1]
    res = {"pagina": page_data, "url": url, "avisos": []}
    if url:
        try:
            orca("create", "--url", url, area="tab", timeout=10)
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            res["avisos"].append(f"could not open the tab in Orca ({e}); open {url}")
    else:
        res["avisos"].append(f"lavish-axi did not return the session url: {(open_result.stderr or open_result.stdout).strip()[:200]}")
    if away_enabled():  # the page stays open for when the user returns; nobody waits for the poll
        if url:
            _mutate_pending(lambda item_list: next(i for i in item_list if i.get("id") == id_).update(link=url))
        res["avisos"].append(f"away is on: pending item {id_} stays open until the user is back (orq away off lists the open ones)")
        return {**res, "efeito": "aberta"}
    if not poll:
        return {**res, "efeito": "aberta"}
    waiting = (wait_min or ASK_MIN) * 60
    output = os.path.join(_path("ask"), f"{id_}.poll")
    try:
        r = subprocess.run([LAVISH, "poll", page_data], capture_output=True, text=True, timeout=waiting)
        with open(output, "w") as f:
            f.write(r.stdout)
        out = lavish_answer(output)
    except subprocess.TimeoutExpired:
        res["avisos"].append(f"no answer in {waiting / 60:g} min: pending item {id_} stays open")
        return {**res, "efeito": "aberta"}
    except ValueError:  # session ended without sending: the poll brings no batch
        res["avisos"].append(f"the session ended without an answer: pending item {id_} stays open")
        return {**res, "efeito": "aberta"}
    effect = out["itens"][0]["efeito"] if out["itens"] else "aberta"
    return {**res, "efeito": effect, "lote": out["lote"], "avisos": res["avisos"] + out["avisos"]}


IDLE_MS = int(os.environ.get("ORQ_OCIOSO_MS") or 2000)  # how long to wait for a terminal's tui-idle before saying it is busy
NOTICE_GAP_S = float(os.environ.get("ORQ_AVISO_GAP_S") or 3)  # between reading the box and the one right before the send: whoever started typing in the meantime blocks the notice (ticket 82)
COORDINATOR_IDLE_MIN = float(os.environ.get("ORQ_COORD_OCIOSO_MIN") or 10)  # user prompt newer than this: the coordinator has someone at it and no notice is typed into it (ticket 82)
WAKE_IDLE_MIN = float(os.environ.get("ORQ_WAKE_OCIOSO_MIN") or 2)  # the same for the notice that wakes the coordinator (worker_done): short window, the delivery does not wait 10 min (ticket 86)
STEER_WAIT_S = float(os.environ.get("ORQ_STEER_ESPERA_S") or 2)  # Orca types its own notice into the busy worker: give it time before deciding nobody warned


def free_terminal(handle):
    """None if the terminal's agent ended the turn (tui-idle) and its box has no draft; otherwise the reason: `ocupado` or `draft`.

    Text typed into a Claude Code in the middle of a turn sits in the box (Enter does not submit, checked on 09/29), and on top of a
    user draft it would become a mixed prompt: in both cases the caller waits for the next round."""
    try:
        orca("wait", "--terminal", handle, "--for", "tui-idle", "--timeout-ms", str(IDLE_MS), area="terminal", timeout=IDLE_MS / 1000 + TIMEOUT_ORCA)
    except (RuntimeError, subprocess.TimeoutExpired):
        return "ocupado"
    try:
        draft = ((orca("read", "--terminal", handle, "--limit", "1", area="terminal").get("terminal") or {}).get("draft") or "").strip()
    except (RuntimeError, subprocess.TimeoutExpired):
        return None  # without reading the box: tui-idle already said typing is possible
    return "rascunho" if draft else None


NOTICE_MAX = 150  # ceiling on what `type_text` and `type_text_busy` send (firstmate #6240: the ~290-character notice did not reach the panel on every re-tap)


def _short(text_value):
    """The text to type, with at most NOTICE_MAX characters. What goes past the cap goes whole into `HOME/notices/<hash>.txt` (same text, same
    file) and what is typed carries the start of it and the full path: `<start>… full_text em <path>`."""
    if len(text_value) <= NOTICE_MAX:
        return text_value
    folder = os.path.join(HOME, "avisos")
    file_path = os.path.join(folder, hashlib.sha1(text_value.encode()).hexdigest()[:12] + ".txt")
    os.makedirs(folder, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(text_value + "\n")
    end = f"… full text at {file_path}"
    return text_value[: max(0, NOTICE_MAX - len(end))].rstrip() + end


def type_text(handle, text_value):
    """Types `text_value` + Enter into the terminal's agent, once. Returns `enviado` (sent), `ocupado`/`draft` (nothing was typed: repeat later) or
    `failed` (Orca refused: nothing was typed). The box is read twice, with NOTICE_GAP_S between them (ticket 82: a user who starts typing
    between the read and the send had the notice on top of their text). tui-idle alone misleads (satisfied at the start of the turn and for about 20 s of the turn, checked
    on the real Orca on 09/29): what really blocks is the send's `agent_prompt_blocked`, which Orca returns with the agent in the middle of a turn. If Orca observes the submission and did not see the turn start, it sends a lone Enter (on an empty box
    it does nothing); a send timeout counts as sent, because the text may have gone out and repeating would stack it."""
    text_value = _short(text_value)
    if reason := free_terminal(handle):
        return reason
    time.sleep(NOTICE_GAP_S)
    if reason := free_terminal(handle):
        return reason
    waiting = ("--wait-submit", "3")
    try:
        res = orca("send", "--terminal", handle, "--text", text_value, "--enter", *waiting, area="terminal", timeout=3 + TIMEOUT_ORCA)
    except subprocess.TimeoutExpired:
        return "enviado"
    except RuntimeError as e:
        return "ocupado" if "agent_prompt_blocked" in str(e) else "falhou"
    prompt = (res.get("send") or {}).get("prompt") or {}
    if prompt.get("observation") == "supported" and not set(prompt.get("stages") or []) - {"input_accepted"}:
        with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired):
            orca("send", "--terminal", handle, "--enter", *waiting, area="terminal", timeout=3 + TIMEOUT_ORCA)
    return "enviado"


STEER_NOTICE_MAX = 300  # characters of the adjustment that the notice typed into the busy worker carries


def _turn_screen_without_draft(handle):
    """True with the spinner (`esc to interrupt`, same in both agents) on the screen, no draft in the box and no menu waiting for a human answer."""
    try:
        t = orca("read", "--terminal", handle, "--screen", "--limit", str(SCREEN_LINES), area="terminal").get("terminal") or {}
    except (RuntimeError, subprocess.TimeoutExpired):
        return False
    tail = t.get("tail") or []
    return not ((t.get("draft") or "").strip() or any(screen_question(tail, h) for h in HARNESS) or "esc to interrupt" not in "\n".join(map(str, tail[-15:])))


def type_text_busy(handle, text_value):
    """Types `text_value` + Enter into an agent in the middle of a turn, for Claude Code to queue and inject into the next tool result (10/01).

    Only with the spinner on the screen, no draft in the box and no menu waiting for a human answer, in both reads (NOTICE_GAP_S between them, ticket 82):
    in all three cases it returns `ocupado` without typing. Returns `ocupado_digitado`, or `ocupado` if Orca blocked the send (agent_prompt_blocked) or it failed."""
    text_value = _short(text_value)
    if not _turn_screen_without_draft(handle):
        return "ocupado"
    time.sleep(NOTICE_GAP_S)
    if not _turn_screen_without_draft(handle):
        return "ocupado"
    try:
        orca("send", "--terminal", handle, "--text", text_value, "--enter", area="terminal", timeout=TIMEOUT_ORCA)
    except subprocess.TimeoutExpired:
        return "ocupado_digitado"  # the text may have gone out: repeating would stack
    except RuntimeError:
        return "ocupado"
    return "ocupado_digitado"


def active_coordinator(now_at=None, minutes_elapsed=None):
    """True if the user's last prompt is newer than `minutes_elapsed` (COORDINATOR_IDLE_MIN): the coordinator has someone there, and typing into it lands in the middle of what they are writing."""
    minutes_elapsed = COORDINATOR_IDLE_MIN if minutes_elapsed is None else minutes_elapsed
    now_at = now_at or datetime.now(timezone.utc)
    last_by_header = next((e["ts"] for e in reversed(read_events()) if e.get("tipo") == "entrada" and e.get("origem") == "usuario" and e.get("ts") and not e.get("grupo")), None)  # the mate's does not
    return bool(last_by_header) and (now_at - _dt(last_by_header)).total_seconds() < minutes_elapsed * 60


def _notify_mac(text_value):
    """Native macOS notification for the notice that was not typed; off by default, only with `"notificar_macos": true` in gerente.json (ticket 82)."""
    if not _manager_cfg().get("notificar_macos"):
        return
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        subprocess.run([os.environ.get("ORQ_OSASCRIPT") or "osascript", "-e", f"display notification {json.dumps(text_value, ensure_ascii=False)} with title \"orq\""],
                       capture_output=True, timeout=3, check=False)


def notify_coordinator(handle, text_value, context=True, minutes_elapsed=None):
    """Delivers a notice to the coordinator without typing over someone who is writing (tickets 82, 107 and 182). With away mode on it types when the coordinator is idle
    (the old-prompt guard and the `type_text` draft guard apply). With away mode off the old-prompt guard is dropped (ticket 182: deliveries sat waiting for the user's next message):
    it types when the coordinator is stopped and the prompt box is empty; with a draft or a turn in progress it holds. Returns `enviado` (typed), `adiado`
    (coordinator with someone at it, or held with away off: the notice waits in the cursor's `notices` queue, goes out in the next prompt's context, `context` False for what the summary already
    shows, and `deliver_notices` types it if the coordinator goes idle) or, with away on, the `type_text` reason (nothing went out: retry later). `adiado` already counts as delivered."""
    away = bool(_dict(_cursor_ro().get("ausente")))
    if not away or not active_coordinator(minutes_elapsed=minutes_elapsed):
        result = type_text(handle, text_value)
        if away or result == "enviado":
            return result
    _cursor_mut(lambda c: c.setdefault("avisos", []).append({"texto": text_value, "ts": now(), "contexto": context, **({"minutos": minutes_elapsed} if minutes_elapsed is not None else {})}))
    _notify_mac(text_value)
    return "adiado"


def deliver_notices():
    """One panel tick: types the oldest notice in the queue (one per tick; the next one finds the coordinator busy). With away on, only with the coordinator idle for more than
    COORDINATOR_IDLE_MIN (WAKE_IDLE_MIN for the wake-up notice); with away off (ticket 182), whenever it is stopped with an empty prompt box (`type_text` checks that). Returns the panel lines."""
    g, queue = _manager_cfg(), _cursor_ro().get("avisos")
    if not g.get("coordenador") or not isinstance(queue, list) or not queue:
        return []
    away = bool(_dict(_cursor_ro().get("ausente")))
    a = next((x for x in queue if not (away and active_coordinator(minutes_elapsed=x.get("minutos")))), None)  # the wake-up notice (2 min) does not wait behind a 10 one
    if not a or type_text(g["coordenador"], a["texto"]) != "enviado":
        return []
    _cursor_mut(lambda c: c.__setitem__("avisos", [x for x in c.get("avisos") or [] if x != a]))
    return ["deferred notice typed into the coordinator, idle"]


REMINDERS = "reminders.json"  # {seq, rems: [{id, what, due, made, status: open|fired|canceled, fired}]}: survives a manager or machine restart
REMIND_LATE_S = 120  # fired later than this after its time, the notice says by how much (the machine slept, the manager was down)
_REMIND_IN = re.compile(r"^(?:(\d+)h)?(?:(\d+)m?)?$")


def remind_due(in_=None, at=None, now_at=None):
    """The UTC stamp a reminder is due: `in_` is a delay (`90m`, `1h30`, `2h`, `45`), `at` a local `HH:MM` (today, or tomorrow when it has passed)."""
    now_at = now_at or _dt(now())
    if bool(in_) == bool(at):
        raise ValueError("pass exactly one of --in (90m, 1h30, 2h) or --at (HH:MM)")
    if in_:
        m = _REMIND_IN.match(in_.strip().lower())
        if not m or not any(m.groups()):
            raise ValueError(f"invalid --in: {in_!r} (use 90m, 1h30 or 2h)")
        minutes = int(m[1] or 0) * 60 + int(m[2] or 0)
        if minutes <= 0:
            raise ValueError("--in must be longer than zero")
        return (now_at + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    m = re.match(r"^(\d{1,2}):(\d{2})$", at.strip())
    if not m or int(m[1]) > 23 or int(m[2]) > 59:
        raise ValueError(f"invalid --at: {at!r} (use HH:MM, 24 h)")
    local = now_at.astimezone()
    target = local.replace(hour=int(m[1]), minute=int(m[2]), second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    return target.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _reminders_mut(fn):
    with _lock("reminders.lock"):
        d = _dict(_read_json(_path(REMINDERS)))
        d.setdefault("rems", [])
        out = fn(d)
        _write_json(_path(REMINDERS), d)
        return out


def remind_add(what, in_=None, at=None):
    """Creates a reminder; returns it."""
    if not (what or "").strip():
        raise ValueError("the reminder needs a text")
    due = remind_due(in_, at)

    def add(d):
        d["seq"] = int(d.get("seq") or 0) + 1
        item = {"id": f"l{d['seq']}", "what": what.strip(), "due": due, "made": now(), "status": "open"}
        d["rems"].append(item)
        return item
    item = _reminders_mut(add)
    append_event({"tipo": "lembrete", "op": "add", "lembrete": item["id"], "due": due})
    return item


def remind_cancel(id_):
    """Cancels an open reminder; raises ValueError when it does not exist or is no longer open."""
    def cancel(d):
        item = next((i for i in d["rems"] if i.get("id") == id_), None)
        if not item or item.get("status") != "open":
            raise ValueError(f"no open reminder {id_}")
        item["status"] = "canceled"
        return item
    item = _reminders_mut(cancel)
    append_event({"tipo": "lembrete", "op": "cancel", "lembrete": id_})
    return item


def remind_list(all_listing=False):
    """One line per reminder, soonest first; `all_listing` also shows the fired and canceled ones."""
    items = _dict(_read_json(_path(REMINDERS))).get("rems") or []
    return [f"{i['id']}  {_hora_local(i['due'])} ({i['due']})  {i['status']}  {i['what']}" for i in sorted(items, key=lambda x: x["due"]) if all_listing or i.get("status") == "open"]


def remind_round(now_at=None):
    """One manager tick: fires each open reminder whose time has come. Marks it fired before notifying, so it goes out once even if the notice fails. The macOS
    notification (with sound) always goes; the short notice typed into the coordinator only with away on (ticket 107). A late one says by how much."""
    now_at = now_at or _dt(now())

    def fire(d):
        done = [i for i in d["rems"] if i.get("status") == "open" and _dt(i["due"]) <= now_at]
        for i in done:
            i.update(status="fired", fired=now())
        return done
    if not any(i.get("status") == "open" and _dt(i["due"]) <= now_at for i in _dict(_read_json(_path(REMINDERS))).get("rems") or []):
        return []
    lines, g = [], _manager_cfg()
    for i in _reminders_mut(fire):
        late = int((now_at - _dt(i["due"])).total_seconds())
        text_value = f"Reminder: {i['what']}" + (f" (late by {late // 60} min)" if late > REMIND_LATE_S else "")
        fail_safe._notify(text_value, sound="Glass")
        append_event({"tipo": "lembrete", "op": "fired", "lembrete": i["id"], "atraso_s": late})
        if _dict(_cursor_ro().get("ausente")) and g.get("coordenador"):
            notify_coordinator(g["coordenador"], f"orq: {text_value}.")
        lines.append(f"{text_value} [{i['id']}]")
    return lines


WAKE_STOPPED_MIN = float(os.environ.get("ORQ_ACORDA_PARADO_MIN") or 5)  # work without the user has been going on this long and the coordinator is idle: the manager wakes it
WAKE_REPEAT_MIN = float(os.environ.get("ORQ_ACORDA_REPETE_MIN") or 30)  # the same reason is not typed again before this
WAKE_FILE = "acorda-parado.json"  # {motivo, desde, avisado}: the current reason, since when the manager sees it and when it typed it


def wake_stopped(now_at=None):
    """One manager tick: the same conditions as the away Stop (`next_without_user` and the old open obligation) hold for WAKE_STOPPED_MIN and the coordinator
    is stopped at the prompt → types a short notice into it, once per reason every WAKE_REPEAT_MIN. The Stop only runs when the coordinator finishes a turn; without a
    new message there is no turn (ticket 174: the integrator cycle, a service worker without capability, went 5 h with nobody seeing it). Only with away on (ticket 107).
    Coordinator busy or with a draft: nothing is marked and the next tick tries again. A reason that goes away resets the count. Returns the panel lines."""
    g, now_at = _manager_cfg(), now_at or datetime.now(timezone.utc)
    if not g or not g.get("coordenador") or not away_enabled():
        return []
    events = read_events()
    reason, _ = _work_without_user(events, now_at)
    if not reason and (old_entries := obligations_to_chase(events, now_at)):
        reason = f"open obligation: {old_entries[0]['entrada']} {old_entries[0]['chave']} ({old_entries[0]['texto']})"
    file_path, ts = _path(WAKE_FILE), now_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    check_state = _dict(_read_json(file_path))
    if not reason:
        if check_state:
            _write_json(file_path, {})
        return []
    if check_state.get("motivo") != reason:
        check_state = {"motivo": reason, "desde": ts}
        _write_json(file_path, check_state)
    minutes_elapsed = (now_at - _dt(check_state["desde"])).total_seconds() / 60
    notified = _ts(check_state.get("avisado"))
    if minutes_elapsed < WAKE_STOPPED_MIN or (notified and (now_at - notified).total_seconds() < WAKE_REPEAT_MIN * 60):
        return []
    if notify_coordinator(g["coordenador"], f"orq: coordinator stopped for {int(minutes_elapsed)} min with work that does not depend on the user: {reason}") not in ("enviado", "adiado"):
        return []
    _write_json(file_path, {**check_state, "avisado": ts})
    return [f"coordinator stopped with work that does not need the user: notice typed ({_quote(reason, 80)})"]


def context_notices():
    """Empties the `notices` queue and returns the user-prompt context line with the notices that were not typed (empty if there are none)."""
    taken = []

    def pega(c):
        taken.extend(c.pop("avisos", None) or [])
    if _cursor_ro().get("avisos"):
        _cursor_mut(pega)
    texts = [re.sub(r"^orq: ", "", a["texto"]) for a in taken if isinstance(a, dict) and a.get("contexto") and a.get("texto")]
    return "[orq] Notices that were not typed (you were writing): " + " | ".join(texts) if texts else ""


def _adjustment_notice(handle, adjustment):
    """The Orca notice with the adjustment summary, on a single line (a line break would submit the text too early)."""
    return f"{_worker_notice(handle)} Coordinator adjustment: {_quote(adjustment, STEER_NOTICE_MAX)}"


def _dispatch_terminal(run, dispatch):
    """agentTerminalHandle of the dispatch in the Run's worker-list, or None (no row, or Orca failed)."""
    try:
        return next((w.get("agentTerminalHandle") for w in _all_workers(run) if w.get("dispatchId") == dispatch), None)
    except (RuntimeError, KeyError):
        return None


def _worker_notice(handle):
    """The notice in Orca's format, typed into the terminal of a worker that Orca did not notify."""
    return f"You have 1 orchestration message. Run `orca orchestration check --terminal {handle}`."


def read_in_transcript(dispatch, msg_id):
    """True if the worker's session transcript cites the message (its `check` brought it), False if not, None with no transcript.

    The inbox `read` only becomes 1 with `check --ack`, and a worker that reads via `check --terminal` without ack leaves it at 0 (confirmed on the real Orca on 29/09:
    the worker read it, replied "recebi" (got it) and the row stayed at read 0). Orca does not expose the open delivery (`deliveries` table), so the reading
    is proved by the id at the end of the transcript, found through the session that the prompt hook recorded in turnos.json."""
    t = _dict(_turns_ro().get(dispatch))
    sid = t.get("sessao")
    files = [t["transcrito"]] if t.get("transcrito") else glob.glob(os.path.join(PROJECTS, "*", f"{glob.escape(sid)}.jsonl")) if sid else []
    for file_path in files:
        try:
            with open(file_path, "rb") as f:
                f.seek(max(0, f.seek(0, 2) - STEER_TRANSCRIPT_BYTES))
                return msg_id.encode() in f.read()
        except OSError as e:
            log(f"transcrito {file_path}: {type(e).__name__}: {e}")
    return None


def _orca_notified(msg_id):
    """The message has `delivered_at` in the inbox: Orca typed its notice into the worker's terminal (confirmed on 29/09: the manager's reply to a
    worker stuck in `ask` and the steer that Orca did not notify are left without it)."""
    try:
        return bool(msg_id) and any(m.get("id") == msg_id and m.get("delivered_at") for m in orca("inbox", "--limit", "20")["messages"] if isinstance(m, dict))
    except (RuntimeError, subprocess.TimeoutExpired):
        return False


def _turn_ended_less_than_tolerance_ago(agent_row, s, now_at):
    """True if the worker's turn ended after the steer (or after the last retype) and less than STEER_READ_S has passed: it is not yet a lack of reading."""
    end = _ts(agent_row.get("turno_fim"))
    return bool(end and end > s["ultima"] and (now_at - end).total_seconds() < STEER_READ_S)


def redeliver_steers(now_at=None):
    """One manager tick over the open steers: the panel lines.

    Only touches Orca with an overdue steer (STEER_READ_S after sending or after the last retype, and after the end of the worker's turn if it ended after that). A message with `read` in the inbox or cited in the
    worker's transcript (read_in_transcript): `steer_fim` (read).
    A dispatch that already delivered: `steer_fim` (closed). Worker `stopped` (stopped; turn ended, per the hooks): retypes the notice with `type_text`, which does not type
    over a turn in progress or a draft and so does not spend the attempt. After STEER_ATTEMPTS retypes without a read it records the
    alert `steer_nao_lido` (summary and `orq agents`) and the steer leaves tracking. A busy worker gets nothing, as with the steer; a worker `perguntando` (asking; open question) neither: no notice and no alert."""
    now_at = now_at or datetime.now(timezone.utc)
    overdue = {m: s for m, s in open_steers(read_events(), now_at).items() if (now_at - s["ultima"]).total_seconds() >= STEER_READ_S}
    if not overdue:
        return []
    msgs = {m.get("id"): m for m in orca("inbox", "--limit", "200", timeout=20)["messages"] if isinstance(m, dict)}
    line_list, agent_rows = [], None
    for m, s in overdue.items():
        st, line = s["steer"], msgs.get(m)
        base = {"msg_id": m, "task": st.get("task"), "dispatch": st.get("dispatch"), "run": st.get("run")}
        if line is None:
            continue  # the inbox no longer shows the message: no proof of reading nor of its absence
        source = "orca" if line.get("read") else "transcrito" if read_in_transcript(st.get("dispatch"), m) else None
        if source:
            append_event({"tipo": "steer_fim", **base, "motivo": "lido", "fonte": source})
            continue
        agent_rows = agent_rows if agent_rows is not None else {a["dispatch"]: a for a in agents()}
        agent_row = agent_rows.get(st.get("dispatch"))
        if not agent_row or agent_row["estado"] in ("entregue", "liberado"):
            append_event({"tipo": "steer_fim", **base, "motivo": "encerrado"})
        elif agent_row["estado"] == "perguntando":
            continue  # the worker is waiting for the coordinator's reply: retyping or alerting only stacks noise, the steer stays open until the worker returns
        elif _turn_ended_less_than_tolerance_ago(agent_row, s, now_at):
            continue  # the worker read (or is reading) at the end of the turn: STEER_READ_S runs from its end, not from the send (lesson #6126)
        elif s["tentativas"] >= STEER_ATTEMPTS:
            append_event({"tipo": "alerta", "alerta": "steer_nao_lido", **base})
            line_list.append(f"{st.get('task')}: steer not read after {s['tentativas']} notices (alert recorded)")
        elif agent_row["estado"] == "parado" and type_text(agent_row["terminal"], _worker_notice(agent_row["terminal"])) == "enviado":
            append_event({"tipo": "steer_reentrega", **base, "tentativa": s["tentativas"] + 1})
            line_list.append(f"{st.get('task')}: steer not read, notice retyped ({s['tentativas'] + 1}/{STEER_ATTEMPTS})")
    return line_list


def reply_to(msg_id, text_value):
    """Replies to a worker's message (`orca orchestration reply`) through the manager's handle, first binding the message's Run.

    Orca only lets you reply from the terminal bound to the message's Run (consumer_fenced with the manager on another Run, seen on 29/09): the run comes from the inbox
    row and `orca()` binds the manager to it. A Run that neither the manager nor the coordinator holds is refused with the missing `run-use`."""
    line = next((x for x in orca("inbox", "--limit", "200", timeout=20)["messages"] if isinstance(x, dict) and x.get("id") == msg_id), None)
    if not line:
        raise ValueError(f"message {msg_id} is not among the 200 newest in the inbox")
    target = line.get("run_id")
    fenced = f"the coordinator must command the message's Run: {bind_tip(target)}"
    if not coordinator_run(target):
        raise ValueError(f"the message belongs to Run {target}; {fenced}")
    try:
        res = orca("reply", "--id", msg_id, "--body", text_value, "--run", target, timeout=10)
    except RuntimeError as e:
        raise ValueError(fenced if "consumer_fenced" in str(e) else str(e))
    dispatch = _msg_dispatch(line)
    woken = wake(dispatch, f"coordinator's answer to your message {msg_id}: {text_value}") if dispatch in _hibernated() else None  # the hibernated worker would not read the reply in the inbox
    return append_event({"tipo": "resposta_worker", "msg_id": msg_id, "run": target, "dispatch": dispatch, "texto": text_value,
                         "resposta_id": (res.get("message") or res).get("id"), **({"acordado": woken["estado"]} if woken else {})})


REQUEST_TITLE = "## User request"
# Ticket 117: each turn resends the whole context, so waiting with sleep + check burns cost for nothing. Goes at the end of every spec and every ticket task.
WAITING_BLOCK = """## Waiting

- After asking (`ask`) or escalating, end the turn: the answer arrives as a message and opens the next turn.
- External waits (CI, PR, merge) go inside a single blocking command, with the tool's maximum timeout (600000 ms in Bash): `gh pr checks --watch`, or `until <check>; do sleep 30; done`.
- If the command returns with no change, repeat the same command, with no check between runs.
- Never background it to poll. Do not wrap `npm run test-app-e2e` or `scripts/e2e-infra.sh` in a loop of your own: the E2E queue already waits inside the command."""

ORQ_WT_TITLE = "## orq worktree"  # the block that the dispatch of one of orq's own tickets appends to the spec (ticket 136)


def _entry_text(entry):
    """Literal text of the entry in events.jsonl (the hook records up to 2000 characters), or None without `entry`. ValueError if it does not exist."""
    if not entry:
        return None
    x = next((x for x in read_events() if x.get("id") == entry and x.get("tipo") == "entrada"), None)
    if not x:
        raise ValueError(f"entry {entry} does not exist")
    return x.get("texto") or ""


def steer(task, text_value, run=None, entry=None):
    """Sends `text_value` to the task's dispatch (send --to dispatch:<id>) and records it.

    Refuses a task that is not dispatched, a nonexistent entry and a Run that is not the one bound to the coordinator (the send would give consumer_fenced).
    """
    request = _entry_text(entry)
    target = default_run(run)
    if not target:
        raise ValueError("no Run bound: pass --run and run run-use --id <r>")
    with _no_run(target):
        return _steer(task, text_value, target, entry, request)


def _steer(task, text_value, target, entry, request):
    """The steer body, with the `target` Run already commanded by the coordinator (or with the refusal that explains what is missing)."""
    fenced = f"the coordinator must command the worker's Run: {bind_tip(target)}"
    if not coordinator_run(target):
        raise ValueError(f"the task is in {target}; {fenced}")
    t = next((t for t in orca("task-list", "--run", target, timeout=20)["tasks"] if t["id"] == task), None)
    if not t:
        raise ValueError(f"task {task} does not exist in Run {target}")
    body_text = text_value if request is None else f"{text_value}\n\n{REQUEST_TITLE} (addition)\n{request}"
    if t.get("dispatch_id") in _hibernated():  # no terminal to receive it: the resume carries the adjustment (even for an already delivered task, which the hibernated worker can still follow)
        r = wake(t["dispatch_id"], f"coordinator adjustment: {body_text}")
        if r["estado"] == "falhou":
            raise ValueError(f"the worker is hibernated and did not wake: {r['aviso']}")
        ev = append_event({"tipo": "steer", "task": task, "dispatch": t["dispatch_id"], "run": target, "texto": text_value, "acordado": r["estado"],
                           **({"pedido": request} if request is not None else {})})
        if entry:
            intake(entry, "steer", task, run=target)
        return ev
    if t.get("status") == "completed" and t.get("dispatch_id"):
        raise ValueError(f"task {task} is completed: to make the worker redo the delivery use `orq send-back {task} \"<reason>\"`")
    if t.get("status") != "dispatched" or not t.get("dispatch_id"):
        raise ValueError(f"task {task} is {t.get('status')}, not dispatched: no worker to receive the adjustment")
    try:
        res = orca("send", "--run", target, "--to", f"dispatch:{t['dispatch_id']}", "--subject", "Adjustment",
                   "--body", body_text, "--priority", "high", timeout=10)
    except RuntimeError as e:
        raise ValueError(fenced if "consumer_fenced" in str(e) else str(e))
    msg = res.get("message") or res
    # Orca types its own notice (the line gets `delivered_at`) into the worker it reaches, and into one that ended its turn idle at the prompt sometimes it does not
    # (seen on 09/29): only that one gets the notice from here, and a notice Orca just typed is not repeated.
    time.sleep(STEER_WAIT_S)
    handle = _dispatch_terminal(target, t["dispatch_id"])
    delivery = "orca" if _orca_notified(msg.get("id")) else type_text(handle, _worker_notice(handle)) if handle else "sem_terminal"
    if delivery == "ocupado":
        delivery = type_text_busy(handle, _adjustment_notice(handle, text_value))
        if delivery == "ocupado_digitado":
            append_event({"tipo": "steer_digitado_ocupado", "task": task, "dispatch": t["dispatch_id"], "run": target, "msg_id": msg.get("id")})
    ev = append_event({"tipo": "steer", "task": task, "dispatch": t["dispatch_id"], "run": target, "texto": text_value, "msg_id": msg.get("id"),
                       **({"pedido": request} if request is not None else {}), **({"aviso_terminal": delivery} if delivery != "ocupado" else {})})
    if entry:
        intake(entry, "steer", task, run=target)
    return ev


def send_back(target, reason, run=None):
    """Gives the delivery of a completed task back to the worker with the correction `reason`: types it into its terminal (or resumes the session if the terminal is gone, or wakes the
    hibernated one), records `send_back` (the delivery leaves the away Stop and the "entregues sem liberar" (delivered, not released) until the new worker_done) and returns the task to `dispatched`.
    ValueError if `target` (task or dispatch) does not exist in the Run or the worker has no way to receive it."""
    run_ = default_run(run)
    if not run_:
        raise ValueError("no Run bound: pass --run and run run-use --id <r>")
    with _no_run(run_):
        t = next((t for t in orca("task-list", "--run", run_, timeout=20)["tasks"] if target in (t["id"], t.get("dispatch_id")) and t.get("dispatch_id")), None)
        if not t:  # task-list zeroes the dispatch_id of the completed task (ticket created with the backlog on); worker-list, which `agents` reads, still links task and dispatch
            t = next(({"id": w["taskId"], "dispatch_id": w["dispatchId"]} for w in _all_workers(run_) if target in (w.get("taskId"), w.get("dispatchId")) and w.get("dispatchId")), None)
        if not t:
            raise ValueError(f"{target} is neither a task nor a dispatch of Run {run_}")
        d, body_text = t["dispatch_id"], f"The delivery was sent back by the coordinator; redo it and send a new worker_done. Reason: {reason}"
        if d in _hibernated():
            r = wake(d, body_text)
            if r["estado"] == "falhou":
                raise ValueError(f"the worker is hibernated and did not wake: {r['aviso']}")
            via = "acordado"
        else:
            handle = _dispatch_terminal(run_, d)
            if handle and not _dead(handle):
                try:
                    orca("send", "--run", run_, "--to", f"dispatch:{d}", "--subject", "Delivery sent back", "--body", body_text, "--priority", "high", timeout=10)
                except RuntimeError as e:
                    raise ValueError(str(e))
                via = type_text(handle, _adjustment_notice(handle, reason)) if _dispatch_terminal(run_, d) else "sem_terminal"
            else:
                tn = _dict(_turns_ro().get(d))
                cwd = tn.get("cwd") or _checkpoint(d).get("caminho")
                if not (tn.get("sessao") and cwd and os.path.isdir(cwd)):
                    raise ValueError(f"the terminal of {d} is gone and there is no recorded session or worktree to resume: `orq relaunch {d} --note '<reason>'`")
                try:
                    cp = _checkpoint(d)
                except (RuntimeError, subprocess.TimeoutExpired):
                    cp = {"head": None, "sujo": None}
                line = {"dispatch": d, "task": t["id"], "run": run_, "titulo": d, "cwd": cwd, "terminal": handle}
                r = _start_session(line, tn["sessao"], tn.get("modelo"), cp, f"start another worker with: orq relaunch {d} --note 'the session could not be resumed'", body_text,
                                  tn.get("harness") or "claude", None)
                if r["estado"] == "falhou":
                    raise ValueError(r["aviso"])
                via = "retomado"
        notice = None
        try:
            orca("task-update", "--id", t["id"], "--status", "dispatched", "--run", run_, timeout=20)
        except RuntimeError as e:
            notice = f"task {t['id']} remains {t.get('status')} ({e}): orca orchestration task-update --id {t['id']} --status dispatched"
        return append_event({"tipo": "devolver", "task": t["id"], "dispatch": d, "run": run_, "texto": reason, "via": via, **({"aviso": notice} if notice else {})})


# ---------- delivery conformance, plan phases and the scratch tracker (ticket 201) ----------
# The 02/10 incident (#2039): a "phase 1 integrated" went out without four tickets of the plan, and nothing proved item by item what each ticket asked for.

CONFORMANCE_TITLE = "## Delivery conformance"
REAL_ENTRY = "[real entry]"
_CHECKBOX = re.compile(r"^[ \t]*- \[[ xX]\][ \t]+(.+?)[ \t]*$", re.M)
_PRODUCTION = re.compile(r"\b(?:startup|boot|cron(?:job|tab)?|roda a cada|runs every|(?:a cada|every) \d+\s*(?:s|ms|min|h))\b", re.I)
_PROOF = re.compile(r"`[^`\n]+`")
_SCRATCH_TICKET = re.compile(r"(?:~?/[^\s`'\"()]*?)?\.scratch/[\w.-]+/issues/\d{2,}-[\w.-]+\.md")
_ISSUE_TICKET = re.compile(r"~?/[^\s`'\"()]+/issues/\d{2,}-[\w.-]+\.md")
_SCRATCH_DONE = ("resolved", "done", "closed", "wontfix")
_PHASE = re.compile(r"\b(?:fase|phase)\s+(\d+)\b", re.I)
_ISSUE_REF = re.compile(r"#(\d{3,})\b")
_PLAN_REF = re.compile(r"(?:~?/[^\s`'\"()]*?)?\.scratch/[\w.-]+")
# "#2039 fase 1, ticket 02: ..." (a product dispatch) or "#2039 fase 1: 04: ..." (an orq ticket): the scratch ticket a title names, before the `scratch` link existed
_TITLE_TICKET = re.compile(r"#(\d{3,})\b.*?\b(?:ticket\s+|(?:fase|phase)\s+\d+\s*[:,]\s*)(\d{2,})\b", re.I)
_INTEGRATES = re.compile(r"\bintegr\w*\b[^()]*\(([\d,\se]+)\)", re.I)  # "integrar a fase 1 (02, 05, 08 e 03)": the tickets an integration dispatch carries


def spec_items(txt):
    """What a delivery must prove, in order: each `- [ ]` item, then each top-level bullet of `## Acceptance criteria`."""
    found = [m.group(1) for m in _CHECKBOX.finditer(txt or "")]
    if m := _ACCEPTANCE.search(txt or ""):
        section = re.split(r"^#{1,6} ", txt[m.end():], maxsplit=1, flags=re.M)[0]
        found += [ln[2:].strip() for ln in section.splitlines() if ln.startswith("- ") and not _CHECKBOX.match(ln)]
    return list(dict.fromkeys(i for i in found if i))


def production_behavior(txt):
    """Does the text describe something that runs on its own in production (at startup, every N s, a cron)? Then a unit test of the function does not prove it."""
    return bool(_PRODUCTION.search(txt or ""))


def conformance_block(items, real):
    """The block the dispatch appends to the spec: the numbered items and how orq checks them when the worker_done arrives."""
    real_line = (f"\nThis ticket describes production behavior (something that starts, runs every N s or at startup): at least one test goes through the real entry point "
                 f"(the server boot or the E2E), not only the isolated function, and its line carries `{REAL_ENTRY}`.\n") if real else ""
    return (f"{CONFORMANCE_TITLE}\n\nYour final report (`{FINAL_REPORT}` in the worktree root, or the file of `--report-path`) carries a `## Conformance` section: one line per item "
            "below, with the same number, and the proof in backticks (the red→green test name, `file:line`, or the command and its output), e.g. "
            "`1. red→green \\`test_x\\` (test_orq.py:120)`. When the worker_done arrives, orq checks it: a missing line sends the delivery back to you with the list, "
            f"and it enters neither the integrator queue nor a PR.\n{real_line}\n" + "\n".join(f"{n}. {_quote(i, 200)}" for n, i in enumerate(items, 1)) + "\n")


def conformance_missing(items, real, text_value):
    """The items with no `## Conformance` line carrying a proof in backticks (`N. ...`), plus the real entry point when the ticket asks for it. Empty: the delivery is complete."""
    m = re.search(r"^#{1,3}[ \t]*(?:Conformance|Conformidade)[ \t]*$", text_value or "", re.M | re.I)
    section = re.split(r"^#{1,3} ", text_value[m.end():], maxsplit=1, flags=re.M)[0] if m else ""
    lines = {}
    for ln in section.splitlines():
        mm = re.match(r"^[ \t]*(?:[-*][ \t]*)?(?:\[[ xX]\][ \t]*)?(\d+)[.):][ \t]*(.*)$", ln)
        if mm and _PROOF.search(mm.group(2)):
            lines[int(mm.group(1))] = mm.group(2)
    missing = [f"{n}. {_quote(i, 100)}" for n, i in enumerate(items, 1) if n not in lines]
    if real and not any(REAL_ENTRY in v.lower() for v in lines.values()):
        missing.append(f"{REAL_ENTRY}: no line proves the behavior through the real entry point (server boot or E2E)")
    return missing


def cited_tickets(text_value, roots=()):
    """The ticket files the text cites that exist: an `issues/NN-*.md` by absolute path, or a `.scratch/<feature>/issues/NN-*.md` relative to one of `roots`."""
    out = []
    for p in [*_ISSUE_TICKET.findall(text_value or ""), *_SCRATCH_TICKET.findall(text_value or "")]:
        p = os.path.expanduser(p)
        for c in [p] if os.path.isabs(p) else [os.path.join(r, p) for r in roots if r]:
            if os.path.isfile(c):
                out.append(os.path.realpath(c))
                break
    return list(dict.fromkeys(out))


def _is_scratch(path):
    return "/.scratch/" in path and os.path.basename(os.path.dirname(path)) == "issues"


def _text_of(path):
    try:
        return open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return ""


def dispatch_conformance(spec, title, roots=(), ticket_file=None):
    """(items, real entry?, scratch tickets) of a dispatch. The items come from the spec, the ticket being executed (the `--ticket` file, or the only `issues/NN-*.md` the
    spec cites: `Leia e execute o ticket <path>`) and the scratch ticket it was born from: the only `.scratch/<feature>/issues/NN-*.md` those cite, when the title names its
    number. A citation inside a cited file is not followed, and a ticket cited for context (a second one, a repro) brings neither items nor a link."""
    own = [os.path.realpath(ticket_file)] if ticket_file and os.path.isfile(ticket_file) else []
    if not own and len(issues := [f for f in cited_tickets(spec, roots) if not _is_scratch(f)]) == 1:
        own = issues
    texts = [spec or "", *map(_text_of, own)]
    scratch = list(dict.fromkeys(f for t in texts for f in cited_tickets(t, roots) if _is_scratch(f)))
    num = scratch[0] and _FILE_NUM.match(os.path.basename(scratch[0])).group(1).lstrip("0") if len(scratch) == 1 else None
    scratch = scratch if num and re.search(rf"(?<!\d)0*{num}(?!\d)", title or "") else []
    texts += map(_text_of, scratch)
    items = list(dict.fromkeys(i for t in texts for i in spec_items(t)))
    return items, production_behavior(" ".join([title or "", *items])), scratch


def _delivery_text(m, p, disp):
    """What the worker wrote for the delivery: subject and body of the worker_done, the `--report-path` file and the final report of the dispatch's worktrees
    (the turn's cwd, Orca's worktree and, for an orq ticket, `<ORQ_WT>/<ticket>`). A final report older than the dispatch belongs to another delivery and is left out."""
    d, parts = p.get("dispatchId"), [m.get("subject") or "", m.get("body") or "", _text_of(os.path.expanduser(p.get("reportPath") or ""))]
    folders = [_dict(_turns_ro().get(d)).get("cwd"), _dispatch_worktree(m.get("run_id"), d), disp.get("ticket") and os.path.join(WT_ROOT, disp["ticket"])]
    since = _ts(disp.get("ts"))
    for f in dict.fromkeys(worker_file(x, FINAL_REPORT) for x in folders if x):
        with contextlib.suppress(OSError):
            if not since or datetime.fromtimestamp(os.path.getmtime(f), timezone.utc) >= since:
                parts.append(_text_of(f))
    return "\n".join(parts)


CONFORMANCE_SEND_BACKS = 2  # after this many send-backs of the same dispatch the next incomplete delivery goes to the coordinator: a false negative does not loop


def _delivery_conformance(m, p, send=True):
    """worker_done `succeeded` of a dispatch that recorded its items -> `conformidade` event; with something missing, `send_back` with the list (up to
    CONFORMANCE_SEND_BACKS times per dispatch, then an alert). True when the delivery may go on (to the integrator queue, to a PR); a dispatch with no items is not checked.

    `send=False` (the prompt hook's `orq inbox --ack`) records the verdict without sending back, which can resume a session: the manager's ingest sends it afterwards."""
    d = p.get("dispatchId")
    if p.get("outcome") != "succeeded" or not d:
        return True
    events = read_events()
    verdicts = [e for e in events if e.get("tipo") == "conformidade" and e.get("dispatch") == d]
    prev = next((e for e in reversed(verdicts) if e.get("msg") == m["id"]), None)
    if prev and (prev.get("ok", True) or prev.get("enviado") or not send):
        return prev.get("ok", True)
    disp = next((e for e in reversed(events) if e.get("tipo") == "despacho" and e.get("dispatch") == d), None)
    if not disp or not disp.get("conformidade"):
        return True
    missing = prev["faltando"] if prev else conformance_missing(disp["conformidade"], disp.get("entrada_real"), _delivery_text(m, p, disp))
    ev = {"tipo": "conformidade", "msg": m["id"], "dispatch": d, "task": p.get("taskId"), "run": m.get("run_id"), "ok": not missing, "faltando": missing}
    sent = len({e.get("msg") for e in verdicts if e.get("enviado")})
    if missing and send and sent >= CONFORMANCE_SEND_BACKS:
        ev["aviso"] = f"sent back {sent} times already: it stays out of the queue for the coordinator (orq send-back {d}, or orq integrate queue add)"
        append_event({"tipo": "alerta", "alerta": "conformidade_repetida", "dispatch": d, "task": p.get("taskId"), "run": m.get("run_id"), "msg": m["id"], "faltando": missing})
    elif missing and send:
        try:
            send_back(d, "the final report has no `## Conformance` line with a proof for: " + "; ".join(missing), m.get("run_id"))
            ev["enviado"] = True
        except Exception as e:  # noqa: BLE001 - the delivery stays out of the queue anyway; the coordinator sends it back by hand
            ev["aviso"] = f"send-back failed ({type(e).__name__}: {e}): orq send-back {d} \"<the missing list>\""
            log(f"conformance: {d}: {ev['aviso']}")
    append_event(ev)
    return not missing


def _md_field(txt, item_name):
    """`Nome: value` or `**Nome:** value` of a ticket's text, or None."""
    m = re.search(rf"^\**{re.escape(item_name)}(?::\**|\**:)[ \t]*(.*?)[ \t]*$", txt, re.M | re.I)
    return m.group(1).strip() if m else None


def scratch_tickets(plan):
    """The tickets of a `.scratch/<feature>/` plan: [{num, arquivo, fase (int or None), status}], by number. `Fase: 1, roteador` is phase 1; `avulso` has none."""
    out = []
    for f in sorted(glob.glob(os.path.join(plan, "issues", "*.md"))):
        if not (m := _FILE_NUM.match(os.path.basename(f))):
            continue
        with contextlib.suppress(OSError):
            txt = open(f, encoding="utf-8").read()
            fase = re.match(r"\d+", _md_field(txt, "Fase") or _md_field(txt, "Phase") or "")
            out.append({"num": m.group(1), "arquivo": os.path.realpath(f), "fase": int(fase.group(0)) if fase else None, "status": (_md_field(txt, "Status") or "?").lower()})
    return sorted(out, key=lambda t: int(t["num"]))


def set_scratch_status(path, status):
    """Rewrites the `**Status:**` line of a scratch ticket (or adds it after the title). The file is the scratch tracker's truth."""
    with open(path, encoding="utf-8") as f:
        txt = f.read()
    new, n = re.subn(r"^(\**Status(?::\**|\**:)[ \t]*).*$", lambda mm: mm.group(1) + status, txt, count=1, flags=re.M | re.I)
    if not n:
        head, _, rest = txt.partition("\n")
        new = f"{head}\n\n**Status:** {status}\n{rest}"
    _write(path, new)


def scratch_roots(extra=()):
    """Where the `.scratch/` plans live: the given folders, their main checkouts (`.scratch` is gitignored and stays in the main one) and each project's `path:` repo."""
    found = [*extra, *(_repo_root(x) for x in extra if x)]
    found += [os.path.realpath(os.path.expanduser(p["repo"][5:])) for p in projects().values() if str(p.get("repo") or "").startswith("path:")]
    return [r for r in dict.fromkeys(found) if r and os.path.isdir(r)]


def _issue_tokens(plan):
    """The issue numbers a plan folder's name carries: whole tokens of 3+ digits (`failover-2039-x` -> 2039), never a date (`erros-2026-09-28`)."""
    return set(re.findall(r"\d{3,}", re.sub(r"\d{4}-\d{2}-\d{2}", "", os.path.basename(plan)))) & set(re.split(r"[-_.]", os.path.basename(plan)))


def cited_plans(text_value, roots):
    """The plans (`.scratch/<feature>/` folders with `issues/`) the text points to: a cited `.scratch/<feature>` path, a `Plano:` file, or the issue (`#2039`) in the folder's name."""
    cands = [os.path.expanduser(p) for p in _PLAN_REF.findall(text_value or "")]
    if plano := _md_field(text_value or "", "Plano") or _md_field(text_value or "", "Plan"):
        cands.append(os.path.dirname(os.path.expanduser(plano.strip("`"))))
    out = [os.path.realpath(c if os.path.isabs(c) else os.path.join(r, c)) for c in cands for r in ([None] if os.path.isabs(c) else roots)]
    issues = set(_ISSUE_REF.findall(text_value or ""))
    out += [d for r in roots for d in sorted(glob.glob(os.path.join(r, ".scratch", "*"))) if issues & _issue_tokens(d)]
    return [d for d in dict.fromkeys(out) if os.path.isdir(os.path.join(d, "issues"))]


def scratch_done(plan, events=None):
    """The scratch tickets of `plan` already delivered, from the file and the log only (so it holds for any cut of the log): Status resolved in the file; an orq ticket
    linked to it (`scratch:` or the title) closed in the log; a dispatch linked to it (`scratch` in the event or the title) with a `succeeded` worker_done or a delivery;
    or a delivered dispatch whose title integrates it by number (`#2039: integrar a fase 1 (02, 05, 08)`)."""
    events = read_events() if events is None else events
    ts = scratch_tickets(plan)
    issues = _issue_tokens(plan)
    by_num = {t["num"].lstrip("0"): t["arquivo"] for t in ts}

    def linked(scratch, title):
        paths = {os.path.realpath(x) for x in scratch or []} & set(by_num.values())
        if not paths and (m := _TITLE_TICKET.search(title or "")) and m.group(1) in issues and m.group(2).lstrip("0") in by_num:
            paths = {by_num[m.group(2).lstrip("0")]}
        return paths

    done = {t["arquivo"] for t in ts if t["status"] in _SCRATCH_DONE}
    closed = {e.get("ticket") for e in events if e.get("tipo") == "ticket" and e.get("op") == "fechar"}
    done |= {p for t in tickets() if t["num"] in closed for p in linked(t.get("scratch") and [t["scratch"]], t["titulo"])}
    worker_done = {(e.get("dispatch"), e.get("outcome") == "succeeded") for e in events if e.get("tipo") == "worker_done"}
    verdict = {e.get("dispatch"): e.get("ok") for e in events if e.get("tipo") == "conformidade"}  # the last one counts
    delivered = {d for d, ok in worker_done if ok} | ({e.get("dispatch") for e in events if e.get("tipo") == "entrega"} - {d for d, ok in worker_done if not ok})
    delivered -= {d for d, ok in verdict.items() if not ok} | set(_sent_back(events))  # an `entrega` alone: a sha with no worker_done in the log (older ingests)
    for e in events:
        if e.get("tipo") != "despacho" or e.get("dispatch") not in delivered:
            continue
        title = e.get("titulo") or ""
        done |= linked(e.get("scratch"), title)
        if (m := _INTEGRATES.search(title)) and issues & set(_ISSUE_REF.findall(title)):
            done |= {by_num[n.lstrip("0")] for n in re.findall(r"\d+", m.group(1)) if n.lstrip("0") in by_num}
    return done


def phase_check(text_value, roots, events=None):
    """For each phase the text cites (`fase 1`, `phase 2`) in each plan it points to: [{plano, fase, faltando: [num]}], only where the plan has tickets of that phase."""
    out = []
    phases = sorted({int(n) for n in _PHASE.findall(text_value or "")})
    for plan in cited_plans(text_value, roots) if phases else []:
        ts, done = scratch_tickets(plan), None
        for f in phases:
            of_phase = [t for t in ts if t["fase"] == f]
            if not of_phase:
                continue
            done = scratch_done(plan, events) if done is None else done
            out.append({"plano": plan, "fase": f, "faltando": [t["num"] for t in of_phase if t["arquivo"] not in done]})
    return out


def phase_refusal(checks):
    """The refusal text for the phases with something missing, or None."""
    bad = [c for c in checks if c["faltando"]]
    return "; ".join(f"phase {c['fase']} of {c['plano']} is incomplete: missing ticket(s) {', '.join(c['faltando'])}" for c in bad) or None


def _declared_phases(events):
    """[(text, ts)] of what declared a phase integrated: an opened PR that cites it (`fase_declarada`) and a dispatch whose title integrates a phase with a `succeeded` worker_done."""
    ok = {e.get("dispatch") for e in events if e.get("tipo") == "worker_done" and e.get("outcome") == "succeeded"}
    out = [(e.get("texto") or "", e.get("ts")) for e in events if e.get("tipo") == "fase_declarada"]
    out += [(e.get("titulo") or "", e.get("ts")) for e in events if e.get("tipo") == "despacho" and e.get("dispatch") in ok
            and re.search(r"integr", e.get("titulo") or "", re.I) and _PHASE.search(e.get("titulo") or "")]
    return out


def doctor_scratch(roots=None, events=None):
    """Scratch tickets still `ready-for-agent` in a phase already declared integrated: {esquecidos: [{plano, fase, ticket, arquivo, entregue}]}. `entregue` (delivered) says
    whether the delivery exists and only the Status was left behind, or whether the declaration left the ticket out."""
    events = read_events() if events is None else events
    roots = scratch_roots([os.getcwd()]) if roots is None else roots
    found, seen = [], set()
    for text_value, _ in _declared_phases(events):
        for plan in cited_plans(text_value, roots):
            phases = {int(n) for n in _PHASE.findall(text_value)}
            done = scratch_done(plan, events)
            for t in scratch_tickets(plan):
                if t["fase"] in phases and t["status"] == STATUS_NEW and t["arquivo"] not in seen:
                    seen.add(t["arquivo"])
                    found.append({"plano": plan, "fase": t["fase"], "ticket": t["num"], "arquivo": t["arquivo"], "entregue": t["arquivo"] in done})
    return {"esquecidos": found}


def doctor_scratch_text(r):
    if not r["esquecidos"]:
        return "scratch: no ready-for-agent ticket in a phase declared integrated"
    return "\n".join(f"scratch {x['ticket']} (phase {x['fase']}) still {STATUS_NEW}: " + (f"delivered, set its Status to resolved: {x['arquivo']}" if x["entregue"]
                     else f"the phase was declared integrated without it: {x['arquivo']}") for x in r["esquecidos"])


# ---------- tickets in files ----------

_FILE_NUM = re.compile(r"^(\d{2,})-.+\.md$")
_ACCEPTANCE = re.compile(r"^##\s+(?:Acceptance criteria|Critérios de aceite)\s*$", re.I | re.M)
_WHAT_TO_BUILD = re.compile(r"^##\s+What to build\s*$", re.I | re.M)


def _header(txt):
    """The text before the first `## ` section: title, Status, Blocked by, Run and Task."""
    return re.split(r"^## ", txt, maxsplit=1, flags=re.M)[0]


def _campo(cab, item_name):
    m = re.search(rf"^{re.escape(item_name)}:[ \t]*(.*?)[ \t]*$", cab, re.M)
    return m.group(1) if m else None


def _trocar_campo(txt, item_name, value):
    """Replaces the header's `Nome: value` line; without it, the line goes in after the others."""
    cab = _header(txt)
    if _campo(cab, item_name) is None:
        new = cab.rstrip("\n") + f"\n{item_name}: {value}\n\n"
    else:
        new = re.sub(rf"^{re.escape(item_name)}:.*$", lambda _: f"{item_name}: {value}", cab, count=1, flags=re.M)
    return new + txt[len(cab):]


def _write(path, txt):
    """Replaces the file in one go (temporary file in the same folder and rename): readers never see a half-written ticket."""
    import tempfile
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".tk-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(txt)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp)
        raise


def read_ticket(path):
    """{num, arquivo, titulo, status, blocked_by, run, task, modelo, effort} from the header of a `NN-slug.md` ticket (`Modelo:` and `Effort:` are optional: what orq
    dispatches on its own when the ticket is released); raises OSError or ValueError if unreadable."""
    with open(path, encoding="utf-8") as f:
        txt = f.read()
    cab = _header(txt)
    title = re.match(r"#[ \t]*(?:\d+[ \t]*:[ \t]*)?(.+)", cab)
    if not title:
        raise ValueError(f"{path} does not start with the title (# NN: Title)")
    return {"num": os.path.basename(path).split("-")[0].zfill(2), "arquivo": path, "titulo": title.group(1).strip(),
            "status": _campo(cab, "Status") or "?", "blocked_by": [n.zfill(2) for n in re.findall(r"\d+", _campo(cab, "Blocked by") or "")],
            "run": _campo(cab, "Run") or None, "task": _campo(cab, "Task") or None, "modelo": _campo(cab, "Model") or _campo(cab, "Modelo") or None, "effort": _campo(cab, "Effort") or None,
            "despacho": _campo(cab, "Dispatch") or _campo(cab, "Despacho") or None, "espera": _campo(cab, "Waiting") or _campo(cab, "Espera") or None,
            "issue": int(m.group(1)) if (m := re.search(r"^issue:[ \t]*#?(\d+)", cab, re.M | re.I)) else None, "scratch": _campo(cab, "Scratch") or None}


def _tickets_in_backlog():
    """Tickets live in the backlog (ORQ_BACKLOG and ORQ_BACKLOG_TICKETS): state, blockers and the link to the Orca task stay there, and the `issues/NN-slug.md` file holds only the text."""
    return bool(BACKLOG and BACKLOG_TICKETS)


def backlog_group(item_name, cfg):
    """The backlog.md of a group (M7): the group JSON's `backlog` or, without it, `groups/<name>/backlog.md` next to the machine's backlog; None with no backlog attached."""
    if cfg.get("backlog"):
        return _path(os.path.expanduser(cfg["backlog"]))
    base = _machine_backlog() or BACKLOG
    return os.path.join(os.path.dirname(base), "grupos", item_name, "backlog.md") if base else None


def _ticket_backlogs():
    """The backlog.md files where a ticket number may be: the process's, the machine's and those of the groups that already exist. The number is unique across the whole machine."""
    findings = [BACKLOG, _machine_backlog(), *(backlog_group(n, c) for n, c in groups().items())]
    return [b for b in dict.fromkeys(findings) if b and (b == BACKLOG or os.path.exists(b))]


def _largest_ticket():
    """The highest ticket number already used: in the ISSUES files and, with tickets in the backlog, in all backlogs (a ticket that already moved to the group still occupies the number)."""
    try:
        numbers = [int(n.split("-")[0]) for n in os.listdir(ISSUES) if _FILE_NUM.match(n)]
    except FileNotFoundError:
        numbers = []
    if _tickets_in_backlog():
        for b in _ticket_backlogs():
            numbers += [int(i["id"][1:]) for i in backlog.read_value(b) if re.fullmatch(r"t\d+", i["id"])]
    return max(numbers, default=0)


def _item_of_ticket(number):
    """The backlog item for ticket `number` (the id may be `t5` or `t05`), or None."""
    return next((i for i in backlog.read_value(BACKLOG) if (m := re.fullmatch(r"t(\d+)", i["id"])) and int(m.group(1)) == int(number)), None)


def _backlog_tickets():
    """The backlog tickets in the format of `tickets()` (see `backlog.ticket_of_item`); `file_name` is the `spec:` relative to the folder that holds ISSUES."""
    try:
        item_list = backlog.read_value(BACKLOG)
    except OSError as e:
        log(f"backlog: {type(e).__name__}: {e}")
        return []
    by_id = {i["id"]: i for i in item_list}
    findings = [t for i in item_list if (t := backlog.ticket_of_item(i, by_id, os.path.dirname(ISSUES)))]
    return sorted(findings, key=lambda t: int(t["num"]))


def tickets():
    """All tickets in ISSUES, by number (or from the backlog, with ORQ_BACKLOG and ORQ_BACKLOG_TICKETS). An unreadable ticket goes to the log and is left out: the session summary does not break because of it."""
    if _tickets_in_backlog():
        return _backlog_tickets()
    try:
        names = sorted((n for n in os.listdir(ISSUES) if _FILE_NUM.match(n)), key=lambda n: int(n.split("-")[0]))
    except OSError:
        return []
    findings = []
    for n in names:
        try:
            findings.append(read_ticket(os.path.join(ISSUES, n)))
        except (OSError, ValueError) as e:
            log(f"ticket {n}: {type(e).__name__}: {e}")
    return findings


def _slug(title):
    s = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")[:48].strip("-") or "ticket"


def dispatch_wait(t, integration, events, without_push):
    """Why the ticket does not enter the dispatch queue on its own, or None. `Despacho: manual[, reason]` never enters; `Espera: integrador empty` only with the
    integrator queue empty and no unpushed commits on main (`without_push`, always read: the integrator does not record the `cycle`)."""
    d = (t.get("despacho") or "").strip()
    if d.lower().startswith("manual"):  # `Dispatch: manual` (or the old `Despacho:`)
        return f"Dispatch: {d}"
    if (t.get("espera") or "").strip().lower() not in ("integrador vazio", "integrator empty"):
        return None
    if integration:
        return f"waiting for the integrator to empty ({len(integration)} in the queue)"
    if without_push:
        return f"waiting for the integrator cycle ({without_push} commit(s) unpushed)"
    return None


def ticket_line(t, waiting=None):
    blocker_text = f"; Blocked by: {', '.join(t['blocked_by'])}" if t["blocked_by"] else ""
    return f"{t['num']} {_quote(t['titulo'], 60)} ({t['status']}{blocker_text})" + (f" [{waiting}]" if waiting else "")


def _waiting_ticket_line(t, integration, events, without_push):
    return ticket_line(t, dispatch_wait(t, integration, events, without_push) if t["status"] == STATUS_NEW and not t["blocked_by"] else None)


def _meta_ticket(model=None, effort=None, dispatch_mode=None, waiting=None):
    """The optional fields that say how the ticket is launched: Modelo, Effort, Despacho and Espera (ticket 142), in the order the header and the backlog store them."""
    return {k: " ".join(v.split()) for k, v in (("modelo", model), ("effort", effort), ("despacho", dispatch_mode), ("espera", waiting)) if v}


def _backlog_ticket_add(number, title, blockers, path, task, run, meta):
    """`add` of ticket `tNN` to the backlog: kind `ticket`, `repo` from the title prefix, one `blocked-by` per blocker and the body with the `spec:` (relative to the ISSUES folder),
    the `orca: <task> <run>` and the meta. The CLI requires the blocker to exist in this same backlog."""
    body_text = backlog.body_with_meta({"spec": os.path.relpath(path, os.path.dirname(ISSUES)), "orca": f"{task} {run}", **meta}, None, backlog.META_TICKET)
    repo = backlog.repo_from_title(title)
    backlog.cli(BACKLOG, "add", f"t{number}", title, "--kind", "ticket", *(["--repo", repo] if repo else []),
                *(x for b in blockers for x in ("--blocked-by", f"t{b}")), "--body", body_text)


def ticket_new(title, spec_file, blocked_by=None, run=None, model=None, effort=None, dispatch_mode=None, waiting=None):
    """Creates `ISSUES/NN-<slug>.md` from the title and the spec file, and the Orca task (`--task-title` equal to the title, a short `--spec` that
    points to the file, `--deps` with the tasks of the Blocked by still open). The `task_id` stays in the ticket, which is the only source of the content.

    The spec carries "## What to build" (otherwise the whole text becomes that section) and "## Acceptance criteria" (required). Without the task the ticket does not stay:
    if `task-create` fails, the file is undone.

    With tickets in the backlog (M5) the file is only the text (`# NN: título` and the sections, without Status, Blocked by, Run or Task) and the state goes to the backlog item `tNN`,
    written after the task: if the `add` fails, the file goes away and the task is completed with `desfeito` (undone). `model`, `effort`, `dispatch_mode` and `waiting` are the
    optional header fields (or the item's meta).
    """
    title = " ".join((title or "").split())
    if not title:
        raise ValueError("ticket without a title")
    no_backlog = _tickets_in_backlog()
    if no_backlog and (p := backlog.problem_title(title)):
        raise ValueError(p)
    meta = _meta_ticket(model, effort, dispatch_mode, waiting)
    try:
        with open(os.path.expanduser(spec_file), encoding="utf-8") as f:
            body_text = f.read().strip()
    except (OSError, ValueError) as e:
        raise ValueError(f"could not read {spec_file}: {getattr(e, 'strerror', None) or e}")
    if not _ACCEPTANCE.search(body_text):
        raise ValueError("the spec needs the '## Acceptance criteria' section (the /to-tickets format)")
    if body_text.startswith("# "):
        body_text = body_text.split("\n", 1)[1].strip() if "\n" in body_text else ""  # the title lives in the ticket's header
    if not _WHAT_TO_BUILD.search(body_text):
        body_text = f"## What to build\n\n{body_text}"
    existing = {t["num"]: t for t in tickets()}
    blockers = list(dict.fromkeys(n.zfill(2) for n in re.findall(r"\d+", blocked_by or "")))
    target = default_run(run)
    deps, notices = [], []
    for n in blockers:
        t = existing.get(n)
        if not t:
            raise ValueError(f"ticket {n} does not exist in {ISSUES}: Blocked by only accepts a ticket that was already created")
        if t["status"] == STATUS_CLOSED or not t["task"]:  # the resolved blocker's task is already completed
            continue
        if target and t["run"] and t["run"] != target:  # Orca refuses --deps on a task from another Run (B23): the block stays only on the Blocked by line
            notices.append(f"ticket {n} belongs to Run {t['run']}, not {target}: Blocked by stayed only in the ticket line, without --deps on the task")
        else:
            deps.append(t["task"])
    if not target:
        raise ValueError("no Run bound: pass --run and run run-use --id <r>")
    with _no_run(target):
        if not coordinator_run(target):
            raise ValueError(f"the ticket is for Run {target}, which the coordinator does not command: Orca refuses task-create in another Run (consumer_fenced); {bind_tip(target)}")
        os.makedirs(ISSUES, exist_ok=True)
        with _lock("ticket.lock"):  # B25: two simultaneous `ticket new` do not pick the same number
            number = f"{_largest_ticket() + 1:02d}"
            path = os.path.join(ISSUES, f"{number}-{_slug(title)}.md")
            cab = "" if no_backlog else f"\nStatus: {STATUS_NEW}\nBlocked by: {', '.join(blockers) or '(nenhum)'}\nRun: {target}\n" + "".join(f"{k.capitalize()}: {v}\n" for k, v in meta.items())
            txt = f"# {number}: {title}\n{cab}\n{body_text}\n"
            with open(path, "x", encoding="utf-8") as f:
                f.write(txt)
        try:
            res = orca("task-create", "--spec", f"Leia e execute o ticket {path}\n\n{WAITING_BLOCK}", "--task-title", title, "--run", target,
                       *(["--deps", json.dumps(deps)] if deps else []), timeout=20)
            task = (res.get("task") or res).get("id")
            if not task:
                raise RuntimeError(f"task-create without id in the response: {json.dumps(res)[:300]}")
        except BaseException:
            with contextlib.suppress(OSError):
                os.remove(path)
            raise
        if no_backlog:
            try:
                _backlog_ticket_add(number, title, blockers, path, task, target, meta)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.remove(path)
                with contextlib.suppress(Exception):
                    orca("task-update", "--id", task, "--status", "completed", "--run", target, "--result", json.dumps({"desfeito": f"ticket {number}: the backlog refused"}), timeout=20)
                raise
        else:
            _write(path, _trocar_campo(txt, "Task", task))
        append_event({"tipo": "ticket", "op": "novo", "ticket": number, "task": task, "run": target, "titulo": title, **({"deps": deps} if deps else {})})
    return {"ticket": number, "arquivo": path, "task": task, "run": target, **({"aviso": "; ".join(notices)} if notices else {})}


def ticket_close(numero, answer):
    """Writes `## Answer` (the text, or the file's contents if `answer` is a path), sets `Status: resolved` and completes the task in Orca if it
    is still open. The file is the truth: an Orca failure becomes a notice and the ticket stays resolved. Returns {ticket, status, task, task_fechada, aviso}.

    With tickets in the backlog (M5) the truth is the item: the `done` comes first (if the CLI refuses, nothing changed), the `## Answer` goes into the file afterwards and the file's
    header is not touched. Dependents leave the block on their own, because the blocker became Done."""
    n = str(numero).strip().zfill(2)
    before = tickets()
    t = next((t for t in before if t["num"] == n), None)
    if not t:
        raise ValueError(f"ticket {n} does not exist in {ISSUES}")
    if t["status"] == STATUS_CLOSED:
        raise ValueError(f"ticket {n} is already {STATUS_CLOSED}")
    path = os.path.expanduser(answer or "")
    answer_text = (open(path, encoding="utf-8").read() if answer and os.path.isfile(path) else answer or "").strip()
    if not answer_text:
        raise ValueError("--answer is empty: say what resolved the ticket (text or file)")
    closed_item, notice = False, ""
    if _tickets_in_backlog():
        backlog.cli(BACKLOG, "done", _item_of_ticket(n)["id"], "--no-prune")
        try:
            with open(t["arquivo"], encoding="utf-8") as f:
                txt = f.read()
            _write(t["arquivo"], txt.rstrip("\n") + f"\n\n## Answer\n\n{answer_text}\n")
        except (OSError, TypeError) as e:  # the item is already Done: the answer stays in the log and in the notice
            notice = f"the ## Answer did not make it into the ticket file ({getattr(e, 'strerror', None) or 'ticket without a spec'}): {_quote(answer_text, 80)}"
    else:
        with open(t["arquivo"], encoding="utf-8") as f:
            txt = f.read()
        _write(t["arquivo"], _trocar_campo(txt, "Status", STATUS_CLOSED).rstrip("\n") + f"\n\n## Answer\n\n{answer_text}\n")
    if t["task"] and t["run"]:
        try:
            with _no_run(t["run"]):
                tk = next((x for x in orca("task-list", "--run", t["run"], timeout=20)["tasks"] if x["id"] == t["task"]), None)
                if tk and tk.get("status") not in ("completed", "failed"):
                    orca("task-update", "--id", t["task"], "--status", "completed", "--run", t["run"], "--result", json.dumps({"ticket": n}), timeout=20)
                    closed_item = True
                    if tk.get("status") == "dispatched":
                        notice = "; ".join(x for x in (notice, f"task {t['task']} was dispatched: check whether the worker is still running (orq agents)") if x)
        except Exception as e:  # noqa: BLE001 - the ticket is already resolved: the task is closed by hand
            notice = "; ".join(x for x in (notice, f"task {t['task']} not closed ({e}): {bind_tip(t['run'])} and orca orchestration task-update --id {t['task']} --status completed") if x)
    if t.get("scratch"):  # one tracker: the scratch ticket this one was born from closes with it (ticket 201)
        try:
            set_scratch_status(t["scratch"], STATUS_CLOSED)
        except OSError as e:
            notice = "; ".join(x for x in (notice, f"the scratch {t['scratch']} kept its Status ({e.strerror}): set **Status:** {STATUS_CLOSED} by hand") if x)
    released, notices = _release_dependents(n, before)
    notice = "; ".join(x for x in (notice, *notices) if x)
    append_event({"tipo": "ticket", "op": "fechar", "ticket": n, "task": t["task"], "task_fechada": closed_item, **({"aviso": notice} if notice else {}),
                  "liberados": [{"ticket": x["ticket"], "prioridade": x["prioridade"]} for x in released]})
    for o in [o for o in open_obligations(read_events()) if o["chave"] == "ticket" and o.get("ticket") == n]:
        _close_obligation(o, "feito", prova=f"ticket {n} {STATUS_CLOSED}")  # orq fulfils it on its own and only records it
    return {"ticket": n, "status": STATUS_CLOSED, "arquivo": t["arquivo"], "task": t["task"], "task_fechada": closed_item, "aviso": notice, "liberados": released}


def ticket_edit(numero, **fields):
    """Replaces `model`, `effort`, `dispatch_mode` or `waiting` of a ticket (an empty value removes the field): with tickets in the backlog, the item body's meta (`tasks-axi update --body`);
    without it, the file's header line. Returns {ticket, campos}."""
    n = str(numero).strip().zfill(2)
    t = next((t for t in tickets() if t["num"] == n), None)
    if not t:
        raise ValueError(f"ticket {n} does not exist")
    fresh = {k: " ".join(v.split()) for k, v in fields.items() if v is not None}
    if not fresh:
        raise ValueError("say what to change: --model, --effort, --dispatch or --waiting")
    if _tickets_in_backlog():
        item = _item_of_ticket(n)
        meta, rest = backlog.body_meta(item["corpo"], backlog.META_TICKET)
        meta = {k: v for k, v in {**meta, **fresh}.items() if v}
        backlog.cli(BACKLOG, "update", item["id"], "--body", backlog.body_with_meta(meta, rest, backlog.META_TICKET))
    else:
        with open(t["arquivo"], encoding="utf-8") as f:
            txt = f.read()
        for k, v in fresh.items():
            item_name = k.capitalize()
            txt = _trocar_campo(txt, item_name, v) if v else re.sub(rf"^{item_name}:.*\n", "", txt, count=1, flags=re.M)
        _write(t["arquivo"], txt)
    return {"ticket": n, "campos": sorted(fresh)}


def _released_worktree(t):
    """(worktree, name) with which the released ticket goes up from the queue: `current` for an orq ticket (its worktree block already says where to work) and, for a
    project one, a new worktree with `--name` from the title (kebab, no accents, up to 40 letters): Orca refuses new-top-level without a name. An invalid Run project raises ValueError."""
    project = dispatch_project(None, t["run"])
    if orq_ticket(t["titulo"], project):
        return "current", None
    return "new-top-level", _slug(t["titulo"])[:40].strip("-")


def _release_dependents(n, before=None):
    """Ticket `n` has just been resolved: removes the number from the `Blocked by:` of whoever depended on it (and the other blockers already resolved).
    With tickets in the backlog there is no line to rewrite: `before` (the tickets from before the `done`) says who depended on `n`, and the rest of the computation is the same.

    Whatever is left with no blockers and is still ready-for-agent is "released": with priority 1 or 2 and `Modelo:` and `Effort:` in the header it enters the
    dispatch queue (the manager launches by slot and priority); without them it only warns; P3 never goes up on its own. The Orca task in `blocked` of whoever became free turns `ready`.
    Returns ([{ticket, prioridade, fila?}] by priority, notices). An Orca failure becomes a notice: the files are already right."""
    ts = tickets()
    status = {t["num"]: t["status"] for t in ts}
    released, notices, free_items = [], [], []
    events, integration = read_events(), integration_queue()
    without_push = _no_push()
    no_backlog = _tickets_in_backlog()
    depended = {x["num"] for x in (before or ts) if n in x["blocked_by"]}
    for t in ts:
        if (t["num"] not in depended if no_backlog else n not in t["blocked_by"]) or t["status"] == STATUS_CLOSED:
            continue
        remaining_blocks = [b for b in t["blocked_by"] if b != n and status.get(b) != STATUS_CLOSED]
        if not no_backlog:
            try:
                with open(t["arquivo"], encoding="utf-8") as f:
                    _write(t["arquivo"], _trocar_campo(f.read(), "Blocked by", ", ".join(remaining_blocks) or "(nenhum)"))
            except OSError as e:
                notices.append(f"ticket {t['num']}: Blocked by not updated ({e.strerror}): remove {n} by hand")
                continue
        if remaining_blocks:
            continue
        free_items.append(t)
        if t["status"] != STATUS_NEW:
            continue
        priority = priority_of(events, t["task"], None, t["titulo"])
        item = {"ticket": t["num"], "prioridade": priority}
        waiting = dispatch_wait(t, integration, events, without_push)
        if waiting and priority < 3:
            notices.append(f"ticket {t['num']} (P{priority}) released, outside the dispatch queue ({waiting}): dispatch it with orq dispatch --ticket {t['num']}")
        elif priority < 3 and t["task"] and t["run"] and t["modelo"] and t["effort"] in HARNESS["claude"]["efforts"]:
            try:
                wt, item_name = _released_worktree(t)
                item["fila"] = _enqueue_dispatch(f"ticket {n} resolved: released {t['num']}", t["run"], t["titulo"], None, t["modelo"], t["effort"], wt, item_name, None, None, t, priority, "claude")["fila"]
            except (OSError, ValueError) as e:
                notices.append(f"ticket {t['num']} released, but did not enter the dispatch queue ({e}): dispatch it with orq dispatch --ticket {t['num']}")
        elif priority < 3:
            notices.append(f"ticket {t['num']} (P{priority}) released without a valid Model:/Effort: in the header: dispatch it with orq dispatch --ticket {t['num']}")
        released.append(item)
    for t in free_items:  # the task blocked by an Orca blocker (worker-stop, deps) becomes ready again
        if not (t["task"] and t["run"]):
            continue
        try:
            with _no_run(t["run"]):
                tk = next((x for x in orca("task-list", "--run", t["run"], timeout=20)["tasks"] if x["id"] == t["task"]), None)
                if tk and tk.get("status") == "blocked":
                    orca("task-update", "--id", t["task"], "--status", "ready", "--run", t["run"], timeout=20)
        except Exception as e:  # noqa: BLE001 - the files are already right; the task goes back to ready by hand
            notices.append(f"task {t['task']} of ticket {t['num']} remains blocked ({e}): orca orchestration task-update --id {t['task']} --status ready")
    return sorted(released, key=lambda x: (x["prioridade"], x["ticket"])), notices


def dispatch_wait_line(ts, events):
    """'fora da fila de despacho: 88 (Despacho: manual, ...)' (outside the dispatch queue): the ready tickets with no blockers that a Despacho/Espera header holds back; '' if there are none."""
    integration = integration_queue()
    without_push = _no_push()
    seg = [(t["num"], m) for t in ts if t["status"] == STATUS_NEW and not t["blocked_by"] and (m := dispatch_wait(t, integration, events, without_push))]
    return f"outside the dispatch queue: {'; '.join(f'{n} ({m})' for n, m in seg)}" if seg else ""


def released_line(events, ts):
    """'liberados: 91, 88 (P1, P2)' (released): the tickets that a `ticket fechar` left with no blockers and that are still ready-for-agent (no worker, no new lock); '' if there are none."""
    by_num = {t["num"]: t for t in ts}
    priority = {x["ticket"]: x["prioridade"] for e in events if e.get("tipo") == "ticket" and e.get("op") == "fechar" for x in e.get("liberados") or []}
    open_items = sorted((p, n) for n, p in priority.items() if by_num.get(n, {}).get("status") == STATUS_NEW and not by_num[n]["blocked_by"])
    return f"released: {', '.join(n for _, n in open_items)} ({', '.join(f'P{p}' for p, _ in open_items)})" if open_items else ""


def doctor_tasks(dry_run=False):
    """`orq doctor tasks`: crosses Orca's open tasks (all Runs) with the tickets. A `blocked` or `pending` task whose ticket is resolved becomes
    `completed` with `supersededBy` (the ticket that replaced it); one with no ticket is only listed, the coordinator decides (orq does not know whether the
    work still makes sense). `dry_run` only lists. Returns {completadas: [{task, ticket, run}], without_ticket: [{task, run, status, titulo}], avisos}."""
    by_task = {t["task"]: t for t in tickets() if t["task"]}
    completed, without_ticket, notices = [], [], []
    for r in _all_runs():
        try:
            tasks = orca("task-list", "--run", r["id"], timeout=20)["tasks"]
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            notices.append(f"Run {r['id']}: task-list failed ({e})")
            continue
        for tk in tasks:
            if tk.get("status") not in ("blocked", "pending"):
                continue
            t = by_task.get(tk["id"])
            if not t:
                if not (tk.get("task_title") or "").startswith(PROOF_PREFIX):
                    without_ticket.append({"task": tk["id"], "run": r["id"], "status": tk["status"], "titulo": tk.get("task_title")})
            elif t["status"] == STATUS_CLOSED:
                if not dry_run:
                    try:
                        with _no_run(r["id"]):
                            orca("task-update", "--id", tk["id"], "--status", "completed", "--run", r["id"],
                                 "--result", json.dumps({"ticket": t["num"], "supersededBy": f"ticket {t['num']}"}), timeout=20)
                    except (RuntimeError, subprocess.TimeoutExpired) as e:
                        notices.append(f"task {tk['id']} remains {tk['status']} ({e}): {bind_tip(r['id'])}")
                        continue
                completed.append({"task": tk["id"], "ticket": t["num"], "run": r["id"]})
    if completed and not dry_run:
        append_event({"tipo": "doctor", "op": "tasks", "completadas": completed})
    return {"completadas": completed, "sem_ticket": without_ticket, "avisos": notices}


def doctor_tasks_text(r, dry_run=False):
    """One line per task: the one that was (or would be) completed and the one with no ticket."""
    verb = "would complete" if dry_run else "completed"
    ls = [f"{verb}: {x['task']} (ticket {x['ticket']} resolved, {x['run']})" for x in r["completadas"]]
    ls += [f"no ticket: {x['task']} ({x['status']}, {x['run']}) {_quote(x.get('titulo') or '', 50)}: decide whether to complete it (task-update --status completed) or turn it into a ticket" for x in r["sem_ticket"]]
    ls += [f"warning: {x}" for x in r["avisos"]]
    return "\n".join(ls) or "no stuck task"


def doctor_old(release_=False, max_age_hours=24, only_tickets=()):
    """`orq doctor old`: dispatches with no `release` closed for more than `max_age_hours`, with no terminal in `orca terminal list` and with the ticket resolved: those that hold a
    worktree in `worktrees clean` with nobody using it. With `release_`, each one leaves the live ones: `orq release` if the worker-list still has it, otherwise a `release`
    event with reason `old_name`. A live terminal, an open ticket / no ticket or a worker still `dispatched` are only listed in `stay` (with the reason).
    `only_tickets` restricts to the given numbers. Orca with no terminal list proves nothing: nothing is touched. Returns {antigos: [{dispatch, ticket, terminal}], ficam: [...], liberados: [dispatch], avisos}."""
    events = read_events()
    live = _alive_terminals()
    if live is None:
        return {"antigos": [], "ficam": [], "liberados": [], "avisos": ["terminal list unavailable: no proof of who died"]}
    try:
        ws = {w.get("dispatchId"): w for w in _all_workers()}
        notices = []
    except (RuntimeError, subprocess.TimeoutExpired, OSError) as e:
        ws, notices = {}, [f"worker-list failed ({e}): only the event log counts"]
    ts = tickets()
    by_num, by_task = {t["num"]: t for t in ts}, {t["task"]: t for t in ts if t["task"]}
    tried = {e.get("dispatch") for e in events if e.get("tipo") == "liberar"}
    # `released`/`already_released` with `closed` false: Orca had already released it and the terminal no longer existed; the event does not count in _released (which is for the closed terminal)
    done_items = _released(events) | {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("estado") in ("released", "already_released")}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    old_items, stay, released = [], [], []
    for e in events:
        d = e.get("dispatch")
        if e.get("tipo") != "despacho" or not d or d in done_items or (_ts(e.get("ts")) or cutoff) > cutoff:
            continue
        w, t = ws.get(d, {}), by_num.get(str(e.get("ticket") or "")) or by_task.get(e.get("task"))
        item = {"dispatch": d, "ticket": t["num"] if t else e.get("ticket"), "terminal": w.get("agentTerminalHandle") or e.get("terminal")}
        if only_tickets and str(item["ticket"]).zfill(2) not in {str(n).zfill(2) for n in only_tickets}:
            continue
        reason = ("live terminal" if item["terminal"] in live else "worker still dispatched" if w.get("dispatchStatus") == "dispatched"
                  else "ticket not registered" if not t else "ticket open" if t["status"] != STATUS_CLOSED else None)
        if reason:
            stay.append({**item, "motivo": reason})
            continue
        old_items.append(item)
        if not release_:
            continue
        try:
            if w and d not in tried:  # already tried (retained, release_unknown): repeating worker-release does not help, the terminal is dead
                release(d)
            else:
                append_event({"tipo": "liberar", "dispatch": d, "task": e.get("task"), "run": e.get("run"), "fechado": True, "motivo": "antigo"})
            released.append(d)
        except (ValueError, RuntimeError, subprocess.TimeoutExpired) as x:
            notices.append(f"{d}: not released ({x})")
    return {"antigos": old_items, "ficam": stay, "liberados": released, "avisos": notices}


def doctor_old_text(r, release_=False):
    """One line per old dispatch (released or to be released) and one for what stays."""
    ls = [f"{'released' if x['dispatch'] in r['liberados'] else 'old'}: {x['dispatch']} (ticket {x['ticket']}, no terminal)" for x in r["antigos"]]
    ls += [f"stays: {x['dispatch']} (ticket {x['ticket']}): {x['motivo']}" for x in r["ficam"]]
    ls += [f"warning: {x}" for x in r["avisos"]]
    if r["antigos"] and not release_:
        ls.append("run `orq doctor old --release` to take them off the live list")
    return "\n".join(ls) or "no old dispatch left unreleased"


def doctor_hooks(pin=False):
    """`orq doctor hooks`: per harness hooks file that exists, the problems with the interpreter of orq's hooks; with `pin`, writes the absolute Python first. Exit 1 while there is a problem."""
    broken = 0
    for agent in HOOKS_FILES:
        if not _hook_commands(HOOKS_FILES[agent]):
            continue
        if pin:
            python, changed = pin_hooks_python(agent)
            print(f"{agent}: " + (f"{changed} hook command(s) now run {python}" if python else "no Python 3.12+ found") + ("; Codex asks to trust them again in /hooks" if changed and agent == "codex" else ""))
        problems = hooks_python_problems(agent)
        broken += len(problems)
        print("\n".join(f"{agent}: {x}" for x in problems) or f"{agent}: hooks ok")
    if pin:
        print("orq link: " + ("wrapper written" if pin_orq_link() else "unchanged"))
    return 1 if broken else 0


def doctor_backlog():
    """`orq doctor backlog` (M6): crosses the tickets of the backlogs (the process's, the machine's and the groups') with Orca's tasks and states the fix for each difference, writing nothing.

    Looks for: In flight with no dispatched task, Done with an open task, Queued whose task has already finished or is dispatched, a task that Orca does not list, a missing spec and a file in
    `issues/` with no item in any backlog. Returns {tickets, tasks, problemas: [{ticket, problema, conserto}], avisos}."""
    backlogs = _ticket_backlogs()
    root = os.path.dirname(ISSUES)
    tks, ids = [], set()
    for b in backlogs:
        item_list = backlog.read_value(b)
        by_id = {i["id"]: i for i in item_list}
        ids |= {i["id"] for i in item_list}
        tks += [(b, t) for i in item_list if (t := backlog.ticket_of_item(i, by_id, root))]
    by_run, notices, problems = {}, [], []
    for run in sorted({t["run"] for _, t in tks if t["run"] and t["task"]}):
        try:
            by_run[run] = {x["id"]: x for x in orca("task-list", "--run", run, timeout=20)["tasks"]}
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            notices.append(f"Run {run}: task-list failed ({e}): its tasks were not checked")

    def add_finding(t, problem, fix):
        problems.append({"ticket": t["num"], "problema": problem, "conserto": fix})

    for b, t in tks:
        n, file_path = t["num"], f"TASKS_AXI_FILE={shlex.quote(b)} tasks-axi"
        if t["arquivo"] and not os.path.exists(t["arquivo"]):
            add_finding(t, f"the spec {t['arquivo']} does not exist", f"restore the file or point to another one with {file_path} update t{n} --body")
        if not (t["task"] and t["run"]) or t["run"] not in by_run:
            continue
        tk = by_run[t["run"]].get(t["task"])
        if not tk:
            add_finding(t, f"task {t['task']} is not in Run {t['run']}", "if the work is done, orq ticket close; otherwise recreate the ticket (orq ticket new)")
            continue
        st, is_open = tk.get("status"), tk.get("status") not in ("completed", "failed")
        if t["status"] == STATUS_CLOSED and is_open:
            add_finding(t, f"Done, but task {t['task']} is {st}", f"orca orchestration task-update --id {t['task']} --status completed --run {t['run']}")
        elif t["status"] == STATUS_IN_PROGRESS and not is_open:
            add_finding(t, f"In flight, but task {t['task']} is already {st}", f"orq ticket close {n} --answer <what resolved it>")
        elif t["status"] == STATUS_IN_PROGRESS and st != "dispatched":
            add_finding(t, f"In flight, but task {t['task']} is {st}, with no dispatched worker", f"orq dispatch --run {t['run']} --ticket {n} --model <m> --effort <e>")
        elif t["status"] == STATUS_NEW and not is_open:
            add_finding(t, f"Queued, but task {t['task']} is already {st}", f"orq ticket close {n} --answer <what resolved it>")
        elif t["status"] == STATUS_NEW and st == "dispatched":
            add_finding(t, f"Queued, but task {t['task']} is dispatched", f"{file_path} start t{n}")
    try:
        for item_name in sorted(os.listdir(ISSUES)):
            if _FILE_NUM.match(item_name) and f"t{item_name.split('-')[0]}" not in ids and f"t{int(item_name.split('-')[0])}" not in ids:
                problems.append({"ticket": item_name.split("-")[0].zfill(2), "problema": f"the file {item_name} has no item in any backlog",
                                  "conserto": f"python3 {shlex.quote(os.path.join(orqpaths.HERE, 'scripts', 'converte-backlog.py'))} --completa"})
    except FileNotFoundError:
        pass
    return {"tickets": len(tks), "tasks": sum(len(v) for v in by_run.values()), "problemas": problems, "avisos": notices}


def doctor_backlog_text(r):
    """One line per difference, with the fix; with none, the count of what was checked."""
    ls = [f"{x['ticket']}: {x['problema']}. Fix: {x['conserto']}" for x in r["problemas"]] + [f"warning: {x}" for x in r["avisos"]]
    return "\n".join(ls) or f"backlog and Orca consistent ({r['tickets']} tickets, {r['tasks']} tasks checked)"


def session_context():
    """What a new coordinator session reads at start: `orq status` (up to 5 lines), the open tickets and the map path, in up to
    SESSION_LINES lines. Tickets that do not fit become `+N open_items`."""
    card = card_first_line(read_events(), datetime.now(timezone.utc))
    line_list = state().splitlines()[:SESSION_LINES - 2 - bool(card)]
    if card:
        line_list.append(card)
    open_items = [t for t in tickets() if t["status"] != STATUS_CLOSED]
    leftover = SESSION_LINES - len(line_list) - 1  # the map's line stays reserved
    if not open_items:
        line_list.append("Open tickets: none.")
    else:
        fit_count = len(open_items) if len(open_items) <= leftover - 1 else leftover - 2  # the header and, if tickets are left over, the +N line
        line_list += [f"Open tickets ({len(open_items)}):", *(f"- {ticket_line(t)}" for t in open_items[:fit_count])]
        if fit_count < len(open_items):
            line_list.append(f"+{len(open_items) - fit_count} open (orq ticket list)")
    line_list.append(f"Map: {MAP}; tickets in {ISSUES}")
    return "\n".join(line_list)


PROOF_PREFIX = "Prova r5"  # the review-5 proof tasks stay out of the default list


def _all_workers(run=None):
    """worker-list, newest first, ACTIVE_PAGES pages: from all Runs (without the bound terminal, `scope.source` all) or only `run`.

    If Orca returns the list scoped to the bound Run (in a worker terminal it binds the dispatch's Run even without ORCA_TERMINAL_HANDLE),
    it raises instead of looking at only one Run: the caller passes --run. ponytail: 300 workers are enough; the rest is history.
    """
    ws, cursor = [], None
    for _ in range(ACTIVE_PAGES):
        res = orca("worker-list", "--limit", "100", *(["--run", run] if run else []), *(["--cursor", cursor] if cursor else []), without_terminal=not run)
        source = (res.get("scope") or {}).get("source")
        if source != ("flag" if run else "all"):
            raise RuntimeError(f"worker-list came back scoped ({source}): the list does not cover all Runs; pass --run <r>")
        ws += res["workers"]
        cursor = (res.get("page") or {}).get("nextCursor")
        if not cursor:
            break
    fresh = {e["dispatch"]: e["terminal"] for e in read_events() if e.get("tipo") == "retomada" and e.get("dispatch") and e.get("terminal")}  # o `orq retomar` subiu outro terminal: o Orca segue apontando o morto
    return [{**w, "agentTerminalHandle": fresh[w["dispatchId"]]} if w.get("dispatchId") in fresh else w for w in ws]


def _retention(w, humans=frozenset()):
    """Why Orca retained the worker's terminal (external_terminal…); `sem_recurso` on the context dispatch, which has no terminal of its own.

    `user_takeover` only counts as retention in the dispatches in `humans` (`orq release` found a user prompt in the worker's transcript):
    Orca marks takeover on any xterm input, with no human, so on its own it proves nothing.
    """
    res = w.get("resource")
    reason = (res.get("retainedReason") if isinstance(res, dict) else "sem_recurso") or None
    return None if reason == "user_takeover" and w.get("dispatchId") not in humans else reason


def _interaction_recorded(events):
    """The dispatches in which `orq release` found a user prompt in the worker's transcript."""
    return {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("interacao")}


def _released(events):
    """The dispatches that `orq release` released and whose terminal closed (`closed`; the retained one stays open and follows the _retention rule)."""
    return {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("fechado")}


def _alive_terminals():
    """The handles from `orca terminal list`; None if Orca fails (the list comes out without that cut)."""
    try:
        r = orca("list", "--limit", "1000", area="terminal", timeout=10)
        if r.get("truncated"):
            log("agentes: terminal list truncado; sem prova de quem morreu")
            return None  # a truncated list does not prove a terminal vanished (B37)
        return {t.get("handle") for t in r["terminals"]}
    except Exception as e:  # noqa: BLE001
        log(f"agentes: terminal list: {type(e).__name__}: {e}")
        return None


def _dead(handle):
    """The terminal is not in `orca terminal list`. With no list (Orca failed or cut it) it proves nothing: False."""
    live = _alive_terminals()
    return live is not None and handle not in live


def _active(w):
    """A worker that still needs attention: running, or terminal not released."""
    return w.get("dispatchStatus") == "dispatched" or w.get("terminalState") != "released"


def _deep_get(d, *keys):
    for c in keys:
        d = d.get(c) if isinstance(d, dict) else None
    return d


def _details(ws):
    """{dispatch: {titulo, modelo, desde, agente}}: task_title from each Run's task-list and model/dispatchedAt/agent from worker-show (only for those not released).

    An isolated failure of a Run or a dispatch leaves the fields empty and goes to the log: the list comes out anyway.
    """
    def titles(r):
        try:
            return {t["id"]: t.get("task_title") for t in orca("task-list", "--run", r, timeout=20)["tasks"]}
        except Exception as e:  # noqa: BLE001
            log(f"agentes: task-list {r}: {type(e).__name__}: {e}")
            return {}

    def show(w):
        try:
            res = orca("worker-show", "--dispatch", w["dispatchId"], timeout=10)
            return w["dispatchId"], {"modelo": _deep_get(res, "worker", "startOptions", "launch", "requested", "model"), "desde": _deep_get(res, "dispatch", "dispatchedAt"),
                                     "effort": _deep_get(res, "worker", "startOptions", "launch", "requested", "effort"),
                                     "agente": _deep_get(res, "worker", "startOptions", "agent")}
        except Exception as e:  # noqa: BLE001
            log(f"agentes: worker-show {w.get('dispatchId')}: {type(e).__name__}: {e}")
            return w["dispatchId"], {}

    runs = sorted({w["runId"] for w in ws if w.get("runId")})
    with ThreadPoolExecutor(8) as ex:
        titles_by_run = dict(zip(runs, ex.map(titles, runs)))
        live = dict(ex.map(show, [w for w in ws if _active(w)]))
    return {w["dispatchId"]: {"titulo": titles_by_run.get(w.get("runId"), {}).get(w.get("taskId")), **live.get(w["dispatchId"], {})} for w in ws}


def screen_question(line_list, agent="claude"):
    """{tipo, texto, opcoes: [[n, rótulo]]} if the end of the screen is an agent menu waiting for a human answer, otherwise None (also for an agent with no adapter).

    Three kinds: `trust` (trust the folder), `permissao` (Do you want to proceed?…) and `question` (AskUserQuestion). An open menu = numbered options
    1, 2… in sequence, one with the cursor `❯`, at the end of the screen (at most SCREEN_FOOTER_MAX footer lines after it); a stray `❯ 1. …` in the history does not count.
    """
    if agent not in HARNESS:
        return None
    defaults = HARNESS[agent]["tela"]
    screen = [re.sub(r"[│╭╮╰╯─]", " ", str(l)).rstrip() for l in line_list or []]
    ops = [(i, defaults["opcao"].match(l)) for i, l in enumerate(screen)]
    ops = [(i, m) for i, m in ops if m]
    block = []
    for i, m in reversed(ops):  # the last block of consecutive options
        if block and (block[0][0] - i > 2 or int(m.group(2)) != int(block[0][1].group(2)) - 1):
            break
        block.insert(0, (i, m))
    if len(block) < 2 or int(block[0][1].group(2)) != 1 or not any(m.group(1) == defaults["cursor"] for _, m in block):
        return None
    if sum(bool(l.strip()) for l in screen[block[-1][0] + 1:]) > SCREEN_FOOTER_MAX:
        return None
    text_value = "\n".join(screen)
    type_name = next((t for t, r in defaults["perguntas"] if r.search(text_value)), None)
    if not type_name:
        return None
    above = [l.strip() for l in screen[:block[0][0]] if l.strip()][-3:]
    return {"tipo": type_name, "texto": _quote(" ".join(above), 300), "opcoes": [[int(m.group(2)), m.group(3)] for _, m in block]}


def screen_limit(line_list, agent="claude"):
    """The plan-limit notice line at the end of the worker's screen (`You've hit your session limit · resets 6:50pm (…)`), or None.

    Looks at the last 15 lines and joins the notice's lines (Claude puts two: the limit and the `continuing automatically`). A notice followed by `esc to interrupt` (the Claude and Codex spinner) is from the past: the worker went back to work on its own
    when the plan renewed (Claude continues on its own: `continuing automatically at …`)."""
    default = (HARNESS.get(agent) or {}).get("tela", {}).get("limite")
    screen = [str(l).rstrip() for l in line_list or []][-15:]
    if not default:
        return None
    found_item = [(i, m) for i, l in enumerate(screen) if (m := default.search(l))]
    if not found_item or any("esc to interrupt" in l for l in screen[found_item[-1][0] + 1:]):
        return None
    return _quote(" — ".join(re.sub(r"^\W+", "", screen[i]) for i, _ in found_item), 220)


def _read_screens(ws, details):
    """{dispatch: {espera, pergunta, limite}} of running workers whose screen (end of `terminal read --screen`) shows a shell/monitor still running
    (`waiting`, the reason), a menu waiting for a human answer (`question`, from screen_question) or the plan-limit notice (`limit`, from screen_limit).
    Only those with any of the three get in.

    Only the refresh, the manager and `orq agents` call this (one read per worker, in parallel); the prompt hooks read what was left in the cache. A read failure proves nothing.
    """
    def read_screen(w):
        try:
            tail = _deep_get(orca("read", "--terminal", w["agentTerminalHandle"], "--screen", "--limit", str(SCREEN_LINES), area="terminal", timeout=10), "terminal", "tail") or []
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as e:
            log(f"agentes: tela de {w['dispatchId']}: {type(e).__name__}: {e}")
            return w["dispatchId"], None
        agent = details[w["dispatchId"]]["agente"]
        waiting = HARNESS[agent]["tela"]["espera"]
        m = waiting and waiting.search("\n".join(map(str, tail[-15:])))
        found_item = {"espera": f"{m.group(0).strip()} (tela)" if m else None, "pergunta": screen_question(tail, agent), "limite": screen_limit(tail, agent)}
        return w["dispatchId"], found_item if m or found_item["pergunta"] or found_item["limite"] else None
    target = [w for w in ws if w.get("dispatchStatus") == "dispatched" and w.get("agentTerminalHandle") and (details.get(w.get("dispatchId")) or {}).get("agente") in HARNESS]
    with ThreadPoolExecutor(8) as ex:
        return {d: a for d, a in ex.map(read_screen, target) if a}


def _screens(ws, details):
    """{dispatch: reason} of the `waiting` part of _read_screens."""
    return {d: a["espera"] for d, a in _read_screens(ws, details).items() if a["espera"]}


def agents(run=None, include_all=False, now_at=None):
    """The state of each dispatch: worker-list of all Runs (or of `run`) plus the inbox. Released ones, ones with no terminal, ones retained by Orca and the "Prova r5" tasks only with `include_all`."""
    ws, events, live, hib = _all_workers(run), read_events(), _alive_terminals(), _hibernated()
    if not include_all:
        released = _released(events)
        ws = [w for w in ws if w.get("dispatchId") in hib or not _no_terminal(w, released, live)]  # the hibernated one closed the terminal on purpose, and the delivered hibernated one has not been released yet; those retained for an Orca reason (external_terminal…) and user_takeover with a human prompt have no possible action: they only show with --todos
        humans = _interaction_recorded(events)
        ws = [w for w in ws if w.get("dispatchId") in hib or _active(w) and (w.get("dispatchStatus") == "dispatched" or not _retention(w, humans))]
    msgs = orca("inbox", "--limit", "200", timeout=20)["messages"]
    now_at = now_at or datetime.now(timezone.utc)
    detail_entry = _details(ws)
    screens_read = _read_screens([w for w in ws if w.get("dispatchId") not in hib], detail_entry)  # the hibernated one's terminal does not exist: nothing to read
    agent_rows = build_agents(ws, msgs, events, now_at, detail_entry, live, _turns_ro(), {d: a["espera"] for d, a in screens_read.items() if a["espera"]},
                        {d: a["pergunta"] for d, a in screens_read.items() if a["pergunta"]}, hib, limits={d: a["limite"] for d, a in screens_read.items() if a["limite"]},
                        paused=_dict(_cursor_ro().get("pausados")))
    unread_ids = {e.get("dispatch") for e in recent_alerts(events, now_at, agent_rows) if e.get("alerta") == "steer_nao_lido"}
    for a in agent_rows:
        if a["dispatch"] in unread_ids:
            a["alerta"] = "steer not read"
        a["prioridade"] = priority_of(events, a["task"], a["dispatch"], a.get("titulo"))
    return agent_rows if include_all else [a for a in agent_rows if not (a.get("titulo") or "").startswith(PROOF_PREFIX)]


def _control_text(c, dispatch):
    """`relaunch ok 14:02 (new ctx_…)`: an entry of the control history; whoever reads the new dispatch sees which one it came from."""
    ref = f" (from {c['dispatch']})" if c.get("novo_dispatch") == dispatch else f" (new {c['novo_dispatch']})" if c.get("novo_dispatch") else ""
    return f"{c['acao']} {c['resultado']} {_hora_local(c.get('ts'))}{ref}" + (f" [{_quote(c['motivo'], 40)}]" if c.get("motivo") else "")


def agents_text(agent_rows):
    """One line per dispatch, with what to do right below the stuck one (orq steer) and the delivered-without-release one (orq liberar)."""
    if not agent_rows:
        return "no dispatch"
    line_list = []
    for a in agent_rows:
        ref = a.get("ultimo_heartbeat") or a.get("desde")
        hb = (f"{a.get('fase') or '-'} {_hora_local(a['ultimo_heartbeat'])} ({a['idade_s'] // 60} min ago)" if a.get("ultimo_heartbeat") and a.get("idade_s") is not None
              else f"no heartbeat (since {_hora_local(ref)})" if ref and a["estado"] in ("rodando", "travado", "perguntando") else "")
        if a["estado"] == "sem_terminal":
            hb = "lost the terminal without worker_done"
        elif a["estado"] == "nao_comecou":
            hb = f"did not start: no turn {a['idade_s'] // 60} min after the dispatch ({_hora_local(a.get('desde'))})"
        elif a["estado"] == "limite":
            hb = f"plan limit: {a['limite']}"
        elif a["estado"] == "parado":
            hb = f"stopped at the prompt for {a['idade_s'] // 60} min"
        elif a["estado"] == "hibernado":
            hb = f"hibernated since {_hora_local(a.get('hibernado_desde'))}" + (f" ({a['motivo_hibernado']})" if a.get("motivo_hibernado") else "")
        elif a["estado"] == "aguardando_integracao":
            i = a["integracao"]
            hb = f"awaiting integration: {i['branch']} (ticket {i['ticket']}) in the integrator queue" + (f" for {a['idade_s'] // 60} min" if a.get("idade_s") is not None else "")
        elif a["estado"] == "servico":
            c = a.get("ciclo")
            hb = f"service, last cycle {_hora_local(c['ts'])} {c.get('hash') or ''}".rstrip() + (f": {c['nota']}" if c.get("nota") else "") if c else "service, no cycle yet"
        if a.get("espera") and a["espera"] not in (a.get("fase") or ""):  # wait seen on screen or coordinator pause: not in the heartbeat phase
            hb += f" — waiting: {a['espera']}"
        elif a["estado"] == "travado" and a.get("motivo"):
            hb += f" — {a['motivo']}"
        line_list.append(f"{a['estado']:<11} {'P' + str(a['prioridade']) + ' ' if a.get('prioridade') else ''}{a['task']}  {_quote(a.get('titulo') or '?', 36)}  {a.get('modelo') or '?'}  {a['terminal']}  {hb}".rstrip())
        for av in a.get("entrega") or []:
            line_list.append(f"            NOTICE: {av}")
        if a.get("controle"):
            line_list.append("            control: " + "; ".join(_control_text(c, a["dispatch"]) for c in a["controle"]))
        if a.get("pergunta"):
            p = a["pergunta"]
            line_list.append(f"            QUESTION ON SCREEN ({p['tipo']}): {p['texto']} [{' | '.join(f'{n}) {r}' for n, r in p['opcoes'])}]")
            line_list.append(f'            -> orq answer-screen {a["task"]} <option>')
        if a.get("alerta"):
            line_list.append(f"            ALERT: {a['alerta']} (the worker did not read the adjustment after {STEER_ATTEMPTS} notices; a check without --ack hides the new messages)")
        if a["estado"] == "sem_terminal":
            line_list.append("            -> orq resume --dry-run (the terminal vanished before worker_done; the steer has nowhere to land)")
        elif a["estado"] in ("travado", "nao_comecou", "parado"):
            line_list.append(f'            -> orq steer {a["task"]} "<ajuste>" --run {a["run"]}')
        elif a["estado"] == "entregue":
            line_list.append(f"            -> orq release {a['dispatch']}" if not a.get("retido") else f"            retained: {a['retido']} (orq release does not close it)")
        elif a["estado"] == "hibernado":
            line_list.append(f"            -> orq wake {a['task']} (steer and reply also wake it)")
    return "\n".join(line_list)


def _dispatch_ack(run_id, dispatch):
    """Confirms the Run's pending deliveries that belong only to this dispatch (worker_done, heartbeat…), up to 10 batches; returns (entregas, aviso).

    A batch with a message from another dispatch is not confirmed: Orca delivers it together and confirming it would lose the other worker's. Without a link to the
    Run (consumer_fenced) the ack is skipped and the release goes on.
    """
    deliveries = []
    try:
        with manager_lock():  # the manager's binding to the Run does not change between the check and the ack
            res = orca("check", "--run", run_id)
            for _ in range(10):
                msgs = res.get("messages") or []
                if not res.get("deliveryId") or not msgs:
                    break
                if any(_msg_dispatch(m) != dispatch for m in msgs):
                    return deliveries, ("ack skipped: the batch has a message from another dispatch, which the coordinator has not read yet" if not deliveries else "")
                deliveries.append(res["deliveryId"])
                res = orca("check", "--run", run_id, "--ack", res["deliveryId"])
    except RuntimeError as e:
        return deliveries, f"ack skipped ({e}): {bind_tip(run_id)} and confirm later"
    return deliveries, ""


def _prompt_text(content):
    """The text of a transcript prompt: the string, or the `text` blocks (tool_result and image are not typed text)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(b.get("text") or "" for b in content if isinstance(b, dict) and b.get("type") == "text")
    return ""


def _worker_human_prompts(dispatch):
    """How many user prompts the worker's session received besides the dispatch, Orca and notifications; None if the transcript does not show up.

    The worker's transcript is the `projects/*` file (touched in the last TRANSCRIPT_DAYS) whose first prompt is the dispatch preamble and
    cites the dispatch. Orca cannot tell whether there was a human: it marks `user_takeover` on any xterm input.
    ponytail: reads the whole worker file once per `orq release`; a transcript of tens of MB costs a few seconds.
    """
    t = _dict(_turns_ro().get(dispatch))
    if t.get("harness") == "codex" and t.get("transcrito"):
        return _codex_human_prompts(t["transcrito"])
    target, limit = dispatch.encode(), time.time() - TRANSCRIPT_DAYS * 86400
    for file_path in glob.glob(os.path.join(PROJECTS, "*", "*.jsonl")):
        try:
            if os.path.getmtime(file_path) < limit:
                continue
            with open(file_path, "rb") as f:
                head = f.read(INITIAL_READ)
            if target not in head:
                continue
            first_item = None
            for line in head.splitlines():
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if e.get("type") == "user" and not e.get("isMeta") and _prompt_text(_dict(e.get("message")).get("content")).strip():
                    first_item = _prompt_text(e["message"]["content"])
                    break
            if first_item is None or origin_name(first_item) != "despacho" or dispatch not in first_item:
                continue
            humans = 0
            with open(file_path, "rb") as f:
                for line in f:
                    if b'"type":"user"' not in line and b'"type": "user"' not in line:
                        continue
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    text_value = _prompt_text(_dict(e.get("message")).get("content")).strip()
                    if e.get("type") != "user" or e.get("isMeta") or not text_value or text_value.startswith("<system-reminder>"):
                        continue
                    if origin_name(text_value) not in ("despacho", "orca", "notificacao", "resumo"):
                        humans += 1
            return humans
        except OSError as e:
            log(f"transcrito {file_path}: {type(e).__name__}: {e}")
    return None


def _codex_human_prompts(file_path):
    """User prompts of a Codex rollout (`response_item` with `role: user`) besides the dispatch, Orca and the context that Codex itself injects
    (`<environment_context>`, the AGENTS.md); None if the file does not open."""
    humans = 0
    try:
        with open(file_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                if '"role":"user"' not in line and '"role": "user"' not in line:
                    continue
                try:
                    p = _dict(json.loads(line).get("payload"))
                except ValueError:
                    continue
                if p.get("type") != "message" or p.get("role") != "user":
                    continue
                text_value = "\n".join(c.get("text") or "" for c in p.get("content") or [] if isinstance(c, dict) and c.get("type") == "input_text").strip()
                if text_value and not text_value.startswith(("<environment_context>", "# AGENTS.md instructions", "<user_instructions>")) and \
                        origin_name(text_value) not in ("despacho", "orca", "notificacao", "resumo"):
                    humans += 1
    except OSError as e:
        log(f"transcrito {file_path}: {type(e).__name__}: {e}")
        return None
    return humans


def _can_close(handle, run_id, dispatch):
    """(yes, reason, human): is the retained terminal this dispatch's worker's, and can orq close it? Re-reads the worker-list after the release.

    The row is the dispatch's own (M10), and the terminal cannot be in use by another dispatch still running (reused terminal). It closes the
    `owned` terminal that Orca retained without saying why, and the `user_takeover` of a terminal created by this dispatch whose session transcript has
    no user prompt (M9); `human` is True when the transcript has one. Any other reason (user_requested, external_terminal,
    identity_unproven, reused_terminal, configured_tab…) is Orca saying the terminal is not only the worker's, and the context dispatch
    (`orchestration dispatch`) uses another session's terminal. It never closes the coordinator's own terminal or the Run's coordinator.
    """
    if not handle:
        return False, "no terminal handle", False
    if handle == os.environ.get("ORCA_TERMINAL_HANDLE"):
        return False, "it is this coordinator's terminal", False
    if (orca("run-show", "--id", run_id)["run"] or {}).get("coordinator_handle") == handle:
        return False, "it is the Run's coordinator", False
    ws = _all_workers(run_id)
    line = next((w for w in ws if w.get("dispatchId") == dispatch), None)
    if not line or line.get("agentTerminalHandle") != handle:
        return False, "the dispatch no longer appears in the worker-list with that terminal", False
    other_item = next((w for w in ws if w.get("agentTerminalHandle") == handle and w.get("dispatchId") != dispatch and w.get("dispatchStatus") == "dispatched"), None)
    if other_item:
        return False, f"the terminal was reused by dispatch {other_item.get('dispatchId')}, which is still running", False
    res = line.get("resource") if isinstance(line.get("resource"), dict) else {}
    reason = res.get("retainedReason")
    if res.get("ownershipState") == "owned" and not reason:
        return True, "", False
    if res.get("ownershipState") == "user_owned" and reason in (None, "user_takeover"):
        if res.get("originDispatchId") != dispatch or res.get("ownerDispatchId") != dispatch:
            return False, "Orca retained it as user_takeover and the terminal was not born from this dispatch", False
        humans = _worker_human_prompts(dispatch)
        if humans is None:
            return False, "Orca retained it as user_takeover and the worker transcript was not found: no proof that nobody typed in it", False
        if humans:
            return False, f"Orca retained it as user_takeover and the worker transcript has {humans} user prompt(s)", True
        return True, "", False
    return False, f"Orca retained it as {reason or res.get('ownershipState') or 'unknown'}, it is not only the worker's", False


def _dispatch_end(dispatch, w):
    """The fields of the `fim_dispatch` event: the reason (inbox and log) and the worktree as it is now (worker-show; without it, only the reason)."""
    try:
        msgs = orca("inbox", "--limit", "200", timeout=20)["messages"]
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError, KeyError) as e:
        log(f"liberar: inbox falhou ({type(e).__name__}: {e}); fim_dispatch sem motivo")
        msgs = None
    try:
        wt = worktree_state(_checkpoint(dispatch).get("caminho"))
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:
        log(f"liberar: worker-show {dispatch}: {e}")
        wt = {}
    return {"motivo": end_reason(dispatch, read_events(), msgs), **wt}


def release(dispatch, run=None, all_cwd=False):
    """pending ack of the dispatch, worker-release and, if the state comes back `retained`, `orca terminal close` of the worker's terminal. Records an event.

    Only acts on a dispatch that appears in the worker-list (of all Runs, or of `run`); refuses one that is still running.
    """
    w = next((w for w in _all_workers(run) if w.get("dispatchId") == dispatch), None)
    if not w:
        raise ValueError(f"dispatch {dispatch} does not appear in the worker-list: orq only releases workers of Orca Runs")
    if w.get("dispatchStatus") == "dispatched":
        raise ValueError(f"dispatch {dispatch} is still running: wait for worker_done or use worker-stop")
    with _no_run(w.get("runId")):
        return _release(dispatch, w, all_cwd)


def _close_setup(dispatch, w, run_id, handle):
    """Closes the worktree terminals that orq asked Orca for this dispatch (the setup one): the shells (without `agentIdentity`) of that worktree, and only it.
    A `current` worktree or one with no dispatch record is not the dispatch's: nothing is touched. Never closes the terminal of the worker, the coordinator (this one or the Run's),
    an agent or another dispatch still running. Returns (closed handles, notices).
    ponytail: a shell that the user opened by hand in that worktree also goes; there is no filter by creator in `terminal list`."""
    dispatch_events = next((e for e in reversed(read_events()) if e.get("tipo") == "despacho" and e.get("dispatch") == dispatch), {})
    wt = ((w.get("resource") or {}).get("worktreeId") or "").split("::", 1)[-1]
    if not wt.startswith("/") or dispatch_events.get("worktree") in (None, "current"):
        return [], []
    try:
        line_list = orca("list", "--worktree", f"path:{wt}", "--limit", "100", area="terminal")["terminals"]
        protected = {handle, os.environ.get("ORCA_TERMINAL_HANDLE"), (orca("run-show", "--id", run_id)["run"] or {}).get("coordinator_handle"),
                      *(x.get("agentTerminalHandle") for x in _all_workers(run_id) if x.get("dispatchStatus") == "dispatched")}
    except (RuntimeError, subprocess.TimeoutExpired, OSError, KeyError) as e:
        return [], [f"setup terminals of {wt} not listed: {e}"]
    closed_items, notices = [], []
    for t in line_list:
        if t.get("worktreePath") != wt or t.get("agentIdentity") or t.get("handle") in protected or not t.get("handle"):
            continue
        try:
            orca("close", "--terminal", t["handle"], area="terminal")
            closed_items.append(t["handle"])
        except RuntimeError as e:
            notices.append(f"setup terminal {t['handle']} did not close: {e}")
    return closed_items, notices


def _release(dispatch, w, all_cwd=False):
    """The release body, with the dispatch's Run already commanded by the coordinator (or with the refusal that explains what is missing)."""
    run_id, handle, notices = w.get("runId"), w.get("agentTerminalHandle"), []
    if dispatch in _released(read_events()):
        again = sweep_notices(terminate_worktree_processes(_dispatch_end(dispatch, w).get("caminho"), all_cwd=True), dispatch) if all_cwd else []  # `--processos` on a released dispatch only sweeps
        return {"tipo": "liberar", "dispatch": dispatch, "task": w.get("taskId"), "run": run_id, "terminal": handle, "estado": w.get("terminalState"),
                "fechado": True, "ack": 0, "aviso": "; ".join(again) or "already released"}  # repeating sends neither worker-release nor terminal close (M13)
    deliveries, notice = _dispatch_ack(run_id, dispatch)
    if notice:
        notices.append(notice)
    end = _dispatch_end(dispatch, w)
    owned = worktree_owned_pids(end.get("caminho"))  # before the terminal closes: afterwards the agent is gone and what it left is below no harness
    try:
        state = orca("worker-release", "--dispatch", dispatch, timeout=30, run=run_id).get("state")
    except RuntimeError as e:
        if "consumer_fenced" in str(e):
            raise ValueError(f"the coordinator must command Run {run_id} to release the dispatch: {bind_tip(run_id)}")
        raise
    closed, human, kept = False, False, False
    if state == "retained":
        try:
            can, reason, human = _can_close(handle, run_id, dispatch)
            if can:
                orca("close", "--terminal", handle, area="terminal")
                closed = True
            else:
                kept = True
                notices.append(f"terminal {handle} kept: {reason}")
        except RuntimeError as e:
            notices.append(f"terminal close of {handle} failed: {e}")
    elif state == "release_pending":
        notices.append("release_pending: Orca is still releasing; repeat orq release later")
    setup = []
    if state != "release_pending":
        setup, a_setup = _close_setup(dispatch, w, run_id, handle)
        notices += a_setup
    if state != "release_pending" and not kept and end.get("caminho"):  # terminal kept: someone still uses the worktree
        notices += sweep_notices(terminate_worktree_processes(end["caminho"], owned=owned, all_cwd=all_cwd), dispatch)
    if state != "release_pending":  # repeating release_pending records the end again; the last one counts
        append_event({"tipo": "fim_dispatch", "dispatch": dispatch, "task": w.get("taskId"), "run": run_id, **end})
    if state != "release_pending":
        _forget_hibernated(dispatch)  # released does not come back: the terminal was already closed and there is nothing to wake
    ev = {"tipo": "liberar", "dispatch": dispatch, "task": w.get("taskId"), "run": run_id, "terminal": handle, "estado": state, "fechado": closed, "ack": deliveries,
          **({"interacao": True} if human else {}), **({"setup_fechados": setup} if setup else {})}
    append_event({**ev, **({"aviso": "; ".join(notices)} if notices else {})})
    refresh_bg()  # the next prompt's summary already goes out without the released dispatch (M13)
    return {**ev, "aviso": "; ".join(notices)}


# ---------- worker control: interrupt, stop and relaunch (ticket 32) ----------

def _control(action, w, result, **fields):
    """Records the `controle` event of dispatch `w` (from the worker-list): the history that `orq agents` shows. An empty field does not go in."""
    return append_event({"tipo": "controle", "acao": action, "dispatch": w.get("dispatchId"), "task": w.get("taskId"), "run": w.get("runId"), "resultado": result,
                         **{k: v for k, v in fields.items() if v not in (None, "")}})


def _dispatch_worker(dispatch, run=None):
    w = next((w for w in _all_workers(run) if w.get("dispatchId") == dispatch), None)
    if not w:
        raise ValueError(f"dispatch {dispatch} does not appear in the worker-list: orq only controls workers of Orca Runs")
    return w


def _checkpoint(dispatch):
    """The dispatch's worktree (path, head, dirty files) and the profile the worker came up with (agent, model, effort), from worker-show.

    The path comes from the terminal or, without it, from the `worktreeId` (`<repo>::<path>`). `head` and `dirty` are None if the path is not a readable git repository."""
    res = orca("worker-show", "--dispatch", dispatch, timeout=10)
    wid = _deep_get(res, "worker", "worktreeId")
    path = _worker_path(res)
    request = _deep_get(res, "worker", "startOptions", "launch", "requested") or {}
    head, dirty = (_git(path, "rev-parse", "HEAD"), _git(path, "status", "--porcelain")) if path and os.path.isdir(path) else (None, None)
    return {"worktree_id": wid, "caminho": path, "agente": _deep_get(res, "worker", "startOptions", "agent"), "modelo": request.get("model"), "effort": request.get("effort"),
            "head": (head or "").strip()[:12] or None, "sujo": len(dirty.splitlines()) if dirty is not None else None}


def _intact(cp):
    """The checkpoint's worktree still exists and the commit it was on is still in its history (the worker may have committed before stopping)."""
    c = cp.get("caminho")
    return bool(c) and os.path.isdir(c) and (not cp.get("head") or _git(c, "merge-base", "--is-ancestor", cp["head"], "HEAD") is not None)


def interrupt(dispatch, run=None):
    """Sends Orca's interrupt to the terminal of the running worker (`terminal send --interrupt`) and records the event.

    The worker stays alive and Claude Code does not confirm the cancellation: the caller checks with `orq agents` or `orca terminal read`."""
    w = _dispatch_worker(dispatch, run)
    if w.get("dispatchStatus") != "dispatched":
        raise ValueError(f"dispatch {dispatch} is not running ({w.get('dispatchStatus')}): there is no turn to interrupt")
    handle = w.get("agentTerminalHandle")
    if not handle:
        raise ValueError(f"dispatch {dispatch} has no agent terminal")
    try:
        orca("send", "--terminal", handle, "--interrupt", area="terminal")
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _control("interromper", w, "falhou", terminal=handle, erro=str(e))
        raise
    return _control("interromper", w, "ok", terminal=handle, aviso="interrupt sent, with no confirmation that the turn stopped")


def answer_screen(task, option, run=None):
    """Answers the menu stuck on the task worker's screen: reads the screen again (the menu must be open and the option must exist), types the option number with Enter
    into its terminal and records who answered and what (`controle` event responder-tela, with `by` = this terminal). `option` is the number or the start of the label.

    With no open menu, or an option that does not exist, it refuses without typing anything: a number typed at the ordinary prompt would become a message to the worker."""
    w = next((w for w in _all_workers(run) if w.get("taskId") == task and w.get("dispatchStatus") == "dispatched"), None)
    if not w or not w.get("agentTerminalHandle"):
        raise ValueError(f"task {task} has no running worker with a terminal: nothing to answer")
    handle = w["agentTerminalHandle"]
    agent = _dict(_turns_ro().get(w["dispatchId"])).get("harness") or "claude"
    p = screen_question(_deep_get(orca("read", "--terminal", handle, "--screen", "--limit", str(SCREEN_LINES), area="terminal", timeout=10), "terminal", "tail") or [], agent)
    if not p:
        raise ValueError(f"the screen of {handle} does not show a menu waiting for an answer: nothing was typed")
    target = next((o for o in p["opcoes"] if str(o[0]) == option.strip() or o[1].lower().startswith(option.strip().lower())), None)
    if not target:
        raise ValueError(f"option {option!r} does not exist in the menu ({' | '.join(f'{n}) {r}' for n, r in p['opcoes'])})")
    try:
        if HARNESS[agent]["tela"].get("enter_separado"):
            orca("send", "--terminal", handle, "--text", str(target[0]), area="terminal", timeout=10)
            time.sleep(0.3)
            orca("send", "--terminal", handle, "--enter", area="terminal", timeout=10)
        else:
            orca("send", "--terminal", handle, "--text", str(target[0]), "--enter", area="terminal", timeout=10)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _control("responder-tela", w, "falhou", terminal=handle, erro=str(e), por=os.environ.get("ORCA_TERMINAL_HANDLE"))
        raise
    append_event({"tipo": "pergunta_tela_fim", "dispatch": w["dispatchId"]})
    return _control("responder-tela", w, "ok", terminal=handle, por=os.environ.get("ORCA_TERMINAL_HANDLE"), opcao=f"{target[0]}) {target[1]}", pergunta=p["texto"], tipo_pergunta=p["tipo"])


def _stop_worker(action, w, base):
    """worker-stop of the dispatch that is still running (Orca closes the agent's terminal and never deletes the worktree); a dispatch that has already finished gets nothing."""
    if w.get("dispatchStatus") != "dispatched":
        return
    try:
        orca("worker-stop", "--dispatch", w["dispatchId"], timeout=30, run=w.get("runId"))
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _control(action, w, "falhou", passo="worker-stop", erro=str(e), **base)
        raise
    _forget_hibernated(w["dispatchId"])


def terminate(dispatch, reason, run=None, stopped_by=None):
    """worker-stop (if it is still running) and then the `release`, with the reason in the log. Stopping is not undone: if the release fails, the worker stays stopped, the
    terminal retained and the worktree where it was (`parcial` event), and `orq release` finishes the job."""
    if not (reason or "").strip():
        raise ValueError("end needs --reason: it stays in the log and in orq agents")
    if stopped_by and stopped_by not in STOPS:
        raise ValueError(f"--stopped-by expects {'|'.join(STOP_EN[p] for p in STOPS)} (got {stopped_by!r})")
    w = _dispatch_worker(dispatch, run)
    try:
        cp = _checkpoint(dispatch)
    except RuntimeError as e:
        log(f"encerrar: worker-show {dispatch}: {e}")
        cp = {}
    base = {"motivo": reason.strip(), "parada": stopped_by, "terminal": w.get("agentTerminalHandle"), "head": cp.get("head"), "sujo": cp.get("sujo")}
    _control("encerrar", w, "iniciado", **base)
    _stop_worker("encerrar", w, base)
    try:
        lib = release(dispatch, w.get("runId"))
    except (RuntimeError, ValueError, subprocess.TimeoutExpired) as e:
        _control("encerrar", w, "parcial", passo="release", erro=str(e), worktree_intacta=_intact(cp), **base)
        raise RuntimeError(f"the worker stopped, but the release failed ({e}); the worktree remains at {cp.get('caminho') or '?'}: run orq release {dispatch}")
    return _control("encerrar", w, "ok", worktree_intacta=_intact(cp), aviso=lib.get("aviso"), **base)


def relaunch(dispatch, note, model=None, effort=None, run=None):
    """Stops the worker and starts another in the SAME worktree and task (`worker-start --task --retry-of`), with the old one's profile or the one from --modelo/--effort.

    Everything that can refuse comes before worker-stop (worktree, linked Run, task, profile). After it, the only way back is the one that exists: if the
    requested profile does not start, the previous one starts (`revertido`); if nothing starts, the old terminal is kept, the worktree intact and the message carries the command to
    repeat with the note (`failed`). An Orca task's spec does not change, so the note goes as the new worker's first adjustment (`orq steer`).
    The new worker uses Orca's setup policy for an existing worktree (no new setup)."""
    note = (note or "").strip()
    if not note:
        raise ValueError("relaunch needs --note: what changed for the new worker")
    if bool(model) != bool(effort):
        raise ValueError("--model and --effort go together: the effort of one model does not apply to another")
    w = _dispatch_worker(dispatch, run)
    run_id, task = w.get("runId"), w.get("taskId")
    cp = _checkpoint(dispatch)
    if not cp["caminho"] or not os.path.isdir(cp["caminho"]):
        raise ValueError(f"the worktree of dispatch {dispatch} does not exist ({cp['caminho'] or 'Orca did not say where'}): nothing was stopped")
    request, old_name = (model or cp["modelo"], effort or cp["effort"]), (cp["modelo"], cp["effort"])
    if not all(request):
        raise ValueError("Orca did not report the model and effort of the old worker: pass --model and --effort")
    if not coordinator_run(run_id):
        raise ValueError(f"the worker belongs to Run {run_id}, which the coordinator does not command: {bind_tip(run_id)}")
    occupancy = machine_occupancy()  # swap one by one: the dispatch's own worker leaves the count (it is stopped before the new one comes up), so only the extra expensive model or an already dead dispatch exceeds the ceiling
    occupancy["vivos"].pop(dispatch, None)
    if reason := machine_slot(request[0], occupancy):
        raise ValueError(f"{reason}: nothing was stopped. Relaunch with a cheap model, wait for a slot or adjust orq machine")
    if not any(t["id"] == task for t in orca("task-list", "--run", run_id, timeout=20)["tasks"]):
        raise ValueError(f"task {task} does not exist in Run {run_id}")
    base = {"nota": note, "terminal": w.get("agentTerminalHandle"), "worktree": cp["caminho"], "head": cp["head"], "sujo": cp["sujo"]}
    _control("relancar", w, "iniciado", modelo=request[0], effort=request[1], **base)
    _stop_worker("relancar", w, {**base, "modelo": request[0], "effort": request[1]})
    selector = f"id:{cp['worktree_id']}" if cp["worktree_id"] else f"path:{cp['caminho']}"
    res, error, launched = None, None, None
    for profile in [request, *([old_name] if request != old_name and all(old_name) else [])]:
        try:
            res = orca("worker-start", "--run", run_id, "--task", task, "--retry-of", dispatch, "--worktree", selector, "--agent", cp["agente"] or "claude",
                       "--model", profile[0], "--effort", profile[1], timeout=180)
            launched = profile
            break
        except subprocess.TimeoutExpired:
            error = "worker-start went over 180 s with no response: the worker may have started, check with orq agents"
            break  # repeating would stack a second worker in the same worktree
        except RuntimeError as e:
            error = error or str(e)
    if not res or not res.get("dispatchId"):
        _control("relancar", w, "falhou", passo="worker-start", erro=error, modelo=request[0], effort=request[1], worktree_intacta=_intact(cp), **base)
        raise RuntimeError(f"no worker started ({error}): dispatch {dispatch} is stopped, its terminal retained and the worktree intact at {cp['caminho']}. "
                           f"Repeat with: orq relaunch {dispatch} --note {shlex.quote(note)}")
    new, notices = res["dispatchId"], []
    if launched != request:
        notices.append(f"the requested profile ({request[0]}/{request[1]}) did not start ({error}); the new worker uses the previous one ({launched[0]}/{launched[1]})")
    for step, fn, lap in (("note not delivered", lambda: steer(task, f"Relaunched after {dispatch}. What changed: {note}", run_id), f"orq steer {task} <note>"),
                             ("terminal of the old worker not released", lambda: release(dispatch, run_id), f"orq release {dispatch}")):
        try:
            fn()
        except (RuntimeError, ValueError, subprocess.TimeoutExpired) as e:
            notices.append(f"{step} ({e}): run {lap}")
    return _control("relancar", w, "ok" if launched == request else "revertido", novo_dispatch=new, modelo=launched[0], effort=launched[1],
                     erro=error if launched != request else None, worktree_intacta=_intact(cp), aviso="; ".join(notices), **base)


# ---------- handoff: the worker continues in another harness in the same worktree (ticket 87) ----------

# The profile that holds in the other harness (worker-routing table: the role comes from the ambiguity, and one harness's efforts do not map 1 to 1 onto the other's).
_SONNET, _OPUS = "claude-sonnet-5-5", "claude-opus-5-5"
HANDOFF_PROFILE = {
    "codex": {("sonnet", "low"): ("gpt-6-luna", "low"), ("sonnet", "medium"): ("gpt-6-luna", "medium"), ("sonnet", "high"): ("gpt-6-luna", "xhigh"),
              ("sonnet", "xhigh"): ("gpt-6-luna", "max"), ("sonnet", "max"): ("gpt-6-sol", "low"),
              ("opus", "low"): ("gpt-6-sol", "low"), ("opus", "medium"): ("gpt-6-sol", "medium"), ("opus", "high"): ("gpt-6-sol", "high"),
              ("opus", "xhigh"): ("gpt-6-astra", "low"), ("opus", "max"): ("gpt-6-astra", "medium")},
    "claude": {("luna", "low"): (_SONNET, "low"), ("luna", "medium"): (_SONNET, "medium"), ("luna", "high"): (_SONNET, "medium"),
               ("luna", "xhigh"): (_SONNET, "high"), ("luna", "max"): (_SONNET, "high"), ("sol", "low"): (_SONNET, "high"),
               ("sol", "medium"): (_OPUS, "high"), ("sol", "high"): (_OPUS, "high"), ("sol", "xhigh"): (_OPUS, "xhigh"), ("sol", "max"): (_OPUS, "xhigh"),
               ("astra", "low"): (_OPUS, "xhigh"), ("astra", "medium"): (_OPUS, "max")},
}
_FAMILIA = re.compile(r"\b(sonnet|opus|luna|sol|astra)\b")
HANDOFF_FILE = "HANDOFF.md"
HANDOFF_HISTORY = 20_000  # letters from the end of the transcript in the package
HANDOFF_MSG_MAX = 6_000  # per-message ceiling (the same as ai-memory)
HISTORY_START, HISTORY_END = "<!-- historico-inicio -->", "<!-- historico-fim -->"


def _handoff_profile(model, effort, to_):
    """(model, effort) in `to_` equivalent to the old worker's; ValueError when there is no equivalent (Haiku, Fable, effort outside the table, high Astra)."""
    fam = _FAMILIA.search(model or "")
    target = HANDOFF_PROFILE[to_].get((fam.group(1), effort)) if fam else None
    if not target:
        raise ValueError(f"there is no equivalent in {to_} for {model or '?'}/{effort or '?'}: pass --model and --effort")
    return target


def _msg_text(parts):
    if isinstance(parts, str):
        return parts
    return "\n".join(c.get("text") or "" for c in parts if isinstance(c, dict) and c.get("type") in ("text", "input_text", "output_text")) if isinstance(parts, list) else ""


_CODEX_TOOL = ("function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output", "local_shell_call")


TRANSCRIPT_TOOL_MAX = 1_000  # letters of a tool call or result in the reader


def _call_args(entry):
    """One line with what the call asked for: a Bash's command, otherwise the compact JSON of the arguments."""
    if isinstance(entry, str):
        return entry
    e = _dict(entry)
    cmd = e.get("command") or e.get("cmd")
    return " ".join(cmd) if isinstance(cmd, list) else cmd if isinstance(cmd, str) else json.dumps(entry, ensure_ascii=False)


def _result_text(c):
    return c if isinstance(c, str) else _msg_text(c) if isinstance(c, list) else json.dumps(c, ensure_ascii=False)


def _record_events(m):
    """([event], [cut]) from one transcript record, Claude (`type` user|assistant) or Codex (`type` response_item).

    Event is `{role, type_name: mensagem|call|result, text_value, item_name?}`; cut is the reason a piece was left out (reasoning, meta record, environment context)."""
    evs, cortes = [], []
    if m.get("type") == "response_item":  # Codex
        p = _dict(m.get("payload"))
        t = p.get("type")
        if t == "message" and p.get("role") in ("user", "assistant"):
            text_value = _msg_text(p.get("content")).strip()
            if text_value.startswith("<environment_context>"):
                cortes.append("environment context")
            elif text_value:
                evs.append({"papel": p["role"], "tipo": "mensagem", "texto": text_value})
        elif t == "reasoning":
            cortes.append("reasoning")
        elif t in ("function_call", "custom_tool_call", "local_shell_call"):
            args = p.get("arguments") or p.get("input") or p.get("action")
            if isinstance(args, str):
                with contextlib.suppress(ValueError):
                    args = json.loads(args)
            evs.append({"papel": "assistant", "tipo": "chamada", "nome": p.get("name") or "shell", "texto": _call_args(args)})
        elif t in ("function_call_output", "custom_tool_call_output"):
            o = p.get("output")
            if isinstance(o, str):
                with contextlib.suppress(ValueError):
                    o = _dict(json.loads(o)).get("output", o)
            evs.append({"papel": "user", "tipo": "resultado", "texto": _result_text(o)})
        else:
            cortes.append(f"meta record ({t or '?'})")
        return evs, cortes
    role = m.get("type")
    if role not in ("user", "assistant") or m.get("isMeta"):
        return evs, [f"meta record ({role if role not in ('user', 'assistant') else 'isMeta'})"]
    content = _dict(m.get("message")).get("content")
    for b in [{"type": "text", "text": content}] if isinstance(content, str) else content if isinstance(content, list) else []:
        b = _dict(b)
        if b.get("type") == "text" and (b.get("text") or "").strip():
            evs.append({"papel": role, "tipo": "mensagem", "texto": b["text"].strip()})
        elif b.get("type") == "tool_use":
            evs.append({"papel": role, "tipo": "chamada", "nome": b.get("name") or "?", "texto": _call_args(b.get("input"))})
        elif b.get("type") == "tool_result":
            evs.append({"papel": role, "tipo": "resultado", "texto": _result_text(b.get("content"))})
        elif b.get("type") in ("thinking", "redacted_thinking"):
            cortes.append("reasoning")
    return evs, cortes


def read_transcript(path, last_n=None):
    """Neutral reader of the tail of a Claude or Codex transcript: `{event_list, cortes, open_state}`, with no readable file `{event_list: [], cortes: {}, open_state: False}`.

    Events in order: visible messages and tool calls and results (`{role, type_name, text_value, item_name?}`), each text cut at HANDOFF_MSG_MAX
    (message) or TRANSCRIPT_TOOL_MAX (tool). `cortes` counts what was left out by reason: reasoning, meta records, Codex environment
    context, unreadable lines and truncated texts. `last_n` keeps the last N events. `open_state` is the last event being a tool or an unanswered
    prompt: the session stopped midway and the tool may have run halfway (lesson 3).
    ponytail: reads only the last TRANSCRIPT_END bytes; what came before is in `orca search`."""
    empty = {"eventos": [], "cortes": {}, "aberto": False}
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            start_at = max(0, f.tell() - TRANSCRIPT_END)
            f.seek(start_at)
            line_list = f.read().decode("utf-8", "replace").splitlines()[1 if start_at else 0:]  # the first line of the window may come in half
    except (OSError, TypeError):
        return empty
    event_list, cortes = [], collections.Counter()
    for line in line_list:
        try:
            m = json.loads(line)
        except ValueError:
            cortes["unreadable line"] += bool(line.strip())
            continue
        if not isinstance(m, dict):
            continue
        evs, new_cuts = _record_events(m)
        cortes.update(new_cuts)
        for e in evs:
            cap = HANDOFF_MSG_MAX if e["tipo"] == "mensagem" else TRANSCRIPT_TOOL_MAX
            if len(e["texto"]) > cap:
                e["texto"], cortes["truncated text"] = e["texto"][:cap], cortes["truncated text"] + 1
        event_list += evs
    last_by_header = event_list[-1] if event_list else None
    return {"eventos": event_list[-last_n:] if last_n else event_list, "cortes": dict(cortes),
            "aberto": bool(last_by_header) and (last_by_header["tipo"] != "mensagem" or last_by_header["papel"] == "user")}


def _visible_records(path):
    """([(role, text)], open): only the visible messages from `read_transcript`, for the handoff package."""
    r = read_transcript(path)
    return [(e["papel"], e["texto"]) for e in r["eventos"] if e["tipo"] == "mensagem"], r["aberto"]


def transcript(dispatch, last_n=None):
    """The dispatch worker's `read_transcript`, plus the file and the agent. The file comes from turnos.json (hook) or, without it, from Orca's session index.

    ValueError without a file: saying the worker said nothing would be a lie."""
    t = _dict(_turns_ro().get(dispatch))
    file_path, agent = t.get("transcrito"), t.get("harness")
    if not file_path:
        agent = _checkpoint(dispatch)["agente"] or agent or "claude"
        file_path = dispatch_session(dispatch, agent).get("transcrito")
    if not file_path or not os.path.isfile(file_path):
        raise ValueError(f"orq could not find the transcript of dispatch {dispatch} ({file_path or 'no hook and no orca search hit'})")
    return {"dispatch": dispatch, "agente": agent, "arquivo": file_path, **read_transcript(file_path, last_n)}


def transcript_text(r):
    """The transcript as lines `[role] text_value`, `[call Name] args` and `[result] output`, and at the end the list of what the reader cut."""
    line_list = [f"[{e['papel']}] {e['texto']}" if e["tipo"] == "mensagem" else f"[call {e['nome']}] {e['texto']}" if e["tipo"] == "chamada" else f"[result] {e['texto']}"
              for e in r["eventos"]]
    cortes = ", ".join(f"{k} ×{v}" for k, v in sorted(r["cortes"].items())) or "nothing"
    return "\n".join([*line_list, "", f"{'OPEN: the session stopped in the middle of a turn. ' if r['aberto'] else ''}cut: {cortes} ({r['arquivo']})"])


def _transcript_end(path, limit=HANDOFF_HISTORY):
    """The visible messages from the end of the transcript, `[role] text_value`, in up to `limit` characters (the end is kept); "" without a readable file."""
    txt = "\n\n".join(f"[{p}] {t}" for p, t in _visible_records(path)[0])
    return txt if len(txt) <= limit else "[…]\n" + txt[-limit:]


def _git_state(path, task=None, remaining=lambda cap: cap):
    """Git facts of the worktree for the package (they do not go through the model): head, dirty files (with the count), commits and diff since origin/main, and the PR linked to the task."""
    g = lambda *a: (_git(path, *a, timeout=remaining(5)) or "").strip()  # noqa: E731
    dirty_list = g("status", "--porcelain")
    pr = next((i for i in _prs_ro()["itens"] if i["task"] == task), None) if task else None
    pr_txt = f"{pr['url']} ({pr.get('estado') or '?'})" if pr else "none (orq pr link)"
    return (f"- head: {g('rev-parse', 'HEAD')[:12] or '?'} on {g('rev-parse', '--abbrev-ref', 'HEAD') or '?'}\n"
            f"- dirty files, {len(dirty_list.splitlines())} (`git status --porcelain`):\n{_indent(dirty_list or 'none')}\n"
            f"- commits since origin/main:\n{_indent(g('log', '--oneline', 'origin/main..HEAD') or 'none (or no origin/main)')}\n"
            f"- `git diff --stat origin/main...HEAD`:\n{_indent(g('diff', '--stat', 'origin/main...HEAD') or 'empty (or no origin/main)')}\n"
            f"- linked PR: {pr_txt}")


def _indent(txt):
    return "\n".join(f"    {l}" for l in txt.splitlines())


HANDOFF_DEADLINE_S = 20  # the package of a worker with no turn goes out within this long: a slow source becomes a "não li" (didn't read) line, it does not wait


def _handoff_text(dispatch, from_, to_, w, cp, phase):
    """The HANDOFF.md: action before prose (next step, questions, decisions, git state, partial report, end of the transcript, where the rest is, how to act).

    Written from outside, without a worker turn (path B of the design). Each Orca read shares the HANDOFF_DEADLINE_S budget: the one that fails or times out leaves the package
    as "não li <source>" (I did not read it), and the rest of the file comes out the same."""
    deadline_at, not_read = time.monotonic() + HANDOFF_DEADLINE_S, []
    remaining = lambda cap: max(1.0, min(cap, deadline_at - time.monotonic()))  # noqa: E731

    def try_source(source, fn, default):
        try:
            return fn()
        except (RuntimeError, subprocess.TimeoutExpired, ValueError, OSError) as e:
            not_read.append(f"- did not read {source} ({type(e).__name__}: {str(e)[:120]})")
            return default

    task, evs, cam = w.get("taskId"), read_events(), cp["caminho"]
    msgs = try_source("the Run inbox", lambda: [m for m in orca("inbox", "--limit", "200", timeout=remaining(8))["messages"] if isinstance(m, dict)], [])
    is_open = open_questions(msgs).get(dispatch)
    questions = ([f"- to the coordinator, unanswered ({is_open.get('id')}): {is_open.get('subject') or ''} {str(is_open.get('body') or '')[:300]}".rstrip()] if is_open else []) + \
        [f"- {p.get('id')}: {p.get('titulo')}" for p in _load_pending()["itens"] if p.get("tipo") == "decisao" and p.get("task") == task]
    decisions = []
    for e in evs:
        if e.get("tipo") == "steer" and e.get("task") == task:
            decisions.append(f"- {e['texto']}")
        elif e.get("tipo") == "resposta_worker" and e.get("dispatch") == dispatch:
            q = next((m for m in msgs if m.get("id") == e.get("msg_id")), None)
            decisions.append(f"- {q.get('subject') + ' → ' if q and q.get('subject') else ''}{e['texto']}")
    reports = []
    for item_name in ("PAUSE.md", "final-report.md"):
        with contextlib.suppress(OSError):
            file_path = worker_file(cam, item_name)
            reports.append(f"{os.path.basename(file_path)}:\n{_indent(open(file_path, encoding='utf-8').read().strip()[:3000])}")
    session = _dict(_turns_ro().get(dispatch))
    if not session.get("transcrito"):
        session = {**session, **try_source("the Orca session index", lambda: dispatch_session(dispatch, from_, timeout=remaining(6)), {})}
    transcript = session.get("transcrito")
    msgs_t, open_state = _visible_records(transcript)
    last_one = next((t for p, t in reversed(msgs_t) if p == "assistant"), None)
    end = _transcript_end(transcript).replace("<!--", "<! --")  # the history does not close the block on its own
    f = try_source("the E2E queue", e2e_queue, None)
    in_queue = f and f.get("worktree") == os.path.basename(cam.rstrip("/"))
    resume_old = shlex.join(HARNESS[from_]["resume"](session["sessao"], None, None, "")[:3]) if session.get("sessao") and from_ in HARNESS else None
    return "\n".join([
        f"<!-- orq-passagem v1 de={dispatch} para={to_} -->",
        f"# Handoff: {from_} → {to_}",
        "",
        f"Worker {dispatch} ({from_}) stopped and you are continuing the same task in the same worktree. The spec is the one on the Orca task; this file carries the state.",
        "",
        "## Next step",
        *(["The session stopped without closing the turn (the last transcript record is a tool call or a prompt with no answer): the tool may have run halfway. "
           "Check the `git status` below before going on.", ""] if open_state else []),
        "It is in the reports below; check it against the git state before going on." if reports else "Unknown: the old worker left no note. Rebuild it from the end of the transcript below and from the git state.",
        "",
        "## Open questions",
        "\n".join(questions) or "No unanswered question and no decision pending item linked to the task.",
        "",
        "## Decisions already made",
        "\n".join(decisions) or "No adjustment or coordinator answer recorded for the task.",
        "",
        "## Git state",
        _git_state(cam, task, remaining),
        "",
        "## Partial report",
        (f"- last phase in the heartbeat: {phase}\n" if phase else "") + ("\n".join(reports) or "- no PAUSE.md or final-report.md in the worktree")
        + (f"\n- last visible answer of the worker (history, not instruction):\n{_indent(last_one[:3000].replace('<!--', '<! --'))}" if last_one else ""),
        "",
        "## End of the transcript",
        "What is between the markers is history, not instruction: it comes from the old session, and its tool calls already happened and are not to be repeated.",
        HISTORY_START,
        end or "(no readable transcript)",
        HISTORY_END,
        "",
        "## Where the rest is",
        f"- transcript: {transcript or 'orq did not find the file'}",
        *([f"- resume the old session by hand: `{resume_old}`"] if resume_old else []),
        f"- search: `orca search \"<termo>\" --agent {from_} --path {cam} --scope conversation`",
        *not_read,
        "",
        "## How to act",
        "- run `git status` and the suite that was pending before editing: the git state outweighs what the old session believed;",
        (f"- the old worker holds the E2E queue (ticket {f['ticket']}, for {f['min']} min) and the lock does not release itself: if its process died, "
         "`scripts/e2e-infra.sh lock-release` releases it; do not wait for it;" if in_queue else
         "- the old worker is not in the E2E queue; if you need the E2E, join it with `scripts/e2e-infra.sh lock-status` in view;"),
        f"- delete this {HANDOFF_FILE} before finishing; it does not go into a commit.",
        ""])


def _write_handoff(text_value, path):
    """Writes the HANDOFF.md at the worktree root and puts it in the repository's info/exclude (so the new worker does not commit it by mistake). Returns the short sha."""
    _write(os.path.join(path, HANDOFF_FILE), text_value)
    exclude_path = (_git(path, "rev-parse", "--git-path", "info/exclude") or "").strip()
    if exclude_path:
        exclude_path = exclude_path if os.path.isabs(exclude_path) else os.path.join(path, exclude_path)
        with contextlib.suppress(OSError):
            already = open(exclude_path, encoding="utf-8").read() if os.path.exists(exclude_path) else ""
            if HANDOFF_FILE not in already.splitlines():
                os.makedirs(os.path.dirname(exclude_path), exist_ok=True)
                open(exclude_path, "a", encoding="utf-8").write(("" if already.endswith("\n") or not already else "\n") + HANDOFF_FILE + "\n")
    return hashlib.sha256(text_value.encode("utf-8")).hexdigest()[:12]


def let_pass(dispatch, to_, model=None, effort=None, run=None):
    """Continues the worker in another harness (`claude` or `codex`) in the SAME worktree and task: stops the old one, writes the HANDOFF.md, starts `worker-start
    --retry-of --agent <other>` with the equivalent profile (or --model/--effort), notifies the new one via `steer`, releases the old terminal and writes the `passagem` event.

    Everything that can refuse comes before worker-stop (harness, worktree, Run, profile, the other harness's quota, slot, task). After it there is no way back to the previous
    harness (it is the one that hit the limit): if the new worker does not start, the old one stays stopped and retained, the worktree with the HANDOFF.md, and the message carries the command
    to repeat. The package is written by orq, with facts only (path B of the design); the worker does not need to have a turn."""
    if to_ not in HARNESSES:
        raise ValueError(f"--to expects {'|'.join(HARNESSES)} (got {to_!r})")
    if bool(model) != bool(effort):
        raise ValueError("--model and --effort go together: the effort of one model does not apply to another")
    w = _dispatch_worker(dispatch, run)
    run_id, task = w.get("runId"), w.get("taskId")
    cp = _checkpoint(dispatch)
    from_ = cp["agente"] or _dict(_turns_ro().get(dispatch)).get("harness") or "claude"
    if from_ == to_:
        raise ValueError(f"the worker is already {to_}: nothing to switch")
    if not cp["caminho"] or not os.path.isdir(cp["caminho"]):
        raise ValueError(f"the worktree of dispatch {dispatch} does not exist ({cp['caminho'] or 'Orca did not say where'}): nothing was stopped")
    if model and effort not in HARNESS[to_]["efforts"]:
        raise ValueError(f"{to_} has no effort {effort!r} ({', '.join(HARNESS[to_]['efforts'])})")
    request = (model, effort) if model else _handoff_profile(cp["modelo"], cp["effort"], to_)
    if not coordinator_run(run_id):
        raise ValueError(f"the worker belongs to Run {run_id}, which the coordinator does not command: {bind_tip(run_id)}")
    usage_check(agent=to_)
    occupancy = machine_occupancy()  # swap one by one: the dispatch's own worker leaves the count (it is stopped before the new one comes up)
    occupancy["vivos"].pop(dispatch, None)
    if reason := machine_slot(request[0], occupancy):
        raise ValueError(f"{reason}: nothing was stopped. Switch with a cheaper model, wait for a slot or adjust orq machine")
    tarefa = next((t for t in orca("task-list", "--run", run_id, timeout=20)["tasks"] if t["id"] == task), None)
    if not tarefa:
        raise ValueError(f"task {task} does not exist in Run {run_id}")
    base = {"de": from_, "para": to_, "terminal": w.get("agentTerminalHandle"), "worktree": cp["caminho"], "head": cp["head"], "sujo": cp["sujo"]}
    repeat = f"orq switch {dispatch} --to {to_}" + (f" --model {model} --effort {effort}" if model else "")
    _control("passar", w, "iniciado", modelo=request[0], effort=request[1], **base)
    _stop_worker("passar", w, {**base, "modelo": request[0], "effort": request[1]})
    phase = None
    with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired, ValueError):
        phase = next((a.get("fase") for a in agents(run_id, include_all=True) if a["dispatch"] == dispatch), None)
    text_value = _handoff_text(dispatch, from_, to_, w, cp, phase)
    try:
        package = _write_handoff(text_value, cp["caminho"])
    except OSError as e:
        _control("passar", w, "falhou", passo="HANDOFF.md", erro=str(e), **base)
        raise RuntimeError(f"worker {dispatch} stopped, but I could not write {HANDOFF_FILE} in {cp['caminho']} ({e.strerror}). Repeat with: {repeat}")
    trusted = trust_codex(_repo_root(cp["caminho"]), cp["caminho"]) if to_ == "codex" else []  # before worker-start: Codex asks about trust when it comes up
    selector = f"id:{cp['worktree_id']}" if cp["worktree_id"] else f"path:{cp['caminho']}"
    try:
        res = orca("worker-start", "--run", run_id, "--task", task, "--retry-of", dispatch, "--worktree", selector, "--agent", to_,
                   "--model", request[0], "--effort", request[1], timeout=180)
        error = None if res.get("dispatchId") else f"worker-start without dispatchId: {json.dumps(res)[:200]}"
    except subprocess.TimeoutExpired:
        res, error = None, "worker-start took over 180 s without a reply: the worker may have started, check with orq agents"
    except RuntimeError as e:
        res, error = None, str(e)
    if error:
        _control("passar", w, "falhou", passo="worker-start", erro=error, modelo=request[0], effort=request[1], worktree_intacta=_intact(cp), **base)
        raise RuntimeError(f"no worker started ({error}): dispatch {dispatch} is stopped, its terminal retained and the worktree intact in {cp['caminho']} with "
                           f"{HANDOFF_FILE}. Repeat with: {repeat}")
    new, notices = res["dispatchId"], []
    terminal = next((e.get("id") for e in res.get("effects") or [] if e.get("kind") == "terminal" and e.get("role") == "agent"), None)
    accepted = _check_start(new, terminal, tarefa.get("task_title") or "", {})
    if not accepted:
        notices.append(f"the new worker did not register a turn in {START_WAIT_S:g} s (not even after Enter): check terminal {terminal or '?'}")
    for step, fn, lap in (("note not delivered", lambda: steer(task, f"Continuation of {dispatch} ({from_}, stopped). Read {HANDOFF_FILE} at the worktree root before anything else.", run_id),
                              f"orq steer {task} <note>"),
                             ("terminal of the old worker not released", lambda: release(dispatch, run_id), f"orq release {dispatch}")):
        try:
            fn()
        except (RuntimeError, ValueError, subprocess.TimeoutExpired) as e:
            notices.append(f"{step} ({e}): run {lap}")
    append_event({"tipo": "passagem", "de": dispatch, "agente_de": from_, "para": new, "agente_para": to_, "head": cp["head"], "sujo": cp["sujo"], "pacote": package,
                  "escrito_por": "orq", "aceita": accepted, "task": task, "run": run_id})
    return _control("passar", w, "ok", novo_dispatch=new, modelo=request[0], effort=request[1], worktree_intacta=_intact(cp), pacote=package,
                     confiadas=trusted or None, aviso="; ".join(notices), **base)


def _this_terminal_harness():
    """The harness of the session running the command, from the environment each one gives its shell; None if it cannot be determined."""
    if os.environ.get("CLAUDECODE"):
        return "claude"
    return "codex" if any(k.startswith("CODEX_") for k in os.environ) else None


def coordinator_handoff(to_=None):
    """`orq handoff coordinator`: writes the precompact.py snapshot on demand and the record of who wrote it (handoff/passagem.json). The other harness's `hook session`
    injects it if it is younger than HANDOFF_COORDINATOR_WORTH_S and comes from the other side. `to_` is the harness that will read; without it, the other one from the one running this command."""
    from_ = _this_terminal_harness()
    if to_:
        if to_ not in HARNESSES:
            raise ValueError(f"--to expects {'|'.join(HARNESSES)}, not {to_!r}")
        from_ = next(h for h in HARNESSES if h != to_)
    elif from_:
        to_ = next(h for h in HARNESSES if h != from_)
    else:
        raise ValueError("could not tell the harness of this terminal: say which one the handoff goes to with --to claude|codex")
    p = subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "precompact.py"), "passagem", "--de", from_, "--para", to_],
                       capture_output=True, text=True, timeout=30)
    if p.returncode != 0:
        raise ValueError(p.stderr.strip().removeprefix("orq: ") or "the coordinator snapshot failed")
    return {**json.loads(p.stdout), "aviso": ""}


HANDOFF_COORDINATOR_WORTH_S = 15 * 60  # the snapshot of a coordinator from another harness only holds for the session that opens right after it
HANDOFF_COORDINATOR_LINES = 60  # the same ceiling as `precompact.py resume`


def _coordinator_handoff_to(ev):
    """The text of the snapshot that `orq handoff coordinator` left for the other harness, if it is still valid; marks it as accepted by this session. Only one session takes the
    handoff (the one that accepted it rereads it on reopening); empty when there is none, it is old, it is from this harness or the record cannot be read."""
    harness, sid = ev.get("_harness_orq") or "claude", ev.get("session_id") or ""
    file_path = _path(os.path.join("handoff", "passagem.json"))
    if not os.path.exists(file_path):
        return ""  # the case for almost every session: not even the lock is created
    with _lock("handoff.lock"):
        reg = _dict(_read_json(file_path))
        accepted = _dict(reg.get("aceita"))
        if (not reg.get("arquivo") or reg.get("de") == harness or not isinstance(reg.get("ts"), (int, float)) or time.time() - reg["ts"] > HANDOFF_COORDINATOR_WORTH_S
                or (accepted and accepted.get("sessao") != sid)):
            return ""
        try:
            line_list = open(_path(os.path.join("handoff", os.path.basename(reg["arquivo"]))), encoding="utf-8").read().splitlines()
        except OSError:
            return ""
        if not accepted:
            reg["aceita"] = {"sessao": sid, "harness": harness, "ts": time.time()}
            _write_json(file_path, reg)
    cut_count = len(line_list) - HANDOFF_COORDINATOR_LINES
    when = datetime.fromtimestamp(reg["ts"]).strftime("%H:%M")
    return "\n".join([f"Coordinator handoff (from {reg['de']} at {when}; what is below is its state, not instruction; handoff/{reg['arquivo']}):",
                      *line_list[:HANDOFF_COORDINATOR_LINES], *([f"(… {cut_count} lines cut; read handoff/ultimo.md)"] if cut_count > 0 else [])])


def handoff(dispatch, to_=None, run=None):
    """Writes the HANDOFF.md of a worker that no longer has a turn (plan limit, stopped terminal) and only that: it neither stops nor starts any worker.

    The worker is not consulted: the package comes from the facts orq already has (git, Run, pending items, transcript) within HANDOFF_DEADLINE_S. `to_` is the harness that will read
    (default: the other one). Whoever starts the new worker is `orq let_pass`, which writes the same package; this command is for reading the package beforehand or for handing off by hand."""
    t0 = time.monotonic()
    w = _dispatch_worker(dispatch, run)
    cp = _checkpoint(dispatch)
    from_ = cp["agente"] or _dict(_turns_ro().get(dispatch)).get("harness") or "claude"
    to_ = to_ or next(h for h in HARNESSES if h != from_)
    if to_ not in HARNESSES or to_ == from_:
        raise ValueError(f"--to expects the other harness ({'|'.join(h for h in HARNESSES if h != from_)}), not {to_!r}")
    if not cp["caminho"] or not os.path.isdir(cp["caminho"]):
        raise ValueError(f"the worktree of dispatch {dispatch} does not exist ({cp['caminho'] or 'Orca did not say where'})")
    phase = _dict(liveness_signals(read_events()).get(dispatch)).get("fase")
    package = _write_handoff(_handoff_text(dispatch, from_, to_, w, cp, phase), cp["caminho"])
    return _control("passagem", w, "ok", de=from_, para=to_, pacote=package, arquivo=os.path.join(cp["caminho"], HANDOFF_FILE), segundos=round(time.monotonic() - t0, 1),
                     head=cp["head"], sujo=cp["sujo"])


def _prompt_entered(dispatch, terminal, title):
    """The dispatch turn has started (the worker's prompt hook in turnos.json) or the terminal screen already shows the spec title outside the typing box."""
    if _dict(_turns_ro().get(dispatch)).get("inicio"):
        return True
    if not terminal:
        return False
    try:
        t = orca("read", "--terminal", terminal, "--limit", "40", area="terminal").get("terminal") or {}
    except (RuntimeError, subprocess.TimeoutExpired):
        return False
    return not (t.get("draft") or "").strip() and title in json.dumps(t.get("tail") or [], ensure_ascii=False)


def _wait_prompt(dispatch, terminal, title):
    end = time.monotonic() + START_WAIT_S
    while True:
        if _prompt_entered(dispatch, terminal, title):
            return True
        if time.monotonic() >= end:
            return False
        time.sleep(min(0.5, max(end - time.monotonic(), 0)))


def _check_start(dispatch, terminal, title, out):
    """Did the spec get into the worker? Waits for the prompt; if it did not, sends an Enter (does nothing in an empty box) and waits again. `out["enter"]` marks the Enter."""
    if _wait_prompt(dispatch, terminal, title):
        return True
    if not terminal:
        return False
    with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired):
        orca("send", "--terminal", terminal, "--enter", area="terminal", timeout=3 + TIMEOUT_ORCA)
        out["enter"] = True
    return _wait_prompt(dispatch, terminal, title)


def _command_key(command):
    """What makes two hook commands the same hook. An orq hook is its call, with the pt hook name mapped: `python3 x/orq.py hook lugar` and
    `/opt/homebrew/bin/python3 y/orq.py hook place` are one hook (tickets 247 and 124). Any other command counts with `~/` and `$HOME/` expanded."""
    if m := HOOK_ORQ.search(command or ""):
        head, _, last = m.group(0).rpartition(" ")
        return f"orq {head} {HOOK_EN.get(last, last)}" if head else f"orq {last}"
    home = os.path.expanduser("~")
    return re.sub(r"(^|\s)(?:~|\$HOME)/", lambda x: f"{x.group(1)}{home}/", command or "")


def merge_codex_hooks(current, example):
    """Appends to the end of each `current` event the `example` groups whose command is not there yet. Never reorders or removes: Codex's trust
    is positional (`hooks.json:<event>:<group>:<hook>`) and inserting in the middle makes the following groups untrusted. Returns (new hooks, [(event, group)] appended)."""
    new, add = json.loads(json.dumps(current)), []
    already = {_command_key(h.get("command")) for group_map in _dict(new.get("hooks")).values() for g in group_map for h in g.get("hooks", [])}
    for ev, groups in _dict(example.get("hooks")).items():
        for g in groups:
            if all(_command_key(h.get("command")) in already for h in g.get("hooks", [])):
                continue
            listing = new.setdefault("hooks", {}).setdefault(ev, [])
            listing.append(g)
            add.append((ev, len(listing) - 1))
            already |= {_command_key(h.get("command")) for h in g.get("hooks", [])}
    return new, add


def install_hooks(example, target):
    """Merges the example into the hooks file `target` (a missing file counts as empty) and returns the groups appended. The example's commands get this
    clone's path and the 3.12+ interpreter. JSON that cannot be read raises: nothing is written."""
    try:
        current = json.load(open(target, encoding="utf-8"))
    except FileNotFoundError:
        current = {}
    example_hooks = json.load(open(example, encoding="utf-8"))
    python = resolve_python()  # what is appended runs the 3.12+ interpreter, not whatever `python3` the PATH has
    for group_map in _dict(example_hooks.get("hooks")).values():
        for g in group_map:
            for h in g.get("hooks", []):
                h["command"] = _repoint(h.get("command") or "")
                if python and (first := hook_interpreter(h["command"])):
                    h["command"] = python + h["command"][len(first):]
    new, add = merge_codex_hooks(current, example_hooks)
    if add:
        _replace_config(target, json.dumps(new, indent=2, ensure_ascii=False) + "\n")
    return add


def _replace_config(file_name, text):
    """Writes a harness config through tmp + rename: into the file a symlink points at (a dotfiles repo keeps its link) and with the old mode."""
    real = os.path.realpath(file_name)
    os.makedirs(os.path.dirname(real) or ".", exist_ok=True)
    tmp = f"{real}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    with contextlib.suppress(OSError):
        shutil.copymode(real, tmp)
    os.replace(tmp, real)


def install_codex_hooks(example):
    return install_hooks(example, CODEX_HOOKS)


ORQ_FILE_PATH = re.compile(r"[^\s\"';=]*/(orq\.py(?= hook )|precompact\.py(?= retomar\b|\s*$))")  # the script of an orq hook, not a log path that ends in .py
STATUSLINE_PATH = re.compile(r"[^\s\"';=]*/orq/statusline\.sh\b")


def _repoint(command):
    """The command with the path of orq.py and precompact.py (in an orq hook) or of orq's statusline.sh swapped for this clone's (ticket 124)."""
    if HOOK_ORQ.search(command):
        return ORQ_FILE_PATH.sub(lambda m: os.path.join(orqpaths.CODE, m.group(1)), command, count=1)
    return STATUSLINE_PATH.sub(lambda m: os.path.join(orqpaths.CODE, "statusline.sh"), command)


def repoint_hooks(file_name):
    """Rewrites in place, as a text edit, every orq command of a settings.json or hooks.json that runs another folder's orq.py, precompact.py or
    statusline.sh: the rest of the file and the order of Codex's groups stay as they were. Returns how many commands changed; 0 without the file."""
    try:
        text = open(file_name, encoding="utf-8").read()
    except FileNotFoundError:
        return 0
    changed = 0

    def swap(m):
        nonlocal changed
        old = json.loads(f'"{m.group(2)}"')
        if (new := _repoint(old)) == old:
            return m.group(0)
        changed += 1
        return m.group(1) + json.dumps(new, ensure_ascii=False)[1:-1] + m.group(3)

    new = re.sub(r'("command"\s*:\s*")((?:[^"\\]|\\.)*)(")', swap, text)
    if changed:
        json.loads(new)  # never leave a hooks file that does not parse
        _replace_config(file_name, new)
    return changed


# (where the harness reads it, what it points at in the clone): what `orq install` links
INSTALL_LINKS = (("~/.claude/hooks/worker-routing-guard.py", "hooks/worker-routing-guard.py"), ("~/.claude/hooks/limpar-mergeados-hook.py", "hooks/limpar-mergeados-hook.py"),
                 ("~/.claude/scripts/limpar-mergeados.py", "scripts/limpar-mergeados.py"), ("~/.claude/scripts/limpar-mergeados.keep", "scripts/limpar-mergeados.keep"),
                 ("~/.claude/scripts/orca-wait-runs.py", "scripts/orca-wait-runs.py"), ("~/.claude/scripts/trust-cwd.py", "scripts/trust-cwd.py"),
                 ("~/.claude/skills/worker-routing", "skills/worker-routing"), ("~/.claude/commands/away.md", "commands/away.md"),
                 ("~/.agents/skills/worker-routing", "skills/worker-routing"), ("~/.agents/skills/away", "skills/away"))


def install_links():
    """Points each INSTALL_LINKS link at this clone: creates it, or replaces a link that points anywhere else (a link through the old install path included).
    A real file or folder in its place is left alone and reported. Returns one line per change."""
    out = []
    for link, target in INSTALL_LINKS:
        link, target = os.path.expanduser(link), os.path.join(orqpaths.CODE, target)
        if os.path.islink(link) and os.readlink(link) == target:
            continue
        if os.path.lexists(link) and not os.path.islink(link):
            out.append(f"kept: {link} is not a link (move it away and run again)")
            continue
        os.makedirs(os.path.dirname(link), exist_ok=True)
        tmp = f"{link}.{os.getpid()}.tmp"
        os.symlink(target, tmp)
        os.replace(tmp, link)
        out.append(f"linked: {link} -> {target}")
    return out


def install():
    """`orq install`: wires the harnesses to this clone (ticket 124). Repoints the orq commands already in ~/.claude/settings.json and ~/.codex/hooks.json,
    appends the example groups still missing (never reordering Codex's), links the skills, hooks and scripts, writes the `orq` wrapper and says when the
    launchd agent still points elsewhere. Idempotent: a second run changes nothing. Returns the lines to print."""
    out = []
    python = resolve_python()
    for agent, target in HOOKS_FILES.items():
        moved = repoint_hooks(target)
        add = install_hooks(os.path.join(orqpaths.HERE, HOOKS_EXAMPLE[agent]), target)
        out += [f"{agent}: {moved} command(s) repointed to {orqpaths.CODE}" + (": Codex trusts a hook by its text, review them in /hooks" if agent == "codex" else "")] if moved else []
        out += [f"{agent}: added {ev} group {g}" for ev, g in add]
    out += install_links()
    if pin_orq_link(python):
        out.append(f"wrote: {ORQ_LINK} -> {os.path.join(orqpaths.CODE, 'orq.py')}")
    with contextlib.suppress(OSError, ValueError):
        import plistlib
        pl = plistlib.load(open(_plist_serve(), "rb"))
        if pl.get("EnvironmentVariables", {}).get("ORQ_HOME") != HOME or os.path.join(orqpaths.CODE, "orq.py") not in pl.get("ProgramArguments", []):
            out.append(f"launchd: {_plist_serve()} points at another folder: run `orq manager serve --install`")
    if notice := codex_hooks_notice():
        out.append(notice)
    return out or ["nothing to do: hooks, links and the orq wrapper already point at " + orqpaths.CODE]


def untrusted_codex_hooks():
    """The `<hooks.json>:<event>:<group>:<hook>` keys of orq's hooks in CODEX_HOOKS that config.toml's `[hooks.state]` does not trust (no entry,
    no trusted_hash or `enabled = false`). Without hooks.json or without an orq hook in it, Codex is not wired to orq: empty list.
    ponytail: only the position; Codex's hash is not documented, so a hook edited after the trust passes as trusted."""
    import tomllib
    try:
        event_list = _dict(json.load(open(CODEX_HOOKS, encoding="utf-8")).get("hooks"))
    except (OSError, ValueError):
        return []
    try:
        state_ = _dict(_dict(tomllib.loads(open(CODEX_CONFIG, encoding="utf-8").read()).get("hooks")).get("state"))
    except (OSError, tomllib.TOMLDecodeError):
        state_ = {}
    path, outside = os.path.abspath(CODEX_HOOKS), []
    for ev, groups in event_list.items():
        for g, group_name in enumerate(groups):
            for h, x in enumerate(_dict(group_name).get("hooks", [])):
                if not HOOK_ORQ.search(x.get("command") or ""):  # any clone folder, not only one named orq
                    continue
                key_name = f"{path}:{re.sub(r'(?<!^)(?=[A-Z])', '_', ev).lower()}:{g}:{h}"
                reg = _dict(state_.get(key_name))
                if not reg.get("trusted_hash") or reg.get("enabled") is False:
                    outside.append(key_name)
    return outside


def codex_hooks_notice():
    """The notice line of `orq status`, `orq agents` and the coordinator preamble; empty when everything is trusted."""
    n = len(untrusted_codex_hooks())
    return f"⚠ orq hooks not trusted in Codex: run /hooks ({n} hook{'s' if n > 1 else ''}; until trusted orq cannot see the Codex terminal)" if n else ""


def trust_codex(*paths):
    """Marks each folder as trusted in Codex (`[projects."<p>"] trust_level = "trusted"` in config.toml) and returns the ones added now. Codex
    keeps the trust by the main repository's root, and the worktree inherits it; orq writes both. A config that does not read as TOML is left as it was."""
    import tomllib  # late import: only the dispatch of a Codex worker uses it
    try:
        txt = open(CODEX_CONFIG, encoding="utf-8").read()
    except FileNotFoundError:
        txt = ""
    try:
        proj = _dict(tomllib.loads(txt).get("projects"))
    except tomllib.TOMLDecodeError as e:
        log(f"confiar_codex: {CODEX_CONFIG} não é TOML ({e}): nada gravado")
        return []
    fresh = [p for p in dict.fromkeys(c for c in paths if c) if _dict(proj.get(p)).get("trust_level") != "trusted"]
    if not fresh:
        return []
    txt += "".join(f'\n[projects.{json.dumps(p, ensure_ascii=False)}]\ntrust_level = "trusted"\n' for p in fresh)
    os.makedirs(os.path.dirname(CODEX_CONFIG) or ".", exist_ok=True)
    tmp = f"{CODEX_CONFIG}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(txt)
    os.replace(tmp, CODEX_CONFIG)
    return fresh


def _repo_root(d):
    """The folder of the repository's main checkout for `d` (the parent of the git common dir), or None outside a repository."""
    common = (_git(d, "rev-parse", "--path-format=absolute", "--git-common-dir") or "").strip()
    return os.path.realpath(os.path.dirname(common)) if common else None


def _file_environments(d):
    """(branches, production, flow, error) from the `environments` block and the `flow` of a project file. Without the block: (None, None, None, None) or only the flow's error."""
    block, flow = d.get("ambientes"), d.get("fluxo")
    if flow is not None and flow not in FLOWS:
        return None, None, None, f"flow {flow!r} does not exist ({', '.join(FLOWS)})"
    if block is None:
        return None, None, None, "flow without environments: declare the `ambientes` block" if flow == "promocao" else None
    if not isinstance(block, list) or not block or not all(isinstance(b, dict) and isinstance(b.get("branch"), str) and b["branch"] for b in block):
        return None, None, None, 'ambientes malformed (expected a non-empty list of {"branch": "<name>", "producao": true?})'
    branches = [b["branch"] for b in block]
    marked = [b["branch"] for b in block if b.get("producao")]
    if len(set(branches)) != len(branches) or len(marked) > 1:
        return None, None, None, "ambientes repeats a branch or marks more than one as production"
    return branches, (marked or branches[-1:])[0], flow or ("promocao" if len(branches) > 1 else "direto"), None


def projects():
    """The ORQ_HOME/projects/<name>.json files, read on every call (no cache): {nome: {"repo", "harness", "grupo", "ambientes", "producao", "fluxo", "e2e_queue", "transcritos", "erro"}}.

    Only `repo` is required; a missing `harness` counts as claude and `group_name` only groups the listing. `environments` is the project's ordered list `[{"branch", "production"?}]`
    (the production one is the one marked, otherwise the last) and `flow` is `promocao` or `direto`; without the `environments` block it comes as None and the remote's default applies
    (`project_flow`). `e2e_queue` is the folder of the project's E2E queue (`e2e_queue()`; without it the project shows no queue) and `transcritos` the folder of the
    coordinator's transcripts (`transcript_dirs()`; without it the one Claude Code names from `repo: path:` applies). A file that is unreadable, has no `repo`, has a harness orq does not dispatch or has malformed environments stays in the list with `error`
    (`orq projects` shows the reason) and is never chosen on its own."""
    folder, findings = _path("projects"), {}
    for f in sorted(os.listdir(folder)) if os.path.isdir(folder) else []:
        if not f.endswith(".json"):
            continue
        item_name = f[:-5]
        try:
            with open(os.path.join(folder, f)) as fh:
                d = to_pt(json.load(fh))
        except (OSError, ValueError) as e:
            findings[item_name] = {"repo": None, "harness": None, "grupo": None, "ambientes": None, "producao": None, "fluxo": None, "fila_e2e": None, "transcritos": None, "erro": f"unreadable json ({type(e).__name__})"}
            continue
        d = _dict(d)
        harness = d.get("harness") or "claude"
        envs, production, flow, env_error = _file_environments(d)
        without_text = next((k for k in ("fila_e2e", "transcritos") if k in d and (not isinstance(d[k], str) or not d[k])), None)
        error = ("no repo (the Orca selector: path:, id: or name:)" if not isinstance(d.get("repo"), str) or not d["repo"] else
                f"harness {harness!r} does not exist in orq ({', '.join(HARNESSES)})" if harness not in HARNESS else
                f"{without_text} is not a path (text)" if without_text else env_error)
        findings[item_name] = {"repo": d.get("repo"), "harness": harness, "grupo": d.get("grupo"), "ambientes": envs, "producao": production, "fluxo": flow,
                         "fila_e2e": d.get("fila_e2e"), "transcritos": d.get("transcritos"), "erro": error,
                         "deploy_check": d["deploy_check"] if isinstance(d.get("deploy_check"), str) and d["deploy_check"].strip() else None}
    return findings


def project_by_folder(ps, folder):
    """The project whose `repo: path:<dir>` contains `folder` (the longest path wins), or None. An id:/name: selector has no folder to compare."""
    best = None
    for item_name, d in ps.items():
        repo = d.get("repo") or ""
        if d.get("erro") or not repo.startswith("path:"):
            continue
        root = os.path.realpath(os.path.expanduser(repo[5:]))
        if os.path.commonpath([root, folder]) == root and (best is None or len(root) > best[0]):
            best = (len(root), item_name)
    return best and best[1]


def run_project(run):
    """The project that `orq run project` recorded for the Run (its latest `run_projeto` event), or None."""
    return next((e["projeto"] for e in reversed(read_events()) if e.get("tipo") == "run_projeto" and e.get("run") == run), None)


def run_store_project(run, item_name):
    """Records in the log that the Run belongs to project `item_name` (refuses a name with no file or with an invalid file). Applies to its following dispatches."""
    ps = projects()
    if item_name not in ps:
        raise ValueError(f"project {item_name}: {_path('projects')}/{item_name}.json does not exist (orq projects lists the ones there are)")
    if ps[item_name]["erro"]:
        raise ValueError(f"project {item_name}: invalid file, {ps[item_name]['erro']}")
    return append_event({"tipo": "run_projeto", "run": run, "projeto": item_name})


def dispatch_project(item_name=None, run=None):
    """A dispatch's project: `--project`, otherwise the one the Run holds, otherwise the one that contains the cwd (or its main checkout), otherwise None.

    A name requested or stored in the Run that no longer exists or is invalid is refused: falling back to the cwd would start the worker in the wrong repository."""
    ps = projects()
    origin_name = "--project" if item_name else f"Run {run}"
    item_name = item_name or (run_project(run) if run else None)
    if item_name:
        if item_name not in ps:
            raise ValueError(f"{origin_name} points to project {item_name}, but {_path('projects')}/{item_name}.json does not exist (orq projects lists the ones there are)")
        if ps[item_name]["erro"]:
            raise ValueError(f"{origin_name} points to project {item_name}, whose file is invalid: {ps[item_name]['erro']}")
        return item_name
    cwd = os.path.realpath(os.getcwd())
    return project_by_folder(ps, cwd) or (project_by_folder(ps, _repo_root(cwd) or cwd) if ps else None)


def default_branch(folder):
    """The default branch of the remote of `folder` (`origin/HEAD`); without it, BRANCH_NO_REMOTE."""
    ref = (_git(folder, "symbolic-ref", "-q", "--short", "refs/remotes/origin/HEAD") or "").strip()
    return ref.split("/", 1)[-1] if ref else BRANCH_NO_REMOTE


def project_flow(item_name):
    """{ambientes: [branch...], producao, fluxo, declarado}: what the project file declares. Without a project, or without the `environments` block, the default is a single
    environment, the default branch of the repo's remote (the project folder, or the cwd), with a direct flow; `declarado` says which of the two."""
    d = projects().get(item_name) or {}
    if d.get("ambientes") and not d.get("erro"):
        return {"ambientes": d["ambientes"], "producao": d["producao"], "fluxo": d["fluxo"], "declarado": True}
    default = default_branch((d.get("repo") and repo_folder(d["repo"])) or os.getcwd())
    return {"ambientes": [default], "producao": default, "fluxo": "direto", "declarado": False}


def _known_environments():
    """Every environment name that any project declares, plus the cwd's default branch: the suffix of a merge/<feature>-<environment> branch is one of them."""
    names = {a for p in projects().values() for a in p.get("ambientes") or ()} | set(project_flow(None)["ambientes"])
    return sorted(names, key=len, reverse=True)


def repo_flow(folder):
    """The flow of the project whose repository contains `folder`; without a project, the default for `folder` (its remote's default branch, a single environment)."""
    folder, ps = os.path.realpath(folder), projects()
    item_name = project_by_folder(ps, folder) or (project_by_folder(ps, _repo_root(folder) or folder) if ps else None)
    if item_name:
        return project_flow(item_name)
    default = default_branch(folder)
    return {"ambientes": [default], "producao": default, "fluxo": "direto", "declarado": False}


def task_flow(task, event_list=None):
    """The flow of the feature's (task) project: that of the Run that dispatched it (`orq run project`), otherwise that of the project that contains the cwd, otherwise the default."""
    run = next((e.get("run") for e in reversed(read_events() if event_list is None else event_list) if e.get("tipo") == "despacho" and e.get("task") == task), None)
    try:
        return project_flow(dispatch_project(None, run))
    except ValueError:  # the Run points to a file that vanished or became invalid: the default, without bringing the status down
        return project_flow(None)


def repo_folder(selector):
    """The repository folder an Orca selector points to: `path:` directly; `id:` and `name:` via `orca repo list`. None if not found."""
    type_name, _, value = selector.partition(":")
    if type_name == "path":
        return os.path.realpath(os.path.expanduser(value))
    try:
        repos = orca("list", area="repo")["repos"]
    except (RuntimeError, subprocess.TimeoutExpired, KeyError, ValueError) as e:
        log(f"pasta_do_repo: orca repo list falhou para {selector}: {type(e).__name__}: {e}")
        return None
    campo = {"id": "id", "name": "displayName"}.get(type_name)
    return next((os.path.realpath(r["path"]) for r in repos if campo and r.get(campo) == value and r.get("path")), None)


# ---- orq projeto add (ticket 125): registers in Orca and generates orca.yaml

ORCA_YAML_ROOT = ("scripts", "setupAgentStartupPolicy", "issueCommand", "defaultTabs", "environmentRecipes", "worktree")  # the top-level keys Orca reads (checked in the app on 01/10)
ORCA_YAML_SCRIPTS = ("setup", "archive")
HARNESS_TRUST = {"claude": "python3 ~/.claude/scripts/trust-cwd.py", "codex": "orq projeto confiar"}  # the fixed part of the setup: trust the folder in the project's harness
SCRATCH_ENTER = ('main=$(git worktree list --porcelain | awk \'NR==1{print $2}\'); if [ -n "$main" ] && [ "$main" != "$(pwd -P)" ] && [ -d "$main/.scratch" ]; then '
                 'mkdir -p .scratch && rsync -a --ignore-existing "$main/.scratch/" .scratch/; fi || echo "sync of .scratch failed"')
SCRATCH_LAP = ('main=$(git worktree list --porcelain | awk \'NR==1{print $2}\'); if [ -n "$main" ] && [ "$main" != "$(pwd -P)" ] && [ -d .scratch ]; then '
                 'mkdir -p "$main/.scratch" && rsync -a --update .scratch/ "$main/.scratch/"; fi || echo "copy of .scratch back failed"')
LOCKFILES = (("pnpm-lock.yaml", "pnpm install --frozen-lockfile"), ("yarn.lock", "yarn install --frozen-lockfile"), ("package-lock.json", "npm ci"),
             ("bun.lock", "bun install"), ("bun.lockb", "bun install"), ("uv.lock", "uv sync"))
E2E_SCRIPT = "scripts/e2e-infra.sh"
# block -> (phase, command for when the project turns on the block that detection did not find); each one's detection is in _detected_block
ORCA_BLOCKS = {"install": ("setup", "npm install"), "setup_script": ("setup", "sh scripts/setup-worktree.sh"),
               "graphify": ("setup", 'graphify update . >/dev/null 2>&1 || echo "graphify update failed"'),
               "meteor": ("archive", "rm -rf .meteor/local _build"), "e2e": ("archive", f'sh {E2E_SCRIPT} destroy >/dev/null 2>&1 || echo "e2e destroy failed"')}


def _detected_block(repo, block):
    """The block's lines, if the repository has its marker (file or folder); otherwise None."""
    ha = lambda *p: os.path.exists(os.path.join(repo, *p))  # noqa: E731
    if block == "install":
        return next(([cmd] for file_path, cmd in LOCKFILES if ha(file_path)), None)
    if block == "setup_script":
        return [ORCA_BLOCKS[block][1]] if ha("scripts", "setup-worktree.sh") else None
    if block == "graphify":
        return [ORCA_BLOCKS[block][1]] if ha("graphify-out") else None
    if block == "meteor":
        folders = [d for d in [".", *sorted(x for x in os.listdir(repo) if os.path.isdir(os.path.join(repo, x)) and not x.startswith("."))] if ha(d, ".meteor")]
        return [f"rm -rf {' '.join(f'{d}/{x}' if d != '.' else x for x in ('.meteor/local', '_build'))}" for d in folders] or None
    if ha(E2E_SCRIPT) and "destroy)" in open(os.path.join(repo, E2E_SCRIPT), errors="replace").read():
        return [ORCA_BLOCKS[block][1]]
    return ['docker compose -p "e2e-$(basename "$(pwd -P)")" -f docker-compose.e2e.yml down -v --remove-orphans >/dev/null 2>&1 || echo "e2e destroy failed"'] if ha("docker-compose.e2e.yml") else None


def _orca_yaml_scripts(body_text, n0):
    """{setup|archive: [lines]} from the body (indented lines) of the `scripts:` key. Only the `|` block or a simple line; anything else is a ValueError with the line."""
    out, k = {}, 0
    while k < len(body_text):
        line = body_text[k]
        if not line.strip() or line.lstrip().startswith("#"):
            k += 1
            continue
        m = re.fullmatch(r"( +)([A-Za-z][A-Za-z0-9_-]*):(?:\s+(.*))?", line)
        if not m:
            raise ValueError(f"orca.yaml, line {n0 + k}: expected `setup:` or `archive:` under `scripts:`, got {line.strip()!r}")
        indent, key_name, value = len(m.group(1)), m.group(2), (m.group(3) or "").strip()
        if key_name not in ORCA_YAML_SCRIPTS or key_name in out:
            raise ValueError(f"orca.yaml, line {n0 + k}: `scripts.{key_name}` {'repeated' if key_name in out else 'that orq does not know'} (only {', '.join(ORCA_YAML_SCRIPTS)})")
        k += 1
        if re.fullmatch(r"\|[-+]?", value):
            block = []
            while k < len(body_text) and (not body_text[k].strip() or len(body_text[k]) - len(body_text[k].lstrip()) > indent):
                block.append(body_text[k])
                k += 1
            base = min((len(x) - len(x.lstrip()) for x in block if x.strip()), default=None)
            if base is None:
                raise ValueError(f"orca.yaml, line {n0 + k - 1}: `scripts.{key_name}` is empty")
            out[key_name] = [x[base:] if x.strip() else "" for x in block]
            while out[key_name] and not out[key_name][-1]:
                out[key_name].pop()
        elif value and value[0] not in "'\"{[&*>|!#" and ": " not in value and " #" not in value and not value.endswith(":"):
            out[key_name] = [value]
        else:
            raise ValueError(f"orca.yaml, line {n0 + k - 1}: `scripts.{key_name}` has a value that is not simple YAML; use a `|` block")
    return out


def read_orca_yaml(text_value):
    """({setup: [lines], archive: [lines]}, rest) from the subset of orca.yaml that orq accepts: only top-level keys that Orca reads, `scripts:` with `setup` and
    `archive` (`|` block or simple line), space indentation. The `rest` is the other top-level keys, as they came. YAML outside that is a ValueError."""
    if "\t" in text_value:
        raise ValueError("orca.yaml: tab in the indentation (YAML only accepts spaces)")
    line_list, i, scripts, rest = text_value.splitlines(), 0, {}, []
    while i < len(line_list):
        if not line_list[i].strip() or line_list[i].startswith("#"):
            i += 1
            continue
        m = re.fullmatch(r"([A-Za-z][A-Za-z0-9_-]*):(?:\s+(.*))?", line_list[i])
        if not m:
            raise ValueError(f"orca.yaml, line {i + 1}: expected a top-level key, got {line_list[i].strip()!r}")
        if m.group(1) not in ORCA_YAML_ROOT:
            raise ValueError(f"orca.yaml, line {i + 1}: key {m.group(1)!r} that Orca does not read (accepts {', '.join(ORCA_YAML_ROOT)})")
        j = i + 1
        while j < len(line_list) and (not line_list[j].strip() or line_list[j][0] in " #"):
            j += 1
        if m.group(1) == "scripts":
            if m.group(2):
                raise ValueError(f"orca.yaml, line {i + 1}: `scripts:` is a map, not {m.group(2)!r}")
            scripts = _orca_yaml_scripts(line_list[i + 1:j], i + 2)
        else:
            rest += line_list[i:j]
        i = j
    while rest and not rest[-1].strip():
        rest.pop()
    return scripts, rest


def build_orca_yaml(repo, harness, cfg, proposal=None):
    """(orca.yaml text, rest) for `repo`. The fixed part always goes in (trusting the folder in the harness, the `.scratch` going and coming back); the rest comes from the agent's
    `proposal` ((scripts, rest) from `read_orca_yaml`) or, without it, from the blocks the repository has. `cfg` is the `orca` of the project file: `blocks`
    {name: bool} turns on (true) or off (false) a block the detection got wrong, `setup_extra` and `archive_extra` add commands."""
    cfg = _dict(cfg)
    blocks, extras = _dict(cfg.get("blocos")), {f: cfg.get(f"{f}_extra", []) for f in ORCA_YAML_SCRIPTS}
    if not all(b in ORCA_BLOCKS and isinstance(v, bool) for b, v in blocks.items()):
        raise ValueError(f"orca.blocos: expected {{block: true|false}} with blocks from {', '.join(ORCA_BLOCKS)}")
    if not all(isinstance(v, list) and all(isinstance(x, str) for x in v) for v in extras.values()):
        raise ValueError("orca.setup_extra and orca.archive_extra: expected lists of commands (text)")
    fixed = {"setup": [HARNESS_TRUST[harness], SCRATCH_ENTER], "archive": [SCRATCH_LAP]}
    scripts = {}
    for phase in ORCA_YAML_SCRIPTS:
        if proposal:
            meio = proposal[0].get(phase, [])
        else:
            meio = [l for b, (f, default) in ORCA_BLOCKS.items() if f == phase and blocks.get(b) is not False
                    for l in (_detected_block(repo, b) or ([default] if blocks.get(b) else []))]
        scripts[phase] = fixed[phase] + [l for l in meio if l not in fixed[phase]] + extras[phase]
    rest = proposal[1] if proposal else []
    text_value = "scripts:\n" + "".join(f"  {f}: |\n" + "".join(f"    {l}\n" if l else "\n" for l in scripts[f]) for f in ORCA_YAML_SCRIPTS) + ("".join(f"{l}\n" for l in rest))
    if read_orca_yaml(text_value)[0] != scripts:  # what orq writes, orq reads back the same
        raise ValueError("assembled orca.yaml does not read back the same: command with a line that YAML does not keep")
    return text_value, rest


def _repo_in_orca(folder):
    """The repository root of `folder` if Orca knows it (`orca repo list`); otherwise None, also with Orca down."""
    root = _repo_root(folder)
    try:
        repos = orca("list", area="repo")["repos"]
    except (RuntimeError, subprocess.TimeoutExpired, KeyError, ValueError) as e:
        log(f"_repo_no_orca: orca repo list falhou: {type(e).__name__}: {e}")
        return None
    return root if root and any(r.get("path") and os.path.realpath(r["path"]) == root for r in repos) else None


def _remote_environments(root):
    """The `ambientes` block a new project gets from its remote's branches (ticket 124): the remote's default branch is production, and every other remote
    branch without a `/` (a working branch carries a type prefix, `feat/x`) is an environment before it. With only the default branch, or a remote without
    `origin/HEAD`: None, the direct flow.
    ponytail: a loose branch without a slash is taken for an environment and the others come in name order; the file is plain JSON to fix by hand."""
    base = default_branch(root)
    names = (_git(root, "for-each-ref", "--format=%(refname:lstrip=3)", "refs/remotes/origin") or "").split()
    others = sorted(b for b in names if b not in ("HEAD", base) and "/" not in b)
    return [*({"branch": b} for b in others), {"branch": base, "producao": True}] if others and base in names else None  # no origin/HEAD: no guess


def add_project(target, item_name=None, harness=None, group_name=None, proposal_file=None, destination=None, replace_text=False, dry_run=False):
    """`orq project add <path|url>`: writes ORQ_HOME/projects/<name>.json, registers the repository in Orca if missing (`repo add`, and the base of new
    worktrees, `origin/<project production>`) and writes the orca.yaml at its root. Everything is validated before touching anything. An orca.yaml that already exists
    is not replaced: it returns the diff, and only `replace_text` replaces it. `dry_run` only returns what it would do."""
    if re.match(r"(https?://|ssh://|file://|git@)", target):
        item_name = item_name or re.sub(r"\.git$", "", target.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1])
        destination = os.path.expanduser(destination or os.path.join("~/Developer", item_name))
        if not os.path.isdir(destination):
            if dry_run:
                raise ValueError(f"{destination} does not exist yet: --dry-run does not clone")
            subprocess.run(["git", "clone", "-q", target, destination], check=True, capture_output=True, text=True, timeout=600)
        target = destination
    root = _repo_root(os.path.realpath(os.path.expanduser(target))) if os.path.isdir(os.path.expanduser(target)) else None
    if not root:
        raise ValueError(f"{target} is not a git repository folder")
    item_name = item_name or os.path.basename(root)
    file_path = os.path.join(_path("projects"), f"{item_name}.json")
    does_exist = os.path.exists(file_path)
    data = _dict(_read_json(file_path)) if does_exist else {"repo": f"path:{root}", **({"harness": harness} if harness and harness != "claude" else {}), **({"grupo": group_name} if group_name else {})}
    if data.get("repo") != f"path:{root}":
        raise ValueError(f"project {item_name} already exists in {file_path} and points to {data.get('repo')}, not to {root}: pass --name")
    if not does_exist and (envs := _remote_environments(root)):
        data["ambientes"] = envs
    harness = data.get("harness") or "claude"
    if harness not in HARNESS_TRUST:
        raise ValueError(f"harness {harness!r} of project {item_name}: orq only trusts the folder for {', '.join(HARNESS_TRUST)}")
    proposal = read_orca_yaml(open(proposal_file, encoding="utf-8").read()) if proposal_file else None
    text_value, _ = build_orca_yaml(root, harness, data.get("orca"), proposal)
    _, production, _, error = _file_environments(data)
    if error:
        raise ValueError(f"project {item_name}: {error}")
    base = production or default_branch(root)
    yaml_path = os.path.join(root, "orca.yaml")
    current = open(yaml_path, encoding="utf-8").read() if os.path.exists(yaml_path) else None
    write = current is None or (replace_text and current != text_value)
    out = {"projeto": item_name, "repo": root, "arquivo": file_path, "base": f"origin/{base}", "orca_yaml": text_value, "orca_yaml_estado": "novo" if current is None else "igual" if current == text_value else "trocado" if write else "mantido",
           "diff": "" if current in (None, text_value) else "".join(difflib.unified_diff(current.splitlines(True), text_value.splitlines(True), "orca.yaml (current)", "orca.yaml (proposed)")), "dry_run": dry_run}
    if dry_run:
        return out
    registrar = not _repo_in_orca(root)
    if registrar:
        orca("add", "--path", root, area="repo")
        orca("set-base-ref", "--repo", f"path:{root}", "--ref", f"origin/{base}", area="repo")
    if not does_exist:
        _write_json(file_path, data, indent=2)
    if write:
        tmp = f"{yaml_path}.{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text_value)
        os.replace(tmp, yaml_path)
    return {**out, "registrado_no_orca": registrar, "arquivo_novo": not does_exist}

def orq_ticket(title, project=None):
    """True if the dispatch touches orq itself: project `orq`, or a title that starts with `orq` (`orq: ...`, `orq ...`)."""
    return project == "orq" or bool(re.match(r"orq\b", (title or "").strip(), re.I))


def orq_worktree_block(number=None):
    """The block that tells the worker of an orq ticket to work in its own worktree: the live checkout (ORQ_INSTALL) receives only the integrator."""
    n = number or "<ticket>"
    live, wt = ORQ_INSTALL, os.path.join(WT_ROOT, n)
    return (f"{ORQ_WT_TITLE}\n\nNever commit on `main` of the live checkout `{live}`: the integrator advances that `main`, and the pre-commit hook refuses the commit.\n"
            f"Create the worktree `{wt}` from `origin/main` on a branch of its own (`git -C {live} worktree add -b <type>/<description> {wt} origin/main`) "
            "and work and commit only in it.\n")


def _with_orq_block(txt, number=None):
    """`txt` with the worktree block at the end; if the block is already there, returns `txt` as it came."""
    return txt if ORQ_WT_TITLE in txt else txt.rstrip("\n") + "\n\n" + orq_worktree_block(number)


def _gate_backlog(tk):
    """The dispatch gate with tickets in the backlog (the firstmate's): the item must exist and be `ready`, with no active hold and no open blocker. Refuses before creating anything."""
    item = _item_of_ticket(tk["num"])
    if not item:
        raise ValueError(f"ticket {tk['num']} is not in the backlog {BACKLOG}: migrate it with scripts/converte-backlog.py --completa")
    if backlog.active_hold(item):
        h = item["hold"]
        raise ValueError(f"ticket {tk['num']} is on hold ({h['motivo']}{', until ' + h['until'] if h.get('until') else ''}): tasks-axi unhold t{tk['num']}")
    if tk["blocked_by"]:
        raise ValueError(f"ticket {tk['num']} is blocked by {', '.join(tk['blocked_by'])}: close the blockers first")


def dispatch_worker(run, title, spec_file, model, effort, worktree=None, name=None, base_branch=None, entry=None, ticket=None, priority_level=None, agent=None, project=None, _draining=False, service=False, direct=None):
    """worker-start (with --model and --effort, which the worker-routing-guard hook requires) + `dispatch_mode` event + entry intake.

    Returns the ids and the waiter's command; waits for nothing. Refuses, before creating the task, whatever Orca would refuse later.

    With `ticket` (the number from an `orq ticket new`) the worker starts on the task the ticket already created (`worker-start --task`), with no title or spec: the
    ticket is the content. With night mode on, refuses after the cutoff time, the dispatch ceiling or consecutive failures (night_check). Also refuses
    when plan usage is above the threshold (usage_check). `priority_level` (1 high to 3 low) stays in the event; without it, the one at the start of the title applies (default_priority).

    Machine budget (ticket 79): with no slot (live workers or expensive ones at the ceiling) or with the machine under pressure, the request goes into the dispatch queue and the response is
    `{state: "enfileirado", queue, posicao, reason}` in place of the worker ids; the manager starts the item by priority when a slot opens (`_draining`: it is the one
    calling, and with no slot it raises SemVaga instead of queueing again).

    `service`: the worker is a service (integrator, secondmate) that stays alive after the first worker_done, when Orca revokes its capability. The event
    carries `service: true`: `orq agents` shows it as `service` (never "entregue sem liberar", delivered without release) and the worker reports each cycle with `orq cycle done`.

    `direct`: why the coordinator dispatched on its own a request that belongs to a group with a mate (`--direct`); it goes in the event.
    """
    if priority_level is not None and priority_level not in (1, 2, 3):
        raise ValueError("--priority expects 1 (high), 2 or 3 (low)")
    explicit_project = bool(project or (run and run_project(run)))
    project = dispatch_project(project, run)  # --projeto, the Run's, the cwd's; with none, the dispatch is the usual one
    repo = projects()[project]["repo"] if project else None
    if not agent:  # --agente wins; without it the project's harness holds, and without a project the usual claude
        agent = projects()[project]["harness"] if project else "claude"
    if repo and worktree == "current":
        if explicit_project:
            raise ValueError(f"project {project} starts the worker in a new worktree of its repo: --worktree current would stay in the cwd (Orca only accepts --repo with new-top-level)")
        repo = None  # project found only by the cwd: the current worktree is already of the same repo
    elif repo:
        worktree = "new-top-level"
    if project and worktree == "new-top-level" and not base_branch and project_flow(project)["declarado"]:
        base_branch = f"origin/{project_flow(project)['producao']}"  # the project that declares environments is born from production (the work branch only receives code from it)
    if agent not in HARNESS:
        raise ValueError(f"--agent {agent}: orq only dispatches {', '.join(HARNESSES)}")
    if effort not in HARNESS[agent]["efforts"]:
        raise ValueError(f"--effort {effort} does not exist in {agent} ({', '.join(HARNESS[agent]['efforts'])})")
    night_check()
    if (name or base_branch) and worktree != "new-top-level":
        raise ValueError("--name and --base-branch only work with --worktree new-top-level (Orca refuses to create a worktree in current)")
    tk = None
    if ticket:
        if title or spec_file:
            raise ValueError("--ticket carries the title and the content: do not combine it with --title or --spec-file")
        n = str(ticket).strip().zfill(2)
        tk = next((t for t in tickets() if t["num"] == n), None)
        if not tk:
            raise ValueError(f"ticket {n} does not exist in {ISSUES}")
        if tk["status"] == STATUS_CLOSED:
            raise ValueError(f"ticket {n} is already {STATUS_CLOSED}")
        if not tk["task"]:
            raise ValueError(f"ticket {n} has no task: create it with orq ticket new")
        if tk["run"] and tk["run"] != run:
            raise ValueError(f"ticket {n} belongs to Run {tk['run']} and the dispatch is for {run}: the task only dispatches in the Run where it was born")
        if _tickets_in_backlog():
            _gate_backlog(tk)
        title, spec = tk["titulo"], None
    elif not (title and spec_file):
        raise ValueError("pass --ticket <NN>, or --title and --spec-file")
    else:
        try:
            with open(os.path.expanduser(spec_file)) as f:
                spec = f.read()
        except OSError as e:
            raise ValueError(f"could not read {spec_file}: {e.strerror}")
    priority = priority_level or priority_of(read_events(), tk and tk["task"], None, title)
    usage_check(priority, agent=agent)
    request = _entry_text(entry)
    _adopt(run)
    if not coordinator_run(run):
        raise ValueError(f"the dispatch is for Run {run}, which the coordinator does not command: {bind_tip(run)}")
    with _lock("dispatch.lock"):  # the checked slot and the worker-start form one step: parallel dispatches do not exceed the ceiling together
        reason = machine_bar(model, run=run, priority=priority, service=service)
        if reason and _draining:
            raise NoSlot(reason)
        if reason:
            return _enqueue_dispatch(reason, run, title, spec, model, effort, worktree, name, base_branch, entry, tk, priority, agent, project, service)
        if orq_ticket(title, project):
            if spec is not None:
                spec = _with_orq_block(spec)
            elif tk["arquivo"]:  # the ticket's content is the file: the block goes into it, once
                with open(tk["arquivo"], encoding="utf-8") as f:
                    txt = f.read()
                if ORQ_WT_TITLE not in txt:
                    _write(tk["arquivo"], _with_orq_block(txt, tk["num"]))
        if spec is not None and not spec.lstrip().startswith("#"):
            spec = f"# {title}\n\n{spec}"  # Claude Code strips the tab name from the start of the prompt
        if spec is not None and request is not None:  # the literal request stays at the top, separate from what the coordinator wrote; the review measures against it
            head, _, rest = spec.partition("\n")
            spec = (f"{head}\n\n{REQUEST_TITLE}\n{request}\n\nWhat the coordinator wrote below does not replace it: done is checked against this request.\n\n"
                    f"{rest.lstrip(chr(10))}")
        if spec is not None:
            spec = f"{spec.rstrip()}\n\n{WAITING_BLOCK}\n"
        environment = night_environment() if night_active(_cursor_ro()) else None  # at night the worker comes up with no git prompt (credential, pinentry)
        folder = (repo_folder(repo) if repo else None) or os.getcwd()  # the project's repo root; a selector with no known folder falls back to the cwd, as before
        items, real, scratch = dispatch_conformance(spec, title, scratch_roots([folder]), tk and tk["arquivo"])  # ticket 201: what the delivery must prove
        if items and spec is not None:
            spec = f"{spec.rstrip()}\n\n{conformance_block(items, real)}"
        elif items and tk["arquivo"]:  # the ticket's content is the file: the block goes into it, once
            with open(tk["arquivo"], encoding="utf-8") as f:
                txt = f.read()
            if CONFORMANCE_TITLE not in txt:
                _write(tk["arquivo"], f"{txt.rstrip()}\n\n{conformance_block(items, real)}")
        trusted = trust_codex(_repo_root(folder) or folder) if agent == "codex" else []  # before worker-start: Codex asks about trust when it comes up
        args = ["worker-start", "--run", run, *(["--task", tk["task"]] if tk else ["--spec", spec, "--task-title", title]),
                "--agent", agent, "--model", model, "--effort", effort]
        for flag, val in (("--worktree", worktree), ("--repo", repo), ("--name", name), ("--base-branch", base_branch)):
            if val:
                args += [flag, val]
        try:
            res = orca(*args, timeout=180, env_extra=environment)
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"worker-start took over 180 s without a reply: the worker may have started, check with orq agents --run {run}")
        task, dispatch = res.get("taskId"), res.get("dispatchId")
        terminal = next((e.get("id") for e in res.get("effects") or [] if e.get("kind") == "terminal" and e.get("role") == "agent"), None)
        if not (task and dispatch):
            raise RuntimeError(f"worker-start without taskId/dispatchId in the reply: {json.dumps(res)[:300]}")
        if terminal:
            try:
                orca("rename", "--terminal", terminal, "--title", title, area="terminal")
            except Exception as e:  # noqa: BLE001 - the tab title is a comfort: failing does not undo the dispatch
                log(f"despachar: rename do terminal {terminal}: {type(e).__name__}: {e}")
        ev = {"tipo": "despacho", "run": run, "task": task, "dispatch": dispatch, "titulo": title, "agente": agent, "modelo": model, "effort": effort, "terminal": terminal,
              **({"worktree": worktree} if worktree else {}), **({"nome": name} if name else {}), **({"entrada": entry} if entry else {}),
              **({"ticket": tk["num"]} if tk else {}), **({"ambiente": list(environment)} if environment else {}), **({"prioridade": priority_level} if priority_level else {}),
              **({"projeto": project} if project else {}),
              **({"servico": True} if service else {}), **({"direct": direct} if direct else {}),
              **({"conformidade": items, "entrada_real": real} if items else {}), **({"scratch": scratch} if scratch else {})}
        append_event(ev)
    out = {"dispatchId": dispatch, "taskId": task, "run": run, "terminal": terminal, "espera": f"python3 ~/.claude/scripts/orca-wait-runs.py {run}"}
    if agent == "codex":
        with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired, KeyError):
            trusted += trust_codex(_checkpoint(dispatch)["caminho"])  # the worktree Orca created
        if trusted:
            out["confiadas"] = trusted
    if tk and scratch and not tk.get("scratch"):  # one tracker: the orq ticket remembers the scratch one, and closing it updates the scratch Status (ticket 201)
        try:
            ticket_edit(tk["num"], scratch=scratch[0])
        except (OSError, ValueError, backlog.BacklogError) as e:
            out["aviso"] = f"ticket {tk['num']} not linked to {scratch[0]} ({e}): its close will not update the scratch Status"
    if tk and _tickets_in_backlog():  # tasks-axi's `start` is what used to be the header's Status: the item goes to In flight after worker-start
        try:
            backlog.cli(BACKLOG, "start", _item_of_ticket(tk["num"])["id"])
        except (backlog.BacklogError, TypeError) as e:
            out["aviso"] = f"the worker started but ticket {tk['num']} did not move to In flight ({e}): run tasks-axi start t{tk['num']} on the backlog"
    elif tk:  # B24: the dispatched ticket stops being "pronto para agente" (ready for agent), otherwise a new session would dispatch it again
        try:
            with open(tk["arquivo"], encoding="utf-8") as f:
                _write(tk["arquivo"], _trocar_campo(f.read(), "Status", STATUS_IN_PROGRESS))
        except OSError as e:
            out["aviso"] = f"the worker started but ticket {tk['num']} was not marked {STATUS_IN_PROGRESS} ({e.strerror}): edit the Status by hand"
    if entry:
        out["entrada"] = entry
        try:
            intake(entry, "tarefa", task, run=run)
        except Exception as e:  # noqa: BLE001 - the worker is already up: the intake becomes a notice with the command to repeat
            out["aviso"] = f"intake not recorded ({e}): run orq intake {entry} task {task} --run {run}"
    if not _check_start(dispatch, terminal, title, out):
        append_event({"tipo": "nao_iniciou", "run": run, "task": task, "dispatch": dispatch, "terminal": terminal})
        out["estado"] = "nao_iniciou"
        out["aviso"] = "; ".join(filter(None, [out.get("aviso"), f"the spec did not reach the worker (not even after Enter): open terminal {terminal}, paste the spec or relaunch with orq relaunch {dispatch}"]))
    return out


def transcript_dirs():
    """The folders to look in for the coordinator's transcript: ORQ_TRANSCRITOS alone, otherwise each project's (the `transcritos` field, otherwise the one
    Claude Code names from its `repo: path:`; an id:/name: selector has no folder) and the cwd's, in that order and without repeats."""
    if TRANSCRIPTS:
        return [TRANSCRIPTS]
    def folder(path):  # Claude Code names the folder with the path, each character outside [A-Za-z0-9] becomes "-"
        return os.path.join(PROJECTS, re.sub(r"[^A-Za-z0-9]", "-", path))
    dirs = [os.path.expanduser(d["transcritos"]) if d.get("transcritos") else
            folder(os.path.realpath(os.path.expanduser(d["repo"][5:]))) if (d.get("repo") or "").startswith("path:") else None
            for d in projects().values() if not d.get("erro")]
    return list(dict.fromkeys(filter(None, [*dirs, folder(os.getcwd())])))


def _coordinator_session(session):
    """Transcript path: the given id (or a prefix of it) or, without an id, the last coordinator session recorded in cursor.json."""
    if not session:
        runs = _dict(_cursor_ro().get("runs"))
        if not runs:
            raise ValueError("no coordinator session recorded in cursor.json: pass --session <id>")
        session = list(runs)[-1]
    if _dict(_cursor_ro().get("harnesses")).get(session) == "codex":
        raise ValueError(f"session {session[:8]} belongs to a coordinator on Codex, which has no AskUserQuestion: there is no box answer to audit (those of `orq ask` are in resposta_lavish)")
    dirs = transcript_dirs()
    findings = sorted(os.path.join(d, f) for d in dirs if os.path.isdir(d) for f in os.listdir(d) if f.endswith(".jsonl") and f.startswith(session))
    if len(findings) != 1:
        raise ValueError(f"transcript of {session!r} in {', '.join(dirs)}: {'none' if not findings else 'more than one'}")
    return findings[0]


def recommended_only_answers(path):
    """(total answered questions, [(when, question, answer)] of those that chose only the recommended one) from the transcript.

    The answer comes in the `toolUseResult` of the tool_result line (answers indexed by the question text, questions with the options);
    the line's timestamp is the time the widget was answered. A dismissed question has no `answers`.
    """
    total, found_labels = 0, []
    with open(path) as f:
        for line in f:
            if '"answers"' not in line:  # the transcript is tens of MB: decode only what may hold a reply
                continue
            try:
                d = json.loads(line)
            except ValueError:
                continue
            tr = d.get("toolUseResult")
            if not isinstance(tr, dict) or not isinstance(tr.get("answers"), dict) or not d.get("timestamp"):
                continue
            for q in tr.get("questions") or []:
                answer_text = tr["answers"].get(q.get("question"))
                if answer_text is None:
                    continue
                total += 1
                if _recommended(answer_text, q):
                    found_labels.append((_dt(d["timestamp"]), q, answer_text))
    return total, found_labels


def audit_answers(session=None):
    """Markdown with the answers that chose only the recommended option within AUDIT_WINDOW_S of an Orca delivery to the coordinator.

    Read-only: reads the session transcript and the inbox of all Runs, and writes nothing. The user is the one who checks, with the list in hand.
    """
    path = _coordinator_session(session)
    total, found_labels = recommended_only_answers(path)
    msgs = [m for m in orca("inbox", "--limit", "5000", timeout=60)["messages"] if _to_coordinator(m)]
    deliveries = [(_dt(m.get("delivered_at") or m["created_at"]), m) for m in msgs]
    suspects_ = []
    for when, q, answer_text in found_labels:
        near = [m for t, m in deliveries if abs((when - t).total_seconds()) <= AUDIT_WINDOW_S]
        if near:
            suspects_.append((when, q, answer_text, near))
    suspects_.sort(key=lambda x: x[0])

    def cel(t):
        return str(t).replace("|", "\\|").replace("\n", " ")

    def hora(t):
        return t.astimezone().strftime("%d/%m/%Y %H:%M:%S")

    since = min((t for t, _ in deliveries), default=None)
    line_list = [
        f"# Suspect answers of session {os.path.basename(path)[:-6]}", "",
        f"Generated by `orq audit-answers`, read-only. {total} questions answered in the transcript, {len(found_labels)} chose only the "
        f"recommended option, {len(suspects_)} of them within {AUDIT_WINDOW_S} s of a message that Orca delivered to the coordinator terminal.", "",
        "Criterion: answer = only the recommended option (the first one, or the one with \"(Recomendado)\" in the label) and a message addressed to "
        "`run:<id>` (any Run) with `delivered_at` within 2 s of the transcript time. The notice \"You have N orchestration message\" "
        "is typed into the terminal at that moment and may have picked the option in place of the user. A worker heartbeat also triggers the notice, "
        "so there are coincidences where the notice did not answer: the list is for the user to check the decision, it proves nothing by itself. "
        "Times in local time zone; \"+N\" in the last column are other messages in the same window.", "",
        f"The Orca inbox covers {len(deliveries)} messages to the coordinator" + (f", the oldest from {hora(since)}." if since else "."), "",
        "| Date and time (local) | Header | Question | Answer | Orca message |", "|---|---|---|---|---|",
    ]
    for when, q, answer_text, near in suspects_:
        m = near[0]
        msg = f"`{m['id']}` {m.get('type')} \"{_quote(m.get('subject'), 40)}\" ({m.get('run_id')}, {hora(_dt(m.get('delivered_at') or m['created_at']))[-8:]})"
        line_list.append(f"| {hora(when)} | {cel(q.get('header'))} | {cel(_quote(q.get('question'), 80))} | {cel(_quote(answer_text, 70))} | "
                      f"{cel(msg)}{f' +{len(near) - 1}' if len(near) > 1 else ''} |")
    if not suspects_:
        line_list.append("| none | | | | |")
    return "\n".join(line_list) + "\n"


# ---------- agent manager in its own terminal ----------

def _adopt(run):
    """New coordinator Run (the raw `run-create` links it to it, and Orca sends it the heartbeat) enters the agent manager before worker-start.

    Only adopts a Run with no coordinator, the coordinator's own or one already the manager's: another terminal's stays as it is and Orca refuses it."""
    g = _manager_cfg()
    mine = os.environ.get("ORCA_TERMINAL_HANDLE")
    if not g or g.get("coordenador") != mine or run in g["runs"]:
        return
    if (orca("run-show", "--id", run)["run"] or {}).get("coordinator_handle") in (None, mine, g["gerente"]):
        manager_bind(g["gerente"], [run])

def manager_bind(terminal, runs=None, take_over=False):
    """Links the Runs to the agent manager terminal (`run-use` with its handle) and writes gerente.json: from then on this coordinator's orq talks
    to Orca through that handle, and Orca's notices (heartbeat included) go to the agent manager terminal, not to the coordinator.

    Adds to the Runs the same manager already has (no duplicates); another manager terminal restarts the list. Without `runs`, the Run linked to the
    coordinator enters (the one from the `run-create` that just ran). Orca links one Run per terminal: only the last stays linked, the panel rotates them.
    `take_over`: this coordinator takes the gerente.json of another that still shows in Orca (terminal swap without the old one disappearing): its Runs come along."""
    mine = os.environ.get("ORCA_TERMINAL_HANDLE")
    if not mine:
        raise ValueError("outside an Orca terminal (no ORCA_TERMINAL_HANDLE)")
    if terminal == mine:
        raise ValueError("the agent manager cannot be the coordinator's own terminal")
    runs = [runs] if isinstance(runs, str) else list(runs or [])
    if not runs:
        current = (orca("run-current", acting_as=mine)["run"] or {}).get("id")  # the Run that run-create or run-use just linked to this terminal
        runs = [current] if current else []
    if not runs:
        raise ValueError("no Run bound: pass --run <r>")
    try:
        orca("show", "--terminal", terminal, area="terminal")
    except RuntimeError as e:
        raise ValueError(f"terminal {terminal} does not exist in Orca ({e})") from e
    before = _manager_cfg()
    already = before["runs"] if before.get("coordenador") == mine and before.get("gerente") == terminal else []
    if before and not already and (take_over or _dead(before.get("coordenador")) or _dead(before.get("gerente"))):
        already = before["runs"]  # crash: the old coordinator or manager no longer exists, their Runs stay with the new manager (ticket 48)
    include_all = list(dict.fromkeys([*already, *runs]))
    _write_json(_path(MANAGER), {"coordenador": mine, "gerente": terminal, "runs": include_all})
    try:
        with manager_lock():
            for r in runs:
                orca("run-use", "--id", r)  # already by the manager's handle
    except Exception:
        _write_json(_path(MANAGER), before) if before else os.remove(_path(MANAGER))
        raise
    return append_event({"tipo": "gerente", "op": "ligar", "terminal": terminal, "run": runs[-1], "runs": include_all})


def manager_check():
    """Outside the hook: is the manager terminal from gerente.json still in `orca terminal list`? Records the finding in PANEL_CHECK. Without a
    reliable list (Orca failed or cut it) it proves nothing: it is not dead."""
    g = _manager_cfg()
    if not g:
        return None
    live = _alive_terminals()
    ck = {"ts": time.time(), "terminal": g["gerente"], "morto": live is not None and g["gerente"] not in live}
    _write_json(_path(PANEL_CHECK), ck)
    return ck


def manager_start(force=False):
    """The agent manager terminal is gone: creates another with painel-agent-manager.sh and relinks to it all the Runs of gerente.json (this coordinator
    takes over the file). Refuses with the old terminal still in Orca, unless with `force`."""
    g = _manager_cfg()
    if not g:
        raise ValueError("no gerente.json: nothing to spawn (use `orq manager bind`)")
    live = _alive_terminals()
    if not force:
        if live is None:
            raise ValueError("Orca did not list the terminals: with no proof that the agent manager died, nothing was done (--force spawns anyway)")
        if g["gerente"] in live:
            raise ValueError(f"terminal {g['gerente']} still exists in Orca: if the panel stopped, restart painel-agent-manager.sh in it (--force spawns another)")
    new = _new_terminal("agent manager", f"sh {shlex.quote(_path('painel-agent-manager.sh'))}")
    ev = manager_bind(new, g["runs"], take_over=True)
    try:
        os.remove(_path(PANEL_CHECK))
    except OSError:
        pass
    return {**ev, "terminal": new, "runs": g["runs"]}


HANDOFF_OPEN_MIN = 15  # minutes with no turn registered by the new worker until the handoff becomes a line in `orq status`


def handoff_lines(events, turns, now_at=None):
    """The `orq status` line with the handoffs (`orq let_pass`) open for more than HANDOFF_OPEN_MIN: the event was not accepted and the new dispatch
    does not have a turn in turnos.json yet. Pure function of the log; with no open handoff, no line."""
    now_at = now_at or datetime.now(timezone.utc)
    item_list = []
    for e in events:
        ts = _ts(e.get("ts"))
        if e.get("tipo") != "passagem" or e.get("aceita") or _dict(turns.get(e.get("para"))).get("inicio") or not ts:
            continue
        minutes_elapsed = int((now_at - ts).total_seconds() // 60)
        if minutes_elapsed > HANDOFF_OPEN_MIN:
            item_list.append(f"{e.get('para')} (de {e.get('de')}, {e.get('agente_de')}→{e.get('agente_para')}, {minutes_elapsed} min)")
    return [f"Open handoffs ({len(item_list)}): " + "; ".join(_lim(item_list, 3, str)) + " — check the terminal of the new worker"] if item_list else []


def status_text():
    """What `orq status` prints: the state, the open handoffs, the PRs, the worktrees, the E2E queue and the machine."""
    return "\n".join([*filter(None, [fail_safe.broken_line()]), state(include_old=True), *handoff_lines(read_events(), _turns_ro()), *pr_lines(), *worktree_lines(), *mate_lines(), *filter(None, [e2e_line(e2e_queue()), machine_line()])])


# ---------- orq iniciar: the coordinator that is already open, in any harness ----------

def own_harness():
    """claude or codex: the first ancestor of this process that is a harness. The environment is inherited and can be stale, so only ancestry
    decides; None without a harness ancestor (or without `ps`)."""
    by_pid = {p["pid"]: p for p in _processes(with_cwd=False) or ()}
    p, seen = by_pid.get(os.getppid()), set()
    while p and p["pid"] not in seen:
        if _agent_of(p):
            return _agent_of(p)
        seen.add(p["pid"])
        p = by_pid.get(p["ppid"])


def _orq_hooks(file_name):
    """{(event, call)} of orq's hooks in a settings.json or hooks.json; empty if the file does not exist or cannot be read."""
    try:
        event_list = _dict(json.load(open(file_name, encoding="utf-8")).get("hooks"))
    except (OSError, ValueError):
        return set()
    return {(ev, m.group(0)) for ev, group_map in event_list.items() for g in group_map for h in _dict(g).get("hooks", []) if (m := HOOK_ORQ.search(_dict(h).get("command") or ""))}


def missing_hooks(agent):
    """The hooks of the harness's example (`settings.hooks.example.json`, `codex.hooks.example.json`) that its hooks file does not have, as `Event: call`."""
    example = _orq_hooks(os.path.join(os.path.dirname(os.path.abspath(__file__)), HOOKS_EXAMPLE[agent]))
    return [f"{ev}: {call}" for ev, call in sorted(example - _orq_hooks(HOOKS_FILES[agent]))]


def _python_ok(exe):
    """Is `exe` a Python at least fail_safe.MIN_PYTHON?"""
    try:
        return subprocess.run([exe, "-c", "import sys; sys.exit(sys.version_info < %r)" % (fail_safe.MIN_PYTHON,)], capture_output=True, timeout=5).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def resolve_python():
    """Absolute path of a Python 3.12+ (ticket 247): ORQ_PYTHON, then the PATH's `python3` (a name that survives Homebrew upgrades), the versioned ones, mise, uv and this process's own. None if none qualifies."""
    candidates = [os.environ.get("ORQ_PYTHON"), *(shutil.which(n) for n in ("python3", "python3.14", "python3.13", "python3.12")),
                  *sorted(glob.glob(os.path.expanduser("~/.local/share/mise/installs/python/*/bin/python3")), reverse=True)]
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        candidates.append(subprocess.run(["uv", "python", "find", ">=%d.%d" % fail_safe.MIN_PYTHON], capture_output=True, text=True, timeout=5).stdout.strip())
    candidates.append(sys.executable)
    return next((c for c in candidates if c and os.path.isabs(c) and _python_ok(c)), None)


def hook_interpreter(command):
    """The interpreter (first word) of an orq hook command, or None when the command is not an orq hook or has none."""
    first = (command or "").split(None, 1)[0] if HOOK_ORQ.search(command or "") else ""
    return first if first and not first.endswith(".py") else None


def _hook_commands(file_name):
    """[(event, call, command)] of orq's hooks in a settings.json or hooks.json; empty if it does not exist or cannot be read."""
    try:
        event_list = _dict(json.load(open(file_name, encoding="utf-8")).get("hooks"))
    except (OSError, ValueError):
        return []
    return [(ev, m.group(0), c) for ev, group_map in event_list.items() for g in group_map for h in _dict(g).get("hooks", [])
            if (c := _dict(h).get("command") or "") and (m := HOOK_ORQ.search(c))]


def hooks_python_problems(agent):
    """What keeps the harness's hooks from running orq (tickets 228 and 247), one line each: the interpreter of an orq hook command that cannot import orqlib
    (too old, missing), and the `python3` written loose, which depends on the PATH of whoever fires the hook."""
    by_python = {}
    for ev, call, command in _hook_commands(HOOKS_FILES[agent]):
        by_python.setdefault(hook_interpreter(command) or "python3", []).append(f"{ev}: {call}")
    problems = []
    for python, hooks in sorted(by_python.items()):
        exe = os.path.expanduser(python) if "/" in python else shutil.which(python)
        listing = ", ".join(hooks[:2]) + (f" +{len(hooks) - 2}" if len(hooks) > 2 else "")
        if not exe:
            problems.append(f"{listing}: `{python}` not found")
            continue
        code = f"import sys; print(sys.version.split()[0]); sys.path.insert(0, {os.path.dirname(os.path.abspath(__file__))!r}); import orqlib"
        try:
            p = subprocess.run([exe, "-c", code], capture_output=True, text=True, timeout=15)
            version, error = (p.stdout.split() or ["?"])[0], (p.stderr.strip().splitlines() or [""])[-1] if p.returncode else ""
        except (OSError, subprocess.SubprocessError) as e:
            version, error = "?", f"{type(e).__name__}: {e}"
        if error:
            problems.append(f"{listing}: `{python}` (Python {version}) cannot import orqlib: {error}")
        elif "/" not in python:
            problems.append(f"{listing}: `{python}` is loose (this shell's PATH finds Python {version}, the harness may not): `orq doctor hooks --pin`")
    return problems


def pin_hooks_python(agent, python=None):
    """Writes the absolute interpreter into the orq hook commands of the harness's hooks file (text edit: the rest of the file keeps its formatting). Returns (python, changed commands);
    (None, 0) when there is no Python 3.12+ to write. Codex trusts a hook by what it says, so the pinned ones need `/hooks` again (codex_hooks_notice)."""
    python = python or resolve_python()
    file_name = HOOKS_FILES[agent]
    if not python or not os.path.exists(file_name):
        return python, 0
    changed = 0

    def pin(m):
        nonlocal changed
        first = hook_interpreter(m.group(2))
        if first is None or first == python:
            return m.group(0)
        changed += 1
        return m.group(1) + python + m.group(2)[len(first):] + m.group(3)

    text = open(file_name, encoding="utf-8").read()
    new = re.sub(r'("command"\s*:\s*")([^"]*)(")', pin, text)
    if changed:
        json.loads(new)  # never leave a hooks file that does not parse
        tmp = f"{file_name}.{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(new)
        os.replace(tmp, file_name)
    return python, changed


def pin_orq_link(python=None):
    """The `orq` the manager loop calls becomes a two-line wrapper that runs orq.py with the resolved interpreter (a symlink would take whatever `python3` the PATH has). True if it wrote."""
    python = python or resolve_python()
    script = os.path.join(orqpaths.CODE, "orq.py")
    wrapper = f"#!/bin/sh\nexec {shlex.quote(python or '')} {shlex.quote(script)} \"$@\"\n"
    if not python or (os.path.isfile(ORQ_LINK) and not os.path.islink(ORQ_LINK) and open(ORQ_LINK, encoding="utf-8").read() == wrapper):
        return False
    os.makedirs(os.path.dirname(ORQ_LINK), exist_ok=True)
    tmp = f"{ORQ_LINK}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(wrapper)
    os.chmod(tmp, 0o755)
    os.replace(tmp, ORQ_LINK)
    return True


def start(agent=None, run=None, objective=None, take_over=False):
    """Links the coordinator that is already open (never opens another): checks the harness hooks, links the Run (`run`, otherwise a new one with `objective`, otherwise the
    one the coordinator already commands), starts or reuses the agent manager and returns the text with the state. A missing hook refuses before touching Orca;
    a Codex hook not yet trusted only warns (trust is granted in `/hooks`, inside Codex itself). A live manager of another live coordinator asks for `take_over`."""
    mine = os.environ.get("ORCA_TERMINAL_HANDLE")
    if not mine:
        raise ValueError("outside an Orca terminal (no ORCA_TERMINAL_HANDLE)")
    agent = agent or own_harness()
    if not agent:
        raise ValueError("found neither claude nor codex among the processes above this one: pass --agent claude|codex")
    if still_missing := missing_hooks(agent):
        acting_as = "orq hooks-codex" if agent == "codex" else f"merge {HOOKS_EXAMPLE[agent]} into {CLAUDE_SETTINGS}"
        raise ValueError(f"orq hooks missing in {HOOKS_FILES[agent]}: {'; '.join(still_missing)}. Install with: {acting_as}")
    found = hooks_python_problems(agent)
    python, pinned = pin_hooks_python(agent)
    pinned_link = pin_orq_link(python)
    g, live = _manager_cfg(), _alive_terminals()
    dead = lambda h: live is not None and h not in live  # noqa: E731 - without a reliable list nothing proves it died
    if g and g.get("coordenador") != mine and not take_over and not dead(g["coordenador"]):
        raise ValueError(f"agent manager {g['gerente']} belongs to coordinator {g['coordenador']}, which still exists in Orca: --take-over takes the manager and its Runs")
    current = run or (None if objective else default_run())
    if not current and not objective:
        raise ValueError("no Run bound to this coordinator: pass --objective '<subject of the stream>' (new Run) or --run <r>")
    if run:
        orca("run-use", "--id", run, acting_as=mine)
    elif objective:
        current = orca("run-create", "--objective", objective, acting_as=mine)["run"]["id"]
    new = not g or dead(g["gerente"])
    terminal = _new_terminal("agent manager", f"sh {shlex.quote(_path('painel-agent-manager.sh'))}") if new else g["gerente"]
    manager_bind(terminal, [current], take_over=take_over)
    problems = hooks_python_problems(agent) if python else [f"no Python {fail_safe.MIN_PYTHON[0]}.{fail_safe.MIN_PYTHON[1]}+ found (Homebrew, mise, uv, python3.12): install one or set ORQ_PYTHON"]
    pin_notice = [f"hooks: Python pinned to {python} in {pinned} hook command(s)" + (" and in the `orq` link" if pinned_link else "")] if pinned or pinned_link else []
    return "\n".join([f"harness: {agent}", *(f"hooks: found: {x}" for x in found), *pin_notice, *([f"hooks: BROKEN {x}" for x in problems] or ["hooks: ok"]), *filter(None, [codex_hooks_notice() if agent == "codex" else ""]),
                      f"Run: {current}", f"manager: {terminal} ({'new' if new else 'already existed'})", status_text()])


def manager_turn_off(run=None, take_over=False):
    """Returns to the coordinator terminal (`run-use` with its own handle) the Run `run`, or all of them, and removes it from gerente.json.

    Without `run` the file is deleted. The coordinator holds only one Run (one per terminal): it keeps the one the manager had linked, and the others
    stay without a coordinator until a `run-use`. `take_over` also unlinks the gerente.json of another coordinator that still shows in Orca."""
    g = _manager_cfg()
    mine = os.environ.get("ORCA_TERMINAL_HANDLE")
    if g and g.get("coordenador") != mine and (take_over or _dead(g.get("coordenador"))):
        g = {**g, "coordenador": mine}  # crash: the old coordinator's terminal no longer exists, this one replaces it (ticket 48)
    if not g or g.get("coordenador") != mine:
        raise ValueError("agent manager is not bound to this coordinator")
    if run and run not in g["runs"]:
        raise ValueError(f"Run {run} is not bound to the agent manager ({', '.join(g['runs']) or 'none'})")
    if run:
        sent_back_items, rest = [run], [r for r in g["runs"] if r != run]
    else:
        try:
            current = (orca("run-current")["run"] or {}).get("id")
        except RuntimeError:  # manager terminal closed: the order recorded at link time holds
            current = None
        sent_back_items, rest = [*(r for r in g["runs"] if r != current), *([current] if current in g["runs"] else [])], []
    with manager_lock():
        if rest:
            _write_json(_path(MANAGER), {**{k: v for k, v in g.items() if k != "run"}, "runs": rest})
        else:
            os.remove(_path(MANAGER))
        for r in sent_back_items:
            orca("run-use", "--id", r, acting_as=mine)
    return append_event({"tipo": "gerente", "op": "desligar", "terminal": g.get("gerente"), "run": sent_back_items[-1] if sent_back_items else None, "runs": sent_back_items})


# ---------- machine budget and dispatch queue (ticket 79) ----------

MACHINE_FILE = "machine.json"  # on top of MACHINE_DEFAULTS; `orq machine set <key> <value>` records it
MACHINE_DEFAULTS = {"max_workers": 4,  # workers alive at the same time (24 GB of RAM, 12 CPUs: each one may bring up an E2E stack)
                  "max_e2e": 1,  # informational only: the global E2E queue (scripts/e2e-lock.sh) already serializes the stacks
                  "max_caros": 2, "modelos_caros": ["claude-opus-*", "gpt-6-astra*", "gpt-6-sol*"],  # glob patterns; the expensive model counts toward max_workers too
                  "mem_livre_min_mb": 3072, "livre_pct_min": 15,  # below either of the two the pressure is high
                  "carga_max": 12,  # 1-min loadavg above this (one per CPU) is high pressure
                  "mem_piso_mb": 1024,  # safety floor: not even the exempt Run comes up with free memory below this
                  "runs_isentos": ["Orquestrador*"],  # glob patterns (Run id or objective): orq's own work comes up under pressure and without the worker ceiling; max_caros, max_e2e and mem_piso_mb hold it back
                  "pausar_sob_pressao": False,  # True: under pressure the manager on its own pauses the lowest-priority worker (orq pausar)
                  "mate_ready_min": 3,  # ready tickets that match a group with no mate before orq proposes opening it (the group's `mate_ready_min` wins)
                  "stop_bloqueia": False}  # True: the coordinator's Stop blocks the end of the turn with any entry that has no effect (GATE_BLOCKERS times per set); turning it on is the user's decision (ticket 27). An entry with no intake in the turn always blocks (ticket 150)
DISPATCH_QUEUE = "dispatch-queue.json"  # {itens: [...]}: what `orq dispatch_worker` and `orq resume` could not bring up; the manager brings it up by priority
DISPATCH_QUEUE_SPECS = "fila-despacho"  # ORQ_HOME/fila-despacho/<id>.md: copy of the spec of a queued dispatch (the coordinator's file may vanish)
MACHINE_ALIVE = ("rodando", "travado", "nao_comecou", "parado", "perguntando")
HEAVY_PROCESSES = ("claude", "codex", "node", "docker")  # the sum of RSS that `orq machine` shows
WAIT_HELD_S = 300  # a queue item held back by plan usage or night mode is only retried after this
QUEUE_FAILURES = 3  # manager attempts with an error before the item leaves the queue


class NoSlot(Exception):
    """The manager tried to start a queue item and the machine has no slot left (another dispatch took the slot)."""


def _machine_kind_ok(default, v):
    if isinstance(default, bool):
        return isinstance(v, bool)
    if isinstance(default, (int, float)):
        return isinstance(v, (int, float)) and not isinstance(v, bool)
    return isinstance(v, list) and all(isinstance(x, str) for x in v)


def machine_cfg():
    """MACHINE_DEFAULTS on top of maquina.json; a key of the wrong type or an unknown one counts as absent."""
    read_text = _dict(_read_json(_path(MACHINE_FILE)))
    return {k: read_text[k] if k in read_text and _machine_kind_ok(p, read_text[k]) else p for k, p in MACHINE_DEFAULTS.items()}


def machine_set(key_name, value):
    """`orq machine set <key> <value>`: value in JSON (4, true, ["claude-opus-*"]); refuses an unknown key or one of another type. Returns the new config."""
    key_name = KEYS_PT.get(key_name, key_name)  # the English key in machine.json counts the same as the pt one
    if key_name not in MACHINE_DEFAULTS:
        raise ValueError(f"unknown key {key_name!r}; the ones that exist: {', '.join(KEYS_EN.get(k, k) for k in MACHINE_DEFAULTS)}")
    try:
        v = json.loads(value)
    except ValueError:
        raise ValueError(f"value {value!r} is not JSON (use 4, true or [\"claude-opus-*\"])")
    if not _machine_kind_ok(MACHINE_DEFAULTS[key_name], v):
        raise ValueError(f"value {value} does not fit {key_name} (expects {type(MACHINE_DEFAULTS[key_name]).__name__})")
    _write_json(_path(MACHINE_FILE), {**_dict(_read_json(_path(MACHINE_FILE))), key_name: v}, indent=2)
    return machine_cfg()


def machine_read():
    """What the machine has right now, using native macOS tools: {mem_free_mb, free_pct, carga, ncpu, rss_mb: {claude, codex, node, docker}}.

    mem_free_mb = (free + inactive + speculative + purgeable) from `vm_stat`; free_pct = the `System-wide memory free percentage` from `memory_pressure`;
    carga = 1-minute loadavg (the same number as `sysctl vm.loadavg`); rss_mb = sum of the RSS of the HEAVY_PROCESSES processes in `ps`. A source that fails
    stays None/empty (the pressure it would measure counts as unknown, never as high). `ORQ_MAQUINA_LEITURA` points to a JSON in place of the reading (tests)."""
    if os.environ.get("ORQ_MAQUINA_LEITURA"):
        simulated = _dict(_read_json(os.environ["ORQ_MAQUINA_LEITURA"]))
        procs = simulated.pop("processos", None)  # the simulated process sample (the _processes list); without it the origin stays unknown
        return {**simulated, "origem": machine_origin(procs)}

    def run_cmd(*cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=5).stdout
        except (OSError, subprocess.TimeoutExpired):
            return ""
    reading = {"mem_livre_mb": None, "livre_pct": None, "carga": None, "ncpu": os.cpu_count(), "rss_mb": {}}
    vm = run_cmd("vm_stat")
    page = re.search(r"page size of (\d+)", vm)
    pages = {n: re.search(rf"^Pages {n}:\s+(\d+)", vm, re.M) for n in ("free", "inactive", "speculative", "purgeable")}
    if page and all(pages.values()):
        reading["mem_livre_mb"] = sum(int(m.group(1)) for m in pages.values()) * int(page.group(1)) // 2**20
    pct = re.search(r"free percentage:\s*(\d+)%", run_cmd("memory_pressure"))
    reading["livre_pct"] = int(pct.group(1)) if pct else None
    with contextlib.suppress(OSError):
        reading["carga"] = round(os.getloadavg()[0], 2)
    rss = dict.fromkeys(HEAVY_PROCESSES, 0)
    for line in run_cmd("ps", "-axo", "rss=,comm=").splitlines():
        kb, _, comm = line.strip().partition(" ")
        base = os.path.basename(comm.strip()).lower()
        item_name = next((p for p in HEAVY_PROCESSES if base == p or (p == "docker" and "docker" in comm.lower())), None)
        if item_name and kb.isdigit():
            rss[item_name] += int(kb) // 1024
    reading["rss_mb"] = rss
    reading["origem"] = machine_origin(_processes(with_cwd=False))
    return reading


def _process_name(args):
    """How the process appears in the culprits list: the app (`OrbStack` from .../OrbStack.app/...) or the executable's name."""
    app = re.search(r"/([^/]+)\.app/", args)
    return app.group(1) if app else os.path.basename((args.split(None, 1) or [""])[0])


def machine_origin(procs):
    """Whose load it is, from the `_processes` list: {orq_cpu, fora_cpu, orq_rss_mb, fora_rss_mb, fora_por_cpu, fora_por_mem} or None without the list.

    Orq's are the agent processes (claude, codex: workers, manager and coordinator) and everything that starts under them, plus the E2E ones (`e2e` in the command and what
    starts under it). The rest is outside. `fora_por_cpu` and `fora_por_mem` are the three largest outside ones, summed by name ([{nome, valor}], %CPU and MB). Limit: macOS `ps`
    %CPU is an average that decays in about a minute, and OrbStack containers count as a single outside process."""
    if not procs:
        return None
    orq = _descendants(procs, [p["pid"] for p in procs if _agent_of(p) or re.search(r"e2e", p["args"], re.I)])
    inside, outside = [p for p in procs if p["pid"] in orq], [p for p in procs if p["pid"] not in orq]

    def top_items(key_name, scale):
        by_name = {}
        for p in outside:
            n = _process_name(p["args"])
            by_name[n] = by_name.get(n, 0) + (p.get(key_name) or 0) / scale
        return [{"nome": n, "valor": round(v)} for n, v in sorted(by_name.items(), key=lambda kv: -kv[1])[:3] if round(v) > 0]
    soma = lambda ps, k, scale=1: round(sum(p.get(k) or 0 for p in ps) / scale)
    return {"orq_cpu": soma(inside, "cpu"), "fora_cpu": soma(outside, "cpu"), "orq_rss_mb": soma(inside, "rss", 1024), "fora_rss_mb": soma(outside, "rss", 1024),
            "fora_por_cpu": top_items("cpu", 1), "fora_por_mem": top_items("rss", 1024)}


def machine_level(reading=None, cfg=None):
    """(`alta` | `ok`, reason): high pressure if free memory, the free percentage or the load go past the limit in maquina.json. A missing reading does not count."""
    l, c = reading if reading is not None else machine_read(), cfg or machine_cfg()
    reasons = []
    if isinstance(l.get("mem_livre_mb"), (int, float)) and l["mem_livre_mb"] < c["mem_livre_min_mb"]:
        reasons.append(f"free memory {l['mem_livre_mb']:g} MB (minimum {c['mem_livre_min_mb']:g} MB)")
    if isinstance(l.get("livre_pct"), (int, float)) and l["livre_pct"] < c["livre_pct_min"]:
        reasons.append(f"free memory {l['livre_pct']:g}% (minimum {c['livre_pct_min']:g}%)")
    if isinstance(l.get("carga"), (int, float)) and l["carga"] > c["carga_max"]:
        reasons.append(f"load {l['carga']:g} (maximum {c['carga_max']:g})")
    return ("alta", "; ".join(reasons)) if reasons else ("ok", None)


def machine_cause(reading=None, cfg=None):
    """(`orq` | `outside` | None, culprits): whose the high pressure is. `orq` if most (half or more) of the CPU, in the load reason, or of the RSS, in the memory one,
    is from orq's processes; `outside` if not, with the largest outside ones ("mds_stores 146%, OrbStack 77%"). None without pressure or without the process sample
    (unknown origin counts as orq's: the notice keeps suggesting to pause)."""
    l, c = reading if reading is not None else machine_read(), cfg or machine_cfg()
    o = l.get("origem")
    if not o:
        return None, None
    cpu_high = isinstance(l.get("carga"), (int, float)) and l["carga"] > c["carga_max"]
    mem_high = (isinstance(l.get("mem_livre_mb"), (int, float)) and l["mem_livre_mb"] < c["mem_livre_min_mb"]) or (isinstance(l.get("livre_pct"), (int, float)) and l["livre_pct"] < c["livre_pct_min"])
    if not (cpu_high or mem_high):
        return None, None
    if (cpu_high and o["orq_cpu"] >= o["fora_cpu"]) or (mem_high and o["orq_rss_mb"] >= o["fora_rss_mb"]):
        return "orq", None
    culprits = [f"{x['nome']} {x['valor']}%" for x in o["fora_por_cpu"]] if cpu_high else []
    culprits += [f"{x['nome']} {x['valor']} MB" for x in o["fora_por_mem"]] if mem_high else []
    return "fora", ", ".join(culprits) or "processes outside orq"


def machine_floor(reading, cfg):
    """The reason free memory is below the safety floor (`mem_piso_mb`), the hard limit that applies even to the exempt Run; or None."""
    free = reading.get("mem_livre_mb")
    if isinstance(free, (int, float)) and free < cfg["mem_piso_mb"]:
        return f"free memory {free:g} MB below the safety floor ({cfg['mem_piso_mb']:g} MB), which applies even to the exempt Run"
    return None


def exempt_run(run, cfg=None):
    """Does the Run match a `runs_isentos` pattern (glob, case-insensitive, against the id or the objective)? A Run that Orca does not show is not exempt."""
    import fnmatch  # only here: kept out of the top level so it doesn't weigh on the hooks
    defaults = (cfg or machine_cfg())["runs_isentos"]
    if not run or not defaults:
        return False
    try:
        objective = (orca("run-show", "--id", run).get("run") or {}).get("objective") or ""
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        objective = ""
    return any(fnmatch.fnmatch(x.lower(), p.lower()) for p in defaults for x in (run, objective) if x)


def expensive_model(model, cfg=None):
    """Does the model match any glob pattern in `modelos_caros` (case-insensitive). An unknown model is not expensive."""
    import fnmatch  # only here: kept out of the top level so it doesn't weigh on the hooks
    return bool(model) and any(fnmatch.fnmatch(str(model).lower(), p.lower()) for p in (cfg or machine_cfg())["modelos_caros"])


RECENT_DISPATCH_WINDOW = 120  # s: how long the dispatch recorded in events.jsonl counts as an occupied slot while worker-list doesn't show it yet


def machine_occupancy():
    """{vivos: {dispatch: model}, dispatched: {dispatch}}: the workers with a live terminal in Orca (all Runs) and the dispatches still `dispatched`.

    The model comes from the dispatch or resume event and, failing that, from worker-show; without a reliable terminal list every `dispatched` counts as live."""
    terminals = _alive_terminals()
    include_all = _all_workers()
    ws = [w for w in include_all if w.get("dispatchStatus") == "dispatched"]
    hibernated = _hibernated()  # a terminal closed on purpose doesn't take a slot, even when the terminal list fails
    live = [w for w in ws if w.get("dispatchId") not in hibernated and (terminals is None or w.get("agentTerminalHandle") in terminals)]
    event_list = read_events()
    models = {e["dispatch"]: e["modelo"] for e in event_list if e.get("tipo") in ("despacho", "retomada") and e.get("dispatch") and e.get("modelo")}
    still_missing = [w for w in live if w["dispatchId"] not in models]
    if still_missing:
        models |= {d: x["modelo"] for d, x in _details(still_missing).items() if x.get("modelo")}
    occupancy = {w["dispatchId"]: models.get(w["dispatchId"]) for w in live}
    listed = {w.get("dispatchId") for w in include_all}
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=RECENT_DISPATCH_WINDOW)
    for e in event_list:  # worker-list lags: a dispatch from seconds ago doesn't show up yet and its slot would stay free for the next one (ticket 145)
        d = e.get("dispatch")
        if e.get("tipo") in ("despacho", "retomada") and d and d not in listed and d not in hibernated and e.get("ts") and _dt(e["ts"]) >= cutoff:
            occupancy[d] = e.get("modelo")
    return {"vivos": occupancy, "dispatched": {w["dispatchId"] for w in ws} | set(occupancy)}


def machine_slot(model, occupancy=None, cfg=None, exempt=False):
    """The reason one more worker of this model does not fit (live workers at the ceiling, or expensive ones at the ceiling), or None if it fits. `exempt`: exempt Run, without the worker ceiling."""
    cfg, occupancy = cfg or machine_cfg(), occupancy or machine_occupancy()
    n = len(occupancy["vivos"])
    if n >= cfg["max_workers"] and not exempt:
        return f"{n}/{cfg['max_workers']:g} live workers"
    expensive_count = sum(expensive_model(m, cfg) for m in occupancy["vivos"].values())
    if expensive_model(model, cfg) and expensive_count >= cfg["max_caros"]:
        return f"{expensive_count}/{cfg['max_caros']:g} expensive workers ({model} is expensive)"
    return None


def machine_bar_item(model, run, occupancy, cfg, pressure, reading, priority=None, service=False):
    """The reason this Run's worker does not start now, or None. Machine pressure comes before the slots; the service or P1 worker of an exempt Run (`runs_isentos`)
    passes through pressure and the worker ceiling, and only stops at the expensive ceiling (cost limit), the memory floor and max_e2e (the global E2E queue). P2/P3 counts toward the ceiling (ticket 168)."""
    cause = machine_cause(reading, cfg) if pressure[0] == "alta" else (None, None)
    reason = (f"machine under pressure: {pressure[1]}" + (f"; it comes from outside orq ({cause[1]})" if cause[0] == "fora" else "")) if pressure[0] == "alta" else machine_slot(model, occupancy, cfg)
    if not reason or not (service or priority == 1) or not exempt_run(run, cfg):
        return reason
    return machine_slot(model, occupancy, cfg, exempt=True) or machine_floor(reading, cfg)


def machine_bar(model, occupancy=None, cfg=None, run=None, priority=None, service=False):
    """The reason the worker cannot start now, or None. Machine pressure comes before slots; exempt Run: see machine_bar_item."""
    cfg = cfg or machine_cfg()
    reading = machine_read()
    return machine_bar_item(model, run, occupancy, cfg, machine_level(reading, cfg), reading, priority, service)


def _dispatch_queue_mut(fn):
    """Reads the dispatch queue, applies fn(items) and writes it back, under fila-despacho.lock. Returns what fn returned."""
    with _lock("dispatch-queue.lock"):
        d = _dict(_read_json(_path(DISPATCH_QUEUE)))
        item_list = [i for i in d.get("itens") or [] if isinstance(i, dict)]
        r = fn(item_list)
        _write_json(_path(DISPATCH_QUEUE), {"itens": item_list}, indent=2)
        return r


def dispatch_queue_items():
    """The queue items in the order they start: priority (1 before 3) and, on a tie, the oldest."""
    item_list = [i for i in _dict(_read_json(_path(DISPATCH_QUEUE))).get("itens") or [] if isinstance(i, dict)]
    return sorted(item_list, key=lambda i: (i.get("prioridade") or 2, i.get("ts") or ""))


def dispatch_queue_add(item, reason):
    """Puts the request in the queue and returns (item, position, already there). A dispatch of the same ticket (or of the same title in the same Run) does not get in twice."""
    import uuid
    new = {"id": "fd" + uuid.uuid4().hex[:6], "ts": now(), "motivo": reason, "falhas": 0, **item}

    def poe(item_list):
        equal = next((i for i in item_list if i.get("tipo") == new["tipo"] and (i.get("dispatch") == new.get("dispatch") if new["tipo"] == "retomada" else
                                                                           (i.get("run"), i.get("ticket") or i.get("titulo")) == (new["run"], new.get("ticket") or new.get("titulo")))), None)
        if equal:
            return equal, True
        item_list.append(new)
        return new, False
    it, already = _dispatch_queue_mut(poe)
    if not already:
        append_event({"tipo": "despacho_fila", "op": "entrou", "id": it["id"], "fila_tipo": it["tipo"], "titulo": it.get("titulo"), "prioridade": it.get("prioridade"), "motivo": reason})
    order = [i["id"] for i in dispatch_queue_items()]
    return it, order.index(it["id"]) + 1, already


def dispatch_queue_rm(id_, op="removido", **extra):
    """Removes the item from the queue (and the spec copy). False if the id is not there."""
    found_item = _dispatch_queue_mut(lambda item_list: next((item_list.pop(n) for n, i in enumerate(item_list) if i.get("id") == id_), None))
    if found_item:
        with contextlib.suppress(OSError):
            os.remove(_path(os.path.join(DISPATCH_QUEUE_SPECS, id_ + ".md")))
        append_event({"tipo": "despacho_fila", "op": op, "id": id_, "fila_tipo": found_item.get("tipo"), "titulo": found_item.get("titulo"), **extra})
    return found_item


def _enqueue_dispatch(reason, run, title, spec, model, effort, worktree, name, base_branch, entry, tk, priority, agent, project=None, service=False):
    """The dispatch that did not fit: stores the request (the spec in a copy under ORQ_HOME) and returns the `orq dispatch_worker` response in place of the worker ids."""
    import uuid
    id_ = "fd" + uuid.uuid4().hex[:6]
    item = {"id": id_, "tipo": "despacho", "run": run, "titulo": title, "modelo": model, "effort": effort, "agente": agent, "prioridade": priority,
            **{k: v for k, v in (("worktree", worktree), ("nome", name), ("base_branch", base_branch), ("entrada", entry), ("ticket", tk and tk["num"]), ("projeto", project), ("servico", service)) if v}}
    if os.environ.get("ORQ_MATE"):  # the Run belongs to the mate and only its terminal commands it: the manager drains with that handle (ticket 80)
        item.update(coord=os.environ.get("ORCA_TERMINAL_HANDLE"), mate=os.environ["ORQ_MATE"])
    copy_file = _path(os.path.join(DISPATCH_QUEUE_SPECS, id_ + ".md"))
    if spec is not None:
        os.makedirs(os.path.dirname(copy_file), exist_ok=True)
        with open(copy_file, "w", encoding="utf-8") as f:
            f.write(spec)
        item["spec_arquivo"] = copy_file
    it, pos, already = dispatch_queue_add(item, reason)
    if already and spec is not None:
        os.remove(copy_file)
    return {"estado": "enfileirado", "fila": it["id"], "posicao": pos, "run": run, "prioridade": priority, "motivo": reason,
            "aviso": f"{'was already' if already else 'entered'} in the dispatch queue (position {pos}): {reason}. The manager starts it when a slot opens; see with orq dispatch-queue list"}


def _occupy(occupancy, dispatch, model):
    """Counts the worker that just started in the occupancy, so the next item in the same batch sees the slot already taken."""
    occupancy["vivos"][dispatch] = model


@contextlib.contextmanager
def _as_coordinator(it=None):
    """The panel runs in the agent manager terminal; the dispatch needs the coordinator handle (it is what commands the Runs, and orca() swaps it for the manager's).
    The item a mate queued (`mate`, `coord_handle`) starts as the mate: the Run is theirs."""
    g, before = _manager_cfg(), {k: os.environ.get(k) for k in ("ORCA_TERMINAL_HANDLE", "ORQ_MATE")}
    it = it or {}
    if it.get("mate") and it.get("coord"):  # the current terminal: the mate may have gone down and come back after enqueuing
        os.environ.update(ORCA_TERMINAL_HANDLE=_dict(_mates().get(it["mate"])).get("terminal") or it["coord"], ORQ_MATE=it["mate"])
    elif g.get("coordenador"):
        os.environ["ORCA_TERMINAL_HANDLE"] = g["coordenador"]
    try:
        yield
    finally:
        for k, v in before.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _promoted_from_queue(it):
    """Starts one queue item. Returns the panel line; raises SemVaga, ValueError or RuntimeError if it did not start (the item goes back)."""
    if it["tipo"] == "despacho":
        with _as_coordinator(it):
            r = dispatch_worker(it["run"], it.get("titulo") if not it.get("ticket") else None, it.get("spec_arquivo"), it["modelo"], it["effort"], it.get("worktree"), it.get("nome"),
                          it.get("base_branch"), it.get("entrada"), it.get("ticket"), it["prioridade"], it.get("agente") or "claude", it.get("projeto"), _draining=True, service=bool(it.get("servico")))
        return f"queue: {it['titulo']} started ({r.get('dispatchId')})"
    d = it["dispatch"]
    cp = _checkpoint(d)
    line = {k: it.get(k) for k in ("task", "run", "titulo", "agente", "modelo", "effort", "sessao", "cwd", "terminal")} | {"dispatch": d}
    r = _start_session(line, it["sessao"], it.get("modelo"), cp, f"start another worker from the task spec with: orq relaunch {d} --note 'the session could not be resumed'",
                      proceed_msg(it.get("coord"), it["task"], d, it["run"]), it.get("agente") or "claude", it.get("effort"))
    return f"queue: {it['titulo']} resumed: {r['estado']}" + (f" ({r['aviso']})" if r.get("aviso") else "")


def _ticket_command(run, ticket, model, effort):
    """`orq dispatch --run <run> --ticket <n> --model <m> --effort <e>`: whatever is missing from the ticket header is left out."""
    return " ".join(["orq dispatch"] + [f"{f} {v}" for f, v in (("--run", run), ("--ticket", ticket), ("--model", model), ("--effort", effort)) if v and v != "None"])


def _abandoned_command(it):
    """The command to dispatch by hand the item the queue gave up on."""
    if it.get("ticket"):
        return _ticket_command(it["run"], it["ticket"], it.get("modelo"), it.get("effort"))
    return "orq dispatch " + (f"--run {it['run']} " if it.get("run") else "") + f"--title {shlex.quote(it.get('titulo') or '')} --spec-file <spec>"


def _notify_abandoned(it, failures, error, cmd):
    """Types into the coordinator, once (the item leaves the queue on giving up), that the queue gave up on the item, with the error and the command to dispatch by hand. The away Stop
    keeps charging until the item is dispatched (away_abandoned). Coordinator busy or no manager: only the Stop remains."""
    coord_handle = it.get("coord") or _manager_cfg().get("coordenador")
    if coord_handle:
        with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired, ValueError, OSError):
            notify_coordinator(coord_handle, f"orq: the queue gave up ({failures} errors). Dispatch by hand: {cmd}. {_quote(it.get('titulo'), 40)}: {_quote(str(error), 100)}")


def away_abandoned(tks, events):
    """Reason (or None) for the away Stop: an item the dispatch queue gave up on (`desistiu`) and nobody dispatched afterwards. Dropped: the item whose ticket (or ticket with the same
    title, for the old event without `ticket`) left ready or became blocked, the one dispatched later (a `dispatch_mode` event with the same ticket, or Run and title, or just the title)
    and the one discarded by hand (`orq queue-dispatch_mode descartar`)."""
    open_items, by_num = {}, {t["num"]: t for t in tks}
    for e in events:
        if e.get("tipo") != "despacho_fila" and e.get("tipo") != "despacho":
            continue
        if e.get("tipo") == "despacho_fila" and e.get("op") == "desistiu":
            open_items[e.get("ticket") or (e.get("run"), e.get("titulo"))] = e
        elif e.get("tipo") == "despacho_fila" and e.get("op") == "descartado":
            open_items = {k: v for k, v in open_items.items() if not e.get("id") or v.get("id") != e["id"]}
        elif e.get("tipo") == "despacho":
            open_items = {k: v for k, v in open_items.items() if k != (e.get("ticket") or (e.get("run"), e.get("titulo"))) and not (e.get("titulo") and v.get("titulo") == e["titulo"])}
    for k, e in open_items.items():
        t = by_num.get(e.get("ticket")) or next((x for x in tks if e.get("titulo") and x.get("titulo") == e["titulo"]), None) or {}
        if t and (t.get("status") != STATUS_NEW or any((by_num.get(b) or {}).get("status") != STATUS_CLOSED for b in t.get("blocked_by") or [])):
            continue
        number = e.get("ticket") or t.get("num")
        cmd = e.get("comando") or (_ticket_command(e.get("run"), number, e.get("modelo") or t.get("modelo"), e.get("effort") or t.get("effort")) if number else _abandoned_command(e))
        return f"the dispatch queue gave up on {_quote(e.get('titulo'), 60)} ({_quote(str(e.get('erro')), 120)}): `{cmd}`"
    return None


def drain_dispatch(cfg=None, now_at=None, only_exempt=False):
    """One queue round: starts, by priority, the first item that fits (one per round, so memory shows what the previous one cost before the next).

    An expensive-model item with the expensive cap full stays behind and the cheap lower-priority one starts. A resume whose dispatch already finished or came back is removed from
    the queue. Whatever plan usage or night mode holds waits WAIT_HELD_S; an error of another cause counts in `failures` and, at the QUEUE_FAILURES-th, the item leaves.
    An item from an exempt Run (`runs_isentos`) starts without the worker cap, stopped only by the expensive cap and the memory floor; `only_exempt` (high pressure) tries only those."""
    cfg, now_at = cfg or machine_cfg(), now_at or time.time()
    item_list = dispatch_queue_items()
    if not item_list:
        return []
    occupancy, line_list = machine_occupancy(), []
    for it in item_list:
        if it.get("nao_antes", 0) > now_at:
            continue
        if it["tipo"] == "retomada" and (it["dispatch"] in occupancy["vivos"] or it["dispatch"] not in occupancy["dispatched"]):
            dispatch_queue_rm(it["id"], "saiu", motivo="the dispatch already finished or already came back")
            line_list.append(f"queue: {it['titulo']} left (the dispatch already finished or already came back)")
            continue
        exempt = (only_exempt or machine_slot(it.get("modelo"), occupancy, cfg)) and (bool(it.get("servico")) or it.get("prioridade") == 1) and exempt_run(it.get("run"), cfg)  # only asks Orca when there is something to exempt
        if only_exempt and not exempt:
            continue
        if exempt and (machine_slot(it.get("modelo"), occupancy, cfg, exempt=True) or machine_floor(machine_read(), cfg)) or not exempt and machine_slot(it.get("modelo"), occupancy, cfg):
            continue
        try:
            line_list.append(_promoted_from_queue(it))
            dispatch_queue_rm(it["id"], "subiu")
            return line_list
        except NoSlot:
            continue
        except (ValueError, RuntimeError, subprocess.TimeoutExpired) as e:
            held = str(e).startswith(("plan usage", "night mode"))
            failures = it.get("falhas", 0) + (0 if held else 1)
            if failures >= QUEUE_FAILURES:
                cmd = _abandoned_command(it)
                dispatch_queue_rm(it["id"], "desistiu", erro=str(e), ticket=it.get("ticket"), task=it.get("task"), run=it.get("run"), modelo=it.get("modelo"), effort=it.get("effort"), comando=cmd)
                _notify_abandoned(it, failures, e, cmd)
                line_list.append(f"queue: {it['titulo']} left the queue after {failures} errors ({e}); dispatch it again by hand")
                continue
            _dispatch_queue_mut(lambda xs, it=it, failures=failures, held=held: [x.update(falhas=failures, nao_antes=now_at + WAIT_HELD_S if held else 0)
                                                                                      for x in xs if x["id"] == it["id"]])
            line_list.append(f"queue: {it['titulo']} is still waiting ({e})")
    return line_list


def _pause_candidate():
    """The live worker with the lowest priority (highest priority number; on a tie the first), except those spared for being in final verification; or None."""
    agent_rows = agents()
    chosen, _ = _pause_list(agent_rows, read_events(), set(), 1, _dict(_cursor_ro().get("pausados")))
    return max(chosen, key=lambda a: a["prioridade"], default=None)


def _machine_notify(reason, in_queue, cfg, cause=(None, None)):
    """High pressure: types into the coordinator, once per episode, that the manager stopped starting workers and which one to pause. With `pausar_sob_pressao` the manager pauses that
    worker by itself (orq pausar waits up to ORQ_PAUSA_ESPERA_S for PAUSE.md: the panel round takes that long to return). Coordinator busy: the next round tries.
    Load that comes from outside orq (`cause` outside): the notice lists the biggest outside ones, holds the dispatch and neither suggests nor does a pause, which would relieve nothing."""
    g = _manager_cfg()
    if not g or not g.get("coordenador") or _cursor_ro().get("maquina_aviso"):
        return []
    outside = cause[0] == "fora"
    target = None
    with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired, ValueError, KeyError):
        target = None if outside else _pause_candidate()
    tip = f" To free up room: orq pause {target['task']} (P{target['prioridade']} {target.get('titulo') or target['task']})." if target else ""
    children = {}
    with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired, ValueError, KeyError):
        children = _children_by_task()
    peso = f" Background processes: {', '.join(f'{t} {n}' for t, n in children.items())}." if children else ""
    text_value = (f"orq: machine under pressure ({reason}). The load comes from outside orq ({cause[1]}): pausing a worker does not help. The manager holds new dispatches ({in_queue} in the dispatch queue)."
             if outside else f"orq: machine under pressure ({reason}). The manager stopped starting workers ({in_queue} in the dispatch queue).{peso}{tip}")
    if notify_coordinator(g["coordenador"], text_value) not in ("enviado", "adiado"):
        return []
    _cursor_mut(lambda c: c.__setitem__("maquina_aviso", {"ts": now(), "motivo": reason}))
    append_event({"tipo": "maquina_aviso", "motivo": reason, "na_fila": in_queue, **({"causa": cause[0]} if cause[0] else {}), **({"sugerido": target["task"]} if target else {}),
                  **({"filhos": children} if children else {})})
    line_list = [f"machine under pressure ({reason}): coordinator notified, nothing starts"]
    if cfg["pausar_sob_pressao"] and target:
        r = pause_workers((target["task"],))
        line_list += [f"automatic pause: {x['task']} {x['estado']}" for x in r["pausados"]]
    return line_list


def machine_round():
    """The dispatch queue work in one round of the agent manager panel: high pressure notifies the coordinator and only the exempt Run starts; otherwise it starts one item that fits."""
    cfg = machine_cfg()
    reading = machine_read()
    level, reason = machine_level(reading, cfg)
    if level == "alta":
        return _machine_notify(reason, len(dispatch_queue_items()), cfg, machine_cause(reading, cfg)) + drain_dispatch(cfg, only_exempt=True)
    if _cursor_ro().get("maquina_aviso"):
        _cursor_mut(lambda c: c.pop("maquina_aviso", None))
    return drain_dispatch(cfg)


def machine_text(cfg=None, reading=None, occupancy=None):
    """The lines of `orq machine`: what the machine has, what orq would decide now for a cheap dispatch and for an expensive one, the slots and the queue."""
    cfg, reading = cfg or machine_cfg(), reading if reading is not None else machine_read()
    level, reason = machine_level(reading, cfg)
    rss = _dict(reading.get("rss_mb"))
    ls = [f"free memory {reading.get('mem_livre_mb')} MB ({reading.get('livre_pct')}% free), load {reading.get('carga')} on {reading.get('ncpu')} CPUs",
          "RSS: " + ", ".join(f"{k} {rss.get(k, 0)} MB" for k in HEAVY_PROCESSES)]
    if o := reading.get("origem"):
        ls.append(f"load by owner: orq {o['orq_cpu']}% of CPU and {o['orq_rss_mb']} MB, outside orq {o['fora_cpu']}% and {o['fora_rss_mb']} MB"
                  + ((" (largest outside: " + ", ".join("%s %s%%" % (x["nome"], x["valor"]) for x in o["fora_por_cpu"]) + ")") if o["fora_por_cpu"] else ""))
    if occupancy is not None:
        expensive_count = sum(expensive_model(m, cfg) for m in occupancy["vivos"].values())
        ls.append(f"slots: {len(occupancy['vivos'])}/{cfg['max_workers']:g} taken, {max(cfg['max_workers'] - len(occupancy['vivos']), 0):g} free; expensive {expensive_count}/{cfg['max_caros']:g}; E2E max {cfg['max_e2e']:g} (the E2E queue serializes); dispatch queue {len(dispatch_queue_items())}")

    def decide(model):
        m = f"high pressure: {reason}" if level == "alta" else machine_slot(model, occupancy, cfg) if occupancy is not None else None
        return f"queue ({m})" if m else "starts"
    ls.append(f"decision now: dispatch of a cheap model {decide('barato')}; of an expensive model {decide(next(iter(cfg['modelos_caros']), 'caro').replace('*', 'x'))}")
    return ls


def machine_panel(agents_=None):
    """What the panel and the digest show about the machine, from files only: {max_workers, max_caros, ocupadas, livres, caros, fila: [{id, tipo, titulo, prioridade, modelo}]}."""
    cfg = machine_cfg()
    live = [a for a in agents_ or [] if isinstance(a, dict) and a.get("estado") in MACHINE_ALIVE]
    return {"max_workers": cfg["max_workers"], "max_caros": cfg["max_caros"], "ocupadas": len(live), "livres": max(cfg["max_workers"] - len(live), 0),
            "caros": sum(expensive_model(a.get("modelo"), cfg) for a in live),
            "fila": [{k: i.get(k) for k in ("id", "tipo", "titulo", "prioridade", "modelo")} for i in dispatch_queue_items()]}


def machine_line():
    """The `orq status` line: slots (from aberto.json, without talking to Orca), pressure and queue. Empty with nothing to say (no live worker, no queue, no pressure)."""
    p = machine_panel(_dict(_read_json(_path("open.json"))).get("agentes"))
    level, reason = machine_level()
    if not p["ocupadas"] and not p["fila"] and level == "ok":
        return ""
    txt = f"Machine: {p['ocupadas']}/{p['max_workers']:g} workers ({p['caros']}/{p['max_caros']:g} expensive), {p['livres']:g} free slots"
    txt += f"; HIGH PRESSURE: {reason}" if level == "alta" else ""
    if p["fila"]:
        txt += f"; {len(p['fila'])} in the dispatch queue: " + ", ".join(f"P{i.get('prioridade') or 2} {_quote(i.get('titulo') or '?', 30)}" for i in p["fila"][:3]) + (f" +{len(p['fila']) - 3}" if len(p["fila"]) > 3 else "")
    return txt


# ---------- resume after a crash (ticket 48) ----------

RESUME_WAIT_S = float(os.environ.get("ORQ_RETOMAR_ESPERA_S") or 20)  # how long to wait for the resumed session to show activity before saying it didn't come back
MSG_CONTINUE = ("Continue where you left off. The session dropped because of a power outage; the Orca terminal and handle changed. First: check git status and the state of "
                "your worktree, and if you were waiting on a suite, run it again (the E2E queue was released). When you finish, send worker_done as before; if "
                "Orca refuses because of the new handle, write the final report in a final-report.md file at the root of your worktree and show the path in the terminal.")
MSG_ESCALATE = ("Never leave a question or confirmation waiting in the terminal: the coordinator ({coord}) cannot see this screen and AskUserQuestion is refused. Doubt, permission or blocker: "
               "`orca orchestration send --from \"$ORCA_TERMINAL_HANDLE\" --to run:{run} --type escalation --subject \"<summary>\" --body \"<details>\" --task-id {task} --dispatch-id {dispatch}`. "
               "Before commands with `rm` and a variable, protect the variable (`\"${{VAR:?}}\"/*`) so the agent guard does not ask for confirmation.")


def proceed_msg(coord_handle, task, dispatch, run):
    """The continuation message of the resumed session: the MSG_CONTINUE and the way to escalate (coordinator handle and command), since the dispatch context was lost."""
    return f"{MSG_CONTINUE} {MSG_ESCALATE.format(coord=coord_handle or '<coordinator>', run=run or '<run>', task=task or '<task>', dispatch=dispatch)}"


def _new_terminal(title, command, cwd=None):
    """`orca terminal create` (in worktree `cwd`, or the current one) and the new terminal's handle."""
    args = ["create", "--title", title, "--command", command, *(["--worktree", f"path:{cwd}"] if cwd else [])]
    return orca(*args, area="terminal", timeout=30)["terminal"]["handle"]


def _came_back(handle, agent="claude"):
    """The terminal session shows activity: `esc to interrupt` on the screen, or the screen changed between two reads; the agent screen's `failure` is what did not come back."""
    before, end = None, time.time() + RESUME_WAIT_S
    while True:
        screen = "\n".join(orca("read", "--terminal", handle, "--screen", area="terminal")["terminal"].get("tail") or [])
        if any(f in screen for f in HARNESS[agent]["tela"]["falha"]):
            return False
        if "esc to interrupt" in screen or (before is not None and screen != before):
            return True
        before = screen
        if time.time() >= end:
            return False
        time.sleep(1)


def _manager_to_relink(g, mine, live):
    """(manager terminal died, gerente.json coordinator changed) when gerente.json belongs to this coordinator or to one that died; otherwise None (another live coordinator owns it)."""
    if not g or (g.get("coordenador") != mine and g.get("coordenador") in live):
        return None
    dead, swapped = g["gerente"] not in live, g.get("coordenador") != mine
    return (dead, swapped) if dead or swapped else None


def dispatch_session(dispatch, agent, timeout=15):
    """{sessao, cwd, transcrito} of the worker session in the Orca session index (`orca search <dispatch>`): the agent's first hit where the dispatch
    appears in a user prompt (the preamble), not in tool output (the coordinator quotes it too). {} with no hit or with Orca down."""
    try:
        hits = orca(dispatch, "--agent", agent, "--scope", "conversation", "--limit", "20", area="search", timeout=timeout).get("hits") or []
    except (RuntimeError, subprocess.TimeoutExpired, ValueError) as e:
        log(f"sessao_do_dispatch {dispatch}: {type(e).__name__}: {e}")
        return {}
    h = next((h for h in hits if _dict(h.get("evidence")).get("role") == "user" and h.get("sessionId")), None)
    return {"sessao": h["sessionId"], "cwd": h.get("cwd"), "transcrito": _dict(h.get("source")).get("filePath")} if h else {}


def _start_session(line, session, model, cp, tip, msg=MSG_CONTINUE, agent="claude", effort=None):
    """The agent resume (HARNESS) of `session` in a new terminal in worktree `line["cwd"]`, `retomada` event and screen check. Returns `line` with the
    state (retomado, sem_atividade or falhou)."""
    command = shlex.join(HARNESS[agent]["resume"](session, model, effort, msg))
    try:
        new = _new_terminal(f"{line['titulo']} (resumed)", command, line["cwd"])
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        return {**line, "estado": "falhou", "aviso": f"terminal create failed ({e}); {tip}"}
    append_event({"tipo": "retomada", "dispatch": line["dispatch"], "task": line["task"], "run": line["run"], "terminal": new, "anterior": line["terminal"],
                  "sessao": session, "cwd": line["cwd"], "modelo": model, "head": cp["head"], "sujo": cp["sujo"]})
    try:
        returned = _came_back(new, agent)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        returned, line = False, {**line, "aviso": f"could not read the screen ({e})"}
    return {**line, "novo": new, "estado": "retomado" if returned else "sem_atividade",
            **({} if returned else {"aviso": f"{line.get('aviso') or 'the screen shows no activity'}; {tip}"})}


def resume(dry_run=False, run=None):
    """After a crash: starts the agent manager that died and resumes, in a new terminal, each worker that has not yet sent worker_done and lost its terminal.

    Resumed worker: `orca terminal create --worktree path:<cwd> --command "claude --resume <sessão> --model <model> ... '<continue>'"`, with the session and the
    cwd the worker hooks stored in turnos.json (the cwd falls back to the worker-show worktree). The new terminal becomes the dispatch's (`retomada` event, which
    _all_workers applies) and its screen is checked. The agent manager starts `panel-agent-manager.sh` in a new tab and rebinds its Runs to this coordinator.
    `dry_run` only lists. Without a reliable terminal list (Orca failed or truncated it) it refuses: with no proof of who died, nothing is started."""
    mine = os.environ.get("ORCA_TERMINAL_HANDLE")
    if not mine:
        raise ValueError("outside an Orca terminal (no ORCA_TERMINAL_HANDLE)")
    live = _alive_terminals()
    if live is None:
        raise ValueError("Orca did not list the terminals: with no proof of who died, nothing was resumed")
    g = _manager_cfg()
    plan = _manager_to_relink(g, mine, live)
    res = {"gerente": None, "workers": []}
    if plan:
        dead, _ = plan
        res["gerente"] = {"terminal": g["gerente"], "runs": g["runs"], "estado": "a_subir" if dead else "a_religar"}
        if not dry_run:
            new = g["gerente"]
            if dead:
                new = _new_terminal("agent manager (resumed)", f"sh {shlex.quote(_path('painel-agent-manager.sh'))}")
            manager_bind(new, g["runs"])
            res["gerente"] = {**res["gerente"], "novo": new, "estado": "religado"}
    paused = _dict(_cursor_ro().get("pausados"))  # those paused by the usage budget come back with `resume --paused`, not here
    hib = _hibernated()  # hibernated ones neither: the terminal was closed on purpose, and what wakes them is orq acordar (or the steer, the reply, the pending item, the merge)
    cand = [w for w in _all_workers(run) if _lost_terminal(w, live, paused, hib)]
    detail_entry, turns, event_list = _details(cand), _turns_ro(), read_events()
    dispatches = {e.get("dispatch"): e for e in event_list if e.get("tipo") == "despacho"}
    by_dispatch_id, launch = {}, []
    for w in cand:
        d = w["dispatchId"]
        t = _dict(turns.get(d))
        dd, ev = detail_entry.get(d) or {}, dispatches.get(d) or {}
        model, effort = dd.get("modelo") or ev.get("modelo"), dd.get("effort") or ev.get("effort")
        agent = dd.get("agente") or ev.get("agente") or t.get("harness") or "claude"
        title = dd.get("titulo") or ev.get("titulo") or d
        cp = _checkpoint(d)  # from the firstmate: the worktree must exist, and the head and dirty files are recorded before the agent comes up
        if not t.get("sessao") and agent in HARNESS:
            t = {**t, **dispatch_session(d, agent)}  # the turn hook didn't see this worker: Orca's session index may have
        cwd = t.get("cwd") or cp["caminho"]
        line = {"dispatch": d, "task": w.get("taskId"), "run": w.get("runId"), "titulo": title, "agente": agent, "modelo": model, "effort": effort,
                 "sessao": t.get("sessao"), "cwd": cwd, "terminal": w.get("agentTerminalHandle")}
        tip = f"start another worker from the task spec with: orq relaunch {d} --note 'the session could not be resumed'"  # from the firstmate: without a session, the brief on disk is the durable instruction
        if not line["sessao"] or not cwd:
            by_dispatch_id[d] = {**line, "estado": "sem_sessao", "aviso": f"no session_id or cwd recorded (the turn hook did not see this worker): {tip}"}
        elif not os.path.isdir(cwd):
            by_dispatch_id[d] = {**line, "estado": "sem_worktree", "aviso": f"the folder {cwd} does not exist: nothing was started"}
        else:
            launch.append((priority_of(event_list, w.get("taskId"), d, title), line, cp, tip))
    if launch:  # the machine budget (ticket 79): climbs by priority up to the ceiling, the rest goes to the dispatch queue
        cfg, occupancy, reading = machine_cfg(), machine_occupancy(), machine_read()
        pressure, services = machine_level(reading, cfg), _services(read_events())
        for priority, line, cp, tip in sorted(launch, key=lambda x: x[0]):  # stable: within the same priority, Orca's order holds
            d = line["dispatch"]
            reason = machine_bar_item(line["modelo"], line["run"], occupancy, cfg, pressure, reading, priority, line["dispatch"] in services)
            if reason:
                if not dry_run:
                    dispatch_queue_add({"tipo": "retomada", "dispatch": d, "task": line["task"], "run": line["run"], "titulo": line["titulo"], "agente": line["agente"],
                                       "modelo": line["modelo"], "effort": line["effort"], "sessao": line["sessao"], "cwd": line["cwd"], "terminal": line["terminal"],
                                       "prioridade": priority, "coord": mine}, reason)
                by_dispatch_id[d] = {**line, "prioridade": priority, "estado": "a_enfileirar" if dry_run else "enfileirado", "aviso": f"{reason}; the manager starts it when a slot opens (orq dispatch-queue list)"}
                continue
            _occupy(occupancy, d, line["modelo"])
            if dry_run:
                by_dispatch_id[d] = {**line, "prioridade": priority, "estado": "a_retomar"}
            else:
                by_dispatch_id[d] = {**_start_session(line, line["sessao"], line["modelo"], cp, tip, proceed_msg(mine, line["task"], d, line["run"]), line["agente"], line["effort"]), "prioridade": priority}
    res["workers"] = [by_dispatch_id[w["dispatchId"]] for w in cand]
    for g, m in _mates().items():  # the mate that went down comes back through its session (ticket 80); the unanswered request keeps its deadline
        if _dict(m).get("terminal") not in live and _dict(m).get("sessao") and not _dict(m).get("dormiu") and g in groups():  # slept on purpose: only the request wakes it
            try:
                res.setdefault("mates", []).append({"grupo": g, "estado": "a_retomar"} if dry_run else {**mate_open(g), "estado": "retomado"})
            except (ValueError, RuntimeError, subprocess.TimeoutExpired) as e:
                res.setdefault("mates", []).append({"grupo": g, "estado": "falhou", "aviso": str(e)})
    return res


def resume_text(res):
    """One line per item, saying what was (or would be) done."""
    g, ls = res["gerente"], []
    if g:
        ls.append(f"manager {g['terminal']}: {g['estado']}" + (f" -> {g['novo']}" if g.get("novo") else "") + f" ({len(g['runs'])} Run(s))")
    for w in res["workers"]:
        ls.append(f"{w['dispatch']} {w['titulo']}: {w['estado']}" + (f" -> {w['novo']}" if w.get("novo") else "") + (f" ({w['aviso']})" if w.get("aviso") else ""))
    for m in res.get("mates") or []:
        ls.append(f"mate {m['grupo']}: {m['estado']}" + (f" -> {m['terminal']}" if m.get("terminal") else "") + (f" ({m['aviso']})" if m.get("aviso") else ""))
    return "\n".join(ls) or "nothing to resume"


# ---------- plan usage budget and pause by priority (ticket 51) ----------

# Usage source: Claude Code passes `rate_limits` ({five_hour, seven_day}: {used_percentage, resets_at}) on the statusline's stdin, and the HUD wrapper from
# OMC (~/.claude/hud/omc-hud-cache.sh) writes that JSON to hud/cache/stdin.<session>.json on every frame. It is the same number as the footer (5h:…% wk:…%), the
# same for all sessions of the account, and reading the file calls neither the network nor Orca: it serves the hooks.
HUD_CACHE = os.environ.get("ORQ_HUD_CACHE") or os.path.expanduser("~/.claude/hud/cache")
FRESH_USAGE_S = 1800  # a cache older than this says nothing about current usage (no session has drawn the footer)
USAGE = "usage.json"  # thresholds, on top of DEFAULT_USAGE: {"semana_avisa": 85, "semana_pausa": 92, "five_h": 90}
DEFAULT_USAGE = {"semana_avisa": 85, "semana_pausa": 92, "cinco_h": 90}  # five_h: warns and holds new dispatch until the window turns over
PAUSE_WAIT_S = float(os.environ.get("ORQ_PAUSA_ESPERA_S") or 300)  # how long to wait for each worker to write PAUSE.md
PAUSE_POLL_S = float(os.environ.get("ORQ_PAUSA_POLL_S") or 2)
MSG_PAUSE = ("Coordinator pause (plan usage limit). In up to 3 lines, write in PAUSE.md at the root of your worktree where you stopped and the next step, "
             "stop any E2E you started and end the turn. Do not send worker_done: you come back later in this same session.")
MSG_RESUME = ("Plan usage is back to normal and the coordinator resumed you. Read PAUSE.md (or PAUSA.md) at the root of your worktree, delete it and go on from the next step. "
             "When you finish, send worker_done as before; if Orca refuses because of the new handle, write the final report in a "
             "final-report.md file at the root of your worktree and show the path in the terminal.")
_HIGH_WORKSTREAM = re.compile(r"seguran|security|produ[cç][aã]o", re.I)
_LOW_WORKSTREAM = re.compile(r"failover|diagn[oó]stic|painel|digest", re.I)
_FINAL_PHASE = re.compile(r"review|verif|final", re.I)
_INITIAL_PHASE = re.compile(r"investig", re.I)


def _usage_window(j, now_at):
    """(percentage, reset) of a rate_limits window; a window that already rolled over counts as 0; with no number, (None, None)."""
    p, r = _dict(j).get("used_percentage"), _dict(j).get("resets_at")
    if isinstance(p, bool) or not isinstance(p, (int, float)):
        return None, None
    return (0 if isinstance(r, (int, float)) and r <= now_at else p), r


def plan_usage(now_at=None, agent="claude"):
    """{semana, semana_reset, five_h, cinco_h_reset} of the `agent` plan (percentages 0-100, resets in epoch), or None with no number.

    Claude: the newest HUD frame (only reads a file, no network or Orca); with no fresh frame, `orca account list`. Codex: `orca account list`,
    which reads each harness's account (the quotas are separate)."""
    now_at = now_at or time.time()
    if agent != "claude":
        return _orca_usage(agent, now_at)
    return _hud_usage(now_at) or _orca_usage(agent, now_at)


def _orca_usage(agent, now_at):
    """The `rateLimits.<agent>` from `orca account list`: `weekly` is the week and `session` the 5 h window (resetsAt in ms). None with no number or with Orca down."""
    try:
        rl = _dict(_dict(orca("list", area="account", timeout=5).get("rateLimits")).get(agent))
    except (RuntimeError, subprocess.TimeoutExpired, ValueError) as e:
        log(f"uso {agent}: {type(e).__name__}: {e}")
        return None

    def window(j):
        p, r = _dict(j).get("usedPercent"), _dict(j).get("resetsAt")
        if isinstance(p, bool) or not isinstance(p, (int, float)):
            return None, None
        r = r / 1000 if isinstance(r, (int, float)) else None
        return (0 if r and r <= now_at else p), r

    (s, sr), (c, five_reset) = window(rl.get("weekly")), window(rl.get("session"))
    return {"semana": s, "semana_reset": sr, "cinco_h": c, "cinco_h_reset": five_reset} if s is not None or c is not None else None


def _hud_usage(now_at):
    """The `rate_limits` of the newest OMC HUD frame, or None with no fresh frame."""
    try:
        files = sorted(glob.glob(os.path.join(HUD_CACHE, "stdin.*.json")), key=os.path.getmtime, reverse=True)
    except OSError:
        return None
    for f in files:
        try:
            if now_at - os.path.getmtime(f) > FRESH_USAGE_S:
                return None  # ordered: the rest are older
        except OSError:
            continue
        rl = _dict(_dict(_read_json(f)).get("rate_limits"))
        (s, sr), (c, five_reset) = _usage_window(rl.get("seven_day"), now_at), _usage_window(rl.get("five_hour"), now_at)
        if s is not None or c is not None:
            return {"semana": s, "semana_reset": sr, "cinco_h": c, "cinco_h_reset": five_reset}
    return None


def _duration(s):
    """`1d8h` or `0h19m`: what is left until the window rolls over."""
    s = max(int(s), 0)
    return f"{s // 86400}d{s % 86400 // 3600}h" if s >= 86400 else f"{s // 3600}h{s % 3600 // 60:02d}m"


def usage_level(usage, now_at=None):
    """(level, reason, reset): `pause` (week above the pause threshold) and `segura` (5 h above the threshold) refuse dispatch, `avisa` only warns, `ok` or
    `desconhecido` (no fresh frame) do nothing. `reset` is that of the window that decided, so the notice applies once per window."""
    if not usage:
        return "desconhecido", None, None
    now_at = now_at or time.time()
    cfg = {**DEFAULT_USAGE, **{k: v for k, v in _dict(_read_json(_path(USAGE))).items() if k in DEFAULT_USAGE and isinstance(v, (int, float))}}

    def txt(item_name, threshold):
        r = usage[f"{item_name}_reset"]
        return f"{'week' if item_name == 'semana' else '5 h window'} at {usage[item_name]:g}% (threshold {threshold:g}%)" + (f", turns over in {_duration(r - now_at)}" if r else "")

    s, c = usage["semana"], usage["cinco_h"]
    if s is not None and s >= cfg["semana_pausa"]:
        return "pausa", txt("semana", cfg["semana_pausa"]), usage["semana_reset"]
    if c is not None and c >= cfg["cinco_h"]:
        return "segura", txt("cinco_h", cfg["cinco_h"]), usage["cinco_h_reset"]
    if s is not None and s >= cfg["semana_avisa"]:
        return "avisa", txt("semana", cfg["semana_avisa"]), usage["semana_reset"]
    return "ok", None, None


def usage_check(priority_level=2, now_at=None, agent="claude"):
    """Refuses the dispatch with ValueError if plan usage went past the pause threshold (week) or the hold threshold (5 h); priority 1 passes the 5 h window
    hold, but not the week pause. With no fresh frame it does not refuse."""
    level, reason, _ = usage_level(plan_usage(now_at, agent), now_at)
    if level == "pausa" or (level == "segura" and priority_level != 1):
        append_event({"tipo": "uso_parou", "nivel": level, "motivo": reason, **({"agente": agent} if agent != "claude" else {})})
        raise ValueError(f"plan usage{_of_harness(agent)}: {reason}; nothing was dispatched. "
                         + ("Run orq pause to free up room." if level == "pausa" else "Wait for the window to turn over, or adjust the threshold in usage.json."))


def _of_harness(agent):
    """" do Codex" for the usage text of another harness; Claude's gets no suffix, as before."""
    return "" if agent == "claude" else f" of {agent.capitalize()}"


def _propose_handoff(agent, now_at=None):
    """" Em vez de pausar, passe: orq passar <dispatch> --para <outro>; …" for each `agent` worker that `orq pause_workers` would pick, if the other
    harness is in `ok` (week below `semana_avisa`). Empty with no worker to pause, no number for the other or with the other also tight."""
    other_item = next((h for h in HARNESSES if h != agent), None)
    if not other_item or usage_level(plan_usage(now_at, other_item), now_at)[0] != "ok":
        return ""
    target, _ = _pause_list(agents(), read_events(), set(), None, _dict(_cursor_ro().get("pausados")))
    line_list = [f"orq switch {a['dispatch']} --to {other_item}" for a in target if (a.get("agente") or "claude") == agent]
    return f" Instead of pausing, switch to {other_item.capitalize()}: {'; '.join(line_list)}." if line_list else ""


def usage_notify(now_at=None, agent="claude"):
    """Types into the coordinator one notice per (level, window): the first panel round after crossing the threshold, and again only if the level rises or the
    window rolls over. Coordinator busy: the next round tries. Returns the panel lines."""
    g = _manager_cfg()
    if not g or not g.get("coordenador"):
        return []
    level, reason, reset = usage_level(plan_usage(now_at, agent), now_at)
    k = "uso_aviso" if agent == "claude" else f"uso_aviso_{agent}"
    if level in ("ok", "desconhecido"):
        if level == "ok" and _cursor_ro().get(k):
            _cursor_mut(lambda c: c.pop(k, None))
        return []
    key_name = f"{level}:{reset}"
    if _dict(_cursor_ro().get(k)).get("chave") == key_name:
        return []
    action = {"pausa": "orq dispatch refuses; run orq pause", "segura": "orq dispatch refuses until the window turns over", "avisa": "avoid dispatching what is not urgent"}[level]
    text_value = f"orq: plan usage{_of_harness(agent)}, {reason}. {action[0].upper() + action[1:]}."
    if level == "pausa":
        text_value += _propose_handoff(agent, now_at)
    if notify_coordinator(g["coordenador"], text_value) not in ("enviado", "adiado"):
        return []
    _cursor_mut(lambda c: c.__setitem__(k, {"chave": key_name, "ts": now()}))
    append_event({"tipo": "uso_aviso", "nivel": level, "motivo": reason, **({"agente": agent} if agent != "claude" else {})})
    return [f"plan usage{_of_harness(agent)}: {level} ({reason}), coordinator notified"]


def default_priority(title):
    """1 (high) to 3 (low) by the title prefix: security and production high; failover, diagnosis and panel low; the rest 2."""
    return 1 if _HIGH_WORKSTREAM.search(title or "") else 3 if _LOW_WORKSTREAM.search(title or "") else 2


def priority_of(events, task, dispatch, title):
    """The task priority: the last one from `orq priority_level`, otherwise the dispatch one (--prioridade), otherwise the title prefix one."""
    for e in reversed(events):
        if e.get("tipo") == "prioridade" and e.get("task") == task:
            return e["valor"]
    for e in reversed(events):
        if e.get("tipo") == "despacho" and e.get("prioridade") and (e.get("dispatch") == dispatch or (task and e.get("task") == task)):
            return e["prioridade"]
    return default_priority(title)


def set_priority(task, value):
    """`orq priority_level <task> <1-3>`: changes a task's priority (the dispatch one and the default one count for less). It applies before and after the task runs; aberto.json is rebuilt so the digest and orq agentes see the change."""
    if value not in (1, 2, 3):
        raise ValueError("the priority is 1 (high), 2 or 3 (low)")
    if not re.fullmatch(r"task_\w+", task or ""):
        raise ValueError(f"{task!r} is not a task id (task_…)")
    ev = append_event({"tipo": "prioridade", "task": task, "valor": value})
    refresh_bg()
    return ev


def _pause_list(agent_rows, events, tasks, up_to_priority, paused):
    """(to pause, preserved): the chosen live workers and those the criterion spared, each with `priority_level`.

    `tasks` (task or dispatch ids) counts alone. Without it: priority >= `up_to_priority`; without the number, the low one (3) and those that are still only investigating.
    In final verification the phase spares in either case."""
    live = [a for a in agent_rows if a["estado"] in ("rodando", "travado", "nao_comecou", "parado", "perguntando") and a["dispatch"] not in paused
             and a.get("terminal") != os.environ.get("ORCA_TERMINAL_HANDLE")]
    if tasks:
        lost = set(tasks) - {x for a in live for x in (a["task"], a["dispatch"])}
        if lost:
            raise ValueError(f"no live worker for {', '.join(sorted(lost))}")
        return [a for a in live if a["task"] in tasks or a["dispatch"] in tasks], []
    chosen, spared = [], []
    for a in live:
        phase = a.get("fase") or ""
        if a["prioridade"] >= (up_to_priority or 3) or (up_to_priority is None and _INITIAL_PHASE.search(phase)):
            (spared if _FINAL_PHASE.search(phase) else chosen).append(a)
    return chosen, spared


def pause_workers(tasks=(), up_to_priority=None, run=None, dry_run=False):
    """Pauses workers to open slack in the plan: sends MSG_PAUSE to each (steer), waits for the new PAUSE.md in its worktree, closes the terminal and stores the
    dispatch in cursor.json `paused` (session, cwd, model). The dispatch stays `dispatched` in Orca with no terminal; `orq resume --paused` starts it again.

    A worker with no stored session or cwd is not paused (there would be no way back). Without PAUSE.md by the deadline, the terminal stays open. Returns
    {pausados: [{dispatch, task, titulo, prioridade, estado…}], preservados: […]}."""
    if up_to_priority is not None and up_to_priority not in (1, 2, 3):
        raise ValueError("--up-to-priority expects 1, 2 or 3")
    target, spared = _pause_list(agents(run), read_events(), set(tasks), up_to_priority, _dict(_cursor_ro().get("pausados")))
    delivered_items = {_msg_dispatch(m) for m in orca("inbox", "--limit", "200", timeout=20)["messages"] if isinstance(m, dict) and m.get("type") == "worker_done"} if target else set()
    done_items = [a for a in target if a["dispatch"] in delivered_items]  # the worker already sent worker_done: there is no turn to pause nor a PAUSE.md to come; the path is release
    if done_items and tasks:
        raise ValueError("; ".join(f"{a['dispatch']} already delivered (worker_done): run orq release {a['dispatch']}, not pause" for a in done_items))
    target = [a for a in target if a["dispatch"] not in delivered_items]
    turns = _turns_ro()
    output, waiting = [{"dispatch": a["dispatch"], "task": a["task"], "run": a["run"], "titulo": a.get("titulo"), "prioridade": a["prioridade"], "estado": "entregue",
                      "aviso": f"already delivered (worker_done): run orq release {a['dispatch']}"} for a in done_items], {}
    for a in target:
        t = _dict(turns.get(a["dispatch"]))
        cwd = t.get("cwd") or _checkpoint(a["dispatch"]).get("caminho")
        line = {"dispatch": a["dispatch"], "task": a["task"], "run": a["run"], "titulo": a.get("titulo"), "prioridade": a["prioridade"], "fase": a.get("fase"),
                 "agente": a.get("agente") or t.get("harness") or "claude", "modelo": a.get("modelo"), "effort": a.get("effort"), "sessao": t.get("sessao"), "cwd": cwd,
                 "terminal": a["terminal"]}
        if not (line["sessao"] and cwd and os.path.isdir(cwd) and a["terminal"]):
            output.append({**line, "estado": "sem_sessao", "aviso": "no session_id, worktree or terminal: no way back, was not paused"})
        elif dry_run:
            output.append({**line, "estado": "a_pausar"})
        else:
            file_path = cwd
            before = _pause_mtime(cwd)
            try:
                steer(a["task"], MSG_PAUSE, a["run"])
            except (ValueError, RuntimeError, subprocess.TimeoutExpired) as e:
                output.append({**line, "estado": "falhou", "aviso": str(e)})
                continue
            waiting[a["dispatch"]] = (line, file_path, before)
    end = time.time() + PAUSE_WAIT_S
    while waiting:
        for d, (line, file_path, before) in list(waiting.items()):
            if _pause_mtime(file_path) > before:
                waiting.pop(d)
                output.append(_close_paused(line))
        if waiting and time.time() < end:
            time.sleep(PAUSE_POLL_S)
        elif waiting:
            output += [{**line, "estado": "sem_pausa_md", "aviso": f"no new PAUSE.md in {PAUSE_WAIT_S:g} s: the terminal stays open"} for line, _, _ in waiting.values()]
            break
    return {"pausados": output, "preservados": [{k: a.get(k) for k in ("dispatch", "task", "titulo", "prioridade", "fase")} for a in spared]}


def _pause_mtime(cwd):
    """The newest mtime between PAUSE.md and PAUSA.md (a worker that follows the old spec) in the worktree; 0 with neither."""
    return max((os.path.getmtime(p) for p in (os.path.join(cwd, "PAUSE.md"), os.path.join(cwd, "PAUSA.md")) if os.path.exists(p)), default=0)


def _close_paused(line):
    """PAUSE.md arrived: closes the worker terminal, stores the dispatch in `paused` and writes the event."""
    terminated = terminate_children(line["cwd"], line["agente"])  # background shells and monitors would be left orphaned and keep weighing on the machine
    try:
        orca("close", "--terminal", line["terminal"], area="terminal")
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        return {**line, "estado": "falhou", "aviso": f"terminal close failed ({e}); PAUSE.md is written", "encerrados": terminated}
    keep = {k: line[k] for k in ("task", "run", "titulo", "prioridade", "agente", "modelo", "effort", "sessao", "cwd", "terminal")}
    _cursor_mut(lambda c: c.setdefault("pausados", {}).__setitem__(line["dispatch"], {**keep, "desde": now()}))
    append_event({"tipo": "pausa_plano", "dispatch": line["dispatch"], **keep, "encerrados": terminated})
    return {**line, "estado": "pausado", "encerrados": terminated}


def resume_paused(run=None, force=False):
    """Starts back up (the harness resume, with MSG_RESUME) the dispatches in cursor.json `paused`, highest priority first, and removes them from the list.
    With the harness plan usage still above the threshold, its worker stays (`uso_alto`), unless `force`: otherwise it would come back only to pause again. If
    that holds for all of them, it refuses."""
    paused = {d: p for d, p in _dict(_cursor_ro().get("pausados")).items() if not run or p.get("run") == run}
    high_agents = {}
    for agent_row in {p.get("agente") or "claude" for p in paused.values()} if not force else ():
        level, reason, _ = usage_level(plan_usage(agent=agent_row))
        if level in ("pausa", "segura"):
            high_agents[agent_row] = reason
    if paused and high_agents and all((p.get("agente") or "claude") in high_agents for p in paused.values()):
        raise ValueError(f"plan usage still high: {'; '.join(f'{m}{_of_harness(agent_row)}' for agent_row, m in high_agents.items())}. Wait for the window to turn over or use --force")
    res, cfg = [], machine_cfg()
    occupancy = machine_occupancy() if paused else None
    reading = machine_read()
    pressure, services = machine_level(reading, cfg), _services(read_events())
    for d, p in sorted(paused.items(), key=lambda kv: kv[1].get("prioridade") or 2):
        if (p.get("agente") or "claude") not in high_agents and (reason := machine_bar_item(p.get("modelo"), p["run"], occupancy, cfg, pressure, reading, p.get("prioridade"), d in services)):
            res.append({"dispatch": d, "task": p["task"], "titulo": p["titulo"], "estado": "sem_vaga", "aviso": f"{reason}; stays paused, run orq resume --paused when a slot opens"})
            continue
        if (p.get("agente") or "claude") in high_agents:
            res.append({"dispatch": d, "task": p["task"], "titulo": p["titulo"], "estado": "uso_alto", "aviso": high_agents[p.get("agente") or "claude"]})
            continue
        line = {"dispatch": d, "task": p["task"], "run": p["run"], "titulo": p["titulo"], "prioridade": p.get("prioridade"), "modelo": p.get("modelo"),
                 "sessao": p["sessao"], "cwd": p["cwd"], "terminal": p["terminal"]}
        if not os.path.isdir(p["cwd"]):
            res.append({**line, "estado": "sem_worktree", "aviso": f"the folder {p['cwd']} does not exist: nothing was started"})
            continue
        try:
            cp = _checkpoint(d)
        except (RuntimeError, subprocess.TimeoutExpired):
            cp = {"head": None, "sujo": None}
        r = _start_session(line, p["sessao"], p.get("modelo"), cp, f"start another worker with: orq relaunch {d} --note 'the paused session could not be resumed'", MSG_RESUME,
                          p.get("agente") or "claude", p.get("effort"))
        if r["estado"] != "falhou":
            _occupy(occupancy, d, p.get("modelo"))
            _cursor_mut(lambda c, d=d: c.get("pausados", {}).pop(d, None))
            append_event({"tipo": "pausa_fim", "dispatch": d, "task": p["task"], "terminal": r.get("novo")})
        res.append(r)
    return res


def pause_text(res):
    """One line per paused or spared worker."""
    ls = [f"{w['dispatch']} P{w['prioridade']} {w.get('titulo') or w['task']}: {w['estado']}" + (f" ({w['aviso']})" if w.get("aviso") else "")
          + (f" [{len(w['encerrados'])} background processes ended]" if w.get("encerrados") else "") for w in res["pausados"]]
    ls += [f"{w['dispatch']} P{w['prioridade']} {w.get('titulo') or w['task']}: preserved (phase {w.get('fase')})" for w in res["preservados"]]
    return "\n".join(ls) or "no worker to pause"


# ---------- hibernate idle worker and wake it when needed (ticket 60) ----------

# Each worker is a live `claude` (with the session's MCP servers) that takes memory even when idle at the prompt. Hibernate = store the session in
# cursor.json `hibernated` and close the terminal; the task stays as it is in Orca. Wake = the harness resume (the same path as `orq resume`),
# with the message that woke it. The criterion is deterministic (screen, worker hooks, processes, orq state), no LLM. Design: docs/design.md.
HIBERNATE_FILE = "hibernate.json"  # {"min": 15, "externa_min": 2}: the thresholds, on top of these defaults
HIBERNATE_MIN = float(os.environ.get("ORQ_HIBERNA_MIN") or 15)  # minutes idle at the prompt (or delivered without release) until hibernating
HIBERNATE_EXTERNAL_MIN = float(os.environ.get("ORQ_HIBERNA_EXTERNA_MIN") or 2)  # waiting on something external that orq knows about: hibernates soon after this idle time
HIBERNATE_ROUND_S = float(os.environ.get("ORQ_HIBERNA_VOLTA_S") or 60)  # the panel checks the workers every so often, not on every 10 s loop
HIBERNATE_RSS_WAIT_S = float(os.environ.get("ORQ_HIBERNA_RSS_ESPERA_S") or 5)  # how long to wait for the closed terminal's process to vanish from ps before measuring RSS afterwards
SCREEN_BUSY = re.compile(r"esc to interrupt", re.I)  # the Claude Code and Codex spinner: the turn is running
MSG_WAKE = ("You were hibernated: the coordinator closed the terminal for being idle to free memory and has now woken you, in the same session. What arrived: {texto}\n"
              "First: check git status and the state of your worktree. When you finish, send worker_done as before; if Orca refuses because of the new handle "
              "or the task already delivered, write the final report in a final-report.md file at the root of your worktree and show the path in the terminal.")


def _hibernated():
    """cursor.json `hibernados`: {dispatch: {task, run, titulo, agente, modelo, effort, sessao, cwd, terminal, entregue, desde, motivo, rss_liberado_mb}}."""
    return {d: p for d, p in _dict(_cursor_ro().get("hibernados")).items() if isinstance(p, dict)}


def _forget_hibernated(dispatch):
    """Removes the dispatch from `hibernated` (woken, released or stopped); without the entry it writes nothing."""
    if dispatch in _hibernated():
        _cursor_mut(lambda c: c.get("hibernados", {}).pop(dispatch, None))


def _hibernate_cfg():
    """{min, externa_min} in minutes: hibernar.json on top of ORQ_HIBERNA_MIN, ORQ_HIBERNA_EXTERNA_MIN and the defaults (15 and 2)."""
    cfg = {"min": HIBERNATE_MIN, "externa_min": HIBERNATE_EXTERNAL_MIN}
    for k, v in _dict(_read_json(_path(HIBERNATE_FILE))).items():
        if k in cfg and isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0:
            cfg[k] = v
    return cfg


def _agent_of(p):
    """The harness of a `ps` process (claude, codex) by executable name, or None."""
    item_name = os.path.basename((p.get("args") or "").split(" ", 1)[0])
    return item_name if item_name in HARNESS else None


def _processes(with_cwd=True, include_all=False):
    """[{pid, ppid, rss (KB), cpu (%), args, cwd}] from `ps`, with the cwd (lsof) only for agent processes (`with_cwd`), or for all of them (`include_all`); None if ps fails. `cwd` None: lsof did not report it.

    ORQ_PROCESSOS: path to a JSON with that list, in place of ps and lsof (tests)."""
    if os.environ.get("ORQ_PROCESSOS"):
        return _read_json(os.environ["ORQ_PROCESSOS"])
    try:
        output = subprocess.run(["ps", "-axo", "pid=,ppid=,rss=,pcpu=,command="], capture_output=True, text=True, timeout=10, check=True).stdout
    except (subprocess.SubprocessError, OSError) as e:
        log(f"hibernar: ps: {type(e).__name__}: {e}")
        return None
    ps = []
    for l in output.splitlines():
        c = l.split(None, 4)
        if len(c) == 5 and all(x.isdigit() for x in c[:3]) and re.fullmatch(r"\d+(\.\d+)?", c[3]):
            ps.append({"pid": int(c[0]), "ppid": int(c[1]), "rss": int(c[2]), "cpu": float(c[3]), "args": c[4], "cwd": None})
    owners = ps if include_all else [p for p in ps if _agent_of(p)] if with_cwd else []
    if owners:
        try:
            lsof = subprocess.run(["lsof", "-a", "-d", "cwd", "-Fpn", *([] if include_all else ["-p", ",".join(str(p["pid"]) for p in owners)])], capture_output=True, text=True, timeout=30).stdout
        except (subprocess.SubprocessError, OSError) as e:
            log(f"hibernar: lsof: {type(e).__name__}: {e}")
            return ps
        pid = None
        for l in lsof.splitlines():
            if l[:1] == "p" and l[1:].isdigit():
                pid = int(l[1:])
            elif l[:1] == "n" and pid is not None:
                owner_name = next((p for p in owners if p["pid"] == pid), None)  # with `include_all`, lsof sees a process that was born after ps
                if owner_name:
                    owner_name["cwd"] = l[1:]
    return ps


def _linked_root(wt):
    """The real path of `wt` if it is a linked worktree (`.git` file); None for no path, `/`, home or the main checkout (a `.git` directory runs the coordinator,
    the manager and the `--worktree current` workers)."""
    root = os.path.realpath(wt) if wt else ""
    if root in ("", "/", os.path.realpath(HOME)) or not os.path.isfile(os.path.join(root, ".git")):
        return None
    return root


def _worker_tree(procs, root):
    """The pids of every harness process (claude, codex) with `cwd` inside `root` and of everything that starts below them."""
    return _descendants(procs, [p["pid"] for p in procs if _agent_of(p) and p.get("cwd") and _inside(p["cwd"], root)])


def worktree_owned_pids(wt):
    """The worker's own processes in worktree `wt` right now (`_worker_tree`), to take before the terminal closes: after that the agent is gone and what it left
    behind is no longer below any harness. Empty with no linked worktree or no process list."""
    root = _linked_root(wt)
    return _worker_tree(_processes(include_all=True) or [], root) if root else set()


def _process_list(ps):
    return [{"pid": p["pid"], "comando": (p.get("args") or "")[:200], "cwd": p.get("cwd")} for p in ps]


def terminate_worktree_processes(wt, wait_s=None, owned=None, all_cwd=False):
    """TERM, wait and KILL only on the worker's own processes with cwd inside worktree `wt` (Meteor, node, watchers, docker compose of the E2E stack).

    `cwd` proves where a process is, not whose it is, so the set is the intersection of the cwd and the worker's tree: `owned` (pids taken by `worktree_owned_pids` before
    the terminal closed) or, without it, what is below a harness process in that worktree now. The rest is listed in the `processes` event and left alone; `all_cwd`
    (`orq release --processos`) lifts that filter and keeps the circuit breaker. A set over ORQ_ENCERRA_MAX is not terminated: it is recorded (`op: recusado`) and
    becomes a pending item. Spares the orq process and those above it (the terminal of whoever called). Returns {"encerrados", "kill", "nao_encerrados", "recusado"}
    and writes the event; None without the process list or with a root that is not a linked worktree."""
    root = _linked_root(wt)
    if not root:
        return None
    procs = _processes(include_all=True)
    if procs is None:
        return None
    by_pid, protected, pid = {p["pid"]: p for p in procs}, set(), os.getpid()
    while pid in by_pid and pid not in protected:
        protected.add(pid)
        pid = by_pid[pid]["ppid"]
    protected.add(os.getpid())
    owned = _worker_tree(procs, root) if owned is None else owned

    def inside():
        return [p for p in (_processes(include_all=True) or []) if p.get("cwd") and _inside(p["cwd"], root) and p["pid"] not in protected]
    found = inside()
    mine = [p for p in found if all_cwd or p["pid"] in owned]
    rest = [p for p in found if p not in mine]
    res = {"encerrados": 0, "kill": 0, "nao_encerrados": len(rest), "recusado": 0}
    ev = {"tipo": "processos", "worktree": root, "nao_encerrados": len(rest), **({"restantes": _process_list(rest)} if rest else {})}
    if len(mine) > END_MAX:
        append_event({**ev, "op": "recusado", "limite": END_MAX, "lista": _process_list(mine)})
        try:
            pending_add(f"processos-{os.path.basename(root)}"[:60], "acao", f"{len(mine)} processes in {root} not terminated (limit {END_MAX})",
                        detail="A sweep this large means the filter is wrong, not that there are that many to end. See the `processos` event (op recusado) for the list.")
        except (ValueError, OSError, RuntimeError) as e:
            log(f"processos: pendencia: {type(e).__name__}: {e}")
        return {**res, "recusado": len(mine)}
    targets = {p["pid"] for p in mine}
    for p in targets:
        _signal_name(p, signal.SIGTERM)
    end = time.time() + (END_WAIT_S if wait_s is None else wait_s)
    remaining_pids = [p for p in inside() if p["pid"] in targets]
    while remaining_pids and time.time() < end:
        time.sleep(0.1)
        remaining_pids = [p for p in inside() if p["pid"] in targets]
    for p in remaining_pids:  # the cwd is checked again: the pid may have been reused
        _signal_name(p["pid"], signal.SIGKILL)
    res.update(encerrados=len(targets), kill=len(remaining_pids))
    if targets or rest:
        append_event({**ev, "op": "encerrar", "encerrados": res["encerrados"], "kill": res["kill"], "lista": _process_list(mine)})
    return res


def sweep_notices(r, dispatch=None):
    """The notice lines of a `terminate_worktree_processes` result (None: nothing to say)."""
    if not r:
        return []
    out = []
    if r["encerrados"]:
        out.append(f"{r['encerrados']} worktree process(es) terminated" + (f", {r['kill']} only with KILL" if r["kill"] else ""))
    if r["nao_encerrados"]:
        out.append(f"{r['nao_encerrados']} worktree process(es) not terminated (not provably the worker's)" + (f": run `orq release {dispatch} --processos`" if dispatch else ""))
    if r["recusado"]:
        out.append(f"{r['recusado']} worktree process(es) over ORQ_ENCERRA_MAX ({END_MAX}): none terminated, pending item opened")
    return out


def _descendants(procs, pids):
    """The pids in `pids` and all the processes below them."""
    seen, queue = set(), list(pids)
    while queue:
        pid = queue.pop()
        if pid not in seen:
            seen.add(pid)
            queue += [p["pid"] for p in procs if p["ppid"] == pid]
    return seen


def agents_rss_mb(procs):
    """Sum of the RSS, in MB, of agent processes (claude, codex) and of everything that starts below them (the session's MCP servers); None without the list."""
    if procs is None:
        return None
    live = _descendants(procs, [p["pid"] for p in procs if _agent_of(p)])
    return round(sum(p["rss"] for p in procs if p["pid"] in live) / 1024)


def _worker_process(procs, cwd, agent):
    """(pids, child): the agent processes whose cwd is the worker's and whether any of its Bash commands is still running.

    `child` True/False; None when there is no proof: no ps, no agent process in that cwd (lsof failed or the worker `cd`ed) or a harness with no
    child pattern (HARNESS[…]["filho"]). Two agents in the same cwd (the coordinator in the same worktree) count together: when in doubt, there is a child."""
    default = HARNESS.get(agent, {}).get("filho")
    pids = [p["pid"] for p in procs or [] if _agent_of(p) == agent and p.get("cwd") == cwd]
    if not default or not pids:
        return pids, None
    return pids, any(default.search(p["args"]) for p in procs if p["pid"] in _descendants(procs, pids) - set(pids))


def _protected(run):
    """The terminals that never hibernate: this process's, the coordinator and the manager from gerente.json and the Run's coordinator."""
    g = _manager_cfg() or {}
    try:
        coord_handle = (orca("run-show", "--id", run)["run"] or {}).get("coordinator_handle")
    except (RuntimeError, subprocess.TimeoutExpired, OSError, KeyError):
        coord_handle = None
    return {x for x in (os.environ.get("ORCA_TERMINAL_HANDLE"), g.get("coordenador"), g.get("gerente"), coord_handle) if x}


END_MAX = int(os.environ.get("ORQ_ENCERRA_MAX") or 12)  # circuit breaker: a sweep of more processes than this ends none
END_WAIT_S = float(os.environ.get("ORQ_ENCERRA_ESPERA_S") or 5)  # from SIGTERM to SIGKILL on the paused worker's children


def _worker_children(procs, cwd, agent):
    """The processes the worker's Bash tool left alive: each agent child command (HARNESS[…]["filho"]) and everything that starts below it. Excluding the agent, the
    session's MCP servers (closing the terminal takes them) and the ancestors of this process. [] with no list, no agent in that cwd or a harness with no pattern."""
    default = HARNESS.get(agent, {}).get("filho")
    pids, _ = _worker_process(procs, cwd, agent)
    if not default or not pids:
        return []
    roots = [p["pid"] for p in procs if p["pid"] in _descendants(procs, pids) - set(pids) and default.search(p["args"])]
    parent = {p["pid"]: p["ppid"] for p in procs}
    ancestor_pids, eu = set(), os.getpid()
    while eu in parent and eu not in ancestor_pids:
        ancestor_pids.add(eu)
        eu = parent[eu]
    return [p for p in procs if p["pid"] in _descendants(procs, roots) and p["pid"] not in ancestor_pids]


def _signal_name(pid, sig):
    """Sends `sig` to the pid. ORQ_PROCESSOS (tests): removes the process from the JSON; SIGTERM removes it only if it does not have `ignora_term`."""
    if os.environ.get("ORQ_PROCESSOS"):
        file_path = os.environ["ORQ_PROCESSOS"]
        ps = _read_json(file_path)
        json.dump([p for p in ps if p["pid"] != pid or (sig == signal.SIGTERM and p.get("ignora_term"))], open(file_path, "w"))
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.kill(pid, sig)


def terminate_children(cwd, agent):
    """Ends the processes the worker left in the background (tests, node, a monitor's sleep/until): SIGTERM, and after END_WAIT_S SIGKILL on those that
    remained. Nothing outside the agent tree is touched. Returns [{pid, args, sinal}] (`signal_name`: the last one that killed it: TERM or KILL)."""
    children = _worker_children(_processes(), cwd, agent)
    for p in children:
        _signal_name(p["pid"], signal.SIGTERM)
    live = {p["pid"] for p in children}
    end = time.time() + END_WAIT_S
    while live and time.time() < end:
        live &= {p["pid"] for p in _processes() or ()}
        if live:
            time.sleep(0.2)
    for pid in live:
        _signal_name(pid, signal.SIGKILL)
    return [{"pid": p["pid"], "args": p["args"][:200], "sinal": "KILL" if p["pid"] in live else "TERM"} for p in children]


def _children_by_task():
    """{task: n background processes} of the live workers that have any, so the pressure notice shows who weighs; {} without ps."""
    procs, turns = _processes(), _turns_ro()
    if not procs:
        return {}
    res = {}
    for a in agents():
        t = _dict(turns.get(a["dispatch"]))
        n = len(_worker_children(procs, t.get("cwd"), a.get("agente") or t.get("harness") or "claude")) if t.get("cwd") else 0
        if n:
            res[a["task"]] = n
    return res


def _busy_screen(handle, agent):
    """The reason if the terminal screen shows the turn running (spinner), a background process or a menu waiting for an answer; otherwise None."""
    tail = _deep_get(orca("read", "--terminal", handle, "--screen", "--limit", str(SCREEN_LINES), area="terminal", timeout=10), "terminal", "tail") or []
    screen = "\n".join(map(str, tail[-15:]))
    waiting = HARNESS[agent]["tela"]["espera"]
    return ("spinner on screen" if SCREEN_BUSY.search(screen) else f"{waiting.search(screen).group(0).strip()} (screen)" if waiting and waiting.search(screen)
            else "menu waiting for an answer on screen" if screen_question(tail, agent) else None)


def _woken_at(events):
    """{dispatch: ts of the last `wake`}: the new resume postpones the next hibernation (the new turn may not be in turnos.json yet)."""
    return {e.get("dispatch"): e.get("ts") for e in events if e.get("tipo") == "acordar"}


def _external_wait(a, pending, prs, tks):
    """What orq already knows the worker is waiting on from outside, as text, or None: branch in the integrator queue, user pending item linked to the task, open PR waiting for merge, blocked ticket."""
    if a.get("estado") == "aguardando_integracao" and a.get("integracao"):
        return f"integration of {a['integracao']['branch']} (ticket {a['integracao']['ticket']})"
    for p in pending:
        if p.get("task") == a["task"]:
            return f"pending item {p.get('id')}"
    for i in prs:
        if i.get("task") == a["task"] and i.get("estado") == "aberto":
            return f"PR #{i.get('numero')} waiting for merge"
    status = {t["num"]: t["status"] for t in tks}
    for t in tks:
        if t.get("task") == a["task"]:
            open_items = [n for n in t["blocked_by"] if status.get(n) != STATUS_CLOSED]
            if open_items:
                return f"ticket {t['num']} blocked by {', '.join(open_items)}"
    return None


def hibernate_reason(a, now_at, cfg, pending=(), prs=(), tks=(), woken_at=None):
    """Pure: why worker `a` (a row from agentes()) should hibernate, or None. Only looks at orq state; the screen and the processes are checked afterwards.

    Only the ended turn counts (end of turn in the worker hooks, no heartbeat or prompt after): `stopped`, what waits on purpose (`running` with
    a declared wait) and `delivered` without release. A stuck question, locked and the screen with a background shell stay out (those keep escalating).
    Three reasons: waiting on something external that orq knows about (already past cfg.externa_min), delivered and not released, or stopped (both past cfg.min)."""
    end, start_at = _ts(a.get("turno_fim")), _ts(a.get("turno_inicio"))
    if a["estado"] not in ("parado", "rodando", "entregue", "aguardando_integracao") or a.get("retido") or a.get("tela") or not end or (start_at and start_at > end):
        return None
    if a["estado"] != "entregue" and a.get("turno") != "parado":
        return None
    since = max(end, _ts(woken_at) or end)
    stopped_min = (now_at - since).total_seconds() / 60
    external = _external_wait(a, pending, prs, tks) if a["estado"] != "entregue" else None
    if external and stopped_min >= cfg["externa_min"]:
        return f"waiting: {external}"
    if stopped_min >= cfg["min"]:
        return "delivered and not released" if a["estado"] == "entregue" else "idle at the prompt"
    return None


def _hibernate_agent(a, reason, procs, force=False):
    """Hibernates worker `a` if nothing prevents it and returns the row with `state`: hibernado, recusado (with `notice`) or falhou. Checks, in this order: protected
    terminal (coordinator, manager), orq worker (session and cwd stored by the hooks, for the resume), screen (spinner, background shell, menu),
    free inbox (`free_terminal`) and live child process (E2E, test, build). Without proof about a child process only `force` passes."""
    t = _dict(_turns_ro().get(a["dispatch"]))
    agent = a.get("agente") or t.get("harness") or "claude"
    try:
        cwd = t.get("cwd") or _checkpoint(a["dispatch"]).get("caminho")
    except (RuntimeError, subprocess.TimeoutExpired):
        cwd = None
    line = {"dispatch": a["dispatch"], "task": a["task"], "run": a["run"], "titulo": a.get("titulo"), "agente": agent, "modelo": a.get("modelo"), "effort": a.get("effort"),
             "sessao": t.get("sessao"), "cwd": cwd, "terminal": a.get("terminal"), "entregue": a["estado"] == "entregue", "motivo": reason}

    def refuse(notice):
        return {**line, "estado": "recusado", "aviso": notice}

    if not a.get("terminal") or a["estado"] in ("hibernado", "liberado", "encerrado"):
        return refuse(f"worker {a['estado']}: no terminal to close")
    if a["terminal"] in _protected(a["run"]):
        return refuse("it is the coordinator or the manager: they never hibernate")
    if not (line["sessao"] and cwd and os.path.isdir(cwd)) or agent not in HARNESS:
        return refuse("no session_id and worktree recorded by the worker hooks (not an orq worker, or no way back)")
    if a["estado"] == "perguntando":
        return refuse("there is a question waiting for an answer")
    try:
        busy = _busy_screen(a["terminal"], agent)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        return refuse(f"unreadable screen ({e})")
    if busy or (busy := free_terminal(a["terminal"])):
        return refuse(f"not free: {busy}")
    _, child = _worker_process(procs, cwd, agent)
    if child:
        return refuse("live child process (E2E, test or build)")
    if child is None and not force:
        return refuse("no proof that there is no child process (ps/lsof did not find the agent process in that worktree, or the harness has no pattern): use --force")
    before = agents_rss_mb(procs)
    keep = {k: line[k] for k in ("task", "run", "titulo", "agente", "modelo", "effort", "sessao", "cwd", "terminal", "entregue", "motivo")}
    _cursor_mut(lambda c: c.setdefault("hibernados", {}).__setitem__(a["dispatch"], {**keep, "desde": now()}))  # before the close: a crash in the middle doesn't let resume bring the worker up
    try:
        orca("close", "--terminal", a["terminal"], area="terminal")
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _forget_hibernated(a["dispatch"])
        return {**line, "estado": "falhou", "aviso": f"terminal close failed ({e})"}
    after, end = before, time.time() + HIBERNATE_RSS_WAIT_S
    while before is not None and (after := agents_rss_mb(_processes())) is not None and after >= before and time.time() < end:
        time.sleep(0.5)
    released_count = max(0, before - after) if before is not None and after is not None else None
    _cursor_mut(lambda c: c.get("hibernados", {}).get(a["dispatch"], {}).__setitem__("rss_liberado_mb", released_count))
    append_event({"tipo": "hibernar", "dispatch": a["dispatch"], **keep, "rss_antes_mb": before, "rss_depois_mb": after, "rss_liberado_mb": released_count})
    return {**line, "estado": "hibernado", "rss_antes_mb": before, "rss_depois_mb": after, "rss_liberado_mb": released_count}


def hibernate(target, run=None, force=False):
    """`orq hibernate <task|dispatch>`: hibernates the worker by hand, with the same refusals as the automatic one (`force` only overrides the lack of proof about a child process).
    Raises ValueError with the reason if it did not hibernate."""
    a = next((x for x in agents(run) if target in (x["task"], x["dispatch"])), None)
    if not a:
        raise ValueError(f"no worker for {target} (orq agents shows the dispatches)")
    r = _hibernate_agent(a, "manual", _processes(), force)
    if r["estado"] != "hibernado":
        raise ValueError(f"{target} did not hibernate: {r['aviso']}")
    return r


def hibernate_idle(now_at=None):
    """One manager round: hibernates what the criterion (hibernate_reason) picks and the screen and processes allow. At most once per HIBERNATE_ROUND_S.
    Returns the panel lines. A refusal on the screen or on the processes is silent: the next round checks again."""
    if time.time() - (_cursor_ro().get("hibernar_volta") or 0) < HIBERNATE_ROUND_S:
        return []
    _cursor_mut(lambda c: c.__setitem__("hibernar_volta", time.time()))
    now_at, cfg = now_at or datetime.now(timezone.utc), _hibernate_cfg()
    woken_at, pending, prs, tks = _woken_at(read_events()), _load_pending()["itens"], _prs_ro()["itens"], tickets()
    cand = [(a, m) for a in agents() if (m := hibernate_reason(a, now_at, cfg, pending, prs, tks, woken_at.get(a["dispatch"])))]
    if not cand:
        return []
    procs, line_list = _processes(), []
    for a, reason in cand:
        r = _hibernate_agent(a, reason, procs)
        if r["estado"] == "recusado":
            log(f"hibernar {a['task']}: recusado ({r['aviso']})")
        elif r["estado"] == "falhou":
            line_list.append(f"{a['task']}: did not hibernate ({r['aviso']})")
        else:
            line_list.append(f"{a['task']}: hibernated ({reason}" + (f", ~{r['rss_liberado_mb']} MB" if r.get("rss_liberado_mb") else "") + ")")
    return line_list


def wake(target, text_value=None):
    """Starts the hibernated worker back up (task or dispatch): the harness resume in a new terminal, through the same path as `orq resume`, with MSG_WAKE
    (the `text_value` of what arrived) and the way to escalate. Removes the dispatch from `hibernated` and writes `wake`, unless the terminal did not even start (`failed`: it stays
    hibernated, and the manager tries again). ValueError if `target` is not hibernated."""
    d, p = next(((d, p) for d, p in _hibernated().items() if target in (d, p.get("task"))), (None, None))
    if not d:
        raise ValueError(f"{target} is not hibernated")
    line = {"dispatch": d, "task": p["task"], "run": p["run"], "titulo": p.get("titulo") or d, "cwd": p["cwd"], "terminal": p["terminal"]}
    if not os.path.isdir(p["cwd"]):
        return {**line, "estado": "sem_worktree", "aviso": f"the folder {p['cwd']} does not exist: nothing was started"}
    try:
        cp = _checkpoint(d)
    except (RuntimeError, subprocess.TimeoutExpired):
        cp = {"head": None, "sujo": None}
    coord_handle = (_manager_cfg() or {}).get("coordenador") or os.environ.get("ORCA_TERMINAL_HANDLE")
    msg = f"{MSG_WAKE.format(texto=text_value or 'woken by hand (orq wake).')} " + MSG_ESCALATE.format(coord=coord_handle or "<coordinator>", run=p["run"], task=p["task"], dispatch=d)
    r = _start_session(line, p["sessao"], p.get("modelo"), cp, f"start another worker with: orq relaunch {d} --note 'the hibernated session could not be resumed'", msg,
                      p.get("agente") or "claude", p.get("effort"))
    if r["estado"] != "falhou":
        _forget_hibernated(d)
        append_event({"tipo": "acordar", "dispatch": d, "task": p["task"], "run": p["run"], "terminal": r.get("novo"), "motivo": _quote(text_value or "manual", 200)})
    return r


def wake_triggers():
    """One manager round: wakes the hibernated worker (that has not delivered yet) whose pending item linked to the task was answered, or whose PR was merged or closed,
    after it hibernated. The steer and the answer wake it right away, through their own commands. Returns the panel lines.
    ponytail: `failed` retries every round (10 s); with Orca refusing the terminal this becomes log noise, with no retry limit."""
    hib = {d: p for d, p in _hibernated().items() if not p.get("entregue")}
    if not hib:
        return []
    events, line_list = read_events(), []
    for d, p in hib.items():
        e = next((e for e in reversed(events) if e.get("task") == p["task"] and (e.get("ts") or "") >= p["desde"]
                  and (e.get("tipo") == "pend" and e.get("op") == "done" or e.get("tipo") == "pr" and e.get("op") in ("entrou", "fechou"))), None)
        if not e:
            continue
        text_value = (f"pending item {e.get('pend')} was answered: {e.get('resposta') or 'closed without an answer'}" if e["tipo"] == "pend"
                 else f"PR #{e.get('numero')} " + (f"entered {e.get('base')}" if e["op"] == "entrou" else "was closed without merge"))
        r = wake(d, text_value)
        line_list.append(f"{p['task']}: woken ({text_value[:70]}) -> {r['estado']}")
    return line_list


def hibernation_lines():
    """The `orq agents` line with how many workers are hibernated and the memory hibernation freed (RSS of agent processes, before and after)."""
    hib = _hibernated()
    mb = sum(p.get("rss_liberado_mb") or 0 for p in hib.values())
    return [f"Hibernated: {len(hib)}" + (f", ~{mb} MB of RSS released (sum of the claude processes and their MCPs, before and after the close)" if mb else "")] if hib else []


def hibernated_text(r):
    """One line for the result of `orq hibernate`."""
    return f"{r['dispatch']} {r.get('titulo') or r['task']}: hibernated ({r['motivo']})" + (f", RSS {r['rss_antes_mb']} -> {r['rss_depois_mb']} MB (~{r['rss_liberado_mb']} MB released)"
                                                                                       if r.get("rss_liberado_mb") is not None else "")


def _manager_notices():
    """gerente-aviso.json as {run: {vistos, ts}}: the ids of the messages (except heartbeat) the panel already notified to the coordinator, per Run, and the
    time of the last notice. The id holds even after confirmation: a delivery the coordinator already read is not notified again. The old format
    (`key_name`) does not count."""
    return {r: v for r, v in _dict(_read_json(_path("manager-notice.json"))).items() if isinstance(v, dict)}


SEEN_MAX = 50  # ids remembered per Run


def _absorb_run(run, bound_run):
    """One agent manager Run: binds the manager to it (with `bound_run`), consumes the inbox and confirms the heartbeat-only batches (confirm_batches, the
    same as the hook). Returns (panel line, messages left over for the coordinator or None). All under the lock: a Run's delivery is
    confirmed only in the same binding in which it was read."""
    with manager_lock():
        if bound_run:
            orca("run-use", "--id", run)
        ev, res = confirm_batches(run, orca("check", "--run", run))
    msgs = res.get("messages") or []
    line = f"{run}: {len(ev['heartbeats']) if ev else 0} heartbeat(s) absorbed"
    return line, (None if not msgs or only_heartbeats(msgs) else msgs)


RUN_STOPPED_CACHE = "run-parado.json"  # {run: when it was last seen alive}


def _run_stopped(run, left_over):
    """Reason to release the Run from the manager, or None: no open task, no message (`left_over`) and no activity for RUN_STOPPED_MIN minutes."""
    if left_over:
        return None
    seen = _read_json(_path(RUN_STOPPED_CACHE), {})
    if time.time() - seen.get(run, 0) < RUN_STOPPED_CACHE_S:
        return None
    r = run_summary((orca("run-show", "--id", run)["run"] or {"id": run}) | {"id": run}, orca("task-list", "--run", run)["tasks"])
    last_one = _ts(r["ultima"])
    if r["abertas"] or not last_one or datetime.now(timezone.utc) - last_one < timedelta(minutes=RUN_STOPPED_MIN):
        _write_json(_path(RUN_STOPPED_CACHE), {**seen, run: time.time()})
        return None
    return f"no open task or message for over {RUN_STOPPED_MIN:g} min (last activity {r['ultima']})"


def manager_release(parados):
    """Removes the Runs {run: reason} from gerente.json, one `soltar` event per Run. The last Run stays (without a Run the manager becomes the ticket 17 format).
    `orq dispatch_worker` on a released Run rebinds it (_adopt)."""
    with manager_lock():
        g = _manager_cfg()
        for r, reason in parados.items():
            if r in g["runs"] and len(g["runs"]) > 1:
                g["runs"].remove(r)
                _write_json(_path(MANAGER), {k: v for k, v in g.items() if k != "run"})
                append_event({"tipo": "gerente", "op": "soltar", "terminal": g["gerente"], "run": r, "motivo": reason})


def _touches_panel():
    """Marks `manager-alive` now: the panel is alive, even in the middle of a slow round (the panel shell only touches it between one round and the next)."""
    p = _path(PANEL_ALIVE)
    with contextlib.suppress(OSError):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "a"):
            os.utime(p)


def _write_round(secs):
    """Stores the round duration in gerente.json (the last REMEMBERED_ROUNDS): the stuck-panel notice limit follows their average."""
    with manager_lock():
        g = _manager_cfg()
        if g.get("gerente") != os.environ.get("ORCA_TERMINAL_HANDLE"):
            return
        loops = [v for v in g.get("voltas_s") or [] if isinstance(v, (int, float)) and not isinstance(v, bool)]
        _write_json(_path(MANAGER), {**{k: v for k, v in g.items() if k != "run"}, "voltas_s": [*loops[-(REMEMBERED_ROUNDS - 1):], round(secs, 1)]})


def manager_absorb():
    """One round of the agent manager panel, in its terminal: goes through the bound Runs (one `run-use` per Run, Orca binds one per terminal),
    absorbs heartbeat and, when a batch with something else is left over, types into the coordinator a notice in Orca's format, once per batch of
    messages, confirming nothing: the coordinator reads the delivery with `check --terminal <manager>`. Returns one line per Run for the panel.

    The coordinator's raw check only counts with the manager bound to the notice's Run: the round ends on it, and the following ones only revisit it (without leaving
    it) until the coordinator confirms the delivery, or until MANAGER_STUCK_S."""
    g = _manager_cfg()
    if not g or g.get("gerente") != os.environ.get("ORCA_TERMINAL_HANDLE"):
        return "agent manager off (orq manager bind --terminal <this terminal>, on the coordinator)"
    _cursor_mut(lambda c: c.__setitem__("gerente_volta", now()))  # the morning card reads here whether the manager was alive
    start_time = time.time()
    _touches_panel()
    bound_run = bool(g["runs"])  # gerente.json from ticket 17 (no runs): the Run is the one bound to the terminal, no rotating
    runs = g["runs"] or [r for r in [(orca("run-current")["run"] or {}).get("id")] if r]
    if not runs:
        return "agent manager has no bound Run: run orq manager bind again on the coordinator"
    notices, now_at = _manager_notices(), time.time()
    stuck_items = [r for r in runs if r in notices and 0 <= now_at - notices[r].get("ts", 0) < MANAGER_STUCK_S][:1]
    line_list, pending_messages = [], {}
    for r in [*stuck_items, *(x for x in runs if x not in stuck_items)]:
        line, msgs = _absorb_run(r, bound_run)
        _touches_panel()  # the loop back under heavy load takes over 60 s: the stamp can't wait for the end of it
        seen = set(notices.get(r, {}).get("vistos") or [])
        ids = sorted(str(m.get("id")) for m in msgs) if msgs else []
        if msgs:
            pending_messages[r] = ids
            kinds = ", ".join(sorted({str(m.get("type")) for m in msgs}))
            line += f"; {kinds} waiting for the coordinator" if set(ids) <= seen else f"; {kinds} waiting for the coordinator to be free"
        elif r in notices:
            notices[r]["ts"] = 0  # the box emptied: the coordinator confirmed, the Run stops being the stuck one
        line_list.append(line)
        if msgs and r in stuck_items:
            break  # stuck and still waiting: doesn't leave the Run
    fresh = [r for r, ids in pending_messages.items() if not set(ids) <= set(notices.get(r, {}).get("vistos") or [])]
    if pending_messages and bound_run:
        with manager_lock():  # the loop ends on the Run of the delivery the coordinator will read
            orca("run-use", "--id", next(iter(pending_messages)))
    for r in fresh:
        n = len(pending_messages[r])
        text_value = f"You have {n} orchestration message{'s' if n > 1 else ''}. Run `orca orchestration check --run {r} --terminal {g['gerente']}`."
        if notify_coordinator(g["coordenador"], text_value, minutes_elapsed=WAKE_IDLE_MIN) not in ("enviado", "adiado"):  # deferred (coordinator busy with a user, ticket 86): already a delivery, goes out in the context
            break  # coordinator mid-turn or with a draft (or Orca refused): nothing was typed, the next loop tries
        seen = [*(notices.get(r, {}).get("vistos") or []), *pending_messages[r]]
        notices[r] = {"vistos": seen[-SEEN_MAX:], "ts": time.time()}
        _write_json(_path("manager-notice.json"), notices)  # per notice: a failure in another Run after it doesn't repeat it
        line_list = [x.replace("waiting for the coordinator to be free", "coordinator notified") if x.startswith(f"{r}:") else x for x in line_list]
    if notices != _manager_notices():
        _write_json(_path("manager-notice.json"), notices)
    if bound_run:
        parados = {}
        for r in runs:
            try:
                reason = _run_stopped(r, r in pending_messages or r in notices and notices[r].get("ts"))
            except RuntimeError:  # Orca down: releases nothing
                continue
            if reason:
                parados[r] = reason
        manager_release(parados)
        line_list += [f"{r}: released from the manager ({m})" for r, m in parados.items()]
    try:
        line_list += redeliver_steers()
    except Exception as e:  # noqa: BLE001 - the panel doesn't go down because of steer tracking; the next loop tries
        log(f"steers: {type(e).__name__}: {e}")
    try:
        line_list += notify_screens()
    except Exception as e:  # noqa: BLE001 - an unreadable screen doesn't take down the panel; the next loop tries
        log(f"telas: {type(e).__name__}: {e}")
    try:
        line_list += [*pr_poll(), *pr_notify(), *deploy_verify(), *pr_production_open(), *notify_e2e_queue(), *usage_notify(), *usage_notify(agent="codex"), *deliver_notices(), *remind_round()]
    except Exception as e:  # noqa: BLE001 - same: gh being down doesn't take down the panel
        log(f"prs: {type(e).__name__}: {e}")
    try:
        line_list += hooks_broken_round()
    except Exception as e:  # noqa: BLE001 - the warning about hooks doesn't take down the panel; the next loop tries
        log(f"hooks quebrados: {type(e).__name__}: {e}")
    try:
        line_list += mate_lap()
    except Exception as e:  # noqa: BLE001 - the mates channel doesn't take down the panel; the next loop tries
        log(f"mates: {type(e).__name__}: {e}")
    try:
        line_list += mate_proposal_lap()
    except Exception as e:  # noqa: BLE001 - the proposal to open a mate doesn't take down the panel; the next loop tries
        log(f"proposta de mate: {type(e).__name__}: {e}")
    try:
        line_list += machine_round()
    except Exception as e:  # noqa: BLE001 - the dispatch queue doesn't take down the panel; the next loop tries
        log(f"fila de despacho: {type(e).__name__}: {e}")
    try:
        line_list += wake_stopped()
    except Exception as e:  # noqa: BLE001 - the notice to the idle coordinator doesn't take down the panel; the next loop tries
        log(f"acorda parado: {type(e).__name__}: {e}")
    try:
        line_list += [*wake_triggers(), *hibernate_idle()]
    except Exception as e:  # noqa: BLE001 - hibernating is a saving, it can't take down the panel; the next loop tries
        log(f"hibernar: {type(e).__name__}: {e}")
    try:
        line_list += mates_sleep()
    except Exception as e:  # noqa: BLE001 - same for the idle mate
        log(f"mates dormir: {type(e).__name__}: {e}")
    _touches_panel()
    _write_round(time.time() - start_time)
    return "\n".join(line_list)



# ---------- agent manager without a terminal: orq manager serve (ticket 128) ----------

SERVE_PID = "gerente-serve.pid"  # the pid of `orq manager serve` and its lock (flock while it lives): a free lock means serve is stopped, whatever pid is written
SERVE_STATE = "manager-state.json"  # {ts, pid, terminal, linhas, agentes, maquina: {cfg, leitura}}: what the TUI reads from the manager
SERVE_LOG = os.path.join("logs", "gerente.log")
SERVE_LAP_S = float(os.environ.get("ORQ_GERENTE_VOLTA_S") or 10)  # the interval of painel-agent-manager.sh
SERVE_DIGEST_ROUNDS = 6  # the digest every ~60 s, like the panel
SERVE_BUSY = 3  # exit code of `orq manager absorver` with serve alive: the panel only shows
LAUNCHD_LABEL = "com.orq.gerente"
LAUNCH_AGENTS = os.environ.get("ORQ_LAUNCH_AGENTS") or os.path.expanduser("~/Library/LaunchAgents")
LAUNCHCTL = os.environ.get("ORQ_LAUNCHCTL") or "launchctl"


def serve_owner():
    """The pid of the live `orq manager serve`, or None: whoever holds the SERVE_PID flock. The lock, not the written pid, says whether a serve exists (a reused pid does not fool it)."""
    try:
        f = open(_path(SERVE_PID), "r")
    except OSError:
        return None
    with f:
        try:
            fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError:
            return int(f.read().strip() or 0) or -1
        fcntl.flock(f, fcntl.LOCK_UN)
    return None


def _serve_log(msg):
    p = _path(SERVE_LOG)
    with contextlib.suppress(OSError):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "a") as f:
            f.write(f"{now()} {msg}\n")


def _plist_serve():
    return os.path.join(LAUNCH_AGENTS, LAUNCHD_LABEL + ".plist")


def serve_status():
    """What `orq manager serve --status` shows: whether it runs, the pid, whether launchd is installed, the log and the last round (the `manager-alive` stamp)."""
    try:
        lap = datetime.fromtimestamp(os.path.getmtime(_path(PANEL_ALIVE)), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except OSError:
        lap = None
    pid = serve_owner()
    return {"rodando": pid is not None, "pid": pid, "instalado": os.path.exists(_plist_serve()), "log": _path(SERVE_LOG), "ultima_volta": lap}


def manager_state_write(line_list):
    """After a serve round: writes SERVE_STATE with the workers (the same as `orq agents --json`) and the machine against the budget. Read-only on Orca."""
    try:
        agent_rows = agents()
    except Exception as e:  # noqa: BLE001 - Orca down: the TUI shows the workers from aberto.json
        log(f"gerente estado: agentes: {type(e).__name__}: {e}")
        agent_rows = None
    _write_json(_path(SERVE_STATE), {"ts": now(), "pid": int(os.environ.get("ORQ_SERVE_PID") or 0) or None, "terminal": _manager_cfg().get("gerente"),
                                      "linhas": [x for x in line_list.splitlines() if x], "agentes": agent_rows,
                                      "maquina": {"cfg": machine_cfg(), "leitura": machine_read()}})


def manager_serve(loops=None):
    """The agent manager without a terminal (spike in relatorios/t128-gerente-sem-terminal.md): Orca accepts the consumer outside the terminal with the handle from
    gerente.json, so each round runs `orq manager absorver` with that handle and with no other ORCA_* variable (the caller's pane key would win over the
    handle). The round is a new process: the updated orq applies on the next round, without restarting the serve. The manager terminal stays only as the binding
    anchor; the panel in it sees the lock and only displays. Two serves is an error."""
    f = open(_path(SERVE_PID), "a+")
    for attempt in range(5):  # another process's `serve_owner` holds a LOCK_SH for a moment
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError:
            if attempt == 4:
                f.seek(0)
                owner_name = f.read().strip() or "?"
                f.close()
                raise ValueError(f"an agent manager is already serving (pid {owner_name}): orq manager serve --status | --stop")
            time.sleep(0.2)
    f.seek(0)
    f.truncate()
    f.write(str(os.getpid()))
    f.flush()
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    env = {k: v for k, v in os.environ.items() if not k.startswith("ORCA_")} | {"ORQ_SERVE_PID": str(os.getpid())}
    orq_py = os.path.join(os.path.dirname(os.path.abspath(__file__)), "orq.py")
    _serve_log(f"serve: pid {os.getpid()} subiu (volta de {SERVE_LAP_S:g} s)")
    n = 0
    try:
        while not stop and (loops is None or n < loops):
            n += 1
            start_time = time.time()
            _touches_panel()  # like the panel's shell: the stamp goes out before orq, holds even with orqlib broken
            g = _manager_cfg()
            e = {**env, **({"ORCA_TERMINAL_HANDLE": g["gerente"]} if g else {})}
            try:
                r = subprocess.run([sys.executable, orq_py, "gerente", "absorver", "--estado"], stdin=subprocess.DEVNULL, capture_output=True, text=True, env=e, timeout=300)
                line = (r.stdout + r.stderr).strip()
                if n % SERVE_DIGEST_ROUNDS == 1:
                    subprocess.run([sys.executable, orq_py, "digest"], stdin=subprocess.DEVNULL, capture_output=True, env=e, timeout=120)
            except (OSError, subprocess.TimeoutExpired) as x:
                line = f"volta falhou: {type(x).__name__}: {x}"
            _serve_log(f"volta {n} ({time.time() - start_time:.1f} s): " + " | ".join(line.splitlines()))
            end = time.time() + SERVE_LAP_S
            while not stop and (loops is None or n < loops) and time.time() < end:
                time.sleep(min(0.2, SERVE_LAP_S))
    finally:
        _serve_log(f"serve: pid {os.getpid()} parou depois de {n} volta(s)")
        f.seek(0)
        f.truncate()
        f.close()
    return n


def serve_stop(wait_s=15):
    """For the serve: through launchd when installed (KeepAlive would start it again; it returns at the next login), otherwise SIGTERM on the pid. Waits for the lock to release."""
    if os.path.exists(_plist_serve()):
        subprocess.run([LAUNCHCTL, "bootout", f"gui/{os.getuid()}/{LAUNCHD_LABEL}"], capture_output=True, timeout=30)
    pid = serve_owner()
    if pid and pid > 0:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGTERM)
    end = time.time() + wait_s
    while serve_owner() and time.time() < end:
        time.sleep(0.1)
    if serve_owner():
        raise ValueError(f"the serve (pid {serve_owner()}) did not stop in {wait_s} s")
    return serve_status()


def serve_install():
    """Writes the launchd agent (starts at login, KeepAlive restarts it if it dies) and loads it. The environment is the current PATH and ORQ_HOME; no ORCA_*."""
    os.makedirs(LAUNCH_AGENTS, exist_ok=True)
    os.makedirs(os.path.dirname(_path(SERVE_LOG)), exist_ok=True)
    import plistlib
    pl = {"Label": LAUNCHD_LABEL, "ProgramArguments": [sys.executable, os.path.join(orqpaths.CODE, "orq.py"), "gerente", "serve"],
          "EnvironmentVariables": {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "ORQ_HOME": HOME}, "RunAtLoad": True, "KeepAlive": True,
          "ThrottleInterval": 30, "StandardOutPath": _path(SERVE_LOG), "StandardErrorPath": _path(SERVE_LOG)}
    with open(_plist_serve(), "wb") as f:
        plistlib.dump(pl, f)
    target = f"gui/{os.getuid()}"
    subprocess.run([LAUNCHCTL, "bootout", f"{target}/{LAUNCHD_LABEL}"], capture_output=True, timeout=30)  # already loaded: reload with the new plist
    r = subprocess.run([LAUNCHCTL, "bootstrap", target, _plist_serve()], capture_output=True, text=True, timeout=30)
    if r.returncode:
        raise ValueError(f"launchctl bootstrap failed: {(r.stderr or r.stdout).strip()}")
    return serve_status()


def serve_uninstall():
    if os.path.exists(_plist_serve()):
        subprocess.run([LAUNCHCTL, "bootout", f"gui/{os.getuid()}/{LAUNCHD_LABEL}"], capture_output=True, timeout=30)
        os.remove(_plist_serve())
    return serve_status()


def manager_tui(theme=None):
    """`orq manager tui`: the TUI in tui/ (OpenTUI on Bun), read-only on the ORQ_HOME files. Without Bun or without the dependencies, it says how to install."""
    tui = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tui")
    bun = shutil.which("bun")
    if not bun:
        print("orq manager tui needs Bun: curl -fsSL https://bun.sh/install | bash (or brew install oven-sh/bun/bun) and then cd "
              f"{shlex.quote(tui)} && bun install", file=sys.stderr)
        return 1
    if not os.path.isdir(os.path.join(tui, "node_modules")):
        print(f"the TUI dependencies are missing: cd {shlex.quote(tui)} && bun install", file=sys.stderr)
        return 1
    return subprocess.run([bun, "run", os.path.join(tui, "src", "index.ts")], env={**os.environ, "ORQ_HOME": HOME, **({"ORQ_TUI_THEME": theme} if theme else {})}).returncode

# ---------- retro: the failure signal collector (ticket 78) ----------

ORQ_INSTALL = os.path.realpath(os.environ.get("ORQ_INSTALL") or orqpaths.CODE)  # the checkout that runs (hooks, panel): nobody works in it


def _in_live_checkout(path):
    """Is `path` inside the live checkout, but not in the ticket worktrees that live in it (.worktrees/, ticket 124)?"""
    real = os.path.realpath(path)
    return real.startswith(ORQ_INSTALL + os.sep) and not real.startswith(os.path.realpath(WT_ROOT) + os.sep)
RETRO_DIR = "retro"  # ORQ_HOME/retro/<YYYY-MM-DDTHHMM>.json: the metrics of each recorded round, to compare week by week
RETRO_DAYS = 7
RETRO_FIX_S = 1800  # the user's reaction to a delivery comes right after it
RETRO_FIX = re.compile(r"^\W*(n[ãa]o|ops|ajust\w*|errad\w*)\b", re.I)
RETRO_RED = ("FAILURE", "TIMED_OUT", "ACTION_REQUIRED", "ERROR", "STARTUP_FAILURE")
RETRO_SIGNALS = {
    "nao_iniciou": "dispatch that did not start",
    "steer_sem_leitura": "steer with no proven read",
    "steer_reentregue": "steer redelivered (notice typed again)",
    "retry": "worker relaunched",
    "controle_falhou": "interrupt, end or relaunch that failed",
    "intervencao": "worker interrupted or ended",
    "pergunta_de_worker": "question or escalation answered",
    "worker_falhou": "worker_done that was not succeeded",
    "sem_entrega": "worker released without a delivery",
    "liberado_sujo": "worker released with a dirty tree",
    "liberado_sem_push": "worker released with commits outside origin/main",
    "entrega_com_aviso": "delivery without proof (missing commit or dirty tree)",
    "checkout_em_uso": "work in the checkout orq is running from",
    "entrada_sem_tratamento": "entry that the Stop found without an effect",
    "intake_descartado": "entry discarded",
    "alerta": "orq alert",
    "binding_perdido": "coordinator with no Run bound",
    "correcao_do_usuario": "user correction right after a delivery",
    "regra_violada": "user rule violated by a worker (transcript)",
    "pr_ci_vermelho": "PR with a red check (gh)",
    "pr_pediu_mudanca": "PR with changes requested in review (gh)",
}
RETRO_BY_MODEL = ("nao_iniciou", "retry", "pergunta_de_worker", "worker_falhou", "sem_entrega", "intervencao", "liberado_sujo")
RETRO_AGENTS_WRITE = re.compile(r"(?:>>?|\btee\b|\bsed\s+-i|\bmv\b|\bcp\b|\brm\b|\bln\b)[^\n|;&]*/\.agents/")
RETRO_GIT_IN_CHECKOUT = re.compile(r"\bgit\s+(?:-C\s+(\S+)\s+)?(?:merge|rebase|reset|checkout|switch|pull|cherry-pick|commit)\b")


RETRO_HEREDOC = re.compile(r"<<-?\s*(['\"]?)(\w+)\1[^\n]*\n.*?\n[ \t]*\2[ \t]*(?=\n|$)", re.S)
RETRO_QUOTES = re.compile(r"\"(?:\\.|[^\"\\])*\"|'[^']*'")


def _retro_violations(tool, text_value, cwd):
    """The names of the user rules that a worker tool call breaks. Narrow, known patterns; a new rule goes in here.

    The command that counts is what is left without the heredoc bodies and without the quoted text: a `git push` inside a `python3 - <<EOF` or an
    `echo` is not a push. Only the trailer looks at the whole text (it lives in the commit message), and only when the `git commit` is really in the command.
    ponytail: regex, no shell parser. `bash -c "git push"` passes; `$(git push)` inside quotes too."""
    if tool in ("Edit", "Write", "MultiEdit", "NotebookEdit"):
        return (["agents_global"] if "/.agents/" in text_value else []) + (["checkout_em_uso_do_orq"] if _in_live_checkout(text_value) else [])
    v = RETRO_QUOTES.sub('""', RETRO_HEREDOC.sub("", text_value))
    rules = []
    if re.search(r"\bgit\s+(?:-C\s+\S+\s+)?push\b", v):
        rules.append("push_de_worker")
    if re.search(r"\bgh\s+(?:workflow\s+run|pr\s+merge)\b", v):
        rules.append("producao")
    if ("git commit" in v or "gh pr create" in v) and re.search(r"Co-Authored-By|Generated with", text_value, re.I):
        rules.append("trailer")
    if RETRO_AGENTS_WRITE.search(v):
        rules.append("agents_global")
    m = RETRO_GIT_IN_CHECKOUT.search(v)
    if m:
        cds = re.findall(r"\bcd\s+(\S+)", v[:m.start()])
        if os.path.realpath(os.path.expanduser(m.group(1) or (cds[-1] if cds else None) or cwd or "/")) == ORQ_INSTALL:
            rules.append("checkout_em_uso_do_orq")
    return rules


def _retro_calls(obj):
    """(tool, text, cwd) of each tool call in a transcript line: `tool_use` in Claude Code, `function_call` in Codex."""
    out = []
    content = _dict(obj.get("message")).get("content")
    for b in content if isinstance(content, list) else []:
        if isinstance(b, dict) and b.get("type") == "tool_use":
            i = _dict(b.get("input"))
            out.append((str(b.get("name")), str(i.get("command") or i.get("file_path") or i.get("path") or ""), obj.get("cwd")))
    p = _dict(obj.get("payload"))
    if p.get("type") == "function_call":
        try:
            a = _dict(json.loads(p.get("arguments") or "{}"))
        except ValueError:
            a = {}
        c = a.get("cmd") or a.get("command") or ""
        out.append(("Bash", " ".join(map(str, c)) if isinstance(c, list) else str(c), None))
    return out


def _retro_rules(dispatches, turns, projects):
    """Cases of a violated rule: each dispatch's transcript (turnos.json gives the session) read once, one case per dispatch and rule.

    ponytail: reads the whole file of each worker in the window; a transcript of tens of MB costs a few seconds. Codex: only the command, no cwd."""
    cases = []
    for d in dispatches:
        t = _dict(turns.get(d.get("dispatch")))
        files = [t["transcrito"]] if t.get("transcrito") else glob.glob(os.path.join(projects, "*", f"{glob.escape(t['sessao'])}.jsonl")) if t.get("sessao") else []
        findings = {}
        for file_path in files:
            try:
                with open(file_path, "rb") as f:
                    for n, line in enumerate(f, 1):
                        if b'"tool_use"' not in line and b'"function_call"' not in line:
                            continue
                        try:
                            obj = json.loads(line)
                        except ValueError:
                            continue
                        for tool_name, text_value, cwd in _retro_calls(_dict(obj)):
                            for rule in _retro_violations(tool_name, text_value, cwd):
                                a = findings.setdefault(rule, {"n": 0, "ponteiro": f"{file_path}:{n}", "ts": _dict(obj).get("timestamp"), "texto": text_value})
                                a["n"] += 1
            except OSError as e:
                log(f"retro: transcrito {file_path}: {type(e).__name__}: {e}")
        for rule, a in findings.items():
            cases.append({"regra": rule, "ts": a["ts"] or d.get("ts"), "task": d.get("task"), "dispatch": d.get("dispatch"), "titulo": d.get("titulo"), "modelo": d.get("modelo"),
                          "effort": d.get("effort"), "onde": d.get("worktree"), "ponteiro": a["ponteiro"],
                          "detalhe": f"{rule}: {_quote(' '.join(a['texto'].split()), 100)}" + (f" (+{a['n'] - 1} times)" if a["n"] > 1 else "")})
    return cases


def _retro_pr_checks(url):
    """{falhos: [names of the red checks], revisao} of the PR via gh, or None without gh, without network or without a response. It is the state as of now, not as of the delivery."""
    try:
        r = subprocess.run([GH, "pr", "view", url, "--json", "statusCheckRollup,reviewDecision"], capture_output=True, text=True, timeout=PR_GH_S)
        d = json.loads(r.stdout) if r.returncode == 0 else None
    except (subprocess.TimeoutExpired, OSError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    return {"falhos": [c.get("name") or c.get("context") or "?" for c in d.get("statusCheckRollup") or []
                       if isinstance(c, dict) and (c.get("conclusion") in RETRO_RED or c.get("state") in RETRO_RED)],
            "revisao": d.get("reviewDecision") or ""}


def retro_collection(events, since, until_at, turns=None, projects=None, pr_checks=None, project=None):
    """The failure signals from [desde, ate) (stamps `AAAA-MM-DDTHH:MM:SSZ`), without an LLM: {desde, ate, sinais {nome: {rotulo, n, casos}}, falhas, by_model}.

    Each case carries title, model, effort and a pointer (events.jsonl line when the event brings `_l`, transcript file:line, PR URL).
    `n` None means "not queried": without `turns` and `projects` the transcripts are not read, without `pr_checks` gh is not called, and that does not become zero.
    `project` keeps only the cases whose path, title or task contains the fragment."""
    ev = [e for e in events if since <= (e.get("ts") or "") < until_at]
    by_dispatch, by_task = {}, {}
    for d in events:
        if d.get("tipo") == "despacho":
            by_dispatch[d.get("dispatch")], by_task[d.get("task")] = d, d
    cases = {k: [] for k in RETRO_SIGNALS}
    read_handles = {e.get("msg_id") for e in events if e.get("tipo") == "steer_fim" and e.get("motivo") == "lido"}
    ends = [(e.get("dispatch"), e["ts"]) for e in events if e.get("tipo") in ("worker_done", "fim_dispatch") and e.get("ts")]
    deliveries = sorted(_dt(e["ts"]).timestamp() for e in events if e.get("ts") and (e.get("tipo") == "worker_done" or e.get("origem") == "relatorio_worker"))
    inside = os.path.relpath(os.path.realpath(WT_ROOT), ORQ_INSTALL)  # .worktrees/ lives in the clone and is not the checkout in use
    install = re.compile(re.escape(ORQ_INSTALL) + r"(?![\w-])" + ("" if inside.startswith("..") else f"(?!/{re.escape(inside)}(?![\\w-]))"))

    def add(item_name, e, detail, pointer=None):
        d = by_dispatch.get(e.get("dispatch")) or by_task.get(e.get("task")) or {}
        where = next((x for x in (e.get("caminho"), e.get("worktree"), d.get("worktree")) if x and x != "current"), None)
        cases[item_name].append({"ts": e.get("ts"), "task": e.get("task") or d.get("task"), "dispatch": e.get("dispatch") or d.get("dispatch"), "titulo": d.get("titulo"),
                            "modelo": d.get("modelo"), "effort": d.get("effort"), "onde": where, "detalhe": detail,
                            "ponteiro": pointer or (f"events.jsonl:{e['_l']}" if e.get("_l") else f"events.jsonl@{e.get('ts')}"),
                            **({"sessao": e["sessao"]} if e.get("sessao") else {})})  # the gap ledger counts sessions: the dispatch, else the task, else this

    gate_notices, prs = {}, {}
    for e in ev:
        t = e.get("tipo")
        if t == "nao_iniciou":
            note = next((x.get("nota") for x in events if x.get("tipo") == "controle" and x.get("acao") == "relancar" and x.get("dispatch") == e.get("dispatch") and x.get("nota")), None)
            add("nao_iniciou", e, note or "the spec prompt did not go in, not even after Enter")
        elif t == "controle":
            if e.get("resultado") == "falhou":
                add("controle_falhou", e, f"{e.get('acao')}: {_quote(str(e.get('erro') or e.get('passo') or ''), 100)}")
            elif e.get("acao") == "relancar" and e.get("resultado") == "iniciado":
                add("retry", e, e.get("nota") or "relaunched without a note")
            elif e.get("acao") in ("interromper", "encerrar"):
                add("intervencao", e, f"{e.get('acao')}: {_quote(str(e.get('motivo') or e.get('aviso') or ''), 100)}")
        elif t == "steer" and e.get("msg_id") and e["msg_id"] not in read_handles:
            after = any(d == e.get("dispatch") and ts > e["ts"] for d, ts in ends)
            add("steer_sem_leitura", e, ("the worker delivered afterwards, with no proven read of the adjustment: " if after else "no steer_fim read: ") + _quote(str(e.get("texto") or ""), 80))
        elif t == "steer_reentrega":
            add("steer_reentregue", e, f"attempt {e.get('tentativa')}")
        elif t == "resposta_worker":
            add("pergunta_de_worker", e, _quote(str(e.get("texto") or ""), 100))
        elif t == "worker_done" and e.get("outcome") != "succeeded":
            add("worker_falhou", e, f"{e.get('outcome')}: {_quote(str(e.get('subject') or ''), 100)}")
        elif t == "fim_dispatch":
            if e.get("motivo") in ("sem worker_done", "falhou", "motivo desconhecido"):
                add("sem_entrega", e, str(e.get("motivo")))
            if (e.get("sujo") or 0) > 0:
                add("liberado_sujo", e, f"{e['sujo']} file(s) in {e.get('caminho')}")
            if (e.get("sem_push") or 0) > 0:
                add("liberado_sem_push", e, f"{e['sem_push']} commit(s) in {e.get('caminho')} (the ticket asks for 'no push': check)")
            if e.get("caminho") and os.path.realpath(e["caminho"]) == ORQ_INSTALL and ((e.get("sujo") or 0) or (e.get("sem_push") or 0)):
                add("checkout_em_uso", e, f"worker released in {e['caminho']}")
        elif t == "entrega" and e.get("avisos"):
            add("entrega_com_aviso", e, "; ".join(map(str, e["avisos"])))
            if any(install.search(str(a)) for a in e["avisos"]):
                add("checkout_em_uso", e, "the delivery notice mentions the checkout in use")
        elif t == "gate_aviso":
            for i in e.get("abertas") or []:
                gate_notices.setdefault(i, e)
        elif t == "intake" and e.get("efeito") == "descartado":
            add("intake_descartado", e, f"{e.get('entrada')}: {_quote(str(e.get('nota') or ''), 100)}")
        elif t == "alerta":
            add("alerta", e, str(e.get("alerta")))
        elif t == "binding_perdido":
            add("binding_perdido", e, f"session {e.get('sessao')}")
        elif t == "entrada" and e.get("origem") == "usuario" and RETRO_FIX.match(str(e.get("texto") or "")) and deliveries:
            now_at = _dt(e["ts"]).timestamp()
            before = [x for x in deliveries if x <= now_at]
            if before and now_at - before[-1] <= RETRO_FIX_S:
                add("correcao_do_usuario", e, _quote(" ".join(str(e["texto"]).split()), 100))
        elif t == "pr" and e.get("op") == "ligar" and e.get("url"):
            prs.setdefault(e["url"], e)
    for i, e in gate_notices.items():
        add("entrada_sem_tratamento", e, f"entry {i} had no effect at the end of a turn")
    if turns is not None and projects is not None:
        cases["regra_violada"] = _retro_rules([d for d in events if d.get("tipo") == "despacho" and since <= (d.get("ts") or "") < until_at], turns, projects)
    else:
        cases["regra_violada"] = None
    if pr_checks is None:
        cases["pr_ci_vermelho"] = cases["pr_pediu_mudanca"] = None
    else:
        with ThreadPoolExecutor(4) as ex:
            for (url, e), c in zip(prs.items(), ex.map(pr_checks, prs)):
                if not c:
                    continue
                if c.get("falhos"):
                    add("pr_ci_vermelho", e, f"PR #{e.get('numero')}: {', '.join(c['falhos'])}", url)
                if c.get("revisao") == "CHANGES_REQUESTED":
                    add("pr_pediu_mudanca", e, f"PR #{e.get('numero')}: review asked for changes", url)
    if project:
        achar = project.lower()
        for k, v in cases.items():
            if v is not None:
                cases[k] = [c for c in v if achar in " ".join(str(c.get(x) or "") for x in ("onde", "titulo", "task")).lower()]
    by_model = {}
    for e in ev:
        if e.get("tipo") == "despacho":
            by_model.setdefault(f"{e.get('modelo')}/{e.get('effort')}", {"despachos": 0, **dict.fromkeys(RETRO_BY_MODEL, 0)})["despachos"] += 1
    for k in RETRO_BY_MODEL:
        for c in cases[k]:
            if c.get("modelo"):
                by_model.setdefault(f"{c['modelo']}/{c['effort']}", {"despachos": 0, **dict.fromkeys(RETRO_BY_MODEL, 0)})[k] += 1
    signals = {k: {"rotulo": RETRO_SIGNALS[k], "n": None if v is None else len(v), "casos": v or []} for k, v in cases.items()}
    return {"desde": since, "ate": until_at, "sinais": signals, "falhas": sum(s["n"] or 0 for s in signals.values()), "por_modelo": by_model}


def _retro_events():
    """The events.jsonl with each event's line in `_l` (the pointer of each case)."""
    out = []
    try:
        with open(_path("events.jsonl")) as f:
            for n, line in enumerate(f, 1):
                try:
                    e = to_pt(json.loads(line))
                except ValueError:
                    continue
                if isinstance(e, dict):
                    out.append({**e, "_l": n})
    except OSError:
        pass
    return out


def _retro_rounds():
    """The rounds stored in ORQ_HOME/retro, from oldest to newest: {desde, ate, falhas, metricas}."""
    rs = [_read_json(p) for p in sorted(glob.glob(_path(os.path.join(RETRO_DIR, "*.json"))))]
    return sorted((r for r in rs if isinstance(r, dict) and r.get("ate")), key=lambda r: r["ate"])


def write_retro(r):
    """Writes the round's metrics (only the numbers, never the cases) to ORQ_HOME/retro/<ate>.json. Returns the path."""
    os.makedirs(_path(RETRO_DIR), exist_ok=True)
    path = _path(os.path.join(RETRO_DIR, _dt(r["ate"]).strftime("%Y-%m-%dT%H%M") + ".json"))
    _write_json(path, {"desde": r["desde"], "ate": r["ate"], "falhas": r["falhas"], "metricas": {k: v["n"] for k, v in r["sinais"].items()}}, indent=2)
    return path


# ---------- retro: the gap ledger, what the retro remembers between weeks (ticket 210) ----------

RETRO_GAPS = os.path.join(RETRO_DIR, "gaps.json")  # ORQ_HOME/retro/gaps.json: {lacunas: [one entry per gap]}
GAP_MIN_SESSIONS = 2  # distinct sessions before a gap becomes a proposal (the skill's exception: one case that lost work or broke orq)
GAP_MAX_AGE_DAYS = 90  # a gap with no new sighting for this long is forgotten, rejection included
GAP_POINTERS = 20  # the pointers kept per gap: the latest ones
GAP_OPEN = ("aberta", "proposta")
RETRO_TEXT_SIGNALS = ("correcao_do_usuario", "pr_pediu_mudanca", "intake_descartado", "entrada_sem_tratamento")  # judgement errors; the rest of the signals is mechanical


def _gap_stamp(ts, default=""):
    """A stamp in the `AAAA-MM-DDTHH:MM:SSZ` form (transcripts carry milliseconds), or `default` when it is not a date."""
    try:
        return _dt(ts).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (AttributeError, ValueError):
        return default


def _gaps_sight(gaps, signal, case, ref_at):
    """Folds one case into the ledger. The gap id is the broken rule (`regra_violada`) or the signal; the session is the dispatch, else the task, else the event's session, else the day.

    The same session never counts twice, but each new pointer is kept. A covered gap that comes back in a new session starts over.
    ponytail: the class is the signal's family (text or check), not a reading of the case: calibration is the skill's call, and its reading wins in the proposal."""
    gid = case.get("regra") or signal
    seen_at = _gap_stamp(case.get("ts"), ref_at)
    g = next((x for x in gaps if x["id"] == gid), None)
    if not g:
        g = {"id": gid, "classe": "texto" if signal in RETRO_TEXT_SIGNALS else "checagem",
             "sessoes": [], "ponteiros": [], "primeira_vez": seen_at, "ultima_vez": seen_at, "estado": "aberta"}
        gaps.append(g)
    session = case.get("dispatch") or case.get("task") or case.get("sessao") or f"dia:{seen_at[:10]}"
    if session not in g["sessoes"]:
        if g["estado"] == "coberta":
            g.update(sessoes=[], estado="aberta", primeira_vez=seen_at)
            g.pop("ticket", None)
        g["sessoes"].append(session)
    if case.get("ponteiro") and case["ponteiro"] not in g["ponteiros"]:
        g["ponteiros"] = [*g["ponteiros"], case["ponteiro"]][-GAP_POINTERS:]
    g["primeira_vez"], g["ultima_vez"] = min(g["primeira_vez"], seen_at), max(g["ultima_vez"], seen_at)


def _gaps_settle(gaps, ref_at, events):
    """The ledger as of `ref_at`: forgets what went 90 days without a sighting, covers the accepted gap whose ticket closed, lets a rejected one back
    once it has more sessions than at the rejection, and graduates the open one that reached two sessions. Returns the surviving gaps."""
    cutoff = (_dt(ref_at) - timedelta(days=GAP_MAX_AGE_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    closed = {str(e.get("ticket")).zfill(2) for e in events if e.get("tipo") == "ticket" and e.get("op") == "fechar"}
    out = []
    for g in gaps:
        if (g.get("ultima_vez") or "") <= cutoff:
            continue
        if g["estado"] == "aceita" and str(g.get("ticket")) in closed:
            g["estado"] = "coberta"
        elif g["estado"] == "rejeitada" and len(g["sessoes"]) > _dict(g.get("rejeicao")).get("sessoes", 0):
            g["estado"] = "proposta"
            g.pop("rejeicao", None)
        if g["estado"] == "aberta" and len(g["sessoes"]) >= GAP_MIN_SESSIONS:
            g["estado"] = "proposta"
        out.append(g)
    return out


def _gaps_read():
    return [g for g in _dict(_read_json(_path(RETRO_GAPS))).get("lacunas") or [] if isinstance(g, dict) and g.get("id")
            and isinstance(g.get("sessoes"), list) and isinstance(g.get("ponteiros"), list)]


def gaps_record(r, ref_at, events):
    """`orq retro --gravar`: adds each case of the round to ORQ_HOME/retro/gaps.json. Running the same window again changes nothing."""
    with _lock("retro.lock"):
        gaps = _gaps_read()
        for signal, s in r["sinais"].items():
            for case in s["casos"]:
                _gaps_sight(gaps, signal, case, ref_at)
        _write_json(_path(RETRO_GAPS), {"lacunas": _gaps_settle(gaps, ref_at, events)}, indent=2)


def gaps_edit(gid, events, edit=None):
    """The settled ledger and, with `edit(gap)`, the change to one gap written back. Raises ValueError for an id the ledger does not know."""
    with _lock("retro.lock"):
        gaps = _gaps_settle(_gaps_read(), now(), events)
        if edit:
            g = next((x for x in gaps if x["id"] == gid), None)
            if not g:
                raise ValueError(f"no gap {gid!r} in the ledger (known: {', '.join(x['id'] for x in gaps) or 'none'})")
            edit(g)
            _write_json(_path(RETRO_GAPS), {"lacunas": gaps}, indent=2)
        return gaps


def gaps_text(gaps):
    """`orq retro gaps`: the number of open gaps and one line per gap, the ones ready to propose first."""
    ls = [f"open gaps: {sum(g['estado'] in GAP_OPEN for g in gaps)}"]
    for g in sorted(gaps, key=lambda g: (g["estado"] != "proposta", g["estado"] != "aberta", g["id"])):
        extra = (f"  ticket {g['ticket']}" if g.get("ticket") else "") + (f"  rejected at {g['rejeicao']['sessoes']} session(s): {g['rejeicao']['motivo']}" if g.get("rejeicao") else "")
        ls.append(f"{g['id']:<26}{g['estado']:<10}{len(g['sessoes']):>3} session(s)  {g['classe']:<11} {g['primeira_vez'][:10]} to {g['ultima_vez'][:10]}{extra}")
    return "\n".join(ls)


def gaps_cmd(a):
    """`orq retro gaps | reject <id> --reason T | accept <id> --ticket N`."""
    events = _retro_events()
    if a.op == "reject":
        if not a.ref or not (a.reason or "").strip():
            raise ValueError("orq retro reject <id> --reason <text>")

        def reject(g):
            g["estado"], g["rejeicao"] = "rejeitada", {"motivo": a.reason.strip(), "sessoes": len(g["sessoes"]), "ts": now()}
        n = next(g for g in gaps_edit(a.ref, events, reject) if g["id"] == a.ref)["rejeicao"]["sessoes"]
        return f"gap {a.ref} rejected at {n} session(s): it only comes back with more than that"
    if a.op == "accept":
        number = str(a.ticket or "").strip().zfill(2)
        if not a.ref or not a.ticket:
            raise ValueError("orq retro accept <id> --ticket <number>")
        if not any(t["num"] == number for t in tickets()):
            raise ValueError(f"ticket {number} does not exist in {ISSUES}")
        gaps_edit(a.ref, events, lambda g: (g.update(estado="aceita", ticket=number), g.pop("rejeicao", None)))
        return f"gap {a.ref} accepted as ticket {number}: it becomes covered when the ticket closes"
    gaps = gaps_edit(None, events)
    return json.dumps({"abertas": sum(g["estado"] in GAP_OPEN for g in gaps), "lacunas": gaps}, ensure_ascii=False) if a.json else gaps_text(gaps)


def retro_text(r, anterior=None, by_case=5):
    """The round as short text: the signals table (with the stored round beside it), the cases with a pointer and the table by model and effort."""
    ant = _dict(_dict(anterior).get("metricas"))
    ls = [f"retro {r['desde'][:10]} to {r['ate'][:10]}", f"failures in the period: {r['falhas']}" + (f" (saved round up to {anterior['ate'][:10]}: {anterior['falhas']})" if anterior else "")]
    ls.append(f"{'signal':<24}{'n':>5}" + ("  before" if anterior else ""))
    for k, s in r["sinais"].items():
        ls.append(f"{k:<24}{'n/a' if s['n'] is None else s['n']:>5}" + (f"  {'n/a' if ant.get(k, '-') is None else ant.get(k, '-')}" if anterior else ""))
    for k, s in r["sinais"].items():
        if s["n"]:
            ls += ["", f"{k} ({s['n']}): {s['rotulo']}"]
            ls += [f"  {(c['ts'] or '')[:16]} {c.get('titulo') or c.get('task') or ''} [{c.get('modelo') or '?'}/{c.get('effort') or '?'}] {c['detalhe']} -> {c['ponteiro']}" for c in s["casos"][:by_case]]
            ls += [f"  +{s['n'] - by_case} more (--json)"] if s["n"] > by_case else []
    if r["por_modelo"]:
        ls += ["", f"{'model/effort':<28}{'disp':>5}" + "".join(f"{k[:9]:>10}" for k in RETRO_BY_MODEL)]
        ls += [f"{m:<28}{v['despachos']:>5}" + "".join(f"{v[k]:>10}" for k in RETRO_BY_MODEL) for m, v in sorted(r["por_modelo"].items())]
    return "\n".join(ls)


def retro_cmd(a):
    """`orq retro`: collects the window (default: the last 7 days), prints it and, with --gravar, saves the metrics for the next round to compare."""
    until_at = _dt(a.until_at).strftime("%Y-%m-%dT%H:%M:%SZ") if a.until_at else now()
    since = _dt(a.since).strftime("%Y-%m-%dT%H:%M:%SZ") if a.since else (_dt(until_at) - timedelta(days=RETRO_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    if a.op in ("reject", "accept", "gaps"):
        return gaps_cmd(a)
    events = _retro_events()
    r = retro_collection(events, since, until_at, None if a.without_transcripts else _turns_ro(), None if a.without_transcripts else PROJECTS,
                     None if a.without_gh else _retro_pr_checks, a.project)
    anterior = next((x for x in reversed(_retro_rounds()) if x["ate"] < until_at), None)
    if a.write_out:
        write_retro(r)
        gaps_record(r, until_at, events)
    return json.dumps(r, ensure_ascii=False) if a.json else retro_text(r, anterior)


# ---------- orq revisar: only the no-mistakes review (ticket 146) ----------

NM_BIN = os.environ.get("ORQ_NM") or "no-mistakes"
NM_HOME = os.environ.get("ORQ_NM_HOME") or os.path.expanduser("~/.no-mistakes-orq")  # orq's NM_HOME: never the personal ~/.no-mistakes
NM_MODEL, NM_EFFORT = "claude-sonnet-5-5", "low"  # the cheap review; the config.yaml of orq's NM_HOME is only written if it doesn't exist, so editing it holds
NM_CONFIG = f"agent: claude\nagent_config:\n  claude:\n    model: {NM_MODEL}\n    effort: {NM_EFFORT}\nauto_fix:\n  review: 0\nintent:\n  enabled: false\n"
NM_SKIP = "test,document,lint,push,pr,ci"  # what remains is the rebase and the review; test brings up a stack outside the E2E queue, push and pr belong to the coordinator
NM_WAIT_S = 720  # `axi run` waits until the first gate (--wait); the process gets slack on top
NM_INTENT_MAX = 6000
REVIEW = "revisao-nm.json"  # [{pid, task, ts}]: the reviews in progress, which count as an expensive slot


def _nm_model():
    """(model, effort) from the config.yaml in orq's NM_HOME (what no-mistakes will use), or orq's defaults if the file doesn't say."""
    try:
        with open(os.path.join(NM_HOME, "config.yaml"), encoding="utf-8") as f:
            m = re.search(r"^[ \t]+claude:[ \t]*\n(?:[ \t]+\w+:[^\n]*\n)*?[ \t]+model:[ \t]*(\S+)", f.read() + "\n", re.M)
    except OSError:
        m = None
    return (m.group(1) if m else NM_MODEL), NM_EFFORT


def _ongoing_reviews():
    """The live reviews in REVISAO (pid still exists); one that died without cleaning up doesn't take a slot."""
    live_output = []
    for r in _read_json(_path(REVIEW)) or []:
        try:
            os.kill(r["pid"], 0)
            live_output.append(r)
        except (OSError, KeyError, TypeError):
            pass
    return live_output


def _review_slot(task):
    """Reserves an expensive slot for the review or raises ValueError: high machine pressure, or expensive workers plus reviews in progress at the `max_caros` limit.
    ponytail: `orq dispatch_worker` only sees the workers; a dispatch during the review can go over the cap by one. Count the review in machine_occupancy if it becomes a problem."""
    with _lock("review.lock"):
        cfg = machine_cfg()
        level, reason = machine_level(None, cfg)
        if level == "alta":
            raise ValueError(f"machine under pressure: {reason}; the review did not run")
        expensive_count = sum(expensive_model(m, cfg) for m in machine_occupancy()["vivos"].values()) + len(_ongoing_reviews())
        if expensive_count >= cfg["max_caros"]:
            raise ValueError(f"{expensive_count}/{cfg['max_caros']:g} expensive slots taken (workers and reviews); the review counts as one")
        _write_json(_path(REVIEW), [*_ongoing_reviews(), {"pid": os.getpid(), "task": task, "ts": now()}])


def _loose_review():
    with _lock("review.lock"):
        _write_json(_path(REVIEW), [r for r in _ongoing_reviews() if r["pid"] != os.getpid()])


def _nm_usage(since):
    """{achados, tokens: {entrada, saida, cache_lido, cache_criado}} from the agent calls in orq's NM_HOME state.sqlite since `since` (epoch s); empty without a database."""
    import sqlite3  # only here: kept out of the top level so it doesn't weigh on the hooks
    empty = {"achados": None, "tokens": None}
    try:
        con = sqlite3.connect(f"file:{os.path.join(NM_HOME, 'state.sqlite')}?mode=ro", uri=True, timeout=5)
        try:
            ach, entry_event, sai, cl, cc = con.execute("select sum(finding_count), sum(input_tokens), sum(output_tokens), sum(cache_read_tokens), sum(cache_creation_tokens) "
                                                "from agent_invocations where started_at >= ?", (int(since),)).fetchone()
        finally:
            con.close()
    except sqlite3.Error:
        return empty
    return {"achados": ach or 0, "tokens": {"entrada": entry_event or 0, "saida": sai or 0, "cache_lido": cl or 0, "cache_criado": cc or 0}}


def review(task):
    """`orq review <task>`: only the no-mistakes review in the task's worktree, with the cheap model from orq's NM_HOME.

    `task` is the task id or the ticket number. Refuses on `pause` or `segura` from `orq usage` (any priority: the review is optional spend), under machine
    pressure and without an expensive slot. The `--intent` is the ticket text (or the dispatch title). Returns {task, worktree, modelo, effort, duracao_s, achados, tokens, saida}
    and records `revisao_nm` in events.jsonl. `output` is what `axi run` printed: the findings and the gate it stopped at."""
    tk = next((t for t in tickets() if task in (t["num"], t["task"]) or task.zfill(2) == t["num"]), None)
    task = tk["task"] if tk and tk["task"] else task
    ev = next((e for e in reversed(read_events()) if e.get("tipo") == "despacho" and e.get("task") == task), None)
    if not ev:
        raise ValueError(f"task {task} has no dispatch in events.jsonl: nothing to review")
    wt = _worker_path(orca("worker-show", "--dispatch", ev["dispatch"], timeout=10))
    if not wt or not os.path.isdir(wt):
        raise ValueError(f"Orca does not give the worktree of dispatch {ev['dispatch']} ({wt or 'no path'}): was the folder already cleaned?")
    usage_check(2)
    if tk:
        body_text = open(tk["arquivo"], encoding="utf-8").read().split("\n## ", 1)
        intent = f"{tk['titulo']}\n\n## {body_text[1]}"[:NM_INTENT_MAX] if len(body_text) > 1 else tk["titulo"]
    else:
        intent = ev.get("titulo") or task
    model, effort = _nm_model()
    os.makedirs(NM_HOME, exist_ok=True)
    if not os.path.exists(os.path.join(NM_HOME, "config.yaml")):
        with open(os.path.join(NM_HOME, "config.yaml"), "w", encoding="utf-8") as f:
            f.write(NM_CONFIG)
    env = {**os.environ, "NM_HOME": NM_HOME}
    _review_slot(task)
    t0, start_time, error, output = int(time.time()), time.monotonic(), None, ""
    try:
        init = subprocess.run([NM_BIN, "init"], cwd=wt, env=env, capture_output=True, text=True, timeout=60)  # per repository; repeating is harmless
        if init.returncode and "already" not in (init.stdout + init.stderr).lower():
            raise RuntimeError(f"no-mistakes init failed: {(init.stderr or init.stdout).strip()[-300:]}")
        r = subprocess.run([NM_BIN, "axi", "run", "--intent", intent, "--skip", NM_SKIP, "--wait", f"{NM_WAIT_S}s"], cwd=wt, env=env,
                           capture_output=True, text=True, timeout=NM_WAIT_S + 60)
        output = r.stdout.strip()
        if r.returncode:
            raise RuntimeError(f"no-mistakes axi run exited with {r.returncode}: {(r.stderr or r.stdout).strip()[-300:]}")
    except (RuntimeError, OSError, subprocess.TimeoutExpired) as e:
        error = f"{type(e).__name__}: {e}"
    finally:
        with contextlib.suppress(OSError, subprocess.TimeoutExpired):  # the review stops at the gate: without aborting, the branch stays `pipeline_owned` and the worker doesn't commit to it
            subprocess.run([NM_BIN, "axi", "abort"], cwd=wt, env=env, capture_output=True, text=True, timeout=60)
        _loose_review()
    usage = _nm_usage(t0)
    res = {"task": task, "worktree": wt, "modelo": model, "effort": effort, "duracao_s": round(time.monotonic() - start_time, 1), **usage, "saida": output}
    append_event({"tipo": "revisao_nm", **{k: v for k, v in res.items() if k != "saida"}, **({"erro": error} if error else {})})
    if error:
        raise RuntimeError(error)
    return res


def review_text(r):
    """The header (model, findings, duration, tokens) and, below it, the no-mistakes output as it came."""
    t = r["tokens"]
    toks = f", tokens: {t['entrada']} input, {t['saida']} output, {t['cache_lido']} cache read" if t else ""
    return f"no-mistakes review of {r['task']}: model {r['modelo']} (effort {r['effort']}), {r['achados'] if r['achados'] is not None else '?'} finding(s), {r['duracao_s']:g} s{toks}\n\n{r['saida']}"


def _implicit(effect, ref=None, run=None):
    """Prints to stderr what `implicit_intake` did or warned about (stdout keeps only the command's JSON)."""
    if line := implicit_intake(effect, ref, run):
        print(line, file=sys.stderr)


FLAG_EN = {  # --pt -> --en (phase 1 of the migration to English); dest stays the pt name, which the rest of the code reads
    "titulo": "title", "spec-arquivo": "spec-file", "detalhe": "detail", "frente": "stream", "comando": "command", "espera": "waiting", "ate": "until",
    "desde": "since", "todas": "all", "todos": "all", "resposta": "answer", "entrada": "entry", "nota": "note", "prova": "proof", "motivo": "reason",
    "tipo": "type", "forcar": "force", "abrir": "open", "passo": "step", "nome": "name", "por": "why", "agente": "agent", "objetivo": "objective",
    "assumir": "take-over", "noite": "night", "parada": "stopped-by", "modelo": "model", "para": "to", "max-despachos": "max-dispatches",
    "max-falhas": "max-failures", "projeto": "project", "prioridade": "priority", "servico": "service", "pergunta": "question", "opcao": "option",
    "recomendada": "recommended", "espera-min": "wait-min", "sem-poll": "no-poll", "sessao": "session", "pausados": "paused", "texto": "text",
    "ate-prioridade": "up-to-priority", "grupo": "group", "prazo": "deadline", "responde": "answers", "sem-gh": "no-gh",
    "sem-transcritos": "no-transcripts", "gravar": "save", "corpo": "body", "ambientes": "environments", "ultimos": "last", "fechados": "closed",
    "destino": "dest", "substituir-orca-yaml": "replace-orca-yaml", "despacho": "dispatch", "parar": "stop", "instalar": "install",
    "desinstalar": "uninstall", "voltas": "rounds", "estado": "state", "liberar": "release", "horas": "hours"}
ARG_DEST = {  # flag key in pt -> the attribute the parsed arguments carry (the dest)
    "titulo": "title", "spec-arquivo": "spec_file", "detalhe": "detail", "frente": "workstream", "comando": "command", "espera": "waiting", "ate":
    "until_at", "desde": "since", "todas": "all_listing", "todos": "include_all", "resposta": "answer_text", "entrada": "entry", "nota": "note",
    "prova": "proof", "motivo": "reason", "tipo": "type_name", "forcar": "force", "abrir": "open_page", "passo": "step", "nome": "item_name", "por":
    "by", "agente": "agent", "objetivo": "objective", "assumir": "take_over", "noite": "night", "parada": "stopped_by", "modelo": "model", "para":
    "to_", "max-despachos": "max_dispatches", "max-falhas": "max_failures", "projeto": "project", "prioridade": "priority_level", "servico":
    "service", "pergunta": "question", "opcao": "option", "recomendada": "recommended", "espera-min": "wait_min", "sem-poll": "without_poll",
    "sessao": "session", "pausados": "paused", "texto": "text_value", "ate-prioridade": "up_to_priority", "grupo": "group_name", "prazo": "deadline",
    "sem-gh": "without_gh", "sem-transcritos": "without_transcripts", "gravar": "write_out", "corpo": "body_text", "ambientes": "environments",
    "ultimos": "last_n", "fechados": "closed_items", "destino": "destination", "substituir-orca-yaml": "replace_orca_yaml", "despacho":
    "dispatch_mode", "parar": "stop", "instalar": "install", "desinstalar": "uninstall", "voltas": "loops", "estado": "state", "liberar": "release",
    "horas": "max_age_hours"}
FLAG_ALIASES = {f"--{pt}": f"--{en}" for pt, en in FLAG_EN.items()}
ALIASES = {  # pt -> en. "" are the commands; the key of each other table is the English command (op) or "<command> <op>" (acao)
    "": {"feito": "fulfill", "adiar": "defer", "fila": "queue", "ausente": "away", "responder": "reply", "iniciar": "start", "ocupadas": "busy",
         "resumo": "summary", "alerta": "alert", "agentes": "agents", "liberar": "release", "interromper": "interrupt", "responder-tela": "answer-screen",
         "encerrar": "end", "relancar": "relaunch", "passagem": "handoff", "passar": "switch", "noite": "night", "despachar": "dispatch",
         "projetos": "projects", "projeto": "project", "fluxo": "flow", "ciclo": "cycle", "integrar": "integrate", "lavish-resposta": "lavish-answer",
         "perguntar": "ask", "auditar-respostas": "audit-answers", "auditar-publicacao": "audit-publication", "gerente": "manager", "retomar": "resume",
         "hibernar": "hibernate", "acordar": "wake", "pausar": "pause", "prioridade": "priority", "uso": "usage", "maquina": "machine",
         "fila-despacho": "dispatch-queue", "grupos": "groups", "limpar": "clean", "devolver": "send-back", "revisar": "review", "caixa": "inbox",
         "transcrito": "transcript", "servico": "service", "lembrar": "remind", "fase": "phase"},
    "pend": {"lista": "list"},
    "backlog": {"mover": "move"},
    "pr": {"ligar": "link", "abrir": "open", "lista": "list", "desligar": "unlink"},
    "queue": {"feito": "done", "lista": "list"},
    "away": {"ligar": "on", "desligar": "off"},
    "alert": {"visto": "seen"},
    "night": {"ligar": "on", "desligar": "off"},
    "run": {"projeto": "project"},
    "project": {"confiar": "trust"},
    "service": {"marcar": "mark"},
    "cycle": {"feito": "done"},
    "integrate": {"fila": "queue", "concluir": "conclude"},
    "integrate queue": {"lista": "list"},
    "ticket": {"novo": "new", "fechar": "close", "editar": "edit", "lista": "list"},
    "manager": {"ligar": "bind", "desligar": "unbind", "checar": "check", "subir": "spawn", "absorver": "absorb", "intervalo": "interval"},
    "doctor": {"antigos": "old"},
    "dispatch-queue": {"lista": "list", "descartar": "discard"},
       "worktrees": {"limpar": "clean"},
    "mate": {"abrir": "open", "dormir": "sleep", "pedir": "request", "subir": "raise", "pedidos": "requests"},
    "retro": {"lacunas": "gaps", "rejeitar": "reject", "aceitar": "accept"},
}
# the `choices` values (pt -> en): the CLI accepts both and delivers the pt one, which is what the code records today
STOP_EN = {"orcamento": "budget", "decisao": "decision", "limite": "limit"}
EFFECT_EN = {"tarefa": "task", "decisao": "decision", "conversa": "conversation", "descartado": "discarded", "lembrete": "reminder"}
HOOK_EN = {"lugar": "place", "externas": "external", "prligar": "prlink"}  # the installed hooks call the pt name and it holds forever: no log


def _arg(p, pt, **kw):
    """`--<en>` and `--<pt>` on the same argument; the dest is the one from ARG_DEST."""
    return p.add_argument(f"--{FLAG_EN[pt]}", f"--{pt}", dest=ARG_DEST.get(pt, pt.replace("-", "_")), **kw)


def _value_from_pt(pt_en, label=None):
    """`type=` of an argument with choices: accepts the value in English or in pt and hands over the pt. With `label`, the pt usage goes to the log."""
    en_pt = {en: pt for pt, en in pt_en.items()}

    def convert(v):
        if v in en_pt:
            return en_pt[v]
        if label and v in pt_en:
            log(f"apelido pt: {label} {v} -> {pt_en[v]}")
        return v
    return convert


def _metavar(pt_en, include_all=None):
    """The `{a,b,c}` in --help with the English names (`include_all`: the whole list, when only some values have a translation)."""
    return "{" + ",".join(pt_en.get(v, v) for v in (include_all or pt_en)) + "}"


def parser():
    """orq's ArgumentParser. Commands, subcommands, flags and values have an English name and the pt name works as an alias (APELIDOS, FLAG_EN)."""
    ap = argparse.ArgumentParser(prog="orq")
    sub = ap.add_subparsers(dest="cmd", required=True)
    hk = sub.add_parser("hook")
    hk.add_argument("kind", type=_value_from_pt(HOOK_EN), choices=list(HOOKS), metavar=_metavar(HOOK_EN, list(HOOKS)))
    hk.add_argument("harness", nargs="?", default="claude", choices=HARNESSES, help="which agent the hook comes from (the default is the one of the hooks installed in Claude)")
    sub.add_parser("install", aliases=["instalar"], help="wires Claude Code and Codex to this clone: hooks, links, the orq wrapper (idempotent)")
    sub.add_parser("hooks-codex", help="appends the orq hooks to ~/.codex/hooks.json without reordering; then trust them in /hooks")
    i = sub.add_parser("intake")
    i.add_argument("entry")
    i.add_argument("effect", type=_value_from_pt(EFFECT_EN, "efeito"))
    i.add_argument("ref", nargs="?")
    i.add_argument("--run")
    _arg(i, "nota")
    fe = sub.add_parser("fulfill", aliases=["feito"], help='orq fulfill <e> <obligation> --proof "<url, version, hash>": closes an obligation of the notice')
    fe.add_argument("entry")
    fe.add_argument("obligation")
    _arg(fe, "prova", required=True)
    ad = sub.add_parser("defer", aliases=["adiar"], help='orq defer <e> <obligation> --reason "…": defers the obligation with a "to do later" ticket')
    ad.add_argument("entry")
    ad.add_argument("obligation")
    _arg(ad, "motivo", required=True)
    ad.add_argument("--run")
    p = sub.add_parser("pend").add_subparsers(dest="op", required=True)
    pa = p.add_parser("add")
    pa.add_argument("--id", required=True)
    _arg(pa, "tipo", required=True, type=_value_from_pt(PENDING_EN, "tipo"), choices=PENDING_TYPES, metavar=_metavar(PENDING_EN))
    _arg(pa, "titulo", required=True)
    for k in ("detalhe", "frente", "comando", "espera", "ate"):
        _arg(pa, k)
    pa.add_argument("--link")
    pa.add_argument("--task")
    pa.add_argument("--run", help="the Run of the gate task (required with the agent manager on more than one Run)")
    pl = p.add_parser("list", aliases=["lista"], help="the live pending items; --all includes the Later ones")
    _arg(pl, "todas", action="store_true")
    pd = p.add_parser("done")
    pd.add_argument("id")
    _arg(pd, "resposta")
    pe = p.add_parser("edit", help='orq pend edit <id> [--title T] [--detail D] [--stream F] [--link L] [--command C] [--waiting E] [--until AAAA-MM-DD]; "" clears the field')
    pe.add_argument("id")
    for k in ("titulo", "detalhe", "frente", "comando", "espera", "ate"):
        _arg(pe, k)
    pe.add_argument("--link")
    bl = sub.add_parser("backlog", help="the tasks-axi backlog (ORQ_BACKLOG): path, CLI version and counts; `move NN... --group G` moves tickets to a group's backlog")
    bl.add_argument("op", nargs="?", choices=["move", "mover"])
    bl.add_argument("ticket_numbers", nargs="*", help="the tickets that leave (move)")
    _arg(bl, "grupo", help="the group that receives the tickets (move)")
    bl.add_argument("--json", action="store_true")
    dv = sub.add_parser("send-back", aliases=["devolver"], help="orq send-back <task|dispatch> \"<reason>\": sends the correction to the worker of an already finished delivery and takes the delivery off the Stop until the new worker_done")
    dv.add_argument("target")
    dv.add_argument("reason")
    dv.add_argument("--run")
    st = sub.add_parser("steer")
    st.add_argument("task")
    st.add_argument("text_value")
    st.add_argument("--run")
    _arg(st, "entrada")
    pr = sub.add_parser("pr", help="the PRs of each feature linked to the task: link, list, unlink, poll (poll runs outside the hooks)").add_subparsers(dest="op", required=True)
    pl2 = pr.add_parser("link", aliases=["ligar"], help="orq pr link <task> <url> [--issue N]: registers the feature's PR (one per project environment, or merge/<feature>-<environment>)")
    pl2.add_argument("task")
    pl2.add_argument("url")
    pl2.add_argument("--issue", type=int, help="GitHub issue number, when there is one")
    pa = pr.add_parser("auto", help="orq pr auto <url> [--head B] [--wt DIR]: links the PR to the task that owns the branch; with no owner, it goes to 'PR without a task' (the prlink hook calls it)")
    pa.add_argument("url")
    pa.add_argument("--head")
    pa.add_argument("--wt")
    pa.add_argument("--cwd", help="where to find the branch's worktree when --head did not come (the branch comes from gh pr view)")
    pl2.add_argument("--tag", help="the feature's label in the digest (security, failover, …)")
    _arg(pl2, "nota", help="what the PR does, in one or two sentences, for the digest")
    po = pr.add_parser("open", aliases=["abrir"], help="orq pr open <dispatch|branch> --title T --body FILE [--environments a,b]: strips the branch prefix, checks merge-tree, pushes and opens one PR per environment linked to the task")
    po.add_argument("target")
    _arg(po, "titulo", required=True)
    _arg(po, "corpo", required=True, help="file with the PR body (sections of the /pr skill)")
    _arg(po, "ambientes", help="comma-separated branches; default: the project environments before production")
    po.add_argument("--cwd", help="where to find the worktree when the target is a branch")
    pr.add_parser("list", aliases=["lista"], help="the linked PRs (of one task, with --task)").add_argument("--task")
    pd2 = pr.add_parser("unlink", aliases=["desligar"], help="orq pr unlink <task> <url>: removes the PR from the task")
    pd2.add_argument("task")
    pd2.add_argument("url")
    _arg(pr.add_parser("poll", help="asks gh for the open PRs; a merge or a close becomes an entry, once"), "forcar", action="store_true", help="ignores the minimum interval")
    lf = sub.add_parser("clean", aliases=["limpar"], help="orq clean --closed [--dry-run]: right away cleans the worktree and branches of the tasks whose PRs are all closed without merge")
    _arg(lf, "fechados", action="store_true", required=True)
    lf.add_argument("--dry-run", action="store_true", help="only shows what it would delete")
    dg = sub.add_parser("digest", help="writes digest/atual.json (the panel contract): merge queue, features, pending items, what happened and live workers")
    _arg(dg, "desde", help="ISO timestamp (YYYY-MM-DDTHH:MM:SSZ) instead of the moment away mode turned on or the user's last message")
    dg.add_argument("--html", action="store_true", help="also writes the page digest/<date>.html")
    _arg(dg, "abrir", action="store_true", help="writes the page and opens it in an Orca tab")
    fi = sub.add_parser("queue", aliases=["fila"], help="the merge order the coordinator declares: add, done, rm, list").add_subparsers(dest="op", required=True)
    fa = fi.add_parser("add", help="orq queue add --step N --name <name> --why <why> <PR>…: declares (or replaces) the step; the PRs must already be linked")
    _arg(fa, "passo", type=int, required=True)
    _arg(fa, "nome", required=True)
    _arg(fa, "por", required=True)
    fa.add_argument("prs", nargs="+", type=int, help="PR numbers of the step")
    fi.add_parser("done", aliases=["feito"], help="marks the step as done by hand").add_argument("step", type=int)
    fi.add_parser("rm", help="removes the step from the queue").add_argument("step", type=int)
    fi.add_parser("list", aliases=["lista"], help="the declared steps")
    aw = sub.add_parser("away", aliases=["ausente"], help="away mode: orq away [on|off|status]; with no op it toggles; when turned off it shows the panel link (the `ausente` alias keeps the old behavior: with no op it shows the state)")
    aw.add_argument("op", nargs="?", choices=["on", "off", "status", "ligar", "desligar"])
    _arg(aw, "ate", help="on: end of the budget, next HH:MM local (default 08:00)")
    _arg(aw, "max-despachos", type=int, help="on: dispatch cap (default none)")
    _arg(aw, "max-falhas", type=int, default=NIGHT_FAILURES, help="on: consecutive worker failures that stop dispatching (default 3)")
    sub.add_parser("steers", help="redelivers the notice of the adjustments the stopped worker did not read and records the alert on the third failure (the manager panel already does it)")
    rp = sub.add_parser("reply", aliases=["responder"], help="answers a worker's question through the manager, binding the message's Run first")
    rp.add_argument("msg_id")
    rp.add_argument("text_value")
    sub.add_parser("status")
    start_at = sub.add_parser("start", aliases=["iniciar"], help="on the already open coordinator (Claude or Codex): checks the hooks, binds the Run, spawns or takes over the agent manager and prints the status")
    _arg(start_at, "agente", choices=HARNESSES, help="the harness of this coordinator; without it, the one of the processes above")
    start_at.add_argument("--run", help="binds this Run (run-use) instead of creating one")
    _arg(start_at, "objetivo", help="creates a new Run with this objective (one Run per stream)")
    _arg(start_at, "assumir", action="store_true", help="takes the agent manager from another coordinator that still shows up in Orca, with its Runs")
    sub.add_parser("busy", aliases=["ocupadas"], help="the paths of the worktrees with a live worker, one per line (limpar-mergeados does not delete those)")
    rs = sub.add_parser("summary", aliases=["resumo"], help="the four parts (with you, landed, in progress, coming) and the decisions since the user's last message")
    _arg(rs, "desde", help="ISO timestamp (YYYY-MM-DDTHH:MM:SSZ) instead of the user's last message")
    _arg(rs, "noite", action="store_true", help="the morning card of the last night (up to 40 lines)")
    rs.add_argument("op", nargs="?", choices=["add"], help="add \"<text>\": writes the summary to .scratch/resumos/<date>.md of the project, with the real local time")
    rs.add_argument("text_value", nargs="?")
    _arg(rs, "projeto", help="with add: the ORQ_HOME/projects file; without it the project that contains the cwd applies")
    al = sub.add_parser("alert", aliases=["alerta"], help="handles a scout alert without a reportPath").add_subparsers(dest="op", required=True)
    al.add_parser("seen", aliases=["visto"], help="marks the task's alert as seen").add_argument("task")
    agent_row = sub.add_parser("agents", aliases=["agentes"], help="the state of each dispatch across all Runs")
    agent_row.add_argument("--json", action="store_true")
    agent_row.add_argument("--run", help="only the dispatches of this Run (required if worker-list comes scoped)")
    _arg(agent_row, "todos", action="store_true", help="includes the released ones and the ones retained by Orca (user_takeover…)")
    li = sub.add_parser("release", aliases=["liberar"], help="pending ack, worker-release and terminal close if it comes back retained")
    li.add_argument("dispatch")
    li.add_argument("--run")
    li.add_argument("--processos", "--processes", dest="all_cwd", action="store_true", help="also ends the processes with cwd in the worktree that are not provably the worker's (still capped by ORQ_ENCERRA_MAX)")
    it = sub.add_parser("interrupt", aliases=["interromper"], help="sends the interrupt to the terminal of the running worker (the worker stays alive)")
    it.add_argument("dispatch")
    it.add_argument("--run")
    rt = sub.add_parser("answer-screen", aliases=["responder-tela"], help="types the option of a menu stuck on the worker's screen (permission, AskUserQuestion, trust)")
    rt.add_argument("task")
    rt.add_argument("option", help="the option number or the start of the label")
    rt.add_argument("--run")
    en = sub.add_parser("end", aliases=["encerrar"], help="worker-stop (if it still runs) and release, with the reason in the log")
    en.add_argument("dispatch")
    _arg(en, "motivo", required=True)
    _arg(en, "parada", type=_value_from_pt(STOP_EN, "parada"), choices=list(STOPS), metavar=_metavar(STOP_EN), help="names the stop on the morning card (stopped: budget|pending decision|usage limit)")
    en.add_argument("--run")
    rl = sub.add_parser("relaunch", aliases=["relancar"], help="stops the worker and starts another in the same worktree and task (--retry-of), with a note on what changed")
    rl.add_argument("dispatch")
    _arg(rl, "nota", required=True)
    _arg(rl, "modelo", help="changes the model (goes together with --effort); without it, the old worker's")
    rl.add_argument("--effort")
    rl.add_argument("--run")
    tc = sub.add_parser("transcript", aliases=["transcrito"], help="reads the end of a worker's transcript (Claude or Codex): messages and tool calls, without reasoning or meta records, and the list of what it cut")
    tc.add_argument("dispatch")
    _arg(tc, "ultimos", type=int, default=20, help="only the last N events (default 20)")
    tc.add_argument("--json", action="store_true")
    pg = sub.add_parser("handoff", aliases=["passagem"], help="writes the HANDOFF.md of a worker with no turn (plan limit), facts only and within 20 s; it neither stops nor starts a worker")
    pg.add_argument("dispatch", help="the worker's dispatch, or `coordenador` to record the snapshot of this session (the other harness's session hook injects it)")
    _arg(pg, "para", choices=list(HARNESSES), help="the harness that will read it (default: the other one)")
    pg.add_argument("--run")
    ps = sub.add_parser("switch", aliases=["passar"], help="continues the worker on another harness (claude|codex) in the same worktree and task: HANDOFF.md, worker-start --retry-of --agent")
    ps.add_argument("dispatch")
    _arg(ps, "para", required=True, choices=list(HARNESSES))
    _arg(ps, "modelo", help="the model on the other harness (goes together with --effort); without it the worker-routing equivalence table applies")
    ps.add_argument("--effort")
    ps.add_argument("--run")
    rm = sub.add_parser("remind", aliases=["lembrar"], help='reminders: orq remind "<text>" --in 1h30 | --at 15:00; orq remind list [--all]; orq remind cancel <id>. The manager fires them with a macOS notification')
    rm.add_argument("op_or_text", nargs="+")
    rm.add_argument("--in", dest="in_", help="90m, 1h30, 2h")
    rm.add_argument("--at", help="local HH:MM (tomorrow if it has passed)")
    rm.add_argument("--all", action="store_true")
    nt = sub.add_parser("night", aliases=["noite"], help="night mode: orq night on --until HH:MM [--max-dispatches N] [--max-failures 3] | off | (no op: state)")
    nt.add_argument("op", nargs="?", choices=["on", "off", "ligar", "desligar"])
    _arg(nt, "ate")
    _arg(nt, "max-despachos", type=int)
    _arg(nt, "max-falhas", type=int, default=NIGHT_FAILURES)
    from_ = sub.add_parser("dispatch", aliases=["despachar"], help="worker-start with model and effort, event and intake")
    from_.add_argument("--run", required=True)
    _arg(from_, "titulo")
    _arg(from_, "spec-arquivo")
    from_.add_argument("--ticket", help="number of a ticket created by orq ticket new: the worker starts on its task (instead of --title and --spec-file)")
    _arg(from_, "agente", help=f"the worker's harness ({', '.join(HARNESSES)}); without it the project's applies and, with no project, claude")
    _arg(from_, "projeto", help="a file in ORQ_HOME/projects (orq projects); without it the project whose repo contains the cwd applies")
    _arg(from_, "modelo", required=True)
    from_.add_argument("--effort", required=True)
    from_.add_argument("--worktree", choices=["current", "new-top-level"])
    from_.add_argument("--name")
    from_.add_argument("--base-branch")
    _arg(from_, "entrada")
    _arg(from_, "prioridade", type=int, choices=[1, 2, 3], help="1 high to 3 low; without it the one from the title's stream applies (security and production 1, failover, diagnostics and panel 3)")
    from_.add_argument("--direct", "--direto", action="store_true", help="dispatch here even when the request belongs to a group with a mate (otherwise it goes to the mate); the event records the reason")
    rp = sub.add_parser("run", help="what orq keeps about a Run").add_subparsers(dest="op", required=True).add_parser("project", aliases=["projeto"], help="binds the Run to a project in ORQ_HOME/projects: its dispatches start in the project's repo")
    rp.add_argument("item_name")
    rp.add_argument("--run")
    pj = sub.add_parser("projects", aliases=["projetos"], help="the projects in ORQ_HOME/projects/<name>.json (repo, workers' harness, group, environments)")
    pj.add_argument("--json", action="store_true")
    pa = sub.add_parser("project", aliases=["projeto"], help="new project: registers in Orca and generates the orca.yaml (add), or trusts the folder in Codex (trust)").add_subparsers(dest="op", required=True)
    paa = pa.add_parser("add", help="orq project add <path|url>: writes projects/<name>.json, registers the repo in Orca if missing and writes the orca.yaml")
    paa.add_argument("target")
    _arg(paa, "nome")
    paa.add_argument("--harness", choices=sorted(HARNESS_TRUST))
    _arg(paa, "grupo")
    _arg(paa, "destino", help="url: the clone folder (default ~/Developer/<name>)")
    paa.add_argument("--orca-yaml", dest="orca_yaml", help="the orca.yaml the agent proposed: replaces the detected blocks (the fixed part always goes in)")
    _arg(paa, "substituir-orca-yaml", action="store_true", help="replaces the orca.yaml the repository already has (without the flag it only shows the diff)")
    paa.add_argument("--dry-run", action="store_true", help="shows the orca.yaml and what it would do, without writing or registering")
    paa.add_argument("--json", action="store_true")
    pa.add_parser("trust", aliases=["confiar"], help="marks the cwd (and its repo root) as trusted in Codex: the fixed line of the orca.yaml setup of a Codex project")
    fl = sub.add_parser("flow", aliases=["fluxo"], help="the environments and production of the project that contains --repo (limpar-mergeados.py reads from here)")
    fl.add_argument("--repo", default=".")
    fl.add_argument("--json", action="store_true")
    _arg(from_, "servico", action="store_true", help="service worker (integrator, secondmate): stays alive after worker_done and reports each cycle with orq cycle done")
    cc = sub.add_parser("cycle", aliases=["ciclo"], help="service worker: reports a finished cycle (without an Orca capability)").add_subparsers(dest="op", required=True)
    sv = sub.add_parser("service", aliases=["servico"], help="marks an existing dispatch as a service").add_subparsers(dest="op", required=True).add_parser("mark", aliases=["marcar"], help="orq service mark <dispatch>: the same effect as --service on the dispatch")
    sv.add_argument("dispatch")
    cf = cc.add_parser("done", aliases=["feito"], help="orq cycle done --dispatch <id> --hash <commit> [--note <text>]")
    cf.add_argument("--dispatch", required=True)
    cf.add_argument("--hash", required=True)
    _arg(cf, "nota")
    ig = sub.add_parser("integrate", aliases=["integrar"], help="the integrator's queue").add_subparsers(dest="op", required=True)
    igf = ig.add_parser("queue", aliases=["fila"], help="the branches waiting for the integrator: add, rm, list").add_subparsers(dest="action", required=True)
    iga = igf.add_parser("add", help="orq integrate queue add <branch> <ticket>: the ticket's worker is left awaiting integration, not stopped")
    iga.add_argument("branch")
    iga.add_argument("ticket")
    igf.add_parser("rm", help="removes the ticket from the queue").add_argument("ticket")
    igf.add_parser("list", aliases=["lista"], help="the branches in the queue").add_argument("--json", action="store_true")
    igc = ig.add_parser("conclude", aliases=["concluir"], help="orq integrate conclude --hash <new main> <branch>...: what integrar.py calls after the fast-forward; closes queue, ticket and worker and records the cycle")
    igc.add_argument("--hash", required=True)
    igc.add_argument("--dispatch", help="the integrator's dispatch (default: the not yet released service titled integrador)")
    igc.add_argument("branches", nargs="+")
    wl = sub.add_parser("worktrees", help="orq worktrees clean [--dry-run]: removes the ORQ_WT worktrees already contained in origin/main").add_subparsers(dest="op", required=True)
    wl.add_parser("clean", aliases=["limpar"], help="removes the ORQ_WT worktrees already contained in origin/main").add_argument("--dry-run", action="store_true")
    au = sub.add_parser("audit-publication", aliases=["auditar-publicacao"], help="orq audit-publication <base>..<head>: refuses a wrong author, trailer, forbidden term and code without a README before publishing main")
    au.add_argument("revs", nargs="+", help="git rev-list args; on a new branch: <head> --not --remotes (after --)")
    tk = sub.add_parser("ticket", help="tickets as files (ISSUES/NN-slug.md) with the task in Orca").add_subparsers(dest="op", required=True)
    tn = tk.add_parser("new", aliases=["novo"], help="creates the file and the task from a title and a spec file")
    _arg(tn, "titulo", required=True)
    _arg(tn, "spec-arquivo", required=True)
    tn.add_argument("--blocked-by", help="numbers of the tickets that block this one, comma-separated")
    tn.add_argument("--run", help="the task's Run (the one bound to the coordinator, by default)")
    for k, h in (("modelo", "the model the released ticket starts with on its own"), ("despacho", "`manual[, reason]`: never starts on its own"), ("espera", "`integrador vazio`")):
        _arg(tn, k, help=h)
    tn.add_argument("--effort", help="its effort")
    tf = tk.add_parser("close", aliases=["fechar"], help="writes the Answer, sets resolved and completes the task")
    tf.add_argument("numero")
    tf.add_argument("--answer", required=True, help="text or the path of a file")
    dc = sub.add_parser("doctor", help="checks orq's state against Orca and fixes what it can").add_subparsers(dest="op", required=True)
    dt = dc.add_parser("tasks", help="completes the blocked/pending task of a resolved ticket (supersededBy) and lists the one with no ticket")
    dt.add_argument("--dry-run", action="store_true", help="only lists")
    dt.add_argument("--json", action="store_true")
    da = dc.add_parser("old", aliases=["antigos"], help="lists (and with --release takes off the live ones) the dispatch older than 24 h that is not released, has no terminal and has a resolved ticket")
    _arg(da, "liberar", action="store_true")
    _arg(da, "horas", type=float, default=24, help="minimum age of the dispatch (default 24)")
    da.add_argument("--ticket", action="append", default=[], help="only this ticket (repeatable)")
    da.add_argument("--json", action="store_true")
    dh = dc.add_parser("hooks", help="checks the interpreter of each orq hook (it must import orqlib: Python 3.12+); --pin writes the absolute path into the hook commands and the `orq` link")
    dh.add_argument("--pin", action="store_true")
    dbk = dc.add_parser("backlog", help="cross-checks the backlog tickets with the Orca tasks and prints the fix for each difference (writes nothing)")
    dbk.add_argument("--json", action="store_true")
    dsc = dc.add_parser("scratch", help="lists the .scratch tickets still ready-for-agent in a phase already declared integrated (writes nothing)")
    dsc.add_argument("--json", action="store_true")
    ph = sub.add_parser("phase", aliases=["fase"], help='orq phase "<text>": for each phase the text cites ("#2039 fase 1", ".scratch/<feature> phase 2"), the plan tickets still missing')
    ph.add_argument("texto", metavar="text")
    ph.add_argument("--json", action="store_true")
    te = tk.add_parser("edit", aliases=["editar"], help="changes the model, effort, dispatch or wait of a ticket (an empty value removes the field)")
    te.add_argument("numero")
    for k in ("modelo", "despacho", "espera"):
        _arg(te, k)
    te.add_argument("--effort")
    tl = tk.add_parser("list", aliases=["lista"], help="the open tickets (--all includes the resolved ones)")
    _arg(tl, "todos", action="store_true")
    tl.add_argument("--json", action="store_true")
    sub.add_parser("lavish-answer", aliases=["lavish-resposta"], help="records the answer of a decision made through Lavish").add_argument("file_name", help="output of `lavish-axi poll` (raw, or its JSON); `-` reads standard input")
    pg = sub.add_parser("ask", aliases=["perguntar"], help="decision through Lavish (works in Claude and Codex): a page with the options, waits for the answer and closes the pending item")
    pg.add_argument("--id", required=True, help="id of the decision pending item (up to 12 characters; created if it does not exist)")
    _arg(pg, "pergunta", required=True)
    _arg(pg, "opcao", action="append", default=[], help="one option; repeat it (minimum 2)")
    _arg(pg, "recomendada", type=int, default=1, help="number (1..N) of the recommended option; it only marks, it does not preselect")
    _arg(pg, "detalhe")
    _arg(pg, "espera-min", type=float, help=f"minutes until giving up (default {ASK_MIN:g}); the pending item stays open")
    _arg(pg, "sem-poll", action="store_true", help="only builds and opens the page; record the poll afterwards with orq lavish-answer")
    _arg(sub.add_parser("audit-answers", aliases=["auditar-respostas"], help="audits a session's coordinator answers"), "sessao", help="session id (or prefix); without it, the last coordinator session in cursor.json")
    ge = sub.add_parser("manager", aliases=["gerente"], help="agent manager in its own terminal: Orca notices go to it, not to the coordinator").add_subparsers(dest="op", required=True)
    gl = ge.add_parser("bind", aliases=["ligar"], help="on the coordinator: binds Runs to the agent manager terminal (adds to the ones it already has)")
    gl.add_argument("--terminal", required=True)
    gl.add_argument("--run", action="append", help="repeat to bind several; without --run the Run bound to the coordinator goes in")
    _arg(gl, "assumir", action="store_true", help="takes the gerente.json from another coordinator that still shows up in Orca, with its Runs")
    gd = ge.add_parser("unbind", aliases=["desligar"], help="on the coordinator: gives one Run (--run) or all of them back to this terminal")
    gd.add_argument("--run")
    _arg(gd, "assumir", action="store_true", help="also unbinds the gerente.json of another coordinator that still shows up in Orca")
    ge.add_parser("check", aliases=["checar"], help="checks in Orca whether the agent manager terminal still exists (the coordinator prompt calls it, in the background)")
    group_map = ge.add_parser("spawn", aliases=["subir"], help="on the coordinator: the agent manager terminal is gone; creates another with the panel and rebinds all the Runs in gerente.json")
    _arg(group_map, "forcar", action="store_true", help="spawns even with the old terminal still in Orca")
    ge.add_parser("interval", aliases=["intervalo"], help="how many seconds the panel sleeps before the next round (the panel shell calls it)")
    ga = ge.add_parser("absorb", aliases=["absorver"], help="in the agent manager terminal: confirms heartbeats and notifies the coordinator of the rest, Run by Run")
    _arg(ga, "estado", action="store_true", help=argparse.SUPPRESS)  # the serve: writes gerente-estado.json after the loop
    gv = ge.add_parser("serve", help="the agent manager without a terminal: absorbs in a loop outside Orca, with a log in logs/gerente.log")
    _arg(gv, "voltas", type=int, help=argparse.SUPPRESS)
    gvo = gv.add_mutually_exclusive_group()
    gvo.add_argument("--status", action="store_true", help="whether it runs, the pid, the launchd and the last round")
    for op, help_text in (("parar", "stops the serve (and the launchd until the next login)"),
                      ("instalar", "writes and loads the launchd agent: starts at login and restarts if it dies"), ("desinstalar", "removes the launchd agent")):
        _arg(gvo, op, action="store_true", help=help_text)
    gt = ge.add_parser("tui", help="opens the TUI (OpenTUI, needs Bun) that follows the manager, the workers and the queues; read-only")
    gt.add_argument("--theme", choices=["light", "dark", "auto"], default="auto", help="color palette; auto asks the terminal (OSC 11), then COLORFGBG and the macOS appearance")
    rt = sub.add_parser("resume", aliases=["retomar"], help="after an outage: spawns the agent manager and resumes, with claude --resume, the workers without worker_done that lost their terminal")
    rt.add_argument("--dry-run", action="store_true", help="only lists")
    rt.add_argument("--run", help="only the dispatches of this Run")
    rt.add_argument("--json", action="store_true")
    _arg(rt, "pausados", action="store_true", help="resumes the workers that orq pause stopped (instead of the ones that dropped); refuses while usage is still high")
    _arg(rt, "forcar", action="store_true", help="with --paused, resumes even with usage above the threshold")
    hb = sub.add_parser("hibernate", aliases=["hibernar"], help="closes the terminal of an idle worker and keeps the session (the manager does it by itself, with the README criterion); steer, reply and orq wake bring it back")
    hb.add_argument("target", help="task or dispatch id")
    hb.add_argument("--run")
    _arg(hb, "forcar", action="store_true", help="skips the lack of proof of a child process (ps/lsof not finding the agent); it never skips a live child process, a busy screen or a coordinator")
    hb.add_argument("--json", action="store_true")
    ac = sub.add_parser("wake", aliases=["acordar"], help="brings the hibernated worker (task or dispatch) back, with the harness resume")
    ac.add_argument("target")
    _arg(ac, "texto", help="what the worker receives on waking (the default is 'woken by hand')")
    ac.add_argument("--json", action="store_true")
    pz = sub.add_parser("pause", aliases=["pausar"], help="pauses workers to free up room in the plan: PAUSE.md, closes the terminal, records the pause. With no argument: low priority and the ones that are only investigating")
    pz.add_argument("tasks", nargs="*", help="task or dispatch ids; without them the priority criterion applies")
    _arg(pz, "ate-prioridade", type=int, choices=[1, 2, 3], help="pauses the ones with priority N and lower (3 = only the low ones)")
    pz.add_argument("--run")
    pz.add_argument("--dry-run", action="store_true", help="only lists")
    pz.add_argument("--json", action="store_true")
    pr_ = sub.add_parser("priority", aliases=["prioridade"], help="changes the priority (1 high to 3 low) of a task; orq agents, the digest, orq pause and the budget refusal use this order")
    pr_.add_argument("task")
    pr_.add_argument("value", type=int, choices=[1, 2, 3])
    uz = sub.add_parser("usage", aliases=["uso"], help="plan usage (week and 5 h window) read from the HUD, the level and the decision on new dispatches")
    uz.add_argument("--json", action="store_true")
    _arg(uz, "agente", default="claude", choices=HARNESSES, help="which plan (the Claude and Codex quotas are separate)")
    rv = sub.add_parser("review", aliases=["revisar"], help="only the no-mistakes review in the task's worktree (ticket 146), with the cheap model of orq's NM_HOME; refuses on usage pause/hold and with no expensive slot")
    rv.add_argument("task", help="the task id or the ticket number")
    rv.add_argument("--json", action="store_true")
    mq = sub.add_parser("machine", aliases=["maquina"], help="the machine budget (ticket 79): what it has now, what orq decides and the slots. `orq machine set <key> <value>` adjusts maquina.json")
    mq.add_argument("op", nargs="?", choices=["set"])
    mq.add_argument("key_name", nargs="?")
    mq.add_argument("value", nargs="?", help="as JSON: 4, true, [\"claude-opus-*\"]")
    mq.add_argument("--json", action="store_true")
    fd = sub.add_parser("dispatch-queue", aliases=["fila-despacho"], help="the dispatches and resumes waiting for a slot on the machine: list | rm <id>").add_subparsers(dest="op", required=True)
    fd.add_parser("list", aliases=["lista"], help="the dispatches in the queue").add_argument("--json", action="store_true")
    fd.add_parser("rm").add_argument("id")
    dc = fd.add_parser("discard", aliases=["descartar"], help="marks the `desistiu` give-up as resolved: takes the item off the away Stop")
    dc.add_argument("id")
    _arg(dc, "motivo", required=True)
    cx = sub.add_parser("inbox", aliases=["caixa"], help="reads the Orca inbox (check) and with --ack acknowledges it in the same generation; returns the binding to the previous Run")
    cx.add_argument("run", nargs="?")
    cx.add_argument("--ack", action="store_true")
    _arg(cx, "todas", action="store_true", help="goes through the Runs with an unread message")
    ru = sub.add_parser("runs", help="the Runs with open or recent work (--all: the archive and the test ones)")
    _arg(ru, "todos", action="store_true")
    ru.add_argument("--json", action="store_true")
    pending_gate_list = sub.add_parser("groups", aliases=["grupos"], help="the groups (ORQ_HOME/groups/*.json) and the mates; with --title/--cwd/--group it says which group the request goes to")
    _arg(pending_gate_list, "titulo")
    pending_gate_list.add_argument("--cwd")
    _arg(pending_gate_list, "grupo")
    mt = sub.add_parser("mate", help="a group's secondmate: open | sleep | request | raise | requests").add_subparsers(dest="op", required=True)
    mt.add_parser("open", aliases=["abrir"], help="opens the group's mate").add_argument("group_name")
    mt.add_parser("sleep", aliases=["dormir"], help="hibernates the group's mate (the manager does it by itself after ORQ_MATE_DORMIR_MIN idle minutes); orq mate request wakes it").add_argument("group_name")
    mp = mt.add_parser("request", aliases=["pedir"], help="asks the group's mate for something")
    mp.add_argument("group_name")
    _arg(mp, "texto", required=True)
    _arg(mp, "prazo", type=int, default=REQUEST_DEADLINE_S, help="seconds from the end of the mate's turn until the repost; 0: does not wait for an answer")
    _arg(mp, "responde", help="the entry the mate raised and this request answers: closes with the mate effect")
    ms = mt.add_parser("raise", aliases=["subir"], help="the mate raises an answer, decision, PR, blocker or summary to the coordinator")
    _arg(ms, "tipo", required=True, type=_value_from_pt(ESCALATION_EN, "tipo"), choices=ESCALATION_TYPES, metavar=_metavar(ESCALATION_EN))
    _arg(ms, "texto", required=True)
    ms.add_argument("--corr")
    ms.add_argument("--link")
    _arg(ms, "grupo")
    _arg(mt.add_parser("requests", aliases=["pedidos"], help="the requests to the mate still without an answer"), "grupo")
    sub.add_parser("ingest").add_argument("--refresh", action="store_true", help="after the ingest, rebuilds open.json")
    rr = sub.add_parser("retro", help="the failure signals from the events, transcripts and PRs of a window (default 7 days), without an LLM: how many, which cases, on which model")
    _arg(rr, "desde", help="start of the window (date or ISO)")
    _arg(rr, "ate", help="end of the window (default: now)")
    _arg(rr, "projeto", help="only the cases whose path, title or task contains the snippet")
    rr.add_argument("op", nargs="?", choices=["gaps", "lacunas", "reject", "rejeitar", "accept", "aceitar"], metavar="{gaps,reject,accept}",
                    help="gaps: the ledger of gaps kept between rounds; reject <id> --reason T: the proposal only comes back with more sessions; accept <id> --ticket N: the gap is covered when the ticket closes")
    rr.add_argument("ref", nargs="?", help="the gap id (reject, accept)")
    rr.add_argument("--ticket", help="accept: the ticket that answers the gap")
    rr.add_argument("--json", action="store_true")
    _arg(rr, "motivo", help="reject: why the proposal was turned down")
    _arg(rr, "sem-gh", action="store_true", help="does not ask gh for the CI and review of the PRs")
    _arg(rr, "sem-transcritos", action="store_true", help="does not read the workers' transcripts (violated rules)")
    _arg(rr, "gravar", action="store_true", help="keeps the metrics in ORQ_HOME/retro for the next round to compare")
    return ap


def normalize_aliases(a, args):
    """Swaps the pt aliases in `a.cmd`, `a.op` and `a.action` for the English name (the dispatch compares against it) and returns the pt aliases used, commands and flags."""
    used = []

    def troca(key_name, value):
        en = ALIASES.get(key_name, {}).get(value)
        if en:
            used.append(f"{value} -> {en}")
        return en or value
    a.cmd = troca("", a.cmd)
    if isinstance(getattr(a, "op", None), str):
        a.op = troca(a.cmd, a.op)
    if isinstance(getattr(a, "action", None), str):
        a.action = troca(f"{a.cmd} {a.op}", a.action)
    used += [f"{f} -> {FLAG_ALIASES[f]}" for f in (t.split("=")[0] for t in args) if f in FLAG_ALIASES]
    return used


def main(argv=None):
    ap = parser()
    args = sys.argv[1:] if argv is None else argv
    if args[:1] == ["hook"]:
        try:
            a = ap.parse_args(args)
        except SystemExit as e:  # fail-open: in Codex, argparse's exit 2 would block the user's prompt
            if e.code:
                log(f"hook with invalid arguments: {args}")
            return 0
    else:
        a = ap.parse_args(args)
    if a.cmd == "hook":
        return run_hook(a.kind, a.harness)
    away_alias = a.cmd == "ausente"  # the alias keeps the old behavior: with no op it shows the state and ligar/desligar (on/off) prints the state
    for u in normalize_aliases(a, args):
        log(f"apelido pt: {u}")
    try:
        if a.cmd == "intake":
            print(json.dumps(intake(a.entry, a.effect, a.ref, a.run, a.note), ensure_ascii=False))
        elif a.cmd == "fulfill":
            print(json.dumps(obligation_done(a.entry, a.obligation, a.proof), ensure_ascii=False))
        elif a.cmd == "defer":
            print(json.dumps(defer_obligation(a.entry, a.obligation, a.reason, a.run), ensure_ascii=False))
        elif a.cmd == "pend":
            if a.op == "add":
                print(json.dumps(pending_add(a.id, a.type_name, a.title, a.detail, a.workstream, a.link, a.command, a.waiting, a.task, a.until_at, a.run), ensure_ascii=False))
                _implicit("decisao" if a.type_name == "decisao" else "pend", a.id)
            elif a.op == "list":
                print("\n".join(pending_list(a.all_listing)) or "no pending items")
            elif a.op == "edit":
                print(json.dumps(pending_edit(a.id, titulo=a.title, detalhe=a.detail, frente=a.workstream, link=a.link, comando=a.command, espera=a.waiting, ate=a.until_at), ensure_ascii=False))
            else:
                done = pending_done(a.id, a.answer_text)
                print(json.dumps(done, ensure_ascii=False))
                if done.get("aviso"):
                    print(f"warning: {done['aviso']}", file=sys.stderr)
        elif a.cmd == "backlog" and a.op == "move":
            if not (a.ticket_numbers and a.group_name):
                print("backlog move: pass the ticket numbers and --group", file=sys.stderr)
                return 2
            print(json.dumps(backlog_mover(a.ticket_numbers, a.group_name), ensure_ascii=False))
        elif a.cmd == "backlog":
            r = backlog_state()
            print(json.dumps(r, ensure_ascii=False) if a.json else "\n".join(f"{k}: {v}" for k, v in r.items()))
        elif a.cmd == "pr":
            if a.op == "link":
                print(json.dumps(pr_link(a.task, a.url, a.issue, a.tag, a.note), ensure_ascii=False))
            elif a.op == "auto":
                print(json.dumps(pr_auto(a.url, a.head, a.wt, a.cwd), ensure_ascii=False))
            elif a.op == "open":
                urls, notices = pr_open(a.target, a.title, a.body_text, [x for x in (a.environments or "").split(",") if x] or None, a.cwd)
                for av in notices:
                    print(f"warning: {av}", file=sys.stderr)
                print("\n".join(urls))
            elif a.op == "unlink":
                pr_unlink(a.task, a.url)
                print(f"PR unlinked from {a.task}")
            elif a.op == "list":
                print("\n".join(pr_list(a.task)) or "no PR linked")
            else:
                print("\n".join(pr_poll(force=a.force)) or "no changes in the PRs")
        elif a.cmd == "send-back":
            print(json.dumps(send_back(a.target, a.reason, a.run), ensure_ascii=False))
        elif a.cmd == "steer":
            ev = steer(a.task, a.text_value, a.run, a.entry)
            print(json.dumps(ev, ensure_ascii=False))
            if not a.entry:
                _implicit("steer", a.task, ev.get("run"))
        elif a.cmd == "steers":
            print("\n".join(redeliver_steers()) or "no adjustment to redeliver")
        elif a.cmd == "reply":
            print(json.dumps(reply_to(a.msg_id, a.text_value), ensure_ascii=False))
        elif a.cmd == "alert":
            print(json.dumps(append_event({"tipo": "alerta_visto", "task": a.task}), ensure_ascii=False))
        elif a.cmd == "clean":
            print("\n".join(clean_closed(dry=a.dry_run)) or "no task with all PRs closed without merge")
        elif a.cmd == "busy":
            print("\n".join(sorted(busy_worktrees())))
        elif a.cmd == "install":
            print("\n".join(install()))
        elif a.cmd == "hooks-codex":
            add = install_codex_hooks(os.path.join(os.path.dirname(os.path.abspath(__file__)), "codex.hooks.example.json"))
            print("\n".join([*(f"added: {ev} group {g}" for ev, g in add), codex_hooks_notice() or "orq hooks trusted in Codex"]))
        elif a.cmd == "status":
            print(status_text())
        elif a.cmd == "start":
            print(start(a.agent, a.run, a.objective, a.take_over))
        elif a.cmd == "summary" and a.op == "add":
            print(add_summary(a.text_value, a.project))
        elif a.cmd == "summary" and a.night:
            print(morning_card())
        elif a.cmd == "summary":
            if a.since:
                _dt(a.since)  # ValueError vira exit 1
            print(summary_four(read_events(), _read_json(_path("open.json")), _pending_ro(), tickets(), a.since and _dt(a.since).strftime("%Y-%m-%dT%H:%M:%SZ"), panel=panel_notice()))
        elif a.cmd == "agents":
            agent_rows = agents(a.run, a.include_all)
            stopped_by = [l for l in night_lines(_cursor_ro(), read_events()) if "Stopped dispatching" in l]
            print(json.dumps(agent_rows, ensure_ascii=False) if a.json else "\n".join([*filter(None, [codex_hooks_notice()]), agents_text(agent_rows), *hibernation_lines(), *stopped_by]))
        elif a.cmd == "transcript":
            r = transcript(a.dispatch, a.last_n)
            print(json.dumps(r, ensure_ascii=False) if a.json else transcript_text(r))
        elif a.cmd == "inbox":
            print("\n".join(inbox(a.run, a.ack, a.all_listing)))
        elif a.cmd == "runs":
            rs = runs_list(a.include_all)
            print(json.dumps(rs, ensure_ascii=False) if a.json else runs_text(rs))
        elif a.cmd == "release":
            r = release(a.dispatch, a.run, a.all_cwd)
            print(json.dumps(r, ensure_ascii=False))
            if r["aviso"]:
                print(f"warning: {r['aviso']}", file=sys.stderr)
        elif a.cmd == "answer-screen":
            print(json.dumps(answer_screen(a.task, a.option, a.run), ensure_ascii=False))
        elif a.cmd in ("interrupt", "end", "relaunch", "switch", "handoff"):
            r = interrupt(a.dispatch, a.run) if a.cmd == "interrupt" else terminate(a.dispatch, a.reason, a.run, a.stopped_by) if a.cmd == "end" \
                else let_pass(a.dispatch, a.to_, a.model, a.effort, a.run) if a.cmd == "switch" else coordinator_handoff(a.to_) if (a.cmd, a.dispatch) == ("handoff", "coordenador") \
                else handoff(a.dispatch, a.to_, a.run) if a.cmd == "handoff" else relaunch(a.dispatch, a.note, a.model, a.effort, a.run)
            print(json.dumps(r, ensure_ascii=False))
            if r.get("aviso"):
                print(f"warning: {r['aviso']}", file=sys.stderr)
        elif a.cmd == "digest":
            d, file_path, page_data = digest_generate(since=_dt(a.since).strftime("%Y-%m-%dT%H:%M:%SZ") if a.since else None, with_html=a.html or a.open_page)
            print("\n".join(x for x in (file_path, page_data) if x))
            print(f"{sum(i['estado'] == 'OPEN' for g in d['fila'] for i in g['prs'])} open PR(s), {len(d['pendencias'])} pending item(s), "
                  f"{len(d['rodando'])} running, {len(d['linha']) + d['pagina']['linha_antes']} in what happened")
            if a.open_page:
                try:
                    digest_open(page_data)
                except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:
                    print(f"warning: the tab did not open ({e}); open {page_data}", file=sys.stderr)
        elif a.cmd == "queue":
            if a.op == "add":
                print(json.dumps(queue_add(a.step, a.item_name, a.by, a.prs), ensure_ascii=False))
            elif a.op in ("done", "rm"):
                queue_mark(a.step, "feito" if a.op == "done" else a.op)
                print(f"step {a.step}: {'marked done' if a.op == 'done' else 'removed from the queue'}")
            else:
                print("\n".join(queue_list()) or "no step declared")
        elif a.cmd == "away" and away_alias:
            if a.op == "on":
                away_on()
            elif a.op == "off":
                away_off()
            print("\n".join(away_lines(_cursor_ro())))
        elif a.cmd == "away":
            print("\n".join(away(a.op, a.until_at, a.max_dispatches, a.max_failures)))
        elif a.cmd == "remind":
            words = a.op_or_text
            if words[0] in ("list", "lista"):
                print("\n".join(remind_list(a.all)) or "no open reminders")
            elif words[0] in ("cancel", "cancelar"):
                print(json.dumps(remind_cancel(words[1] if len(words) > 1 else ""), ensure_ascii=False))
            else:
                item = remind_add(" ".join(words), a.in_, a.at)
                print(json.dumps(item, ensure_ascii=False))
                _implicit("lembrete")
        elif a.cmd == "night":
            if a.op == "on":
                night_on(a.until_at, a.max_dispatches, a.max_failures)
            elif a.op == "off":
                print("night mode off" if night_off() else "night mode was already off")
                return 0
            print("\n".join(night_lines(_cursor_ro(), read_events())) or "night mode off")
        elif a.cmd == "dispatch":
            group_name, group_reason = (None, None) if a.service else dispatch_group(a.title, a.ticket, a.project, a.run)
            if group_name and not a.direct:
                print(json.dumps(dispatch_to_mate(group_name, group_reason, a.run, a.title, a.ticket, a.spec_file, a.model, a.effort), ensure_ascii=False))
                return 0
            r = dispatch_worker(a.run, a.title, a.spec_file, a.model, a.effort, a.worktree, a.name, a.base_branch, a.entry, a.ticket, a.priority_level, a.agent, a.project, service=a.service,
                                direct=f"--direct: group {group_name} ({group_reason})" if group_name else None)
            print(json.dumps(r, ensure_ascii=False))
            if r.get("aviso"):
                print(f"warning: {r['aviso']}", file=sys.stderr)
            if not a.entry and r.get("taskId"):  # queued doesn't have a task yet
                _implicit("tarefa", r["taskId"], r["run"])
        elif a.cmd == "run":
            run = default_run(a.run)
            if not run:
                raise ValueError("no Run bound: pass --run")
            print(json.dumps(run_store_project(run, a.item_name), ensure_ascii=False))
        elif a.cmd == "flow":
            flow_info = repo_flow(a.repo)
            print(json.dumps(flow_info, ensure_ascii=False) if a.json else f"production {flow_info['producao']}; environments {', '.join(flow_info['ambientes'])}; flow {flow_info['fluxo']}" + ("" if flow_info["declarado"] else " (default: no ambientes block)"))
        elif a.cmd == "project" and a.op == "trust":
            cwd = os.path.realpath(os.getcwd())
            print("\n".join(f"codex: trusted folder {p}" for p in trust_codex(_repo_root(cwd), cwd)))
        elif a.cmd == "project":
            r = add_project(a.target, a.item_name, a.harness, a.group_name, a.orca_yaml, a.destination, a.replace_orca_yaml, a.dry_run)
            if a.json:
                print(json.dumps(r, ensure_ascii=False))
            else:
                print(f"project {r['projeto']}: {r['repo']} (worktree base {r['base']})" + (" [dry-run: nothing written]" if r["dry_run"] else
                      f"\n  file {r['arquivo']} {'created' if r['arquivo_novo'] else 'kept'}; Orca: {'registered' if r['registrado_no_orca'] else 'already registered'}"))
                print(f"orca.yaml ({r['orca_yaml_estado']}):\n{r['orca_yaml']}")
                if r["diff"]:
                    print(f"difference from the orca.yaml the repository has:\n{r['diff']}")
                if r["orca_yaml_estado"] == "mantido":
                    print("the existing orca.yaml was not replaced: ask the user and, if they accept, run again with --replace-orca-yaml")
        elif a.cmd == "projects":
            ps = projects()
            if a.json:
                print(json.dumps([{"nome": n, **d} for n, d in ps.items()], ensure_ascii=False))
            else:
                print("\n".join(f"{n}  {d['repo'] or '-'}  {d['harness'] or '-'}" + (f"  group {d['grupo']}" if d["grupo"] else "") +
                                (f"  environments {' > '.join(d['ambientes'])} ({d['fluxo']}, production {d['producao']})" if d["ambientes"] else "") +
                                (f"  E2E queue {d['fila_e2e']}" if d["fila_e2e"] else "") +
                                (f"  invalid: {d['erro']}" if d["erro"] else "") for n, d in ps.items()) or f"no project in {_path('projects')}")
        elif a.cmd == "service":
            print(json.dumps(mark_service(a.dispatch), ensure_ascii=False))
        elif a.cmd == "cycle":
            print(json.dumps(cycle_done(a.dispatch, a.hash, a.note), ensure_ascii=False))
        elif a.cmd == "integrate" and a.op == "conclude":
            r = integrate_conclude(a.hash, a.branches, a.dispatch)
            print(json.dumps(r, ensure_ascii=False))
            for x in r["avisos"]:
                print(f"warning: {x}", file=sys.stderr)
        elif a.cmd == "integrate" and a.action == "add":
            print(json.dumps(integrate_queue_add(a.branch, a.ticket), ensure_ascii=False))
        elif a.cmd == "integrate" and a.action == "rm":
            print(json.dumps(integrate_queue_rm(a.ticket), ensure_ascii=False))
        elif a.cmd == "integrate":
            item_list = list(integration_queue().values())
            print(json.dumps(item_list, ensure_ascii=False) if a.json else "\n".join(f"{i['ticket']} {i['branch']} (since {_hora_local(i['ts'])})" for i in item_list) or "integrator queue empty")
        elif a.cmd == "worktrees":
            r = clean_orq_worktrees(dry_run=a.dry_run)
            print(f"{'would remove' if a.dry_run else 'removed'}: {len(r['removidas'])}; kept: {len(r['ficaram'])}" + (f"; bundle {r['bundle']}" if r["bundle"] else ""))
            print("\n".join([f"  removes {x['pasta']} ({x['branch']})" for x in r["removidas"]] + [f"  keeps {x['pasta']}: {x['motivo']}" for x in r["ficaram"]]))
        elif a.cmd == "audit-publication":
            reasons = audit_publication(a.revs)
            print("\n".join(f"audit-publication: {m}" for m in reasons), file=sys.stderr)
            return 1 if reasons else 0
        elif a.cmd == "doctor" and a.op == "backlog":
            r = doctor_backlog()
            print(json.dumps(r, ensure_ascii=False) if a.json else doctor_backlog_text(r))
            return 1 if r["problemas"] else 0
        elif a.cmd == "doctor" and a.op == "scratch":
            r = doctor_scratch()
            print(json.dumps(r, ensure_ascii=False) if a.json else doctor_scratch_text(r))
            return 1 if r["esquecidos"] else 0
        elif a.cmd == "phase":
            r = phase_check(a.texto, scratch_roots([os.getcwd()]))
            print(json.dumps(r, ensure_ascii=False) if a.json else "\n".join(f"phase {c['fase']} of {c['plano']}: " + (f"missing {', '.join(c['faltando'])}" if c["faltando"] else "complete")
                                                                         for c in r) or "no plan with tickets of the cited phase")
            return 1 if phase_refusal(r) else 0
        elif a.cmd == "doctor" and a.op == "hooks":
            return doctor_hooks(a.pin)
        elif a.cmd == "doctor" and a.op == "old":
            r = doctor_old(a.release, a.max_age_hours, a.ticket)
            print(json.dumps(r, ensure_ascii=False) if a.json else doctor_old_text(r, a.release))
        elif a.cmd == "doctor":
            r = doctor_tasks(a.dry_run)
            print(json.dumps(r, ensure_ascii=False) if a.json else doctor_tasks_text(r, a.dry_run))
        elif a.cmd == "ticket":
            if a.op == "new":
                r = ticket_new(a.title, a.spec_file, a.blocked_by, a.run, a.model, a.effort, a.dispatch_mode, a.waiting)
                print(json.dumps(r, ensure_ascii=False))
                _implicit("tarefa", r["task"], r["run"])
            elif a.op == "edit":
                print(json.dumps(ticket_edit(a.numero, modelo=a.model, effort=a.effort, despacho=a.dispatch_mode, espera=a.waiting), ensure_ascii=False))
            elif a.op == "close":
                r = ticket_close(a.numero, a.answer)
                print(json.dumps(r, ensure_ascii=False))
                if r["aviso"]:
                    print(f"warning: {r['aviso']}", file=sys.stderr)
            else:
                ts = [t for t in tickets() if a.include_all or t["status"] != STATUS_CLOSED]
                integration, evs = integration_queue(), read_events()
                without_push = _no_push()
                if a.json:
                    print(json.dumps([{**t, "espera_motivo": dispatch_wait(t, integration, evs, without_push) if t["status"] == STATUS_NEW and not t["blocked_by"] else None} for t in ts], ensure_ascii=False))
                else:
                    print("\n".join(_waiting_ticket_line(t, integration, evs, without_push) for t in ts) or "no open ticket")
        elif a.cmd == "ask":
            r = ask(a.id, a.question, a.option, a.recommended, a.detail, a.wait_min, not a.without_poll)
            print(json.dumps(r, ensure_ascii=False))
            for x in r["avisos"]:
                print(f"warning: {x}", file=sys.stderr)
            _implicit("decisao", a.id)
        elif a.cmd == "lavish-answer":
            r = lavish_answer(a.file_name)
            print(json.dumps(r, ensure_ascii=False))
            for x in r["avisos"]:
                print(f"warning: {x}", file=sys.stderr)
        elif a.cmd == "manager":
            if a.op == "bind":
                print(json.dumps(manager_bind(a.terminal, a.run, a.take_over), ensure_ascii=False))
            elif a.op == "unbind":
                print(json.dumps(manager_turn_off(a.run, a.take_over), ensure_ascii=False))
            elif a.op == "check":
                print(json.dumps(manager_check(), ensure_ascii=False))
            elif a.op == "spawn":
                print(json.dumps(manager_start(a.force), ensure_ascii=False))
            elif a.op == "interval":
                print(panel_interval_s(_read_json(_path(MANAGER))))
            elif a.op == "serve":
                action = serve_status if a.status else serve_stop if a.stop else serve_install if a.install else serve_uninstall if a.uninstall else None
                if action:
                    print(json.dumps(action(), ensure_ascii=False))
                else:
                    manager_serve(a.loops)
            elif a.op == "tui":
                return manager_tui(None if a.theme == "auto" else a.theme)
            else:
                owner_name = serve_owner()
                if owner_name and str(owner_name) != os.environ.get("ORQ_SERVE_PID"):
                    print(f"orq manager serve running (pid {owner_name}): this panel does not absorb, the log is at {_path(SERVE_LOG)}")
                    return SERVE_BUSY
                line = manager_absorb()
                print(line)
                if a.state:
                    manager_state_write(line)
        elif a.cmd == "resume" and a.paused:
            r = {"gerente": None, "workers": resume_paused(a.run, a.force)}
            print(json.dumps(r, ensure_ascii=False) if a.json else resume_text(r))
        elif a.cmd == "resume":
            r = resume(a.dry_run, a.run)
            print(json.dumps(r, ensure_ascii=False) if a.json else resume_text(r))
        elif a.cmd == "hibernate":
            r = hibernate(a.target, a.run, a.force)
            print(json.dumps(r, ensure_ascii=False) if a.json else hibernated_text(r))
        elif a.cmd == "wake":
            r = wake(a.target, a.text_value)
            print(json.dumps(r, ensure_ascii=False) if a.json else resume_text({"gerente": None, "workers": [r]}))
            if r["estado"] in ("falhou", "sem_worktree"):
                return 1
        elif a.cmd == "pause":
            r = pause_workers(a.tasks, a.up_to_priority, a.run, a.dry_run)
            print(json.dumps(r, ensure_ascii=False) if a.json else pause_text(r))
        elif a.cmd == "priority":
            print(json.dumps(set_priority(a.task, a.value), ensure_ascii=False))
        elif a.cmd == "groups" and (a.title or a.cwd or a.group_name):
            item_name, reason = group_of(groups(), a.title, a.cwd, a.group_name)
            t = item_name and _dict(_mates().get(item_name)).get("terminal")
            live = _alive_terminals() if t else None
            print(json.dumps({"grupo": item_name, "motivo": reason, "mate": t if t and (live is None or t in live) else None}, ensure_ascii=False))
        elif a.cmd == "groups":
            print(groups_text(groups(), _mates(), _alive_terminals() if _mates() else None, read_events(), datetime.now(timezone.utc)))
        elif a.cmd == "mate" and a.op == "open":
            print(json.dumps(mate_open(a.group_name), ensure_ascii=False))
        elif a.cmd == "mate" and a.op == "sleep":
            print(json.dumps(mate_sleep(a.group_name), ensure_ascii=False))
        elif a.cmd == "mate" and a.op == "request":
            r = mate_request(a.group_name, a.text_value, a.deadline, a.responde)
            print(json.dumps(r, ensure_ascii=False))
            if not a.responde:
                _implicit("mate", r["corr"])
        elif a.cmd == "mate" and a.op == "raise":
            print(json.dumps(mate_raise(a.type_name, a.text_value, a.corr, a.link, a.group_name), ensure_ascii=False))
        elif a.cmd == "mate":
            ps = [p for p in mate_pending(read_events(), _mates(), datetime.now(timezone.utc)) if not a.group_name or p["grupo"] == a.group_name]
            print("\n".join(f"{p['corr']} {p['grupo']} {p['estado']}: {_quote(p['texto'])}" for p in ps) or "no unanswered request")
        elif a.cmd == "review":
            r = review(a.task)
            print(json.dumps(r, ensure_ascii=False) if a.json else review_text(r))
        elif a.cmd == "machine" and a.op == "set":
            if not a.key_name or a.value is None:
                raise ValueError("orq machine set <key> <value>")
            print(json.dumps(machine_set(a.key_name, a.value), ensure_ascii=False))
        elif a.cmd == "machine":
            cfg, reading = machine_cfg(), machine_read()
            level, reason = machine_level(reading, cfg)
            occupancy = machine_occupancy()
            print(json.dumps({"config": cfg, "leitura": reading, "nivel": level, "motivo": reason, "vivos": occupancy["vivos"], "fila": dispatch_queue_items()}, ensure_ascii=False)
                  if a.json else "\n".join(machine_text(cfg, reading, occupancy)))
        elif a.cmd == "dispatch-queue" and a.op == "rm":
            if not dispatch_queue_rm(a.id):
                raise ValueError(f"{a.id} is not in the dispatch queue")
            print(f"{a.id} left the dispatch queue")
        elif a.cmd == "dispatch-queue" and a.op == "discard":
            if not any(e.get("tipo") == "despacho_fila" and e.get("op") == "desistiu" and e.get("id") == a.id for e in read_events()):
                raise ValueError(f"{a.id} has no recorded give-up")
            append_event({"tipo": "despacho_fila", "op": "descartado", "id": a.id, "motivo": a.reason})
            print(f"{a.id} discarded from the away Stop")
        elif a.cmd == "dispatch-queue":
            item_list = dispatch_queue_items()
            print(json.dumps(item_list, ensure_ascii=False) if a.json else "\n".join(
                f"{n} {i['id']} P{i.get('prioridade') or 2} {i['tipo']} {i.get('titulo')} ({i.get('modelo')}) since {i.get('ts')}: {i.get('motivo')}" for n, i in enumerate(item_list, 1)) or "dispatch queue empty")
        elif a.cmd == "usage":
            u = plan_usage(agent=a.agent)
            level, reason, _ = usage_level(u)
            print(json.dumps({"uso": u, "nivel": level, "motivo": reason}, ensure_ascii=False) if a.json else
                  f"{level}" + (f": {reason}" if reason else "") + (f" (week {u['semana']}%, 5 h {u['cinco_h']}%)" if u else " (no fresh HUD frame)"))
        elif a.cmd == "audit-answers":
            print(audit_answers(a.session), end="")
        elif a.cmd == "retro":
            print(retro_cmd(a))
        else:
            n = ingest()
            print("ingest running in another process" if n is None else
                  f"ingest: {n[0]} new entry(ies)" + (f", {n[1]} gate(s) resolved" if n[1] else ""))
            if a.refresh:
                ab = refresh_open()
                print("refresh running in another process" if ab is None else
                      f"open.json: backlog {len(ab['backlog'])}, running {ab['rodando']}, blocked {len(ab['bloqueado'])}, gates {len(ab['gates'])}")
    except Exception as e:  # noqa: BLE001 - an expected error (refusal) only goes to the screen; the ingest and the unexpected also go to the log
        print(f"orq: {e}" if isinstance(e, (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired)) else f"orq: {type(e).__name__}: {e}", file=sys.stderr)
        if a.cmd == "ingest" or not isinstance(e, (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired)):
            log(f"{a.cmd}{' --refresh' if getattr(a, 'refresh', False) else ''}: {type(e).__name__}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
