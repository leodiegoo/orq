"""orq backlog in the tasks-axi format (ticket 101). stdlib only.

Reading is done in Python, because a tasks-axi process takes 74 to 120 ms and the hooks have a 100 ms ceiling. Writing only through the CLI, at version `VERSION`
(the grammar is still 0.x), always with `TASKS_AXI_FILE` in the environment and never `--file`. The grammar read here is that of
`tasks-axi/dist/src/backends/markdown-grammar.js`: the state comes from the section header, and the line's trailing tags become fields.
The contract test (test_orq.py, `contrato`) runs the real tasks-axi and compares both sides.
"""
import os
import re
import shutil
import subprocess
from datetime import date

VERSION = "0.2.6"
TOML = '[markdown]\ndone_keep = 100000\n'  # the `.tasks.toml` of a backlog folder: archiving would take out of the file a ticket that orq still consults (numbering, Blocked by)
CLI_TIMEOUT_S = 20
HOLD_KINDS = ("captain", "external", "load", "parked", "future")
META_PENDING = ("frente", "link", "comando", "espera", "gate", "gate_run", "task")  # `key_name: value` lines at the top of the pending item's body
META_TICKET = ("spec", "orca", "modelo", "effort", "issue", "despacho", "espera", "scratch", "projeto", "wave", "role")  # same for the ticket (`wave: N` and `role: milestone|join` mark the tickets of a wave, ticket 342); `orca: task_x run_y` is the bridge to Orca

_ID = r"[A-Za-z0-9][A-Za-z0-9._-]*"
ID_RE = re.compile(rf"^{_ID}$")
_BULLET = {"done": [re.compile(rf"^- \[x\] ({_ID}) - (.*)$")],
           "queued": [re.compile(rf"^- \[ \] ({_ID}) - (.*)$")],
           "in_flight": [re.compile(rf"^- \*\*({_ID})\*\* - (.*)$"), re.compile(rf"^- \[ \] ({_ID}) - (.*)$")]}
_DATA = r"\d{4}-\d{2}-\d{2}"
_DEP = r"(?:blocked-by|parent|discovered-from)"
# the order is that of tasks-axi's extractTags: each pass strips one tag from the end of the line, until none matches
_TAGS = (("dep", re.compile(rf"\s*({_DEP}):\s*({_ID})(?:\s+-\s+((?:(?!\s+{_DEP}:\s).)+?))?\s*$")),
         ("repo", re.compile(r"\s*\((?:[^()]*\+\s*)?repo:\s*([^)]+)\)\s*$")),
         ("kind", re.compile(r"\s*\(kind:\s*([^)]+)\)\s*$")),
         ("prioridade", re.compile(r"\s*\(priority:\s*([0-4])\)\s*$")),
         ("since", re.compile(rf"\s*\(since\s+({_DATA})\)\s*$")),
         ("closed", re.compile(rf"\s*\((?:merged|reported|done|closed)\s+({_DATA})\)\s*$")),
         ("hold_until", re.compile(rf"\s*\(hold-until:\s*({_DATA})\)\s*$")),
         ("hold_kind", re.compile(rf"\s*\(hold-kind:\s*({'|'.join(HOLD_KINDS)})\)\s*$")),
         ("hold", re.compile(r"\s*\(hold:\s*([^()]+)\)\s*$")))
_META = re.compile(r"^([a-z_]+):[ \t]?(.*)$")


class BacklogError(RuntimeError):
    """The tasks-axi CLI refused, is at the wrong version, or does not exist."""


def _state(header):
    m = re.match(r"^##\s+(.*?)\s*$", header)
    t = m.group(1).lower() if m else ""
    return "in_flight" if t == "in flight" else "queued" if t == "queued" else "done" if t.startswith("done") else None


def _tags(rest):
    findings, deps, title = {}, [], rest
    while True:
        for item_name, rx in _TAGS:
            m = rx.search(title)
            if m:
                if item_name == "dep":
                    deps.insert(0, {"tipo": m.group(1), "id": m.group(2)})
                else:
                    findings.setdefault(item_name, m.group(1).strip() if item_name in ("repo", "kind", "hold") else m.group(1))
                title = title[:m.start()]
                break
        else:
            break
    hold = {"motivo": findings["hold"], "kind": findings.get("hold_kind"), "until": findings.get("hold_until")} if "hold" in findings else None
    return title.strip(), findings, deps, hold


def parse(src):
    """Items of a backlog.md text: [{id, titulo, estado, kind, repo, prioridade, since, closed, bloqueios, hold, corpo}], in file order.

    `blockers` are the `blocked-by` ids (the other edge types do not block); `hold` is {motivo, kind, until} or None; `body_text` is the
    text of the indented lines without the 2 spaces. An item outside a known section is not an item.
    """
    item_list, state, current = [], None, None
    for line in src.split("\n"):
        line = line.rstrip("\r")
        if line.startswith("##") and re.match(r"^##\s+", line):
            state, current = _state(line), None
            continue
        m = next((m for rx in _BULLET.get(state, ()) if (m := rx.match(line))), None)
        if m:
            title, t, deps, hold = _tags(m.group(2))
            current = {"id": m.group(1), "titulo": title, "estado": state, "kind": t.get("kind"), "repo": t.get("repo"),
                     "prioridade": int(t["prioridade"]) if "prioridade" in t else None, "since": t.get("since"), "closed": t.get("closed"),
                     "bloqueios": [d["id"] for d in deps if d["tipo"] == "blocked-by"], "hold": hold, "_corpo": []}
            item_list.append(current)
        elif current is not None and (line.strip() == "" or line.startswith("  ")):
            current["_corpo"].append(line[2:] if line.strip() else "")
        else:
            current = None
    for i in item_list:
        c = i.pop("_corpo")
        while c and c[-1] == "":
            c.pop()
        i["corpo"] = "\n".join(c)
    return item_list


def read_value(path):
    """Items of the backlog.md; a missing file is empty. A read error raises OSError (it never becomes a silent empty list)."""
    try:
        with open(path, encoding="utf-8") as f:
            return parse(f.read())
    except FileNotFoundError:
        return []


def body_meta(body_text, keys):
    """(meta, rest): the `key_name: value` lines at the top of the body whose keys are in `keys`, and the remaining text.

    The first line that is not meta ends the meta; a blank line right after it (or at the start, with no meta) is the separator and is not part of the rest.
    ponytail: a free text that starts with a `link: x` line and has no meta before it becomes meta; `body_with_meta` puts a blank line in front
    in that case, but a hand-written body with that shape will be read as meta.
    """
    line_list, meta = body_text.split("\n") if body_text else [], {}
    while line_list and (m := _META.match(line_list[0])) and m.group(1) in keys:
        meta[m.group(1)] = m.group(2).strip()
        line_list.pop(0)
    if line_list and line_list[0] == "":
        line_list.pop(0)
    return meta, "\n".join(line_list)


def body_with_meta(meta, text_value, keys):
    """The inverse of body_meta: the `meta` lines (only those of `keys` that have a value, one per line), a blank line and the text."""
    line_list = [f"{k}: {' '.join(str(meta[k]).split())}" for k in keys if meta.get(k)]
    text_value = text_value or ""
    first = text_value.split("\n", 1)[0]
    if text_value and (line_list or ((m := _META.match(first)) and m.group(1) in keys)):
        line_list.append("")
    return "\n".join([*line_list, *([text_value] if text_value else [])])


def active_hold(item, today=None):
    """The hold holds back an item that is not Done: without `until` it always applies, with `until` it applies while the date is in the future (on the day itself it already lets go)."""
    h = item.get("hold")
    return bool(h) and item["estado"] != "done" and (not h.get("until") or h["until"] > (today or date.today()).isoformat())


def is_blocked(item, by_id):
    """The item is not Done and some `blocked-by` points to an item that exists and is not Done; a blocker that does not exist counts as resolved."""
    return item["estado"] != "done" and any(by_id.get(b) and by_id[b]["estado"] != "done" for b in item["bloqueios"])


def ready(item_list, today=None, repo=None):
    """The `tasks-axi ready`: Queued, with no open blocker and no active hold; `repo` filters like --repo."""
    by_id = {i["id"]: i for i in item_list}
    return [i for i in item_list if i["estado"] == "queued" and not is_blocked(i, by_id) and not active_hold(i, today) and (repo is None or i["repo"] == repo)]


def emit(item_list, header="# Backlog"):
    """The text of a backlog.md with `item_list` in the canonical grammar (the same one `tasks-axi render` returns): one line per item and the indented body.

    The item is {id, titulo, estado, kind?, repo?, prioridade?, since?, closed?, bloqueios?, hold?, corpo?}; the creation and closing dates come
    from the caller (the CLI only knows how to stamp "today").
    """
    sections = {"in_flight": [], "queued": [], "done": []}
    for i in item_list:
        parts = [i["titulo"].strip(), *(f"blocked-by: {b}" for b in i.get("bloqueios") or [])]
        if i.get("repo"):
            parts.append(f"(repo: {i['repo']})")
        if i.get("kind"):
            parts.append(f"(kind: {i['kind']})")
        if i.get("prioridade") is not None:
            parts.append(f"(priority: {i['prioridade']})")
        if i["estado"] != "done" and i.get("since"):
            parts.append(f"(since {i['since']})")
        if i["estado"] == "done" and i.get("closed"):
            parts.append(f"(done {i['closed']})")
        h = i.get("hold")
        if h:
            parts += [f"(hold: {h['motivo']})", *([f"(hold-kind: {h['kind']})"] if h.get("kind") else []), *([f"(hold-until: {h['until']})"] if h.get("until") else [])]
        mark = "x" if i["estado"] == "done" else " "
        body_text = [f"  {ln}" if ln else "" for ln in (i.get("corpo") or "").split("\n")] if i.get("corpo") else []
        sections[i["estado"]].append("\n".join([f"- [{mark}] {i['id']} - {' '.join(parts)}", *body_text]))
    names = (("in_flight", "In flight"), ("queued", "Queued"), ("done", "Done"))
    return f"{header}\n\n" + "\n\n".join(f"## {n}\n" + "\n".join(sections[k]) for k, n in names) + "\n"


def pending_to_item(p):
    """The backlog item for a pending item from pendencias.json: kind = type, repo `pending`, meta at the top of the body and the detail after it. The hold keeps the
    pending item out of `ready` (kind `external` if it waits on someone, `captain` if it belongs to the user) and carries `until_at` as until; the reason does not accept parentheses."""
    reason = f"waiting on {p['espera']}" if p.get("espera") else "pending user decision" if p.get("tipo") == "decisao" else "pending user item"
    return {"id": p["id"], "titulo": " ".join(p["titulo"].split()), "estado": "queued", "kind": p["tipo"], "repo": "pend", "since": p.get("desde"),
            "hold": {"motivo": " ".join(re.sub(r"[()]", " ", reason).split()), "kind": "external" if p.get("espera") else "captain", "until": p.get("ate")},
            "corpo": body_with_meta(p, p.get("detalhe"), META_PENDING)}


def pending_from_item(i):
    """The pending item (pendencias.json format) from a `pending` item of the backlog; keys in the order in which `pending add` writes them."""
    meta, detail = body_meta(i["corpo"], META_PENDING)
    fields = (("detalhe", detail), ("frente", meta.get("frente")), ("desde", i["since"]), ("link", meta.get("link")), ("comando", meta.get("comando")),
              ("espera", meta.get("espera")), ("ate", (i["hold"] or {}).get("until")), ("gate", meta.get("gate")), ("gate_run", meta.get("gate_run")), ("task", meta.get("task")))
    return {"id": i["id"], "tipo": i["kind"], "titulo": i["titulo"], **{k: v for k, v in fields if v}}


TICKET_STATE = {"queued": "ready-for-agent", "in_flight": "claimed", "done": "resolved"}  # the ticket header's `Status:`


def ticket_of_item(i, by_id, root):
    """The ticket (format of orq's `tickets()`) from a `tNN` item of kind `ticket`, or None if the item is something else. `blocked_by` brings only the blockers that are still
    open (in a Done ticket, all of them: it is the history that `orq ticket listing` shows); `run` and `task` come from the `orca: <task> <run>` line; `file_name` is the `spec:` relative to `root`."""
    m = re.fullmatch(r"t(\d+)", i["id"])
    if not m or i["kind"] != "ticket":
        return None
    meta, _ = body_meta(i["corpo"], META_TICKET)
    task, _, run = (meta.get("orca") or "").partition(" ")
    issue = meta.get("issue", "").lstrip("#")
    return {"num": m.group(1).zfill(2), "arquivo": os.path.join(root, meta["spec"]) if meta.get("spec") else None, "titulo": i["titulo"], "status": TICKET_STATE[i["estado"]],
            "blocked_by": [b[1:].zfill(2) for b in i["bloqueios"] if re.fullmatch(r"t\d+", b) and (i["estado"] == "done" or by_id.get(b, {}).get("estado") not in (None, "done"))],
            "run": run.strip() or None, "task": task or None, "modelo": meta.get("modelo"), "effort": meta.get("effort"), "issue": int(issue) if issue.isdigit() else None,
            "despacho": meta.get("despacho"), "espera": meta.get("espera"), "projeto": meta.get("projeto"),  # the `Despacho:` and `Espera:` headers (ticket 142)
            "scratch": meta.get("scratch"),  # the `.scratch/<feature>/issues/NN-*.md` the ticket was born from (ticket 201)
            "wave": int(meta["wave"]) if meta.get("wave", "").isdigit() else None, "role": meta.get("role"),  # ticket 342: the wave the ticket belongs to; `role` is milestone or join for the wave's own tickets
            "fechado_em": i["closed"]}


def repo_from_title(title):
    """A ticket's `repo`: the title prefix before `:` ("orq: x" -> "orq"), or None."""
    m = re.match(r"^([a-z][a-z0-9-]{1,15}):\s", title)
    return m.group(1) if m else None


def problem_title(title):
    """Why the CLI would not store this title as is (the grammar would read the end as a tag, or the CLI would read the start as an option), or None."""
    if _tags(title)[0] != title:
        return f"the title ends in a backlog tag (blocked-by:, (repo: …), (kind: …), (since …), (hold: …)): rewrite {title!r}"
    if title.startswith("-"):
        return f"the title cannot start with '-' (the CLI would read it as an option): rewrite {title!r}"
    return None


def binary():
    return os.environ.get("ORQ_TASKS_AXI") or shutil.which("tasks-axi") or os.path.expanduser("~/.local/share/mise/npm-global/bin/tasks-axi")


_VERSIONS = {}


def check_version(exe):
    """Refuses, with the install instruction, a tasks-axi that is not `VERSION`; the check holds once per process."""
    if exe not in _VERSIONS:
        try:
            r = subprocess.run([exe, "--version"], capture_output=True, text=True, timeout=CLI_TIMEOUT_S)
        except (OSError, subprocess.TimeoutExpired) as e:
            raise BacklogError(f"could not run {exe} ({e}): npm i -g tasks-axi@{VERSION}")
        _VERSIONS[exe] = r.stdout.strip()
    if _VERSIONS[exe] != VERSION:
        raise BacklogError(f"tasks-axi {_VERSIONS[exe] or '?'} at {exe}: orq writes the backlog only with {VERSION} (npm i -g tasks-axi@{VERSION})")


def cli(path, *args):
    """Runs `tasks-axi <args>` on the backlog `path` (TASKS_AXI_FILE, cwd in its folder, where .tasks.toml lives) and returns the stdout.

    Refuses `--file` and a backlog that is a symlink (the CLI's rename would replace it with a regular file). A non-zero exit code raises BacklogErro
    with what the CLI said.
    """
    if any(a == "--file" or a.startswith("--file=") for a in args):
        raise ValueError("the backlog is chosen by ORQ_BACKLOG, never by --file")
    if os.path.islink(path):
        raise BacklogError(f"{path} is a symlink: the tasks-axi write would replace it with a regular file")
    exe = binary()
    check_version(exe)
    folder = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)
    try:
        r = subprocess.run([exe, *args], env={**os.environ, "TASKS_AXI_FILE": os.path.abspath(path)}, cwd=folder, capture_output=True, text=True, timeout=CLI_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        raise BacklogError(f"tasks-axi {args[0]} exceeded {CLI_TIMEOUT_S} s")
    if r.returncode != 0:
        raise BacklogError(f"tasks-axi {' '.join(args[:2])}: {(r.stdout + r.stderr).strip()[:300]}")
    return r.stdout
