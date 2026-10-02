"""Backlog do orq no formato do tasks-axi (ticket 101). Só stdlib.

Leitura em Python, porque um processo do tasks-axi leva de 74 a 120 ms e os hooks têm teto de 100 ms. Escrita só pela CLI, na versão `VERSAO`
(a gramática ainda é 0.x), sempre com `TASKS_AXI_FILE` no ambiente e nunca `--file`. A gramática lida aqui é a de
`tasks-axi/dist/src/backends/markdown-grammar.js`: o estado vem do cabeçalho da seção, e as tags de cauda da linha viram campo.
O teste de contrato (test_orq.py, `contrato`) roda o tasks-axi de verdade e compara os dois lados.
"""
import os
import re
import shutil
import subprocess
from datetime import date

VERSION = "0.2.6"
TOML = '[markdown]\ndone_keep = 100000\n'  # o `.tasks.toml` de uma pasta de backlog: o arquivamento tiraria do arquivo ticket que o orq ainda consulta (numeração, Blocked by)
CLI_TIMEOUT_S = 20
HOLD_KINDS = ("captain", "external", "load", "parked", "future")
META_PENDING = ("frente", "link", "comando", "espera", "gate", "gate_run", "task")  # linhas `chave: valor` no topo do corpo da pendência
META_TICKET = ("spec", "orca", "modelo", "effort", "issue", "despacho", "espera")  # idem no ticket; `orca: task_x run_y` é a ponte com o Orca

_ID = r"[A-Za-z0-9][A-Za-z0-9._-]*"
ID_RE = re.compile(rf"^{_ID}$")
_BULLET = {"done": [re.compile(rf"^- \[x\] ({_ID}) - (.*)$")],
           "queued": [re.compile(rf"^- \[ \] ({_ID}) - (.*)$")],
           "in_flight": [re.compile(rf"^- \*\*({_ID})\*\* - (.*)$"), re.compile(rf"^- \[ \] ({_ID}) - (.*)$")]}
_DATA = r"\d{4}-\d{2}-\d{2}"
_DEP = r"(?:blocked-by|parent|discovered-from)"
# a ordem é a do extractTags do tasks-axi: cada volta tira uma tag do fim da linha, até nenhuma casar
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
    """A CLI do tasks-axi recusou, está na versão errada ou não existe."""


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
    """Itens do texto de um backlog.md: [{id, titulo, estado, kind, repo, prioridade, since, closed, bloqueios, hold, corpo}], na ordem do arquivo.

    `bloqueios` são os ids de `blocked-by` (os outros tipos de aresta não bloqueiam); `hold` é {motivo, kind, until} ou None; `corpo` é o
    texto das linhas indentadas sem os 2 espaços. Item fora de uma seção conhecida não é item.
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
    """Itens do backlog.md; arquivo ausente é vazio. Erro de leitura levanta OSError (nunca vira lista vazia calada)."""
    try:
        with open(path, encoding="utf-8") as f:
            return parse(f.read())
    except FileNotFoundError:
        return []


def body_meta(body_text, keys):
    """(meta, resto): as linhas `chave: valor` do topo do corpo cujas chaves estão em `chaves`, e o texto que sobra.

    A primeira linha que não é meta termina a meta; uma linha em branco logo depois dela (ou no começo, sem meta) é o separador e não entra no resto.
    ponytail: um texto livre que comece com uma linha `link: x` e nenhuma meta antes vira meta; `corpo_com_meta` põe uma linha em branco na frente
    nesse caso, mas um corpo escrito à mão com essa forma será lido como meta.
    """
    line_list, meta = body_text.split("\n") if body_text else [], {}
    while line_list and (m := _META.match(line_list[0])) and m.group(1) in keys:
        meta[m.group(1)] = m.group(2).strip()
        line_list.pop(0)
    if line_list and line_list[0] == "":
        line_list.pop(0)
    return meta, "\n".join(line_list)


def body_with_meta(meta, text_value, keys):
    """O inverso de meta_corpo: as linhas de `meta` (só as de `chaves` com valor, numa linha cada), uma linha em branco e o texto."""
    line_list = [f"{k}: {' '.join(str(meta[k]).split())}" for k in keys if meta.get(k)]
    text_value = text_value or ""
    first = text_value.split("\n", 1)[0]
    if text_value and (line_list or ((m := _META.match(first)) and m.group(1) in keys)):
        line_list.append("")
    return "\n".join([*line_list, *([text_value] if text_value else [])])


def active_hold(item, today=None):
    """O hold segura o item que não está Done: sem `until` vale sempre, com `until` vale enquanto a data for futura (no dia dela já solta)."""
    h = item.get("hold")
    return bool(h) and item["estado"] != "done" and (not h.get("until") or h["until"] > (today or date.today()).isoformat())


def is_blocked(item, by_id):
    """O item não está Done e algum `blocked-by` aponta para um item que existe e não está Done; bloqueador que não existe conta como resolvido."""
    return item["estado"] != "done" and any(by_id.get(b) and by_id[b]["estado"] != "done" for b in item["bloqueios"])


def ready(item_list, today=None, repo=None):
    """O `tasks-axi ready`: Queued, sem bloqueio aberto e sem hold ativo; `repo` filtra como o --repo."""
    by_id = {i["id"]: i for i in item_list}
    return [i for i in item_list if i["estado"] == "queued" and not is_blocked(i, by_id) and not active_hold(i, today) and (repo is None or i["repo"] == repo)]


def emit(item_list, header="# Backlog"):
    """O texto de um backlog.md com `itens` na gramática canônica (a mesma que `tasks-axi render` devolve): uma linha por item e o corpo indentado.

    O item é {id, titulo, estado, kind?, repo?, prioridade?, since?, closed?, bloqueios?, hold?, corpo?}; a data de criação e a de fechamento vêm
    do chamador (a CLI só sabe carimbar "hoje").
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
    """O item de backlog de uma pendência do pendencias.json: kind = tipo, repo `pend`, meta no topo do corpo e o detalhe depois. O hold segura a
    pendência fora do `ready` (kind `external` se espera alguém, `captain` se é do usuário) e leva o `ate` como until; o motivo não aceita parênteses."""
    reason = f"waiting on {p['espera']}" if p.get("espera") else "pending user decision" if p.get("tipo") == "decisao" else "pending user item"
    return {"id": p["id"], "titulo": " ".join(p["titulo"].split()), "estado": "queued", "kind": p["tipo"], "repo": "pend", "since": p.get("desde"),
            "hold": {"motivo": " ".join(re.sub(r"[()]", " ", reason).split()), "kind": "external" if p.get("espera") else "captain", "until": p.get("ate")},
            "corpo": body_with_meta(p, p.get("detalhe"), META_PENDING)}


def pending_from_item(i):
    """A pendência (formato do pendencias.json) de um item `pend` do backlog; chaves na ordem em que `pend add` as grava."""
    meta, detail = body_meta(i["corpo"], META_PENDING)
    fields = (("detalhe", detail), ("frente", meta.get("frente")), ("desde", i["since"]), ("link", meta.get("link")), ("comando", meta.get("comando")),
              ("espera", meta.get("espera")), ("ate", (i["hold"] or {}).get("until")), ("gate", meta.get("gate")), ("gate_run", meta.get("gate_run")), ("task", meta.get("task")))
    return {"id": i["id"], "tipo": i["kind"], "titulo": i["titulo"], **{k: v for k, v in fields if v}}


TICKET_STATE = {"queued": "ready-for-agent", "in_flight": "claimed", "done": "resolved"}  # o `Status:` do cabeçalho do ticket


def ticket_of_item(i, by_id, root):
    """O ticket (formato de `tickets()` do orq) de um item `tNN` kind `ticket`, ou None se o item é outra coisa. `blocked_by` traz só os bloqueadores ainda
    abertos (num ticket Done, todos: é o histórico que `orq ticket lista` mostra); `run` e `task` vêm da linha `orca: <task> <run>`; `arquivo` é o `spec:` relativo a `raiz`."""
    m = re.fullmatch(r"t(\d+)", i["id"])
    if not m or i["kind"] != "ticket":
        return None
    meta, _ = body_meta(i["corpo"], META_TICKET)
    task, _, run = (meta.get("orca") or "").partition(" ")
    issue = meta.get("issue", "").lstrip("#")
    return {"num": m.group(1).zfill(2), "arquivo": os.path.join(root, meta["spec"]) if meta.get("spec") else None, "titulo": i["titulo"], "status": TICKET_STATE[i["estado"]],
            "blocked_by": [b[1:].zfill(2) for b in i["bloqueios"] if re.fullmatch(r"t\d+", b) and (i["estado"] == "done" or by_id.get(b, {}).get("estado") not in (None, "done"))],
            "run": run.strip() or None, "task": task or None, "modelo": meta.get("modelo"), "effort": meta.get("effort"), "issue": int(issue) if issue.isdigit() else None,
            "despacho": meta.get("despacho"), "espera": meta.get("espera"),  # os cabeçalhos `Despacho:` e `Espera:` (ticket 142)
            "fechado_em": i["closed"]}


def repo_from_title(title):
    """O `repo` de um ticket: o prefixo do título antes de `:` ("orq: x" -> "orq"), ou None."""
    m = re.match(r"^([a-z][a-z0-9-]{1,15}):\s", title)
    return m.group(1) if m else None


def problem_title(title):
    """Por que a CLI não guardaria este título como está (a gramática leria o fim como tag, ou a CLI leria o começo como opção), ou None."""
    if _tags(title)[0] != title:
        return f"the title ends in a backlog tag (blocked-by:, (repo: …), (kind: …), (since …), (hold: …)): rewrite {title!r}"
    if title.startswith("-"):
        return f"the title cannot start with '-' (the CLI would read it as an option): rewrite {title!r}"
    return None


def binary():
    return os.environ.get("ORQ_TASKS_AXI") or shutil.which("tasks-axi") or os.path.expanduser("~/.local/share/mise/npm-global/bin/tasks-axi")


_VERSIONS = {}


def check_version(exe):
    """Recusa, com a instrução de instalar, um tasks-axi que não seja o `VERSAO`; a conferência vale uma vez por processo."""
    if exe not in _VERSIONS:
        try:
            r = subprocess.run([exe, "--version"], capture_output=True, text=True, timeout=CLI_TIMEOUT_S)
        except (OSError, subprocess.TimeoutExpired) as e:
            raise BacklogError(f"could not run {exe} ({e}): npm i -g tasks-axi@{VERSION}")
        _VERSIONS[exe] = r.stdout.strip()
    if _VERSIONS[exe] != VERSION:
        raise BacklogError(f"tasks-axi {_VERSIONS[exe] or '?'} at {exe}: orq writes the backlog only with {VERSION} (npm i -g tasks-axi@{VERSION})")


def cli(path, *args):
    """Roda `tasks-axi <args>` sobre o backlog `caminho` (TASKS_AXI_FILE, cwd na pasta dele, onde mora o .tasks.toml) e devolve o stdout.

    Recusa `--file` e backlog que seja symlink (o rename da CLI o trocaria por arquivo comum). Código de saída diferente de zero levanta BacklogErro
    com o que a CLI disse.
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
