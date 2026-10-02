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

VERSAO = "0.2.6"
CLI_TIMEOUT_S = 20
HOLD_KINDS = ("captain", "external", "load", "parked", "future")
META_PEND = ("frente", "link", "comando", "espera", "gate", "gate_run", "task")  # linhas `chave: valor` no topo do corpo da pendência
META_TICKET = ("spec", "orca", "modelo", "effort", "issue")  # idem no ticket; `orca: task_x run_y` é a ponte com o Orca

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


class BacklogErro(RuntimeError):
    """A CLI do tasks-axi recusou, está na versão errada ou não existe."""


def _estado(cabecalho):
    m = re.match(r"^##\s+(.*?)\s*$", cabecalho)
    t = m.group(1).lower() if m else ""
    return "in_flight" if t == "in flight" else "queued" if t == "queued" else "done" if t.startswith("done") else None


def _tags(resto):
    achados, deps, titulo = {}, [], resto
    while True:
        for nome, rx in _TAGS:
            m = rx.search(titulo)
            if m:
                if nome == "dep":
                    deps.insert(0, {"tipo": m.group(1), "id": m.group(2)})
                else:
                    achados.setdefault(nome, m.group(1).strip() if nome in ("repo", "kind", "hold") else m.group(1))
                titulo = titulo[:m.start()]
                break
        else:
            break
    hold = {"motivo": achados["hold"], "kind": achados.get("hold_kind"), "until": achados.get("hold_until")} if "hold" in achados else None
    return titulo.strip(), achados, deps, hold


def interpreta(src):
    """Itens do texto de um backlog.md: [{id, titulo, estado, kind, repo, prioridade, since, closed, bloqueios, hold, corpo}], na ordem do arquivo.

    `bloqueios` são os ids de `blocked-by` (os outros tipos de aresta não bloqueiam); `hold` é {motivo, kind, until} ou None; `corpo` é o
    texto das linhas indentadas sem os 2 espaços. Item fora de uma seção conhecida não é item.
    """
    itens, estado, atual = [], None, None
    for linha in src.split("\n"):
        linha = linha.rstrip("\r")
        if linha.startswith("##") and re.match(r"^##\s+", linha):
            estado, atual = _estado(linha), None
            continue
        m = next((m for rx in _BULLET.get(estado, ()) if (m := rx.match(linha))), None)
        if m:
            titulo, t, deps, hold = _tags(m.group(2))
            atual = {"id": m.group(1), "titulo": titulo, "estado": estado, "kind": t.get("kind"), "repo": t.get("repo"),
                     "prioridade": int(t["prioridade"]) if "prioridade" in t else None, "since": t.get("since"), "closed": t.get("closed"),
                     "bloqueios": [d["id"] for d in deps if d["tipo"] == "blocked-by"], "hold": hold, "_corpo": []}
            itens.append(atual)
        elif atual is not None and (linha.strip() == "" or linha.startswith("  ")):
            atual["_corpo"].append(linha[2:] if linha.strip() else "")
        else:
            atual = None
    for i in itens:
        c = i.pop("_corpo")
        while c and c[-1] == "":
            c.pop()
        i["corpo"] = "\n".join(c)
    return itens


def ler(caminho):
    """Itens do backlog.md; arquivo ausente é vazio. Erro de leitura levanta OSError (nunca vira lista vazia calada)."""
    try:
        with open(caminho, encoding="utf-8") as f:
            return interpreta(f.read())
    except FileNotFoundError:
        return []


def meta_corpo(corpo, chaves):
    """(meta, resto): as linhas `chave: valor` do topo do corpo cujas chaves estão em `chaves`, e o texto que sobra.

    A primeira linha que não é meta termina a meta; uma linha em branco logo depois dela (ou no começo, sem meta) é o separador e não entra no resto.
    ponytail: um texto livre que comece com uma linha `link: x` e nenhuma meta antes vira meta; `corpo_com_meta` põe uma linha em branco na frente
    nesse caso, mas um corpo escrito à mão com essa forma será lido como meta.
    """
    linhas, meta = corpo.split("\n") if corpo else [], {}
    while linhas and (m := _META.match(linhas[0])) and m.group(1) in chaves:
        meta[m.group(1)] = m.group(2).strip()
        linhas.pop(0)
    if linhas and linhas[0] == "":
        linhas.pop(0)
    return meta, "\n".join(linhas)


def corpo_com_meta(meta, texto, chaves):
    """O inverso de meta_corpo: as linhas de `meta` (só as de `chaves` com valor, numa linha cada), uma linha em branco e o texto."""
    linhas = [f"{k}: {' '.join(str(meta[k]).split())}" for k in chaves if meta.get(k)]
    texto = texto or ""
    primeira = texto.split("\n", 1)[0]
    if texto and (linhas or ((m := _META.match(primeira)) and m.group(1) in chaves)):
        linhas.append("")
    return "\n".join([*linhas, *([texto] if texto else [])])


def hold_ativo(item, hoje=None):
    """O hold segura o item que não está Done: sem `until` vale sempre, com `until` vale enquanto a data for futura (no dia dela já solta)."""
    h = item.get("hold")
    return bool(h) and item["estado"] != "done" and (not h.get("until") or h["until"] > (hoje or date.today()).isoformat())


def bloqueado(item, por_id):
    """O item não está Done e algum `blocked-by` aponta para um item que existe e não está Done; bloqueador que não existe conta como resolvido."""
    return item["estado"] != "done" and any(por_id.get(b) and por_id[b]["estado"] != "done" for b in item["bloqueios"])


def prontos(itens, hoje=None, repo=None):
    """O `tasks-axi ready`: Queued, sem bloqueio aberto e sem hold ativo; `repo` filtra como o --repo."""
    por_id = {i["id"]: i for i in itens}
    return [i for i in itens if i["estado"] == "queued" and not bloqueado(i, por_id) and not hold_ativo(i, hoje) and (repo is None or i["repo"] == repo)]


def emite(itens, cabecalho="# Backlog"):
    """O texto de um backlog.md com `itens` na gramática canônica (a mesma que `tasks-axi render` devolve): uma linha por item e o corpo indentado.

    O item é {id, titulo, estado, kind?, repo?, prioridade?, since?, closed?, bloqueios?, hold?, corpo?}; a data de criação e a de fechamento vêm
    do chamador (a CLI só sabe carimbar "hoje").
    """
    secoes = {"in_flight": [], "queued": [], "done": []}
    for i in itens:
        partes = [i["titulo"].strip(), *(f"blocked-by: {b}" for b in i.get("bloqueios") or [])]
        if i.get("repo"):
            partes.append(f"(repo: {i['repo']})")
        if i.get("kind"):
            partes.append(f"(kind: {i['kind']})")
        if i.get("prioridade") is not None:
            partes.append(f"(priority: {i['prioridade']})")
        if i["estado"] != "done" and i.get("since"):
            partes.append(f"(since {i['since']})")
        if i["estado"] == "done" and i.get("closed"):
            partes.append(f"(done {i['closed']})")
        h = i.get("hold")
        if h:
            partes += [f"(hold: {h['motivo']})", *([f"(hold-kind: {h['kind']})"] if h.get("kind") else []), *([f"(hold-until: {h['until']})"] if h.get("until") else [])]
        marca = "x" if i["estado"] == "done" else " "
        corpo = [f"  {ln}" if ln else "" for ln in (i.get("corpo") or "").split("\n")] if i.get("corpo") else []
        secoes[i["estado"]].append("\n".join([f"- [{marca}] {i['id']} - {' '.join(partes)}", *corpo]))
    nomes = (("in_flight", "In flight"), ("queued", "Queued"), ("done", "Done"))
    return f"{cabecalho}\n\n" + "\n\n".join(f"## {n}\n" + "\n".join(secoes[k]) for k, n in nomes) + "\n"


def pend_a_item(p):
    """O item de backlog de uma pendência do pendencias.json: kind = tipo, repo `pend`, meta no topo do corpo e o detalhe depois. O hold segura a
    pendência fora do `ready` (kind `external` se espera alguém, `captain` se é do usuário) e leva o `ate` como until; o motivo não aceita parênteses."""
    motivo = f"esperando {p['espera']}" if p.get("espera") else "decisão do usuário pendente" if p.get("tipo") == "decisao" else "pendência do usuário"
    return {"id": p["id"], "titulo": " ".join(p["titulo"].split()), "estado": "queued", "kind": p["tipo"], "repo": "pend", "since": p.get("desde"),
            "hold": {"motivo": " ".join(re.sub(r"[()]", " ", motivo).split()), "kind": "external" if p.get("espera") else "captain", "until": p.get("ate")},
            "corpo": corpo_com_meta(p, p.get("detalhe"), META_PEND)}


def pend_de_item(i):
    """A pendência (formato do pendencias.json) de um item `pend` do backlog; chaves na ordem em que `pend add` as grava."""
    meta, detalhe = meta_corpo(i["corpo"], META_PEND)
    campos = (("detalhe", detalhe), ("frente", meta.get("frente")), ("desde", i["since"]), ("link", meta.get("link")), ("comando", meta.get("comando")),
              ("espera", meta.get("espera")), ("ate", (i["hold"] or {}).get("until")), ("gate", meta.get("gate")), ("gate_run", meta.get("gate_run")), ("task", meta.get("task")))
    return {"id": i["id"], "tipo": i["kind"], "titulo": i["titulo"], **{k: v for k, v in campos if v}}


ESTADO_TICKET = {"queued": "ready-for-agent", "in_flight": "claimed", "done": "resolved"}  # o `Status:` do cabeçalho do ticket


def ticket_de_item(i, por_id, raiz):
    """O ticket (formato de `tickets()` do orq) de um item `tNN` kind `ticket`, ou None se o item é outra coisa. `blocked_by` traz só os bloqueadores ainda
    abertos; `run` e `task` vêm da linha `orca: <task> <run>`; `arquivo` é o `spec:` relativo a `raiz`."""
    m = re.fullmatch(r"t(\d+)", i["id"])
    if not m or i["kind"] != "ticket":
        return None
    meta, _ = meta_corpo(i["corpo"], META_TICKET)
    task, _, run = (meta.get("orca") or "").partition(" ")
    issue = meta.get("issue", "").lstrip("#")
    return {"num": m.group(1).zfill(2), "arquivo": os.path.join(raiz, meta["spec"]) if meta.get("spec") else None, "titulo": i["titulo"], "status": ESTADO_TICKET[i["estado"]],
            "blocked_by": [b[1:].zfill(2) for b in i["bloqueios"] if re.fullmatch(r"t\d+", b) and por_id.get(b, {}).get("estado") not in (None, "done")],
            "run": run.strip() or None, "task": task or None, "modelo": meta.get("modelo"), "effort": meta.get("effort"), "issue": int(issue) if issue.isdigit() else None,
            "fechado_em": i["closed"]}


def binario():
    return os.environ.get("ORQ_TASKS_AXI") or shutil.which("tasks-axi") or os.path.expanduser("~/.local/share/mise/npm-global/bin/tasks-axi")


_VERSOES = {}


def confere_versao(exe):
    """Recusa, com a instrução de instalar, um tasks-axi que não seja o `VERSAO`; a conferência vale uma vez por processo."""
    if exe not in _VERSOES:
        try:
            r = subprocess.run([exe, "--version"], capture_output=True, text=True, timeout=CLI_TIMEOUT_S)
        except (OSError, subprocess.TimeoutExpired) as e:
            raise BacklogErro(f"não consegui rodar {exe} ({e}): npm i -g tasks-axi@{VERSAO}")
        _VERSOES[exe] = r.stdout.strip()
    if _VERSOES[exe] != VERSAO:
        raise BacklogErro(f"tasks-axi {_VERSOES[exe] or '?'} em {exe}: o orq escreve o backlog só na {VERSAO} (npm i -g tasks-axi@{VERSAO})")


def cli(caminho, *args):
    """Roda `tasks-axi <args>` sobre o backlog `caminho` (TASKS_AXI_FILE, cwd na pasta dele, onde mora o .tasks.toml) e devolve o stdout.

    Recusa `--file` e backlog que seja symlink (o rename da CLI o trocaria por arquivo comum). Código de saída diferente de zero levanta BacklogErro
    com o que a CLI disse.
    """
    if any(a == "--file" or a.startswith("--file=") for a in args):
        raise ValueError("o backlog é escolhido por ORQ_BACKLOG, nunca por --file")
    if os.path.islink(caminho):
        raise BacklogErro(f"{caminho} é um symlink: a escrita do tasks-axi o trocaria por um arquivo comum")
    exe = binario()
    confere_versao(exe)
    pasta = os.path.dirname(os.path.abspath(caminho))
    os.makedirs(pasta, exist_ok=True)
    try:
        r = subprocess.run([exe, *args], env={**os.environ, "TASKS_AXI_FILE": os.path.abspath(caminho)}, cwd=pasta, capture_output=True, text=True, timeout=CLI_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        raise BacklogErro(f"tasks-axi {args[0]} passou de {CLI_TIMEOUT_S} s")
    if r.returncode != 0:
        raise BacklogErro(f"tasks-axi {' '.join(args[:2])}: {(r.stdout + r.stderr).strip()[:300]}")
    return r.stdout
