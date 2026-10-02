#!/usr/bin/env python3
"""Converts the tickets (ORQ_ISSUES) and pendencias.json (ORQ_PENDENCIAS) into a tasks-axi backlog.md (ticket 101, M2). Only reads the sources.

    converte-backlog.py [--saida ORQ_BACKLOG] [--issues D] [--pendencias F] [--eventos F] [--forcar | --completa [--outros B]]

Writes only to a new file (refuses one that exists, unless --forcar), so running it again on the copy is safe. A ticket becomes `tNN`
(kind `ticket`, `repo` from the title prefix before `:`), with `spec:` for the file, `orca: <task> <run>` and `model:`/`effort:`/`issue:`/`dispatch_mode:`/`waiting:`
when the header has them; a pending item becomes the `repo: pending` item that `orq pending add` creates. Dates come from events.jsonl (`ticket new|fechar`),
with the file's mtime as a fallback. `Blocked by` reads only the numbers at the start of the field ("none (… 30/09)" blocks nobody).
After writing, it runs `tasks-axi render` (checks that the grammar was accepted and that nothing underneath changed) and compares the counts
(Done, blocking edges, ready) with the sources'; any difference exits with code 1.

`--complete` (ticket 102) rewrites nothing: it appends, through the CLI, the tickets in `issues/` that the backlog doesn't have yet (the ones created after the migration, before `ticket new` wrote
to the backlog), prints the count before and after and exits 1 if `after` doesn't add up. The `groups/*/backlog.md` backlogs next to the output count as already migrated.
"""
import argparse
import glob
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
import backlog  # noqa: E402
import orqpaths  # noqa: E402

STATE = {"resolved": "done", "claimed": "in_flight", "ready-for-agent": "queued"}


def _campo(cab, item_name):
    m = re.search(rf"^{item_name}:[ \t]*(.*)$", cab, re.M)
    return m.group(1).strip() if m else None


def blockers_of(campo):
    """The numbers that open the `Blocked by` field ("125, 126 e 128"), without picking up a date or a stray number from a comment afterwards."""
    m = re.match(r"\s*(\d+(?:\s*(?:,|;|e|and)\s*\d+)*)", campo or "")
    return re.findall(r"\d+", m.group(1)) if m else []


def read_ticket(path):
    item_name = os.path.basename(path)
    with open(path, encoding="utf-8") as f:
        cab = f.read().split("\n## ", 1)[0]
    title = re.match(r"#[ \t]*(?:\d+[ \t]*:[ \t]*)?(.+)", cab)
    if not title:
        raise ValueError(f"{item_name} não começa com o título (# NN: Título)")
    issue = re.search(r"^issue:[ \t]*#?(\d+)", cab, re.M | re.I)
    return {"num": item_name.split("-")[0], "nome": item_name, "titulo": title.group(1).strip(), "status": _campo(cab, "Status") or "?",
            "bloqueios": blockers_of(_campo(cab, "Blocked by")), "run": _campo(cab, "Run"), "task": _campo(cab, "Task"),
            "modelo": _campo(cab, "Modelo"), "effort": _campo(cab, "Effort"), "issue": issue.group(1) if issue else None,
            "despacho": _campo(cab, "Despacho"), "espera": _campo(cab, "Espera")}


def event_dates(path):
    """({num: date of `ticket new`}, {num: date of `ticket fechar`}), the first of each; a missing file is empty."""
    new, did_close = {}, {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if (e.get("tipo") or e.get("type")) == "ticket" and e.get("op") in ("novo", "fechar") and e.get("ticket") and e.get("ts"):
                    (new if e["op"] == "novo" else did_close).setdefault(str(e["ticket"]).zfill(2), e["ts"][:10])
    except FileNotFoundError:
        pass
    return new, did_close


def convert(issues, pending_items, event_list):
    """(backlog text, expected summary {done, bloqueios, prontos, tickets, pendencias}) from the sources."""
    names = sorted((n for n in os.listdir(issues) if re.match(r"^\d+-.+\.md$", n)), key=lambda n: int(n.split("-")[0]))
    ts = [read_ticket(os.path.join(issues, n)) for n in names]
    numbers = {t["num"].zfill(2) for t in ts}
    new, did_close = event_dates(event_list)
    item_list, edges = [], 0
    for t in ts:
        n = t["num"].zfill(2)
        state = STATE.get(t["status"], "queued")
        mtime = time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(os.path.join(issues, t["nome"]))))
        bl = [f"t{b.zfill(2)}" for b in dict.fromkeys(t["bloqueios"]) if b.zfill(2) in numbers and b.zfill(2) != n]
        edges += len(bl)
        meta = {"spec": f"issues/{t['nome']}", "orca": f"{t['task']} {t['run']}" if t["task"] and t["run"] else None,
                "modelo": t["modelo"], "effort": t["effort"], "issue": t["issue"], "despacho": t["despacho"], "espera": t["espera"]}
        item_list.append({"id": f"t{n}", "titulo": t["titulo"], "estado": state, "kind": "ticket", "repo": backlog.repo_from_title(t["titulo"]), "bloqueios": bl,
                      "since": new.get(n) or mtime, "closed": did_close.get(n) or mtime, "corpo": backlog.body_with_meta(meta, None, backlog.META_TICKET)})
    try:
        with open(pending_items, encoding="utf-8") as f:
            live_output = json.load(f)["itens"]
    except FileNotFoundError:
        live_output = []
    item_list += [backlog.pending_to_item(p) for p in live_output]
    by_state = {t["num"].zfill(2): STATE.get(t["status"], "queued") for t in ts}
    open_items = {n for n, e in by_state.items() if e != "done"}
    ready = [t for t in ts if by_state[t["num"].zfill(2)] == "queued" and not any(b.zfill(2) in open_items for b in t["bloqueios"] if b.zfill(2) in numbers)]
    return backlog.emit(item_list), {"tickets": len(ts), "pendencias": len(live_output), "done": sum(e == "done" for e in by_state.values()), "bloqueios": edges,
                                  "prontos": len(ready)}, item_list


def complete(output, issues, pending_items, event_list, others=()):
    """Appends to the existing backlog the tickets from `issues` that neither it nor the `others` backlogs (the groups' ones) have, through the CLI: locked and atomic, and the creation date
    becomes today's (they are the ones created after the migration). Returns (tickets before, tickets after, appended ids, notices); `after` only checks that everything went in."""
    _, _, item_list = convert(issues, pending_items, event_list)
    tickets = [i for i in item_list if i["kind"] == "ticket"]
    mine = {i["id"] for i in backlog.read_value(output)}
    numbers = {int(i[1:]) for i in mine | {i["id"] for o in others for i in backlog.read_value(o)} if re.fullmatch(r"t\d+", i)}
    before = sum(i["kind"] == "ticket" for i in backlog.read_value(output))
    still_missing, notices, done_items = [i for i in tickets if int(i["id"][1:]) not in numbers], [], []
    for i in still_missing:
        if (p := backlog.problem_title(i["titulo"])):
            notices.append(f"{i['id']}: {p}")
            continue
        backlog.cli(output, "add", i["id"], i["titulo"], "--kind", "ticket", *(["--repo", i["repo"]] if i["repo"] else []), *(["--body", i["corpo"]] if i["corpo"] else []))
        done_items.append(i)
    local_ids = mine | {i["id"] for i in done_items}
    for i in done_items:
        for b in i["bloqueios"]:
            if b in local_ids:
                backlog.cli(output, "block", i["id"], "--by", b)
            else:
                notices.append(f"{i['id']}: o bloqueador {b} está em outro backlog; a aresta não foi criada")
        if i["estado"] == "in_flight":
            backlog.cli(output, "start", i["id"])
        elif i["estado"] == "done":
            backlog.cli(output, "done", i["id"], "--no-prune")
    after = sum(i["kind"] == "ticket" for i in backlog.read_value(output))
    return before, after, [i["id"] for i in done_items], notices


def ready_ids(cli_output):
    """The ids in tasks-axi's `ready[N]{...}:` table."""
    return re.findall(r"^  ([^,\s]+),", cli_output.split("ready[", 1)[-1], re.M) if "ready[" in cli_output else []


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--saida", default=os.environ.get("ORQ_BACKLOG"), dest="output")
    ap.add_argument("--issues", default=os.environ.get("ORQ_ISSUES") or os.path.join(orqpaths.PLAN, "issues"))
    ap.add_argument("--pendencias", default=os.environ.get("ORQ_PENDENCIAS") or os.path.expanduser("~/.claude/dashboard/data/pendencias.json"), dest="pending_items")
    ap.add_argument("--eventos", default=os.path.join(orqpaths.HOME, "events.jsonl"), dest="event_list")
    ap.add_argument("--forcar", action="store_true", help="sobrescreve a saída que já existe", dest="force")
    ap.add_argument("--completa", action="store_true", help="acrescenta ao backlog que existe os tickets de issues/ que ele ainda não tem (pela CLI, sem reescrever o arquivo)", dest="complete")
    ap.add_argument("--outros", action="append", default=[], help="outro backlog que já guarda tickets (os de grupos/*/backlog.md ao lado da saída entram sozinhos); --completa não os repete", dest="others")
    a = ap.parse_args(argv)
    if not a.output:
        ap.error("diga onde escrever: --saida ou ORQ_BACKLOG")
    if a.complete:
        if not os.path.exists(a.output):
            print(f"{a.output} não existe: --completa acrescenta a um backlog que já existe", file=sys.stderr)
            return 1
        others = [*glob.glob(os.path.join(os.path.dirname(os.path.abspath(a.output)), "grupos", "*", "backlog.md")), *a.others]
        before, after, done_items, notices = complete(a.output, a.issues, a.pending_items, a.event_list, others)
        print(f"tickets no backlog: antes {before}, depois {after} (+{len(done_items)}: {', '.join(done_items) or 'nenhum'})")
        for x in notices:
            print(f"AVISO {x}", file=sys.stderr)
        if after != before + len(done_items):
            print(f"DIFERENÇA: esperava {before + len(done_items)} tickets e o backlog tem {after}", file=sys.stderr)
        return 1 if notices or after != before + len(done_items) else 0
    if os.path.exists(a.output) and not a.force:
        print(f"{a.output} já existe: a conversão só escreve em arquivo novo (--forcar sobrescreve)", file=sys.stderr)
        return 1
    text_value, expected, _ = convert(a.issues, a.pending_items, a.event_list)
    os.makedirs(os.path.dirname(os.path.abspath(a.output)), exist_ok=True)
    with open(a.output, "w", encoding="utf-8") as f:
        f.write(text_value)
    toml = os.path.join(os.path.dirname(os.path.abspath(a.output)), ".tasks.toml")
    if not os.path.exists(toml):
        with open(toml, "w", encoding="utf-8") as f:
            f.write(backlog.TOML)
    backlog.cli(a.output, "render")
    with open(a.output, encoding="utf-8") as f:
        after = f.read()
    changed = [l for l in after.splitlines() if l.strip()] != [l for l in text_value.splitlines() if l.strip()]
    item_list = backlog.read_value(a.output)
    by_id = {i["id"]: i for i in item_list}
    tickets = [i for i in item_list if i["kind"] == "ticket"]
    obtained = {"tickets": len(tickets), "pendencias": sum(i["repo"] == "pend" for i in item_list),
              "done": sum(i["estado"] == "done" for i in tickets), "bloqueios": sum(len(i["bloqueios"]) for i in tickets),
              "prontos": len([i for i in backlog.ready(item_list) if i["kind"] == "ticket"])}
    ready_cli = [i for i in ready_ids(backlog.cli(a.output, "ready")) if by_id.get(i, {}).get("kind") == "ticket"]
    error_list = [f"{k}: fonte {expected[k]}, backlog {obtained[k]}" for k in expected if expected[k] != obtained[k]]
    if len(ready_cli) != expected["prontos"]:
        error_list.append(f"prontos: fonte {expected['prontos']}, tasks-axi ready {len(ready_cli)}")
    if changed:
        error_list.append("o `tasks-axi render` mudou o arquivo além das linhas em branco: a emissão saiu da gramática")
    print(f"{obtained['tickets']} tickets ({obtained['done']} Done), {obtained['pendencias']} pendências, {obtained['bloqueios']} bloqueios, {obtained['prontos']} prontos -> {a.output}")
    for e in error_list:
        print(f"DIFERENÇA {e}", file=sys.stderr)
    return 1 if error_list else 0


if __name__ == "__main__":
    sys.exit(main())
