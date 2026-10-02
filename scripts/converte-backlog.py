#!/usr/bin/env python3
"""Converte os tickets (ORQ_ISSUES) e o pendencias.json (ORQ_PENDENCIAS) num backlog.md do tasks-axi (ticket 101, M2). Só lê as fontes.

    converte-backlog.py [--saida ORQ_BACKLOG] [--issues D] [--pendencias F] [--eventos F] [--forcar | --completa [--outros B]]

Escreve só em arquivo novo (recusa um que exista, a não ser com --forcar), então rodar de novo na cópia é seguro. Um ticket vira `tNN`
(kind `ticket`, `repo` do prefixo do título antes de `:`), com `spec:` para o arquivo, `orca: <task> <run>` e `modelo:`/`effort:`/`issue:`/`despacho:`/`espera:`
quando o cabeçalho os tem; uma pendência vira o item `repo: pend` que `orq pend add` cria. As datas vêm do events.jsonl (`ticket novo|fechar`),
com o mtime do arquivo na falta. O `Blocked by` lê só os números do começo do campo ("none (… 30/09)" não bloqueia ninguém).
Depois de escrever, roda o `tasks-axi render` (confere que a gramática foi aceita e que nada de fundo mudou) e compara as contagens
(Done, arestas de bloqueio, prontos) com as das fontes; qualquer diferença sai com código 1.

`--completa` (ticket 102) não reescreve nada: acrescenta, pela CLI, os tickets de `issues/` que o backlog ainda não tem (os criados depois da migração, antes de `ticket novo` escrever
no backlog), imprime a contagem antes e depois e sai com 1 se o `depois` não fechar. Os backlogs de `grupos/*/backlog.md` ao lado da saída contam como já migrados.
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

ESTADO = {"resolved": "done", "claimed": "in_flight", "ready-for-agent": "queued"}


def _campo(cab, nome):
    m = re.search(rf"^{nome}:[ \t]*(.*)$", cab, re.M)
    return m.group(1).strip() if m else None


def bloqueios_de(campo):
    """Os números que abrem o campo `Blocked by` ("125, 126 e 128"), sem pegar data nem número solto de um comentário depois."""
    m = re.match(r"\s*(\d+(?:\s*(?:,|;|e|and)\s*\d+)*)", campo or "")
    return re.findall(r"\d+", m.group(1)) if m else []


def le_ticket(caminho):
    nome = os.path.basename(caminho)
    with open(caminho, encoding="utf-8") as f:
        cab = f.read().split("\n## ", 1)[0]
    titulo = re.match(r"#[ \t]*(?:\d+[ \t]*:[ \t]*)?(.+)", cab)
    if not titulo:
        raise ValueError(f"{nome} não começa com o título (# NN: Título)")
    issue = re.search(r"^issue:[ \t]*#?(\d+)", cab, re.M | re.I)
    return {"num": nome.split("-")[0], "nome": nome, "titulo": titulo.group(1).strip(), "status": _campo(cab, "Status") or "?",
            "bloqueios": bloqueios_de(_campo(cab, "Blocked by")), "run": _campo(cab, "Run"), "task": _campo(cab, "Task"),
            "modelo": _campo(cab, "Modelo"), "effort": _campo(cab, "Effort"), "issue": issue.group(1) if issue else None,
            "despacho": _campo(cab, "Despacho"), "espera": _campo(cab, "Espera")}


def datas_dos_eventos(caminho):
    """({num: data do `ticket novo`}, {num: data do `ticket fechar`}), a primeira de cada; arquivo ausente é vazio."""
    novo, fechou = {}, {}
    try:
        with open(caminho, encoding="utf-8") as f:
            for linha in f:
                try:
                    e = json.loads(linha)
                except ValueError:
                    continue
                if (e.get("tipo") or e.get("type")) == "ticket" and e.get("op") in ("novo", "fechar") and e.get("ticket") and e.get("ts"):
                    (novo if e["op"] == "novo" else fechou).setdefault(str(e["ticket"]).zfill(2), e["ts"][:10])
    except FileNotFoundError:
        pass
    return novo, fechou


def converte(issues, pendencias, eventos):
    """(texto do backlog, resumo esperado {done, bloqueios, prontos, tickets, pendencias}) das fontes."""
    nomes = sorted((n for n in os.listdir(issues) if re.match(r"^\d+-.+\.md$", n)), key=lambda n: int(n.split("-")[0]))
    ts = [le_ticket(os.path.join(issues, n)) for n in nomes]
    nums = {t["num"].zfill(2) for t in ts}
    novo, fechou = datas_dos_eventos(eventos)
    itens, arestas = [], 0
    for t in ts:
        n = t["num"].zfill(2)
        estado = ESTADO.get(t["status"], "queued")
        mtime = time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(os.path.join(issues, t["nome"]))))
        bl = [f"t{b.zfill(2)}" for b in dict.fromkeys(t["bloqueios"]) if b.zfill(2) in nums and b.zfill(2) != n]
        arestas += len(bl)
        meta = {"spec": f"issues/{t['nome']}", "orca": f"{t['task']} {t['run']}" if t["task"] and t["run"] else None,
                "modelo": t["modelo"], "effort": t["effort"], "issue": t["issue"], "despacho": t["despacho"], "espera": t["espera"]}
        itens.append({"id": f"t{n}", "titulo": t["titulo"], "estado": estado, "kind": "ticket", "repo": backlog.repo_do_titulo(t["titulo"]), "bloqueios": bl,
                      "since": novo.get(n) or mtime, "closed": fechou.get(n) or mtime, "corpo": backlog.corpo_com_meta(meta, None, backlog.META_TICKET)})
    try:
        with open(pendencias, encoding="utf-8") as f:
            vivas = json.load(f)["itens"]
    except FileNotFoundError:
        vivas = []
    itens += [backlog.pend_a_item(p) for p in vivas]
    por_estado = {t["num"].zfill(2): ESTADO.get(t["status"], "queued") for t in ts}
    abertos = {n for n, e in por_estado.items() if e != "done"}
    prontos = [t for t in ts if por_estado[t["num"].zfill(2)] == "queued" and not any(b.zfill(2) in abertos for b in t["bloqueios"] if b.zfill(2) in nums)]
    return backlog.emite(itens), {"tickets": len(ts), "pendencias": len(vivas), "done": sum(e == "done" for e in por_estado.values()), "bloqueios": arestas,
                                  "prontos": len(prontos)}, itens


def completa(saida, issues, pendencias, eventos, outros=()):
    """Acrescenta ao backlog que existe os tickets de `issues` que nem ele nem os `outros` backlogs (os dos grupos) têm, pela CLI: travada e atômica, e a data de criação
    vira a de hoje (são os criados depois da migração). Devolve (tickets antes, tickets depois, ids acrescentados, avisos); `depois` só confere se tudo entrou."""
    _, _, itens = converte(issues, pendencias, eventos)
    tickets = [i for i in itens if i["kind"] == "ticket"]
    meu = {i["id"] for i in backlog.ler(saida)}
    nums = {int(i[1:]) for i in meu | {i["id"] for o in outros for i in backlog.ler(o)} if re.fullmatch(r"t\d+", i)}
    antes = sum(i["kind"] == "ticket" for i in backlog.ler(saida))
    faltam, avisos, feitos = [i for i in tickets if int(i["id"][1:]) not in nums], [], []
    for i in faltam:
        if (p := backlog.problema_titulo(i["titulo"])):
            avisos.append(f"{i['id']}: {p}")
            continue
        backlog.cli(saida, "add", i["id"], i["titulo"], "--kind", "ticket", *(["--repo", i["repo"]] if i["repo"] else []), *(["--body", i["corpo"]] if i["corpo"] else []))
        feitos.append(i)
    aqui = meu | {i["id"] for i in feitos}
    for i in feitos:
        for b in i["bloqueios"]:
            if b in aqui:
                backlog.cli(saida, "block", i["id"], "--by", b)
            else:
                avisos.append(f"{i['id']}: o bloqueador {b} está em outro backlog; a aresta não foi criada")
        if i["estado"] == "in_flight":
            backlog.cli(saida, "start", i["id"])
        elif i["estado"] == "done":
            backlog.cli(saida, "done", i["id"], "--no-prune")
    depois = sum(i["kind"] == "ticket" for i in backlog.ler(saida))
    return antes, depois, [i["id"] for i in feitos], avisos


def ids_do_ready(saida_cli):
    """Os ids da tabela `ready[N]{...}:` do tasks-axi."""
    return re.findall(r"^  ([^,\s]+),", saida_cli.split("ready[", 1)[-1], re.M) if "ready[" in saida_cli else []


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--saida", default=os.environ.get("ORQ_BACKLOG"))
    ap.add_argument("--issues", default=os.environ.get("ORQ_ISSUES") or os.path.expanduser("~/.claude/orquestrador-plan/issues"))
    ap.add_argument("--pendencias", default=os.environ.get("ORQ_PENDENCIAS") or os.path.expanduser("~/.claude/dashboard/data/pendencias.json"))
    ap.add_argument("--eventos", default=os.path.join(os.environ.get("ORQ_HOME") or os.path.expanduser("~/.claude/orq"), "events.jsonl"))
    ap.add_argument("--forcar", action="store_true", help="sobrescreve a saída que já existe")
    ap.add_argument("--completa", action="store_true", help="acrescenta ao backlog que existe os tickets de issues/ que ele ainda não tem (pela CLI, sem reescrever o arquivo)")
    ap.add_argument("--outros", action="append", default=[], help="outro backlog que já guarda tickets (os de grupos/*/backlog.md ao lado da saída entram sozinhos); --completa não os repete")
    a = ap.parse_args(argv)
    if not a.saida:
        ap.error("diga onde escrever: --saida ou ORQ_BACKLOG")
    if a.completa:
        if not os.path.exists(a.saida):
            print(f"{a.saida} não existe: --completa acrescenta a um backlog que já existe", file=sys.stderr)
            return 1
        outros = [*glob.glob(os.path.join(os.path.dirname(os.path.abspath(a.saida)), "grupos", "*", "backlog.md")), *a.outros]
        antes, depois, feitos, avisos = completa(a.saida, a.issues, a.pendencias, a.eventos, outros)
        print(f"tickets no backlog: antes {antes}, depois {depois} (+{len(feitos)}: {', '.join(feitos) or 'nenhum'})")
        for x in avisos:
            print(f"AVISO {x}", file=sys.stderr)
        if depois != antes + len(feitos):
            print(f"DIFERENÇA: esperava {antes + len(feitos)} tickets e o backlog tem {depois}", file=sys.stderr)
        return 1 if avisos or depois != antes + len(feitos) else 0
    if os.path.exists(a.saida) and not a.forcar:
        print(f"{a.saida} já existe: a conversão só escreve em arquivo novo (--forcar sobrescreve)", file=sys.stderr)
        return 1
    texto, esperado, _ = converte(a.issues, a.pendencias, a.eventos)
    os.makedirs(os.path.dirname(os.path.abspath(a.saida)), exist_ok=True)
    with open(a.saida, "w", encoding="utf-8") as f:
        f.write(texto)
    toml = os.path.join(os.path.dirname(os.path.abspath(a.saida)), ".tasks.toml")
    if not os.path.exists(toml):
        with open(toml, "w", encoding="utf-8") as f:
            f.write(backlog.TOML)
    backlog.cli(a.saida, "render")
    with open(a.saida, encoding="utf-8") as f:
        depois = f.read()
    mudou = [l for l in depois.splitlines() if l.strip()] != [l for l in texto.splitlines() if l.strip()]
    itens = backlog.ler(a.saida)
    por_id = {i["id"]: i for i in itens}
    tickets = [i for i in itens if i["kind"] == "ticket"]
    obtido = {"tickets": len(tickets), "pendencias": sum(i["repo"] == "pend" for i in itens),
              "done": sum(i["estado"] == "done" for i in tickets), "bloqueios": sum(len(i["bloqueios"]) for i in tickets),
              "prontos": len([i for i in backlog.prontos(itens) if i["kind"] == "ticket"])}
    cli_prontos = [i for i in ids_do_ready(backlog.cli(a.saida, "ready")) if por_id.get(i, {}).get("kind") == "ticket"]
    erros = [f"{k}: fonte {esperado[k]}, backlog {obtido[k]}" for k in esperado if esperado[k] != obtido[k]]
    if len(cli_prontos) != esperado["prontos"]:
        erros.append(f"prontos: fonte {esperado['prontos']}, tasks-axi ready {len(cli_prontos)}")
    if mudou:
        erros.append("o `tasks-axi render` mudou o arquivo além das linhas em branco: a emissão saiu da gramática")
    print(f"{obtido['tickets']} tickets ({obtido['done']} Done), {obtido['pendencias']} pendências, {obtido['bloqueios']} bloqueios, {obtido['prontos']} prontos -> {a.saida}")
    for e in erros:
        print(f"DIFERENÇA {e}", file=sys.stderr)
    return 1 if erros else 0


if __name__ == "__main__":
    sys.exit(main())
