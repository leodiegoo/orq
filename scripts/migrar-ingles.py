#!/usr/bin/env python3
"""Migra o estado do ORQ_HOME para o inglês (fase 2 da migração; plano em ~/.claude/orquestrador-plan/orq-ingles-plano.md).

Recusa rodar com worker vivo no Orca (fora o terminal que chama), com o gerente rodando (`orq gerente serve` ou o painel do agent manager) ou
com alguma trava do orq presa. Copia antes os arquivos que vai mexer para ORQ_HOME/backup-pt-<data>/, reescreve o events.jsonl linha a linha
(a ilegível vai como está e entra no relatório), confere o número de linhas e troca por os.replace; depois traduz e renomeia cada .json.
O digest/ fica em pt (contrato digest-v1). Rodar de novo não muda nada: sem nada em pt, não há backup nem gravação.

    python3 scripts/migrar-ingles.py [--dry-run]
"""
import argparse
import contextlib
import fcntl
import glob
import json
import os
import re
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import orqlib as o  # noqa: E402

PASTAS = ("groups", "projects", "retro", "handoff")  # as pastas de estado com .json; digest/ fica em pt
TRAVAS_ANTIGAS = {"fila.lock": "merge-queue.lock", "fila-despacho.lock": "dispatch-queue.lock", "integrar-fila.lock": "integrate-queue.lock",
                  "turnos.lock": "turns.lock", "gerente.lock": "manager.lock", "despacho.lock": "dispatch.lock", "pend.lock": "pending.lock",
                  "passagem.lock": "handoff.lock", "revisao.lock": "review.lock"}
# chave que sobrou em pt depois do para_en: acento, sufixo ou palavra que o inglês não tem
PT = re.compile(r"[^\x00-\x7f]|(?:cao|coes|agem|ados?|idas?|idos?|ndo|eiro|ento)$|^(?:sem|com|por|para|de|em|na|no)_|_(?:em|de|do|da|na|no)$")


def _vivos():
    """O que impede a migração: workers vivos, gerente rodando, travas presas. Lista vazia é caminho livre."""
    out = []
    eu = os.environ.get("ORCA_TERMINAL_HANDLE")
    try:
        ws = [w for w in o._workers_todos() if w.get("dispatchStatus") == "dispatched" and w.get("agentTerminalHandle") != eu]
    except Exception as e:  # noqa: BLE001 - sem o Orca não há como saber se há worker vivo: recusa
        return [f"não consegui listar os workers no Orca ({type(e).__name__}: {e})"]
    out += [f"worker vivo: {w.get('dispatchId')} (task {w.get('taskId')}, terminal {w.get('agentTerminalHandle')})" for w in ws]
    if pid := o.serve_dono():
        out.append(f"gerente rodando: orq gerente serve (pid {pid})")
    vivo = o._path(o.PAINEL_VIVO)
    if os.path.exists(vivo) and time.time() - os.path.getmtime(vivo) < o.PAINEL_LIMITE_MIN_S:
        out.append(f"gerente rodando: o painel do agent manager tocou {os.path.basename(vivo)} há {time.time() - os.path.getmtime(vivo):.0f} s")
    return out


@contextlib.contextmanager
def _travas():
    """Toma todas as travas do orq (nomes novos e antigos) sem esperar e as segura até o fim: append de hook espera a troca do events.jsonl."""
    abertas, presas = [], []
    try:
        for nome in sorted({os.path.basename(p) for p in glob.glob(os.path.join(o.HOME, "*.lock"))} | {"cursor.lock"}):
            f = open(os.path.join(o.HOME, nome), "a")
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                presas.append(f"trava presa: {nome}")
                f.close()
                continue
            abertas.append(f)
        yield presas
    finally:
        for f in abertas:
            f.close()


def _ler(caminho):
    with open(caminho, encoding="utf-8") as f:
        return json.load(f)


def _gravar_json(caminho, dado):
    o._write_json(caminho, dado, indent=2)  # o _write_json passa pelo para_en: o que já está em inglês fica igual


def _sobras(obj, achadas):
    """Junta em `achadas` as chaves que parecem pt depois do para_en."""
    if isinstance(obj, list):
        for x in obj:
            _sobras(x, achadas)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and PT.search(k) and k not in o.CHAVES_PT:
                achadas[k] = achadas.get(k, 0) + 1
            _sobras(v, achadas)


def plano():
    """O que a migração faria: {eventos: (linhas, mudam, ilegíveis), arquivos: [(origem, destino)], travas: [...], sobras: {chave: n}}."""
    sobras, arquivos = {}, []
    ev = os.path.join(o.HOME, "events.jsonl")
    linhas = mudam = 0
    ilegiveis = []
    if os.path.exists(ev):
        with open(ev, encoding="utf-8") as f:
            for n, linha in enumerate(f, 1):
                linhas += 1
                try:
                    e = json.loads(linha)
                except ValueError:
                    ilegiveis.append(n)
                    continue
                en = o.para_en(e)
                _sobras(en, sobras)
                mudam += en != e
    for novo, antigo in o.ARQ_ANTIGO.items():
        a, b = os.path.join(o.HOME, antigo), os.path.join(o.HOME, novo)
        if os.path.exists(a):
            arquivos.append((a, b))
            if a.endswith(".json"):
                with contextlib.suppress(OSError, ValueError):
                    _sobras(o.para_en(_ler(a)), sobras)
    nomes = {os.path.join(o.HOME, x) for x in o.ARQ_ANTIGO.values()}
    for p in sorted(glob.glob(os.path.join(o.HOME, "*.json")) + [q for d in PASTAS for q in glob.glob(os.path.join(o.HOME, d, "*.json"))]):
        if p in nomes or not o._do_orq(p):
            continue
        try:
            d = _ler(p)
        except (OSError, ValueError):
            continue
        en = o.para_en(d)
        _sobras(en, sobras)
        if en != d:
            arquivos.append((p, p))
    travas = [os.path.join(o.HOME, t) for t in TRAVAS_ANTIGAS if os.path.exists(os.path.join(o.HOME, t))]
    return {"eventos": (linhas, mudam, ilegiveis), "arquivos": arquivos, "travas": travas, "sobras": sobras}


def _backup(p):
    pasta = os.path.join(o.HOME, f"backup-pt-{time.strftime('%Y-%m-%dT%H-%M-%S')}")
    rel = lambda x: os.path.relpath(x, o.HOME)  # noqa: E731
    copiar = ([os.path.join(o.HOME, "events.jsonl")] if p["eventos"][1] else []) + [a for a, _ in p["arquivos"]]
    for a in copiar:
        destino = os.path.join(pasta, rel(a))
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        (shutil.copytree if os.path.isdir(a) else shutil.copy2)(a, destino)
    return pasta


def _eventos():
    """Reescreve o events.jsonl com para_en num tmp ao lado; confere as linhas e troca. A linha ilegível vai como está."""
    ev = os.path.join(o.HOME, "events.jsonl")
    tmp = f"{ev}.migrar-{os.getpid()}.tmp"
    entrada = saida = 0
    with open(ev, encoding="utf-8") as f, open(tmp, "w", encoding="utf-8") as g:
        for linha in f:
            entrada += 1
            try:
                e = json.loads(linha)
            except ValueError:
                g.write(linha if linha.endswith("\n") else linha + "\n")
            else:
                g.write(json.dumps(o.para_en(e), ensure_ascii=False) + "\n")
            saida += 1
    with open(tmp, encoding="utf-8") as g:
        conferidas = sum(1 for _ in g)
    if not entrada == saida == conferidas:
        os.unlink(tmp)
        raise RuntimeError(f"events.jsonl: {entrada} linhas lidas, {conferidas} gravadas; nada foi trocado")
    os.replace(tmp, ev)
    return entrada


def _arquivo(a, b):
    """Traduz e grava `a` em `b` (o nome novo), apaga `a`. Pasta (perguntar/ -> ask/) e arquivo que não é JSON (gerente-vivo) só trocam de nome."""
    if os.path.isdir(a) or not a.endswith(".json"):
        if os.path.exists(b):
            return f"{os.path.basename(a)}: {os.path.basename(b)} já existe, ficou o novo (o antigo está no backup)"
        os.replace(a, b)
        return None
    try:
        d = _ler(a)
    except (OSError, ValueError) as e:
        return f"{os.path.basename(a)}: ilegível ({e}), ficou como está"
    if a != b and os.path.exists(b):
        os.unlink(a)
        return f"{os.path.basename(a)}: {os.path.basename(b)} já existe, ficou o novo (o antigo está no backup)"
    _gravar_json(b, d)
    if a != b:
        os.unlink(a)
    return None


def relatorio(p, feito=None):
    linhas, mudam, ilegiveis = p["eventos"]
    rel = lambda x: os.path.relpath(x, o.HOME)  # noqa: E731
    out = [f"ORQ_HOME: {o.HOME}", f"events.jsonl: {linhas} linhas, {mudam} em pt" + (f", {len(ilegiveis)} ilegíveis (linhas {', '.join(map(str, ilegiveis[:10]))}) vão como estão" if ilegiveis else "")]
    out += [f"  {rel(a)} -> {rel(b)}" if a != b else f"  {rel(a)}: traduzido" for a, b in p["arquivos"]] or ["  nenhum arquivo em pt"]
    out += [f"  trava antiga apagada: {rel(t)}" for t in p["travas"]]
    out.append("chaves pt sem tradução: " + (", ".join(f"{k} ({n})" for k, n in sorted(p["sobras"].items())) or "nenhuma"))
    if feito:
        out += feito
    return "\n".join(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="só mostra o que faria")
    a = ap.parse_args(argv)
    if bloqueios := _vivos():
        print("migração recusada:\n" + "\n".join(f"- {b}" for b in bloqueios), file=sys.stderr)
        return 2
    with _travas() as presas:
        if presas:
            print("migração recusada:\n" + "\n".join(f"- {b}" for b in presas), file=sys.stderr)
            return 2
        p = plano()
        if a.dry_run:
            print(relatorio(p) + "\n(dry-run: nada foi gravado)")
            return 0
        if not p["eventos"][1] and not p["arquivos"] and not p["travas"]:
            print(relatorio(p) + "\nnada a migrar")
            return 0
        feito = [f"backup: {_backup(p)}"]
        if p["eventos"][1]:
            feito.append(f"events.jsonl reescrito: {_eventos()} linhas")
        for origem, destino in p["arquivos"]:
            if aviso := _arquivo(origem, destino):
                feito.append(f"aviso: {aviso}")
        for t in p["travas"]:
            with contextlib.suppress(OSError):
                os.unlink(t)
    print(relatorio(p, feito))
    return 0


if __name__ == "__main__":
    sys.exit(main())
