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

FOLDERS = ("groups", "projects", "retro", "handoff")  # as pastas de estado com .json; digest/ fica em pt
OLD_LOCKS = {"fila.lock": "merge-queue.lock", "fila-despacho.lock": "dispatch-queue.lock", "integrar-fila.lock": "integrate-queue.lock",
                  "turnos.lock": "turns.lock", "gerente.lock": "manager.lock", "despacho.lock": "dispatch.lock", "pend.lock": "pending.lock",
                  "passagem.lock": "handoff.lock", "revisao.lock": "review.lock"}
# chave que sobrou em pt depois do para_en: acento, sufixo ou palavra que o inglês não tem
PT = re.compile(r"[^\x00-\x7f]|(?:cao|coes|agem|ados?|idas?|idos?|ndo|eiro|ento)$|^(?:sem|com|por|para|de|em|na|no)_|_(?:em|de|do|da|na|no)$")


def _live_blockers():
    """O que impede a migração: workers vivos, gerente rodando, travas presas. Lista vazia é caminho livre."""
    out = []
    eu = os.environ.get("ORCA_TERMINAL_HANDLE")
    try:
        ws = [w for w in o._all_workers() if w.get("dispatchStatus") == "dispatched" and w.get("agentTerminalHandle") != eu]
    except Exception as e:  # noqa: BLE001 - sem o Orca não há como saber se há worker vivo: recusa
        return [f"não consegui listar os workers no Orca ({type(e).__name__}: {e})"]
    out += [f"worker vivo: {w.get('dispatchId')} (task {w.get('taskId')}, terminal {w.get('agentTerminalHandle')})" for w in ws]
    if pid := o.serve_owner():
        out.append(f"gerente rodando: orq gerente serve (pid {pid})")
    alive = o._path(o.PANEL_ALIVE)
    if os.path.exists(alive) and time.time() - os.path.getmtime(alive) < o.PANEL_LIMIT_MIN_S:
        out.append(f"gerente rodando: o painel do agent manager tocou {os.path.basename(alive)} há {time.time() - os.path.getmtime(alive):.0f} s")
    return out


@contextlib.contextmanager
def _locks():
    """Toma todas as travas do orq (nomes novos e antigos) sem esperar e as segura até o fim: append de hook espera a troca do events.jsonl."""
    open_entries, stuck_locks = [], []
    try:
        for item_name in sorted({os.path.basename(p) for p in glob.glob(os.path.join(o.HOME, "*.lock"))} | {"cursor.lock"}):
            f = open(os.path.join(o.HOME, item_name), "a")
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                stuck_locks.append(f"trava presa: {item_name}")
                f.close()
                continue
            open_entries.append(f)
        yield stuck_locks
    finally:
        for f in open_entries:
            f.close()


def _read_value(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _write_json_file(path, dado):
    o._write_json(path, dado, indent=2)  # o _write_json passa pelo para_en: o que já está em inglês fica igual


def _leftovers(obj, found_labels):
    """Junta em `achadas` as chaves que parecem pt depois do para_en."""
    if isinstance(obj, list):
        for x in obj:
            _leftovers(x, found_labels)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and PT.search(k) and k not in o.KEYS_PT:
                found_labels[k] = found_labels.get(k, 0) + 1
            _leftovers(v, found_labels)


def plan():
    """O que a migração faria: {eventos: (linhas, mudam, ilegíveis), arquivos: [(origem, destino)], travas: [...], sobras: {chave: n}}."""
    leftovers, files_set = {}, []
    ev = os.path.join(o.HOME, "events.jsonl")
    line_list = to_change = 0
    unreadable = []
    if os.path.exists(ev):
        with open(ev, encoding="utf-8") as f:
            for n, line in enumerate(f, 1):
                line_list += 1
                try:
                    e = json.loads(line)
                except ValueError:
                    unreadable.append(n)
                    continue
                en = o.to_en(e)
                _leftovers(en, leftovers)
                to_change += en != e
    for new, old_name in o.OLD_FILE.items():
        a, b = os.path.join(o.HOME, old_name), os.path.join(o.HOME, new)
        if os.path.exists(a):
            files_set.append((a, b))
            if a.endswith(".json"):
                with contextlib.suppress(OSError, ValueError):
                    _leftovers(o.to_en(_read_value(a)), leftovers)
    names = {os.path.join(o.HOME, x) for x in o.OLD_FILE.values()}
    for p in sorted(glob.glob(os.path.join(o.HOME, "*.json")) + [q for d in FOLDERS for q in glob.glob(os.path.join(o.HOME, d, "*.json"))]):
        if p in names or not o._is_orq_path(p):
            continue
        try:
            d = _read_value(p)
        except (OSError, ValueError):
            continue
        en = o.to_en(d)
        _leftovers(en, leftovers)
        if en != d:
            files_set.append((p, p))
    locks = [os.path.join(o.HOME, t) for t in OLD_LOCKS if os.path.exists(os.path.join(o.HOME, t))]
    return {"eventos": (line_list, to_change, unreadable), "arquivos": files_set, "travas": locks, "sobras": leftovers}


def _backup(p):
    folder = os.path.join(o.HOME, f"backup-pt-{time.strftime('%Y-%m-%dT%H-%M-%S')}")
    rel = lambda x: os.path.relpath(x, o.HOME)  # noqa: E731
    to_copy = ([os.path.join(o.HOME, "events.jsonl")] if p["eventos"][1] else []) + [a for a, _ in p["arquivos"]]
    for a in to_copy:
        destination = os.path.join(folder, rel(a))
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        (shutil.copytree if os.path.isdir(a) else shutil.copy2)(a, destination)
    return folder


def _events():
    """Reescreve o events.jsonl com para_en num tmp ao lado; confere as linhas e troca. A linha ilegível vai como está."""
    ev = os.path.join(o.HOME, "events.jsonl")
    tmp = f"{ev}.migrar-{os.getpid()}.tmp"
    entry = output = 0
    with open(ev, encoding="utf-8") as f, open(tmp, "w", encoding="utf-8") as g:
        for line in f:
            entry += 1
            try:
                e = json.loads(line)
            except ValueError:
                g.write(line if line.endswith("\n") else line + "\n")
            else:
                g.write(json.dumps(o.to_en(e), ensure_ascii=False) + "\n")
            output += 1
    with open(tmp, encoding="utf-8") as g:
        checked = sum(1 for _ in g)
    if not entry == output == checked:
        os.unlink(tmp)
        raise RuntimeError(f"events.jsonl: {entry} linhas lidas, {checked} gravadas; nada foi trocado")
    os.replace(tmp, ev)
    return entry


def _file_path(a, b):
    """Traduz e grava `a` em `b` (o nome novo), apaga `a`. Pasta (perguntar/ -> ask/) e arquivo que não é JSON (gerente-vivo) só trocam de nome."""
    if os.path.isdir(a) or not a.endswith(".json"):
        if os.path.exists(b):
            return f"{os.path.basename(a)}: {os.path.basename(b)} já existe, ficou o novo (o antigo está no backup)"
        os.replace(a, b)
        return None
    try:
        d = _read_value(a)
    except (OSError, ValueError) as e:
        return f"{os.path.basename(a)}: ilegível ({e}), ficou como está"
    if a != b and os.path.exists(b):
        os.unlink(a)
        return f"{os.path.basename(a)}: {os.path.basename(b)} já existe, ficou o novo (o antigo está no backup)"
    _write_json_file(b, d)
    if a != b:
        os.unlink(a)
    return None


def report(p, done=None):
    line_list, to_change, unreadable = p["eventos"]
    rel = lambda x: os.path.relpath(x, o.HOME)  # noqa: E731
    out = [f"ORQ_HOME: {o.HOME}", f"events.jsonl: {line_list} linhas, {to_change} em pt" + (f", {len(unreadable)} ilegíveis (linhas {', '.join(map(str, unreadable[:10]))}) vão como estão" if unreadable else "")]
    out += [f"  {rel(a)} -> {rel(b)}" if a != b else f"  {rel(a)}: traduzido" for a, b in p["arquivos"]] or ["  nenhum arquivo em pt"]
    out += [f"  trava antiga apagada: {rel(t)}" for t in p["travas"]]
    out.append("chaves pt sem tradução: " + (", ".join(f"{k} ({n})" for k, n in sorted(p["sobras"].items())) or "nenhuma"))
    if done:
        out += done
    return "\n".join(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="só mostra o que faria")
    a = ap.parse_args(argv)
    if blockers := _live_blockers():
        print("migração recusada:\n" + "\n".join(f"- {b}" for b in blockers), file=sys.stderr)
        return 2
    with _locks() as stuck_locks:
        if stuck_locks:
            print("migração recusada:\n" + "\n".join(f"- {b}" for b in stuck_locks), file=sys.stderr)
            return 2
        p = plan()
        if a.dry_run:
            print(report(p) + "\n(dry-run: nada foi gravado)")
            return 0
        if not p["eventos"][1] and not p["arquivos"] and not p["travas"]:
            print(report(p) + "\nnada a migrar")
            return 0
        done = [f"backup: {_backup(p)}"]
        if p["eventos"][1]:
            done.append(f"events.jsonl reescrito: {_events()} linhas")
        for origin_name, destination in p["arquivos"]:
            if notice := _file_path(origin_name, destination):
                done.append(f"aviso: {notice}")
        for t in p["travas"]:
            with contextlib.suppress(OSError):
                os.unlink(t)
    print(report(p, done))
    return 0


if __name__ == "__main__":
    sys.exit(main())
