#!/usr/bin/env python3
"""Snapshot determinístico do coordenador no PreCompact e retomada no SessionStart(compact).

  precompact.py            (stdin: JSON do PreCompact) grava handoff/<data>.md, atualiza ultimo.md e salva no engram
  precompact.py retomar    (stdin: JSON do SessionStart) imprime o ultimo.md como additionalContext se source == compact
  precompact.py passagem --de H --para H   (`orq passagem coordenador`) grava o mesmo snapshot sem compactar e handoff/passagem.json (quem o escreveu e quando);
                           o `orq hook session` do outro harness o injeta (ticket 93). Imprime o registro em JSON; sem Run ligado, sai 1 com a causa no stderr
Só no coordenador (regra de papel do orq.py, importada sem alterá-lo). Fail-open: qualquer erro vira exit 0 e uma linha no orq.log.
Variáveis para teste: ORQ_HOME, ORQ_ORCA, ORQ_PENDENCIAS (as do orq.py), ORQ_CLI, ORQ_ENGRAM, ORQ_GH, ORQ_DESENHO.
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
    import falha_segura
    falha_segura.sair("precompact.py", e)

TETO_S = 20  # PreCompact: o hook tem o timeout do settings (30 s); o script para de coletar aos 20 s
LINHAS_RETOMADA = 60  # o montar respeita orçamentos por seção que cabem aqui (M12); passou disso, a retomada avisa que cortou
VELHO_S = 15 * 60  # ultimo.md mais velho que isso não é do compact que acabou de acontecer (B33)
DESENHO = os.environ.get("ORQ_DESENHO") or os.path.expanduser("~/.claude/orquestrador-plan/desenho.md")
CLI = shlex.split(os.environ.get("ORQ_CLI") or f"python3 {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'orq.py')}")
ENGRAM = os.environ.get("ORQ_ENGRAM") or "engram"
GH = os.environ.get("ORQ_GH") or "gh"
WAITER = os.path.expanduser("~/.claude/scripts/orca-wait-runs.py")
HANDOFF = os.path.join(orq.HOME, "handoff")
MAX_VIVOS, MAX_ENTREGUES, MAX_PEND, MAX_LISTA = 5, 4, 5, 4  # orçamento de linhas por seção (M12)


def rodar(cmd, timeout=5, cwd=None):
    """Saída de um comando, ou '' se falhou: uma seção que falha não derruba as outras. O que passa do teto de TETO_S do hook fica sem rodar (B33)."""
    resta = TETO_S - (time.monotonic() - T0)
    if resta <= 0.5:
        orq.log(f"precompact: {cmd[0]} pulado, orçamento de {TETO_S} s esgotado")
        return ""
    timeout = min(timeout, resta)
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        return p.stdout.strip() if p.returncode == 0 else ""
    except Exception as e:
        orq.log(f"precompact: {cmd[0]} falhou: {e}")
        return ""


def secao_agentes():
    txt = rodar(CLI + ["agentes", "--json"])
    try:
        ags = json.loads(txt)
    except ValueError:
        return "(orq agentes indisponível)"
    vivos = [a for a in ags if a.get("estado") not in ("liberado", "entregue")]
    entregues = [a for a in ags if a.get("estado") == "entregue" and not a.get("retido")]
    linhas = [f"- {a.get('estado')} {a.get('task')} {a.get('titulo')} ({a.get('modelo')}, {a.get('terminal')}, fase: {a.get('fase') or '-'})"
              for a in vivos[:MAX_VIVOS]] or ["Nenhum agente rodando, travado ou perguntando."]
    if len(vivos) > MAX_VIVOS:
        linhas.append(f"+{len(vivos) - MAX_VIVOS} (orq agentes)")
    if vivos:
        runs = sorted({a["run"] for a in vivos if a.get("run")})
        linhas.append(f"Waiter: `python3 {WAITER} {' '.join(runs)}`")
    linhas += [f"- entregue {a.get('task')} {a.get('titulo')}: orq liberar {a.get('dispatch')}" for a in entregues[:MAX_ENTREGUES]]  # B32
    if len(entregues) > MAX_ENTREGUES:
        linhas.append(f"+{len(entregues) - MAX_ENTREGUES} entregues (orq agentes)")
    return "\n".join(linhas)


def secao_pendencias():
    try:
        itens = orq._pend_ro()["itens"]  # o pendencias.json, ou o backlog com ORQ_BACKLOG
    except Exception:
        return "(pendencias.json ilegível)"
    if not itens:
        return "Nenhuma."
    linhas = [f"- {i.get('id')} [{i.get('tipo')}] {i.get('titulo')} (desde {i.get('desde') or '?'}, espera: {i.get('espera') or '-'})" for i in itens[:MAX_PEND]]
    return "\n".join(linhas + ([f"+{len(itens) - MAX_PEND} (orq pend)"] if len(itens) > MAX_PEND else []))


def cortar(txt, n=MAX_LISTA):
    """As n primeiras linhas de txt e '+N' com o que ficou de fora."""
    ls = txt.splitlines()
    return "\n".join(ls[:n] + ([f"+{len(ls) - n}"] if len(ls) > n else []))


def secao_prs(cwd):
    if not rodar(["git", "rev-parse", "--git-dir"], cwd=cwd):
        return "(cwd fora de repo git)"
    txt = rodar([GH, "pr", "list", "--author", "@me", "--state", "open", "--json", "number,title,url"], timeout=8, cwd=cwd)
    try:
        prs = json.loads(txt)
    except ValueError:
        return "(gh indisponível)"
    return cortar("\n".join(f"- #{p['number']} {p['title']} {p['url']}" for p in prs)) or "Nenhum."


def secao_entradas():
    try:
        ev = orq.read_events()
    except Exception:
        return "(events.jsonl ilegível)"
    efeito = {e.get("entrada"): e for e in ev if e.get("tipo") == "intake"}
    us = [e for e in ev if e.get("tipo") == "entrada" and e.get("origem") == "usuario"][-10:]
    out = []
    for e in us:
        i = efeito.get(e.get("id")) or {}
        out.append(f"- {e.get('id')} {e.get('ts')}: {(e.get('texto') or '')[:120]!r} -> {i.get('efeito') or 'sem efeito'}" + (f" {i['ref']}" if i.get("ref") else ""))
    return "\n".join(out) or "Nenhuma."


def montar(run, cwd):
    """O snapshot em markdown; cada seção é independente."""
    # o que mais importa vem primeiro: a retomada corta o fim (M12). Sem "orq status": o `orq hook session` injeta um mais novo no compact (B29)
    secoes = [
        ("Run ligado", run["id"]),
        ("Últimas 10 entradas do usuário", secao_entradas()),
        ("Mapa", DESENHO),
        ("Agentes", secao_agentes()),
        ("Pendências do usuário", secao_pendencias()),
        ("Tickets abertos", cortar(rodar(CLI + ["ticket", "lista"])) or "Nenhum (ou orq indisponível)."),
        ("PRs abertos do usuário", secao_prs(cwd)),
    ]
    return "\n\n".join(f"## {t}\n{c}" for t, c in secoes)


def gravar(md, agora):
    os.makedirs(HANDOFF, exist_ok=True)
    nome = os.path.join(HANDOFF, agora.strftime("%Y-%m-%dT%H-%M") + ".md")
    tmp = nome + ".tmp"
    open(tmp, "w").write(f"# Handoff {agora:%Y-%m-%d %H:%M}\n\n{md}\n")
    os.replace(tmp, nome)
    ultimo = os.path.join(HANDOFF, "ultimo.md")
    if os.path.lexists(ultimo):
        os.remove(ultimo)
    os.symlink(os.path.basename(nome), ultimo)
    return nome


def engram(md, agora, cwd):
    projeto = os.path.basename(rodar(["git", "rev-parse", "--show-toplevel"], cwd=cwd) or cwd)
    resumo = "\n".join(md.splitlines()[:60])[:3500]
    rodar([ENGRAM, "save", f"Handoff {agora:%Y-%m-%d %H:%M}", resumo, "--project", projeto, "--type", "decision", "--topic", "sessao/handoff"], timeout=10)


def precompact(ev):
    sid = ev.get("session_id") or ""
    run = orq.coordenador({"session_id": sid})
    if not run:
        return
    cwd = ev.get("cwd") or os.getcwd()
    agora = datetime.now()
    md = montar(run, cwd)
    if time.monotonic() - T0 > TETO_S:
        orq.log("precompact: passou do teto; snapshot gravado mesmo assim")
    gravar(md, agora)
    engram(md, agora, cwd)


def retomar(ev):
    if ev.get("source") != "compact" or not orq.coordenador({"session_id": ev.get("session_id") or ""}):
        return
    try:
        arq = os.path.join(HANDOFF, "ultimo.md")
        todas, idade = open(arq).read().splitlines(), time.time() - os.path.getmtime(arq)
    except OSError:
        return
    linhas = todas[:LINHAS_RETOMADA] + ([f"(… {len(todas) - LINHAS_RETOMADA} linhas cortadas; leia handoff/ultimo.md)"] if len(todas) > LINHAS_RETOMADA else [])
    titulo = ("Handoff salvo antes do compact (handoff/ultimo.md):" if idade < VELHO_S else
              f"Handoff VELHO de {datetime.fromtimestamp(os.path.getmtime(arq)):%Y-%m-%d %H:%M}: o PreCompact não gravou um novo (handoff/ultimo.md):")  # B33
    ctx = titulo + "\n" + "\n".join(linhas)
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": ctx}}))


def passagem(de, para):
    """O snapshot do PreCompact sob demanda, para o coordenador que troca de harness. O registro (de, para, ts, Run) é o que o `orq hook session`
    do outro lado confere: ele só injeta se o `ts` tem menos de 15 min e o `de` não é o harness dele. Sem Engram: o servidor é o mesmo nos dois harnesses."""
    run = orq.orca("run-current")["run"]
    if not run:
        print("orq: sem Run ligado a este terminal: não há estado de coordenador para passar (`orca orchestration run-use --id <run>`)", file=sys.stderr)
        return 1
    agora = datetime.now()
    nome = gravar(montar(run, os.getcwd()), agora)
    reg = {"arquivo": os.path.basename(nome), "de": de, "para": para, "ts": time.time(), "run": run["id"], "aceita": None}
    orq._write_json(os.path.join(HANDOFF, "passagem.json"), reg)
    print(json.dumps(reg, ensure_ascii=False))
    return 0


def main(argv):
    global T0
    T0 = time.monotonic()
    if argv[1:2] == ["passagem"]:
        ap = argparse.ArgumentParser(prog="precompact.py passagem")
        ap.add_argument("--de", required=True)
        ap.add_argument("--para", required=True)
        a = ap.parse_args(argv[2:])
        try:
            return passagem(a.de, a.para)
        except Exception as e:  # noqa: BLE001 - comando, não hook: a causa vai para o stderr em vez de sumir no log
            print(f"orq: passagem do coordenador falhou: {type(e).__name__}: {e}", file=sys.stderr)
            return 1
    try:
        raw = sys.stdin.read()
        ev = json.loads(raw) if raw.strip() else {}
        (retomar if argv[1:2] == ["retomar"] else precompact)(ev)
    except Exception as e:
        orq.log(f"precompact: {type(e).__name__}: {e}")
    return 0


T0 = time.monotonic()
if __name__ == "__main__":
    sys.exit(main(sys.argv))
