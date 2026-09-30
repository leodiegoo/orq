#!/usr/bin/env python3
"""Espera evento em vários Runs do Orca, porque o coordenador só consome o mailbox do Run a que está ligado.

Uso: orca-wait-runs.py <run_id> [<run_id> ...]
Sai quando uma tarefa `dispatched` de qualquer Run muda de status ou quando chega mensagem no Run ligado (com o agent manager ligado, em qualquer Run dele). Nos
demais Runs da lista, sai com o que não for heartbeat e ainda não foi lido no inbox (question e escalation não mudam o status da task); não consome nada.
Imprime o Run e as tarefas que mudaram; ler e dar ack exige `orca orchestration run-use --run <id>` antes.

Heartbeat de worker é confirmado sem acordar ninguém, pela mesma rotina do hook `orq hook prompt` (`orq.confirmar_lotes`): o Orca repete a
entrega atual quando o mesmo lote é confirmado duas vezes, então hook e waiter juntos não fazem double-ack nem perdem mensagem, e cada
lote confirmado vira um evento heartbeat_absorvido no painel. Um lote que não é só heartbeat é impresso e fica em aberto para o `check`.
"""
import contextlib
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.expanduser("~/.claude/orq"))
try:
    import orq
except Exception:  # noqa: BLE001 - sem o orq o waiter confirma heartbeat sozinho, sem gravar o evento
    orq = None

POLL_S = float(os.environ.get("ORQ_WAIT_POLL") or 30)
MAX_S = float(os.environ.get("ORQ_WAIT_MAX") or 3600)
ORCA = os.environ.get("ORQ_ORCA") or "orca"  # os testes do orq apontam para o Orca falso


def orca(*args):
    h = orq.handle_orca() if orq else None  # com o agent manager ligado, o Run é lido pelo terminal dele
    env = {**os.environ, "ORCA_TERMINAL_HANDLE": h} if h else None
    if orq and args[0] in orq.MUTA_RUN and "--run" in args and orq._do_gerente(args[args.index("--run") + 1]):
        orq.orca("run-use", "--id", args[args.index("--run") + 1])  # o gerente liga um Run por vez; quem chama segura a trava
    out = subprocess.run([ORCA, "orchestration", *args, "--json"], capture_output=True, text=True, timeout=30, env=env).stdout
    return json.loads(out or "{}")


def open_tasks(run):
    r = orca("task-list", "--run", run).get("result") or {}
    ts = r.get("tasks", r) if isinstance(r, dict) else r
    return {t["id"]: t["status"] for t in ts if t.get("status") not in ("completed",)}


runs = sys.argv[1:]
before = {run: open_tasks(run) for run in runs}
bound = (orca("run-current").get("result") or {}).get("run", {}).get("id")
vigiados = (orq.runs_do_gerente() if orq else []) or ([bound] if bound else [])  # com o gerente ligado, a caixa de cada Run dele
start = time.time()
while time.time() - start < MAX_S:
    time.sleep(POLL_S)
    for bound in vigiados:
        with orq.trava_gerente() if orq else contextlib.nullcontext():  # ligar, ler e confirmar o Run na mesma ligação do gerente
            res = orca("check", "--run", bound).get("result") or {}
            if orq and res.get("messages"):
                try:
                    res = orq.confirmar_lotes(bound, res)[1]
                except Exception:  # noqa: BLE001 - lote consumido e sem ack é repetido no próximo check: nada se perde
                    res = {}
        msgs = res.get("messages") or []
        if not orq and msgs and all(m.get("type") == "heartbeat" for m in msgs):
            orca("check", "--run", bound, "--ack", res["deliveryId"])
            msgs = []
        elif orq and msgs and orq.so_heartbeats(msgs):  # sobrou lote de heartbeat além do teto de lotes: fica para a próxima volta
            msgs = []
        if msgs:
            print(json.dumps({"run": bound, "messages": msgs}, ensure_ascii=False))
            sys.exit(0)
    for run in runs:
        if run not in vigiados:  # Run que o coordenador não lê (o check dá consumer_fenced): question/escalation/worker_done só aparecem no inbox
            msgs = [m for m in (orca("inbox", "--limit", "200").get("result") or {}).get("messages") or []
                    if isinstance(m, dict) and m.get("to_handle") == f"run:{run}" and not m.get("read") and m.get("type") != "heartbeat"]
            if msgs:  # nada é consumido: as mensagens saem num lote quando o coordenador faz run-use --id <run>
                print(json.dumps({"run": run, "messages": msgs}, ensure_ascii=False))
                sys.exit(0)
        now = open_tasks(run)
        changed = {t: s for t, s in before[run].items() if now.get(t, "completed") != s}
        if changed:
            print(json.dumps({"run": run, "changed": {t: now.get(t, "completed") for t in changed}}))
            sys.exit(0)
print(json.dumps({"timeout": True}))
