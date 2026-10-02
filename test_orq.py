#!/usr/bin/env python3
"""Testes do orq (fatias 1 e 3). Rodam com `python3 test_orq.py`: ORQ_HOME num diretório temporário e ORQ_ORCA num Orca falso."""
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
import time
from concurrent.futures import ThreadPoolExecutor

AQUI = os.path.dirname(os.path.abspath(__file__))
ORQ = os.path.join(AQUI, "orq.py")
LIMPAR = os.path.join(AQUI, "hooks", "limpar-mergeados-hook.py")
sys.path.insert(0, AQUI)
import orq as orq_mod  # noqa: E402
os.environ["ORQ_AVISO_GAP_S"] = "0"  # a segunda leitura da caixa não espera nos testes
os.environ["E2E_LOCK_DIR"] = "/nonexistent/e2e-queue"  # o digest e o status dos testes não leem a fila real da máquina
orq_mod.CODEX_CONFIG = os.path.join(tempfile.mkdtemp(), "codex-config.toml")
orq_mod.CODEX_HOOKS = os.path.join(tempfile.mkdtemp(), "hooks.json")  # idem: o hooks.json de verdade tem hook do orq e pode estar não confiado  # nenhum teste grava no ~/.codex/config.toml de verdade

FAKE = '''#!/usr/bin/env python3
import base64, json, os, sys, time
d = os.environ["FAKE_DIR"]
a = sys.argv[2:]
cmd = a[0]
def opt(n, padrao=None):
    return a[a.index(n) + 1] if n in a else padrao
bound = None
try:
    # o Run ligado ao terminal que chama. Como o Orca real (conferido em 29/09): um handle desconhecido (term_0000…) não tem ligação e dá o
    # escopo "all"; SEM a variável o Orca cai no Run do coordenador ativo, ou seja, no Run ligado
    if not os.environ.get("ORCA_TERMINAL_HANDLE", "").startswith("term_0000"):
        _r = json.load(open(os.path.join(d, "run.json"))) or {}
        # run.json com "handle": só esse terminal está ligado ao Run (o agent manager, seção 25); sem a chave, qualquer terminal conhecido
        if _r.get("handle") in (None, os.environ.get("ORCA_TERMINAL_HANDLE")):
            bound = _r.get("id")
except (OSError, AttributeError):
    pass
multi = os.path.join(d, "binds.json")
if os.path.exists(multi):
    # vários Runs ({run: {"handle", "gen"}}): um Run por terminal, e ligar outro tira o terminal do anterior, como o Orca real (conferido em 29/09)
    _b = json.load(open(multi))
    bound = next((r for r, v in _b.items() if v["handle"] == os.environ.get("ORCA_TERMINAL_HANDLE")), None)
# como o Orca real: sem --run, task-list, gate-list e worker-list ficam no escopo do Run ligado ao terminal
run = opt("--run") or (bound if a[0] in ("task-list", "gate-list", "worker-list") else None)
open(os.path.join(d, "calls.log"), "a").write(json.dumps(a) + "\\n")
if os.environ.get("FAKE_SLEEP"):
    time.sleep(float(os.environ["FAKE_SLEEP"]))
if os.environ.get("FAKE_SLEEP_CMD", ":").split(":")[0] == cmd:
    time.sleep(float(os.environ["FAKE_SLEEP_CMD"].split(":")[1]))  # só o comando nomeado demora
if os.environ.get("FAKE_CRASH"):
    print("isto não é json"); sys.exit(3)
def ler(nome, padrao):
    try:
        return json.load(open(os.path.join(d, nome)))
    except OSError:
        return padrao
def ler_linhas(nome):
    try:
        return open(os.path.join(d, nome)).read().splitlines()
    except OSError:
        return []
def pagina(itens):
    # --limit/--cursor como o Orca: o cursor é o deslocamento em base64; devolve (itens, próximo cursor)
    ini = int(base64.b64decode(opt("--cursor")).decode()) if opt("--cursor") else 0
    fim = ini + int(opt("--limit", 100))
    return itens[ini:fim], (base64.b64encode(str(fim).encode()).decode() if fim < len(itens) else None)
def falha(msg):
    print(json.dumps({"ok": False, "error": {"message": msg}})); sys.exit(0)
def turno_comeca(dispatch):
    # o hook prompt do worker grava o início do turno em turnos.json (ORQ_HOME); FAKE_INICIO: nunca (o spec não entra) | depois_do_enter (só o Enter submete)
    p = os.path.join(os.environ["ORQ_HOME"], "turnos.json")
    try:
        t = json.load(open(p))
    except (OSError, ValueError):
        t = {}
    t[dispatch] = {"task": None, "sessao": "s", "inicio": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "fim": None}
    os.makedirs(os.path.dirname(p), exist_ok=True); json.dump(t, open(p, "w"))
if (os.environ.get("FAKE_FAIL") == cmd and cmd != "send") or (os.environ.get("FAKE_FAIL_RUN") and run == os.environ["FAKE_FAIL_RUN"]):
    falha("falhou " + cmd)
if sys.argv[1] == "search":
    # orca search <query>: search.json traz os hits do índice de sessões (agent, sessionId, cwd, source.filePath, evidence.role), filtrados por --agent
    hits = [h for h in ler("search.json", []) if not opt("--agent") or h.get("agent") == opt("--agent")]
    print(json.dumps({"ok": True, "result": {"kind": "search", "hits": hits}})); sys.exit(0)
if sys.argv[1] == "account":
    # orca account list: account.json é o rateLimits ({claude|codex: {session, weekly}}) que o Orca lê de cada conta
    print(json.dumps({"ok": True, "result": {"rateLimits": ler("account.json", {})}})); sys.exit(0)
if sys.argv[1] == "terminal" and cmd == "create":
    # orca terminal create: grava no create.log, dá o handle term_ret<N> e o põe no terminals.json (ticket 48); FAKE_FAIL_CREATE_PATH falha nessa worktree
    if os.environ.get("FAKE_FAIL_CREATE_PATH") and os.environ["FAKE_FAIL_CREATE_PATH"] in opt("--worktree", ""):
        falha("selector_not_found")
    open(os.path.join(d, "create.log"), "a").write(json.dumps(a) + "\\n")
    novo = "term_ret%d" % len(ler_linhas("create.log"))
    json.dump(ler("terminals.json", []) + [novo], open(os.path.join(d, "terminals.json"), "w"))
    print(json.dumps({"ok": True, "result": {"terminal": {"handle": novo, "title": opt("--title")}}})); sys.exit(0)
if sys.argv[1] == "terminal" and cmd in ("close", "rename", "send"):
    # orca terminal close|rename|send: grava no close.log|rename.log|send.log; close tira o handle do terminals.json
    open(os.path.join(d, cmd + ".log"), "a").write(json.dumps(a) + "\\n")
    if os.environ.get("FAKE_FAIL_TERMINAL") == cmd:
        falha("falhou terminal " + cmd)
    if cmd == "send" and os.environ.get("FAKE_PROMPT_BLOCKED") and "--text" in a:
        falha("agent_prompt_blocked Terminal prompt request ID: x")  # o Orca sabe que o agente está no meio do turno, mesmo com o tui-idle satisfeito
    if cmd == "send" and os.environ.get("FAKE_FAIL_SEND_DEPOIS") and len(ler_linhas("send.log")) > int(os.environ["FAKE_FAIL_SEND_DEPOIS"]):
        falha("falhou terminal send")  # o enésimo send em diante falha (o texto do anterior já foi digitado)
    if cmd == "close":
        json.dump([h for h in ler("terminals.json", []) if h != opt("--terminal")], open(os.path.join(d, "terminals.json"), "w"))
        arq = os.environ.get("ORQ_PROCESSOS")
        if arq and os.path.exists(arq):
            # fechar o terminal mata o agente e tudo o que sobe abaixo dele (ORQ_PROCESSOS: a lista de processos falsa, com o terminal de cada agente)
            ps = json.load(open(arq))
            mortos = {p["pid"] for p in ps if p.get("terminal") == opt("--terminal")}
            while any(p["ppid"] in mortos and p["pid"] not in mortos for p in ps):
                mortos |= {p["pid"] for p in ps if p["ppid"] in mortos}
            json.dump([p for p in ps if p["pid"] not in mortos], open(arq, "w"))
    res = {"handle": opt("--terminal")}
    if cmd == "send" and "--text" not in a and os.environ.get("FAKE_INICIO") == "depois_do_enter":
        turno_comeca("ctx_" + opt("--terminal"))
    if cmd == "send":
        # como o Orca real com o agente ocioso (conferido em 29/09): o turno começa; FAKE_ENTER_PERDIDO: o texto entra e o Enter não submete,
        # só um Enter sozinho começa o turno; FAKE_SEM_OBSERVACAO: o Orca não sabe observar (provider unsupported)
        res["accepted"] = True
        perdido = os.environ.get("FAKE_ENTER_PERDIDO") and "--text" in a
        res["prompt"] = {"stages": ["input_accepted"] if perdido or os.environ.get("FAKE_SEM_OBSERVACAO") else ["input_accepted", "turn_started"],
                         "observation": "unsupported" if os.environ.get("FAKE_SEM_OBSERVACAO") else "supported"}
    print(json.dumps({"ok": True, "result": {cmd: res}})); sys.exit(0)
if sys.argv[1] == "terminal" and cmd == "wait":
    # busy.json: handles no meio do turno; o wait deles estoura o prazo, como o Orca real
    if opt("--terminal") in ler("busy.json", []):
        print(json.dumps({"ok": False, "error": {"code": "timeout", "message": "timeout"}})); sys.exit(0)
    print(json.dumps({"ok": True, "result": {"wait": {"handle": opt("--terminal"), "condition": opt("--for"), "satisfied": True}}})); sys.exit(0)
if sys.argv[1] == "terminal" and cmd == "read":
    # drafts.json: {handle: texto que o usuário deixou na caixa}
    rascunho = ler("drafts.json", {}).get(opt("--terminal"))
    print(json.dumps({"ok": True, "result": {"terminal": {"handle": opt("--terminal"), "status": "running", "tail": ler("screens.json", {}).get(opt("--terminal"), []), **({"draft": rascunho} if rascunho else {})}}})); sys.exit(0)
if sys.argv[1] == "terminal" and cmd == "list":
    # orca terminal list: terminals.json guarda os handles vivos; sem o arquivo, todo worker do workers.json tem terminal aberto
    vivos = ler("terminals.json", None)
    if vivos is None:
        vivos = [w["handle"] for w in ler("workers.json", [])]
    # terminals_truncados: como o Orca com --limit menor que o total (truncated: true)
    print(json.dumps({"ok": True, "result": {"terminals": [{"handle": h} for h in vivos], "truncated": os.path.exists(os.path.join(d, "terminals_truncados"))}})); sys.exit(0)
if sys.argv[1] == "terminal":
    # orca terminal show: terminals.json guarda os handles vivos; handle morto é terminal_handle_stale, como no Orca real
    if opt("--terminal") in ler("terminals.json", []):
        print(json.dumps({"ok": True, "result": {"terminal": {"handle": opt("--terminal")}}})); sys.exit(0)
    print(json.dumps({"ok": False, "error": {"code": "terminal_handle_stale", "message": "terminal_handle_stale"}})); sys.exit(0)
if sys.argv[1] == "repo" and cmd == "add":
    # orca repo add --path: grava o repo no repos.json, como o Orca registra a pasta
    json.dump(ler("repos.json", []) + [{"id": "r%d" % (len(ler("repos.json", [])) + 1), "path": opt("--path"), "displayName": os.path.basename(opt("--path"))}], open(os.path.join(d, "repos.json"), "w"))
    print(json.dumps({"ok": True, "result": {"repo": {"id": "r1"}}})); sys.exit(0)
if sys.argv[1] == "repo" and cmd == "set-base-ref":
    print(json.dumps({"ok": True, "result": {"repo": {"worktreeBaseRef": opt("--ref")}}})); sys.exit(0)
if sys.argv[1] == "worktree":
    import subprocess
    wts = ler("worktrees.json", [])
    if cmd == "rm":
        path = opt("--worktree").removeprefix("path:")
        w = next(x for x in wts if x["path"] == path)
        rel = os.environ.get("ORQ_RELATORIOS", "")
        open(os.path.join(d, "wtrm.log"), "a").write(json.dumps({"args": a, "relatorios": os.listdir(rel) if os.path.isdir(rel) else []}) + "\\n")
        subprocess.run(["git", "-C", w["repo"], "worktree", "remove", "--force", path], check=True, capture_output=True)
        json.dump([x for x in wts if x is not w], open(os.path.join(d, "worktrees.json"), "w"))
    print(json.dumps({"ok": True, "result": {"worktrees": wts}})); sys.exit(0)
if sys.argv[1] == "repo" and cmd == "list":
    print(json.dumps({"ok": True, "result": {"repos": ler("repos.json", [])}})); sys.exit(0)
if sys.argv[1] == "tab":
    # orca tab create --url <url>: só o calls.log guarda a chamada (FAKE_FAIL=create a recusa)
    print(json.dumps({"ok": True, "result": {"tab": {"id": "tab_1", "url": opt("--url")}}})); sys.exit(0)
if sys.argv[1] == "automations":
    p = os.path.join(d, "automations_runs.json")
    print(open(p).read() if os.path.exists(p) else json.dumps({"ok": True, "result": {"runs": []}}))
    sys.exit(0)
if cmd == "inbox":
    p = os.path.join(d, "inbox.json")
    msgs = (json.load(open(p))["result"]["messages"] if os.path.exists(p) else [])
    # como o Orca real: dispatch completed traz o worker_done dele (sem_done no workers.json: concluído sem ele); fica fora do --limit e da sequência dos testes
    for i, w in enumerate(ler("workers.json", [])):
        dd = w.get("dispatch", "ctx_" + w["handle"])
        if w.get("status", "completed") == "completed" and not w.get("sem_done") and not any(m.get("type") == "worker_done" and (m.get("payload") or "").find(dd) >= 0 for m in msgs):
            msgs.append({"id": "msg_done%d" % i, "run_id": w["run"], "type": "worker_done", "subject": "done", "from_handle": "dispatch:" + dd, "to_handle": "run:" + w["run"],
                         "read": 1, "sequence": -1 - i, "created_at": "2000-01-01 00:00:00", "payload": json.dumps({"taskId": w.get("task"), "dispatchId": dd, "outcome": "succeeded"})})
    msgs = sorted(msgs, key=lambda m: -m["sequence"])
    if opt("--terminal"):
        msgs = []  # como o Orca real: o destinatário é `run:<id>`, então o filtro por terminal do coordenador volta vazio
    if opt("--limit"):
        msgs = msgs[: int(opt("--limit"))]  # como o Orca: as N mais novas
    print(json.dumps({"ok": True, "result": {"messages": msgs, "count": len(msgs)}}))
    sys.exit(0)
if cmd == "check":
    # como o Orca real (conferido num Run de teste): só o Run ligado lê (--peek também); o lote é a sequência inicial de mensagens não
    # confirmadas do mesmo tipo; o check repete a entrega em aberto até o --ack; o --ack confirma o lote e já devolve o próximo;
    # confirmar de novo o mesmo id repete a entrega atual; id desconhecido é stale_delivery
    alvo = opt("--run")
    if bound is None or (alvo and alvo != bound):
        print(json.dumps({"ok": False, "error": {"code": "consumer_fenced", "message": "This coordinator terminal is no longer bound to Run " + str(alvo)}})); sys.exit(0)
    if os.environ.get("FAKE_FAIL_ACK") and "--ack" in a:
        falha("falhou --ack")
    caixa = ler("mailbox.json", {"n": 0, "msgs": []})
    ms = [m for m in caixa["msgs"] if m["run_id"] == alvo]
    def salva():
        json.dump(caixa, open(os.path.join(d, "mailbox.json"), "w"))
    def limpo(m):
        return {k: v for k, v in m.items() if k not in ("status", "delivery", "gen")}
    gen = _b[alvo]["gen"] if os.path.exists(multi) and alvo in _b else None
    if gen is not None:
        for m in ms:  # a entrega de uma geração antiga do consumidor caiu: o check seguinte a refaz
            if m["status"] == "out" and m.get("gen") != gen:
                m["status"] = "unread"
    if "--all" in a:
        print(json.dumps({"ok": True, "result": {"messages": [limpo(m) for m in ms], "count": len(ms), "acknowledged": None, "runId": alvo}})); sys.exit(0)
    abertas = [m for m in ms if m["status"] != "acked"]
    if "--peek" in a:
        print(json.dumps({"ok": True, "result": {"runId": alvo, "messages": [limpo(m) for m in abertas], "count": len(abertas), "acknowledged": None}})); sys.exit(0)
    ack = opt("--ack")
    if ack:
        if not any(m.get("delivery") == ack for m in ms):
            print(json.dumps({"ok": False, "error": {"code": "stale_delivery", "message": "Delivery " + ack + " does not belong to this mailbox."}})); sys.exit(0)
        if any(m.get("delivery") == ack and m.get("gen") != gen for m in ms) and gen is not None:
            print(json.dumps({"ok": False, "error": {"code": "consumer_fenced", "message": "This mailbox Delivery belongs to a fenced consumer generation."}})); sys.exit(0)
        for m in ms:
            if m.get("delivery") == ack:
                m["status"] = "acked"
        abertas = [m for m in ms if m["status"] != "acked"]
    emaberto = [m for m in abertas if m["status"] == "out"]
    if emaberto:
        lote = [m for m in abertas if m.get("delivery") == emaberto[0]["delivery"]]
    else:
        lote = []
        for m in abertas:
            if lote and (m["type"] != lote[0]["type"] or m.get("priority") != lote[0].get("priority")):
                break
            lote.append(m)
        if lote:
            caixa["n"] += 1
            for m in lote:
                m["status"], m["delivery"], m["gen"] = "out", "delivery_%d" % caixa["n"], gen
    if os.environ.get("FAKE_ARRIVE_ON_CHECK") and lote and not ack:
        # mensagem que chega logo depois do primeiro check que consome (a corrida entre o peek e o lote seguinte)
        caixa["msgs"].append({"id": "msg_chegou", "run_id": alvo, "to_handle": "run:" + alvo, "type": os.environ["FAKE_ARRIVE_ON_CHECK"],
                              "subject": "x", "payload": "{}", "created_at": "2026-09-29T17:59:00Z", "delivered_at": None, "status": "unread"})
    salva()
    print(json.dumps({"ok": True, "result": {"runId": alvo, "deliveryId": lote[0]["delivery"] if lote else None, "messages": [limpo(m) for m in lote],
                                            "count": len(lote), "acknowledged": ack}})); sys.exit(0)
if cmd == "run-current" and os.path.exists(multi):
    res = {"run": {"id": bound} if bound else None}
elif cmd == "run-current":
    res = {"run": ler("run.json", None) if bound or os.environ.get("ORCA_TERMINAL_HANDLE", "").startswith("term_0000") else None}
elif cmd == "run-use" and os.path.exists(multi):
    # ligar o Run ao terminal tira o terminal do Run anterior (a geração dos dois sobe); ligar o Run já ligado não muda nada
    h = os.environ.get("ORCA_TERMINAL_HANDLE")
    open(os.path.join(d, "binds.log"), "a").write(json.dumps([opt("--id"), h]) + "\\n")
    for r_, v in _b.items():
        if v["handle"] == h and r_ != opt("--id"):
            v["handle"], v["gen"] = None, v["gen"] + 1
    v = _b.setdefault(opt("--id"), {"handle": None, "gen": 0})
    if v["handle"] != h:
        v["handle"], v["gen"] = h, v["gen"] + 1
    json.dump(_b, open(multi, "w"))
    res = {"run": {"id": opt("--id"), "coordinator_handle": h}}
elif cmd == "run-create":
    # como o Orca real: o Run novo fica ligado ao terminal que chama
    json.dump({"id": "run_novo", "handle": os.environ.get("ORCA_TERMINAL_HANDLE")}, open(os.path.join(d, "run.json"), "w"))
    res = {"run": {"id": "run_novo", "objective": opt("--objective"), "coordinator_handle": os.environ.get("ORCA_TERMINAL_HANDLE")}}
elif cmd == "run-use":
    # como o Orca real: liga o Run ao terminal que chama (a variável ORCA_TERMINAL_HANDLE), e o terminal anterior perde a ligação
    json.dump({"id": opt("--id"), "handle": os.environ.get("ORCA_TERMINAL_HANDLE")}, open(os.path.join(d, "run.json"), "w"))
    res = {"run": {"id": opt("--id"), "coordinator_handle": os.environ.get("ORCA_TERMINAL_HANDLE")}}
elif cmd == "run-list":
    itens, prox = pagina(ler("runs.json", []))
    res = {"runs": itens, "nextCursor": prox}
elif cmd == "run-show" and os.path.exists(multi):
    res = {"run": {**next((r for r in ler("runs.json", []) if r["id"] == opt("--id")), {}), "id": opt("--id"), "coordinator_handle": _b.get(opt("--id"), {}).get("handle")}}
elif cmd == "run-show":
    res = {"run": next((r for r in ler("runs.json", []) if r["id"] == opt("--id")), {"id": opt("--id")})}
elif cmd == "worker-list":
    # workers.json: [{"handle", "run", "status"?}], os mais novos primeiro. Como o Orca real: --run, senão o Run ligado ao terminal, senão todos
    # (scope.source flag|bound|all); dispatchStatus "dispatched" é o despacho ativo, o resto (padrão "completed") já terminou
    # terminalState/resource como o Orca real: dispatched -> active; completed -> released (padrão), ou retained com retainedReason
    todos = [{"agentTerminalHandle": w["handle"], "dispatchId": w.get("dispatch", "ctx_" + w["handle"]), "runId": w["run"], "taskId": w.get("task", "task_" + w["handle"]),
              "dispatchStatus": w.get("status", "completed"),
              "terminalState": w.get("terminal", "active" if w.get("status") == "dispatched" else "released"),
              "resource": None if w.get("sem_resource") else {"ownershipState": w.get("ownership", "owned"), "retainedReason": w.get("reason"), "terminalHandle": w["handle"],
                                                            "originDispatchId": w.get("origem", w.get("dispatch", "ctx_" + w["handle"])), "ownerDispatchId": w.get("dono", w.get("dispatch", "ctx_" + w["handle"]))}}
             for w in ler("workers.json", [])]
    itens, prox = pagina([w for w in todos if not run or w["runId"] == run])
    res = {"workers": itens, "page": {"limit": int(opt("--limit", 100)), "hasMore": bool(prox), "nextCursor": prox},
           "scope": {"run": run, "source": "flag" if opt("--run") else "bound" if bound else "all"}}
elif cmd == "task-list":
    res = {"tasks": ler(f"tasks_{run}.json", [])}
elif cmd == "gate-list":
    res = {"gates": ler(f"gates_{run}.json", [])}
elif cmd == "gate-create":
    # como o Orca real: id único, e o gate é do Run ligado (gates_map.json guarda gate -> Run)
    open(os.path.join(d, "gates.log"), "a").write(json.dumps(a) + "\\n")
    mapa = ler("gates_map.json", {})
    gid = f"gate_{len(mapa) + 1}"
    mapa[gid] = bound
    json.dump(mapa, open(os.path.join(d, "gates_map.json"), "w"))
    res = {"gate": {"id": gid, "task_id": opt("--task"), "status": "pending", "run_id": bound, "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())}}
elif cmd == "gate-resolve":
    # como o Orca real: gate desconhecido ou de outro Run que o ligado ao terminal é "Gate not found"
    mapa = ler("gates_map.json", {})
    if opt("--id") not in mapa or mapa[opt("--id")] != bound:
        falha("Gate not found: " + str(opt("--id")))
    open(os.path.join(d, "gates.log"), "a").write(json.dumps(a) + "\\n")
    res = {"gate": {"id": opt("--id"), "status": "resolved"}}
elif cmd in ("worker-show", "worker-release"):
    ws = ler("workers.json", [])
    w = next((w for w in ws if w.get("dispatch", "ctx_" + w["handle"]) == opt("--dispatch")), None)
    if w is None:
        falha("Dispatch not found: " + str(opt("--dispatch")))
    if cmd == "worker-release" and os.path.exists(multi) and w["run"] != bound:
        print(json.dumps({"ok": False, "error": {"code": "consumer_fenced", "message": "consumer_fenced"}})); sys.exit(0)
    if cmd == "worker-show":
        # dispatch.dispatchedAt e worker.startOptions.launch.requested.model como no Orca real
        res = {"dispatch": {"status": w.get("status", "completed"), "dispatchedAt": w.get("desde", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - 600))),
                            "lastHeartbeatAt": None},
               "worker": {"worktreeId": ("repo_1::" + w["worktree"]) if w.get("worktree") else None,
                          "startOptions": {"agent": w.get("agente"), "launch": {"requested": {"model": w.get("modelo", "claude-sonnet-5-5"), "effort": w.get("effort", "high")}}}},
               "terminal": {"handle": w["handle"], "title": "x", **({"worktreePath": w["worktree"]} if w.get("worktree") else {})}}
    else:
        # worker-release: released | retained | release_pending | already_released (w["release"]); retained deixa o terminal aberto com o motivo
        open(os.path.join(d, "released.log"), "a").write(json.dumps(a) + "\\n")
        estado = w.get("release", "released")
        if estado in ("released", "retained"):
            w["terminal"] = estado
        if estado == "retained":
            w["reason"], w["ownership"] = w.get("release_reason"), w.get("release_ownership", w.get("ownership", "owned"))
        json.dump(ws, open(os.path.join(d, "workers.json"), "w"))
        res = {"dispatchId": opt("--dispatch"), "state": estado, "processAction": "none"}
elif cmd == "worker-stop":
    # como o Orca real (conferido em 30/09): o dispatch vira failed, o terminal fica retained (o pty morre) e a task blocked; a worktree não é tocada
    ws = ler("workers.json", [])
    w = next((w for w in ws if w.get("dispatch", "ctx_" + w["handle"]) == opt("--dispatch")), None)
    if w is None:
        falha("Dispatch not found: " + str(opt("--dispatch")))
    if os.path.exists(multi) and w["run"] != bound:
        print(json.dumps({"ok": False, "error": {"code": "consumer_fenced", "message": "consumer_fenced"}})); sys.exit(0)
    open(os.path.join(d, "stopped.log"), "a").write(json.dumps(a) + "\\n")
    w["status"], w["terminal"] = "failed", "retained"
    json.dump(ws, open(os.path.join(d, "workers.json"), "w"))
    ts = ler("tasks_%s.json" % w["run"], [])
    for t in ts:
        if t["id"] == w.get("task", "task_" + w["handle"]):
            t["status"], t["dispatch_id"] = "blocked", None
    json.dump(ts, open(os.path.join(d, "tasks_%s.json" % w["run"]), "w"))
    res = {"dispatchId": opt("--dispatch"), "state": "stopped", "alreadySettled": False, "processAction": "closed_agent_terminal"}
elif cmd == "worker-start":
    # como o Orca real: precisa do coordenador ligado ao Run; cria task + dispatch + terminal do agente e devolve runId/taskId/dispatchId/effects
    if os.path.exists(multi):  # a ligação vale na hora em que o worker-start termina de demorar, não na em que começou
        _b = json.load(open(multi))
        bound = next((r_ for r_, v in _b.items() if v["handle"] == os.environ.get("ORCA_TERMINAL_HANDLE")), None)
    if bound is None or (run and run != bound):
        print(json.dumps({"ok": False, "error": {"code": "consumer_fenced", "message": "consumer_fenced"}})); sys.exit(0)
    open(os.path.join(d, "started.log"), "a").write(json.dumps(a) + "\\n")
    open(os.path.join(d, "started-env.log"), "a").write(json.dumps({k: os.environ.get(k) for k in ("GIT_TERMINAL_PROMPT", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0", "GIT_CONFIG_KEY_1", "GIT_CONFIG_VALUE_1")}) + "\\n")
    if os.environ.get("FAKE_FAIL_START_MODEL") and os.environ["FAKE_FAIL_START_MODEL"] == opt("--model"):
        falha("model not available: " + str(opt("--model")))
    ws = ler("workers.json", [])
    n = len(ws) + 1
    tid = opt("--task") or "task_novo%d" % n  # --task despacha uma task que já existe (a do ticket) em vez de criar outra
    novo = {"handle": "term_novo%d" % n, "run": run or bound, "task": tid, "status": "dispatched"}
    if opt("--retry-of"):  # como o Orca real (30/09): a retentativa reaproveita a worktree que o --worktree nomeia e sobe com o perfil pedido
        novo.update({"worktree": opt("--worktree", "").split("::", 1)[-1], "modelo": opt("--model"), "effort": opt("--effort"), "agente": opt("--agent")})
    ws.insert(0, novo)
    json.dump(ws, open(os.path.join(d, "workers.json"), "w"))
    ts = ler("tasks_%s.json" % (run or bound), [])
    if opt("--task"):
        for t in ts:
            if t["id"] == tid:
                t["status"], t["dispatch_id"] = "dispatched", "ctx_term_novo%d" % n
    else:
        ts.append({"id": tid, "task_title": opt("--task-title"), "status": "dispatched", "dispatch_id": "ctx_term_novo%d" % n,
                   "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())})
    json.dump(ts, open(os.path.join(d, "tasks_%s.json" % (run or bound)), "w"))
    if os.environ.get("FAKE_INICIO") not in ("nunca", "depois_do_enter"):
        turno_comeca("ctx_term_novo%d" % n)
    res = {"runId": run or bound, "taskId": tid, "dispatchId": "ctx_term_novo%d" % n, "state": "ready", "stage": "input_accepted",
           "effects": [{"kind": "worktree", "action": "reused", "id": "wt"}, {"kind": "terminal", "role": "agent", "action": "created", "id": "term_novo%d" % n},
                       {"kind": "dispatch_input", "role": "agent", "id": "term_novo%d" % n, "state": "accepted"}]}
elif cmd in ("task-create", "task-update"):
    # como o Orca real (conferido num Run de teste em 29/09): só o Run ligado escreve (consumer_fenced); task-create devolve {task} com status
    # pending se há deps e ready se não; task-update muda o status e devolve {task}; id desconhecido é erro
    if bound is None or (run and run != bound):
        print(json.dumps({"ok": False, "error": {"code": "consumer_fenced", "message": "consumer_fenced"}})); sys.exit(0)
    open(os.path.join(d, ("created" if cmd == "task-create" else "updated") + ".log"), "a").write(json.dumps(a) + "\\n")
    alvo = run or bound
    ts = ler("tasks_%s.json" % alvo, [])
    if cmd == "task-create":
        deps = json.loads(opt("--deps", "[]"))
        ts.append({"id": "task_tk%d" % (len(ts) + 1), "task_title": opt("--task-title"), "spec": opt("--spec"), "deps": json.dumps(deps),
                   "status": "pending" if deps else "ready", "dispatch_id": None, "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())})
        t = ts[-1]
    else:
        t = next((t for t in ts if t["id"] == opt("--id")), None)
        if t is None:
            falha("Task not found: " + str(opt("--id")))
        t["status"] = opt("--status")
        t["result"] = opt("--result")
    json.dump(ts, open(os.path.join(d, "tasks_%s.json" % alvo), "w"))
    res = {"task": t}
elif cmd == "reply":
    # como o Orca real (visto em 29/09): só o terminal ligado ao Run da mensagem responde; o gerente em outro Run recebe consumer_fenced
    if bound is None or (run and run != bound):
        print(json.dumps({"ok": False, "error": {"code": "consumer_fenced", "message": "consumer_fenced"}})); sys.exit(0)
    open(os.path.join(d, "replied.log"), "a").write(json.dumps(a) + "\\n")
    res = {"message": {"id": "msg_r1"}}
elif cmd == "send":
    if os.environ.get("FAKE_FAIL") == "send" or bound is None or (run and run != bound):  # consumer_fenced: só o Run ligado recebe send
        print(json.dumps({"ok": False, "error": {"code": "consumer_fenced", "message": "consumer_fenced"}})); sys.exit(0)
    open(os.path.join(d, "sent.log"), "a").write(json.dumps(a) + "\\n")
    res = {"message": {"id": "msg_9"}}
else:
    falha("comando " + cmd)
print(json.dumps({"ok": True, "result": res}))
'''


class Amb:
    """Diretório temporário com ORQ_HOME, Orca falso e pendencias.json."""

    def __init__(self, run="run_a", **env):
        self.tmp = tempfile.TemporaryDirectory()
        t = self.tmp.name
        self.home, self.fake = os.path.join(t, "orq"), os.path.join(t, "fake")
        os.makedirs(self.fake)
        self.bin = os.path.join(t, "orca")
        with open(self.bin, "w") as f:
            f.write(FAKE)
        os.chmod(self.bin, 0o755)
        self.env = {**os.environ, "ORQ_HOME": self.home, "ORQ_ORCA": self.bin, "FAKE_DIR": self.fake, "ORQ_NO_BG": "1", "ORQ_LIMPAR": "/nao/existe/limpar.py",
                    "ORQ_LOG": os.path.join(t, "orq.log"), "ORQ_PENDENCIAS": os.path.join(t, "pendencias.json"),
                    "ORQ_ISSUES": os.path.join(t, "issues"), "ORQ_MAPA": os.path.join(t, "desenho.md"),
                    "ORCA_TERMINAL_HANDLE": "term_coord", "ORQ_ORCA_TIMEOUT": "10", "ORQ_STEER_ESPERA_S": "0", "ORQ_HUD_CACHE": os.path.join(t, "hud"), "ORQ_CODEX_CONFIG": os.path.join(t, "codex-config.toml"), "ORQ_CODEX_HOOKS": os.path.join(t, "hooks.json"), "ORQ_MAQUINA_LEITURA": os.path.join(t, "maquina-leitura.json"), "ORQ_OCIOSO_MS": "50", "ORQ_AVISO_GAP_S": "0", "ORQ_INICIO_ESPERA_S": "0.3", **env}
        self.set("run.json", {"id": run} if run else None)
        self.maquina()
        self.set("../pendencias.json", {"itens": [{"id": "freio-prod", "tipo": "decisao"}, {"id": "avisar-x", "tipo": "avisar"}]})

    def set(self, nome, dado):
        with open(os.path.join(self.fake, nome), "w") as f:
            json.dump(dado, f)

    def maquina(self, mem_livre_mb=16000, livre_pct=60, carga=2.0, processos=None):
        """A leitura simulada da máquina (ORQ_MAQUINA_LEITURA): o padrão dos testes é uma máquina folgada, para não depender da carga real de quem roda a suíte.
        `processos`: a amostra de processos simulada ([{pid, ppid, cpu, rss (KB), args}]); sem ela a origem da carga fica desconhecida."""
        with open(self.env["ORQ_MAQUINA_LEITURA"], "w") as f:
            json.dump({"mem_livre_mb": mem_livre_mb, "livre_pct": livre_pct, "carga": carga, "ncpu": 12, "rss_mb": {"claude": 900, "codex": 0, "node": 1500, "docker": 2000},
                       **({"processos": processos} if processos else {})}, f)

    def orq(self, *args, stdin=None, cwd=None, **env):
        return subprocess.run([sys.executable, ORQ, *args], input=stdin, capture_output=True, text=True, cwd=cwd,
                              env={**self.env, **env}, timeout=30)

    def prompt(self, texto, **env):
        return self.orq("hook", "prompt", stdin=json.dumps({"prompt": texto, "session_id": "abcdef123456"}), **env)

    def events(self):
        return orq_mod_events(self.home)

    def caixa(self, *msgs, run="run_a"):
        """Acrescenta mensagens ao mailbox do Orca falso: cada uma é (type, dict do payload)."""
        p = os.path.join(self.fake, "mailbox.json")
        cx = json.load(open(p)) if os.path.exists(p) else {"n": 0, "msgs": []}
        for tipo, payload, *prio in msgs:
            i = len(cx["msgs"]) + 1
            cx["msgs"].append({"id": f"msg_{i}", "run_id": run, "to_handle": "run:" + run, "type": tipo, "subject": tipo, "priority": (prio or ["normal"])[0],
                               "payload": json.dumps(payload), "created_at": f"2026-09-29T17:3{i % 10}:00Z", "delivered_at": None, "status": "unread"})
        json.dump(cx, open(p, "w"))

    def estados(self):
        """{id: status} das mensagens do mailbox falso (unread, out ou acked)."""
        p = os.path.join(self.fake, "mailbox.json")
        return {m["id"]: m["status"] for m in json.load(open(p))["msgs"]} if os.path.exists(p) else {}

    def log(self):
        try:
            return open(self.env["ORQ_LOG"]).read()
        except OSError:
            return ""


def orq_mod_events(home):
    try:
        return [json.loads(x) for x in open(os.path.join(home, "events.jsonl"))]
    except OSError:
        return []


def test_origem_cinco_tipos():
    o = orq_mod.origem
    assert o("<task-notification>\n<task-id>x</task-id>merged</task-notification>") == "notificacao"
    assert o("You have 2 orchestration messages. Run `orca orchestration check`") == "orca"
    assert o("<command-name>/clear</command-name>") == "comando"
    assert o("<local-command-stdout>ok</local-command-stdout>") == "comando"
    assert o("/compact") == "comando"
    assert o("This session is being continued from a previous conversation") == "resumo"
    assert o("Please carry out this task from my Orca coordinator by following the brief") == "despacho"
    assert o("cria a task do ticket 03") == "usuario"
    assert o("  \n<task-notification>") == "notificacao"  # espaço à esquerda não engana
    assert o("") == "usuario" and o(None) == "usuario"


def test_so_usuario_vira_entrada_e_injeta():
    a = Amb()
    for texto in ("<task-notification>x</task-notification>", "You have 1 orchestration message",
                  "<command-name>/x</command-name>", "This session is being continued"):
        r = a.prompt(texto)
        assert r.returncode == 0 and r.stdout == "", (texto, r)
    assert a.events() == []
    r = a.prompt("cria a task do ticket 03")
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["hookEventName"] == "UserPromptSubmit" and "entrada e1 (usuário)" in out["additionalContext"]
    (e,) = a.events()
    assert e["tipo"] == "entrada" and e["origem"] == "usuario" and e["sessao"] == "abcdef12" and e["id"] == "e1"


def test_worker_run_null_sai_sem_efeito():
    a = Amb(run=None)
    r = a.prompt("qualquer coisa")
    assert (r.returncode, r.stdout) == (0, ""), r
    r = a.orq("hook", "stop", stdin=json.dumps({"stop_hook_active": False}))
    assert (r.returncode, r.stdout) == (0, ""), r
    assert a.events() == [] and not os.path.exists(a.home), "worker não pode gravar nada"
    assert a.log() == "", "worker não é erro"


def test_binding_perdido_avisa_em_vez_de_calar():
    a = Amb(run="run_a")
    a.prompt("primeira")
    assert json.load(open(os.path.join(a.home, "cursor.json")))["runs"] == {"abcdef123456": "run_a"}
    a.set("run.json", None)  # hibernação ou resume: o Run sumiu
    r = a.prompt("segunda")
    ctx = json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    assert "binding perdido: rode run-use --id run_a" in ctx
    assert [e["tipo"] for e in a.events()] == ["entrada", "binding_perdido"] and a.events()[-1]["run"] == "run_a"
    assert Amb(run=None).prompt("worker").stdout == "", "sessão que nunca teve Run segue worker"


def test_excecao_vira_exit_0_e_log():
    a = Amb()
    r = a.orq("hook", "prompt", stdin="isto não é json")
    assert (r.returncode, r.stdout) == (0, "") and "hook prompt" in a.log()
    a = Amb(FAKE_CRASH="1")
    r = a.prompt("oi")
    assert (r.returncode, r.stdout) == (0, "") and "hook prompt" in a.log()
    r = a.orq("hook", "stop", stdin="{}")
    assert r.returncode == 0 and a.log().count("hook") == 2
    assert a.events() == []


def test_orca_lento_falha_aberto_pelo_timeout_da_chamada():
    a = Amb(FAKE_SLEEP="6", ORQ_ORCA_TIMEOUT="2.5")
    t = time.time()
    r = a.prompt("oi")
    assert r.returncode == 0 and r.stdout == "" and time.time() - t < 4.5
    assert "hook prompt: TimeoutExpired" in a.log()  # o alarme de 3 s tem os testes do achado 8


def test_intake_recusa_ref_inexistente():
    a = Amb()
    a.prompt("faz isso")
    a.set("tasks_run_a.json", [{"id": "task_1", "status": "ready", "created_at": "2026-09-18T00:00:00Z"}])
    r = a.orq("intake", "e1", "tarefa", "task_inexistente")
    assert r.returncode == 1 and "task_inexistente" in r.stderr
    r = a.orq("intake", "e1", "steer", "task_inexistente")
    assert r.returncode == 1
    r = a.orq("intake", "e1", "tarefa")
    assert r.returncode == 1 and "pede o id" in r.stderr
    r = a.orq("intake", "e1", "pend", "nao-existe")
    assert r.returncode == 1 and "pendencias.json" in r.stderr
    r = a.orq("intake", "e1", "decisao", "nao-existe")
    assert r.returncode == 1
    r = a.orq("intake", "e99", "conversa")
    assert r.returncode == 1 and "e99" in r.stderr
    r = a.orq("intake", "e1", "inventado")
    assert r.returncode == 1
    assert [e for e in a.events() if e["tipo"] == "intake"] == [], "recusa não grava"
    # os que existem passam
    assert a.orq("intake", "e1", "tarefa", "task_1").returncode == 0
    a.prompt("outra")
    assert a.orq("intake", "e2", "decisao", "freio-prod").returncode == 0
    a.prompt("e outra")
    assert a.orq("intake", "e3", "conversa").returncode == 0
    a.prompt("mais uma")
    r = a.orq("intake", "e4", "descartado", "--nota", "duplicada")
    assert r.returncode == 0 and json.loads(r.stdout)["nota"] == "duplicada"
    ints = [e for e in a.events() if e["tipo"] == "intake"]
    assert [i["efeito"] for i in ints] == ["tarefa", "decisao", "conversa", "descartado"]
    assert ints[0]["ref"] == "task_1" and ints[0]["run"] == "run_a"


def test_intake_com_run_de_outro_run_avisa():
    a = Amb()
    a.prompt("faz isso")
    a.set("tasks_run_b.json", [{"id": "task_b", "status": "ready", "created_at": "2026-09-18T00:00:00Z"}])
    r = a.orq("intake", "e1", "tarefa", "task_b", "--run", "run_b")
    assert r.returncode == 0 and "run-use" in r.stderr, r
    assert [e for e in a.events() if e["tipo"] == "intake"][0]["run"] == "run_b"
    r = a.orq("intake", "e1", "tarefa", "task_b", "--run", "run_c")
    assert r.returncode == 1 and "run_c" in r.stderr


def test_contagem_de_entradas_sem_efeito():
    a = Amb()
    for t in ("um", "dois", "três"):
        a.prompt(t)
    ev = a.events()
    assert [e["id"] for e in orq_mod.abertas(ev)] == ["e1", "e2", "e3"]
    a.orq("intake", "e2", "conversa")
    assert [e["id"] for e in orq_mod.abertas(a.events())] == ["e1", "e3"]
    # o Stop em modo aviso: systemMessage, grava gate_aviso e nunca bloqueia, nem com stop_hook_active
    for ativo in (False, True):
        r = a.orq("hook", "stop", stdin=json.dumps({"stop_hook_active": ativo, "session_id": "s"}))
        out = json.loads(r.stdout)
        assert r.returncode == 0 and "decision" not in out and "continue" not in out
        assert "2 entrada(s) sem efeito: e1 ('um'), e3 ('três')" in out["systemMessage"]
    avisos = [e for e in a.events() if e["tipo"] == "gate_aviso"]
    assert len(avisos) == 2 and avisos[0]["abertas"] == ["e1", "e3"]
    for e in ("e1", "e3"):
        a.orq("intake", e, "conversa")
    r = a.orq("hook", "stop", stdin="{}")
    assert (r.returncode, r.stdout) == (0, "")


def test_resumo_no_maximo_5_linhas():
    a = Amb()
    a.set("runs.json", [{"id": "run_a"}])
    a.set("tasks_run_a.json", [
        {"id": "task_4bdad9c61375", "status": "ready", "task_title": "Ticket 01\ncom quebra de linha", "created_at": "2026-09-18T00:00:00Z"},
        {"id": "task_2", "status": "dispatched", "created_at": "2026-09-28T00:00:00Z"},
        {"id": "task_3", "status": "blocked", "created_at": "2026-09-28T00:00:00Z"},
    ])
    assert a.orq("ingest", "--refresh").returncode == 0
    for i in range(12):
        a.prompt(f"mensagem longa número {i} " + "x" * 300)
    r = a.prompt("a última\n\nem várias\nlinhas")
    ctx = json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    linhas = ctx.splitlines()
    assert 1 <= len(linhas) <= 5, ctx
    assert "entrada e13 (usuário)" in linhas[0] and "+9" in linhas[0]
    assert "backlog 1 (task_4bda… 'Ticket 01 com quebra de linha'," in linhas[1] and "rodando 1, bloqueado 1, gates 0" in linhas[1]
    assert "Com você: 2 (1 decisões)" in linhas[2]
    assert len(a.orq("status").stdout.strip().splitlines()) <= 5
    # sem cache, sem pendências, sem entradas: ainda cabe
    assert len(orq_mod.resumo([], None, None).splitlines()) <= 5


def test_idade_aceita_created_at_sem_fuso():
    from datetime import datetime, timezone
    agora = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
    assert orq_mod.dias_desde("2026-09-18 12:00:00", agora) == 11  # formato real do Orca: UTC sem fuso
    assert orq_mod.dias_desde("2026-09-18T12:00:00Z", agora) == 11


def test_refresh_aberto_le_todos_os_runs():
    a = Amb()
    a.set("runs.json", [{"id": "run_a"}, {"id": "run_b"}])
    a.set("tasks_run_a.json", [{"id": "t1", "status": "ready", "spec": "x", "created_at": "2026-09-27T00:00:00Z"}])
    a.set("tasks_run_b.json", [{"id": "t2", "status": "pending", "task_title": "velha", "created_at": "2026-09-10 00:00:00"},
                               {"id": "t3", "status": "completed", "created_at": "2026-09-01T00:00:00Z"},
                               {"id": "t4", "status": "blocked", "task_title": "b", "created_at": "2026-09-01T00:00:00Z"}])
    a.set("gates_run_b.json", [{"id": "g1", "task_id": "t4"}])
    r = a.orq("ingest", "--refresh")
    assert r.returncode == 0, r
    ab = json.load(open(os.path.join(a.home, "aberto.json")))
    assert [i["id"] for i in ab["backlog"]] == ["t2", "t1"], "mais velha primeiro"
    assert [i["id"] for i in ab["bloqueado"]] == ["t4"] and len(ab["gates"]) == 1 and ab["rodando"] == 0


def test_hook_prompt_dispara_refresh_em_segundo_plano():
    a = Amb()
    a.env.pop("ORQ_NO_BG")
    a.set("runs.json", [{"id": "run_a"}])
    a.set("tasks_run_a.json", [{"id": "t1", "status": "ready", "spec": "x", "created_at": "2026-09-27T00:00:00Z"}])
    r = a.prompt("oi")
    assert "cache ainda não existe" in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    for _ in range(50):
        if os.path.exists(os.path.join(a.home, "aberto.json")):
            break
        time.sleep(0.1)
    assert json.load(open(os.path.join(a.home, "aberto.json")))["backlog"][0]["id"] == "t1"
    assert "backlog 1" in json.loads(a.prompt("de novo").stdout)["hookSpecificOutput"]["additionalContext"]


def test_ids_sequenciais_com_flock():
    a = Amb()
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(lambda i: a.prompt(f"m{i}"), range(8)))
    ids = sorted(int(e["id"][1:]) for e in a.events() if e["tipo"] == "entrada")
    assert ids == list(range(1, 9)), ids


def _limpar(prompt, amb=None):
    """Roda o hook de limpeza com HOME falso e o Orca falso do `amb` (coordenador por padrão); devolve o stdout.
    O stub só existe para o disparo não tocar em nada real."""
    amb = amb or Amb()
    with tempfile.TemporaryDirectory() as t:
        os.makedirs(f"{t}/.claude/logs")
        os.makedirs(f"{t}/.claude/scripts")
        os.makedirs(f"{t}/.claude/orq")
        os.symlink(ORQ, f"{t}/.claude/orq/orq.py")
        open(f"{t}/.claude/scripts/limpar-mergeados.py", "w").write("")
        subprocess.run(["git", "init", "-q", t], check=True)
        r = subprocess.run([sys.executable, LIMPAR], input=json.dumps({"prompt": prompt, "cwd": t, "session_id": "abcdef123456"}),
                           capture_output=True, text=True, env={**amb.env, "HOME": t}, timeout=30)
        assert r.returncode == 0, r.stderr
        return r.stdout


def test_limpeza_nao_dispara_com_notificacao_nem_pergunta():
    assert _limpar("<task-notification>\n<result>o PR foi merged</result></task-notification>") == ""
    assert _limpar("You have 1 orchestration message: merged") == ""
    assert _limpar("This session is being continued: merged") == ""
    assert _limpar("esse já foi merged?") == ""
    assert _limpar("o 1163 já foi merged?  \n") == ""
    assert _limpar("bom dia") == ""
    # controle positivo: sem ele os "" acima não provariam nada
    assert "Limpeza de branches mergeadas" in _limpar("1163 merged")
    assert "Limpeza de branches mergeadas" in _limpar("pode seguir, está merged.")


def test_monta_aberto_de_todos_os_runs_com_deps_e_cancelada():
    from datetime import datetime, timezone
    agora = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
    def t(id, status, dias, deps="[]", result=None, titulo=None):
        d = f"2026-09-{29 - dias:02d} 11:00:00"
        return {"id": id, "status": status, "created_at": d, "deps": deps, "result": result, "task_title": titulo or id}
    r1 = {"id": "run_1", "objective": "Frente A"}
    r2 = {"id": "run_2", "objective": "Frente B"}
    tasks1 = [t("t1", "ready", 11, titulo="Ticket 01"), t("t2", "pending", 5, deps='["t1"]'), t("t3", "pending", 4, deps='["t9"]'),
              t("t4", "blocked", 2), t("t5", "dispatched", 0), t("t6", "completed", 20, result='{"cancelado":"duplicata"}'),
              t("t9", "completed", 9)]
    gates1 = [{"id": "g1", "task_id": "t4", "question": "Sobe o freio?", "created_at": "2026-09-27 10:00:00"}]
    ab = orq_mod.monta_aberto([(r1, tasks1, gates1), (r2, [t("u1", "ready", 3)], [])], agora)
    assert [b["id"] for b in ab["backlog"]] == ["t1", "t2", "t3", "u1"], ab["backlog"]  # mais velho primeiro
    por_id = {b["id"]: b for b in ab["backlog"]}
    assert por_id["t1"]["dias"] == 11 and por_id["t1"]["objetivo"] == "Frente A" and por_id["t1"]["run"] == "run_1"
    assert por_id["t2"]["deps_faltando"] == ["t1"]
    assert por_id["t3"]["deps_faltando"] == []  # t9 concluída
    assert ab["rodando"] == 1
    assert [b["id"] for b in ab["bloqueado"]] == ["t4"]
    assert ab["gates"] == [{"id": "g1", "run": "run_1", "task": "t4", "objetivo": "Frente A", "pergunta": "Sobe o freio?", "dias": 2}]
    todos = json.dumps(ab)
    assert "t6" not in todos  # cancelada (completed) não é backlog nem bloqueio


# ---------- fatia 3: orq pend + orq hook ask ----------

def _pend(a):
    return json.load(open(a.env["ORQ_PENDENCIAS"]))


def _pergunta(header, opcoes, multi=False, pergunta=None):
    return {"question": pergunta or f"pergunta de {header}?", "header": header, "multiSelect": multi,
            "options": [{"label": l, "description": d} for l, d in opcoes]}


def _ask(qs, respostas, onde="tool_response"):
    """Payload do PostToolUse de AskUserQuestion; respostas indexadas pelo texto da pergunta (formato do toolUseResult real)."""
    ev = {"session_id": "abcdef123456", "hook_event_name": "PostToolUse", "tool_name": "AskUserQuestion",
          "tool_input": {"questions": qs}, "tool_use_id": "toolu_x"}
    if onde == "tool_response":
        ev["tool_response"] = {"questions": qs, "answers": respostas, "annotations": {}}
    else:
        ev["tool_input"]["answers"] = respostas
    return json.dumps(ev)


def test_pend_add_done_escrita_atomica_e_evento():
    a = Amb()
    os.remove(a.env["ORQ_PENDENCIAS"])
    r = a.orq("pend", "add", "--id", "avisar-alice", "--tipo", "avisar", "--titulo", "Avisar o Alice", "--espera", "Alice", "--frente", "cache")
    assert r.returncode == 0, r
    d = _pend(a)
    (item,) = d["itens"]
    assert list(item) == ["id", "tipo", "titulo", "frente", "desde", "espera"] and item["espera"] == "Alice"
    assert len(item["desde"]) == 10 and "atualizadoEm" in d
    assert a.orq("pend", "add", "--id", "outra", "--tipo", "acao", "--titulo", "X", "--detalhe", "d", "--link", "l", "--comando", "c").returncode == 0
    assert [i["id"] for i in _pend(a)["itens"]] == ["avisar-alice", "outra"]
    assert not [f for f in os.listdir(os.path.dirname(a.env["ORQ_PENDENCIAS"])) if f.startswith("tmp")], "tmp sobrou"
    r = a.orq("pend", "done", "avisar-alice", "--resposta", "avisei")
    assert r.returncode == 0 and [i["id"] for i in _pend(a)["itens"]] == ["outra"]
    evs = [e for e in a.events() if e["tipo"] == "pend"]
    assert [(e["op"], e["pend"]) for e in evs] == [("add", "avisar-alice"), ("add", "outra"), ("done", "avisar-alice")]
    assert evs[2]["resposta"] == "avisei"


def test_pend_recusas():
    a = Amb()
    antes = open(a.env["ORQ_PENDENCIAS"]).read()
    r = a.orq("pend", "add", "--id", "id-com-mais-de-12", "--tipo", "decisao", "--titulo", "X")
    assert r.returncode == 1 and "12" in r.stderr
    assert a.orq("pend", "add", "--id", "doze-chars-1", "--tipo", "decisao", "--titulo", "X").returncode == 0, "12 cabe"
    assert a.orq("pend", "add", "--id", "id-longo-mas-acao-ok", "--tipo", "acao", "--titulo", "X").returncode == 0, "limite só vale para decisão"
    r = a.orq("pend", "add", "--id", "freio-prod", "--tipo", "acao", "--titulo", "duplicado")
    assert r.returncode == 1 and "já existe" in r.stderr
    assert a.orq("pend", "add", "--id", "x", "--tipo", "inventado", "--titulo", "X").returncode == 2
    assert a.orq("pend", "add", "--id", " ", "--tipo", "acao", "--titulo", "X").returncode == 1
    r = a.orq("pend", "done", "nao-existe")
    assert r.returncode == 1 and "nao-existe" in r.stderr
    assert len([e for e in a.events() if e["tipo"] == "pend"]) == 2, "recusa não grava evento"
    # arquivo corrompido nunca é sobrescrito
    open(a.env["ORQ_PENDENCIAS"], "w").write("{quebrado")
    assert a.orq("pend", "add", "--id", "y", "--tipo", "acao", "--titulo", "X").returncode == 1
    assert open(a.env["ORQ_PENDENCIAS"]).read() == "{quebrado"
    assert antes  # o arquivo de partida existia


def test_pend_add_concorrente_nao_perde_item():
    a = Amb()
    with ThreadPoolExecutor(6) as ex:
        list(ex.map(lambda i: a.orq("pend", "add", "--id", f"c{i}", "--tipo", "acao", "--titulo", f"t{i}"), range(6)))
    ids = {i["id"] for i in _pend(a)["itens"]}
    assert {f"c{i}" for i in range(6)} <= ids and len(ids) == 8


def test_ask_header_de_pendencia_fecha_e_guarda_a_resposta():
    a = Amb()
    q = [_pergunta("freio-prod", [("Teto por pod (Recomendado)", "x"), ("Sem freio", "y")])]
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Teto por pod (Recomendado)"}))
    assert (r.returncode, r.stdout) == (0, ""), r
    assert [i["id"] for i in _pend(a)["itens"]] == ["avisar-x"], "decisão respondida fecha"
    res = [e for e in a.events() if e["tipo"] == "resposta"]
    assert res[0]["header"] == "freio-prod" and res[0]["resposta"] == "Teto por pod (Recomendado)" and res[0]["pergunta"] == q[0]["question"]
    done = [e for e in a.events() if e["tipo"] == "pend" and e["op"] == "done"]
    assert done[0]["pend"] == "freio-prod" and done[0]["resposta"] == "Teto por pod (Recomendado)"


def test_ask_texto_livre_guarda_o_texto_e_nao_fecha_a_decisao_answers_em_tool_input():
    a = Amb()
    q = [_pergunta("freio-prod", [("A", "x"), ("B", "y")])]
    texto = "nenhum dos dois, vamos ver com o time de dados"
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: texto}, onde="tool_input"))
    assert [i["id"] for i in _pend(a)["itens"]] == ["freio-prod", "avisar-x"], "review 2, M3: texto livre não é decisão"
    (res,) = [e for e in a.events() if e["tipo"] == "resposta"]
    assert res["resposta"] == texto and res["livre"] is True and "fechou" not in res
    assert not [e for e in a.events() if e["tipo"] == "pend"]
    assert 'resposta livre em freio-prod: se decidiu, feche com orq pend done freio-prod --resposta "<o que foi decidido>"' in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]


def test_ask_header_desconhecido_so_registra_e_sem_answers_loga():
    a = Amb()
    q = [_pergunta("outra", [("A", "x")])]
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "A"}))
    assert len(_pend(a)["itens"]) == 2 and [e["header"] for e in a.events() if e["tipo"] == "resposta"] == ["outra"]
    n = len(a.events())
    a.orq("hook", "ask", stdin=_ask(q, {}))  # o usuário dispensou a pergunta
    assert len(a.events()) == n and "sem answers" in a.log()


def test_ask_ja_fez_fecha_as_marcadas_pelo_id_da_descricao():
    a = Amb()
    for i in ("alice", "bob", "dana"):
        a.orq("pend", "add", "--id", i, "--tipo", "avisar", "--titulo", i)
    q = [_pergunta("ja-fez", [("Alice: schemaVersion", "[alice] Avisar o Alice"), ("Bob: maxmemory", "[bob] Avisar o Bob"),
                              ("Dana: widgets", "[dana] Avisar o Dana"), ("Sem descrição de id", "nada")], multi=True)]
    resp = "Alice: schemaVersion, Dana: widgets, \"já falei com o Bob também, mas por email\""
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: resp}))
    assert r.returncode == 0, r
    ids = [i["id"] for i in _pend(a)["itens"]]
    assert "alice" not in ids and "dana" not in ids and "bob" in ids, ids
    assert [e["op"] for e in a.events() if e["tipo"] == "pend" and e["op"] == "done"] == ["done", "done"]
    (res,) = [e for e in a.events() if e["tipo"] == "resposta"]
    assert res["resposta"] == resp and sorted(res["fechou"]) == ["alice", "dana"]
    # id que já saiu não derruba o hook
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Alice: schemaVersion"}))
    assert a.log() == ""


def test_marcadas_prefere_o_label_mais_longo():
    ops = [{"label": "Alice"}, {"label": "Alice: schemaVersion"}]
    assert orq_mod.marcadas("Alice: schemaVersion", ops) == (["Alice: schemaVersion"], "")
    assert orq_mod.marcadas("Alice, Alice: schemaVersion, texto livre", ops) == (["Alice: schemaVersion", "Alice"], "texto livre")


def test_resposta_suspeita_colada_a_notificacao_do_orca_nao_fecha():
    a = Amb()
    q = [_pergunta("freio-prod", [("Teto por pod (Recomendado)", "x"), ("Sem freio", "y")])]
    a.prompt("You have 1 orchestration message. Run `orca orchestration check`")  # chega e o hook do prompt marca
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Teto por pod (Recomendado)"}))
    assert r.returncode == 0
    assert "resposta suspeita em freio-prod: confirme" in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    assert [i["id"] for i in _pend(a)["itens"]] == ["freio-prod", "avisar-x"], "suspeita não fecha"
    tipos = [e["tipo"] for e in a.events()]
    assert "resposta_suspeita" in tipos and "resposta" not in tipos
    assert not [e for e in a.events() if e["tipo"] == "pend" and e["op"] == "done"]
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "resposta suspeita em freio-prod: confirme" in ctx and len(ctx.splitlines()) <= 5
    # a pergunta refeita e respondida de verdade (chegada já velha) fecha e limpa o aviso
    cur = json.load(open(os.path.join(a.home, "cursor.json")))
    cur["chegada"]["t"] -= 60
    json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sem freio"}))
    assert [i["id"] for i in _pend(a)["itens"]] == ["avisar-x"]
    assert "suspeita" not in json.loads(a.prompt("depois").stdout)["hookSpecificOutput"]["additionalContext"]


def test_resposta_suspeita_task_notification_e_texto_de_notificacao():
    a = Amb()
    q = [_pergunta("freio-prod", [("A", "x")])]
    a.prompt("<task-notification><task-id>x</task-id></task-notification>")
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "A"}))
    assert "suspeita" in r.stdout and len(_pend(a)["itens"]) == 2
    # resposta velha (>= 3 s) não é suspeita; e o texto digitado que é o próprio aviso do Orca é suspeito sem precisar de timing
    cur = json.load(open(os.path.join(a.home, "cursor.json")))
    cur["chegada"]["t"] -= 10
    json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "You have 2 orchestration messages. Run `orca orchestration check`"}))
    assert "suspeita" in r.stdout and len(_pend(a)["itens"]) == 2
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "A"}))
    assert (r.returncode, r.stdout) == (0, "") and [i["id"] for i in _pend(a)["itens"]] == ["avisar-x"]


def test_chegada_de_usuario_ou_comando_nao_marca_e_entrada_nao_muda():
    a = Amb()
    a.prompt("bom dia")
    a.prompt("<command-name>/clear</command-name>")
    assert "chegada" not in json.load(open(os.path.join(a.home, "cursor.json")))
    a.prompt("You have 3 orchestration messages")
    assert json.load(open(os.path.join(a.home, "cursor.json")))["chegada"]["origem"] == "orca"
    assert [e["id"] for e in orq_mod.abertas(a.events())] == ["e1"], "notificação continua não virando entrada"


def test_ask_worker_e_excecao_saem_com_exit_0():
    a = Amb(run=None)
    q = [_pergunta("freio-prod", [("A", "x")])]
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "A"}))
    assert (r.returncode, r.stdout) == (0, "") and len(_pend(a)["itens"]) == 2 and a.events() == []
    a = Amb()
    r = a.orq("hook", "ask", stdin="lixo")
    assert (r.returncode, r.stdout) == (0, "") and "hook ask" in a.log()
    open(a.env["ORQ_PENDENCIAS"], "w").write("{quebrado")
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "A"}))
    assert r.returncode == 0 and open(a.env["ORQ_PENDENCIAS"]).read() == "{quebrado"


# ---------- fatia 4: orq ingest ----------

FIX = os.path.join(AQUI, "fixtures")
REPO_FIX = os.path.join(FIX, "repo")  # relatórios de exemplo, no formato dos que as automations gravam em .scratch
REAL_AUD = os.path.join(REPO_FIX, ".scratch", "auditoria-diaria", "2026-09-29.md")
REAL_CACHE = os.path.join(REPO_FIX, ".scratch", "acompanhamento-cache", "2026-09-29-manha.md")
DESDE_CEDO = "2026-09-29T00:00:00Z"


def _fix(nome):
    d = json.load(open(os.path.join(FIX, nome)))
    if nome == "automations_runs.json":  # os relatórios citados ficam nas fixtures, não no repositório do projeto
        for r in d["result"]["runs"]:
            if r.get("runContext"):
                r["runContext"]["path"] = REPO_FIX
    return d


def _ingest_env(a, runs=None, inbox=None, desde=DESDE_CEDO):
    """Põe as fixtures (ou cópias mutadas em memória) no Orca falso e planta o cursor de partida (None = primeira execução)."""
    a.set("automations_runs.json", runs if runs is not None else _fix("automations_runs.json"))
    a.set("inbox.json", inbox if inbox is not None else _fix("inbox.json"))
    if desde:
        os.makedirs(a.home, exist_ok=True)
        json.dump({"ingest": {"desde": desde, "inbox_seq": 0, "runs": []}}, open(os.path.join(a.home, "cursor.json"), "w"))


def _entradas(a, origem):
    return [e for e in a.events() if e["tipo"] == "entrada" and e["origem"] == origem]


def test_itens_de_acao_titulos_e_formatos():
    f = orq_mod.itens_de_acao
    novo = "# R\n\n## Itens de ação\n\n1. **Um.** detalhe\n2. Dois\n   continuação\n3) Três\n\n## Depois\n\n4. fora\n"
    assert f(novo) == ["Um. detalhe", "Dois", "Três"]
    assert f("## O que fazer hoje\n\n1. a\n2. b\n") == ["a", "b"]
    assert f("### O que precisa de ação\n| # | Ação | Número |\n|---|---|---|\n| 1 | Combinar com a Alice | 77 |\n| 2 | Levar o `#101` | 6% |\n\nFecham:\n- x") == ["Combinar com a Alice", "Levar o #101"]
    assert f("# R\n\n## Resumo\n\n1. não é ação\n") is None, "sem a seção"
    assert f("## Itens de ação\n\nnada a fazer\n") is None, "seção sem itens numerados"
    assert len(f("## Itens de ação\n1. " + "x" * 900)[0]) <= 300


def test_itens_dos_relatorios_de_exemplo():
    aud = orq_mod.itens_de_acao(open(REAL_AUD).read())
    assert len(aud) == 3 and aud[0].startswith("Back-merge do `main`".replace("`", "")), aud
    cache = orq_mod.itens_de_acao(open(REAL_CACHE).read())
    assert len(cache) == 5 and cache[0].startswith("Combinar com a Alice") and cache[1].startswith("Levar o #101"), cache


def test_caminho_do_relatorio_no_texto_da_automation():
    c = "O relatório está em `.scratch/auditoria-diaria/2026-09-29.md` e o resumo foi salvo."
    assert orq_mod.caminho_do_relatorio(c, "/repo") == "/repo/.scratch/auditoria-diaria/2026-09-29.md"
    assert orq_mod.caminho_do_relatorio("gravado em /abs/x/.scratch/a/b.md ok", "/repo") == "/abs/x/.scratch/a/b.md"
    assert orq_mod.caminho_do_relatorio("sem caminho nenhum", "/repo") is None


def test_ingest_primeira_execucao_nao_despeja_o_historico():
    a = Amb()
    _ingest_env(a, desde=None)
    r = a.orq("ingest")
    assert r.returncode == 0, r
    assert a.events() == [], "as fixtures são todas anteriores a 29/09 15:00Z"
    ing = json.load(open(os.path.join(a.home, "cursor.json")))["ingest"]
    assert ing["desde"] == "2026-09-29T15:00:00Z", ing
    # o que for posterior ao ponto de partida entra
    runs = _fix("automations_runs.json")
    for r_ in runs["result"]["runs"]:
        r_["createdAt"] = 1790697600000  # 29/09 16:00Z
        if r_.get("outputSnapshot"):
            r_["outputSnapshot"]["capturedAt"] = 1790697660000
    a.set("automations_runs.json", runs)
    assert a.orq("ingest").returncode == 0
    assert len(_entradas(a, "relatorio")) == 10, "3 da auditoria + 5 do cache + 1 'ler' de cada Weekday completed (runs 1 e 3), que não citam arquivo"


def test_ingest_run_completed_vira_uma_entrada_por_item_com_o_caminho():
    a = Amb()
    _ingest_env(a)
    assert a.orq("ingest").returncode == 0
    rel = _entradas(a, "relatorio")
    aud = [e for e in rel if e["caminho"] == REAL_AUD]
    cache = [e for e in rel if e["caminho"] == REAL_CACHE]
    assert len(aud) == 3 and len(cache) == 5, [e["fonte"] for e in rel]
    assert aud[0]["fonte"] == "Auditoria diária (demo-app) run 4" and aud[0]["ref"] == "99aeaa55-b701-4c22-b99a-66214acd4ac2"
    assert aud[0]["item"] == 1 and "Back-merge" in aud[0]["texto"]
    assert not [e for e in rel if "run 3" in e["fonte"] or "run 2" in e["fonte"]], "28/09 e run skipped ficam de fora"
    assert [e["id"] for e in orq_mod.abertas(a.events())] == [e["id"] for e in a.events() if e["tipo"] == "entrada"], "cada item é cobrado pelo Stop"
    n = len(a.events())
    assert a.orq("ingest").returncode == 0 and len(a.events()) == n, "segundo ingest não duplica"
    assert set(json.load(open(os.path.join(a.home, "cursor.json")))["ingest"]["runs"]) >= {"99aeaa55-b701-4c22-b99a-66214acd4ac2"}


def test_ingest_relatorio_sem_a_secao_ou_sem_caminho_vira_item_ler():
    a = Amb()
    runs = _fix("automations_runs.json")
    os.makedirs(os.path.join(a.tmp.name, ".scratch", "x"))
    sem_secao = os.path.join(a.tmp.name, ".scratch", "x", "solto.md")
    open(sem_secao, "w").write("# Solto\n\n## Resumo\n\n1. isto não é seção de ação\n")
    ok = [r for r in runs["result"]["runs"] if r["status"] == "completed" and r["createdAt"] > 1790600000000]
    ok[0]["outputSnapshot"]["content"] = f"O relatório está em `{sem_secao}`."
    ok[1]["outputSnapshot"]["content"] = "Terminei, mas não gravei arquivo nenhum."
    runs["result"]["runs"] = ok
    _ingest_env(a, runs=runs)
    a.orq("ingest")
    rel = _entradas(a, "relatorio")
    assert len(rel) == 2
    por_ref = {e["ref"]: e for e in rel}
    assert por_ref[ok[0]["id"]]["texto"].endswith("ler solto.md") and por_ref[ok[0]["id"]]["caminho"] == sem_secao
    assert "ler" in por_ref[ok[1]["id"]]["texto"] and not por_ref[ok[1]["id"]].get("caminho")


def test_ingest_run_em_andamento_espera_e_entra_quando_completa():
    a = Amb()
    runs = _fix("automations_runs.json")
    runs["result"]["runs"] = [r for r in runs["result"]["runs"] if r["runNumber"] == 4 and r["title"].startswith("Auditoria")]
    runs["result"]["runs"][0]["status"] = "running"
    _ingest_env(a, runs=runs)
    a.orq("ingest")
    assert _entradas(a, "relatorio") == []
    runs["result"]["runs"][0]["status"] = "completed"
    a.set("automations_runs.json", runs)
    a.orq("ingest")
    assert len(_entradas(a, "relatorio")) == 3


def test_ingest_inbox_so_worker_done_com_reportpath_vira_entrada():
    a = Amb()
    _ingest_env(a)
    assert a.orq("ingest").returncode == 0
    rw = _entradas(a, "relatorio_worker")
    assert len(rw) == 2, rw
    por = {e["run"]: e for e in rw}
    assert por["run_44769cae0dc5"]["caminho"] == "docs/research/2026-09-29-escritas-no-secundario.md"
    assert por["run_44769cae0dc5"]["texto"].startswith("Escritas no banco secundário") and por["run_44769cae0dc5"]["task"] == "task_d23305d9dca3"
    assert por["run_0927b30c9065"]["caminho"].endswith(".md")
    assert "sem reportPath" in a.log() and "task_10c7b0acb36e" in a.log(), "os outros só no log"
    ing = json.load(open(os.path.join(a.home, "cursor.json")))["ingest"]
    assert ing["inbox_seq"] == 933
    n = len(a.events())
    a.orq("ingest")
    assert len(a.events()) == n, "cursor da inbox evita repetir"


def test_ingest_inbox_primeira_execucao_respeita_o_ponto_de_partida():
    a = Amb()
    _ingest_env(a, desde=None)
    a.orq("ingest")
    assert a.events() == []
    ib = _fix("inbox.json")
    ib["result"]["messages"].append({**[m for m in ib["result"]["messages"] if "reportPath" in (m["payload"] or "")][0],
                                     "id": "msg_novo", "sequence": 1000, "created_at": "2026-09-29T15:30:00Z"})
    a.set("inbox.json", ib)
    a.orq("ingest")
    (e,) = _entradas(a, "relatorio_worker")
    assert e["ref"] == "msg_novo"


def _scout(a, titulo):
    a.set("tasks_run_44769cae0dc5.json", [{"id": "task_59149fb97408", "status": "completed", "task_title": titulo,
                                           "created_at": "2026-09-29T13:00:00Z"},
                                          {"id": "task_d23305d9dca3", "status": "completed", "task_title": "[scout] com relatório",
                                           "created_at": "2026-09-29T13:00:00Z"}])


def test_scout_sem_reportpath_vira_alerta_no_resumo():
    a = Amb()
    _ingest_env(a)
    _scout(a, "[scout] Diagnosticar failover")
    a.orq("ingest")
    (al,) = [e for e in a.events() if e["tipo"] == "alerta"]
    assert al["alerta"] == "scout_sem_relatorio" and al["task"] == "task_59149fb97408" and al["run"] == "run_44769cae0dc5"
    assert not [e for e in a.events() if e["tipo"] == "alerta" and e["task"] == "task_d23305d9dca3"], "scout com reportPath não alerta"
    assert not [e for e in a.events() if e["tipo"] == "alerta" and e["task"] == "task_10c7b0acb36e"], "ship sem reportPath é normal"
    ctx = json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "Alerta: scout 'Diagnosticar failover'" in ctx and len(ctx.splitlines()) <= 5, ctx
    n = len(a.events())
    a.orq("ingest")
    assert len(a.events()) == n
    # sem [scout] no título não alerta
    b = Amb()
    _ingest_env(b)
    _scout(b, "Implementar failover")
    b.orq("ingest")
    assert not [e for e in b.events() if e["tipo"] == "alerta"]


def test_alerta_de_scout_expira_em_24h():
    from datetime import datetime, timezone
    ev = [{"ts": "2026-09-28T10:00:00Z", "tipo": "alerta", "alerta": "scout_sem_relatorio", "task": "t", "titulo": "[scout] velho"}]
    assert "Alerta" not in orq_mod.resumo(ev, None, None, agora=datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc))
    assert "Alerta" in orq_mod.resumo(ev, None, None, agora=datetime(2026, 9, 28, 20, 0, tzinfo=timezone.utc))


def test_resumo_com_relatorios_alerta_e_suspeita_cabe_em_5_linhas():
    from datetime import datetime, timezone
    ev = []
    for i in range(1, 13):
        ev.append({"tipo": "entrada", "origem": "usuario", "id": f"e{i}", "texto": "mensagem longa " + "x" * 200})
    for i in range(13, 21):
        ev.append({"tipo": "entrada", "origem": "relatorio", "id": f"e{i}", "texto": "item", "fonte": "Auditoria diária (demo-app) run 4" if i < 17 else "Acompanhamento de cache run 9",
                   "caminho": "/r/.scratch/auditoria-diaria/2026-09-29.md"})
    ev.append({"tipo": "entrada", "origem": "relatorio_worker", "id": "e21", "texto": "Escritas no banco secundário: a plataforma não escreve", "fonte": "worker Escritas no banco secundário",
               "caminho": "docs/research/x.md"})
    ev.append({"ts": "2026-09-29T12:00:00Z", "tipo": "alerta", "alerta": "scout_sem_relatorio", "task": "task_1234567890", "titulo": "[scout] " + "y" * 200})
    ev.append({"tipo": "resposta_suspeita", "header": "freio-prod"})
    ctx = orq_mod.resumo(ev, None, {"itens": []}, {"id": "e12"}, agora=datetime(2026, 9, 29, 13, 0, tzinfo=timezone.utc))
    linhas = ctx.splitlines()
    assert len(linhas) <= 5 and all(len(l) <= 500 for l in linhas), ctx
    extra = [l for l in linhas if "Relatório" in l]
    assert extra and "resposta suspeita em freio-prod" in extra[0] and "Alerta: scout" in extra[0], ctx
    assert "Auditoria diária" in extra[0] and "×4" in extra[0] and "e13-e16" in extra[0], extra
    assert "worker_done com relatório ×1 [e21] research/x.md" in extra[0], extra
    assert "Sem efeito:" in linhas[0] and "e13" not in linhas[0], "l1 é só das mensagens do usuário"
    # só relatórios abertos: l1 não diz "nenhum" de forma enganosa
    so_rel = [e for e in ev if e.get("origem") in ("relatorio",)]
    assert "Relatório" in orq_mod.resumo(so_rel, None, None)


def test_hook_prompt_dispara_ingest_em_segundo_plano():
    a = Amb()
    a.env.pop("ORQ_NO_BG")
    _ingest_env(a)
    a.set("runs.json", [])
    a.prompt("bom dia")
    for _ in range(50):
        if len(_entradas(a, "relatorio")) == 8 and len(_entradas(a, "relatorio_worker")) == 2:
            break
        time.sleep(0.1)
    assert len(_entradas(a, "relatorio")) == 8 and len(_entradas(a, "relatorio_worker")) == 2
    ctx = json.loads(a.prompt("e agora").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "Relatório" in ctx and len(ctx.splitlines()) <= 5, ctx


def test_ingest_falha_aberta_e_uma_fonte_nao_derruba_a_outra():
    a = Amb()
    _ingest_env(a)
    r = a.orq("ingest", FAKE_FAIL="inbox")
    assert r.returncode == 0 and "ingest inbox" in a.log()
    assert len(_entradas(a, "relatorio")) == 8 and not _entradas(a, "relatorio_worker")
    r = a.orq("ingest", FAKE_FAIL="runs")
    assert r.returncode == 0 and "ingest automations" in a.log()
    assert len(_entradas(a, "relatorio_worker")) == 2, "a inbox segue mesmo com as automations fora"
    assert Amb(FAKE_CRASH="1").orq("ingest", FAKE_CRASH="1").returncode == 0


def test_ingest_dois_ao_mesmo_tempo_nao_duplicam():
    a = Amb()
    _ingest_env(a)
    with ThreadPoolExecutor(4) as ex:
        list(ex.map(lambda _: a.orq("ingest"), range(4)))
    assert len(_entradas(a, "relatorio")) == 8 and len(_entradas(a, "relatorio_worker")) == 2


def test_relatorio_ilegivel_nao_derruba_o_ingest():
    a = Amb()
    runs = _fix("automations_runs.json")
    ok = [r for r in runs["result"]["runs"] if r["status"] == "completed" and r["createdAt"] > 1790600000000]
    ok[0]["outputSnapshot"]["content"] = "O relatório está em `.scratch/nao/existe.md`."
    runs["result"]["runs"] = ok
    _ingest_env(a, runs=runs)
    assert a.orq("ingest").returncode == 0
    rel = [e for e in _entradas(a, "relatorio") if e["ref"] == ok[0]["id"]]
    assert len(rel) == 1 and "ler existe.md" in rel[0]["texto"]


def _steer_env(a):
    a.set("tasks_run_a.json", [{"id": "task_rodando", "status": "dispatched", "dispatch_id": "ctx_1"},
                               {"id": "task_parada", "status": "ready", "dispatch_id": None},
                               {"id": "task_feita", "status": "completed", "dispatch_id": "ctx_0"}])
    a.set("tasks_run_b.json", [{"id": "task_b", "status": "dispatched", "dispatch_id": "ctx_2"}])
    return a.prompt("ajusta o worker")


def _enviados(a):
    try:
        return [json.loads(x) for x in open(os.path.join(a.fake, "sent.log"))]
    except OSError:
        return []


def test_steer_manda_o_send_e_grava_o_evento_e_o_intake():
    a = Amb()
    _steer_env(a)
    r = a.orq("steer", "task_rodando", "use o índice novo", "--entrada", "e1")
    assert r.returncode == 0, r.stderr
    assert _enviados(a) == [["send", "--run", "run_a", "--to", "dispatch:ctx_1", "--subject", "Ajuste",
                             "--body", "use o índice novo\n\n## Pedido do usuário (acréscimo)\najusta o worker", "--priority", "high", "--json"]]
    ev = [e for e in a.events() if e["tipo"] == "steer"]
    assert len(ev) == 1 and (ev[0]["task"], ev[0]["dispatch"], ev[0]["run"], ev[0]["texto"], ev[0]["msg_id"]) == \
        ("task_rodando", "ctx_1", "run_a", "use o índice novo", "msg_9")
    ints = [e for e in a.events() if e["tipo"] == "intake"]
    assert len(ints) == 1 and ints[0]["entrada"] == "e1" and ints[0]["efeito"] == "steer" and ints[0]["ref"] == "task_rodando"


def test_steer_sem_entrada_nao_grava_intake():
    a = Amb()
    _steer_env(a)
    assert a.orq("steer", "task_rodando", "oi").returncode == 0
    assert not [e for e in a.events() if e["tipo"] == "intake"] and len(_enviados(a)) == 1


def test_steer_recusa_task_que_nao_esta_dispatched_ou_nao_existe():
    a = Amb()
    _steer_env(a)
    for t in ("task_parada", "task_feita", "task_fantasma"):
        r = a.orq("steer", t, "oi")
        assert r.returncode == 1 and "orq:" in r.stderr, t
    assert "dispatched" in a.orq("steer", "task_parada", "oi").stderr
    assert not _enviados(a) and not [e for e in a.events() if e["tipo"] == "steer"]


def test_steer_em_outro_run_liga_o_run_sozinho_e_religa_o_anterior():
    a = Amb()
    _steer_env(a)
    r = a.orq("steer", "task_b", "oi", "--run", "run_b")
    assert r.returncode == 0, r.stderr
    assert len(_enviados(a)) == 1 and [e["task"] for e in a.events() if e["tipo"] == "steer"] == ["task_b"]
    assert json.load(open(os.path.join(a.fake, "run.json")))["id"] == "run_a", "o Run que estava ligado volta"
    sem = Amb(run=None)  # sem Run ligado: o orq liga o da task e nada precisa voltar
    _steer_env(sem)
    r = sem.orq("steer", "task_rodando", "oi", "--run", "run_a")
    assert r.returncode == 0, r.stderr
    assert len(_enviados(sem)) == 1


def test_steer_consumer_fenced_do_send_vira_a_mesma_mensagem():
    a = Amb()
    _steer_env(a)
    r = a.orq("steer", "task_rodando", "oi", FAKE_FAIL="send")
    assert r.returncode == 1 and "run-use --id run_a" in r.stderr
    assert not [e for e in a.events() if e["tipo"] == "steer"]


def test_steer_entrada_inexistente_nao_envia():
    a = Amb()
    _steer_env(a)
    r = a.orq("steer", "task_rodando", "oi", "--entrada", "e99")
    assert r.returncode == 1 and "e99" in r.stderr and not _enviados(a)


# ---------- review de 29/09 (review-fatias.md): um teste por achado ----------

class EmProcesso:
    """Aponta os globais do módulo orq para o Amb: as funções gravam nos arquivos de verdade, sem mock de gravação."""

    def __init__(self, a):
        self.a = a

    def __enter__(self):
        m = orq_mod
        self.antes, self.issues = (m.HOME, m.ORCA, m.LOG, m.PEND, dict(os.environ)), m.ISSUES
        m.HOME, m.ORCA, m.LOG, m.PEND, m.ISSUES = self.a.home, self.a.bin, self.a.env["ORQ_LOG"], self.a.env["ORQ_PENDENCIAS"], self.a.env["ORQ_ISSUES"]
        os.environ.update({k: self.a.env[k] for k in ("FAKE_DIR", "ORCA_TERMINAL_HANDLE")})
        return self

    def __exit__(self, *_):
        m = orq_mod
        m.HOME, m.ORCA, m.LOG, m.PEND, env = self.antes
        m.ISSUES = self.issues
        os.environ.clear()
        os.environ.update(env)


def _iso(delta=0):
    """Data no formato do Orca (UTC sem fuso), `delta` segundos a partir de agora."""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() + delta))


def _msg(delta, seq=1000, run="run_a", tipo="heartbeat", para=None):
    """Mensagem do inbox com os campos do Orca real; `para` é o to_handle (o padrão é o mailbox do Run, que o Orca avisa no coordenador)."""
    return {"id": f"msg_{seq}", "run_id": run, "type": tipo, "priority": "normal", "subject": "alive", "body": "", "payload": None,
            "from_handle": "term_worker", "to_handle": para or f"run:{run}", "read": 1, "sequence": seq,
            "created_at": _iso(delta), "delivered_at": _iso(delta)}


def _inbox(a, *msgs):
    a.set("inbox.json", {"ok": True, "result": {"messages": list(msgs), "count": len(msgs)}})


def _cursor(a):
    return json.load(open(os.path.join(a.home, "cursor.json")))


RECOMENDADA = "Teto por pod (Recomendado)"
OPCOES_FREIO = [(RECOMENDADA, "x"), ("Sem freio", "y")]


def _ask_freio(a, resposta, opcoes=OPCOES_FREIO, **env):
    q = [_pergunta("freio-prod", opcoes)]
    return a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: resposta}), **env)


def _ids_pend(a):
    return [i["id"] for i in _pend(a)["itens"]]


# achado 1: resposta suspeita com o sinal do próprio Orca

def test_achado_1_mensagem_entregue_pelo_orca_perto_da_resposta_marca_suspeita_sem_o_hook_de_prompt():
    a = Amb()
    _inbox(a, _msg(-2))  # o Orca entregou uma mensagem ao Run 2 s antes; nenhum UserPromptSubmit a viu
    r = _ask_freio(a, RECOMENDADA)
    ctx = json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    assert "resposta suspeita em freio-prod: confirme" in ctx, r
    assert _ids_pend(a) == ["freio-prod", "avisar-x"], "suspeita não fecha"
    (s,) = [e for e in a.events() if e["tipo"] == "resposta_suspeita"]
    assert s["chegada"] == "inbox" and s["msg"] == "msg_1000"
    assert not [e for e in a.events() if e["tipo"] == "resposta"]


def test_achado_1_so_e_suspeita_com_mensagem_perto_ao_coordenador_e_resposta_recomendada():
    casos = [  # (mensagens, resposta, suspeita?)
        ([_msg(-2)], RECOMENDADA, True),
        ([_msg(+1)], RECOMENDADA, True),  # entrega carimbada logo depois da resposta
        ([_msg(-3)], RECOMENDADA, True),  # o carimbo do Orca é truncado ao segundo
        ([_msg(-30)], RECOMENDADA, False),  # velha
        ([_msg(-2)], "Sem freio", False),  # o usuário escolheu outra opção
        ([_msg(-2, run="run_outro")], RECOMENDADA, True),  # review 2, M4: o Orca avisa o terminal também de Runs a que ele não está ligado
        ([_msg(-2, para="dispatch:ctx_1")], RECOMENDADA, False),  # mensagem para um worker não é digitada no coordenador
        ([], RECOMENDADA, False),
    ]
    for msgs, resposta, suspeita in casos:
        a = Amb()
        _inbox(a, *msgs)
        r = _ask_freio(a, resposta)
        assert r.returncode == 0, r
        assert ("suspeita" in r.stdout) == suspeita, (msgs, resposta, r.stdout)
        assert ("freio-prod" in _ids_pend(a)) == suspeita, (msgs, resposta)


def test_achado_1_recomendada_que_nao_e_a_primeira_e_multiselect_com_so_a_primeira():
    a = Amb()
    _inbox(a, _msg(-1))
    r = _ask_freio(a, "Outra (Recomendado)", opcoes=[("Primeira", "x"), ("Outra (Recomendado)", "y")])
    assert "suspeita" in r.stdout and "freio-prod" in _ids_pend(a)
    a = Amb()
    _inbox(a, _msg(-1))
    for i in ("alice", "bob"):
        a.orq("pend", "add", "--id", i, "--tipo", "avisar", "--titulo", i)
    q = [_pergunta("ja-fez", [("Alice: schemaVersion", "[alice] Avisar"), ("Bob: maxmemory", "[bob] Avisar")], multi=True)]
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Alice: schemaVersion"}))
    assert "suspeita" in r.stdout and "alice" in _ids_pend(a)
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Bob: maxmemory"}))  # marcou a segunda: resposta de gente
    assert "suspeita" not in r.stdout and "bob" not in _ids_pend(a)


def test_achado_1_inbox_fora_do_ar_nao_fecha_nada_e_loga():
    a = Amb()
    r = _ask_freio(a, RECOMENDADA, FAKE_FAIL="inbox")
    assert r.returncode == 0 and "hook ask" in a.log(), r
    assert _ids_pend(a) == ["freio-prod", "avisar-x"] and not [e for e in a.events() if e["tipo"].startswith("resposta")]


# achado 2: cursor.json perdido não reinicia os ids nem duplica o ingest

def test_achado_2_cursor_apagado_nao_reinicia_os_ids():
    a = Amb()
    a.prompt("primeira")
    a.prompt("segunda")
    a.orq("intake", "e1", "conversa")
    a.orq("intake", "e2", "conversa")
    os.remove(os.path.join(a.home, "cursor.json"))
    a.prompt("terceira, nunca tratada")
    ids = [e["id"] for e in a.events() if e["tipo"] == "entrada"]
    assert ids == ["e1", "e2", "e3"], ids
    assert [e["id"] for e in orq_mod.abertas(a.events())] == ["e3"], "o intake antigo de e1 não pode fechar a entrada nova"
    a.prompt("quarta")
    assert _cursor(a)["entrada"] == 4


def test_achado_2_cursor_apagado_nao_repete_o_ingest():
    a = Amb()
    runs = _fix("automations_runs.json")
    for r_ in runs["result"]["runs"]:
        r_["createdAt"] = 1790697600000  # 29/09 16:00Z, depois do ponto de partida
        if r_.get("outputSnapshot"):
            r_["outputSnapshot"]["capturedAt"] = 1790697660000
    ib = _fix("inbox.json")
    for m in ib["result"]["messages"]:
        m["created_at"] = "2026-09-29T16:00:00Z"
    _ingest_env(a, runs=runs, inbox=ib, desde=None)
    assert a.orq("ingest").returncode == 0
    antes = [(e["origem"], e.get("ref"), e.get("item")) for e in a.events() if e["tipo"] == "entrada"]
    assert len(antes) == 12, len(antes)
    os.remove(os.path.join(a.home, "cursor.json"))
    assert a.orq("ingest").returncode == 0
    depois = [(e["origem"], e.get("ref"), e.get("item")) for e in a.events() if e["tipo"] == "entrada"]
    assert depois == antes, "o ingest recomeçou do zero e duplicou as entradas"


def test_achado_2_cursor_corrompido_guarda_a_copia_e_recomeca_pelo_log():
    a = Amb()
    a.prompt("primeira")
    caminho = os.path.join(a.home, "cursor.json")
    open(caminho, "w").write("{quebrado")
    r = a.prompt("segunda")
    assert "entrada e2" in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"], "o prompt vira entrada, com id do log"
    copias = [f for f in os.listdir(a.home) if f.startswith("cursor.json.corrompido-")]
    assert len(copias) == 1 and open(os.path.join(a.home, copias[0])).read() == "{quebrado", "o que não se leu fica guardado"
    assert _cursor(a)["entrada"] == 2 and _cursor(a)["recuperado"]["copia"] == copias[0]
    assert "cursor.json ilegível" in a.log() and copias[0] in a.log()
    _ingest_env(a, desde=None)
    open(caminho, "w").write("[1, 2]")  # raiz que não é objeto
    r = a.orq("ingest")
    assert r.returncode == 0, r
    assert isinstance(_cursor(a), dict) and "ingest" in _cursor(a)


# achado 3: o header só fecha decisão

def test_achado_3_header_de_pendencia_que_nao_e_decisao_nao_fecha():
    a = Amb()
    assert a.orq("pend", "add", "--id", "expire-0929", "--tipo", "acao", "--titulo", "Rodar o deploy manual").returncode == 0
    q = [_pergunta("expire-0929", [("Amanhã", "x"), ("Hoje", "y")])]
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Amanhã"}))
    assert "expire-0929" in _ids_pend(a), "a ação sumiu sem ter sido feita"
    (res,) = [e for e in a.events() if e["tipo"] == "resposta"]
    assert "fechou" not in res
    # decisão continua fechando pelo header, e o já-fez fecha qualquer tipo
    _ask_freio(a, RECOMENDADA)
    assert "freio-prod" not in _ids_pend(a)
    ja = [_pergunta("ja-fez", [("Deploy manual", "[expire-0929] Rodar o deploy")], multi=True)]
    a.orq("hook", "ask", stdin=_ask(ja, {ja[0]["question"]: "Deploy manual"}))
    assert "expire-0929" not in _ids_pend(a)


# achado 4: o fluxo documentado (add, pergunta respondida, intake) tem de funcionar

def test_achado_4_intake_de_decisao_ja_respondida_funciona():
    a = Amb()
    a.prompt("decide isso")
    assert a.orq("pend", "add", "--id", "nova-dec", "--tipo", "decisao", "--titulo", "Qual?").returncode == 0
    q = [_pergunta("nova-dec", [("A", "x"), ("B", "y")])]
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "B"}))
    assert "nova-dec" not in _ids_pend(a), "a resposta fechou a pendência"
    r = a.orq("intake", "e1", "decisao", "nova-dec")
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["ref"] == "nova-dec"
    a.prompt("outra")
    assert a.orq("intake", "e2", "pend", "nunca-existiu").returncode == 1, "id inventado segue recusado"


# achado 5: coordenador x worker (review 2, M1: o papel vem do preâmbulo de despacho, não do worker-list)

DESPACHO = "Please carry out this task from my Orca coordinator by following the brief I pasted below. You are a dispatched worker."


def test_achado_5_worker_que_criou_run_nao_vira_coordenador():
    a = Amb(run="run_w")  # o worker rodou run-create: o terminal está ligado a um Run que ele mesmo criou
    a.set("workers.json", [{"handle": "term_coord", "run": "run_do_despacho"}])  # o worker é de outro Run: o worker-list sem --run não o acha
    json.dump({"entrada": 3}, open(_mk(a, "cursor.json"), "w"))  # o preâmbulo é o primeiro prompt do worker: sem papel e sem Run registrado (review 4, B16)
    r = a.prompt(DESPACHO)
    assert (r.returncode, r.stdout) == (0, ""), r
    r = a.prompt("Ajusta a fatia 6, por favor")
    assert (r.returncode, r.stdout) == (0, ""), r
    r = a.orq("hook", "stop", stdin=json.dumps({"session_id": "abcdef123456"}))
    assert (r.returncode, r.stdout) == (0, "")
    q = [_pergunta("freio-prod", [("A", "x")])]
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "A"}))
    assert a.events() == [] and _ids_pend(a) == ["freio-prod", "avisar-x"], "worker não grava entrada nem fecha pendência do usuário"
    cur = _cursor(a)
    assert "abcdef123456" not in cur.get("runs", {}) and cur["papeis"]["abcdef123456"] == "worker", cur
    a.set("run.json", None)  # o binding cai: worker não recebe "rode run-use"
    assert a.prompt("de novo").stdout == ""
    assert a.log() == ""


def test_achado_5_papel_de_worker_vale_com_o_orca_falso_no_escopo_real_do_worker_list():
    # o worker-list sem --run só lista o Run ligado ao terminal: o Run que o worker criou nunca o tem
    a = Amb(run="run_w")
    a.set("workers.json", [{"handle": "term_coord", "run": "run_do_despacho"}])
    p = subprocess.run([a.bin, "orchestration", "worker-list", "--json"], capture_output=True, text=True, env=a.env)
    res = json.loads(p.stdout)["result"]
    assert res["workers"] == [] and res["scope"] == {"run": "run_w", "source": "bound"}
    a.set("run.json", None)
    res = json.loads(subprocess.run([a.bin, "orchestration", "worker-list", "--json"], capture_output=True, text=True, env=a.env).stdout)["result"]
    assert [w["agentTerminalHandle"] for w in res["workers"]] == ["term_coord"] and res["scope"]["source"] == "all"


def test_achado_5_coordenador_sem_preambulo_continua_registrado_sem_chamar_o_worker_list():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_de_worker", "run": "run_a"}])
    out = json.loads(a.prompt("primeira").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "entrada e1" in out
    a.prompt("segunda")
    cur = _cursor(a)
    assert cur["runs"] == {"abcdef123456": "run_a"} and "papeis" not in cur
    chamadas = [json.loads(x) for x in open(os.path.join(a.fake, "calls.log"))]
    assert not [c for c in chamadas if c[0] == "worker-list"], "o worker-list não é sinal de papel"
    a.set("run.json", None)
    assert "binding perdido" in a.prompt("terceira").stdout


def test_achado_5_despacho_vale_mesmo_sem_run_ligado_e_a_sessao_de_worker_nao_chama_o_orca():
    a = Amb(run=None)
    assert a.prompt(DESPACHO).stdout == ""
    assert _cursor(a)["papeis"] == {"abcdef123456": "worker"}
    a.set("run.json", {"id": "run_criado_depois"})  # o worker roda run-create depois do preâmbulo
    n = len(open(os.path.join(a.fake, "calls.log")).read().splitlines()) if os.path.exists(os.path.join(a.fake, "calls.log")) else 0
    assert a.prompt("de novo").stdout == "" and a.events() == []
    depois = len(open(os.path.join(a.fake, "calls.log")).read().splitlines()) if os.path.exists(os.path.join(a.fake, "calls.log")) else 0
    assert depois == n, "sessão de worker já decidida não chama o Orca"


def test_achado_5_preambulo_de_despacho_nao_e_entrada():
    assert orq_mod.origem("Please carry out this task from my Orca coordinator by following the brief") == "despacho"
    a = Amb()
    r = a.prompt("Please carry out this task from my Orca coordinator by following the brief I pasted below.")
    assert (r.returncode, r.stdout) == (0, "") and a.events() == []


def _mk(a, nome):
    os.makedirs(a.home, exist_ok=True)
    return os.path.join(a.home, nome)


# achado 6: a mensagem do Orca dispara o ingest

def test_achado_6_mensagem_do_orca_dispara_o_ingest_em_segundo_plano():
    a = Amb()
    a.env.pop("ORQ_NO_BG")
    _ingest_env(a)
    a.set("runs.json", [])
    r = a.prompt("You have 1 orchestration message. Run `orca orchestration check`")
    assert (r.returncode, r.stdout) == (0, "")
    for _ in range(60):
        if len(_entradas(a, "relatorio_worker")) == 2:
            break
        time.sleep(0.1)
    assert len(_entradas(a, "relatorio_worker")) == 2, "o relatório do worker só entrava no próximo prompt do usuário"
    assert not [e for e in a.events() if e["tipo"] == "entrada" and e["origem"] == "usuario"]


# achado 7: um Run quebrado não derruba o refresh

def _run_quebrado(a):
    a.set("runs.json", [{"id": "run_a"}, {"id": "run_b"}, {"id": "run_c"}])
    a.set("tasks_run_a.json", [{"id": "t1", "status": "ready", "spec": "x", "created_at": "2026-09-27T00:00:00Z"}])
    a.set("tasks_run_b.json", [{"id": "t2", "status": "ready", "spec": "sem created_at"}])
    a.set("tasks_run_c.json", [{"id": "t3", "status": "blocked", "task_title": "c", "created_at": "2026-09-01T00:00:00Z"}])


def test_achado_7_run_com_dado_quebrado_ou_task_list_falhando_nao_derruba_os_outros():
    a = Amb()
    _run_quebrado(a)
    assert a.orq("ingest", "--refresh").returncode == 0
    ab = json.load(open(os.path.join(a.home, "aberto.json")))
    assert [i["id"] for i in ab["backlog"]] == ["t1"] and [i["id"] for i in ab["bloqueado"]] == ["t3"], ab
    assert ab["falhas"] == ["run_b"] and "run_b" in a.log()
    b = Amb()
    _run_quebrado(b)
    b.set("tasks_run_b.json", [])
    assert b.orq("ingest", "--refresh", FAKE_FAIL_RUN="run_c").returncode == 0
    ab = json.load(open(os.path.join(b.home, "aberto.json")))
    assert [i["id"] for i in ab["backlog"]] == ["t1"] and ab["falhas"] == ["run_c"] and "run_c" in b.log()
    assert "1 Run sem leitura" in orq_mod.resumo([], ab, None), "o resumo diz que faltou um Run"


def test_achado_7_refresh_segue_o_next_cursor_do_run_list():
    a = Amb()
    a.set("runs.json", [{"id": f"run_{i}"} for i in range(205)])
    a.set("tasks_run_204.json", [{"id": "t_ultimo", "status": "ready", "spec": "x", "created_at": "2026-09-27T00:00:00Z"}])
    assert a.orq("ingest", "--refresh").returncode == 0
    ab = json.load(open(os.path.join(a.home, "aberto.json")))
    assert [i["id"] for i in ab["backlog"]] == ["t_ultimo"], "o Run 205 está na terceira página"


def test_achado_7_erro_no_refresh_vai_para_o_log():
    a = Amb()
    r = a.orq("ingest", "--refresh", FAKE_FAIL="run-list")
    assert r.returncode == 1 and "Traceback" not in r.stderr and "falhou run-list" in r.stderr, r
    assert "ingest --refresh: RuntimeError: falhou run-list" in a.log(), "antes o erro só saía na tela e o cache velho parecia novo"


def test_achado_7_resumo_e_painel_mostram_a_idade_do_cache():
    from datetime import datetime, timezone
    ab = {"ts": "2026-09-29T14:48:30Z", "backlog": [], "rodando": 0, "bloqueado": [], "gates": []}
    hora = datetime(2026, 9, 29, 14, 48, 30, tzinfo=timezone.utc).astimezone().strftime("%H:%M")
    assert f"Aberto (cache de {hora}):" in orq_mod.resumo([], ab, None)


# achado 8: o teto de 3 s passa pelo alarme

def test_achado_8_stdin_que_nunca_fecha_e_cortado_pelo_alarme_de_3s():
    a = Amb()
    t = time.time()
    p = subprocess.Popen([sys.executable, ORQ, "hook", "stop"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, env=a.env)
    try:
        assert p.wait(timeout=8) == 0
    finally:
        p.stdin.close()
    dt = time.time() - t
    assert 2.7 < dt < 3.6 and p.stdout.read() == "", dt
    assert "TimeoutError: hook stop passou de 3s" in a.log()


def test_achado_8_cursor_lock_preso_e_cortado_pelo_alarme_de_3s():
    import fcntl
    a = Amb()
    with open(_mk(a, "cursor.lock"), "w") as preso:
        fcntl.flock(preso, fcntl.LOCK_EX)
        t = time.time()
        r = a.prompt("oi")
        dt = time.time() - t
    assert (r.returncode, r.stdout) == (0, "") and 2.7 < dt < 3.6, (dt, r)
    assert "TimeoutError: hook prompt passou de 3s" in a.log()
    assert a.events() == []


# achado 9: a limpeza de mergeados só roda no coordenador

def test_achado_9_limpeza_so_no_coordenador():
    limpou = "Limpeza de branches mergeadas"
    assert limpou in _limpar("1163 merged"), "coordenador dispara"
    sem_run = Amb(run=None)
    assert _limpar("1163 merged", sem_run) == "", "terminal sem Run é worker"
    worker_com_run = Amb(run="run_w")
    worker_com_run.prompt(DESPACHO)
    assert _limpar("1163 merged", worker_com_run) == "", "worker que criou um Run de teste não limpa"
    assert _limpar("Please carry out this task from my Orca coordinator ... o PR #7 foi merged; ajuste o changelog") == ""
    sem_handle = Amb()
    sem_handle.env.pop("ORCA_TERMINAL_HANDLE")
    assert _limpar("1163 merged", sem_handle) == "", "fora do Orca não limpa"


def test_achado_9_json_de_entrada_corrompido_sai_com_0():
    with tempfile.TemporaryDirectory() as t:
        os.makedirs(f"{t}/.claude/orq")
        os.symlink(ORQ, f"{t}/.claude/orq/orq.py")
        r = subprocess.run([sys.executable, LIMPAR], input="isto não é json", capture_output=True, text=True, env={**os.environ, "HOME": t}, timeout=30)
    assert (r.returncode, r.stdout, r.stderr) == (0, "", "")


# achado 10: linha de entrada sem id não cega o Stop

def test_achado_10_entrada_sem_id_nao_cega_o_stop():
    a = Amb()
    a.prompt("boa")
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"tipo": "entrada", "origem": "usuario", "texto": "editada à mão, sem id"}) + "\n")
        f.write(json.dumps({"tipo": "intake", "efeito": "conversa"}) + "\n")
    r = a.orq("hook", "stop", stdin="{}")
    assert "1 entrada(s) sem efeito: e1" in json.loads(r.stdout)["systemMessage"], r
    assert a.log() == ""
    assert "entrada e2" in a.prompt("outra").stdout


# achado 11: resposta suspeita expira

def test_achado_11_suspeita_expira_em_24h_e_sai_com_a_proxima_resposta_da_sessao():
    from datetime import datetime, timezone
    agora = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
    sus = {"ts": "2026-09-29T10:00:00Z", "tipo": "resposta_suspeita", "header": "Destino", "sessao": "s1"}
    assert orq_mod.suspeitas([sus], agora) == ["Destino"]
    assert orq_mod.suspeitas([{**sus, "ts": "2026-09-28T10:00:00Z"}], agora) == [], "25 h depois"
    refeita = {"ts": "2026-09-29T10:05:00Z", "tipo": "resposta", "header": "Destino #2", "sessao": "s1"}
    assert orq_mod.suspeitas([sus, refeita], agora) == [], "a pergunta refeita com outro header e respondida de verdade"
    assert orq_mod.suspeitas([sus, {**refeita, "sessao": "s2"}], agora) == ["Destino"], "outra sessão não limpa"
    assert orq_mod.suspeitas([sus, {"tipo": "pend", "op": "done", "pend": "Destino"}], agora) == []


def test_achado_11_hook_ask_grava_a_sessao_da_resposta():
    a = Amb()
    _ask_freio(a, RECOMENDADA)
    (res,) = [e for e in a.events() if e["tipo"] == "resposta"]
    assert res["sessao"] == "abcdef12"


# achado 12: item venenoso não trava o ingest

def test_achado_12_mensagem_e_relatorio_venenosos_nao_travam_o_ingest():
    a = Amb()
    ib = _fix("inbox.json")
    ruim = {**ib["result"]["messages"][0], "id": "msg_ruim", "sequence": 900, "run_id": None, "type": "worker_done",
            "created_at": "2026-09-29T15:20:00Z", "payload": json.dumps({"taskId": "task_x", "outcome": "succeeded"})}
    ib["result"]["messages"].append(ruim)
    runs = _fix("automations_runs.json")
    ok = [r for r in runs["result"]["runs"] if r["status"] == "completed" and r["createdAt"] > 1790600000000]
    os.makedirs(os.path.join(a.tmp.name, ".scratch", "x"))
    binario = os.path.join(a.tmp.name, ".scratch", "x", "binario.md")
    open(binario, "wb").write(b"# R\n\xff\xfe\x00 nao e utf-8")
    ok[0]["outputSnapshot"]["content"] = f"O relatório está em `{binario}`."
    runs["result"]["runs"] = ok
    _ingest_env(a, runs=runs, inbox=ib)
    assert a.orq("ingest").returncode == 0
    assert len(_entradas(a, "relatorio_worker")) == 2, "a mensagem venenosa não pode segurar as outras"
    assert "msg_ruim" in a.log()
    rel = [e for e in _entradas(a, "relatorio") if e["ref"] == ok[0]["id"]]
    assert len(rel) == 1 and "ler binario.md" in rel[0]["texto"], "relatório que não é UTF-8 vira o item 'ler'"
    assert _cursor(a)["ingest"]["inbox_seq"] == 933
    tamanho = len(open(a.env["ORQ_LOG"]).read())
    a.orq("ingest")
    assert len(open(a.env["ORQ_LOG"]).read()) == tamanho, "o cursor avançou: o veneno não volta a cada ingest"


def test_achado_12_janela_da_inbox_perdida_e_logada():
    a = Amb()
    ib = _fix("inbox.json")
    _ingest_env(a, inbox=ib)
    cur = _cursor(a)
    cur["ingest"]["inbox_seq"] = 100  # o ingest anterior parou na 100 e a janela de 200 só começa depois
    for m in ib["result"]["messages"]:
        m["sequence"] += 1000
    a.set("inbox.json", ib)
    json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
    assert a.orq("ingest").returncode == 0
    assert "janela perdida" in a.log()


# achado 13: o dedupe das automations não tem teto

def test_achado_13_run_antigo_que_sai_da_lista_de_200_nao_reentra():
    a = Amb()
    base = 1790690000000
    runs = {"ok": True, "result": {"runs": [
        {"id": f"auto_{i:03d}", "title": "Auto", "status": "completed", "createdAt": base + i * 1000,
         "outputSnapshot": {"content": "feito", "capturedAt": base + i * 1000 + 500}, "runContext": {"path": "/x"}} for i in range(210)]}}
    _ingest_env(a, runs=runs, desde="2026-09-29T00:00:00Z")
    assert a.orq("ingest").returncode == 0
    assert len(_entradas(a, "relatorio")) == 210
    assert a.orq("ingest").returncode == 0
    assert len(_entradas(a, "relatorio")) == 210, "os 10 primeiros saíram da lista de 200 ids e voltaram"
    assert _cursor(a)["ingest"]["auto_desde"] > "2026-09-29T00:00:00Z" and _cursor(a)["ingest"]["desde"] == "2026-09-29T00:00:00Z"


# achado 14: fora do Orca o hook não chama o Orca nem enche o log

def test_achado_14_sem_terminal_do_orca_sai_antes_de_chamar_o_orca():
    a = Amb()
    a.env.pop("ORCA_TERMINAL_HANDLE")
    for kind in ("prompt", "stop", "ask"):
        r = a.orq("hook", kind, stdin=json.dumps({"prompt": "oi", "session_id": "abcdef123456"}))
        assert (r.returncode, r.stdout) == (0, ""), r
    assert a.log() == "" and not os.path.exists(os.path.join(a.fake, "calls.log")) and a.events() == []


# achado 15: parser de relatório

def test_achado_15_parser_nao_corta_em_hash_sem_espaco_nem_em_bloco_de_codigo():
    f = orq_mod.itens_de_acao
    assert f("## Itens de ação\n1. a\n#101 precisa de review\n2. b\n") == ["a", "b"]
    assert f("## Itens de ação\n1. a\n```bash\n# comentário\n3. isto é código\n```\n2. b\n") == ["a", "b"]
    assert f("## Itens de ação\n1. a\n## Depois\n2. fora\n") == ["a"], "título de verdade continua encerrando"
    assert f("## Itens de ação\n1. a\n#### Sub\n2. fora\n") == ["a"]


def test_achado_15_caminho_do_relatorio_prefere_o_da_data_do_run():
    c = "Ontem: `.scratch/aud/2026-09-28.md`. Hoje o relatório está em `.scratch/aud/2026-09-29.md`; veja também `.scratch/aud/2026-09-27.md`."
    hoje = 1790680194490  # 29/09/2026 ~10:29Z
    assert orq_mod.caminho_do_relatorio(c, "/repo", hoje) == "/repo/.scratch/aud/2026-09-29.md"
    outro = "citou `.scratch/a/x.md` e depois `.scratch/a/y.md`"
    assert orq_mod.caminho_do_relatorio(outro, "/repo", hoje) == "/repo/.scratch/a/y.md", "sem data no nome, vale o último citado"
    assert orq_mod.caminho_do_relatorio(outro, "/repo") == "/repo/.scratch/a/y.md"


# condições de corrida

def test_corrida_hook_ask_com_pendencia_fechada_por_outro_orq_segue_nas_proximas_perguntas():
    a = Amb()
    a.orq("pend", "add", "--id", "dois-x", "--tipo", "decisao", "--titulo", "X")
    qs = [_pergunta("freio-prod", [("A", "x")]), _pergunta("dois-x", [("A", "x")])]
    real = orq_mod._load_pend
    fechou = []

    def velho():
        d = real()
        if not fechou:  # outro orq fecha freio-prod entre a leitura e o pend_done
            fechou.append(True)
            orq_mod.pend_done("freio-prod")
        return d

    with EmProcesso(a):
        orq_mod._load_pend = velho
        try:
            ev = json.loads(_ask(qs, {qs[0]["question"]: "A", qs[1]["question"]: "A"}))
            orq_mod.hook_ask(ev, {"id": "run_a"})
        finally:
            orq_mod._load_pend = real
    assert "dois-x" not in _ids_pend(a), "o ValueError da primeira pergunta interrompeu a segunda"


def test_corrida_alarme_no_meio_do_pend_done_nao_deixa_estado_pela_metade():
    import signal
    a = Amb()
    real = orq_mod.append_event

    def lento(*args, **kw):
        time.sleep(1.4)
        return real(*args, **kw)

    def alarme(*_):
        raise TimeoutError("alarme")

    with EmProcesso(a):
        orq_mod.append_event = lento
        signal.signal(signal.SIGALRM, alarme)
        signal.alarm(1)
        try:
            try:
                orq_mod.pend_done("freio-prod", "ok")
            except TimeoutError:
                pass  # o alarme só é entregue depois de gravar os dois lados
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            orq_mod.append_event = real
    done = [e for e in a.events() if e["tipo"] == "pend" and e["op"] == "done"]
    assert "freio-prod" not in _ids_pend(a) and len(done) == 1, "pendência saiu do arquivo sem o evento pend done"


def test_corrida_escrita_que_falha_nao_deixa_tmp():
    a = Amb()
    pasta = os.path.dirname(a.env["ORQ_PENDENCIAS"])
    try:
        orq_mod._write_json(os.path.join(pasta, "x.json"), {"objeto": object()})
    except TypeError:
        pass
    assert not [f for f in os.listdir(pasta) if f.startswith("tmp")], os.listdir(pasta)


# divergências entre o desenho e o código

def _gates(a):
    try:
        return [json.loads(x) for x in open(os.path.join(a.fake, "gates.log"))]
    except OSError:
        return []


def test_divergencia_decisao_com_task_cria_o_gate_e_a_resposta_resolve():
    a = Amb()
    r = a.orq("pend", "add", "--id", "gate-dec", "--tipo", "decisao", "--titulo", "Sobe o freio?", "--task", "task_1")
    assert r.returncode == 0, r.stderr
    assert _gates(a) == [["gate-create", "--task", "task_1", "--question", "Sobe o freio?", "--json"]]
    assert next(i for i in _pend(a)["itens"] if i["id"] == "gate-dec")["gate"] == "gate_1"
    q = [_pergunta("gate-dec", [("Sim", "x"), ("Não", "y")])]
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Não"}))
    assert "gate-dec" not in _ids_pend(a)
    assert _gates(a)[1] == ["gate-resolve", "--id", "gate_1", "--resolution", "Não", "--json"]
    # fechada à mão também resolve o gate; resposta suspeita não resolve
    a.orq("pend", "add", "--id", "gate-2", "--tipo", "decisao", "--titulo", "Outra?", "--task", "task_2")
    assert a.orq("pend", "done", "gate-2").returncode == 0
    assert _gates(a)[3][:4] == ["gate-resolve", "--id", "gate_2", "--resolution"], "review 3, B9: o Orca falso dá um id por gate"
    b = Amb()
    b.orq("pend", "add", "--id", "gate-dec", "--tipo", "decisao", "--titulo", "Sobe?", "--task", "task_1")
    _inbox(b, _msg(-1))
    q = [_pergunta("gate-dec", [("Sim", "x")])]
    b.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sim"}))
    assert len(_gates(b)) == 1 and "gate-dec" in _ids_pend(b)


def test_divergencia_gate_que_falha_desfaz_a_pendencia_e_task_so_vale_para_decisao():
    a = Amb()
    r = a.orq("pend", "add", "--id", "gate-dec", "--tipo", "decisao", "--titulo", "Sobe?", "--task", "task_1", FAKE_FAIL="gate-create")
    assert r.returncode == 1 and "gate" in r.stderr
    assert "gate-dec" not in _ids_pend(a) and not [e for e in a.events() if e["tipo"] == "pend"]
    r = a.orq("pend", "add", "--id", "acao-x", "--tipo", "acao", "--titulo", "X", "--task", "task_1")
    assert r.returncode == 1 and "decisão" in r.stderr and "acao-x" not in _ids_pend(a)


def test_divergencia_pendencia_com_espera_nao_vira_pergunta():
    a = Amb()
    r = a.orq("pend", "add", "--id", "esp-dec", "--tipo", "decisao", "--titulo", "X", "--espera", "Alice")
    assert r.returncode == 1 and "espera" in r.stderr and "esp-dec" not in _ids_pend(a)
    a.orq("pend", "add", "--id", "avisar-alice", "--tipo", "avisar", "--titulo", "Avisar o Alice", "--espera", "Alice")
    q = [_pergunta("avisar-alice", [("Sim", "x")])]
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sim"}))
    assert "avisar-alice" in _ids_pend(a), "pendência que espera terceiro não fecha por pergunta"


def test_divergencia_aviso_do_intake_de_outro_run_diz_que_e_recusado():
    a = Amb()
    a.prompt("faz isso")
    a.set("tasks_run_b.json", [{"id": "task_b", "status": "ready", "created_at": "2026-09-18T00:00:00Z"}])
    r = a.orq("intake", "e1", "tarefa", "task_b", "--run", "run_b")
    assert "é recusado (consumer_fenced)" in r.stderr and "pode ser recusado" not in r.stderr and "run-use --id run_b" in r.stderr, r.stderr


# ---------- segundo review de 29/09 (review-2.md): um teste por achado ----------

def _pos_despacho_sem_binding(a):
    return [json.loads(x) for x in open(os.path.join(a.fake, "calls.log"))] if os.path.exists(os.path.join(a.fake, "calls.log")) else []


def test_review2_m2_cursor_ilegivel_nao_desliga_o_orq_e_avisa_no_resumo_e_no_stop():
    a = Amb()
    for t in ("um", "dois", "três"):
        a.prompt(t)
    a.orq("intake", "e1", "conversa")
    a.orq("intake", "e2", "conversa")  # e3 fica sem efeito
    open(os.path.join(a.home, "cursor.json"), "w").write("{quebrado")
    r = a.orq("hook", "stop", stdin=json.dumps({"session_id": "abcdef123456"}))
    out = json.loads(r.stdout)["systemMessage"]
    assert "1 entrada(s) sem efeito: e3" in out and "cursor.json estava ilegível" in out, "o Stop não fica mudo"
    ctx = json.loads(a.prompt("quarta mensagem").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "entrada e4 (usuário)" in ctx and "cursor.json estava ilegível" in ctx and "Sem efeito: e3" in ctx, ctx
    assert [e["id"] for e in a.events() if e["tipo"] == "entrada"] == ["e1", "e2", "e3", "e4"], "id vem do maior eN do log, mais um"
    (copia,) = [f for f in os.listdir(a.home) if f.startswith("cursor.json.corrompido-")]
    assert open(os.path.join(a.home, copia)).read() == "{quebrado"
    q = [_pergunta("freio-prod", OPCOES_FREIO)]
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sem freio"}))  # o hook ask segue funcionando
    assert "freio-prod" not in _ids_pend(a)
    assert copia in a.log()


def test_review2_m2_o_aviso_de_cursor_recuperado_expira_em_24h():
    from datetime import datetime, timezone
    agora = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    rec = {"recuperado": {"ts": "2026-09-29T13:00:00Z", "copia": "cursor.json.corrompido-x"}}
    assert orq_mod.cursor_recuperado(rec, agora) is not None
    assert orq_mod.cursor_recuperado(rec, datetime(2026, 9, 30, 13, 1, tzinfo=timezone.utc)) is None
    assert "cursor.json estava ilegível" in orq_mod.resumo([], None, None, cursor=rec, agora=agora)
    assert "ilegível" not in orq_mod.resumo([], None, None, cursor={}, agora=agora)


OPCOES_INDICADOR = [("A: badge só quando falta (Recomendado)", "x"), ("B: badge sempre", "y")]


def test_review2_m3_texto_livre_nao_fecha_a_decisao_nem_resolve_o_gate():
    a = Amb()
    a.prompt("decide o indicador")
    assert a.orq("pend", "add", "--id", "Indicador", "--tipo", "decisao", "--titulo", "Qual badge?", "--task", "task_1").returncode == 0
    q = [_pergunta("Indicador", OPCOES_INDICADOR)]
    livre = "Quero ver, não vi como ficou o protótipo"
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: livre}))
    assert 'resposta livre em Indicador: se decidiu, feche com orq pend done Indicador --resposta "<o que foi decidido>"' in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    assert "Indicador" in _ids_pend(a), "a pendência segue aberta"
    assert not [x for x in open(os.path.join(a.fake, "gates.log")) if "gate-resolve" in x], "o gate segue pendente"
    (res,) = [e for e in a.events() if e["tipo"] == "resposta"]
    assert res["resposta"] == livre and res["livre"] is True and "fechou" not in res
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert 'resposta livre em Indicador: se decidiu, feche com orq pend done Indicador --resposta "<o que foi decidido>"' in ctx, "o resumo lembra enquanto a pendência está aberta"
    # opção marcada junto com texto também é livre; só a opção sozinha fecha e resolve
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "B: badge sempre, mas depois eu vejo"}))
    assert "Indicador" in _ids_pend(a)
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "B: badge sempre"}))
    assert "Indicador" not in _ids_pend(a)
    assert '"gate-resolve", "--id", "gate_1", "--resolution", "B: badge sempre"' in open(os.path.join(a.fake, "gates.log")).read().replace("\\", "")
    assert "resposta livre" not in json.loads(a.prompt("fechou?").stdout)["hookSpecificOutput"]["additionalContext"]


def test_review2_m3_pend_done_a_mao_apaga_o_aviso_de_resposta_livre():
    a = Amb()
    q = [_pergunta("freio-prod", OPCOES_FREIO)]
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "vamos ver primeiro"}))
    assert "resposta livre em freio-prod" in a.prompt("oi").stdout
    assert a.orq("pend", "done", "freio-prod", "--resposta", "decidi: sem freio").returncode == 0
    assert "resposta livre" not in a.prompt("oi de novo").stdout


def test_review2_m4_mensagem_de_outro_run_perto_da_resposta_recomendada_marca_suspeita():
    a = Amb(run="run_a")
    _inbox(a, _msg(-1, run="run_b"))  # o heartbeat do Run B digitado com o terminal ligado ao Run A
    r = _ask_freio(a, RECOMENDADA)
    assert "resposta suspeita em freio-prod" in r.stdout and "freio-prod" in _ids_pend(a), r
    (s,) = [e for e in a.events() if e["tipo"] == "resposta_suspeita"]
    assert s["chegada"] == "inbox" and s["msg"] == "msg_1000"


def test_review2_b1_recomendado_pelo_worker_conta_como_recomendada():
    ops = [("A: badge só quando falta", "x"), ("B: badge com texto sempre (Recomendado pelo worker)", "y")]
    assert orq_mod._recomendada("B: badge com texto sempre (Recomendado pelo worker)", _pergunta("h", ops))
    assert not orq_mod._recomendada("A: badge só quando falta (por ora)", _pergunta("h", [("Zero", "x"), ("A: badge só quando falta (por ora)", "y")]))
    a = Amb()
    _inbox(a, _msg(-1))
    r = _ask_freio(a, ops[1][0], opcoes=ops)
    assert "suspeita" in r.stdout and "freio-prod" in _ids_pend(a)


def test_review2_b3_aviso_do_orca_ingere_o_inbox_sem_refazer_o_aberto():
    a = Amb()
    a.env.pop("ORQ_NO_BG")
    _ingest_env(a)
    a.set("runs.json", [{"id": "run_a"}])
    assert a.prompt("You have 1 orchestration message. Run `orca orchestration check`").stdout == ""
    for _ in range(60):
        if len(_entradas(a, "relatorio_worker")) == 2:
            break
        time.sleep(0.1)
    assert len(_entradas(a, "relatorio_worker")) == 2
    time.sleep(0.6)
    cmds = [c[0] for c in _pos_despacho_sem_binding(a)]
    assert "inbox" in cmds and "run-list" not in cmds, cmds  # o refresh começa sempre pelo run-list
    a.prompt("uma mensagem do usuário")  # o prompt do usuário continua refazendo o cache
    for _ in range(60):
        if "run-list" in [c[0] for c in _pos_despacho_sem_binding(a)]:
            break
        time.sleep(0.1)
    assert "run-list" in [c[0] for c in _pos_despacho_sem_binding(a)]


def _gate_pend(a, id_="gate-dec"):
    a.orq("pend", "add", "--id", id_, "--tipo", "decisao", "--titulo", "Sobe?", "--task", "task_1")
    return [_pergunta(id_, [("Sim", "x"), ("Não", "y")])]


def _gates_log(a):
    try:
        return [json.loads(x) for x in open(os.path.join(a.fake, "gates.log"))]
    except OSError:
        return []


def test_review2_b4_alarme_que_vence_depois_da_gravacao_nao_deixa_o_gate_pendente():
    a = Amb()
    q = _gate_pend(a)
    t = time.time()
    # run-current + inbox levam 2,4 s; a pendência é gravada; o gate-resolve começa e o teto de 3 s vence dentro dele
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sim"}), FAKE_SLEEP="1.2")
    assert r.returncode == 0 and time.time() - t < 4.5, r
    assert "gate-resolve gate_1: TimeoutError" in a.log(), a.log()
    assert "gate-dec" not in _ids_pend(a), "a pendência foi fechada"
    assert not [g for g in _gates_log(a) if g[0] == "gate-resolve"], "o gate ficou pendente"
    assert [e for e in a.events() if e["tipo"] == "pend" and e["op"] == "done"][0]["gate"] == "gate_1", "o evento guarda o gate"
    assert a.orq("ingest").returncode == 0  # o próximo ingest refaz o gate-resolve
    (res,) = [g for g in _gates_log(a) if g[0] == "gate-resolve"]
    assert res[res.index("--id") + 1] == "gate_1" and res[res.index("--resolution") + 1] == "Sim"
    assert [e["gate"] for e in a.events() if e["tipo"] == "gate_resolvido"] == ["gate_1"]
    a.orq("ingest")
    assert len([g for g in _gates_log(a) if g[0] == "gate-resolve"]) == 1, "não repete o que já foi resolvido"


def test_review2_b4_orca_que_recusa_o_gate_e_tentado_ate_o_teto_e_depois_para():
    a = Amb()
    q = _gate_pend(a)
    a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sim"}), FAKE_FAIL="gate-resolve")
    for _ in range(5):
        a.orq("ingest", FAKE_FAIL="gate-resolve")
    assert len([e for e in a.events() if e["tipo"] == "gate_falha"]) == orq_mod.MAX_TENTATIVAS
    assert not [e for e in a.events() if e["tipo"] == "gate_resolvido"]


def _inbox_scout(a, seq=960):
    m = {**_msg(-5, seq=seq), "type": "worker_done", "created_at": "2026-09-29T16:30:00Z",
         "payload": json.dumps({"taskId": "task_s", "outcome": "succeeded"})}
    _ingest_env(a, inbox={"ok": True, "result": {"messages": [m], "count": 1}})
    a.set("tasks_run_a.json", [{"id": "task_s", "task_title": "[scout] varre o cache", "status": "completed"}])


def test_review2_b5_falha_transitoria_do_orca_nao_perde_a_mensagem_do_inbox():
    a = Amb()
    _inbox_scout(a)
    assert a.orq("ingest", FAKE_FAIL="task-list").returncode == 0  # o task-list do título do scout falha
    assert _cursor(a)["ingest"]["inbox_seq"] == 0 and "adiada" in a.log(), "o cursor não passa da mensagem"
    assert not [e for e in a.events() if e["tipo"] == "alerta"]
    assert a.orq("ingest").returncode == 0
    (al,) = [e for e in a.events() if e["tipo"] == "alerta"]
    assert al["alerta"] == "scout_sem_relatorio" and al["task"] == "task_s"
    assert _cursor(a)["ingest"]["inbox_seq"] == 960


def test_review2_b5_mensagem_que_sempre_falha_e_descartada_no_teto():
    a = Amb()
    _inbox_scout(a)
    for _ in range(orq_mod.MAX_TENTATIVAS):
        a.orq("ingest", FAKE_FAIL="task-list")
    assert _cursor(a)["ingest"]["inbox_seq"] == 960 and "descartada depois de" in a.log(), "não trava o ingest para sempre"


def test_review2_b5_run_de_automation_que_falha_nao_e_pulado_pelo_auto_desde():
    a = Amb()
    def run_(id_, quando, **extra):
        return {"id": id_, "title": "Auditoria", "status": "completed", "createdAt": quando,
                "outputSnapshot": {"capturedAt": quando, "content": "sem relatório"}, **extra}
    ok1, ruim, ok3 = run_("run_1", 1790697600000), run_("run_2", 1790697660000), run_("run_3", 1790697720000)
    del ruim["title"]
    ruim["outputSnapshot"] = "quebrado"  # r.get("outputSnapshot") or {} devolve uma string: .get falha
    _ingest_env(a, runs={"ok": True, "result": {"runs": [ok1, ruim, ok3]}}, desde=None)
    assert a.orq("ingest").returncode == 0
    assert {e["ref"] for e in _entradas(a, "relatorio")} == {"run_1", "run_3"}
    auto = _cursor(a)["ingest"]["auto_desde"]
    assert auto == "2026-09-29T16:00:00Z", auto  # parou no run 1: o run 2 ainda vai ser tentado
    ruim["outputSnapshot"] = {"capturedAt": 1790697660000, "content": "agora sim"}
    _ingest_env(a, runs={"ok": True, "result": {"runs": [ok1, ruim, ok3]}}, desde=None)
    assert a.orq("ingest").returncode == 0
    assert {e["ref"] for e in _entradas(a, "relatorio")} == {"run_1", "run_2", "run_3"}, "o run que falhou entra na rodada seguinte"
    assert len([e for e in _entradas(a, "relatorio") if e["ref"] == "run_3"]) == 1, "sem duplicar o que já entrou"


def _transcrito(a, respostas):
    """Transcrito falso com um tool_result por (hora ISO, header, opções, resposta), no formato do toolUseResult real."""
    a.env["ORQ_TRANSCRITOS"] = os.path.join(a.tmp.name, "projetos")
    os.makedirs(a.env["ORQ_TRANSCRITOS"], exist_ok=True)
    caminho = os.path.join(a.env["ORQ_TRANSCRITOS"], "deee6621-3e71-5171-1b9a-b7292a3f80c4.jsonl")
    with open(caminho, "w") as f:
        f.write(json.dumps({"type": "assistant", "timestamp": "2026-09-25T15:00:00.000Z", "message": {"content": "sem answers"}}) + "\n")
        for quando, header, ops, resposta in respostas:
            q = {"question": f"pergunta de {header}?", "header": header, "multiSelect": False,
                 "options": [{"label": l, "description": "d"} for l in ops]}
            f.write(json.dumps({"type": "user", "timestamp": quando, "message": {"role": "user", "content": []},
                                "toolUseResult": {"questions": [q], "answers": {q["question"]: resposta}, "annotations": {}}}) + "\n")
        f.write("linha quebrada\n")
    return caminho


def test_auditar_respostas_lista_so_a_recomendada_a_ate_2s_de_uma_entrega_ao_coordenador():
    a = Amb()
    ops = ["Teto (Recomendado)", "Sem freio"]
    def m(seq, hora, **kw):
        return {**_msg(0, seq=seq, **kw), "created_at": hora, "delivered_at": hora}
    _transcrito(a, [
        ("2026-09-25T15:24:12.400Z", "Princípio", ops, "Teto (Recomendado)"),  # 1 s de um heartbeat de outro Run: suspeita
        ("2026-09-25T15:30:00.100Z", "Super admin", ops, "Sem freio"),  # outra opção: não
        ("2026-09-25T15:31:00.000Z", "Permissão", ops, "Teto (Recomendado)"),  # sem entrega perto: não
        ("2026-09-25T15:32:00.000Z", "Cache", ops, "Teto (Recomendado)"),  # só há mensagem para um worker: não
        ("2026-09-25T15:33:00.000Z", "Livre", ops, "Teto (Recomendado), mas depois eu vejo"),  # texto livre: não
        ("2026-09-25T15:34:00.000Z", "Longe", ops, "Teto (Recomendado)"),  # 3 s: fora da janela
    ])
    _inbox(a, m(1, "2026-09-25T15:24:13Z", run="run_b"), m(2, "2026-09-25T15:30:00Z"), m(3, "2026-09-25T15:32:00Z", para="dispatch:ctx_1"),
           m(4, "2026-09-25T15:33:00Z"), m(5, "2026-09-25T15:34:03Z"))
    antes = sorted(os.listdir(a.tmp.name))
    r = a.orq("auditar-respostas", "--sessao", "deee6621")
    assert r.returncode == 0, r.stderr
    assert "6 perguntas respondidas" in r.stdout and "4 escolheram só a opção recomendada, 1 delas" in r.stdout, r.stdout
    linhas = [l for l in r.stdout.splitlines() if l.startswith("| 2")]  # as linhas de dados começam com a data
    assert len(linhas) == 1 and "Princípio" in linhas[0] and "msg_1" in linhas[0] and "run_b" in linhas[0], r.stdout
    assert not any(h in r.stdout for h in ("Super admin", "Permissão", "Livre", "Longe")), r.stdout
    assert linhas[0].startswith("| 25/09/2026")
    assert sorted(os.listdir(a.tmp.name)) == antes and not os.path.exists(a.home), "só leitura: nada foi gravado"
    assert a.orq("auditar-respostas", "--sessao", "inexistente").returncode == 1
    assert "coordenador" in a.orq("auditar-respostas").stderr, "sem --sessao e sem cursor.json pede o id"


# ---------- terceiro review de 29/09 (review-3.md): um teste por achado ----------

PREAMBULO = "You are working inside Orca, a multi-agent IDE.\nYour coordinator's terminal handle is: term_x"
ABERTURA = "Please carry out this task from my Orca coordinator by following the brief I pasted below."


def _calls(a, cmd):
    return [c for c in _pos_despacho_sem_binding(a) if c[0] == cmd]


def _resolucoes(a):
    return [g[g.index("--resolution") + 1] for g in _gates_log(a) if g[0] == "gate-resolve"]


def test_review3_m5_decisao_fechada_em_outro_run_avisa_e_o_gate_espera_o_run_certo():
    a = Amb(run="run_a")
    a.prompt("decide o freio")
    _gate_pend(a)  # gate_1, criado com o coordenador ligado ao run_a
    (item,) = [i for i in _pend(a)["itens"] if i["id"] == "gate-dec"]
    assert item["gate"] == "gate_1" and item["gate_run"] == "run_a", item
    a.set("run.json", {"id": "run_b"})  # o coordenador foi para outra frente
    r = a.orq("pend", "done", "gate-dec", "--resposta", "Sim")
    assert r.returncode == 0, r
    assert "gate_1" in r.stderr and "run_a" in r.stderr and "run-use --id run_a" in r.stderr, r.stderr
    assert not _calls(a, "gate-resolve"), "o Orca recusaria: nem tenta"
    (done,) = [e for e in a.events() if e["tipo"] == "pend" and e["op"] == "done"]
    assert done["gate"] == "gate_1" and done["gate_run"] == "run_a"
    for _ in range(5):  # o ingest no Run errado não tenta e não gasta as 3 tentativas
        assert a.orq("ingest").returncode == 0
    assert not _calls(a, "gate-resolve") and not [e for e in a.events() if e["tipo"] == "gate_falha"]
    ctx = json.loads(a.prompt("e o gate?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "ainda pendentes" in ctx and "gate_1" in ctx and "run-use --id run_a" in ctx, ctx
    a.set("run.json", {"id": "run_a"})  # volta ao Run certo: o próximo ingest resolve
    a.orq("ingest")
    assert [e["gate"] for e in a.events() if e["tipo"] == "gate_resolvido"] == ["gate_1"] and _resolucoes(a) == ["Sim"]
    assert "ainda pendentes" not in json.loads(a.prompt("resolvido?").stdout)["hookSpecificOutput"]["additionalContext"]


def test_review3_m5_resposta_do_ask_com_gate_de_outro_run_avisa_no_contexto():
    a = Amb(run="run_a")
    q = _gate_pend(a)
    a.set("run.json", {"id": "run_b"})
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sim"}))
    ctx = json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    assert "gate_1" in ctx and "run-use --id run_a" in ctx, ctx
    assert "gate-dec" not in _ids_pend(a) and not _calls(a, "gate-resolve")


def test_review3_m5_gate_sem_run_guardado_continua_sendo_tentado():
    a = Amb(run="run_a")
    _gate_pend(a)
    a.orq("pend", "done", "gate-dec", "--resposta", "Sim", FAKE_FAIL="gate-resolve")  # recusa: fica sem gate_resolvido
    linhas = [json.loads(x) for x in open(os.path.join(a.home, "events.jsonl"))]
    with open(os.path.join(a.home, "events.jsonl"), "w") as f:  # evento antigo, de antes do gate_run
        for e in linhas:
            if e.get("tipo") == "pend" and e.get("op") == "done":
                e.pop("gate_run", None)
            if e.get("tipo") != "gate_falha":
                f.write(json.dumps(e) + "\n")
    antes = len(_calls(a, "gate-resolve"))
    a.set("run.json", {"id": "run_b"})
    a.orq("ingest")
    assert len(_calls(a, "gate-resolve")) > antes, "sem gate_run não há como saber: tenta como antes"


def test_review3_m6_pend_done_sem_resposta_usa_a_ultima_resposta_livre():
    a = Amb()
    q = _gate_pend(a, "dec1")
    txt = "Use B, mas só depois do deploy"
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: txt}))
    ctx = json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    assert 'orq pend done dec1 --resposta "<o que foi decidido>"' in ctx, ctx
    assert a.orq("pend", "done", "dec1").returncode == 0
    assert _resolucoes(a) == [txt], "o worker destravado recebe o texto do usuário"
    assert [e for e in a.events() if e["tipo"] == "pend" and e["op"] == "done"][0]["resposta"] == txt
    b = Amb()  # sem resposta livre, o texto antigo
    _gate_pend(b, "dec2")
    b.orq("pend", "done", "dec2")
    assert _resolucoes(b) == ["fechada sem resposta"]
    c = Amb()  # --resposta explícita ganha da resposta livre
    q = _gate_pend(c, "dec3")
    c.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "vamos ver"}))
    c.orq("pend", "done", "dec3", "--resposta", "decidi A")
    assert _resolucoes(c) == ["decidi A"]
    d = Amb()  # a resposta livre de uma pendência anterior com o mesmo id não vale para a nova
    q = _gate_pend(d, "dec4")
    d.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "texto velho"}))
    d.orq("pend", "done", "dec4", "--resposta", "fechei")
    _gate_pend(d, "dec4")
    d.orq("pend", "done", "dec4")
    assert _resolucoes(d) == ["fechei", "fechada sem resposta"]


def test_review3_b6_preambulo_sem_a_linha_de_abertura_tambem_e_despacho():
    o = orq_mod.origem
    assert o(PREAMBULO) == "despacho"
    assert o(ABERTURA + " " + PREAMBULO) == "despacho"
    assert o(ABERTURA + "\n" + PREAMBULO) == "despacho"
    assert o('<pasted_content id="abc123">\n' + PREAMBULO + "\n</pasted_content>") == "despacho"
    assert o(ABERTURA + '\n<pasted_content id="abc123">\n' + PREAMBULO) == "despacho"
    assert o("You are working inside Orca") == "usuario" and o("Explique: You are working inside Orca, a multi-agent IDE.") == "usuario"
    a = Amb(run="run_a")  # ponta a ponta: o papel de worker vem do preâmbulo puro, e o run-create dele não o torna coordenador
    assert a.prompt(PREAMBULO).stdout == ""
    assert _cursor(a)["papeis"]["abcdef123456"] == "worker"
    assert a.prompt("agora sigo o brief").stdout == "" and a.events() == []


def test_review3_b7_cursor_legivel_com_a_forma_errada_nao_desliga_o_orq():
    for conteudo in ("null", "[]", "42", '{"ingest": null}', '{"ingest": []}', '{"papeis": null, "runs": [], "chegada": 5}',
                     '{"papeis": [], "runs": null, "chegada": []}', '{"entrada": "x"}'):
        a = Amb()
        os.makedirs(a.home)
        open(os.path.join(a.home, "cursor.json"), "w").write(conteudo)
        r = a.prompt("oi")
        assert r.returncode == 0 and "entrada e1 (usuário)" in r.stdout, (conteudo, r.stdout, a.log())
        assert "Sem efeito" in a.orq("status").stdout
        s = a.orq("hook", "stop", stdin=json.dumps({"session_id": "abcdef123456"}))
        assert "sem efeito" in s.stdout, (conteudo, s.stdout, a.log())
        q = [_pergunta("freio-prod", OPCOES_FREIO)]
        a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sem freio"}))
        assert "freio-prod" not in _ids_pend(a), conteudo
        i = a.orq("ingest")
        assert i.returncode == 0, (conteudo, i.stderr)
        assert "AttributeError" not in a.log() and "TypeError" not in a.log(), (conteudo, a.log())


def test_review3_b8_grupo_de_worker_done_mostra_quantos_relatorios_e_o_mais_novo():
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)

    def entradas(*nomes):
        return [{"tipo": "entrada", "id": f"e{i}", "origem": "relatorio_worker", "texto": "x", "fonte": "worker x",
                 "caminho": f"/r/orquestrador-plan/{n}.md", "ref": f"m{i}"} for i, n in enumerate(nomes, 3)]
    ev = entradas("a", "b", "c", "d", "e")
    linha = orq_mod._extra(ev, orq_mod.abertas(ev), agora)
    assert "×5" in linha and "5 relatórios" in linha and "orquestrador-plan/e.md" in linha and "a.md" not in linha, linha
    ev = entradas("a", "a", "a")
    linha = orq_mod._extra(ev, orq_mod.abertas(ev), agora)
    assert "×3" in linha and "orquestrador-plan/a.md" in linha and "relatórios" not in linha, linha


def _orca_direto(a, *args):
    p = subprocess.run([a.bin, "orchestration", *args, "--json"], capture_output=True, text=True, env=a.env)
    return json.loads(p.stdout)


def test_review3_b9_orca_falso_recusa_gate_resolve_de_outro_run_e_desconhecido_e_da_um_id_por_gate():
    a = Amb(run="run_a")
    g1 = _orca_direto(a, "gate-create", "--task", "t1", "--question", "q")["result"]["gate"]
    g2 = _orca_direto(a, "gate-create", "--task", "t2", "--question", "q")["result"]["gate"]
    assert g1["id"] != g2["id"] and g1["run_id"] == "run_a" and g1["created_at"][4] == "-" and " " in g1["created_at"]
    assert _orca_direto(a, "gate-resolve", "--id", g1["id"], "--resolution", "x")["ok"] is True
    a.set("run.json", {"id": "run_b"})
    r = _orca_direto(a, "gate-resolve", "--id", g2["id"], "--resolution", "x")
    assert r["ok"] is False and "Gate not found" in r["error"]["message"], r
    assert _orca_direto(a, "gate-resolve", "--id", "gate_999", "--resolution", "x")["ok"] is False
    a.set("run.json", {"id": "run_a"})
    assert _orca_direto(a, "gate-resolve", "--id", g2["id"], "--resolution", "x")["ok"] is True


def test_review3_b10_nota_da_opcao_marcada_e_resposta_livre():
    a = Amb()
    q = _gate_pend(a, "dec1")
    ev = json.loads(_ask(q, {q[0]["question"]: "Sim"}))
    ev["tool_response"]["annotations"] = {q[0]["question"]: {"notes": "só se o deploy passar"}}
    r = a.orq("hook", "ask", stdin=json.dumps(ev))
    assert "dec1" in _ids_pend(a) and not _calls(a, "gate-resolve"), "a nota impede o fechamento"
    (res,) = [e for e in a.events() if e["tipo"] == "resposta"]
    assert res["livre"] is True and res["nota"] == "só se o deploy passar" and res["resposta"] == "Sim", res
    assert 'orq pend done dec1 --resposta "<o que foi decidido>"' in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    a.orq("pend", "done", "dec1")
    (res,) = _resolucoes(a)
    assert "Sim" in res and "só se o deploy passar" in res, res
    b = Amb()  # anotação sem notes (só preview) é a opção limpa: fecha
    q = _gate_pend(b, "dec2")
    ev = json.loads(_ask(q, {q[0]["question"]: "Sim"}))
    ev["tool_response"]["annotations"] = {q[0]["question"]: {"preview": "x"}}
    b.orq("hook", "ask", stdin=json.dumps(ev))
    assert "dec2" not in _ids_pend(b) and _resolucoes(b) == ["Sim"]
    c = Amb()  # e a nota também vale quando o harness a põe no tool_input
    q = _gate_pend(c, "dec3")
    ev = json.loads(_ask(q, {q[0]["question"]: "Sim"}, onde="tool_input"))
    ev["tool_input"]["annotations"] = {q[0]["question"]: {"notes": "depois"}}
    c.orq("hook", "ask", stdin=json.dumps(ev))
    assert "dec3" in _ids_pend(c)


# ---------- canal de decisão pelo Lavish (seção 16 do desenho) ----------

def _lote(a, itens, nome="poll.json", forma="data"):
    caminho = os.path.join(a.tmp.name, nome)
    doc = {"data": {"items": itens}} if forma == "data" else {"prompts": [{"tag": "tracked-batch", "data": {"items": itens}}]} \
        if forma == "prompts" else [{"tag": "tracked-batch", "data": {"items": itens}}]
    json.dump(doc, open(caminho, "w"))
    return caminho


def test_lavish_resposta_grava_um_evento_por_item_e_fecha_so_a_escolha_explicita():
    a = Amb()
    a.orq("pend", "add", "--id", "Indicador", "--tipo", "decisao", "--titulo", "Qual badge?", "--task", "task_1")
    itens = [{"id": "L1", "header": "freio-prod", "resposta": "Sem freio", "disposicao": "escolha"},
             {"id": "L2", "header": "Indicador", "resposta": "quero ver o protótipo antes", "disposicao": "livre"},
             {"id": "L3", "header": "sem-pendencia", "resposta": "A", "disposicao": "escolha"},
             {"id": "L4", "header": "avisar-x", "resposta": "ok", "disposicao": "escolha"},
             {"id": "L5", "header": "", "resposta": "", "disposicao": "adiar"}]
    arq = _lote(a, itens)
    r = a.orq("lavish-resposta", arq)
    assert r.returncode == 0, r.stderr
    evs = [e for e in a.events() if e["tipo"] == "resposta_lavish"]
    assert [e["item"] for e in evs] == ["L1", "L2", "L3", "L4", "L5"], "um evento por item"
    assert evs[0]["header"] == "freio-prod" and evs[0]["resposta"] == "Sem freio" and evs[0]["disposicao"] == "escolha" and "livre" not in evs[0]
    assert evs[1]["livre"] is True and evs[4]["livre"] is True
    assert sorted(_ids_pend(a)) == ["Indicador", "avisar-x"], "só a decisão com escolha explícita fecha; ação/aviso e texto livre não"
    (done,) = [e for e in a.events() if e["tipo"] == "pend" and e["op"] == "done"]
    assert done["pend"] == "freio-prod" and done["resposta"] == "Sem freio"
    assert 'orq pend done Indicador --resposta "<o que foi decidido>"' in r.stdout + r.stderr, "o aviso de resposta livre"
    saida = json.loads(r.stdout)
    assert [i["item"] for i in saida["itens"]] == ["L1", "L2", "L3", "L4", "L5"] and saida["itens"][0]["efeito"] == "fechou"
    ctx = json.loads(a.prompt("e o Indicador?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert 'resposta livre em Indicador: se decidiu, feche com orq pend done Indicador --resposta' in ctx, ctx
    n = len(a.events())
    assert a.orq("lavish-resposta", arq).returncode == 0 and len(a.events()) == n
    assert len([e for e in a.events() if e["tipo"] == "resposta_lavish"]) == 5, "o mesmo lote duas vezes não duplica"
    r = a.orq("lavish-resposta", _lote(a, [{"id": "L6", "header": "Indicador", "resposta": "A: badge", "disposicao": "escolha"}], "p2.json", "prompts"))
    assert r.returncode == 0 and "Indicador" not in _ids_pend(a), r.stderr
    assert _resolucoes(a) == ["A: badge"], "a escolha resolve o gate da decisão"
    assert "resposta livre" not in json.loads(a.prompt("fechou?").stdout)["hookSpecificOutput"]["additionalContext"]


def test_lavish_resposta_aceita_as_tres_formas_do_json_e_recusa_o_que_nao_e_lote():
    a = Amb()
    for i, forma in enumerate(("data", "prompts", "lista")):
        arq = _lote(a, [{"id": f"F{i}", "header": "x", "resposta": "y", "disposicao": "escolha"}], f"f{i}.json", forma)
        assert a.orq("lavish-resposta", arq).returncode == 0, forma
    assert [e["item"] for e in a.events() if e["tipo"] == "resposta_lavish"] == ["F0", "F1", "F2"]
    n = len(a.events())
    vazio = os.path.join(a.tmp.name, "vazio.json")
    open(vazio, "w").write('{"data": {"items": []}}')
    ruim = os.path.join(a.tmp.name, "ruim.json")
    open(ruim, "w").write("{quebrado")
    for arq in (vazio, ruim, os.path.join(a.tmp.name, "nao-existe.json")):
        r = a.orq("lavish-resposta", arq)
        assert r.returncode == 1 and "orq:" in r.stderr, (arq, r)
    assert len(a.events()) == n


def test_lavish_resposta_com_gate_de_outro_run_avisa():
    a = Amb(run="run_a")
    _gate_pend(a)
    a.set("run.json", {"id": "run_b"})
    r = a.orq("lavish-resposta", _lote(a, [{"id": "L1", "header": "gate-dec", "resposta": "Sim", "disposicao": "escolha"}]))
    assert r.returncode == 0 and "gate_1" in r.stderr and "run-use --id run_a" in r.stderr, r
    assert "gate-dec" not in _ids_pend(a) and not _calls(a, "gate-resolve")


# ---------- hook PreToolUse de AskUserQuestion (seção 16) ----------

def _guard(a, tool="AskUserQuestion", **env):
    return a.orq("hook", "guard", stdin=json.dumps({"session_id": "abcdef123456", "hook_event_name": "PreToolUse", "tool_name": tool,
                                                      "tool_input": {"questions": []}}), **env)


def _workers(a, *linhas):
    a.set("workers.json", [{"handle": h, "run": r, "status": s} for h, r, s in linhas])


def test_guard_recusa_a_caixa_com_despacho_ativo_em_qualquer_run():
    a = Amb(run="run_a")
    _workers(a, ("w_ativo", "run_b", "dispatched"), ("w_velho", "run_a", "completed"))  # o ativo está em outro Run que o ligado
    t = time.time()
    r = _guard(a)
    assert r.returncode == 0 and time.time() - t < 2, r
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["hookEventName"] == "PreToolUse" and out["permissionDecision"] == "deny"
    motivo = out["permissionDecisionReason"]
    assert "Lavish" in motivo and "orq lavish-resposta" in motivo and "1 despacho ativo" in motivo and "run_b" in motivo, motivo
    lista = _calls(a, "worker-list")
    assert lista and "--run" not in lista[0], "lista de todos os Runs: o worker-list escopado só veria o Run ligado"


def test_guard_libera_sem_despacho_ativo_e_fora_do_orca():
    a = Amb(run="run_a")
    _workers(a, ("w1", "run_a", "completed"), ("w2", "run_b", "failed"))
    r = _guard(a)
    assert (r.returncode, r.stdout) == (0, ""), r
    b = Amb(run="run_a")
    _workers(b, ("w1", "run_b", "dispatched"))
    b.prompt(PREAMBULO)  # esta sessão é de worker
    assert "deny" in _guard(b).stdout and _calls(b, "worker-list") == [], "worker não usa a caixa (ticket 52: escala pelo Orca) e nem chama o Orca"
    c = Amb(run="run_a")
    _workers(c, ("w1", "run_b", "dispatched"))
    r = _guard(c, ORCA_TERMINAL_HANDLE="")
    assert (r.returncode, r.stdout, _pos_despacho_sem_binding(c)) == (0, "", [])
    d = Amb(run="run_a")
    _workers(d, ("w1", "run_b", "dispatched"))
    assert _guard(d, tool="Bash").stdout == ""
    e = Amb(run=None)  # sem Run ligado não é coordenador
    _workers(e, ("w1", "run_b", "dispatched"))
    assert _guard(e).stdout == ""


def test_guard_falha_aberto_e_loga():
    a = Amb(run="run_a")
    _workers(a, ("w1", "run_b", "dispatched"))
    r = _guard(a, FAKE_CRASH="1")
    assert (r.returncode, r.stdout) == (0, "") and "hook guard" in a.log(), (r, a.log())
    b = Amb(run="run_a")
    _workers(b, ("w1", "run_b", "dispatched"))
    t = time.time()
    r = _guard(b, FAKE_SLEEP="6")
    assert (r.returncode, r.stdout) == (0, "") and time.time() - t < 4.5 and "hook guard" in b.log()
    c = Amb(run="run_a")
    _workers(c, ("w1", "run_b", "dispatched"))
    c.env["ORQ_ORCA"] = "/nao/existe/orca"
    r = _guard(c)
    assert (r.returncode, r.stdout) == (0, "") and "hook guard: FileNotFoundError" in c.log()
    d = Amb(run="run_a")
    r = d.orq("hook", "guard", stdin="isto não é json")
    assert (r.returncode, r.stdout) == (0, "") and "hook guard" in d.log()


def test_guard_cache_curto_evita_repetir_a_lista_e_expira():
    a = Amb(run="run_a")
    _workers(a, ("w1", "run_b", "dispatched"))
    assert "deny" in _guard(a).stdout and "deny" in _guard(a).stdout
    assert len(_calls(a, "worker-list")) == 1, "a segunda caixa lê o cache"
    cache = os.path.join(a.home, "ativos.json")
    c = json.load(open(cache))
    c["t"] -= orq_mod.ASK_GUARD_TTL + 1
    json.dump(c, open(cache, "w"))
    _workers(a)  # o worker terminou
    assert _guard(a).stdout == "" and len(_calls(a, "worker-list")) == 2


def test_guard_le_a_segunda_pagina_e_o_arquivo_off_libera():
    a = Amb(run="run_a")
    _workers(a, *[(f"w{i}", "run_b", "completed") for i in range(120)], ("w_ativo", "run_c", "dispatched"))
    r = _guard(a)
    assert "deny" in r.stdout and "run_c" in r.stdout and len(_calls(a, "worker-list")) == 2, r
    os.remove(os.path.join(a.home, "ativos.json"))
    open(os.path.join(a.home, "ask-guard.off"), "w").close()  # saída de emergência: despacho preso que nunca termina
    assert _guard(a).stdout == ""



# ---------- heartbeat absorvido (fatia 7) ----------

AVISO_A = "You have 1 orchestration message. Run `orca orchestration check --run run_a`."


def _hb(fase, dispatch="ctx_1", task="task_1"):
    return ("heartbeat", {"taskId": task, "dispatchId": dispatch, "phase": fase})


def _chamadas_check(a):
    return _calls(a, "check")


def _bloqueado(r):
    return r.returncode == 0 and json.loads(r.stdout).get("decision") == "block"


def test_heartbeat_so_heartbeats_bloqueia_e_confirma():
    a = Amb()
    a.caixa(_hb("lendo"), _hb("testando"))
    r = a.prompt(AVISO_A)
    out = json.loads(r.stdout)
    assert _bloqueado(r) and out["reason"] == f"{orq_mod.MARCA} 2 sinais de vida absorvidos (run_a)" and "\n" not in out["reason"] and "hookSpecificOutput" not in out, r
    assert set(a.estados().values()) == {"acked"}, "consumiu e confirmou o lote"
    calls = _chamadas_check(a)
    assert [("--peek" in c, "--ack" in c) for c in calls] == [(True, False), (False, False), (False, True)], calls
    assert a.log() == ""


def test_heartbeat_grava_o_evento_com_run_ids_e_fase_de_cada_um():
    a = Amb()
    a.caixa(_hb("lendo", "ctx_1", "task_1"), _hb("testando", "ctx_2", "task_2"))
    assert _bloqueado(a.prompt(AVISO_A))
    (ev,) = [e for e in a.events() if e["tipo"] == "heartbeat_absorvido"]
    assert ev["run"] == "run_a" and ev["entregas"] == ["delivery_1"]
    assert [(h["msg"], h["task"], h["dispatch"], h["fase"]) for h in ev["heartbeats"]] == [
        ("msg_1", "task_1", "ctx_1", "lendo"), ("msg_2", "task_2", "ctx_2", "testando")]
    assert all(h["ts"].startswith("2026-09-29T17:3") for h in ev["heartbeats"])
    assert not any(e["tipo"] == "entrada" for e in a.events()), "absorvido não vira entrada nem ingest"


def test_heartbeat_misto_passa_sem_consumir_nem_confirmar():
    a = Amb()
    a.caixa(_hb("lendo"), ("worker_done", {"taskId": "task_1"}))
    r = a.prompt(AVISO_A)
    assert (r.returncode, r.stdout) == (0, ""), r
    assert set(a.estados().values()) == {"unread"}
    assert [("--peek" in c) for c in _chamadas_check(a)] == [True], "só olhou com --peek"
    assert not [e for e in a.events() if e["tipo"] == "heartbeat_absorvido"]
    assert json.load(open(os.path.join(a.home, "cursor.json")))["chegada"]["origem"] == "orca", "o aviso segue o caminho de sempre"


def test_heartbeat_tipo_desconhecido_passa():
    for tipo in ("question", "escalation", "decision_gate", "tipo_novo_do_orca", None):
        a = Amb()
        a.caixa(_hb("lendo"), (tipo, {}))
        r = a.prompt(AVISO_A)
        assert (r.returncode, r.stdout) == (0, ""), (tipo, r)
        assert set(a.estados().values()) == {"unread"}, tipo
    a = Amb()
    a.caixa((None, {}))
    assert a.prompt(AVISO_A).stdout == "" and set(a.estados().values()) == {"unread"}


def test_heartbeat_run_diferente_do_ligado_passa_sem_chamar_o_check():
    a = Amb(run="run_a")
    a.caixa(_hb("lendo"), run="run_b")
    r = a.prompt("You have 1 orchestration message. Run `orca orchestration check --run run_b`.")
    assert (r.returncode, r.stdout) == (0, ""), r
    assert _chamadas_check(a) == [], "consumer_fenced evitado: nem tentou"
    assert a.estados() == {"msg_1": "unread"}


def test_heartbeat_aviso_sem_run_passa():
    a = Amb()
    a.caixa(_hb("lendo"))
    r = a.prompt("You have 2 orchestration messages. Run `orca orchestration check`")
    assert (r.returncode, r.stdout) == (0, "") and _chamadas_check(a) == [] and a.estados() == {"msg_1": "unread"}


def test_heartbeat_orca_fora_do_ar_passa():
    for falha in ({"FAKE_FAIL": "check"}, {"FAKE_CRASH": "1"}, {"FAKE_SLEEP_CMD": "check:6"}):
        a = Amb(**falha)
        a.caixa(_hb("lendo"))
        t = time.time()
        r = a.prompt(AVISO_A)
        assert (r.returncode, r.stdout) == (0, "") and time.time() - t < 4.5, (falha, r)
        assert set(a.estados().values()) == {"unread"}, falha
        assert not [e for e in a.events() if e["tipo"] == "heartbeat_absorvido"]


def test_heartbeat_falha_no_ack_passa_e_nada_e_gravado():
    a = Amb(FAKE_FAIL_ACK="1")
    a.caixa(_hb("lendo"))
    r = a.prompt(AVISO_A)
    assert (r.returncode, r.stdout) == (0, ""), r
    assert not [e for e in a.events() if e["tipo"] == "heartbeat_absorvido"] and "heartbeat:" in a.log()
    assert a.estados() == {"msg_1": "out"}, "consumido e sem ack: o Orca repete a entrega no check do coordenador"


def test_heartbeat_falha_do_orca_no_aviso_ainda_dispara_o_ingest_e_a_chegada():
    a = Amb(FAKE_FAIL="check")
    a.caixa(_hb("lendo"))
    a.prompt(AVISO_A)
    assert json.load(open(os.path.join(a.home, "cursor.json")))["chegada"]["origem"] == "orca"


def test_heartbeat_lotes_de_heartbeat_seguidos_entram_na_mesma_passada():
    a = Amb()
    a.caixa(_hb("a"), _hb("b"), ("heartbeat", {"taskId": "task_1", "dispatchId": "ctx_1", "phase": "c"}, "high"))
    assert _bloqueado(a.prompt(AVISO_A))
    (ev,) = [e for e in a.events() if e["tipo"] == "heartbeat_absorvido"]
    assert ev["entregas"] == ["delivery_1", "delivery_2"] and [h["fase"] for h in ev["heartbeats"]] == ["a", "b", "c"]
    assert set(a.estados().values()) == {"acked"}


def test_heartbeat_lote_com_worker_done_no_meio_passa_e_nada_e_consumido():
    a = Amb()
    a.caixa(_hb("a"), ("worker_done", {"taskId": "t"}), _hb("b"))
    r = a.prompt(AVISO_A)
    assert (r.returncode, r.stdout) == (0, ""), "há worker_done na caixa: o aviso passa"
    assert set(a.estados().values()) == {"unread"}


def test_heartbeat_mensagem_que_chega_entre_o_peek_e_o_lote_seguinte_fica_para_o_coordenador():
    a = Amb(FAKE_ARRIVE_ON_CHECK="worker_done")
    a.caixa(_hb("a"))
    assert _bloqueado(a.prompt(AVISO_A)), "o heartbeat que o peek viu sai"
    assert a.estados() == {"msg_1": "acked", "msg_chegou": "out"}, "o worker_done ficou em aberto, sem ack"
    (ev,) = [e for e in a.events() if e["tipo"] == "heartbeat_absorvido"]
    assert [h["msg"] for h in ev["heartbeats"]] == ["msg_1"]
    r = subprocess.run([a.bin, "orchestration", "check", "--run", "run_a", "--json"], capture_output=True, text=True, env=a.env)
    m = json.loads(r.stdout)["result"]["messages"]
    assert [x["id"] for x in m] == ["msg_chegou"], "o coordenador ainda recebe o worker_done"


def test_heartbeat_aviso_atrasado_de_lote_ja_absorvido_tambem_bloqueia():
    a = Amb()
    a.caixa(_hb("a"), _hb("b"))
    assert _bloqueado(a.prompt(AVISO_A))
    r = a.prompt("You have 1 orchestration message. Run `orca orchestration check --run run_a`.")  # o segundo aviso da fila
    assert _bloqueado(r), "caixa vazia logo depois de um lote absorvido: é o aviso do lote que já saiu"
    assert len([e for e in a.events() if e["tipo"] == "heartbeat_absorvido"]) == 1, "o atrasado não grava evento"
    c = json.load(open(os.path.join(a.home, "cursor.json")))
    c["hb_absorvido"]["run_a"] -= orq_mod.HB_JANELA_S + 1
    json.dump(c, open(os.path.join(a.home, "cursor.json"), "w"))
    r = a.prompt(AVISO_A)
    assert (r.returncode, r.stdout) == (0, ""), "passou a janela: caixa vazia sem lote recente passa"


def test_heartbeat_caixa_vazia_sem_lote_recente_passa():
    a = Amb()
    r = a.prompt(AVISO_A)
    assert (r.returncode, r.stdout) == (0, "") and not a.events()


def test_heartbeat_mensagem_nova_depois_do_lote_absorvido_passa():
    a = Amb()
    a.caixa(_hb("a"))
    assert _bloqueado(a.prompt(AVISO_A))
    a.caixa(("worker_done", {"taskId": "t"}))
    r = a.prompt(AVISO_A)
    assert (r.returncode, r.stdout) == (0, ""), "a janela só vale para a caixa vazia: worker_done acorda o coordenador"
    assert a.estados()["msg_2"] == "unread"


def test_heartbeat_worker_nao_absorve_nem_chama_o_check():
    a = Amb(run=None)
    a.caixa(_hb("a"))
    r = a.prompt(AVISO_A)
    assert (r.returncode, r.stdout) == (0, "") and _chamadas_check(a) == []


def test_heartbeat_so_o_aviso_do_orca_e_absorvido():
    a = Amb()
    a.caixa(_hb("a"))
    r = a.prompt("por favor rode orca orchestration check --run run_a")  # mensagem do usuário que cita o comando
    assert "additionalContext" in r.stdout and _chamadas_check(a) == [] and a.estados() == {"msg_1": "unread"}


def test_resumo_mostra_fase_e_hora_do_heartbeat_de_cada_dispatch_rodando():
    a = Amb()
    a.caixa(_hb("lendo", "ctx_1", "task_aaaaaaaaaa"), _hb("testando", "ctx_1", "task_aaaaaaaaaa"))
    assert _bloqueado(a.prompt(AVISO_A))
    a.set("../orq/aberto.json", {"ts": "2026-09-29T17:30:00Z", "backlog": [], "rodando": 2, "bloqueado": [], "gates": [], "falhas": [],
                                 "andamento": [{"task": "task_aaaaaaaaaa", "dispatch": "ctx_1", "run": "run_a", "titulo": "x"},
                                               {"task": "task_bbbbbbbbbb", "dispatch": "ctx_2", "run": "run_a", "titulo": "y"}]})
    ctx = json.loads(a.prompt("cria a task").stdout)["hookSpecificOutput"]["additionalContext"]
    linhas = ctx.splitlines()
    assert len(linhas) <= 5
    l2 = next(l for l in linhas if l.startswith("Aberto"))
    assert "Vivos: task_aaaa… testando " in l2 and "task_bbbb… sem heartbeat" in l2 and "lendo" not in l2, l2


def test_resumo_sem_despacho_rodando_nao_ganha_a_linha_de_vivos():
    ab = {"ts": "2026-09-29T17:30:00Z", "backlog": [], "rodando": 0, "andamento": [], "bloqueado": [], "gates": [], "falhas": []}
    assert "Vivos" not in orq_mod.resumo([], ab, {"itens": []})
    assert "Vivos" not in orq_mod.resumo([], {**ab, "andamento": None}, {"itens": []})
    assert orq_mod.linha_vivos([], None) == ""


def test_monta_aberto_lista_os_despachos_rodando():
    agora = orq_mod.datetime.now(orq_mod.timezone.utc)
    tasks = [{"id": "t1", "status": "dispatched", "dispatch_id": "ctx_9", "task_title": "T", "created_at": "2026-09-29T10:00:00Z"},
             {"id": "t2", "status": "ready", "task_title": "U", "created_at": "2026-09-29T10:00:00Z"}]
    ab = orq_mod.monta_aberto([({"id": "run_a", "objective": "F"}, tasks, [])], agora)
    assert ab["rodando"] == 1 and ab["andamento"] == [{"task": "t1", "dispatch": "ctx_9", "run": "run_a", "titulo": "T"}]


def test_so_heartbeats_puro():
    assert orq_mod.so_heartbeats([{"type": "heartbeat"}]) and orq_mod.so_heartbeats([{"type": "heartbeat"}, {"type": "heartbeat"}])
    assert not orq_mod.so_heartbeats([]) and not orq_mod.so_heartbeats(None)
    assert not orq_mod.so_heartbeats([{"type": "heartbeat"}, {"type": "worker_done"}])
    assert not orq_mod.so_heartbeats([{"type": "heartbeat"}, {}]) and not orq_mod.so_heartbeats([{"type": "heartbeat"}, "x"])


WAITER = os.path.join(AQUI, "scripts", "orca-wait-runs.py")


def _waiter(a, *runs):
    return subprocess.run([sys.executable, WAITER, *runs], capture_output=True, text=True, timeout=30,
                          env={**a.env, "ORQ_WAIT_POLL": "0.05", "ORQ_WAIT_MAX": "0.4"})


def _check_puro(a, *args):
    r = subprocess.run([a.bin, "orchestration", "check", "--run", "run_a", *args, "--json"], capture_output=True, text=True, env=a.env)
    return json.loads(r.stdout)


def test_waiter_confirma_heartbeat_grava_o_evento_e_segue_esperando():
    a = Amb()
    a.caixa(_hb("lendo"), _hb("testando"))
    r = _waiter(a)
    assert r.returncode == 0 and json.loads(r.stdout) == {"timeout": True}, r
    assert set(a.estados().values()) == {"acked"}
    (ev,) = [e for e in a.events() if e["tipo"] == "heartbeat_absorvido"]
    assert [h["fase"] for h in ev["heartbeats"]] == ["lendo", "testando"]


def test_waiter_confirma_o_heartbeat_e_imprime_o_worker_done_na_mesma_volta():
    a = Amb()
    a.caixa(_hb("lendo"), ("worker_done", {"taskId": "t"}))
    r = _waiter(a)
    out = json.loads(r.stdout)
    assert out["run"] == "run_a" and [m["type"] for m in out["messages"]] == ["worker_done"], r
    assert a.estados() == {"msg_1": "acked", "msg_2": "out"}, "worker_done impresso e sem ack: o check do coordenador o repete"
    assert _check_puro(a)["result"]["messages"][0]["id"] == "msg_2", "e o check do coordenador o recebe"


def test_waiter_e_hook_juntos_nao_fazem_double_ack_nem_perdem_mensagem():
    a = Amb()
    a.caixa(_hb("lendo"))
    d = _check_puro(a)["result"]["deliveryId"]  # o waiter consumiu o lote e ainda não confirmou
    assert _bloqueado(a.prompt(AVISO_A)), "o hook pega a mesma entrega (repetida) e confirma"
    a.caixa(("worker_done", {"taskId": "t"}))  # chega depois
    tardio = _check_puro(a, "--ack", d)  # o ack tardio do waiter, do mesmo lote
    assert tardio["ok"] and tardio["result"]["acknowledged"] == d, "confirmar de novo o mesmo lote é inofensivo"
    assert [m["id"] for m in tardio["result"]["messages"]] == ["msg_2"], "e devolve o worker_done, que continua para o coordenador"
    assert a.estados() == {"msg_1": "acked", "msg_2": "out"}, "o ack repetido não confirmou o lote seguinte"
    r = _waiter(a)
    assert [m["id"] for m in json.loads(r.stdout)["messages"]] == ["msg_2"], "a mensagem não se perdeu"


def test_waiter_sem_orq_confirma_sozinho_como_antes():
    a = Amb()
    a.caixa(_hb("lendo"))
    env = {**a.env, "ORQ_WAIT_POLL": "0.05", "ORQ_WAIT_MAX": "0.3", "PYTHONPATH": ""}
    src = open(WAITER).read().replace('sys.path.insert(0, os.path.expanduser("~/.claude/orq"))', 'sys.path.insert(0, "/nao/existe")')
    p = os.path.join(a.tmp.name, "w.py")
    open(p, "w").write(src)
    r = subprocess.run([sys.executable, p], capture_output=True, text=True, timeout=30, env=env)
    assert a.estados() == {"msg_1": "acked"}, r.stdout


# ---------- quarto review de 29/09 (review-4.md): um teste por achado ----------

POLL_ESPERADO = {"poll-escolha-simples.txt": 1, "poll-texto-livre.txt": 1, "poll-escolha-com-limite.txt": 1, "poll-lote-nao-sei.txt": 17,
                 "poll-fatia-a.txt": 1, "poll-fatia-b.txt": 1, "poll-escolha-com-opcao.txt": 1, "poll-lote-revisao.txt": 17, "poll-tickets.txt": 1}


def _poll_a_mao(caminho):
    """A extração que se fazia com uma regex: os itens do JSON depois de "Context data:" dentro do prompt."""
    import re
    itens = []
    for m in re.finditer(r'"(?:[^"\\\n]|\\.)*"', open(caminho).read()):
        s = json.loads(m.group(0))
        if "\n\nContext data:\n" in s:
            itens += json.loads(s.split("\n\nContext data:\n", 1)[1])["items"]
    return itens


def test_review4_a1_lavish_resposta_le_a_saida_crua_de_cada_poll_de_exemplo():
    assert {f for f in os.listdir(FIX) if f.startswith("poll-")} == set(POLL_ESPERADO)
    for nome, n in POLL_ESPERADO.items():
        a = Amb()
        r = a.orq("lavish-resposta", os.path.join(FIX, nome))
        assert r.returncode == 0, (nome, r.stderr)
        assert len(json.loads(r.stdout)["itens"]) == n and len([e for e in a.events() if e["tipo"] == "resposta_lavish"]) == n, nome


def test_review4_a1_o_texto_livre_sai_intacto_do_poll():
    a = Amb()
    assert a.orq("lavish-resposta", os.path.join(FIX, "poll-texto-livre.txt")).returncode == 0
    (e,) = [e for e in a.events() if e["tipo"] == "resposta_lavish"]
    assert e["resposta"] == 'Deixar como está | Vamos deixar como está por enquanto, pois isso teoricamente foi uma "correcao" feita pela alice', e
    assert e["header"] == "item-82" and e["disposicao"] == "livre"
    b = Amb()
    b.orq("lavish-resposta", os.path.join(FIX, "poll-lote-revisao.txt"))
    por_id = {e["item"]: e for e in b.events() if e["tipo"] == "resposta_lavish"}
    itens = [i for i in _poll_a_mao(os.path.join(FIX, "poll-lote-revisao.txt")) if i.get("resposta") != "__conversar"]
    assert itens and all(por_id[i["id"]]["resposta"] == str(i.get("resposta") or "").strip() for i in itens)


def test_review4_a1_saida_crua_e_json_extraido_sao_o_mesmo_lote():
    a = Amb()
    bruto = os.path.join(FIX, "poll-lote-nao-sei.txt")
    assert a.orq("lavish-resposta", bruto).returncode == 0
    n = len(a.events())
    extraido = os.path.join(a.tmp.name, "extraido.json")
    json.dump({"data": {"items": _poll_a_mao(bruto)}}, open(extraido, "w"), ensure_ascii=False)
    r = a.orq("lavish-resposta", extraido)
    assert r.returncode == 0 and len(a.events()) == n, "o mesmo lote em outra forma não duplica"
    assert {i["efeito"] for i in json.loads(r.stdout)["itens"]} == {"repetido"}
    assert a.orq("lavish-resposta", bruto).returncode == 0 and len(a.events()) == n


def test_review4_a1_le_o_poll_da_entrada_padrao_e_o_json_puro_continua_valendo():
    a = Amb()
    r = a.orq("lavish-resposta", "-", stdin=open(os.path.join(FIX, "poll-fatia-a.txt")).read())
    assert r.returncode == 0 and json.loads(r.stdout)["itens"][0]["item"] == "fatia-a", r
    prompt_json = os.path.join(a.tmp.name, "p.json")
    corpo = "Lote\n\nContext data:\n" + json.dumps({"items": [{"id": "J1", "header": "x", "resposta": "y", "disposicao": "escolha"}]}, indent=2)
    json.dump({"prompts": [{"uid": "1", "prompt": corpo}]}, open(prompt_json, "w"))
    assert a.orq("lavish-resposta", prompt_json).returncode == 0, "prompt de um JSON puro também traz o Context data"
    assert [e["item"] for e in a.events() if e["tipo"] == "resposta_lavish"] == ["fatia-a", "J1"]
    sem = os.path.join(a.tmp.name, "sem.txt")
    open(sem, "w").write('session:\n  status: feedback\nprompts[1]{uid,prompt}:\n  "1","sem lote nenhum"\n')
    r = a.orq("lavish-resposta", sem)
    assert r.returncode == 1 and "poll" in r.stderr, r


# ---- A2: relatório de automation ----

def _iso_min(minutos):
    return (time.time() - minutos * 60) * 1000


def _run_cache(a, criado_min=10, snapshot=True, conteudo=None, base=REPO_FIX, plantar=True):
    """O run de exemplo da automation de cache, com os carimbos deslocados para agora e o relatório apontando para as fixtures."""
    r = json.load(open(os.path.join(FIX, "automation-run-cache.json")))
    r["createdAt"] = _iso_min(criado_min)
    r["runContext"]["path"] = base
    if snapshot:
        r["outputSnapshot"]["capturedAt"] = _iso_min(max(criado_min - 8, 0))
        if conteudo is not None:
            r["outputSnapshot"]["content"] = conteudo
    else:
        r.pop("outputSnapshot", None)
    runs = {"ok": True, "result": {"runs": [r]}}
    if plantar:
        _ingest_env(a, runs=runs, inbox={"ok": True, "result": {"messages": []}})
    else:  # o mesmo run visto de novo, com o cursor e o log como estão
        a.set("automations_runs.json", runs)
    return r


TARDE = os.path.join(REPO_FIX, ".scratch", "acompanhamento-cache", "2026-09-29-tarde.md")


def test_review4_a2_run_completed_sem_snapshot_espera_e_entra_com_os_seis_itens_uma_vez_so():
    a = Amb()
    r = _run_cache(a, snapshot=False)
    assert a.orq("ingest").returncode == 0
    assert _entradas(a, "relatorio") == []
    ing = _cursor(a)["ingest"]
    assert r["id"] not in ing["runs"] and "auto_desde" not in ing, "run sem snapshot ainda não terminou: não é visto e não segura o resto"
    r2 = _run_cache(a, plantar=False)  # o agente terminou e o Orca capturou o resultado
    a.orq("ingest")
    rel = _entradas(a, "relatorio")
    assert len(rel) == 6 and {e["caminho"] for e in rel} == {TARDE} and rel[0]["texto"].startswith("Decidir o que fazer com a janela de promoção"), rel
    n = len(a.events())
    a.orq("ingest")
    a.orq("ingest")
    assert len(a.events()) == n, "seis itens uma vez só"
    assert r2["id"] in _cursor(a)["ingest"]["runs"]


def test_review4_a2_arquivo_citado_que_ainda_nao_existe_espera_ate_aparecer():
    a = Amb()
    base = os.path.join(a.tmp.name, "repo")
    _run_cache(a, base=base)
    a.orq("ingest")
    assert _entradas(a, "relatorio") == [], "o snapshot cita o arquivo, mas ele ainda não foi gravado"
    os.makedirs(os.path.join(base, ".scratch", "acompanhamento-cache"))
    open(os.path.join(base, ".scratch", "acompanhamento-cache", "2026-09-29-tarde.md"), "w").write(open(TARDE).read())
    a.orq("ingest")
    assert len(_entradas(a, "relatorio")) == 6


def test_review4_a2_depois_de_uma_hora_sem_arquivo_desiste_e_grava_o_item_ler():
    a = Amb()
    _run_cache(a, criado_min=120, base=os.path.join(a.tmp.name, "repo"))
    a.orq("ingest")
    (e,) = _entradas(a, "relatorio")
    assert e["texto"] == "ler 2026-09-29-tarde.md", e
    b = Amb()
    _run_cache(b, criado_min=120, snapshot=False)
    b.orq("ingest")
    (e,) = _entradas(b, "relatorio")
    assert "sem arquivo de relatório" in e["texto"] and not e.get("caminho"), e


def test_review4_a2_snapshot_sem_caminho_espera_e_depois_procura_o_arquivo_da_data_do_run():
    a = Amb()
    base = os.path.join(a.tmp.name, "repo")
    r = _run_cache(a, conteudo="Terminei. O relatório ficou gravado, veja o engram.", base=base)
    a.orq("ingest")
    assert _entradas(a, "relatorio") == [], "sem caminho e com o run novo: espera"
    hoje = time.strftime("%Y-%m-%d", time.gmtime(r["createdAt"] / 1000))
    pasta = os.path.join(base, ".scratch", "acompanhamento-cache")
    os.makedirs(pasta)
    open(os.path.join(pasta, f"{hoje}-tarde.md"), "w").write(open(TARDE).read())
    open(os.path.join(pasta, "2020-01-01-velho.md"), "w").write("# velho\n")
    a.orq("ingest")
    rel = _entradas(a, "relatorio")
    assert len(rel) == 6 and rel[0]["caminho"] == os.path.join(pasta, f"{hoje}-tarde.md"), rel
    b = Amb()  # dois candidatos do mesmo dia: não chuta
    base_b = os.path.join(b.tmp.name, "repo")
    _run_cache(b, conteudo="Terminei.", base=base_b)
    pasta_b = os.path.join(base_b, ".scratch", "x")
    os.makedirs(pasta_b)
    for n in ("a", "b"):
        open(os.path.join(pasta_b, f"{hoje}-{n}.md"), "w").write(open(TARDE).read())
    b.orq("ingest")
    assert _entradas(b, "relatorio") == []


def test_review4_a2_run_ja_gravado_sem_caminho_ganha_os_itens_quando_o_arquivo_aparece():
    a = Amb()
    r = _run_cache(a, criado_min=20, snapshot=False)
    os.makedirs(a.home, exist_ok=True)
    velho = {"ts": "2026-09-29T17:05:11Z", "tipo": "entrada", "origem": "relatorio", "id": "e47", "fonte": r["title"], "ref": r["id"], "item": 1,
             "texto": "ler o resultado do run (sem arquivo de relatório)"}
    open(os.path.join(a.home, "events.jsonl"), "w").write(json.dumps(velho) + "\n")
    json.dump({"entrada": 47, "ingest": {"desde": DESDE_CEDO, "inbox_seq": 0, "runs": [r["id"]], "auto_desde": "2026-09-29T00:00:01Z"}},
              open(os.path.join(a.home, "cursor.json"), "w"))
    _run_cache(a, criado_min=20, plantar=False)
    a.orq("ingest")
    novos = [e for e in _entradas(a, "relatorio") if e.get("caminho")]
    assert len(novos) == 6 and novos[0]["id"] == "e48", novos
    (fim,) = [e for e in a.events() if e["tipo"] == "intake"]
    assert fim["entrada"] == "e47" and fim["efeito"] == "descartado" and "tarde.md" in fim["nota"], fim
    assert [e["id"] for e in orq_mod.abertas(a.events())] == [e["id"] for e in novos]
    n = len(a.events())
    a.orq("ingest")
    assert len(a.events()) == n
    b = Amb()  # e47 já fechado à mão: os itens entram e nada mais é fechado
    _run_cache(b, criado_min=20, snapshot=False)
    os.makedirs(b.home, exist_ok=True)
    open(os.path.join(b.home, "events.jsonl"), "w").write(json.dumps(velho) + "\n" + json.dumps({"tipo": "intake", "entrada": "e47", "efeito": "conversa"}) + "\n")
    json.dump({"entrada": 47, "ingest": {"desde": DESDE_CEDO, "inbox_seq": 0, "runs": [r["id"]]}}, open(os.path.join(b.home, "cursor.json"), "w"))
    _run_cache(b, criado_min=20, plantar=False)
    b.orq("ingest")
    assert len([e for e in _entradas(b, "relatorio") if e.get("caminho")]) == 6 and len([e for e in b.events() if e["tipo"] == "intake"]) == 1


# ---- M7: contrato da disposicao ----

def test_review4_m7_o_motivo_da_recusa_diz_quais_disposicoes_fecham():
    a = Amb(run="run_a")
    _workers(a, ("w_ativo", "run_b", "dispatched"))
    motivo = json.loads(_guard(a).stdout)["hookSpecificOutput"]["permissionDecisionReason"]
    assert 'disposicao "escolha"' in motivo and "livre" in motivo and "adiar" in motivo and "saída do poll" in motivo, motivo
    assert "JSON devolvido" not in motivo


def test_review4_m7_manter_e_trocar_com_resposta_fecham_e_conversar_adia_sem_virar_resolucao():
    a = Amb()
    for i in ("D-man", "D-troc", "D-conv", "D-vazio"):
        a.orq("pend", "add", "--id", i, "--tipo", "decisao", "--titulo", "Q?", "--task", "task_1")
    r = a.orq("lavish-resposta", _lote(a, [
        {"id": "1", "header": "D-man", "resposta": "Manter", "disposicao": "manter"},
        {"id": "2", "header": "D-troc", "resposta": "Trocar por B", "disposicao": "trocar"},
        {"id": "3", "header": "D-conv", "resposta": "__conversar", "disposicao": "conversar"},
        {"id": "4", "header": "D-vazio", "resposta": "", "disposicao": "manter"}]))
    assert r.returncode == 0, r.stderr
    assert sorted(_ids_pend(a)) == ["D-conv", "D-vazio", "avisar-x", "freio-prod"], "manter e trocar com resposta fecham; sem resposta e conversar não"
    ev = {e["item"]: e for e in a.events() if e["tipo"] == "resposta_lavish"}
    assert ev["3"]["livre"] is True and ev["3"]["resposta"] == "", "__conversar é adiamento, sem texto"
    a.orq("pend", "done", "D-conv")
    assert "__conversar" not in _resolucoes(a) and _resolucoes(a)[-1] == "fechada sem resposta", _resolucoes(a)


# ---- M8: falha do Orca no meio do lavish-resposta ----

def test_review4_m8_run_current_que_falha_nao_grava_nada_e_a_segunda_rodada_fecha():
    a = Amb()
    a.orq("pend", "add", "--id", "dec1", "--tipo", "decisao", "--titulo", "Qual?", "--task", "task_1")
    arq = _lote(a, [{"id": "L1", "header": "dec1", "resposta": "A", "disposicao": "escolha"}])
    r = a.orq("lavish-resposta", arq, FAKE_FAIL="run-current")
    assert r.returncode == 1 and "run-current" in r.stderr, (r.returncode, r.stderr[-300:])
    assert not [e for e in a.events() if e["tipo"] == "resposta_lavish"] and "dec1" in _ids_pend(a), "nada foi gravado"
    r = a.orq("lavish-resposta", arq)
    assert r.returncode == 0 and json.loads(r.stdout)["itens"][0]["efeito"] == "fechou", (r.returncode, r.stderr[-300:], r.stdout[-300:])
    assert "dec1" not in _ids_pend(a) and _resolucoes(a) == ["A"]


def test_review4_m8_o_mesmo_header_duas_vezes_no_lote_fecha_uma_vez_sem_erro():
    a = Amb()
    r = a.orq("lavish-resposta", _lote(a, [{"id": "1", "header": "freio-prod", "resposta": "A", "disposicao": "escolha"},
                                            {"id": "2", "header": "freio-prod", "resposta": "B", "disposicao": "escolha"}]))
    assert r.returncode == 0, r
    assert [i["efeito"] for i in json.loads(r.stdout)["itens"]] == ["fechou", "so registrada"]


# ---- B11 a B19 ----

def test_review4_b11_gate_abandonado_depois_de_tres_recusas_sai_do_resumo():
    ev = [{"tipo": "pend", "op": "done", "pend": "d", "gate": "gate_9", "gate_run": "run_a"}]
    assert orq_mod.gates_pendentes(ev) == [("gate_9", "run_a")]
    ev += [{"tipo": "gate_falha", "gate": "gate_9"}] * 2
    assert orq_mod.gates_pendentes(ev) == [("gate_9", "run_a")], "ainda vai ser tentado"
    ev += [{"tipo": "gate_falha", "gate": "gate_9"}]
    assert orq_mod.gates_pendentes(ev) == [], "desistiu: a linha não tem ação possível"


def test_review4_b12_intake_mostra_qual_entrada_fechou_e_recusa_conversa_em_relatorio():
    a = Amb()
    a.prompt("cria a task do ticket 03 e me avisa quando terminar de subir tudo")
    r = a.orq("intake", "e1", "conversa", "--nota", "só um papo")
    out = json.loads(r.stdout)
    assert r.returncode == 0 and out["origem"] == "usuario" and out["texto"].startswith("cria a task do ticket 03") and len(out["texto"]) <= 40, out
    b = Amb()
    _ingest_env(b)
    b.orq("ingest")
    rel = _entradas(b, "relatorio")
    r = b.orq("intake", rel[0]["id"], "conversa", "--nota", "D02 confirmado pelo usuário via Lavish")
    assert r.returncode == 1 and "relatório" in r.stderr and "descartado" in r.stderr, r
    assert not [e for e in b.events() if e["tipo"] == "intake"], "nada foi gravado"
    r = b.orq("intake", rel[0]["id"], "descartado", "--nota", "já tratado")
    out = json.loads(r.stdout)
    assert r.returncode == 0 and out["origem"] == "relatorio" and out["fonte"] == rel[0]["fonte"] and out["texto"], out
    rw = _entradas(b, "relatorio_worker")[0]
    assert b.orq("intake", rw["id"], "conversa").returncode == 1


def test_review4_b12_o_stop_cita_as_tres_primeiras_entradas_sem_efeito():
    a = Amb()
    for t in ("um assunto longo demais para caber inteiro na citação do stop", "dois", "três", "quatro"):
        a.prompt(t)
    msg = json.loads(a.orq("hook", "stop", stdin=json.dumps({"session_id": "s"})).stdout)["systemMessage"]
    longo = orq_mod._cita("um assunto longo demais para caber inteiro na citação do stop")
    assert f"4 entrada(s) sem efeito: e1 ({longo!r}), e2 ('dois'), e3 ('três') +1" in msg and longo.endswith("…"), msg


def test_review4_b13_o_motivo_lista_o_despacho_e_diz_como_sair():
    a = Amb(run="run_a")
    _workers(a, ("w_preso", "run_b", "dispatched"))
    motivo = json.loads(_guard(a).stdout)["hookSpecificOutput"]["permissionDecisionReason"]
    assert "ctx_w_preso" in motivo and "worker-stop --dispatch" in motivo and "ask-guard.off" in motivo, motivo


def test_review4_b14_despacho_de_run_coordenado_por_outro_terminal_vivo_nao_trava_a_caixa():
    a = Amb(run="run_a")
    a.set("runs.json", [{"id": "run_b", "coordinator_handle": "term_outro"}])
    _workers(a, ("w1", "run_b", "dispatched"))
    a.set("terminals.json", ["term_outro", "term_coord"])
    assert _guard(a).stdout == "", "outro terminal vivo coordena o Run b"
    b = Amb(run="run_a")
    b.set("runs.json", [{"id": "run_b", "coordinator_handle": "term_morto"}])
    _workers(b, ("w1", "run_b", "dispatched"))
    b.set("terminals.json", ["term_coord"])
    assert "deny" in _guard(b).stdout, "coordenador fechado: o despacho segue valendo"
    c = Amb(run="run_a")
    c.set("runs.json", [{"id": "run_b", "coordinator_handle": "term_coord"}])
    _workers(c, ("w1", "run_b", "dispatched"))
    c.set("terminals.json", ["term_coord"])
    assert "deny" in _guard(c).stdout, "Run coordenado por este terminal"
    d = Amb(run="run_a")
    d.set("runs.json", [{"id": "run_b", "coordinator_handle": "term_outro"}])
    _workers(d, ("w1", "run_b", "dispatched"))
    d.set("terminals.json", ["term_outro"])
    _guard(d)
    _guard(d)
    assert len(_calls(d, "run-show")) == 1 and len(_calls(d, "worker-list")) == 1, "o cache guarda a lista já filtrada"


def test_review4_b15_coordenador_sem_run_ligado_com_worker_ativo_continua_travado():
    a = Amb(run="run_a")
    a.prompt("primeira")  # a sessão coordenou o run_a
    a.set("run.json", None)  # hibernação: o binding se perdeu
    _workers(a, ("w1", "run_a", "dispatched"))
    assert "deny" in _guard(a).stdout
    b = Amb(run=None)
    _workers(b, ("w1", "run_a", "dispatched"))
    assert _guard(b).stdout == "", "sessão que nunca coordenou"
    c = Amb(run="run_a")
    c.prompt(PREAMBULO)
    c.set("run.json", None)
    _workers(c, ("w1", "run_a", "dispatched"))
    assert "orchestration ask" in _guard(c).stdout, "worker: a caixa é recusada com o jeito de escalar (ticket 52), não pela regra do despacho ativo"


def test_review4_b16_preambulo_colado_numa_sessao_que_coordena_nao_a_vira_worker():
    a = Amb(run="run_a")
    a.prompt("primeira")
    r = a.prompt(PREAMBULO + "\nO que isso quer dizer?")
    assert (r.returncode, r.stdout) == (0, "")
    assert _cursor(a).get("papeis", {}).get("abcdef123456") != "worker" and _cursor(a)["runs"] == {"abcdef123456": "run_a"}, _cursor(a)
    assert "preâmbulo" in a.log()
    assert "entrada e2" in json.loads(a.prompt("segunda").stdout)["hookSpecificOutput"]["additionalContext"], "o coordenador segue registrando"


def test_review4_b17_a_dica_de_resposta_livre_sai_com_aspas_quando_o_header_tem_espaco():
    a = Amb()
    a.orq("pend", "add", "--id", "4 quem", "--tipo", "decisao", "--titulo", "Quem?")
    q = [_pergunta("4 quem", [("A", "x"), ("B", "y")])]
    ctx = json.loads(a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "nenhuma, outra coisa"})).stdout)["hookSpecificOutput"]["additionalContext"]
    assert "orq pend done '4 quem' --resposta" in ctx, ctx
    r = a.orq("lavish-resposta", _lote(a, [{"id": "1", "header": "4 quem", "resposta": "depois vejo", "disposicao": "livre"}]))
    assert "orq pend done '4 quem' --resposta" in r.stderr, r.stderr
    assert "orq pend done '4 quem' --resposta" in json.loads(a.prompt("e aí?").stdout)["hookSpecificOutput"]["additionalContext"]


def test_review4_b18_o_ingest_nao_conta_gate_resolvido_como_entrada_nova():
    a = Amb(run="run_a")
    a.prompt("decide o freio")
    _gate_pend(a)
    a.set("run.json", {"id": "run_b"})
    a.orq("pend", "done", "gate-dec", "--resposta", "Sim")
    a.set("run.json", {"id": "run_a"})
    r = a.orq("ingest")
    assert r.returncode == 0 and "0 entrada(s) nova(s)" in r.stdout and "1 gate(s) resolvido(s)" in r.stdout, r.stdout
    assert "gate(s)" not in a.orq("ingest").stdout, "sem gate resolvido a linha não aparece"


# ---------- fatia 8: agent manager (orq agentes, alerta de travado, orq liberar, orq despachar) ----------

def _hbi(seq, dispatch, fase, delta, run="run_a"):
    """Heartbeat do inbox (payload com taskId, dispatchId e phase, como o Orca real)."""
    return {"id": f"msg_hb{seq}", "run_id": run, "type": "heartbeat", "priority": "normal", "subject": "alive", "body": "",
            "payload": json.dumps({"taskId": "t", "dispatchId": dispatch, "phase": fase}), "from_handle": "term_w", "to_handle": f"run:{run}",
            "sequence": seq, "created_at": _iso(delta), "delivered_at": _iso(delta)}


def _pergunta_ask(seq, dispatch, run="run_a"):
    """Pergunta do `orca orchestration ask`: thread_id é o próprio id, o remetente é dispatch:<id>."""
    return {"id": f"msg_q{seq}", "run_id": run, "type": "question", "priority": "normal", "subject": "Question", "body": "qual?", "thread_id": f"msg_q{seq}",
            "payload": json.dumps({"taskId": "t", "dispatchId": dispatch, "question": "qual?"}), "from_handle": f"dispatch:{dispatch}",
            "to_handle": f"run:{run}", "sequence": seq, "created_at": _iso(-30), "delivered_at": _iso(-30)}


def _agentes_env(a):
    """Cinco dispatches em dois Runs: rodando, travado, perguntando, entregue (terminal aberto) e liberado."""
    a.set("workers.json", [
        {"handle": "term_r1", "run": "run_a", "task": "task_r1", "status": "dispatched", "modelo": "claude-haiku-4-5"},
        {"handle": "term_t1", "run": "run_b", "task": "task_t1", "status": "dispatched", "modelo": "claude-opus-5-5"},
        {"handle": "term_q1", "run": "run_a", "task": "task_q1", "status": "dispatched"},
        {"handle": "term_e1", "run": "run_b", "task": "task_e1", "status": "completed", "terminal": "retained", "modelo": "claude-sonnet-5-5"},
        {"handle": "term_l1", "run": "run_a", "task": "task_l1", "status": "completed"}])
    a.set("tasks_run_a.json", [{"id": "task_r1", "task_title": "Ticket 01", "status": "dispatched", "created_at": _iso(-900), "dispatch_id": "ctx_term_r1"},
                               {"id": "task_q1", "task_title": "Ticket 02", "status": "dispatched", "created_at": _iso(-900), "dispatch_id": "ctx_term_q1"},
                               {"id": "task_l1", "task_title": "Ticket 00", "status": "completed", "created_at": _iso(-9000), "dispatch_id": "ctx_term_l1"}])
    a.set("tasks_run_b.json", [{"id": "task_t1", "task_title": "Ticket 03", "status": "dispatched", "created_at": _iso(-9000), "dispatch_id": "ctx_term_t1"},
                               {"id": "task_e1", "task_title": "Ticket 04", "status": "completed", "created_at": _iso(-9000), "dispatch_id": "ctx_term_e1"}])
    _inbox(a, _pergunta_ask(50, "ctx_term_q1"), _hbi(40, "ctx_term_t1", "fase-1", -1200, "run_b"), _hbi(30, "ctx_term_r1", "fase-2", -400),
           _hbi(20, "ctx_term_r1", "fase-3", -120))


def _agentes(a, *args, **env):
    r = a.orq("agentes", "--json", *args, **env)
    assert r.returncode == 0, r.stderr
    return {x["dispatch"]: x for x in json.loads(r.stdout)}


def test_agentes_da_o_estado_de_cada_dispatch_em_todos_os_runs():
    a = Amb(run="run_a")
    _agentes_env(a)
    ag = _agentes(a)
    assert {d: x["estado"] for d, x in ag.items()} == {"ctx_term_r1": "rodando", "ctx_term_t1": "travado", "ctx_term_q1": "perguntando",
                                                        "ctx_term_e1": "entregue"}, "o liberado só entra com --todos"
    r1 = ag["ctx_term_r1"]
    assert (r1["task"], r1["run"], r1["titulo"], r1["modelo"], r1["terminal"], r1["fase"]) == \
        ("task_r1", "run_a", "Ticket 01", "claude-haiku-4-5", "term_r1", "fase-3"), r1
    assert 100 <= r1["idade_s"] <= 200 and r1["ultimo_heartbeat"], r1
    t1 = ag["ctx_term_t1"]
    assert (t1["run"], t1["titulo"], t1["modelo"], t1["fase"]) == ("run_b", "Ticket 03", "claude-opus-5-5", "fase-1") and 1150 <= t1["idade_s"] <= 1300, t1
    assert ag["ctx_term_e1"]["terminal"] == "term_e1" and ag["ctx_term_e1"]["titulo"] == "Ticket 04"
    calls = [json.loads(x) for x in open(os.path.join(a.fake, "calls.log"))]
    assert any(c[0] == "worker-list" and "--run" not in c for c in calls), "worker-list sem --run: escopo all"
    assert any(c[0] == "inbox" for c in calls)


def test_agentes_esconde_o_retido_pelo_orca_ate_o_todos():
    a = Amb(run="run_a")
    _agentes_env(a)
    ws = json.load(open(os.path.join(a.fake, "workers.json")))
    ws.append({"handle": "term_u1", "run": "run_a", "status": "completed", "terminal": "retained", "reason": "external_terminal"})  # user_takeover não esconde (M9)
    ws.append({"handle": "term_u2", "run": "run_a", "status": "dispatched", "reason": "user_takeover", "ownership": "user_owned", "desde": _iso(-60)})
    ws.append({"handle": "term_c1", "run": "run_a", "status": "completed", "terminal": "retained", "sem_resource": True})  # dispatch de contexto (unsupervised)
    a.set("workers.json", ws)
    assert "ctx_term_c1" not in _agentes(a) and _agentes(a, "--todos")["ctx_term_c1"]["retido"] == "sem_recurso"
    assert _agentes(a)["ctx_term_u2"]["estado"] == "rodando", "o usuário no terminal não tira o worker da lista enquanto ele roda"
    assert "ctx_term_u1" not in _agentes(a)
    u = _agentes(a, "--todos")["ctx_term_u1"]
    assert u["estado"] == "entregue" and u["retido"] == "external_terminal"
    a.set("runs.json", [{"id": "run_a", "objective": "A"}, {"id": "run_b", "objective": "B"}])
    a.orq("ingest", "--refresh")
    assert "ctx_term_u1" not in {x["dispatch"] for x in json.load(open(os.path.join(a.home, "aberto.json")))["agentes"]}


def test_agentes_todos_inclui_os_liberados_e_run_filtra():
    a = Amb(run="run_a")
    _agentes_env(a)
    assert _agentes(a, "--todos")["ctx_term_l1"]["estado"] == "liberado"
    so_b = _agentes(a, "--run", "run_b")
    assert set(so_b) == {"ctx_term_t1", "ctx_term_e1"}


def test_agentes_nao_lista_como_entregue_o_ja_liberado_nem_o_sem_terminal():
    a = Amb(run="run_a")
    _agentes_env(a)
    ws = json.load(open(os.path.join(a.fake, "workers.json")))
    ws.append({"handle": "term_e2", "run": "run_b", "task": "task_e2", "status": "completed", "terminal": "retained"})  # já passou por orq liberar
    ws.append({"handle": "term_e3", "run": "run_b", "task": "task_e3", "status": "completed", "terminal": "retained"})  # terminal fechado à mão
    a.set("workers.json", ws)
    a.set("terminals.json", ["term_r1", "term_t1", "term_q1", "term_e1", "term_e2"])
    os.makedirs(a.home, exist_ok=True)
    open(os.path.join(a.home, "events.jsonl"), "w").write(json.dumps({"tipo": "liberar", "dispatch": "ctx_term_e2", "fechado": True}) + "\n")
    ag = _agentes(a)
    assert {d for d, x in ag.items() if x["estado"] == "entregue"} == {"ctx_term_e1"}, ag
    todos = _agentes(a, "--todos")
    assert todos["ctx_term_e2"]["estado"] == "liberado" and todos["ctx_term_e3"]["estado"] == "liberado"
    a.set("runs.json", [{"id": "run_a", "objective": "A"}, {"id": "run_b", "objective": "B"}])
    a.orq("ingest", "--refresh")
    cache = json.load(open(os.path.join(a.home, "aberto.json")))["agentes"]
    assert sum(x["estado"] == "entregue" for x in cache) == 1, "o contador do resumo bate com a lista"


def test_agentes_esconde_as_tasks_de_prova_ate_o_todos():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_p1", "run": "run_a", "task": "task_p1", "status": "completed", "terminal": "retained"},
                           {"handle": "term_p2", "run": "run_a", "task": "task_p2", "status": "completed", "terminal": "retained"}])
    a.set("tasks_run_a.json", [{"id": "task_p1", "task_title": "Prova r5 M9", "status": "completed", "created_at": _iso(-900), "dispatch_id": "ctx_term_p1"},
                               {"id": "task_p2", "task_title": "Ticket 05", "status": "completed", "created_at": _iso(-900), "dispatch_id": "ctx_term_p2"}])
    assert set(_agentes(a)) == {"ctx_term_p2"}
    assert set(_agentes(a, "--todos")) == {"ctx_term_p1", "ctx_term_p2"}


def test_agentes_sem_heartbeat_conta_a_idade_desde_o_despacho():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_v", "run": "run_a", "status": "dispatched", "desde": _iso(-1800)},
                           {"handle": "term_n", "run": "run_a", "status": "dispatched", "desde": _iso(-180)}])
    ag = _agentes(a)
    assert ag["ctx_term_v"]["estado"] == "travado" and ag["ctx_term_v"]["fase"] is None and 1750 <= ag["ctx_term_v"]["idade_s"] <= 1900
    assert ag["ctx_term_n"]["estado"] == "rodando" and ag["ctx_term_n"]["ultimo_heartbeat"] is None


def test_agentes_usa_o_heartbeat_absorvido_quando_o_inbox_nao_alcanca():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_r1", "run": "run_a", "status": "dispatched", "desde": _iso(-3000)}])
    os.makedirs(a.home, exist_ok=True)
    with open(os.path.join(a.home, "events.jsonl"), "w") as f:
        f.write(json.dumps({"ts": "2026-09-29T17:00:00Z", "tipo": "heartbeat_absorvido", "run": "run_a", "entregas": ["d1"],
                            "heartbeats": [{"msg": "m", "task": "t", "dispatch": "ctx_term_r1", "fase": "fase-9", "ts": _iso(-60).replace(" ", "T") + "Z"}]}) + "\n")
    r1 = _agentes(a)["ctx_term_r1"]
    assert r1["estado"] == "rodando" and r1["fase"] == "fase-9" and r1["idade_s"] < 200, r1


def test_agentes_pergunta_respondida_volta_a_rodando():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_q1", "run": "run_a", "status": "dispatched", "desde": _iso(-60)}])
    resp = {**_pergunta_ask(51, "ctx_term_q1"), "id": "msg_r51", "type": "status", "thread_id": "msg_q50", "payload": None, "from_handle": "term_coord",
            "to_handle": "dispatch:ctx_term_q1"}
    _inbox(a, resp, _pergunta_ask(50, "ctx_term_q1"))
    assert _agentes(a)["ctx_term_q1"]["estado"] == "rodando"
    _inbox(a, _pergunta_ask(50, "ctx_term_q1"))
    assert _agentes(a)["ctx_term_q1"]["estado"] == "perguntando"


def test_agentes_texto_lista_estado_e_sugere_o_steer_do_travado():
    a = Amb(run="run_a")
    _agentes_env(a)
    r = a.orq("agentes")
    assert r.returncode == 0, r.stderr
    assert "travado" in r.stdout and "perguntando" in r.stdout and "Ticket 03" in r.stdout
    assert 'orq steer task_t1 "' in r.stdout and "orq liberar ctx_term_e1" in r.stdout, r.stdout


def test_agentes_worker_list_escopado_recusa_sem_run():
    a = Amb(run="run_a")
    _agentes_env(a)
    # o Orca devolve o escopo do Run ligado (bound) mesmo sem ORCA_TERMINAL_HANDLE: a lista não cobre todos os Runs
    cmd = a.orq("agentes", "--json", ORQ_ORCA=_orca_escopado(a))
    assert cmd.returncode == 1 and "--run" in cmd.stderr, cmd
    assert a.orq("agentes", "--json", "--run", "run_a", ORQ_ORCA=_orca_escopado(a)).returncode == 0


def _orca_escopado(a):
    """Um Orca falso que sempre responde worker-list como 'bound' (o que o Orca de verdade faz num terminal de worker)."""
    p = os.path.join(a.tmp.name, "orca_escopado")
    with open(p, "w") as f:
        f.write(FAKE.replace('"source": "flag" if opt("--run") else "bound" if bound else "all"', '"source": "flag" if opt("--run") else "bound"'))
    os.chmod(p, 0o755)
    return p


def test_aberto_ganha_os_agentes_ativos():
    a = Amb(run="run_a")
    _agentes_env(a)
    a.set("runs.json", [{"id": "run_a", "objective": "A"}, {"id": "run_b", "objective": "B"}])
    r = a.orq("ingest", "--refresh")
    assert r.returncode == 0, r.stderr
    ab = json.load(open(os.path.join(a.home, "aberto.json")))
    ag = {x["dispatch"]: x for x in ab["agentes"]}
    assert set(ag) == {"ctx_term_r1", "ctx_term_t1", "ctx_term_q1", "ctx_term_e1"}, "o liberado fica fora do cache"
    assert ag["ctx_term_t1"]["estado"] == "travado" and ag["ctx_term_t1"]["titulo"] == "Ticket 03" and ag["ctx_term_r1"]["fase"] == "fase-3"


def _aberto_ag(estado, **k):
    return {"ts": now_iso(), "backlog": [], "rodando": 1, "bloqueado": [], "gates": [], "falhas": [], "andamento": [],
            "agentes": [{"dispatch": "ctx_1", "task": "task_aaaaaaaaaa", "run": "run_a", "titulo": "Ticket 01", "terminal": "term_1", "modelo": "m",
                         "estado": estado, "fase": "fase-3", "ultimo_heartbeat": None, "desde": None, **k}]}


def now_iso(delta=0):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + delta))


def test_resumo_uma_linha_de_agentes_e_alerta_de_travado_com_o_steer():
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)
    ab = _aberto_ag("rodando", ultimo_heartbeat=now_iso(-1200), desde=now_iso(-3000))
    txt = orq_mod.resumo([], ab, {"itens": []}, agora=agora)
    assert len(txt.splitlines()) <= 5, txt
    assert "Travado: task_aaaaaaaaaa" in txt and 'orq steer task_aaaaaaaaaa "' in txt, txt
    assert "Vivos:" in txt and "TRAVADO" in txt, txt
    ok = orq_mod.resumo([], _aberto_ag("rodando", ultimo_heartbeat=now_iso(-120), desde=now_iso(-3000)), {"itens": []}, agora=agora)
    assert "Travado" not in ok and "TRAVADO" not in ok and "fase-3" in ok, ok


def test_resumo_reavalia_o_travado_com_o_heartbeat_mais_novo_do_log():
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)
    ab = _aberto_ag("travado", ultimo_heartbeat=now_iso(-2000), desde=now_iso(-3000))
    fresco = [{"tipo": "heartbeat_absorvido", "run": "run_a", "heartbeats": [{"dispatch": "ctx_1", "task": "task_aaaaaaaaaa", "fase": "fase-7", "ts": now_iso(-30)}]}]
    txt = orq_mod.resumo(fresco, ab, {"itens": []}, agora=agora)
    assert "Travado" not in txt and "fase-7" in txt, txt
    velho = orq_mod.resumo([], _aberto_ag("rodando", ultimo_heartbeat=None, desde=now_iso(-960)), {"itens": []}, agora=agora)
    assert "Travado: task_aaaaaaaaaa" in velho, "sem heartbeat, a idade conta desde o despacho"


def test_resumo_perguntando_e_entregue_sem_liberar_entram_na_mesma_linha():
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)
    ab = _aberto_ag("perguntando")
    ab["agentes"].append({**ab["agentes"][0], "dispatch": "ctx_2", "task": "task_bbbbbbbbbb", "estado": "entregue"})
    ab["agentes"].append({**ab["agentes"][0], "dispatch": "ctx_3", "task": "task_cccccccccc", "estado": "entregue", "retido": "user_takeover"})
    txt = orq_mod.resumo([], ab, {"itens": []}, agora=agora)
    l2 = next(l for l in txt.splitlines() if l.startswith("Aberto"))
    assert "pergunta" in l2 and "sem liberar 1" in l2, l2
    assert len(txt.splitlines()) <= 5


def test_travado_e_uma_constante_com_nome():
    assert orq_mod.TRAVADO_S == 15 * 60


def test_hook_prompt_injeta_o_travado_do_cache_em_ate_cinco_linhas():
    a = Amb(run="run_a")
    os.makedirs(a.home, exist_ok=True)
    a.set("../orq/aberto.json", _aberto_ag("rodando", ultimo_heartbeat=now_iso(-1300), desde=now_iso(-3000)))
    ctx = json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]
    assert len(ctx.splitlines()) <= 5 and "Travado: task_aaaaaaaaaa" in ctx and "orq steer" in ctx, ctx


# ---- orq liberar ----

def _lib_env(a, release="released", **w):
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_w1", "status": "completed", "terminal": "active", "release": release, **w},
                           {"handle": "term_w2", "run": "run_a", "task": "task_w2", "status": "dispatched"}])
    a.set("terminals.json", ["term_w1", "term_w2", "term_coord"])
    a.caixa(("worker_done", {"taskId": "task_w1", "dispatchId": "ctx_term_w1"}))


def _log(a, nome):
    try:
        return [json.loads(x) for x in open(os.path.join(a.fake, nome))]
    except OSError:
        return []


def _ordem(a):
    return [c[0] if c[0] != "close" else "terminal-close" for c in _log(a, "calls.log") if c[0] in ("worker-release", "close") or "--ack" in c]


def test_liberar_faz_ack_depois_release_e_grava_o_evento():
    a = Amb(run="run_a")
    _lib_env(a)
    r = a.orq("liberar", "ctx_term_w1")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert (out["dispatch"], out["estado"], out["fechado"]) == ("ctx_term_w1", "released", False), out
    assert a.estados() == {"msg_1": "acked"}, "o worker_done pendente do dispatch foi confirmado"
    assert _ordem(a) == ["check", "worker-release"], _ordem(a)
    assert not _log(a, "close.log")
    (ev,) = [e for e in a.events() if e["tipo"] == "liberar"]
    assert (ev["dispatch"], ev["task"], ev["run"], ev["estado"], ev["fechado"], ev["terminal"]) == ("ctx_term_w1", "task_w1", "run_a", "released", False, "term_w1"), ev


def test_liberar_retained_fecha_o_terminal_do_worker_depois_do_release():
    a = Amb(run="run_a")
    _lib_env(a, release="retained")
    r = a.orq("liberar", "ctx_term_w1")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert (out["estado"], out["fechado"]) == ("retained", True), out
    assert _log(a, "close.log") == [["close", "--terminal", "term_w1", "--json"]]
    assert _ordem(a) == ["check", "worker-release", "terminal-close"], _ordem(a)
    (ev,) = [e for e in a.events() if e["tipo"] == "liberar"]
    assert ev["estado"] == "retained" and ev["fechado"] is True


def test_liberar_nunca_fecha_terminal_retido_por_decisao_do_usuario():
    for motivo in ("user_takeover", "user_requested", "reused_terminal", "configured_tab", "external_terminal", "identity_unproven", "motivo_novo"):
        a = Amb(run="run_a")
        _lib_env(a, release="retained", release_reason=motivo)
        out = json.loads(a.orq("liberar", "ctx_term_w1").stdout)
        assert out["estado"] == "retained" and out["fechado"] is False and motivo in out["aviso"], (motivo, out)
        assert not _log(a, "close.log"), motivo
    a = Amb(run="run_a")
    _lib_env(a, release="retained", release_ownership="user_owned")
    assert json.loads(a.orq("liberar", "ctx_term_w1").stdout)["fechado"] is False and not _log(a, "close.log")


def test_liberar_nao_fecha_terminal_que_nao_e_de_worker_do_run():
    a = Amb(run="run_a")
    _lib_env(a)
    r = a.orq("liberar", "ctx_fantasma")
    assert r.returncode == 1 and "worker-list" in r.stderr, r
    assert not _log(a, "released.log") and not _log(a, "close.log")
    # o terminal do próprio coordenador nunca é fechado, mesmo que apareça como retained num dispatch
    b = Amb(run="run_a")
    _lib_env(b, release="retained")
    b.set("workers.json", [{"handle": "term_coord", "run": "run_a", "status": "completed", "terminal": "active", "release": "retained"}])
    out = json.loads(b.orq("liberar", "ctx_term_coord").stdout)
    assert out["fechado"] is False and not _log(b, "close.log"), out
    # nem o terminal que é o coordenador de um Run
    c = Amb(run="run_a")
    _lib_env(c, release="retained")
    c.set("runs.json", [{"id": "run_a", "coordinator_handle": "term_w1"}])
    assert json.loads(c.orq("liberar", "ctx_term_w1").stdout)["fechado"] is False and not _log(c, "close.log")


def test_liberar_nunca_fecha_o_terminal_de_dispatch_de_contexto():
    a = Amb(run="run_a")
    _lib_env(a, release="retained", sem_resource=True)
    out = json.loads(a.orq("liberar", "ctx_term_w1").stdout)
    assert out["estado"] == "retained" and out["fechado"] is False and not _log(a, "close.log"), out


def test_liberar_recusa_dispatch_ainda_rodando():
    a = Amb(run="run_a")
    _lib_env(a)
    r = a.orq("liberar", "ctx_term_w2")
    assert r.returncode == 1 and "rodando" in r.stderr and not _log(a, "released.log"), r


def test_liberar_so_confirma_as_mensagens_do_proprio_dispatch():
    a = Amb(run="run_a")
    _lib_env(a)
    a.caixa(("question", {"taskId": "task_w2", "dispatchId": "ctx_term_w2"}))
    r = a.orq("liberar", "ctx_term_w1")
    assert r.returncode == 0, r.stderr
    assert a.estados()["msg_1"] == "acked" and a.estados()["msg_2"] in ("unread", "out") and a.estados()["msg_2"] != "acked", a.estados()
    b = Amb(run="run_a")
    _lib_env(b)
    b.caixa(("worker_done", {"taskId": "task_w2", "dispatchId": "ctx_term_w2"}))
    out = json.loads(b.orq("liberar", "ctx_term_w1").stdout)
    assert "acked" not in b.estados().values() and "outro dispatch" in out["aviso"], (b.estados(), out)  # o Orca entrega os dois no mesmo lote
    c = Amb(run="run_a")
    c.set("workers.json", [{"handle": "term_w1", "run": "run_a", "status": "completed", "terminal": "active"}])
    c.caixa(("worker_done", {"taskId": "t", "dispatchId": "ctx_term_outro"}))
    c.orq("liberar", "ctx_term_w1")
    assert c.estados()["msg_1"] != "acked"


def test_liberar_com_o_gerente_em_outro_run_pula_o_ack_e_ainda_libera():
    a = Amb(run="run_b")  # coordenador com o gerente ligado a outro Run: o check dá consumer_fenced e o orq não toma o Run do gerente
    _lib_env(a)
    _gerente(a, "run_b")
    r = a.orq("liberar", "ctx_term_w1")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["estado"] == "released" and "ack" in out["aviso"] and "orq gerente ligar --terminal term_ger --run run_a" in out["aviso"], out
    assert _log(a, "released.log")


def test_liberar_release_pending_nao_fecha_e_avisa():
    a = Amb(run="run_a")
    _lib_env(a, release="release_pending")
    out = json.loads(a.orq("liberar", "ctx_term_w1").stdout)
    assert out["estado"] == "release_pending" and out["fechado"] is False and "pending" in out["aviso"] and not _log(a, "close.log"), out


def test_liberar_falha_do_close_nao_perde_o_evento():
    a = Amb(run="run_a")
    _lib_env(a, release="retained")
    r = a.orq("liberar", "ctx_term_w1", FAKE_FAIL_TERMINAL="close")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["fechado"] is False and "close" in out["aviso"], out
    assert [e for e in a.events() if e["tipo"] == "liberar"][0]["fechado"] is False


# ---- orq despachar ----

def _spec(a, texto="Faça X.\n"):
    p = os.path.join(a.tmp.name, "spec.md")
    open(p, "w").write(texto)
    return p


def _despachar(a, *extra, spec=None, **env):
    return a.orq("despachar", "--run", "run_a", "--titulo", "Ticket 05", "--spec-arquivo", spec or _spec(a), "--modelo", "claude-sonnet-5-5",
                 "--effort", "medium", *extra, **env)


def test_despachar_roda_o_worker_start_com_modelo_e_effort_e_devolve_os_ids():
    a = Amb(run="run_a")
    r = _despachar(a, spec=_spec(a, "# Ticket 05\n\nFaça X.\n"))
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert (out["dispatchId"], out["taskId"], out["run"], out["terminal"]) == ("ctx_term_novo1", "task_novo1", "run_a", "term_novo1"), out
    (arg,) = _log(a, "started.log")
    assert arg[:1] == ["worker-start"] and arg[arg.index("--run") + 1] == "run_a" and arg[arg.index("--agent") + 1] == "claude"
    assert arg[arg.index("--model") + 1] == "claude-sonnet-5-5" and arg[arg.index("--effort") + 1] == "medium"
    assert arg[arg.index("--task-title") + 1] == "Ticket 05" and arg[arg.index("--spec") + 1] == "# Ticket 05\n\nFaça X.\n"
    assert "--worktree" not in arg
    (ev,) = [e for e in a.events() if e["tipo"] == "despacho"]
    assert (ev["run"], ev["task"], ev["dispatch"], ev["titulo"], ev["modelo"], ev["effort"], ev["terminal"]) == \
        ("run_a", "task_novo1", "ctx_term_novo1", "Ticket 05", "claude-sonnet-5-5", "medium", "term_novo1"), ev


def test_despachar_imprime_o_comando_do_waiter_pronto_e_nao_cria_outro():
    a = Amb(run="run_a")
    out = json.loads(_despachar(a).stdout)
    assert out["espera"] == "python3 ~/.claude/scripts/orca-wait-runs.py run_a", out
    assert not [c for c in _log(a, "calls.log") if c[0] in ("check", "inbox")], "o orq despachar não espera nada"


def test_despachar_com_entrada_liga_o_intake_na_task_nova():
    a = Amb(run="run_a")
    a.prompt("cria o ticket 05")
    r = _despachar(a, "--entrada", "e1")
    assert r.returncode == 0, r.stderr
    (i,) = [e for e in a.events() if e["tipo"] == "intake"]
    assert (i["entrada"], i["efeito"], i["ref"], i["run"]) == ("e1", "tarefa", "task_novo1", "run_a"), i
    assert json.loads(r.stdout)["entrada"] == "e1"


def test_despachar_com_entrada_poe_o_pedido_literal_no_topo_do_spec():
    a = Amb(run="run_a")
    a.prompt("cria o ticket 05, com \"aspas\" e acento")
    _despachar(a, "--entrada", "e1", spec=_spec(a, "# Ticket 05\n\nFaça X.\n"))
    (arg,) = _log(a, "started.log")
    spec = arg[arg.index("--spec") + 1]
    assert spec == ('# Ticket 05\n\n## Pedido do usuário\ncria o ticket 05, com "aspas" e acento\n\n'
                    'O que o coordenador escreveu abaixo não o substitui: o pronto se confere contra este pedido.\n\nFaça X.\n'), spec


def test_despachar_sem_entrada_nao_muda_o_spec():
    a = Amb(run="run_a")
    _despachar(a, spec=_spec(a, "# Ticket 05\n\nFaça X.\n"))
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--spec") + 1] == "# Ticket 05\n\nFaça X.\n"


def test_despachar_entrada_com_spec_sem_titulo_poe_o_pedido_depois_do_titulo_que_o_orq_acrescenta():
    a = Amb(run="run_a")
    a.prompt("pedido")
    _despachar(a, "--entrada", "e1", spec=_spec(a, "Faça X.\n"))
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--spec") + 1].startswith("# Ticket 05\n\n## Pedido do usuário\npedido\n"), arg


def test_despachar_entrada_inexistente_nao_despacha():
    a = Amb(run="run_a")
    r = _despachar(a, "--entrada", "e99")
    assert r.returncode == 1 and "e99" in r.stderr and not _log(a, "started.log"), r


def test_despachar_worktree_nova_repassa_name_e_base_branch():
    a = Amb(run="run_a")
    r = _despachar(a, "--worktree", "new-top-level", "--name", "feat/x", "--base-branch", "development")
    assert r.returncode == 0, r.stderr
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--worktree") + 1] == "new-top-level" and arg[arg.index("--name") + 1] == "feat/x" and arg[arg.index("--base-branch") + 1] == "development"
    (ev,) = [e for e in a.events() if e["tipo"] == "despacho"]
    assert ev["worktree"] == "new-top-level" and ev["nome"] == "feat/x"


def test_despachar_recusa_o_que_o_orca_recusaria_sem_criar_task():
    a = Amb(run="run_a")
    r = _despachar(a, "--worktree", "current", "--name", "feat/x")
    assert r.returncode == 1 and "new-top-level" in r.stderr and not _log(a, "started.log"), r
    r = _despachar(a, "--base-branch", "development")
    assert r.returncode == 1 and not _log(a, "started.log"), r
    r = a.orq("despachar", "--run", "run_a", "--titulo", "T", "--spec-arquivo", _spec(a), "--effort", "low")
    assert r.returncode != 0 and "--modelo" in r.stderr and not _log(a, "started.log"), "sem modelo o argparse recusa"
    r = a.orq("despachar", "--run", "run_a", "--titulo", "T", "--spec-arquivo", _spec(a), "--modelo", "m")
    assert r.returncode != 0 and "--effort" in r.stderr and not _log(a, "started.log")
    r = a.orq("despachar", "--run", "run_a", "--titulo", "T", "--spec-arquivo", "/nao/existe.md", "--modelo", "m", "--effort", "low")
    assert r.returncode == 1 and "existe.md" in r.stderr and not _log(a, "started.log")


def test_despachar_run_diferente_do_ligado_pede_run_use():
    a = Amb(run="run_b")
    r = _despachar(a)
    assert r.returncode == 1 and "run-use --id run_a" in r.stderr and not _log(a, "started.log"), r
    sem = Amb(run=None)
    r = _despachar(sem)
    assert r.returncode == 1 and "run-use --id run_a" in r.stderr and not _log(sem, "started.log")


def test_despachar_mantem_o_titulo_na_aba_do_worker():
    a = Amb(run="run_a")
    _despachar(a, spec=_spec(a, "# Ticket 05\n\nx\n"))
    assert _log(a, "rename.log") == [["rename", "--terminal", "term_novo1", "--title", "Ticket 05", "--json"]]
    # a falha do rename não desfaz o despacho
    b = Amb(run="run_a")
    r = _despachar(b, FAKE_FAIL_TERMINAL="rename")
    assert r.returncode == 0 and json.loads(r.stdout)["dispatchId"] == "ctx_term_novo1" and "rename" in b.log()


def test_despachar_spec_sem_titulo_ganha_o_titulo_como_primeira_linha():
    a = Amb(run="run_a")
    _despachar(a, spec=_spec(a, "Faça X.\n"))
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--spec") + 1] == "# Ticket 05\n\nFaça X.\n", "o Claude Code tira o nome da aba do começo do prompt"


def test_despachar_falha_do_worker_start_nao_grava_evento():
    a = Amb(run="run_a")
    r = _despachar(a, FAKE_FAIL="worker-start")
    assert r.returncode == 1 and not [e for e in a.events() if e["tipo"] == "despacho"], r


def test_despachar_com_o_prompt_que_entra_nao_manda_enter_nem_marca_nao_iniciou():
    a = Amb(run="run_a")
    out = json.loads(_despachar(a).stdout)
    assert "estado" not in out and not _log(a, "send.log") and not [e for e in a.events() if e["tipo"] == "nao_iniciou"], out


def test_despachar_com_o_prompt_que_so_entra_depois_do_enter_manda_um_enter_so():
    a = Amb(run="run_a")
    out = json.loads(_despachar(a, FAKE_INICIO="depois_do_enter").stdout)
    assert "estado" not in out and out["enter"] is True, out
    (env,) = _log(a, "send.log")
    assert "--enter" in env and "--text" not in env
    assert not [e for e in a.events() if e["tipo"] == "nao_iniciou"]


def test_despachar_com_o_prompt_que_nunca_entra_marca_nao_iniciou_e_avisa():
    a = Amb(run="run_a")
    r = _despachar(a, FAKE_INICIO="nunca")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["estado"] == "nao_iniciou" and out["enter"] is True and "não entrou" in out["aviso"] and "aviso:" in r.stderr, out
    assert len(_log(a, "send.log")) == 1, "um Enter só"
    (ev,) = [e for e in a.events() if e["tipo"] == "nao_iniciou"]
    assert ev["dispatch"] == "ctx_term_novo1" and ev["run"] == "run_a"
    ag = _agentes(a)["ctx_term_novo1"]
    assert ag["estado"] == "nao_comecou", "sem esperar NAO_COMECOU_S"


def test_agentes_so_mostra_entregue_com_worker_done_registrado():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_e1", "run": "run_a", "task": "task_e1", "status": "completed", "terminal": "retained", "sem_done": True}])
    assert _agentes(a)["ctx_term_e1"]["estado"] == "encerrado"


def test_despachar_o_dispatch_novo_aparece_em_orq_agentes():
    a = Amb(run="run_a")
    _despachar(a)
    ag = _agentes(a)
    assert ag["ctx_term_novo1"]["estado"] == "rodando" and ag["ctx_term_novo1"]["terminal"] == "term_novo1"


def test_guard_do_worker_routing_cobre_o_orq_despachar():
    guard = os.path.join(AQUI, "hooks", "worker-routing-guard.py")

    def roda(cmd):
        r = subprocess.run([sys.executable, guard], input=json.dumps({"tool_name": "Bash", "tool_input": {"command": cmd}}), capture_output=True, text=True)
        return json.loads(r.stdout)["hookSpecificOutput"]["permissionDecision"] if r.stdout.strip() else "allow"

    assert roda("orq despachar --run r --titulo T --spec-arquivo f --effort low") == "deny"
    assert roda("orq despachar --run r --titulo T --spec-arquivo f --modelo m") == "deny"
    assert roda("orq despachar --run r --titulo T --spec-arquivo f --modelo m --effort low") == "allow"
    assert roda("orca orchestration worker-start --spec x") == "deny", "o guard antigo continua valendo"
    assert roda("orq agentes --json") == "allow"
    assert roda("python3 ~/.claude/orq/orq.py despachar --run r --titulo T --spec-arquivo f") == "deny"
    assert roda("cd /tmp && orq despachar --run r --titulo T --spec-arquivo f --effort low") == "deny"
    assert roda('orca orchestration send --type worker_done --body "usei o orq despachar e o orq liberar"') == "allow", "citar o comando num texto não é despachar"



# ---- sinal de vida de outro Run também não acorda o coordenador (pedido do usuário, fatia 8) ----

AVISO_B = "You have 1 orchestration message. Run `orca orchestration check --run run_b`."


def _hb_inbox(a, dispatch, fase, delta, seq, run="run_b", tipo="heartbeat"):
    """Mensagem do inbox de um Run que o coordenador não coordena: to_handle run:<r>, payload do Orca."""
    m = _hbi(seq, dispatch, fase, delta, run)
    return {**m, "type": tipo, "to_handle": f"run:{run}"}


def _chamadas(a, cmd):
    return [c for c in _log(a, "calls.log") if c[0] == cmd]


def test_aviso_de_heartbeat_de_outro_run_e_bloqueado_sem_consumir_nada():
    a = Amb(run="run_a")
    a.caixa(_hb("lendo"), run="run_b")
    _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -5, 900))
    r = a.prompt(AVISO_B)
    assert _bloqueado(r), r
    assert "run_b" in json.loads(r.stdout)["reason"]
    assert _chamadas(a, "check") == [], "nem peek, nem check, nem ack: consumer_fenced evitado e a caixa daquele Run fica como estava"
    assert a.estados() == {"msg_1": "unread"}
    (ev,) = [e for e in a.events() if e["tipo"] == "heartbeat_visto"]
    assert ev["run"] == "run_b" and [(h["dispatch"], h["fase"]) for h in ev["heartbeats"]] == [("ctx_9", "fase-4")], ev


def test_aviso_de_outro_run_com_outra_mensagem_passa():
    for tipo in ("worker_done", "question", "escalation", "tipo_novo"):
        a = Amb(run="run_a")
        _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -5, 900), _hb_inbox(a, "ctx_9", "x", -3, 901, tipo=tipo))
        r = a.prompt(AVISO_B)
        assert (r.returncode, r.stdout) == (0, "") and _chamadas(a, "check") == [] and not [e for e in a.events() if e["tipo"] == "heartbeat_visto"], (tipo, r)


def test_aviso_de_outro_run_sem_mensagem_recente_passa():
    a = Amb(run="run_a")
    _inbox(a, {**_hb_inbox(a, "ctx_9", "fase-4", -600, 900), "read": 1})  # já lido: não é o do aviso (M11: vale o `read`, não a idade)
    assert a.prompt(AVISO_B).stdout == ""
    b = Amb(run="run_a")
    _inbox(b, _hb_inbox(b, "ctx_9", "fase-4", -5, 900, run="run_c"))  # heartbeat de um terceiro Run
    assert b.prompt(AVISO_B).stdout == ""


def test_aviso_de_outro_run_com_o_orca_fora_do_ar_passa():
    a = Amb(run="run_a")
    _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -5, 900))
    r = a.prompt(AVISO_B, FAKE_FAIL="inbox")
    assert (r.returncode, r.stdout) == (0, ""), r


def test_heartbeat_visto_de_outro_run_mantem_o_dispatch_vivo_no_resumo_e_no_agentes():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_o1", "run": "run_b", "task": "task_o1", "status": "dispatched", "desde": _iso(-3000)}])
    _inbox(a, _hb_inbox(a, "ctx_term_o1", "fase-8", -5, 900))
    assert _bloqueado(a.prompt(AVISO_B))
    o1 = _agentes(a)["ctx_term_o1"]
    assert o1["estado"] == "rodando" and o1["fase"] == "fase-8", o1
    from datetime import datetime, timezone
    ab = _aberto_ag("travado", ultimo_heartbeat=now_iso(-2000), desde=now_iso(-3000), dispatch="ctx_term_o1")
    evs = orq_mod_events(a.home)
    txt = orq_mod.resumo(evs, ab, {"itens": []}, agora=datetime.now(timezone.utc))
    assert "Travado" not in txt and "fase-8" in txt, txt


def test_heartbeat_de_outro_run_bloqueado_nao_perde_o_worker_done_que_chega_depois():
    a = Amb(run="run_a")
    a.caixa(_hb("lendo"), run="run_b")
    _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -5, 900))
    assert _bloqueado(a.prompt(AVISO_B))
    a.caixa(("worker_done", {"taskId": "t", "dispatchId": "ctx_9"}), run="run_b")
    _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -8, 900), {**_hb_inbox(a, "ctx_9", "", -2, 901, tipo="worker_done")})
    r = a.prompt(AVISO_B)
    assert (r.returncode, r.stdout) == (0, ""), "o worker_done acorda"
    assert set(a.estados().values()) == {"unread"}, "nada foi confirmado"
    a.set("run.json", {"id": "run_b"})  # o coordenador faz run-use no Run: a caixa sai inteira, lote a lote
    got = subprocess.run([a.bin, "orchestration", "check", "--run", "run_b", "--all", "--json"], env=a.env, capture_output=True, text=True)
    assert [m["type"] for m in json.loads(got.stdout)["result"]["messages"]] == ["heartbeat", "worker_done"]


# ---------- fatia 9: tickets em arquivo, mapa e retomada em sessão nova (seção 20 do desenho) ----------

ACEITE = "\n\n## Acceptance criteria\n\n- [ ] X funciona\n- [ ] o teste de X passa\n"


def _spec_tk(a, corpo="Faça X.", nome="spec.md", aceite=ACEITE):
    caminho = os.path.join(a.tmp.name, nome)
    with open(caminho, "w") as f:
        f.write(corpo + aceite)
    return caminho


def _novo(a, titulo="Ticket de teste", *extra, spec=None, **env):
    return a.orq("ticket", "novo", "--titulo", titulo, "--spec-arquivo", spec or _spec_tk(a), *extra, **env)


def _ticket(a, nn):
    (arq,) = [f for f in os.listdir(a.env["ORQ_ISSUES"]) if f.startswith(f"{nn}-")]
    return os.path.join(a.env["ORQ_ISSUES"], arq)


def _lido(a, nn):
    return open(_ticket(a, nn)).read()


def test_ticket_novo_cria_o_arquivo_no_formato_do_to_tickets():
    a = Amb(run="run_a")
    r = _novo(a, "Ticket de teste")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["ticket"] == "01" and out["task"] == "task_tk1" and out["run"] == "run_a", out
    assert os.path.basename(out["arquivo"]) == "01-ticket-de-teste.md" and os.path.dirname(out["arquivo"]) == a.env["ORQ_ISSUES"]
    txt = _lido(a, "01")
    assert txt.startswith("# 01: Ticket de teste\n"), txt
    cab = txt.split("\n## ")[0]
    assert "Status: ready-for-agent" in cab and "Blocked by: (nenhum)" in cab and "Run: run_a" in cab and "Task: task_tk1" in cab, cab
    assert "\n## What to build\n\nFaça X." in txt and "\n## Acceptance criteria\n" in txt and "## Answer" not in txt, txt


def test_ticket_novo_numera_de_01_em_diante_e_nao_reaproveita_numero():
    a = Amb(run="run_a")
    for i, t in enumerate(("Primeiro", "Segundo Ticket: com pontuação!")):
        assert json.loads(_novo(a, t).stdout)["ticket"] == f"0{i + 1}"
    assert sorted(os.listdir(a.env["ORQ_ISSUES"])) == ["01-primeiro.md", "02-segundo-ticket-com-pontuacao.md"]
    os.remove(_ticket(a, "01"))
    assert json.loads(_novo(a, "Terceiro").stdout)["ticket"] == "03", "o número segue o maior existente"


def test_ticket_novo_cria_a_task_com_o_titulo_e_o_spec_curto():
    a = Amb(run="run_a")
    out = json.loads(_novo(a, "Título exato do ticket").stdout)
    (arg,) = _log(a, "created.log")
    assert arg[:1] == ["task-create"] and arg[arg.index("--task-title") + 1] == "Título exato do ticket"
    assert arg[arg.index("--spec") + 1] == f"Leia e execute o ticket {out['arquivo']}", arg
    assert "--deps" not in arg and arg[arg.index("--run") + 1] == "run_a"
    tasks = json.load(open(os.path.join(a.fake, "tasks_run_a.json")))
    assert [(t["id"], t["task_title"], t["status"]) for t in tasks] == [("task_tk1", "Título exato do ticket", "ready")]
    assert "Task: task_tk1" in _lido(a, "01"), "a task_id fica no ticket"
    (ev,) = [e for e in a.events() if e["tipo"] == "ticket"]
    assert (ev["op"], ev["ticket"], ev["task"], ev["run"]) == ("novo", "01", "task_tk1", "run_a")


def test_ticket_novo_o_ticket_e_a_unica_fonte_do_conteudo():
    a = Amb(run="run_a")
    _novo(a, "Um", spec=_spec_tk(a, "Faça algo muito específico e longo.\n\nCom detalhes que não vão para o Orca."))
    (arg,) = _log(a, "created.log")
    assert "muito específico" not in json.dumps(arg), "o spec da task só aponta para o arquivo"
    assert "muito específico" in _lido(a, "01")


def test_ticket_novo_blocked_by_vira_deps_da_task_e_linha_do_ticket():
    a = Amb(run="run_a")
    _novo(a, "Base")
    _novo(a, "Outro")
    out = json.loads(_novo(a, "Depende", "--blocked-by", "01,02").stdout)
    assert out["ticket"] == "03"
    arg = _log(a, "created.log")[-1]
    assert json.loads(arg[arg.index("--deps") + 1]) == ["task_tk1", "task_tk2"], arg
    assert "Blocked by: 01, 02" in _lido(a, "03").split("\n## ")[0]
    assert json.load(open(os.path.join(a.fake, "tasks_run_a.json")))[2]["status"] == "pending"


def test_ticket_novo_blocker_resolvido_nao_entra_nas_deps():
    a = Amb(run="run_a")
    _novo(a, "Base")
    assert a.orq("ticket", "fechar", "01", "--answer", "feito").returncode == 0
    _novo(a, "Depois", "--blocked-by", "01")
    arg = _log(a, "created.log")[-1]
    assert "--deps" not in arg, "a task do blocker já está completed"
    assert "Blocked by: 01" in _lido(a, "02")


def test_ticket_novo_recusa_sem_criar_arquivo_nem_task():
    a = Amb(run="run_a")
    r = _novo(a, "Sem aceite", spec=_spec_tk(a, "Faça X.", aceite=""))
    assert r.returncode == 1 and "Acceptance criteria" in r.stderr, r.stderr
    r = _novo(a, "Blocker fantasma", "--blocked-by", "07")
    assert r.returncode == 1 and "07" in r.stderr, r.stderr
    r = a.orq("ticket", "novo", "--titulo", "Spec ausente", "--spec-arquivo", os.path.join(a.tmp.name, "nao-existe.md"))
    assert r.returncode == 1, r.stderr
    assert not os.path.exists(a.env["ORQ_ISSUES"]) or os.listdir(a.env["ORQ_ISSUES"]) == []
    assert _log(a, "created.log") == []


def test_ticket_novo_falha_do_task_create_nao_deixa_ticket_sem_task():
    a = Amb(run="run_a")
    r = _novo(a, "Vai falhar", FAKE_FAIL="task-create")
    assert r.returncode == 1 and "task-create" in r.stderr, r.stderr
    assert not os.path.exists(a.env["ORQ_ISSUES"]) or os.listdir(a.env["ORQ_ISSUES"]) == [], "o arquivo é desfeito"
    a.set("run.json", None)  # sem Run ligado
    r = _novo(a, "Sem Run")
    assert r.returncode == 1 and "--run" in r.stderr, r.stderr


def test_ticket_fechar_grava_answer_resolved_e_fecha_a_task():
    a = Amb(run="run_a")
    _novo(a, "Fechar este")
    r = a.orq("ticket", "fechar", "01", "--answer", "Resolvido pelo commit abc; ver desenho, seção 19.")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert (out["ticket"], out["status"], out["task"], out["task_fechada"]) == ("01", "resolved", "task_tk1", True), out
    txt = _lido(a, "01")
    assert "Status: resolved" in txt.split("\n## ")[0] and "ready-for-agent" not in txt
    assert txt.rstrip().endswith("## Answer\n\nResolvido pelo commit abc; ver desenho, seção 19."), txt
    assert txt.count("Status:") == 1
    (arg,) = _log(a, "updated.log")
    assert arg[:1] == ["task-update"] and arg[arg.index("--id") + 1] == "task_tk1" and arg[arg.index("--status") + 1] == "completed"
    assert arg[arg.index("--run") + 1] == "run_a"
    assert json.load(open(os.path.join(a.fake, "tasks_run_a.json")))[0]["status"] == "completed"
    (ev,) = [e for e in a.events() if e["tipo"] == "ticket" and e["op"] == "fechar"]
    assert (ev["ticket"], ev["task"], ev["task_fechada"]) == ("01", "task_tk1", True)


def test_ticket_fechar_desbloqueia_o_ticket_que_dependia_dele():
    a = Amb(run="run_a")
    _novo(a, "Base")
    _novo(a, "Depende", "--blocked-by", "01")
    a.orq("ticket", "fechar", "01", "--answer", "ok")
    tasks = json.load(open(os.path.join(a.fake, "tasks_run_a.json")))
    assert [t["status"] for t in tasks] == ["completed", "pending"], "o Orca faz o resto"  # o fake não reavalia deps: só conferimos o completed do blocker


def test_ticket_fechar_answer_pode_ser_um_arquivo():
    a = Amb(run="run_a")
    _novo(a, "Com arquivo")
    resp = os.path.join(a.tmp.name, "resposta.md")
    open(resp, "w").write("Linha 1\n\nLinha 2 do relatório.\n")
    assert a.orq("ticket", "fechar", "1", "--answer", resp).returncode == 0
    assert _lido(a, "01").rstrip().endswith("## Answer\n\nLinha 1\n\nLinha 2 do relatório."), "aceita 1 e 01"


def test_ticket_fechar_task_ja_concluida_nao_e_tocada():
    a = Amb(run="run_a")
    _novo(a, "Já concluída")
    a.set("tasks_run_a.json", [{"id": "task_tk1", "task_title": "x", "status": "completed", "deps": "[]"}])
    out = json.loads(a.orq("ticket", "fechar", "01", "--answer", "ok").stdout)
    assert out["task_fechada"] is False and out["status"] == "resolved", out
    assert _log(a, "updated.log") == []


def test_ticket_fechar_recusa_ticket_resolvido_e_inexistente():
    a = Amb(run="run_a")
    _novo(a, "Um")
    assert a.orq("ticket", "fechar", "01", "--answer", "ok").returncode == 0
    antes = _lido(a, "01")
    r = a.orq("ticket", "fechar", "01", "--answer", "de novo")
    assert r.returncode == 1 and "resolved" in r.stderr and _lido(a, "01") == antes, r.stderr
    r = a.orq("ticket", "fechar", "09", "--answer", "x")
    assert r.returncode == 1 and "09" in r.stderr, r.stderr
    r = _novo(a, "Dois") and a.orq("ticket", "fechar", "02", "--answer", "  ")
    assert r.returncode == 1 and "Status: resolved" not in _lido(a, "02"), "answer vazio não fecha"


def test_ticket_fechar_falha_do_orca_resolve_o_ticket_e_avisa():
    a = Amb(run="run_a")
    _novo(a, "Orca fora")
    r = a.orq("ticket", "fechar", "01", "--answer", "ok", FAKE_FAIL="task-update")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["status"] == "resolved" and out["task_fechada"] is False and "task-update" in out["aviso"], out
    assert "Status: resolved" in _lido(a, "01"), "o arquivo é a verdade; a task fecha depois"
    assert "aviso" in r.stderr


def test_ticket_fechar_ticket_sem_task_so_grava_o_arquivo():
    a = Amb(run="run_a")
    os.makedirs(a.env["ORQ_ISSUES"])
    with open(os.path.join(a.env["ORQ_ISSUES"], "01-antigo.md"), "w") as f:
        f.write("# Antigo\n\nStatus: claimed\nBlocked by: (nenhum)\n\n## What to build\n\nx\n")
    out = json.loads(a.orq("ticket", "fechar", "01", "--answer", "ok").stdout)
    assert out["status"] == "resolved" and out["task"] is None and out["task_fechada"] is False
    assert _log(a, "updated.log") == []


def test_ticket_lista_mostra_titulo_status_e_blocked_by():
    a = Amb(run="run_a")
    _novo(a, "Base")
    _novo(a, "Depende", "--blocked-by", "01")
    a.orq("ticket", "fechar", "01", "--answer", "ok")
    todos = json.loads(a.orq("ticket", "lista", "--todos", "--json").stdout)
    assert [(t["num"], t["titulo"], t["status"], t["blocked_by"]) for t in todos] == \
        [("01", "Base", "resolved", []), ("02", "Depende", "ready-for-agent", [])], "fechar o 01 tira o 01 do Blocked by do 02 (ticket 105)"
    abertos = a.orq("ticket", "lista").stdout
    assert "Depende" in abertos and "Blocked by" not in abertos and "Base" not in abertos, abertos
    _novo(a, "Outro bloqueado", "--blocked-by", "02")
    assert "Blocked by: 02" in a.orq("ticket", "lista").stdout, "o bloqueio que continua aberto aparece"


# o hook SessionStart

def _session(a, sid="abcdef123456", **env):
    return a.orq("hook", "session", stdin=json.dumps({"session_id": sid, "source": "startup", "hook_event_name": "SessionStart"}), **env)


def _ctx_session(a, **env):
    r = _session(a, **env)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["hookEventName"] == "SessionStart"
    return out["additionalContext"]


def test_hook_session_injeta_status_tickets_abertos_e_o_caminho_do_mapa():
    a = Amb(run="run_a")
    _novo(a, "Base")
    _novo(a, "Depende do base", "--blocked-by", "01")
    _novo(a, "Já feito")
    a.orq("ticket", "fechar", "03", "--answer", "ok")
    ctx = _ctx_session(a)
    linhas = ctx.splitlines()
    assert len(linhas) <= 12, ctx
    assert "[orq]" in linhas[0] and "Sem efeito" in linhas[0] and "Com você:" in ctx, "o orq status"
    assert "Tickets abertos (2)" in ctx and "01 Base (ready-for-agent)" in ctx, ctx
    assert "02 Depende do base (ready-for-agent; Blocked by: 01)" in ctx, ctx
    assert "Já feito" not in ctx, "resolvido não aparece"
    assert f"Mapa: {a.env['ORQ_MAPA']}" in linhas[-1], linhas[-1]


def test_hook_session_sem_ticket_aberto_diz_isso_em_uma_linha():
    a = Amb(run="run_a")
    ctx = _ctx_session(a)
    assert "Tickets abertos: nenhum" in ctx and len(ctx.splitlines()) <= 12 and "Mapa:" in ctx


def test_hook_session_muitos_tickets_ficam_em_12_linhas_com_o_resto_contado():
    a = Amb(run="run_a")
    for i in range(9):
        _novo(a, f"Ticket numero {i + 1}")
    ctx = _ctx_session(a)
    linhas = ctx.splitlines()
    assert len(linhas) == 12, (len(linhas), ctx)
    assert "Tickets abertos (9)" in ctx and "+" in linhas[-2] and "orq ticket lista" in linhas[-2], linhas[-2]
    assert linhas[-1].startswith("Mapa:") and "01 Ticket numero 1 " in ctx


def test_hook_session_so_no_coordenador_worker_fica_mudo():
    a = Amb(run=None)  # worker: sem Run ligado
    _novo_fora = Amb(run="run_a")
    r = _session(a)
    assert (r.returncode, r.stdout) == (0, ""), r
    assert not os.path.exists(a.home) and a.log() == "", "worker não grava nada nem é erro"
    b = Amb(run="run_a")  # sessão que já recebeu o preâmbulo de despacho é worker, mesmo com Run ligado
    b.prompt("Please carry out this task from my Orca coordinator by following the brief")
    r = _session(b)
    assert (r.returncode, r.stdout) == (0, ""), r
    c = Amb(run="run_a", ORCA_TERMINAL_HANDLE="")  # fora do Orca
    assert _session(c, ORCA_TERMINAL_HANDLE="").stdout == ""


def test_hook_session_e_fail_open():
    a = Amb(run="run_a")
    r = a.orq("hook", "session", stdin="isto não é json")
    assert (r.returncode, r.stdout) == (0, "") and "hook session" in a.log()
    b = Amb(run="run_a")
    r = _session(b, FAKE_CRASH="1")  # o Orca responde lixo
    assert (r.returncode, r.stdout) == (0, ""), r
    c = Amb(run="run_a")
    os.makedirs(c.env["ORQ_ISSUES"])
    open(os.path.join(c.env["ORQ_ISSUES"], "01-quebrado.md"), "wb").write(b"\xff\xfe nao e utf-8 \x00")
    r = _session(c)
    assert r.returncode == 0, r.stderr  # ticket ilegível não derruba o resumo


def test_hook_session_esta_registrado_no_settings_e_o_comando_funciona():
    cfg = json.load(open(os.path.expanduser("~/.claude/settings.json")))
    cmds = [h["command"] for g in cfg["hooks"]["SessionStart"] for h in g["hooks"] if h.get("type") == "command" and "orq.py hook session" in h["command"]]
    assert len(cmds) == 1, cmds
    a = Amb(run="run_a")
    _novo(a, "Pelo comando registrado")
    r = subprocess.run(cmds[0], shell=True, input=json.dumps({"session_id": "s1", "source": "resume"}), capture_output=True, text=True, env={**a.env, "ORQ_HOME": a.home}, timeout=30)
    assert r.returncode == 0 and "Pelo comando registrado" in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"], r


# guarda de lugar errado (ticket 33)

def _repo(base, ramo="main"):
    """Checkout principal com um commit, na branch `ramo`, e uma worktree ligada `wt`; devolve (principal, worktree)."""
    p, w = os.path.join(base, "repo"), os.path.join(base, "wt")
    g = lambda *a, cwd=p: subprocess.run(["git", "-C", cwd, *a], check=True, capture_output=True, env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                                                                                                         "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"})
    os.makedirs(p)
    g("init", "-q", "-b", "main")
    g("commit", "-q", "--allow-empty", "-m", "x")
    g("worktree", "add", "-q", "-b", "feat/w", w)
    if ramo != "main":
        g("checkout", "-q", "-b", ramo)
    return p, w


def _lugar(a, cwd, cmd="git commit -m x", tool="Bash", sid="abcdef123456", **env):
    ti = {"command": cmd} if tool == "Bash" else {"file_path": os.path.join(cwd, "a.txt")}
    return a.orq("hook", "lugar", stdin=json.dumps({"session_id": sid, "cwd": cwd, "hook_event_name": "PreToolUse", "tool_name": tool, "tool_input": ti}), **env)


def _aviso(r):
    assert r.returncode == 0, r
    if not r.stdout:
        return ""
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["hookEventName"] == "PreToolUse" and "permissionDecision" not in out, "só avisa, nunca bloqueia"
    return out["additionalContext"]


def test_lugar_checkout_principal_na_branch_padrao_nao_avisa():
    a = Amb(run="run_a")
    a.prompt("oi")  # a sessão vira coordenadora (Run guardado)
    p, _ = _repo(a.tmp.name)
    assert _aviso(_lugar(a, p)) == ""
    assert _aviso(_lugar(a, p, tool="Edit")) == ""
    assert _aviso(_lugar(a, p, cmd="git status")) == "", "comando que não escreve passa"


def test_lugar_checkout_principal_fora_da_branch_padrao_avisa():
    a = Amb(run="run_a")
    a.prompt("oi")
    p, _ = _repo(a.tmp.name, ramo="feat/outra")
    for r in (_lugar(a, p), _lugar(a, p, cmd="cd x && git push origin HEAD"), _lugar(a, p, tool="Write")):
        msg = _aviso(r)
        assert "lugar errado" in msg and "feat/outra" in msg and "main" in msg, msg
    assert _aviso(_lugar(a, p, cmd="echo git commit")) == ""


def test_lugar_cwd_em_worktree_de_worker_avisa_mas_a_casa_do_coordenador_nao():
    a = Amb(run="run_a")
    a.prompt("oi")
    p, w = _repo(a.tmp.name)
    msg = _aviso(_lugar(a, w, CLAUDE_PROJECT_DIR=p))
    assert "lugar errado" in msg and os.path.realpath(w) in msg, msg
    assert _aviso(_lugar(a, w, CLAUDE_PROJECT_DIR=w)) == "", "coordenador que mora numa worktree ligada não é engano"


def test_lugar_so_no_coordenador_e_abaixo_de_100_ms():
    a = Amb(run="run_a")
    p, _ = _repo(a.tmp.name, ramo="feat/outra")
    assert _aviso(_lugar(a, p)) == "", "sessão sem Run guardado não é coordenador"
    a.prompt("oi")
    w = Amb(run="run_a")  # worker: preâmbulo de despacho grava o papel
    pw, _ = _repo(w.tmp.name, ramo="feat/outra")
    w.prompt(PREAMBULO)
    assert _aviso(_lugar(w, pw)) == "" and _calls(w, "run-current") == []
    assert _aviso(_lugar(a, p, ORCA_TERMINAL_HANDLE="")) == "", "fora do Orca"
    ev = {"session_id": "abcdef123456", "cwd": p, "tool_name": "Bash", "tool_input": {"command": "git commit -m x"}}
    t = time.perf_counter()
    orq_mod.hook_lugar(ev, None)
    assert time.perf_counter() - t < 0.1, "o hook em si (git incluído) passa de 100 ms"


def test_lugar_esta_no_settings_de_exemplo():
    cfg = json.load(open(os.path.join(AQUI, "settings.hooks.example.json")))
    g = [x for x in cfg["hooks"]["PreToolUse"] if "orq.py hook lugar" in json.dumps(x)]
    assert len(g) == 1 and "Bash" in g[0]["matcher"] and "Edit" in g[0]["matcher"], g


# a migração

MIGRACAO = os.path.join(AQUI, "fixtures", "issues-migracao")  # tickets 01 a 08 de exemplo (B20: nunca a pasta real, que muda a cada `orq ticket novo`)


def _confere_migracao(d):
    """A migração da fatia 9: os tickets 01 a 08 e o formato de cada um. Não exige o total de arquivos nem o status do 07."""
    arqs = sorted(os.listdir(d))[:8]
    assert [a[:2] for a in arqs] == ["01", "02", "03", "04", "05", "06", "07", "08"], arqs
    def cab(nome):
        return open(os.path.join(d, nome)).read().split("\n## ")[0]
    for nome, fatia in zip(arqs[:6], (1, 2, 3, 4, 5, 6)):
        txt = open(os.path.join(d, nome)).read()
        assert txt.startswith(f"# {nome[:2]}: demo slice {fatia}:") and "Status: resolved" in cab(nome), nome
        resp = txt.split("\n## Answer\n")[1]
        assert "docs/design.md" in resp, f"{nome}: o Answer aponta para o design"
        assert "## What to build" in txt and "## Acceptance criteria" in txt
    f8, f9 = arqs[6], arqs[7]
    assert open(os.path.join(d, f8)).read().startswith("# 07: demo slice 7: agent manager") and "Task: task_demo00000007" in cab(f8)
    assert "Task: task_demo00000008" in cab(f9) and open(os.path.join(d, f9)).read().startswith("# 08: demo slice 8:")
    for nome in (f8, f9):
        assert "## What to build" in open(os.path.join(d, nome)).read() and "## Acceptance criteria" in open(os.path.join(d, nome)).read()


def test_migracao_um_ticket_por_fatia_entregue_e_um_para_cada_aberta():
    _confere_migracao(MIGRACAO)


def test_worker_routing_manda_a_tarefa_nova_nascer_como_ticket():
    txt = open(os.path.join(AQUI, "skills", "worker-routing", "SKILL.md")).read()
    linha = [l for l in txt.splitlines() if "orq ticket novo" in l]
    assert len(linha) == 1 and "ticket" in linha[0].lower() and "orq ticket fechar" in linha[0], linha


# "já fez" pelo Lavish (pedido de 29/09, lote 339e0af655)

def _pends_lavish(a):
    a.orq("pend", "add", "--id", "git-flow", "--tipo", "decisao", "--titulo", "Qual fluxo?")
    a.orq("pend", "add", "--id", "dana-ia", "--tipo", "avisar", "--titulo", "Avisar o Dana")
    a.orq("pend", "add", "--id", "vendor-ia", "--tipo", "acao", "--titulo", "Rodar a checagem")


def test_lavish_feito_fecha_pendencia_de_acao_e_aviso_e_o_lote_de_29_09_agora_fecha():
    a = Amb()
    _pends_lavish(a)
    itens = [{"id": "git-flow", "header": "git-flow", "resposta": "feito", "disposicao": "escolha"},
             {"id": "dana-ia", "header": "dana-ia", "resposta": "feito", "disposicao": "escolha"},
             {"id": "vendor-ia-x", "header": "vendor-ia", "resposta": "", "disposicao": "feito"}]
    r = a.orq("lavish-resposta", _lote(a, itens))
    assert r.returncode == 0, r.stderr
    assert sorted(_ids_pend(a)) == ["avisar-x", "freio-prod"], "os três fecharam, qualquer tipo"
    saida = json.loads(r.stdout)
    assert [i["efeito"] for i in saida["itens"]] == ["fechou"] * 3, saida
    evs = [e for e in a.events() if e["tipo"] == "resposta_lavish"]
    assert [e.get("fechou") for e in evs] == [["git-flow"], ["dana-ia"], ["vendor-ia"]] and not any(e.get("livre") for e in evs), evs


def test_lavish_ja_fez_com_os_ids_estruturados_fecha_so_os_marcados():
    a = Amb()
    _pends_lavish(a)
    itens = [{"id": "ja-fez", "header": "ja-fez", "resposta": "dana-ia e vendor-ia", "ids": ["dana-ia", "vendor-ia", "nada-a-ver"], "disposicao": "escolha"}]
    r = a.orq("lavish-resposta", _lote(a, itens))
    assert r.returncode == 0, r.stderr
    assert sorted(_ids_pend(a)) == ["avisar-x", "freio-prod", "git-flow"], "fechou dana-ia e vendor-ia; git-flow segue aberto"
    (ev,) = [e for e in a.events() if e["tipo"] == "resposta_lavish"]
    assert sorted(ev["fechou"]) == ["dana-ia", "vendor-ia"], ev
    a2 = Amb()
    _pends_lavish(a2)
    r = a2.orq("lavish-resposta", _lote(a2, [{"id": "ja-fez", "header": "ja-fez", "resposta": "", "ids": ["git-flow"], "disposicao": "feito"}]))
    assert "git-flow" not in _ids_pend(a2), "o lote também pode trazer a lista em ids"


def test_lavish_texto_livre_e_adiamento_seguem_abertos_regra_do_m3():
    a = Amb()
    _pends_lavish(a)
    itens = [{"id": "L1", "header": "dana-ia", "resposta": "acho que já fiz, mas confere", "disposicao": "livre"},
             {"id": "L2", "header": "vendor-ia", "resposta": "feito", "disposicao": "adiar"},
             {"id": "L3", "header": "git-flow", "resposta": "__conversar", "disposicao": "conversar"},
             {"id": "ja-fez", "header": "ja-fez", "resposta": "dana-ia", "disposicao": "conversar"}]
    r = a.orq("lavish-resposta", _lote(a, itens))
    assert r.returncode == 0, r.stderr
    assert {"git-flow", "dana-ia", "vendor-ia"} <= set(_ids_pend(a)), "nada fechou"
    assert [i["efeito"] for i in json.loads(r.stdout)["itens"]][:2] == ["so registrada", "so registrada"]


def test_despachar_ticket_usa_a_task_do_ticket_e_nao_cria_outra():
    a = Amb(run="run_a")
    out = json.loads(_novo(a, "Ticket para despachar").stdout)
    r = a.orq("despachar", "--run", "run_a", "--ticket", "01", "--modelo", "claude-sonnet-5-5", "--effort", "medium")
    assert r.returncode == 0, r.stderr
    res = json.loads(r.stdout)
    assert res["taskId"] == out["task"] and res["dispatchId"] == "ctx_term_novo1", res
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--task") + 1] == out["task"] and "--spec" not in arg and "--task-title" not in arg, arg
    assert arg[arg.index("--model") + 1] == "claude-sonnet-5-5" and arg[arg.index("--effort") + 1] == "medium"
    assert [t["id"] for t in json.load(open(os.path.join(a.fake, "tasks_run_a.json")))] == [out["task"]], "nenhuma task nova"
    (ev,) = [e for e in a.events() if e["tipo"] == "despacho"]
    assert (ev["task"], ev["titulo"], ev["ticket"]) == (out["task"], "Ticket para despachar", "01"), ev


def test_despachar_ticket_recusa_resolvido_sem_task_ou_junto_do_spec():
    a = Amb(run="run_a")
    _novo(a, "Um")
    a.orq("ticket", "fechar", "01", "--answer", "ok")
    base = ["despachar", "--run", "run_a", "--modelo", "claude-sonnet-5-5", "--effort", "medium"]
    r = a.orq(*base, "--ticket", "01")
    assert r.returncode == 1 and "resolved" in r.stderr, r.stderr
    r = a.orq(*base, "--ticket", "09")
    assert r.returncode == 1 and "09" in r.stderr, r.stderr
    r = a.orq(*base, "--ticket", "01", "--spec-arquivo", _spec(a))
    assert r.returncode != 0, "--ticket e --spec-arquivo não andam juntos"
    b = Amb(run="run_a")
    _novo(b, "Outro run", "--run", "run_a")
    r = b.orq("despachar", "--run", "run_b", "--ticket", "01", "--modelo", "claude-sonnet-5-5", "--effort", "medium")
    assert r.returncode == 1 and "run_a" in r.stderr, "o ticket é de outro Run: " + r.stderr
    assert _log(a, "started.log") == [] and _log(b, "started.log") == []


# ---------- review-5 (review-5.md): M9 a M11 e B20 a B28. Cada achado tem um teste que falha no orq.py de antes ----------

PREAMBULO_WORKER = ("Please carry out this task from my Orca coordinator by following the brief I pasted below. You are working inside Orca, a multi-agent IDE.\n"
             "orca orchestration send --from term_x --type worker_done --task-id task_w1 --dispatch-id {d} --outcome succeeded\n")


def _linha_user(conteudo, meta=False):
    return json.dumps({"type": "user", "isMeta": meta, "message": {"role": "user", "content": conteudo}})


def _transcrito_do_worker(a, dispatch, *prompts, preambulo=True, nome="worker.jsonl"):
    """O transcrito da sessão do worker no formato do Claude Code: o preâmbulo do despacho, tool_result, meta e os `prompts` digitados depois."""
    pasta = os.path.join(a.tmp.name, "projetos", "-proj")
    os.makedirs(pasta, exist_ok=True)
    a.env["ORQ_PROJETOS"] = os.path.join(a.tmp.name, "projetos")
    linhas = [json.dumps({"type": "summary", "summary": "x"})]
    if preambulo:
        linhas.append(_linha_user([{"type": "text", "text": PREAMBULO_WORKER.format(d=dispatch)}]))
    linhas += [json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "ok"}]}}),
               _linha_user([{"type": "tool_result", "tool_use_id": "t1", "content": "saída"}]),
               _linha_user("<system-reminder>hook</system-reminder>", meta=True),
               _linha_user("You have 1 orchestration message. Run `orca orchestration check --run run_a`."),
               _linha_user("<task-notification><task-id>x</task-id></task-notification>")]
    linhas += [_linha_user(p) for p in prompts]
    with open(os.path.join(pasta, nome), "w") as f:
        f.write("\n".join(linhas) + "\n")


def _takeover(a, **w):
    """O worker concluído cujo terminal o Orca reteve como user_takeover (o Orca marca isso a qualquer onData do xterm, sem humano)."""
    _lib_env(a, release="retained", release_reason="user_takeover", release_ownership="user_owned", **w)


def test_review5_m9_user_takeover_sem_prompt_humano_no_transcrito_fecha_o_terminal_do_proprio_dispatch():
    a = Amb(run="run_a")
    _takeover(a)
    _transcrito_do_worker(a, "ctx_term_w1")
    out = json.loads(a.orq("liberar", "ctx_term_w1").stdout)
    assert (out["estado"], out["fechado"]) == ("retained", True), out
    assert _log(a, "close.log") == [["close", "--terminal", "term_w1", "--json"]]
    (ev,) = [e for e in a.events() if e["tipo"] == "liberar"]
    assert ev["fechado"] is True and not ev.get("interacao"), ev


def test_review5_m9_user_takeover_com_prompt_humano_nao_fecha_e_registra_a_interacao():
    for texto in ("para aí, deixa que eu vejo", [{"type": "text", "text": "e o teste?"}]):
        a = Amb(run="run_a")
        _takeover(a)
        _transcrito_do_worker(a, "ctx_term_w1", texto)
        out = json.loads(a.orq("liberar", "ctx_term_w1").stdout)
        assert out["fechado"] is False and "user_takeover" in out["aviso"] and not _log(a, "close.log"), out
        (ev,) = [e for e in a.events() if e["tipo"] == "liberar"]
        assert ev["interacao"] is True, ev


def test_review5_m9_user_takeover_sem_transcrito_ou_de_outro_dispatch_nao_fecha():
    a = Amb(run="run_a")
    _takeover(a)  # nenhum transcrito
    out = json.loads(a.orq("liberar", "ctx_term_w1").stdout)
    assert out["fechado"] is False and "transcrito" in out["aviso"] and not _log(a, "close.log"), out
    b = Amb(run="run_a")
    _takeover(b)
    _transcrito_do_worker(b, "ctx_outro_dispatch")  # o transcrito é de outro worker
    assert json.loads(b.orq("liberar", "ctx_term_w1").stdout)["fechado"] is False and not _log(b, "close.log")
    c = Amb(run="run_a")
    _takeover(c, origem="ctx_de_outro")  # o terminal não nasceu deste dispatch (worker-start --terminal)
    _transcrito_do_worker(c, "ctx_term_w1")
    assert json.loads(c.orq("liberar", "ctx_term_w1").stdout)["fechado"] is False and not _log(c, "close.log")


def test_review5_m9_transcrito_limpo_nao_abre_o_que_o_orca_reteve_por_outro_motivo():
    for motivo in ("external_terminal", "configured_tab", "reused_terminal", "identity_unproven", "user_requested", "motivo_novo"):
        a = Amb(run="run_a")
        _lib_env(a, release="retained", release_reason=motivo, release_ownership="external")
        _transcrito_do_worker(a, "ctx_term_w1")
        out = json.loads(a.orq("liberar", "ctx_term_w1").stdout)
        assert out["fechado"] is False and motivo in out["aviso"] and not _log(a, "close.log"), (motivo, out)


def test_review5_m9_o_prompt_do_coordenador_e_do_orca_nao_conta_como_humano():
    a = Amb(run="run_a")
    _takeover(a)
    _transcrito_do_worker(a, "ctx_term_w1", "You have 2 orchestration messages. Run `orca orchestration check --run run_a`.",
                          "<task-notification><task-id>b</task-id></task-notification>", "This session is being continued from a previous conversation")
    assert json.loads(a.orq("liberar", "ctx_term_w1").stdout)["fechado"] is True


def test_review5_m9_agentes_mostra_o_user_takeover_entregue_com_o_orq_liberar_e_esconde_so_depois_da_interacao():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_w1", "status": "completed", "terminal": "retained",
                            "reason": "user_takeover", "ownership": "user_owned", "release": "retained", "release_reason": "user_takeover", "release_ownership": "user_owned"}])
    a.set("runs.json", [{"id": "run_a", "objective": "A"}])
    ag = _agentes(a)["ctx_term_w1"]
    assert ag["estado"] == "entregue" and not ag.get("retido"), ag
    r = a.orq("agentes")
    assert "orq liberar ctx_term_w1" in r.stdout, r.stdout
    a.orq("ingest", "--refresh")
    assert "ctx_term_w1" in {x["dispatch"] for x in json.load(open(os.path.join(a.home, "aberto.json")))["agentes"]}
    a.set("terminals.json", ["term_w1"])
    _transcrito_do_worker(a, "ctx_term_w1", "estou mexendo aqui")
    a.orq("liberar", "ctx_term_w1")
    assert "ctx_term_w1" not in _agentes(a), "com o usuário de fato no terminal, sai da lista"
    assert _agentes(a, "--todos")["ctx_term_w1"]["retido"] == "user_takeover"
    a.orq("ingest", "--refresh")
    assert "ctx_term_w1" not in {x["dispatch"] for x in json.load(open(os.path.join(a.home, "aberto.json")))["agentes"]}


def test_review5_m9_o_resumo_conta_o_user_takeover_entre_os_entregues_sem_liberar():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_w1", "status": "completed", "terminal": "retained",
                            "reason": "user_takeover", "ownership": "user_owned"}])
    a.set("runs.json", [{"id": "run_a", "objective": "A"}])
    a.orq("ingest", "--refresh")
    ctx = json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "Entregues sem liberar 1" in ctx, ctx


def test_review5_m10_liberar_acha_a_linha_pelo_dispatch_e_nao_fecha_terminal_reaproveitado_por_worker_rodando():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_t", "dispatch": "ctx_b", "run": "run_a", "task": "task_b", "status": "dispatched"},  # o mais novo, rodando no mesmo terminal
                           {"handle": "term_t", "dispatch": "ctx_a", "run": "run_a", "task": "task_a", "status": "completed", "terminal": "active", "release": "retained"}])
    a.set("terminals.json", ["term_t", "term_coord"])
    out = json.loads(a.orq("liberar", "ctx_a").stdout)
    assert out["fechado"] is False and "ctx_b" in out["aviso"] and not _log(a, "close.log"), out
    b = Amb(run="run_a")  # sem ninguém no terminal, o liberar do mais velho continua fechando o dele
    b.set("workers.json", [{"handle": "term_t", "dispatch": "ctx_b", "run": "run_a", "task": "task_b", "status": "completed", "terminal": "released"},
                           {"handle": "term_t", "dispatch": "ctx_a", "run": "run_a", "task": "task_a", "status": "completed", "terminal": "active", "release": "retained"}])
    b.set("terminals.json", ["term_t", "term_coord"])
    assert json.loads(b.orq("liberar", "ctx_a").stdout)["fechado"] is True


def test_review5_m11_aviso_de_outro_run_com_worker_done_nao_lido_e_antigo_passa_mesmo_com_heartbeat_novo():
    a = Amb(run="run_a")
    _inbox(a, _hb_inbox(a, "ctx_9", "", -200, 900, tipo="worker_done"), _hb_inbox(a, "ctx_9", "fase-4", -30, 901))
    r = a.prompt(AVISO_B)
    assert (r.returncode, r.stdout) == (0, ""), r
    assert not [e for e in a.events() if e["tipo"] == "heartbeat_visto"]
    b = Amb(run="run_a")  # o worker_done já lido não segura: o aviso era dele e ele foi confirmado
    _inbox(b, {**_hb_inbox(b, "ctx_9", "", -200, 900, tipo="worker_done"), "read": 1}, _hb_inbox(b, "ctx_9", "fase-4", -30, 901))
    assert _bloqueado(b.prompt(AVISO_B))
    c = Amb(run="run_a")  # heartbeat sem ack de minutos atrás, sozinho: sem janela de tempo
    _inbox(c, _hb_inbox(c, "ctx_9", "fase-4", -600, 900))
    assert _bloqueado(c.prompt(AVISO_B))


# B20: a migração é conferida contra a cópia em fixtures, e o uso normal dos tickets não a quebra
def test_review5_b20_a_migracao_aguenta_ticket_novo_e_ticket_fechado_na_pasta():
    import shutil
    a = Amb(run="run_a")
    d = os.path.join(a.tmp.name, "issues-real")
    shutil.copytree(MIGRACAO, d)
    for n in os.listdir(d):
        if n.startswith("07-"):  # o coordenador fecha o 07 com orq ticket fechar
            p = os.path.join(d, n)
            txt = open(p).read()
            open(p, "w").write(txt.replace("Status: claimed", "Status: resolved"))
    open(os.path.join(d, "09-novo.md"), "w").write("# 09: Novo\n\nStatus: ready-for-agent\n")
    _confere_migracao(d)  # não exige o total de arquivos nem o status do 07
    assert "ISSUES" not in open(__file__).read().split("def _confere_migracao")[1].split("def test_migracao")[0], "não lê a pasta real"


# B21
def test_review5_b21_o_guard_nao_barra_comando_que_so_cita_o_orq_despachar_entre_aspas():
    guard = os.path.join(AQUI, "hooks", "worker-routing-guard.py")

    def roda(cmd):
        r = subprocess.run([sys.executable, guard], input=json.dumps({"tool_name": "Bash", "tool_input": {"command": cmd}}), capture_output=True, text=True)
        return json.loads(r.stdout)["hookSpecificOutput"]["permissionDecision"] if r.stdout.strip() else "allow"

    assert roda('echo "veja: python3 ~/.claude/orq/orq.py despachar --run r"') == "allow"
    assert roda("echo 'veja: python3 ~/.claude/orq/orq.py despachar --run r'") == "allow"
    assert roda('git commit -m "fix; python3 orq.py despachar sem modelo"') == "allow"
    assert roda("python3 ~/.claude/orq/orq.py despachar --run r --titulo 'a b' --spec-arquivo f") == "deny", "o comando de verdade continua barrado"
    assert roda('cd /tmp && python3 orq.py despachar --run r --titulo "a b" --spec-arquivo f') == "deny"
    assert roda("orq despachar --run r --titulo T --spec-arquivo f --modelo m --effort low") == "allow"


# B22
def test_review5_b22_ja_fez_em_texto_livre_sem_ids_nao_fecha_nada():
    a = Amb()
    _pends_lavish(a)
    itens = [{"id": "ja-fez", "header": "ja-fez", "resposta": "feito o dana-ia, menos o vendor-ia", "disposicao": "escolha"}]
    r = a.orq("lavish-resposta", _lote(a, itens))
    assert r.returncode == 0, r.stderr
    assert {"git-flow", "dana-ia", "vendor-ia"} <= set(_ids_pend(a)), "sem ids estruturados nada fecha"
    saida = json.loads(r.stdout)
    assert saida["itens"][0]["efeito"] == "so registrada" and any("ids" in x for x in saida["avisos"]), saida
    (ev,) = [e for e in a.events() if e["tipo"] == "resposta_lavish"]
    assert ev.get("livre") is True and not ev.get("fechou"), ev


# B23
def test_review5_b23_blocker_de_outro_run_fica_so_na_linha_e_o_comando_avisa():
    a = Amb(run="run_a")
    _novo(a, "Do run a")
    p = _ticket(a, "01")
    txt = open(p).read()
    open(p, "w").write(txt.replace("Run: run_a", "Run: run_z"))  # o ticket 01 nasceu em outro Run
    r = _novo(a, "Depende de outro run", "--blocked-by", "01")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert "run_z" in out["aviso"] and "01" in out["aviso"], out
    assert "--deps" not in _log(a, "created.log")[-1], "a task de outro Run não pode entrar nas deps"
    assert "Blocked by: 01" in _lido(a, "02")


# B24
def test_review5_b24_despachar_ticket_marca_claimed_no_arquivo():
    a = Amb(run="run_a")
    _novo(a, "Para despachar")
    assert "Status: ready-for-agent" in _lido(a, "01")
    r = a.orq("despachar", "--run", "run_a", "--ticket", "01", "--modelo", "claude-sonnet-5-5", "--effort", "medium")
    assert r.returncode == 0, r.stderr
    assert "Status: claimed" in _lido(a, "01") and "ready-for-agent" not in _lido(a, "01")
    assert "01 Para despachar (claimed)" in a.orq("ticket", "lista").stdout
    b = Amb(run="run_a")  # worker-start que falha não marca
    _novo(b, "Falha")
    b.orq("despachar", "--run", "run_a", "--ticket", "01", "--modelo", "m", "--effort", "low", FAKE_FAIL="worker-start")
    assert "Status: ready-for-agent" in _lido(b, "01")


# B25
def test_review5_b25_ticket_novo_toma_a_trava_do_numero():
    import fcntl
    a = Amb(run="run_a")
    os.makedirs(a.home, exist_ok=True)
    spec = _spec_tk(a)
    with open(os.path.join(a.home, "ticket.lock"), "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)  # outro `ticket novo` está escolhendo o número
        p = subprocess.Popen([sys.executable, ORQ, "ticket", "novo", "--titulo", "Espera", "--spec-arquivo", spec], env=a.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(1.5)
        criados = [n for n in (os.listdir(a.env["ORQ_ISSUES"]) if os.path.isdir(a.env["ORQ_ISSUES"]) else []) if n.endswith(".md")]
        assert p.poll() is None and not criados, "preso na trava, sem arquivo"
    out, err = p.communicate(timeout=20)
    assert p.returncode == 0, err
    assert json.loads(out)["ticket"] == "01"


# B26
def test_review5_b26_aviso_do_orca_colado_no_texto_do_usuario_e_separado_e_marcado():
    a = Amb(run="run_a")
    r = a.prompt("s\n" + AVISO_A)
    assert json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"], r
    (e,) = [x for x in a.events() if x["tipo"] == "entrada"]
    assert e["origem"] == "usuario" and e["texto"] == "s" and e.get("com_aviso") is True, e
    assert _cursor(a).get("chegada"), "o aviso digitado marca a chegada, como o aviso puro"
    b = Amb(run="run_a")
    b.prompt("me lembra: You have 3 orchestration messages. Run é o texto do Orca, mas aqui vai a frase inteira")
    (e,) = [x for x in b.events() if x["tipo"] == "entrada"]
    assert not e.get("com_aviso") and e["texto"].startswith("me lembra"), e
    c = Amb(run="run_a")
    assert c.prompt(AVISO_A).stdout == "" and c.events() == [], "aviso puro segue sem virar entrada"


# B27
def test_review5_b27_session_start_sem_cache_pede_o_refresh():
    a = Amb(run="run_a")
    a.set("runs.json", [{"id": "run_a", "objective": "A"}])
    assert not os.path.exists(os.path.join(a.home, "aberto.json"))
    r = a.orq("hook", "session", stdin=json.dumps({"session_id": "s1", "source": "startup", "hook_event_name": "SessionStart"}), ORQ_NO_BG="")
    assert r.returncode == 0 and "additionalContext" in r.stdout, r
    for _ in range(80):
        if os.path.exists(os.path.join(a.home, "aberto.json")):
            break
        time.sleep(0.1)
    assert os.path.exists(os.path.join(a.home, "aberto.json")), "o hook de sessão disparou o refresh"


# B28
def test_review5_b28_os_hooks_python_saem_com_zero_e_sem_traceback_em_entrada_ruim():
    hooks = [os.path.join(AQUI, "hooks", "worker-routing-guard.py"), LIMPAR]
    for h in hooks:
        for entrada in ("", "lixo", "[]", "null", '{"prompt":123}', '{"tool_name":"Bash","tool_input":[]}', '{"tool_name":"Bash","tool_input":{"command":5}}', '{"cwd":5,"prompt":"merged"}'):
            r = subprocess.run([sys.executable, h], input=entrada, capture_output=True, text=True, timeout=20)
            assert r.returncode == 0 and "Traceback" not in r.stderr, (os.path.basename(h), entrada, r.returncode, r.stderr[-200:])


# ---------- marca visual (ticket 10) ----------
def test_marca_heartbeat_absorvido_uma_linha_com_a_contagem():
    a = Amb()
    a.caixa(_hb("a"), _hb("b"), _hb("c"))
    assert json.loads(a.prompt(AVISO_A).stdout)["reason"] == "🟣 orq · 3 sinais de vida absorvidos (run_a)"


def test_marca_heartbeat_atrasado():
    a = Amb()
    a.caixa(_hb("a"))
    a.prompt(AVISO_A)
    assert json.loads(a.prompt(AVISO_A).stdout)["reason"].startswith(orq_mod.MARCA)


def test_marca_heartbeat_de_outro_run():
    a = Amb(run="run_a")
    a.caixa(_hb("lendo"), run="run_b")
    _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -5, 900))
    reason = json.loads(a.prompt(AVISO_B).stdout)["reason"]
    assert reason.startswith(orq_mod.MARCA) and "run_b" in reason and "\n" not in reason, reason


def test_marca_stop():
    a = Amb()
    a.prompt("boa")
    assert json.loads(a.orq("hook", "stop", stdin="{}").stdout)["systemMessage"].startswith(orq_mod.MARCA)


def test_marca_guard():
    a = Amb(run="run_a")
    _workers(a, ("w_ativo", "run_a", "dispatched"))
    out = json.loads(_guard(a).stdout)["hookSpecificOutput"]
    assert out["permissionDecisionReason"].startswith(orq_mod.MARCA)


# ---- review-6 (M12, M13, B29 a B37; M12, B29, B32 e B33 estão no test_precompact.py) ----

def test_review6_m13_resumo_nao_pede_para_liberar_o_que_o_liberar_ja_liberou():
    from datetime import datetime, timezone
    ab = _aberto_ag("entregue")
    d = ab["agentes"][0]["dispatch"]
    assert "sem liberar 1" in orq_mod.linha_vivos([], ab, datetime.now(timezone.utc))
    assert orq_mod.linha_vivos([{"tipo": "liberar", "dispatch": d, "fechado": True}], ab) == ""
    assert orq_mod.linha_vivos([{"tipo": "liberar", "dispatch": d, "estado": "released", "fechado": False}], ab) == ""


def test_review6_m13_liberar_repetido_nao_manda_release_nem_close_e_o_resumo_bate_com_agentes():
    a = Amb(run="run_a")
    _lib_env(a, release="retained")
    a.orq("ingest", "--refresh")
    assert "sem liberar 1" in json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]
    assert json.loads(a.orq("liberar", "ctx_term_w1").stdout)["fechado"] is True
    a.set("terminals.json", ["term_w2", "term_coord"])
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "sem liberar" not in ctx, ctx
    n = len(_log(a, "calls.log"))
    out = json.loads(a.orq("liberar", "ctx_term_w1").stdout)
    assert out["aviso"] == "já liberado" and len([e for e in a.events() if e["tipo"] == "liberar"]) == 1
    assert not [c for c in _log(a, "calls.log")[n:] if c[0] in ("worker-release", "close")]
    assert len(_log(a, "close.log")) == 1


def test_review6_b30_alerta_some_com_visto_liberar_ou_intake_da_task():
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)
    al = {"tipo": "alerta", "alerta": "scout_sem_relatorio", "task": "task_1", "titulo": "[scout] x", "ts": now_iso(-60)}
    assert len(orq_mod.alertas_recentes([al], agora)) == 1
    for depois in ({"tipo": "alerta_visto", "task": "task_1"}, {"tipo": "liberar", "task": "task_1", "dispatch": "ctx_1"},
                   {"tipo": "intake", "entrada": "e1", "efeito": "tarefa", "ref": "task_1"}):
        assert orq_mod.alertas_recentes([al, depois], agora) == [], depois
    assert len(orq_mod.alertas_recentes([{"tipo": "alerta_visto", "task": "task_1"}, al], agora)) == 1, "visto antes do alerta não vale"
    a = Amb()
    assert a.orq("alerta", "visto", "task_9").returncode == 0
    assert [(e["tipo"], e["task"]) for e in a.events()] == [("alerta_visto", "task_9")]


def test_review6_b31_timeout_do_orca_vem_do_ambiente_e_o_runner_mostra_o_fim_da_mensagem():
    r = subprocess.run([sys.executable, "-c", "import orq; print(orq.TIMEOUT_ORCA)"], capture_output=True, text=True, cwd=AQUI,
                       env={**os.environ, "ORQ_ORCA_TIMEOUT": "7.5"})
    assert r.stdout.strip() == "7.5", r
    assert orq_mod.TIMEOUT_ORCA == float(os.environ.get("ORQ_ORCA_TIMEOUT") or 2.5)
    assert "str(e)[-400:]" in open(__file__).read().split('if __name__ == "__main__":')[-1], "o runner tem de mostrar o fim da mensagem (B40)"  # a linha do assert fica antes do __main__
    assert Amb().env["ORQ_ORCA_TIMEOUT"] == "10"


def test_review6_b37_terminal_list_truncado_nao_prova_que_o_terminal_morreu():
    a = Amb(run="run_a")
    a.set("terminals.json", ["term_a", "term_b"])
    env = {k: a.env[k] for k in ("ORQ_HOME", "ORQ_ORCA", "FAKE_DIR", "ORQ_LOG")}
    prog = "import orq; print(sorted(orq._terminais_vivos() or ['NONE']))"
    def vivos():
        return subprocess.run([sys.executable, "-c", prog], capture_output=True, text=True, cwd=AQUI, env={**os.environ, **env}).stdout.strip()
    assert vivos() == "['term_a', 'term_b']" and ["list", "--limit", "1000", "--json"] in _log(a, "calls.log")
    open(os.path.join(a.fake, "terminals_truncados"), "w").close()
    assert vivos() == "['NONE']"


def test_review7_b38_dispatch_liberado_nao_vira_travado_com_cache_de_antes_do_worker_done():
    from datetime import datetime, timedelta, timezone
    agora = datetime.now(timezone.utc)
    hb = (agora - timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for estado in ("rodando", "travado"):
        ab = _aberto_ag(estado, ultimo_heartbeat=hb)
        assert orq_mod.linha_vivos([{"tipo": "liberar", "dispatch": "ctx_1", "fechado": True}], ab, agora) == "", estado
        assert not [a for a in orq_mod.reavalia(ab["agentes"], [{"tipo": "liberar", "dispatch": "ctx_1", "fechado": True}], agora) if a["estado"] == "travado"]
    assert "TRAVADO" in orq_mod.linha_vivos([], _aberto_ag("rodando", ultimo_heartbeat=hb), agora)


def test_review7_b39_alerta_some_quando_o_agente_da_task_esta_liberado_no_cache():
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)
    al = {"tipo": "alerta", "alerta": "scout_sem_relatorio", "task": "task_1", "titulo": "[scout] x", "ts": now_iso(-60)}
    assert len(orq_mod.alertas_recentes([al], agora, [{"task": "task_1", "estado": "entregue"}])) == 1
    assert orq_mod.alertas_recentes([al], agora, [{"task": "task_1", "estado": "liberado"}]) == []


def test_review7_b40_liberar_refaz_o_cache_do_resumo_em_segundo_plano():
    a = Amb(run="run_a", ORQ_NO_BG="")  # o Amb desliga o segundo plano; aqui ele liga
    _lib_env(a, release="retained")
    cache = os.path.join(a.home, "aberto.json")
    assert not os.path.exists(cache)
    assert json.loads(a.orq("liberar", "ctx_term_w1").stdout)["fechado"] is True
    for _ in range(100):
        if os.path.exists(cache):
            break
        time.sleep(0.1)
    assert os.path.exists(cache), "o liberar tem de chamar refresh_bg (B40)"


# ---------- agent manager em terminal próprio (seção 25, ticket 17) ----------

def _away_ligado(home):
    """Liga o modo ausente no cursor.json: sem ele o orq não digita aviso no coordenador (ticket 107)."""
    caminho = os.path.join(home, "cursor.json")
    cur = json.load(open(caminho)) if os.path.exists(caminho) else {}
    json.dump({**cur, "ausente": {"ligada_em": "2026-10-01T12:00:00Z"}}, open(caminho, "w"))


def _gerente(a, run="run_a"):
    """O Run ligado ao terminal do agent manager (term_ger), e o coordenador (term_coord) apontando para ele no gerente.json."""
    a.set("run.json", {"id": run, "handle": "term_ger"})
    os.makedirs(a.home, exist_ok=True)
    json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": [run]}, open(os.path.join(a.home, "gerente.json"), "w"))
    _away_ligado(a.home)


def test_gerente_ligado_despachar_e_agentes_falam_com_o_orca_pelo_handle_do_gerente():
    a = Amb(run="run_a")
    a.set("run.json", {"id": "run_a", "handle": "term_ger"})
    assert _despachar(a).returncode == 1, "sem o gerente.json o coordenador não está ligado ao Run: o worker-start é recusado"
    _gerente(a)
    r = _despachar(a)
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["run"] == "run_a"
    r = a.orq("agentes", "--json")
    assert r.returncode == 0 and [x["dispatch"] for x in json.loads(r.stdout)] == ["ctx_term_novo1"], r


def test_gerente_ligado_o_hook_de_prompt_ve_o_coordenador_e_injeta_o_estado():
    a = Amb(run="run_a")
    _gerente(a)
    r = a.prompt("oi")
    assert r.returncode == 0 and "additionalContext" in r.stdout and "binding perdido" not in r.stdout, r


def test_gerente_de_outro_coordenador_nao_muda_nada():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_outro")
    _gerente(a)
    assert _despachar(a).returncode == 1, "o gerente.json só vale para o coordenador que o ligou"


def test_gerente_ligar_faz_run_use_pelo_gerente_e_desligar_devolve_ao_coordenador():
    a = Amb(run="run_a")
    a.set("terminals.json", ["term_ger", "term_coord"])
    r = a.orq("gerente", "ligar", "--terminal", "term_ger")
    assert r.returncode == 0, r.stderr
    assert json.load(open(os.path.join(a.fake, "run.json"))) == {"id": "run_a", "handle": "term_ger"}
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ger", "runs": ["run_a"]}
    assert _despachar(a).returncode == 0, "depois de ligar, o coordenador despacha pelo gerente"
    r = a.orq("gerente", "desligar")
    assert r.returncode == 0, r.stderr
    assert json.load(open(os.path.join(a.fake, "run.json"))) == {"id": "run_a", "handle": "term_coord"}
    assert not os.path.exists(os.path.join(a.home, "gerente.json"))
    assert [e["op"] for e in a.events() if e["tipo"] == "gerente"] == ["ligar", "desligar"]


def test_gerente_religar_depois_de_run_create_leva_o_run_novo_do_coordenador():
    a = Amb(run="run_a")
    _gerente(a)
    a.set("terminals.json", ["term_ger", "term_coord"])
    a.set("run.json", {"id": "run_novo", "handle": "term_coord"})  # run-create cru no coordenador: o Run novo fica ligado a ele
    r = a.orq("gerente", "ligar", "--terminal", "term_ger")
    assert r.returncode == 0 and json.loads(r.stdout)["run"] == "run_novo", r
    assert json.load(open(os.path.join(a.fake, "run.json"))) == {"id": "run_novo", "handle": "term_ger"}


def test_gerente_desligar_com_o_gerente_fora_do_ar_usa_o_run_gravado():
    a = Amb(run="run_a")
    a.set("terminals.json", ["term_ger", "term_coord"])
    assert a.orq("gerente", "ligar", "--terminal", "term_ger").returncode == 0
    r = a.orq("gerente", "desligar", FAKE_FAIL="run-current")
    assert r.returncode == 0, r.stderr
    assert json.load(open(os.path.join(a.fake, "run.json"))) == {"id": "run_a", "handle": "term_coord"}


def test_gerente_ligar_recusa_terminal_morto_o_proprio_e_sem_run():
    a = Amb(run="run_a")
    a.set("terminals.json", ["term_coord"])
    r = a.orq("gerente", "ligar", "--terminal", "term_ger")
    assert r.returncode == 1 and "term_ger" in r.stderr, r
    r = a.orq("gerente", "ligar", "--terminal", "term_coord")
    assert r.returncode == 1 and "próprio" in r.stderr, r
    b = Amb(run=None)
    b.set("terminals.json", ["term_ger"])
    r = b.orq("gerente", "ligar", "--terminal", "term_ger")
    assert r.returncode == 1 and "--run" in r.stderr, r
    for x in (a, b):
        assert not os.path.exists(os.path.join(x.home, "gerente.json")) and not [c for c in _log(x, "calls.log") if c[0] == "run-use"]


def test_gerente_absorver_confirma_heartbeat_sem_avisar_o_coordenador():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
    _gerente(a)
    a.caixa(_hb("lendo"), _hb("testando"))
    r = a.orq("gerente", "absorver")
    assert r.returncode == 0, r.stderr
    assert set(a.estados().values()) == {"acked"}
    (ev,) = [e for e in a.events() if e["tipo"] == "heartbeat_absorvido"]
    assert [h["fase"] for h in ev["heartbeats"]] == ["lendo", "testando"]
    assert _log(a, "send.log") == [], "heartbeat não chega ao coordenador"


def test_gerente_absorver_avisa_o_coordenador_uma_vez_por_entrega_e_nao_confirma():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
    _gerente(a)
    a.caixa(_hb("lendo"), ("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    assert a.orq("gerente", "absorver").returncode == 0
    assert a.orq("gerente", "absorver").returncode == 0
    (env,) = _log(a, "send.log")
    assert env[env.index("--terminal") + 1] == "term_coord" and "--enter" in env
    texto = env[env.index("--text") + 1]
    assert orq_mod.origem(texto) == "orca" and orq_mod.AVISO_RUN.search(texto).group(1) == "run_a" and "--terminal term_ger" in texto, texto
    assert orq_mod.separa_aviso("meio texto " + texto) == ("meio texto", True), "colado no que o usuário digitava (B26)"
    assert a.estados() == {"msg_1": "acked", "msg_2": "out"}, "o worker_done fica em aberto para o check do coordenador"
    a.caixa(("question", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    assert _check_puro(a, "--ack", "delivery_2")["ok"]  # o coordenador processou o worker_done; a pergunta é a entrega seguinte
    assert a.orq("gerente", "absorver").returncode == 0
    assert len(_log(a, "send.log")) == 2, "entrega nova, aviso novo"


def test_gerente_aviso_repassado_passa_no_hook_do_coordenador():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
    _gerente(a)
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    a.orq("gerente", "absorver")
    (env,) = _log(a, "send.log")
    r = a.prompt(env[env.index("--text") + 1], ORCA_TERMINAL_HANDLE="term_coord")
    assert r.returncode == 0 and not r.stdout.strip(), "o aviso passa: o coordenador acorda para o worker_done"
    assert a.estados() == {"msg_1": "out"}


def test_gerente_absorver_fora_do_gerente_nao_faz_nada():
    a = Amb(run="run_a")  # term_coord, sem gerente.json: o painel sem gerente ligado
    a.caixa(_hb("lendo"))
    r = a.orq("gerente", "absorver")
    assert r.returncode == 0 and "desligado" in r.stdout, r
    assert a.estados() == {"msg_1": "unread"} and not _calls(a, "check")


def test_gerente_ligado_o_guard_ainda_ve_os_despachos_do_run_do_gerente():
    a = Amb(run="run_a")
    _gerente(a)
    a.set("runs.json", [{"id": "run_a", "coordinator_handle": "term_ger"}])
    a.set("terminals.json", ["term_ger", "term_coord"])
    _workers(a, ("w_ativo", "run_a", "dispatched"))
    out = json.loads(_guard(a).stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny", "o Run é coordenado pelo gerente deste coordenador, não por outro terminal"


def test_waiter_com_gerente_le_o_run_pelo_handle_do_gerente():
    a = Amb(run="run_a")
    _gerente(a)
    a.caixa(_hb("lendo"), ("worker_done", {"taskId": "t"}))
    out = json.loads(_waiter(a).stdout)
    assert out.get("run") == "run_a" and [m["type"] for m in out["messages"]] == ["worker_done"], out


# ---------- agent manager para vários Runs (seção 26, ticket 19) ----------
# O Orca liga um Run por terminal (ligar outro tira o terminal do anterior) e cancela a entrega de quem perde a ligação entre o check e o ack.

def _multi(a, ligados, runs=None):
    """Orca falso no modo vários Runs: ligados = {run: handle}. Com `runs`, o gerente.json do coordenador (term_coord) para term_ger."""
    a.set("binds.json", {r: {"handle": h, "gen": 1} for r, h in ligados.items()})
    a.set("terminals.json", ["term_ger", "term_coord"])
    if runs is not None:
        os.makedirs(a.home, exist_ok=True)
        json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": runs}, open(os.path.join(a.home, "gerente.json"), "w"))
        _away_ligado(a.home)


def _binds(a):
    return {r: v["handle"] for r, v in json.load(open(os.path.join(a.fake, "binds.json"))).items()}


def _gerente_runs(a):
    return json.load(open(os.path.join(a.home, "gerente.json")))["runs"]


def _check_run(a, run, *args):
    r = subprocess.run([a.bin, "orchestration", "check", "--run", run, *args, "--json"], capture_output=True, text=True, env=a.env)
    return json.loads(r.stdout)


def test_gerente_ligar_aceita_varios_runs_e_soma_ao_gerente_ja_ligado():
    a = Amb()
    _multi(a, {"run_a": "term_coord", "run_b": None, "run_c": None})
    r = a.orq("gerente", "ligar", "--terminal", "term_ger", "--run", "run_a", "--run", "run_b")
    assert r.returncode == 0, r.stderr
    assert _gerente_runs(a) == ["run_a", "run_b"]
    assert _binds(a) == {"run_a": None, "run_b": "term_ger", "run_c": None}, "um Run por terminal: só o último fica ligado, e o coordenador não fica com nenhum"
    assert json.loads(r.stdout)["runs"] == ["run_a", "run_b"]
    a.set("binds.json", {"run_a": {"handle": None, "gen": 3}, "run_b": {"handle": "term_ger", "gen": 3}, "run_c": {"handle": "term_coord", "gen": 1}})  # run-create cru
    r = a.orq("gerente", "ligar", "--terminal", "term_ger")
    assert r.returncode == 0, r.stderr
    assert _gerente_runs(a) == ["run_a", "run_b", "run_c"] and _binds(a)["run_c"] == "term_ger"
    r = a.orq("gerente", "ligar", "--terminal", "term_ger", "--run", "run_a")
    assert r.returncode == 0 and _gerente_runs(a) == ["run_a", "run_b", "run_c"] and _binds(a)["run_a"] == "term_ger", "religar um Run já ligado não o duplica"


def _despachar_em(a, run, **env):
    return a.orq("despachar", "--run", run, "--titulo", "Ticket 05", "--spec-arquivo", _spec(a), "--modelo", "claude-sonnet-5-5", "--effort", "medium", **env)


def test_gerente_despachar_num_run_novo_liga_o_run_ao_gerente_e_o_coordenador_fica_sem_heartbeat():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_novo": "term_coord"}, ["run_a"])  # run_novo: run-create cru, ligado ao coordenador
    r = _despachar_em(a, "run_novo")
    assert r.returncode == 0, r.stderr
    assert _binds(a) == {"run_a": None, "run_novo": "term_ger"}, "nenhum Run ligado ao coordenador: o Orca não lhe manda aviso de heartbeat"
    assert _gerente_runs(a) == ["run_a", "run_novo"]
    assert len(_log(a, "started.log")) == 1


def test_gerente_despachar_em_run_do_gerente_que_nao_e_o_ligado_religa_antes_do_worker_start():
    a = Amb()
    _multi(a, {"run_a": None, "run_b": "term_ger"}, ["run_a", "run_b"])
    r = _despachar_em(a, "run_a")
    assert r.returncode == 0, r.stderr
    assert _binds(a) == {"run_a": "term_ger", "run_b": None}


def test_gerente_despachar_nao_toma_run_de_outro_terminal():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_x": "term_outro"}, ["run_a"])
    r = _despachar_em(a, "run_x")
    assert r.returncode == 1 and not _log(a, "started.log"), r
    assert _binds(a) == {"run_a": "term_ger", "run_x": "term_outro"} and _gerente_runs(a) == ["run_a"]


def test_gerente_despachar_segura_a_ligacao_enquanto_o_painel_roda():
    a = Amb()
    _multi(a, {"run_a": None, "run_b": "term_ger"}, ["run_a", "run_b"])
    with ThreadPoolExecutor(2) as ex:
        f = ex.submit(_despachar_em, a, "run_a", FAKE_SLEEP_CMD="worker-start:1.5")
        time.sleep(0.6)
        g = ex.submit(a.orq, "gerente", "absorver", ORCA_TERMINAL_HANDLE="term_ger")
        assert f.result().returncode == 0, "o painel não pode religar o gerente no meio do worker-start"
        assert g.result().returncode == 0
    assert len(_log(a, "started.log")) == 1


def test_gerente_absorver_percorre_todos_os_runs_e_avisa_do_worker_done_de_qualquer_um():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.caixa(_hb("lendo"), run="run_a")
    a.caixa(_hb("testando"), ("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}), run="run_b")
    r = a.orq("gerente", "absorver")
    assert r.returncode == 0, r.stderr
    linhas = r.stdout.strip().splitlines()
    assert len(linhas) == 2 and "run_a" in linhas[0] and "run_b" in linhas[1], r.stdout
    assert a.estados() == {"msg_1": "acked", "msg_2": "acked", "msg_3": "out"}
    assert sorted(e["run"] for e in a.events() if e["tipo"] == "heartbeat_absorvido") == ["run_a", "run_b"]
    (env,) = _log(a, "send.log")
    texto = env[env.index("--text") + 1]
    assert env[env.index("--terminal") + 1] == "term_coord" and orq_mod.AVISO_RUN.search(texto).group(1) == "run_b" and "--terminal term_ger" in texto, texto
    assert _binds(a)["run_b"] == "term_ger", "termina ligado ao Run do aviso: o check cru do coordenador só vale com o gerente nele"


def test_gerente_absorver_fica_no_run_do_aviso_ate_o_coordenador_confirmar():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}), run="run_b")
    assert a.orq("gerente", "absorver").returncode == 0
    a.caixa(_hb("depois"), run="run_a")  # chega em outro Run enquanto o coordenador não leu
    r = a.orq("gerente", "absorver")
    assert "esperando o coordenador" in r.stdout and "run_a" not in r.stdout, r.stdout
    assert a.estados()["msg_2"] == "unread" and _binds(a)["run_b"] == "term_ger", "não sai do Run: a entrega do coordenador cairia (consumer_fenced)"
    assert len(_log(a, "send.log")) == 1, "um aviso por entrega"
    d = _check_run(a, "run_b")["result"]["deliveryId"]
    assert _check_run(a, "run_b", "--ack", d)["ok"], "a entrega segue de pé"
    r = a.orq("gerente", "absorver")
    assert a.estados()["msg_2"] == "acked" and "run_a" in r.stdout, r.stdout


def test_gerente_absorver_solta_o_run_do_aviso_depois_do_prazo():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}), run="run_b")
    a.orq("gerente", "absorver")
    a.caixa(_hb("depois"), run="run_a")
    a.orq("gerente", "absorver", ORQ_GERENTE_PRESO_S="0")  # o coordenador não veio: o painel volta a percorrer todos
    assert a.estados()["msg_2"] == "acked" and len(_log(a, "send.log")) == 1, "a entrega em aberto não é avisada de novo"


def test_gerente_desligar_devolve_um_run_ou_todos():
    a = Amb()
    _multi(a, {"run_a": None, "run_b": "term_ger", "run_c": None}, ["run_a", "run_b", "run_c"])
    r = a.orq("gerente", "desligar", "--run", "run_a")
    assert r.returncode == 0, r.stderr
    assert _gerente_runs(a) == ["run_b", "run_c"] and _binds(a)["run_a"] == "term_coord"
    r = a.orq("gerente", "desligar")
    assert r.returncode == 0, r.stderr
    assert not os.path.exists(os.path.join(a.home, "gerente.json"))
    assert _binds(a) == {"run_a": None, "run_b": "term_coord", "run_c": None}, "o coordenador segura um Run só: o que o gerente tinha ligado"
    assert [e["op"] for e in a.events() if e["tipo"] == "gerente"] == ["desligar", "desligar"]


def test_gerente_desligar_run_que_nao_e_do_gerente_recusa():
    a = Amb()
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    r = a.orq("gerente", "desligar", "--run", "run_z")
    assert r.returncode == 1 and "run_z" in r.stderr and _gerente_runs(a) == ["run_a"], r


def test_gerente_liberar_em_run_que_nao_e_o_ligado_religa_e_confirma_dentro_da_mesma_ligacao():
    a = Amb()
    _lib_env(a)
    _multi(a, {"run_a": None, "run_b": "term_ger"}, ["run_a", "run_b"])
    r = a.orq("liberar", "ctx_term_w1")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["estado"] == "released" and out["ack"] == ["delivery_1"] and not out["aviso"], out
    assert a.estados() == {"msg_1": "acked"}


def test_gerente_waiter_le_a_caixa_de_todos_os_runs_do_gerente():
    a = Amb()
    _multi(a, {"run_a": None, "run_b": "term_ger"}, ["run_a", "run_b"])
    a.caixa(_hb("lendo"), ("worker_done", {"taskId": "t"}), run="run_a")
    out = json.loads(_waiter(a, "run_a", "run_b").stdout)
    assert out.get("run") == "run_a" and [m["type"] for m in out["messages"]] == ["worker_done"], out
    assert a.estados()["msg_1"] == "acked"


def test_gerente_aviso_de_run_que_nao_e_o_ligado_passa_no_hook_do_coordenador():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}), run="run_b")
    a.orq("gerente", "absorver")
    (env,) = _log(a, "send.log")
    texto = env[env.index("--text") + 1]
    a.set("binds.json", {"run_a": {"handle": "term_ger", "gen": 9}, "run_b": {"handle": None, "gen": 9}})  # o painel já andou para run_a
    r = a.prompt(texto, ORCA_TERMINAL_HANDLE="term_coord")
    assert r.returncode == 0 and not r.stdout.strip(), "o aviso passa: o coordenador acorda para o worker_done"
    assert _binds(a)["run_b"] == "term_ger", "o hook ligou o gerente ao Run do aviso antes de ler"


def test_waiter_acorda_com_question_e_escalation_de_run_que_o_coordenador_nao_le():
    a = Amb(run="run_a")  # sem gerente: só o mailbox do Run ligado é legível; os outros Runs aparecem no inbox
    esc = {**_pergunta_ask(51, "ctx_9", "run_b"), "id": "msg_e51", "type": "escalation", "subject": "Blocked: sem acesso", "thread_id": None}
    _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -5, 900), _pergunta_ask(50, "ctx_9", "run_b"), esc)
    out = json.loads(_waiter(a, "run_a", "run_b").stdout)
    assert out.get("run") == "run_b" and sorted(m["type"] for m in out["messages"]) == ["escalation", "question"], out
    assert not [c for c in _log(a, "calls.log") if c[0] == "check" and "run_b" in c], "nada é consumido: o Run continua desligado do coordenador"


def test_waiter_nao_acorda_com_so_heartbeat_de_outro_run():
    a = Amb(run="run_a")
    _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -5, 900))
    assert json.loads(_waiter(a, "run_a", "run_b").stdout) == {"timeout": True}


def test_gerente_waiter_acorda_com_escalation_de_run_do_gerente_que_nao_e_o_ligado():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.caixa(_hb("lendo"), ("escalation", {"taskId": "t", "dispatchId": "ctx_9"}), run="run_b")
    out = json.loads(_waiter(a, "run_a", "run_b").stdout)
    assert out.get("run") == "run_b" and [m["type"] for m in out["messages"]] == ["escalation"], out


# ---------- ticket 20: aviso do painel repetido e sem Enter; steer que não chega a worker ocioso ----------

def _avisos_enviados(a, handle="term_coord"):
    """Os send.log de `orca terminal send` para o terminal, só os que digitam texto."""
    return [e for e in _log(a, "send.log") if "--text" in e and e[e.index("--terminal") + 1] == handle]


def _dois_runs_com_worker_done(a):
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}), run="run_a")
    a.caixa(("worker_done", {"taskId": "task_2", "dispatchId": "ctx_2"}), run="run_b")


def test_ticket20_aviso_de_um_run_nao_se_repete_quando_o_send_de_outro_run_falha():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _dois_runs_com_worker_done(a)
    a.orq("gerente", "absorver", FAKE_FAIL_SEND_DEPOIS="1")  # o aviso de run_a foi digitado; o de run_b, recusado
    assert len(_avisos_enviados(a)) == 2, "run_a digitado, run_b tentado e recusado"
    a.orq("gerente", "absorver")  # preso em run_a até o coordenador confirmar
    d = _check_run(a, "run_a")["result"]["deliveryId"]
    assert _check_run(a, "run_a", "--ack", d)["ok"]
    for _ in range(2):
        a.orq("gerente", "absorver")
    textos = [e[e.index("--text") + 1] for e in _avisos_enviados(a)]  # inclui a tentativa recusada de run_b
    assert [("run_a" in t, "run_b" in t) for t in textos] == [(True, False), (False, True), (False, True)], f"run_a uma vez só: {textos}"


def test_ticket20_entrega_ja_confirmada_que_volta_a_aparecer_nao_e_avisada_de_novo():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    a.orq("gerente", "absorver")
    assert _check_puro(a, "--ack", "delivery_1")["ok"], "o coordenador leu e confirmou"
    a.orq("gerente", "absorver")  # caixa vazia
    p = os.path.join(a.fake, "mailbox.json")
    cx = json.load(open(p))
    cx["msgs"][0]["status"] = "unread"  # o Orca devolve o mesmo id (ack perdido na troca de geração)
    json.dump(cx, open(p, "w"))
    a.orq("gerente", "absorver")
    assert len(_avisos_enviados(a)) == 1, "o coordenador já viu essa mensagem"


def test_ticket20_coordenador_no_meio_do_turno_nao_recebe_texto_e_o_aviso_sai_uma_vez_quando_ele_fica_ocioso():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    a.set("busy.json", ["term_coord"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    for _ in range(3):
        r = a.orq("gerente", "absorver")
        assert r.returncode == 0 and "ficar livre" in r.stdout, r.stdout
    assert _avisos_enviados(a) == [], "texto digitado no meio do turno fica parado na caixa: nada é digitado"
    a.set("busy.json", [])
    r = a.orq("gerente", "absorver")
    assert "avisado ao coordenador" in r.stdout, r.stdout
    a.orq("gerente", "absorver")
    (env,) = _avisos_enviados(a)
    assert "--enter" in env, env
    calls = [c[0] for c in _log(a, "calls.log") if c[0] in ("wait", "send")]
    assert calls[-2:] == ["wait", "send"] or "wait" in calls, "espera o tui-idle antes de digitar"


def test_ticket20_rascunho_do_usuario_na_caixa_do_coordenador_segura_o_aviso():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    a.set("drafts.json", {"term_coord": "estou digitando"})
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    a.orq("gerente", "absorver")
    assert _avisos_enviados(a) == []
    a.set("drafts.json", {})
    a.orq("gerente", "absorver")
    assert len(_avisos_enviados(a)) == 1


def test_ticket20_enter_que_nao_submeteu_recebe_um_enter_sozinho_e_nenhum_texto_novo():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    a.orq("gerente", "absorver", FAKE_ENTER_PERDIDO="1")
    a.orq("gerente", "absorver", FAKE_ENTER_PERDIDO="1")
    envios = _log(a, "send.log")
    assert len(envios) == 2 and "--text" in envios[0] and "--text" not in envios[1] and "--enter" in envios[1], envios


def test_ticket20_orca_sem_observacao_de_submissao_nao_ganha_enter_extra():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    a.orq("gerente", "absorver", FAKE_ENTER_PERDIDO="1", FAKE_SEM_OBSERVACAO="1")
    assert len(_log(a, "send.log")) == 1


def _steer_com_worker(a, **env):
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_rodando", "dispatch": "ctx_1", "status": "dispatched"}])
    a.set("terminals.json", ["term_w1", "term_coord"])
    _steer_env(a)
    return a.orq("steer", "task_rodando", "use o índice novo", **env)


def test_ticket20_steer_a_worker_ocioso_digita_o_aviso_no_terminal_dele_uma_vez():
    a = Amb()
    r = _steer_com_worker(a)
    assert r.returncode == 0, r.stderr
    (env,) = _avisos_enviados(a, "term_w1")
    texto = env[env.index("--text") + 1]
    assert "--enter" in env and texto.startswith("You have 1 orchestration message.") and "check --terminal term_w1" in texto, env
    assert len(_enviados(a)) == 1, "a mensagem do steer continua indo pela caixa do dispatch"
    (ev,) = [e for e in a.events() if e["tipo"] == "steer"]
    assert ev["aviso_terminal"] == "enviado", ev


def test_ticket20_steer_a_worker_ocupado_nao_digita_nada_no_terminal():
    a = Amb()
    a.set("busy.json", ["term_w1"])
    r = _steer_com_worker(a)
    assert r.returncode == 0, r.stderr
    assert _log(a, "send.log") == [] and len(_enviados(a)) == 1, "o Orca avisa o worker ocupado sozinho: sem duplicar"


def _steer_ocupado74(tela, draft=None, **env):
    a = Amb()
    a.set("busy.json", ["term_w1"])
    a.set("screens.json", {"term_w1": tela})
    if draft:
        a.set("drafts.json", {"term_w1": draft})
    return a, _steer_com_worker(a, **env)


def test_ticket74_steer_a_worker_no_meio_do_turno_digita_o_aviso_com_o_resumo_e_registra():
    a, r = _steer_ocupado74(["● Lendo os arquivos", "✻ Pensando… (12s · esc to interrupt)"])
    assert r.returncode == 0, r.stderr
    (env,) = _avisos_enviados(a, "term_w1")
    texto = env[env.index("--text") + 1]
    assert "--enter" in env and "use o índice novo" in texto and "orca orchestration check --terminal term_w1" in texto, env
    assert [e["tipo"] for e in a.events()].count("steer_digitado_ocupado") == 1
    (ev,) = [e for e in a.events() if e["tipo"] == "steer"]
    assert ev["aviso_terminal"] == "ocupado_digitado", ev


def test_ticket74_o_resumo_do_aviso_tem_ate_300_caracteres_numa_linha_so():
    a = Amb()
    a.set("busy.json", ["term_w1"])
    a.set("screens.json", {"term_w1": ["esc to interrupt"]})
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_rodando", "dispatch": "ctx_1", "status": "dispatched"}])
    a.set("terminals.json", ["term_w1", "term_coord"])
    _steer_env(a)
    assert a.orq("steer", "task_rodando", "ajuste\n" + "x" * 900).returncode == 0
    (env,) = _avisos_enviados(a, "term_w1")
    texto = env[env.index("--text") + 1]
    assert "\n" not in texto and "x" * 250 in texto and "x" * 301 not in texto, texto


def test_ticket74_steer_a_worker_ocupado_nao_digita_por_cima_de_rascunho_nem_de_menu():
    a, r = _steer_ocupado74(["esc to interrupt"], draft="estou digitando")
    assert r.returncode == 0 and _log(a, "send.log") == [], "rascunho do usuário"
    b, r = _steer_ocupado74(_tela52("tela-permissao.txt"))
    assert r.returncode == 0 and _log(b, "send.log") == [], "menu de permissão"
    c, r = _steer_ocupado74(["● sem spinner na tela"])
    assert r.returncode == 0 and _log(c, "send.log") == [], "sem turno em andamento na tela"
    for x in (a, b, c):
        assert "steer_digitado_ocupado" not in [e["tipo"] for e in x.events()]
        assert len(_enviados(x)) == 1, "a mensagem do steer segue pela caixa"


def test_ticket74_orca_que_barra_o_texto_no_meio_do_turno_deixa_o_steer_como_estava():
    a, r = _steer_ocupado74(["esc to interrupt"], FAKE_PROMPT_BLOCKED="1")
    assert r.returncode == 0 and "steer_digitado_ocupado" not in [e["tipo"] for e in a.events()]
    assert "aviso_terminal" not in [e for e in a.events() if e["tipo"] == "steer"][0]


def test_ticket20_steer_sem_terminal_do_worker_ou_com_falha_no_terminal_nao_derruba_o_steer():
    a = Amb()
    _steer_env(a)  # worker-list vazio: o dispatch não aparece
    assert a.orq("steer", "task_rodando", "oi").returncode == 0 and _log(a, "send.log") == []
    b = Amb()
    r = _steer_com_worker(b, FAKE_FAIL_TERMINAL="send")
    assert r.returncode == 0, r.stderr
    assert [e["aviso_terminal"] for e in b.events() if e["tipo"] == "steer"] == ["falhou"] and len(_enviados(b)) == 1



def test_ticket20_agent_prompt_blocked_do_orca_conta_como_ocupado_no_painel_e_no_steer():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}))
    r = a.orq("gerente", "absorver", FAKE_PROMPT_BLOCKED="1")
    assert "ficar livre" in r.stdout and not os.path.exists(os.path.join(a.home, "gerente-aviso.json")), r.stdout
    assert "avisado ao coordenador" in a.orq("gerente", "absorver").stdout, "passou o turno: o aviso sai"
    b = Amb()
    assert _steer_com_worker(b, FAKE_PROMPT_BLOCKED="1").returncode == 0
    assert "aviso_terminal" not in [e for e in b.events() if e["tipo"] == "steer"][0], "worker ocupado: o Orca avisa sozinho"


# ---------- fim de vida dos Runs (ticket 23) ----------

VELHO = "2026-09-01 10:00:00"


def _run_parado(a, run="run_b", **extra):
    """Run sem task aberta, criado e concluído em setembro: parado há muito mais que RUN_PARADO_MIN."""
    a.set("tasks_%s.json" % run, [{"id": "task_x", "status": "completed", "created_at": VELHO, "completed_at": "2026-09-01T10:05:00Z"}])
    return {"id": run, "objective": "Frente antiga", "created_at": VELHO, **extra}


def _run_ativo(a, run="run_a"):
    a.set("tasks_%s.json" % run, [{"id": "task_y", "status": "dispatched", "created_at": VELHO}])
    return {"id": run, "objective": "Frente viva", "created_at": VELHO}


def test_gerente_absorver_solta_run_parado_sem_task_aberta_e_sem_mensagem():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.set("runs.json", [_run_ativo(a), _run_parado(a)])
    r = a.orq("gerente", "absorver")
    assert r.returncode == 0, r.stderr
    assert _gerente_runs(a) == ["run_a"], "run_b não tem task aberta nem mensagem há mais de RUN_PARADO_MIN"
    (ev,) = [e for e in a.events() if e["tipo"] == "gerente" and e["op"] == "soltar"]
    assert ev["run"] == "run_b" and "sem task aberta" in ev["motivo"], ev


def test_gerente_absorver_nao_solta_run_recente_com_mensagem_ou_o_ultimo():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": None, "run_c": None}, ["run_a", "run_b", "run_c"])
    agora = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    a.set("runs.json", [_run_ativo(a), {**_run_parado(a), "created_at": agora}, _run_parado(a, "run_c")])
    a.set("tasks_run_b.json", [])  # criado agora, sem task: ainda dentro do prazo
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}), run="run_c")  # parado, mas com mensagem esperando
    assert a.orq("gerente", "absorver").returncode == 0
    assert _gerente_runs(a) == ["run_a", "run_b", "run_c"]
    b = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(b, {"run_b": "term_ger"}, ["run_b"])
    b.set("runs.json", [_run_parado(b)])
    assert b.orq("gerente", "absorver").returncode == 0 and _gerente_runs(b) == ["run_b"], "o gerente nunca fica sem Run"


def test_despachar_religa_run_que_o_gerente_soltou():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": "term_ger"}, ["run_a", "run_b"])
    a.set("runs.json", [_run_ativo(a), _run_parado(a)])
    a.orq("gerente", "absorver")
    assert _gerente_runs(a) == ["run_a"]
    a.env["ORCA_TERMINAL_HANDLE"] = "term_coord"
    assert _despachar_em(a, "run_b").returncode == 0
    assert _gerente_runs(a) == ["run_a", "run_b"]


def _runs_fixture(a):
    a.set("runs.json", [{**_run_ativo(a), "objective": "Frente viva"}, _run_parado(a, "run_b"),
                        {**_run_parado(a, "run_t"), "objective": "teste ticket 20 (apagar)"},
                        {**_run_parado(a, "run_n"), "objective": "Frente nova", "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())}])
    a.set("tasks_run_n.json", [{"id": "task_n", "status": "completed", "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())}])


def test_orq_runs_lista_abertas_concluidas_gerente_e_esconde_arquivo_e_teste():
    a = Amb()
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    _runs_fixture(a)
    r = a.orq("runs", "--json")
    assert r.returncode == 0, r.stderr
    por = {x["id"]: x for x in json.loads(r.stdout)}
    assert sorted(por) == ["run_a", "run_n"], "run_b (concluído em setembro) vai para o arquivo e o Run de teste nunca aparece"
    assert por["run_a"]["abertas"] == 1 and por["run_a"]["concluidas"] == 0 and por["run_a"]["gerente"] is True
    assert por["run_n"]["abertas"] == 0 and por["run_n"]["concluidas"] == 1 and por["run_n"]["gerente"] is False and por["run_n"]["ultima"]
    todos = {x["id"] for x in json.loads(a.orq("runs", "--todos", "--json").stdout)}
    assert todos == {"run_a", "run_b", "run_t", "run_n"}
    txt = a.orq("runs").stdout
    assert "Frente viva" in txt and "Frente antiga" not in txt


def test_resumo_e_aberto_mostram_so_runs_com_trabalho_aberto_ou_recentes():
    a = Amb()
    _runs_fixture(a)
    assert a.orq("ingest", "--refresh").returncode == 0
    ab = json.load(open(os.path.join(a.home, "aberto.json")))
    assert sorted(x["id"] for x in ab["runs"]) == ["run_a", "run_n"]
    st = a.orq("status").stdout
    assert "Frente viva" in st and "Frente nova" in st and "Frente antiga" not in st and "teste ticket" not in st, st


# ---------- ticket 24: estado ocioso do worker pelos hooks do próprio worker ----------

PREAMBULO_24 = ("Please carry out this task from my Orca coordinator by following the brief I pasted below. You are working inside Orca, a multi-agent IDE.\n"
                "Your coordinator's terminal handle is: term_c\nYour task ID is: task_t24\n"
                "orca orchestration send --from term_w --type worker_done --task-id task_t24 --dispatch-id ctx_d24 --outcome succeeded\n")


def _turnos(a):
    return json.load(open(os.path.join(a.home, "turnos.json")))


def _hook(a, kind, **ev):
    r = a.orq("hook", kind, stdin=json.dumps({"session_id": "abcdef123456", **ev}))
    assert r.returncode == 0, r.stderr
    return r


def _chamadas_ao_orca(a):
    p = os.path.join(a.fake, "calls.log")
    return len(open(p).read().splitlines()) if os.path.exists(p) else 0


def test_ticket24_hooks_do_worker_gravam_inicio_e_fim_do_turno_sem_chamar_o_orca():
    a = Amb(run=None)
    _hook(a, "prompt", prompt=PREAMBULO_24)
    t = _turnos(a)["ctx_d24"]
    assert t["task"] == "task_t24" and t["inicio"] and t["fim"] is None, t
    _hook(a, "stop")
    fim = _turnos(a)["ctx_d24"]["fim"]
    assert fim and fim >= t["inicio"]
    _hook(a, "prompt", prompt="ajuste: use o outro arquivo")  # steer: novo turno do mesmo dispatch, lido da sessão
    t2 = _turnos(a)["ctx_d24"]
    assert t2["fim"] is None and t2["inicio"] >= fim and list(_turnos(a)) == ["ctx_d24"], t2
    assert _chamadas_ao_orca(a) == 0, "o hook do worker não chama o Orca"
    assert a.events() == [], "o turno não vira evento do log"


def test_ticket24_sessao_sem_papel_de_worker_nao_grava_turno():
    a = Amb(run=None)
    _hook(a, "stop")
    _hook(a, "prompt", prompt="oi")
    assert not os.path.exists(os.path.join(a.home, "turnos.json"))
    a2 = Amb(run="run_a")  # coordenador: também não
    _hook(a2, "prompt", prompt="oi")
    _hook(a2, "stop")
    assert not os.path.exists(os.path.join(a2.home, "turnos.json"))


def test_ticket24_novo_preambulo_no_mesmo_terminal_abre_outro_dispatch():
    a = Amb(run=None)
    _hook(a, "prompt", prompt=PREAMBULO_24)
    _hook(a, "stop")
    _hook(a, "prompt", prompt=PREAMBULO_24.replace("ctx_d24", "ctx_d25").replace("task_t24", "task_t25"))
    t = _turnos(a)
    assert set(t) == {"ctx_d24", "ctx_d25"} and t["ctx_d24"]["fim"] and t["ctx_d25"]["fim"] is None and t["ctx_d25"]["task"] == "task_t25"
    _hook(a, "stop")
    assert _turnos(a)["ctx_d25"]["fim"], "o Stop fecha o dispatch mais novo da sessão"


def test_ticket24_hook_do_worker_fica_abaixo_de_100_ms():
    a = Amb(run=None)
    _hook(a, "prompt", prompt=PREAMBULO_24)
    tempos = []
    for _ in range(5):
        t0 = time.perf_counter()
        _hook(a, "stop")
        tempos.append(time.perf_counter() - t0)
    assert min(tempos) < 0.1, tempos


def _agentes_24(a, turnos):
    a.set("workers.json", [{"handle": "term_n", "run": "run_a", "status": "dispatched", "desde": _iso(-180), "agente": "claude"},
                           {"handle": "term_p", "run": "run_a", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"},
                           {"handle": "term_t", "run": "run_a", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"},
                           {"handle": "term_r", "run": "run_a", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"},
                           {"handle": "term_j", "run": "run_a", "status": "dispatched", "desde": _iso(-60), "agente": "claude"},
                           {"handle": "term_x", "run": "run_a", "status": "dispatched", "desde": _iso(-300), "agente": "cursor"},
                           {"handle": "term_s", "run": "run_a", "status": "dispatched", "desde": _iso(-300)}])
    os.makedirs(a.home, exist_ok=True)
    json.dump(turnos, open(os.path.join(a.home, "turnos.json"), "w"))
    _inbox(a, _hbi(30, "ctx_term_t", "compilando", -1200), _hbi(20, "ctx_term_r", "testando", -30))


def test_ticket24_agentes_distingue_nao_comecou_parado_e_travado():
    a = Amb(run="run_a")
    _agentes_24(a, {"ctx_term_p": {"task": "task_term_p", "inicio": now_iso(-600), "fim": now_iso(-300)},
                    "ctx_term_t": {"task": "task_term_t", "inicio": now_iso(-2000), "fim": None},
                    "ctx_term_r": {"task": "task_term_r", "inicio": now_iso(-100), "fim": None}})
    ag = _agentes(a)
    assert {d: x["estado"] for d, x in ag.items()} == {"ctx_term_n": "nao_comecou", "ctx_term_p": "parado", "ctx_term_t": "travado", "ctx_term_r": "rodando",
                                                        "ctx_term_j": "rodando", "ctx_term_x": "rodando", "ctx_term_s": "rodando"}, ag
    assert 150 <= ag["ctx_term_n"]["idade_s"] <= 260 and 250 <= ag["ctx_term_p"]["idade_s"] <= 400, "não começou conta desde o despacho; parado, desde o fim do turno"
    assert [x["estado"] for x in _agentes(a).values()][:3] == ["travado", "nao_comecou", "parado"], "a ordem põe o que pede ação primeiro"
    assert ag["ctx_term_j"]["turno"] == "unknown", "dentro da janela de 2 min não dá para dizer que não começou"


def test_ticket24_worker_sem_hook_do_orq_ou_sem_agente_conhecido_fica_unknown_nunca_ocioso():
    a = Amb(run="run_a")
    _agentes_24(a, {})
    ag = _agentes(a)
    for d in ("ctx_term_x", "ctx_term_s"):  # cursor não tem o hook do orq; worker-show sem agente não prova nada
        assert ag[d]["turno"] == "unknown" and ag[d]["estado"] == "rodando", ag[d]
    assert ag["ctx_term_n"]["estado"] == "nao_comecou"


def test_ticket24_heartbeat_depois_do_fim_do_turno_vale_como_rodando():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_p", "run": "run_a", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"}])
    os.makedirs(a.home, exist_ok=True)
    json.dump({"ctx_term_p": {"task": "task_term_p", "inicio": now_iso(-600), "fim": now_iso(-300)}}, open(os.path.join(a.home, "turnos.json"), "w"))
    _inbox(a, _hbi(30, "ctx_term_p", "voltei", -100))
    assert _agentes(a)["ctx_term_p"]["estado"] == "rodando"


def test_ticket24_dispatch_com_heartbeat_e_sem_registro_de_turno_nao_e_nao_comecou():
    a = Amb(run="run_a")  # o worker que já rodava quando os hooks entraram
    a.set("workers.json", [{"handle": "term_v", "run": "run_a", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"}])
    _inbox(a, _hbi(30, "ctx_term_v", "fase-1", -100))
    v = _agentes(a)["ctx_term_v"]
    assert v["estado"] == "rodando" and v["turno"] == "unknown", v


def test_ticket24_texto_de_agentes_diz_nao_comecou_e_parado_no_prompt_com_o_steer():
    a = Amb(run="run_a")
    _agentes_24(a, {"ctx_term_p": {"task": "task_term_p", "inicio": now_iso(-600), "fim": now_iso(-300)}})
    r = a.orq("agentes")
    assert r.returncode == 0, r.stderr
    assert "não começou" in r.stdout and "parado no prompt há 5 min" in r.stdout, r.stdout
    assert 'orq steer task_term_p "' in r.stdout and 'orq steer task_term_n "' in r.stdout, r.stdout


def test_ticket24_resumo_e_aberto_carregam_o_nao_comecou_e_o_parado():
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)
    ab = _aberto_ag("parado", agente="claude", desde=now_iso(-3000), turno_inicio=now_iso(-900), turno_fim=now_iso(-600), idade_s=600)
    ab["agentes"].append({**ab["agentes"][0], "dispatch": "ctx_2", "task": "task_bbbbbbbbbb", "estado": "nao_comecou", "turno_inicio": None, "turno_fim": None, "desde": now_iso(-400)})
    txt = orq_mod.resumo([], ab, {"itens": []}, agora=agora)
    assert "Parado no prompt: task_aaaaaaaaaa" in txt and "há 10 min" in txt and "Não começou: task_bbbbbbbbbb" in txt and 'orq steer task_aaaaaaaaaa "' in txt, txt
    assert len(txt.splitlines()) <= 5
    vivo = orq_mod.resumo([], ab, {"itens": []}, agora=agora, turnos={"ctx_1": {"inicio": now_iso(-5), "fim": None}})
    assert "Parado no prompt" not in vivo, "o turnos.json mais novo que o cache tira o parado"


def test_ticket24_constantes_com_nome():
    assert (orq_mod.NAO_COMECOU_S, orq_mod.PARADO_S) == (120, 60)


def _espera_25(fase, hb_min, **k):
    """Agente do cache com a fase e o último heartbeat de `hb_min` minutos atrás, reavaliado agora."""
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)
    ab = _aberto_ag("rodando", fase=fase, ultimo_heartbeat=now_iso(-hb_min * 60), desde=now_iso(-7200), **k)
    return orq_mod.reavalia(ab["agentes"], [], agora)[0], ab, agora


def _hhmm_25(min_):
    return time.strftime("%H:%M", time.localtime(time.time() + min_ * 60))


def test_ticket25_esperando_ha_30_min_nao_e_travado():
    ag, _, _ = _espera_25("esperando: fila de E2E", 30)
    assert ag["estado"] == "rodando" and ag["espera"] == "fila de E2E", ag


def test_ticket25_espera_sem_prazo_vira_travada_depois_do_teto_com_o_motivo():
    ag, _, _ = _espera_25("esperando: fila de E2E", 61)
    assert ag["estado"] == "travado" and ag["motivo"] == "espera vencida", ag


def test_ticket25_fase_comum_continua_com_os_15_min():
    assert _espera_25("fase-3", 16)[0]["estado"] == "travado"
    assert _espera_25("fase-3", 14)[0]["estado"] == "rodando"
    assert "motivo" not in _espera_25("fase-3", 16)[0]


def test_ticket25_prazo_declarado_vale_ate_o_horario_e_depois_vence():
    assert _espera_25(f"esperando: CI até {_hhmm_25(40)}", 30)[0]["estado"] == "rodando"
    ag = _espera_25(f"esperando: CI até {_hhmm_25(-5)}", 30)[0]
    assert ag["estado"] == "travado" and ag["motivo"] == "espera vencida", ag


def test_ticket25_monta_agentes_usa_a_mesma_regra():
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc)
    msgs = [{"id": "m1", "type": "heartbeat", "payload": json.dumps({"taskId": "t", "dispatchId": "ctx_1", "phase": "esperando: deploy"}), "created_at": _iso(-1800)}]
    ws = [{"dispatchId": "ctx_1", "taskId": "t", "dispatchStatus": "dispatched"}]
    assert orq_mod.monta_agentes(ws, msgs, [], agora)[0]["estado"] == "rodando"
    msgs[0]["created_at"] = _iso(-3700)
    ag = orq_mod.monta_agentes(ws, msgs, [], agora)[0]
    assert ag["estado"] == "travado" and ag["motivo"] == "espera vencida"


def test_ticket25_resumo_mostra_a_espera_numa_linha_so_uma_vez():
    ag, ab, agora = _espera_25("esperando: fila de E2E", 30)
    txt = orq_mod.resumo([], ab, {"itens": []}, agora=agora)
    assert txt.count("esperando: fila de E2E") == 1 and "Travado" not in txt, txt
    ab["agentes"][0]["ultimo_heartbeat"] = now_iso(-3700)
    venc = orq_mod.resumo([], ab, {"itens": []}, agora=agora)
    assert "espera vencida" in venc and "Travado: task_aaaaaaaaaa" in venc, venc


def _ws_43(hb_min, fase="compilando", controle_min=None, turno=True):
    """monta_agentes de um worker que encerrou o turno há 10 min (parado), com o último heartbeat de `hb_min` atrás; `controle_min`: interromper ok há tanto."""
    agora = datetime.now(timezone.utc)
    ws = [{"dispatchId": "ctx_1", "taskId": "task_1", "runId": "run_a", "dispatchStatus": "dispatched"}]
    det = {"ctx_1": {"agente": "claude", "desde": _iso(-7200).replace(" ", "T") + "Z"}}
    turnos = {"ctx_1": {"task": "task_1", "inicio": now_iso(-(hb_min + 5) * 60), "fim": now_iso(-600)}}
    msgs = [{"id": "m1", "type": "heartbeat", "payload": json.dumps({"taskId": "task_1", "dispatchId": "ctx_1", "phase": fase}), "created_at": _iso(-hb_min * 60)}]
    events = [{"tipo": "controle", "acao": "interromper", "resultado": "ok", "dispatch": "ctx_1", "ts": now_iso(-controle_min * 60)}] if controle_min else []
    return ws, msgs, events, agora, det, turnos


def test_ticket43_tela_com_shell_em_execucao_e_espera_e_nao_parado():
    ws, msgs, ev, agora, det, turnos = _ws_43(20)
    ag = orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos, telas={"ctx_1": "1 shell still running (tela)"})[0]
    assert ag["estado"] == "rodando" and ag["espera"] == "1 shell still running (tela)" and ag["turno"] == "parado", ag
    sem = orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos)[0]
    assert sem["estado"] == "parado", "sem a tela o turno encerrado continua parado"
    r = orq_mod.reavalia([ag], ev, agora, turnos)[0]
    assert r["estado"] == "rodando" and r["espera"], "o cache dos hooks de prompt guarda a espera sem ler a tela"
    assert "esperando: 1 shell still running" in orq_mod.texto_agentes([ag])


def test_ticket43_shell_em_execucao_sem_heartbeat_alem_do_teto_e_travado():
    ws, msgs, ev, agora, det, turnos = _ws_43(46)
    ag = orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos, telas={"ctx_1": "shell still running (tela)"})[0]
    assert ag["estado"] == "travado" and ag["motivo"] == "shell sem heartbeat", ag
    assert orq_mod.reavalia([ag], ev, agora, turnos)[0]["motivo"] == "shell sem heartbeat"


def test_ticket43_tela_lida_antes_de_novo_heartbeat_deixa_de_valer():
    ws, msgs, ev, agora, det, turnos = _ws_43(20)
    ag = orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos, telas={"ctx_1": "shell still running (tela)"})[0]
    ag["tela_ts"] = now_iso(-900)  # lida há 15 min; o heartbeat (20 min atrás) é anterior, vale
    assert orq_mod.reavalia([ag], ev, agora, turnos)[0]["estado"] == "rodando"
    ag["tela_ts"] = now_iso(-1500)  # lida antes do heartbeat: o worker deu sinal depois, a tela é velha
    assert not orq_mod.reavalia([ag], ev, agora, turnos)[0].get("espera")


def test_ticket43_heartbeat_waiting_em_ingles_e_espera():
    ws, msgs, ev, agora, det, turnos = _ws_43(30, fase="waiting for E2E queue")
    ag = orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos)[0]
    assert ag["estado"] == "rodando" and ag["espera"] == "for E2E queue", ag
    ws, msgs, ev, agora, det, turnos = _ws_43(61, fase="waiting…")
    ag = orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos)[0]
    assert ag["estado"] == "travado" and ag["motivo"] == "espera vencida", "waiting sem prazo vence no teto"
    assert orq_mod.espera_declarada("waiting…", now_iso(0))[0] == "esperando"


def test_ticket43_interromper_pausa_o_worker_e_nao_e_travado():
    ws, msgs, ev, agora, det, turnos = _ws_43(127, controle_min=5)
    ag = orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos)[0]
    assert ag["estado"] == "rodando" and ag["espera"] == "interrompido pelo coordenador", ag
    assert orq_mod.reavalia([ag], ev, agora, turnos)[0]["estado"] == "rodando"
    ws, msgs, ev, agora, det, turnos = _ws_43(127, controle_min=200)  # interrompido antes do último heartbeat: o worker voltou a dar sinal
    assert orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos)[0]["estado"] == "parado"
    ws, msgs, ev, agora, det, turnos = _ws_43(127, controle_min=5)
    turnos["ctx_1"]["inicio"], turnos["ctx_1"]["fim"] = now_iso(-60), None  # o steer abriu turno novo depois do interrupt
    ag = orq_mod.monta_agentes(ws, msgs, ev, agora, det, turnos=turnos)[0]
    assert ag["estado"] == "travado" and "espera" not in ag, ag


def test_ticket43_telas_le_so_worker_claude_rodando_e_falha_de_leitura_nao_prova_nada():
    lidos, orig = [], orq_mod.orca

    def falso(*args, **k):
        lidos.append(args[args.index("--terminal") + 1])
        if args[args.index("--terminal") + 1] == "term_f":
            raise RuntimeError("orca fora")
        return {"terminal": {"tail": ["x", "  2 shells still running"]}}
    orq_mod.orca = falso
    try:
        ws = [{"dispatchId": f"ctx_{h}", "agentTerminalHandle": f"term_{h}", "dispatchStatus": st} for h, st in (("a", "dispatched"), ("f", "dispatched"), ("c", "dispatched"), ("d", "completed"))]
        det = {"ctx_a": {"agente": "claude"}, "ctx_f": {"agente": "claude"}, "ctx_c": {"agente": "cursor"}, "ctx_d": {"agente": "claude"}}
        assert orq_mod._telas(ws, det) == {"ctx_a": "2 shells still running (tela)"}, "agente sem adaptador e completed nem são lidos; o read que falha fica de fora"
        assert sorted(lidos) == ["term_a", "term_f"], lidos
    finally:
        orq_mod.orca = orig


def test_ticket43_tela_espera_reconhece_o_rodape_do_claude_code():
    for t in ("1 shell still running", "2 shells still running", "monitor still running", "3 monitors still running"):
        assert orq_mod.TELA_ESPERA.search(f"  ⏵⏵ bypass permissions on · {t}"), t
    assert not orq_mod.TELA_ESPERA.search("esc to interrupt")
    assert orq_mod.TELA_TETO_S == 45 * 60


def test_ticket25_teto_com_nome():
    assert orq_mod.ESPERA_TETO_S == 60 * 60


# ---------- ticket 26: steer com reentrega e alerta; orq responder ----------

def _steer_26(a, lido=0, **linha):
    """Um steer a worker que o Orca já avisou (ocupado: o orq não digitou nada), o worker parado no prompt e a linha do inbox com `read`."""
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_rodando", "dispatch": "ctx_1", "status": "dispatched",
                            "desde": _iso(-3000), "agente": "claude"}])
    a.set("terminals.json", ["term_w1", "term_coord"])
    a.set("busy.json", ["term_w1"])
    _steer_env(a)
    r = a.orq("steer", "task_rodando", "use o índice novo")
    assert r.returncode == 0, r.stderr
    assert _log(a, "send.log") == [], "o steer a worker ocupado não digita nada"
    a.set("busy.json", [])
    _parado_26(a)
    _linha_26(a, lido, **linha)


def _parado_26(a, parado=True):
    os.makedirs(a.home, exist_ok=True)
    fim = now_iso(-300) if parado else None
    json.dump({"ctx_1": {"task": "task_rodando", "inicio": now_iso(-600), "fim": fim}}, open(os.path.join(a.home, "turnos.json"), "w"))


def _linha_26(a, lido=0, **campos):
    """A mensagem do steer no inbox do Orca (msg_9 é o id que o Orca falso devolve ao send)."""
    _inbox(a, {"id": "msg_9", "run_id": "run_a", "type": "status", "priority": "high", "subject": "Ajuste", "body": "use o índice novo", "payload": None,
               "from_handle": "term_coord", "to_handle": "dispatch:ctx_1", "read": lido, "sequence": 900, "created_at": _iso(-5), "delivered_at": None, **campos})


def _envelhece_26(a, seg):
    """Recua `seg` segundos o carimbo dos steers e das reentregas: o tempo que o gerente espera antes de agir."""
    import calendar
    p = os.path.join(a.home, "events.jsonl")
    linhas = []
    for x in open(p).read().splitlines():
        e = json.loads(x)
        if e["tipo"] in ("steer", "steer_reentrega"):
            e["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(calendar.timegm(time.strptime(e["ts"], "%Y-%m-%dT%H:%M:%SZ")) - seg))
        linhas.append(json.dumps(e))
    open(p, "w").write("\n".join(linhas) + "\n")


def _digitados_26(a):
    return [e[e.index("--text") + 1] for e in _avisos_enviados(a, "term_w1")]


def test_ticket26_sem_leitura_depois_de_90_s_com_o_worker_parado_redigita_o_aviso():
    a = Amb()
    _steer_26(a)
    assert a.orq("steers").returncode == 0 and _digitados_26(a) == [], "menos de 90 s: ainda dá tempo de ler"
    _envelhece_26(a, 100)
    r = a.orq("steers")
    assert r.returncode == 0, r.stderr
    (texto,) = _digitados_26(a)
    assert texto.startswith("You have 1 orchestration message.") and "check --terminal term_w1" in texto, texto
    (ev,) = [e for e in a.events() if e["tipo"] == "steer_reentrega"]
    assert (ev["msg_id"], ev["dispatch"], ev["tentativa"]) == ("msg_9", "ctx_1", 1), ev
    assert _digitados_26(a) == [texto] and a.orq("steers").returncode == 0 and len(_digitados_26(a)) == 1, "a tentativa recomeça a contagem dos 90 s"


def test_ticket26_worker_ocupado_nao_recebe_nada():
    a = Amb()
    _steer_26(a)
    _parado_26(a, parado=False)  # o turno está aberto: o Orca avisa sozinho quando ele terminar
    _envelhece_26(a, 500)
    assert a.orq("steers").returncode == 0
    assert _digitados_26(a) == [] and not [e for e in a.events() if e["tipo"] in ("steer_reentrega", "alerta")]
    b = Amb()
    _steer_26(b)
    _envelhece_26(b, 500)
    r = b.orq("steers", FAKE_PROMPT_BLOCKED="1")  # o hook diz parado, mas o Orca sabe que o agente está no meio do turno
    assert r.returncode == 0 and not [e for e in b.events() if e["tipo"] == "steer_reentrega"], "o Orca recusou o texto: nenhuma tentativa gasta"
    assert not [e for e in b.events() if e["tipo"] == "alerta"]


def test_ticket26_terceira_falha_grava_o_alerta_e_para_de_digitar():
    a = Amb()
    _steer_26(a)
    for _ in range(3):
        _envelhece_26(a, 100)
        assert a.orq("steers").returncode == 0
    assert len(_digitados_26(a)) == 3 and not [e for e in a.events() if e["tipo"] == "alerta"], "três tentativas, ainda sem alerta"
    _envelhece_26(a, 100)
    r = a.orq("steers")
    assert "steer não lido" in r.stdout, r.stdout
    (al,) = [e for e in a.events() if e["tipo"] == "alerta"]
    assert (al["alerta"], al["task"], al["dispatch"], al["run"], al["msg_id"]) == ("steer_nao_lido", "task_rodando", "ctx_1", "run_a", "msg_9"), al
    _envelhece_26(a, 100)
    a.orq("steers")
    assert len(_digitados_26(a)) == 3 and len([e for e in a.events() if e["tipo"] == "alerta"]) == 1, "depois do alerta nada mais é digitado nem gravado"
    assert "steer não lido" in a.orq("status").stdout, "o resumo carrega o alerta"
    ag = _agentes(a)["ctx_1"]
    assert ag["alerta"] == "steer não lido", ag
    assert "steer não lido" in a.orq("agentes").stdout
    a.orq("alerta", "visto", "task_rodando")
    assert "steer não lido" not in a.orq("status").stdout and "alerta" not in _agentes(a)["ctx_1"], "orq alerta visto trata o alerta"


def test_ticket118_worker_com_pergunta_aberta_nao_recebe_aviso_nem_alerta():
    a = Amb()
    _steer_26(a)
    for _ in range(3):
        _envelhece_26(a, 100)
        assert a.orq("steers").returncode == 0
    assert len(_digitados_26(a)) == 3
    _inbox(a, _pergunta_ask(950, "ctx_1"), {"id": "msg_9", "run_id": "run_a", "type": "status", "priority": "high", "subject": "Ajuste", "body": "x",
                                          "payload": None, "from_handle": "term_coord", "to_handle": "dispatch:ctx_1", "read": 0, "sequence": 900,
                                          "created_at": _iso(-5), "delivered_at": None})
    _envelhece_26(a, 100)
    r = a.orq("steers")
    assert r.returncode == 0 and "steer não lido" not in r.stdout, r.stdout
    assert not [e for e in a.events() if e["tipo"] == "alerta"], "a pergunta aberta é a razão do silêncio: sem alerta"
    assert len(_digitados_26(a)) == 3


def test_ticket26_nenhuma_reentrega_depois_da_leitura():
    a = Amb()
    _steer_26(a)
    _envelhece_26(a, 100)
    a.orq("steers")
    assert len(_digitados_26(a)) == 1
    _linha_26(a, lido=1)  # o worker rodou o check
    for _ in range(4):
        _envelhece_26(a, 100)
        assert a.orq("steers").returncode == 0
    assert len(_digitados_26(a)) == 1 and not [e for e in a.events() if e["tipo"] == "alerta"], "lido: sem nova tentativa e sem alerta"
    assert [(e["motivo"], e["fonte"]) for e in a.events() if e["tipo"] == "steer_fim"] == [("lido", "orca")]
    b = Amb()
    _steer_26(b, lido=1)
    _envelhece_26(b, 500)
    b.orq("steers")
    assert _digitados_26(b) == []


def _transcrito_26(a, texto, sessao="sess26"):
    """Transcrito da sessão do worker em ORQ_PROJETOS, com `texto` no fim, e o turnos.json apontando para ela."""
    pasta = os.path.join(a.tmp.name, "projetos", "p")
    os.makedirs(pasta, exist_ok=True)
    open(os.path.join(pasta, sessao + ".jsonl"), "w").write(json.dumps({"type": "user", "message": {"content": "oi"}}) + "\n" + json.dumps({"type": "user", "message": {"content": texto}}) + "\n")
    json.dump({"ctx_1": {"task": "task_rodando", "sessao": sessao, "inicio": now_iso(-600), "fim": now_iso(-300)}}, open(os.path.join(a.home, "turnos.json"), "w"))
    return {"ORQ_PROJETOS": os.path.join(a.tmp.name, "projetos")}


def test_ticket26_worker_que_leu_por_check_sem_ack_conta_como_lido_pelo_transcrito():
    a = Amb()
    _steer_26(a)  # read segue 0: o check do worker não confirmou a entrega (visto no Orca real)
    env = _transcrito_26(a, 'tool_result {"messages": [{"id": "msg_9", "subject": "Ajuste"}]}')
    _envelhece_26(a, 100)
    assert a.orq("steers", **env).returncode == 0
    assert _digitados_26(a) == [] and [(e["motivo"], e["fonte"]) for e in a.events() if e["tipo"] == "steer_fim"] == [("lido", "transcrito")]
    b = Amb()
    _steer_26(b)
    env = _transcrito_26(b, "o worker rodou outra coisa e não viu o ajuste")
    _envelhece_26(b, 100)
    assert b.orq("steers", **env).returncode == 0 and len(_digitados_26(b)) == 1, "transcrito sem o id: não leu"


def test_ticket26_nenhum_aviso_duplicado_quando_o_proprio_orca_ja_avisou_o_worker():
    a = Amb()
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_rodando", "dispatch": "ctx_1", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"}])
    a.set("terminals.json", ["term_w1", "term_coord"])
    _steer_env(a)
    _linha_26(a, delivered_at=_iso(-1))  # o Orca digitou o aviso dele e o turno do worker ainda não começou (o terminal segue ocioso)
    assert a.orq("steer", "task_rodando", "use o índice novo").returncode == 0
    assert _log(a, "send.log") == [], "o orq não digita um segundo aviso"
    assert [e["aviso_terminal"] for e in a.events() if e["tipo"] == "steer"] == ["orca"]
    b = Amb()
    b.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_rodando", "dispatch": "ctx_1", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"}])
    b.set("terminals.json", ["term_w1", "term_coord"])
    _steer_env(b)
    _linha_26(b)  # sem delivered_at: o Orca não avisou este worker parado no prompt, o orq avisa
    assert b.orq("steer", "task_rodando", "use o índice novo").returncode == 0 and len(_digitados_26(b)) == 1


def test_ticket26_dispatch_encerrado_fecha_o_acompanhamento_sem_alerta():
    a = Amb()
    _steer_26(a)
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_rodando", "dispatch": "ctx_1", "status": "completed", "desde": _iso(-3000), "agente": "claude"}])
    _envelhece_26(a, 500)
    assert a.orq("steers").returncode == 0
    assert _digitados_26(a) == [] and [e["motivo"] for e in a.events() if e["tipo"] == "steer_fim"] == ["encerrado"]


def test_ticket26_sem_steer_aberto_nao_chama_o_orca():
    a = Amb()
    assert a.orq("steers").returncode == 0 and not os.path.exists(os.path.join(a.fake, "calls.log"))


def test_ticket26_painel_do_gerente_reentrega_a_cada_volta():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "task_rodando", "dispatch": "ctx_1", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"}])
    a.set("terminals.json", ["term_w1", "term_ger", "term_coord"])
    a.set("busy.json", ["term_w1"])
    _steer_env(a)
    assert a.orq("steer", "task_rodando", "use o índice novo", ORCA_TERMINAL_HANDLE="term_coord").returncode == 0
    a.set("busy.json", [])
    _parado_26(a)
    _linha_26(a)
    _envelhece_26(a, 100)
    r = a.orq("gerente", "absorver")
    assert r.returncode == 0 and len(_digitados_26(a)) == 1 and "task_rodando" in r.stdout and "redigitado" in r.stdout, (r.stdout, r.stderr)


def test_ticket26_constantes_com_nome():
    assert (orq_mod.STEER_LEITURA_S, orq_mod.STEER_TENTATIVAS) == (90, 3)


def test_ticket26_responder_liga_o_run_da_pergunta_e_responde_pelo_gerente():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_coord")
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    _inbox(a, _pergunta_ask(50, "ctx_q", run="run_b"))
    r = a.orq("responder", "msg_q50", "use o índice")
    assert r.returncode == 0, r.stderr
    (chamada,) = _log(a, "replied.log")
    assert chamada[:5] == ["reply", "--id", "msg_q50", "--body", "use o índice"] and chamada[chamada.index("--run") + 1] == "run_b", chamada
    assert _binds(a)["run_b"] == "term_ger", "o gerente foi ligado ao Run da pergunta antes de responder"
    (ev,) = [e for e in a.events() if e["tipo"] == "resposta_worker"]
    assert (ev["msg_id"], ev["run"], ev["dispatch"]) == ("msg_q50", "run_b", "ctx_q"), ev


def test_ticket26_responder_no_run_ligado_do_coordenador_sem_gerente():
    a = Amb(run="run_a")
    _inbox(a, _pergunta_ask(50, "ctx_q", run="run_a"))
    r = a.orq("responder", "msg_q50", "sim")
    assert r.returncode == 0, r.stderr
    assert len(_log(a, "replied.log")) == 1


def test_ticket26_responder_recusa_run_que_o_coordenador_nao_segura_e_mensagem_inexistente():
    a = Amb(run="run_a")
    _inbox(a, _pergunta_ask(50, "ctx_q", run="run_b"))
    r = a.orq("responder", "msg_q50", "sim")
    assert r.returncode == 1 and "run-use --id run_b" in r.stderr and not _log(a, "replied.log"), r.stderr
    r = a.orq("responder", "msg_fantasma", "sim")
    assert r.returncode == 1 and "msg_fantasma" in r.stderr and not _log(a, "replied.log")


def test_ticket28_pendencia_viva_futura_vencida_esperando_e_envelhecida():
    from datetime import date
    hoje = date(2026, 9, 29)
    base = {"id": "p", "tipo": "acao", "titulo": "t", "desde": "2026-09-25"}
    assert orq_mod.pend_depois(base, hoje) is None, "viva"
    assert orq_mod.pend_depois({**base, "ate": "2026-10-05"}, hoje) == "até 2026-10-05", "data futura"
    assert orq_mod.pend_depois({**base, "ate": "2026-09-29"}, hoje) is None, "data de hoje volta"
    assert orq_mod.pend_depois({**base, "ate": "2026-09-20", "espera": "Ana", "desde": "2026-08-01"}, hoje) is None, "data vencida volta, mesmo esperando e velha"
    assert orq_mod.pend_depois({**base, "espera": "Ana"}, hoje) == "esperando Ana"
    assert orq_mod.pend_depois({**base, "desde": "2026-09-15"}, hoje) is None, "14 dias ainda é viva"
    assert orq_mod.pend_depois({**base, "desde": "2026-09-14"}, hoje) == "parada há 15 d", "envelhecida"
    assert orq_mod.pend_depois({**base, "desde": "2026-08-01", "gate": "g1"}, hoje) is None, "decisão com gate não some"
    assert orq_mod.pend_depois({"id": "sem-desde", "tipo": "avisar", "titulo": "t"}, hoje) is None, "item antigo sem desde segue vivo"


def test_ticket28_resumo_mostra_so_as_vivas_e_conta_depois():
    a = Amb()
    a.set("../pendencias.json", {"itens": [
        {"id": "viva", "tipo": "decisao", "titulo": "t", "desde": "2099-01-01"},
        {"id": "futura", "tipo": "acao", "titulo": "t", "desde": "2099-01-01", "ate": "2099-12-31"},
        {"id": "espera", "tipo": "acao", "titulo": "t", "desde": "2099-01-01", "espera": "Ana"},
        {"id": "velha", "tipo": "acao", "titulo": "t", "desde": "2020-01-01"},
    ]})
    ctx = json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "Com você: 1 (1 decisões). Depois: 3." in ctx, ctx
    assert "Depois" not in orq_mod.resumo([], None, {"itens": [{"id": "v", "tipo": "acao", "titulo": "t", "desde": "2099-01-01"}]})


def test_ticket28_pend_add_ate_e_lista():
    a = Amb()
    a.set("../pendencias.json", {"itens": [{"id": "antiga", "tipo": "decisao", "titulo": "sem ate nem desde"}]})
    r = a.orq("pend", "add", "--id", "depois", "--tipo", "acao", "--titulo", "mais tarde", "--ate", "2099-12-31")
    assert r.returncode == 0 and json.loads(r.stdout)["ate"] == "2099-12-31", r.stderr
    r = a.orq("pend", "add", "--id", "ruim", "--tipo", "acao", "--titulo", "x", "--ate", "31/12/2099")
    assert r.returncode == 1 and "AAAA-MM-DD" in r.stderr
    viva = a.orq("pend", "lista").stdout
    assert "antiga" in viva and "depois" not in viva, "as pendências atuais continuam aparecendo"
    todas = a.orq("pend", "lista", "--todas").stdout
    assert "antiga" in todas and "depois  acao  mais tarde  [Depois: até 2099-12-31]" in todas, todas


def _amb_resumo():
    """Fixture do ticket 29: um dia de eventos, um aberto.json com um worker vivo e tickets em todos os estados."""
    a = Amb()
    ev = [
        {"ts": "2026-09-29T09:00:00Z", "tipo": "entrada", "id": "e1", "origem": "usuario", "texto": "pedido antigo"},
        {"ts": "2026-09-29T09:01:00Z", "tipo": "intake", "entrada": "e1", "efeito": "conversa"},
        {"ts": "2026-09-29T09:02:00Z", "tipo": "resposta", "header": "Antiga", "resposta": "Sim"},
        {"ts": "2026-09-29T10:00:00Z", "tipo": "entrada", "id": "e2", "origem": "usuario", "texto": "corrija o filtro de marca"},
        {"ts": "2026-09-29T10:01:00Z", "tipo": "intake", "entrada": "e2", "efeito": "tarefa", "ref": "task_f1"},
        {"ts": "2026-09-29T10:05:00Z", "tipo": "entrada", "id": "e3", "origem": "relatorio", "texto": "Relatório semanal: 3 itens"},
        {"ts": "2026-09-29T10:06:00Z", "tipo": "entrada", "id": "e4", "origem": "relatorio_worker", "texto": "worker_done da task_f1"},
        {"ts": "2026-09-29T10:07:00Z", "tipo": "intake", "entrada": "e4", "efeito": "pend", "ref": "revisar-pr"},
        {"ts": "2026-09-29T10:08:00Z", "tipo": "resposta", "header": "Publicar orq", "resposta": "Recriar o repo (Recomendado)"},
        {"ts": "2026-09-29T10:09:00Z", "tipo": "resposta_lavish", "item": "D01", "header": "Inspetor", "resposta": "Mascarada + id", "disposicao": "manter"},
        {"ts": "2026-09-29T10:10:00Z", "tipo": "pend", "op": "done", "pend": "freio-prod", "resposta": "liberado"},
    ]
    with open(os.path.join(a.home, "events.jsonl") if os.path.isdir(a.home) else _mk29(a.home), "a") as f:
        f.write("\n".join(json.dumps(e) for e in ev) + "\n")
    with open(os.path.join(a.home, "aberto.json"), "w") as f:
        json.dump({"ts": "2026-09-29T10:11:00Z", "backlog": [], "rodando": 1, "andamento": [], "bloqueado": [], "gates": [], "falhas": [], "runs": [],
                   "agentes": [{"dispatch": "ctx_1", "task": "task_f1", "run": "run_a", "titulo": "Corrigir o filtro de marca", "estado": "rodando", "fase": "implementing"},
                               {"dispatch": "ctx_2", "task": "task_f2", "run": "run_a", "titulo": "Worker parado", "estado": "entregue_sem_liberar", "fase": None}]}, f)
    with open(a.env["ORQ_PENDENCIAS"], "w") as f:
        json.dump({"itens": [{"id": "publicar", "tipo": "decisao", "titulo": "Publicar o repositório"},
                             {"id": "depois", "tipo": "acao", "titulo": "Fica pra depois", "ate": "2099-12-31"}]}, f)
    os.makedirs(a.env["ORQ_ISSUES"])
    for n, nome, status, bl in (("01", "Base", "resolved", ""), ("02", "Pronto", "ready-for-agent", "01"), ("03", "Andando", "claimed", "01"),
                                ("04", "Travado", "ready-for-agent", "02")):
        with open(os.path.join(a.env["ORQ_ISSUES"], f"{n}-{nome.lower()}.md"), "w") as f:
            f.write(f"# {n}: {nome}\n\nStatus: {status}\nBlocked by: {bl or '(nenhum)'}\n")
    return a


def _mk29(home):
    os.makedirs(home)
    return os.path.join(home, "events.jsonl")


def test_ticket29_resumo_traz_as_quatro_partes_e_as_decisoes():
    a = _amb_resumo()
    r = a.orq("resumo")
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "Com você (1)" in out and "publicar" in out and "Publicar o repositório" in out and "Fica pra depois" not in out, out
    assert "Entrou" in out and "e2" in out and "tarefa task_f1" in out and "e3" in out and "sem efeito" in out and "e4" in out and "pend revisar-pr" in out, out
    assert "e1" not in out, "a entrada anterior à última mensagem do usuário fica de fora"
    assert "Anda (1)" in out and "Corrigir o filtro de marca" in out and "implementing" in out and "Worker parado" not in out, out
    assert "Vem" in out and "pronto: 02 Pronto" in out and "bloqueado: 04 Travado (espera 02)" in out and "Andando" not in out and "Base" not in out, out
    assert "Decisões (3)" in out and "Publicar orq: Recriar o repo" in out and "Inspetor: Mascarada + id" in out and "freio-prod: liberado" in out and "Antiga" not in out, out


def test_ticket29_resumo_desde_muda_a_janela():
    a = _amb_resumo()
    out = a.orq("resumo", "--desde", "2026-09-29T08:00:00Z").stdout
    assert "e1" in out and "Antiga: Sim" in out, out
    vazio = a.orq("resumo", "--desde", "2026-09-30T00:00:00Z").stdout
    assert "Entrou: nada" in vazio and "Decisões: nenhuma" in vazio, vazio


def test_ticket29_resumo_e_curto_e_sem_dados_nao_cai():
    a = _amb_resumo()
    assert len(a.orq("resumo").stdout.splitlines()) <= 40
    b = Amb()
    r = b.orq("resumo")
    assert r.returncode == 0 and "Com você" in r.stdout and "Vem: nenhum ticket aberto" in r.stdout, r.stdout + r.stderr
    assert b.orq("resumo", "--desde", "ontem").returncode == 1


def test_audiencia_check_acha_termo_proibido_e_pula_sem_lista():
    """scripts/audiencia-check.py: falha com arquivo:linha, passa limpo, pula sem a lista; o pre-commit do repositório o chama"""
    check = os.path.join(AQUI, "scripts", "audiencia-check.py")
    with tempfile.TemporaryDirectory() as d:
        subprocess.run(["git", "init", "-q", d], check=True)
        with open(os.path.join(d, "doc.md"), "w") as f:
            f.write("limpo\nfala da Empresa Secreta aqui\n")
        subprocess.run(["git", "-C", d, "add", "doc.md"], check=True)
        lista = os.path.join(d, "termos.txt")
        with open(lista, "w") as f:
            f.write("# comentário\nempresa secreta\nre:host-\\d+\n")

        def rodar(termos):
            return subprocess.run([sys.executable, check, d], capture_output=True, text=True, env={**os.environ, "ORQ_TERMOS": termos})

        r = rodar(lista)
        assert r.returncode == 1 and "doc.md:2:" in r.stdout and "doc.md:1:" not in r.stdout, r.stdout + r.stderr
        with open(os.path.join(d, "doc.md"), "w") as f:
            f.write("limpo\n")
        assert rodar(lista).returncode == 0
        r = rodar(os.path.join(d, "nao-existe.txt"))
        assert r.returncode == 0 and "pulada" in r.stderr, r.stderr
    hook = os.path.join(AQUI, "githooks", "pre-commit")
    assert os.access(hook, os.X_OK) and "audiencia-check.py" in open(hook).read()
    # o repositório atual passa (sem a lista privada a checagem pula, e passa também)
    assert subprocess.run([sys.executable, check], capture_output=True, text=True).returncode == 0


def _repo_git(tmp, sujo=False):
    """Repositório git com um commit; devolve (caminho, sha curto)."""
    repo = os.path.join(tmp, "r" + str(len(os.listdir(tmp))))
    os.makedirs(repo)
    g = lambda *x: subprocess.run(["git", "-C", repo, *x], capture_output=True, text=True, check=True).stdout.strip()  # noqa: E731
    g("init", "-q")
    open(os.path.join(repo, "f"), "w").write("x")
    g("add", "f")
    g("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "c")
    if sujo:
        open(os.path.join(repo, "novo"), "w").write("y")
    return repo, g("rev-parse", "--short=7", "HEAD")


def test_it_should_be_silent_when_the_cited_commit_exists_and_the_tree_is_clean():
    with tempfile.TemporaryDirectory() as t:
        repo, sha = _repo_git(t)
        assert orq_mod.confere_entrega(f"pronto, commit {sha} sem push", [repo]) == []
        assert orq_mod.confere_entrega("sem commit citado", [repo]) == []


def test_it_should_warn_when_the_cited_commit_does_not_exist():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        av = orq_mod.confere_entrega("commit abc1234 publicado", [repo])
        assert len(av) == 1 and "entrega sem commit" in av[0] and "abc1234" in av[0], av


def test_it_should_warn_when_the_tree_is_dirty():
    with tempfile.TemporaryDirectory() as t:
        repo, sha = _repo_git(t, sujo=True)
        av = orq_mod.confere_entrega(f"commit {sha}", [repo])
        assert len(av) == 1 and "árvore suja" in av[0], av


def test_it_should_check_the_pr_commits_with_gh_and_ignore_a_missing_gh():
    with tempfile.TemporaryDirectory() as t:
        repo, sha = _repo_git(t)
        txt = f"commit {sha} no PR https://github.com/o/r/pull/7"
        assert orq_mod.confere_entrega(txt, [repo], lambda u: [sha + "0" * 33]) == []
        av = orq_mod.confere_entrega(txt, [repo], lambda u: ["deadbeef" * 5])
        assert len(av) == 1 and "PR" in av[0], av
        assert orq_mod.confere_entrega(txt, [repo], lambda u: None) == [], "sem gh ou sem rede não avisa"


def test_it_should_show_the_delivery_warning_in_the_resumo_and_in_orq_agentes():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(ORQ_REPOS=repo)
        a.set("automations_runs.json", {"result": {"runs": []}})
        a.set("inbox.json", {"result": {"messages": [{"id": "msg_e1", "run_id": "run_a", "type": "worker_done", "subject": "pronto", "sequence": 5,
                                         "body": "commit abc1234 sem push", "created_at": "2099-01-01T00:00:00Z",
                                         "payload": json.dumps({"taskId": "task_e", "dispatchId": "ctx_e", "outcome": "succeeded"})}]}})
        os.makedirs(a.home, exist_ok=True)
        json.dump({"ingest": {"desde": DESDE_CEDO, "inbox_seq": 0, "runs": []}}, open(os.path.join(a.home, "cursor.json"), "w"))
        a.orq("ingest")
        ev = [e for e in a.events() if e.get("tipo") == "entrega"]
        assert len(ev) == 1 and ev[0]["dispatch"] == "ctx_e", a.events()
        assert "Entrega sem prova" in a.orq("resumo").stdout and "abc1234" in a.orq("resumo").stdout
        ags = orq_mod.monta_agentes([{"dispatchId": "ctx_e", "taskId": "task_e", "dispatchStatus": "completed", "agentTerminalHandle": "term_e"}],
                                    [], a.events(), datetime.now(timezone.utc), vivos=["term_e"])
        assert ags[0]["entrega"] and "AVISO: entrega sem commit" in orq_mod.texto_agentes(ags)


# ---------- ticket 32: interromper, encerrar e relançar ----------

def _ctl_env(a, repo, **w):
    """Um worker rodando (ctx_w1, task_w1) numa worktree que é um repositório git de verdade."""
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "dispatch": "ctx_w1", "task": "task_w1", "status": "dispatched", "worktree": repo,
                            "modelo": "claude-sonnet-5-5", "agente": "claude", **w}])
    a.set("tasks_run_a.json", [{"id": "task_w1", "task_title": "Tarefa w1", "status": "dispatched", "dispatch_id": "ctx_w1"}])
    a.set("inbox.json", {"result": {"messages": []}})


def _ctl_eventos(a, acao=None):
    return [e for e in a.events() if e["tipo"] == "controle" and (acao is None or e["acao"] == acao)]


def _head(repo):
    return subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def test_it_should_send_an_interrupt_to_the_terminal_and_log_it():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        r = a.orq("interromper", "ctx_w1")
        assert r.returncode == 0, r.stderr
        envios = _log(a, "send.log")
        assert envios and "--interrupt" in envios[0] and "term_w1" in envios[0], envios
        assert "--text" not in envios[0], "o interrupt não digita texto"
        (ev,) = _ctl_eventos(a, "interromper")
        assert (ev["dispatch"], ev["task"], ev["run"], ev["terminal"], ev["resultado"]) == ("ctx_w1", "task_w1", "run_a", "term_w1", "ok"), ev
        assert not _log(a, "stopped.log"), "interromper deixa o worker vivo"


def test_it_should_refuse_to_interrupt_a_dispatch_that_is_not_running():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo, status="completed")
        r = a.orq("interromper", "ctx_w1")
        assert r.returncode == 1 and "não está rodando" in r.stderr, r
        assert not _log(a, "send.log") and not _ctl_eventos(a)
        assert a.orq("interromper", "ctx_fantasma").returncode == 1


def test_it_should_log_a_failed_interrupt():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        r = a.orq("interromper", "ctx_w1", FAKE_FAIL_TERMINAL="send")
        assert r.returncode == 1, r
        (ev,) = _ctl_eventos(a, "interromper")
        assert ev["resultado"] == "falhou" and "send" in ev["erro"], ev


def test_it_should_stop_then_release_with_the_reason_and_keep_the_worktree():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t, sujo=True)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        antes = _head(repo)
        r = a.orq("encerrar", "ctx_w1", "--motivo", "travado no CI")
        assert r.returncode == 0, r.stderr
        chamadas = [c[0] for c in _log(a, "calls.log") if c[0] in ("worker-stop", "worker-release")]
        assert chamadas == ["worker-stop", "worker-release"], chamadas
        assert _log(a, "stopped.log")[0][:3] == ["worker-stop", "--dispatch", "ctx_w1"]
        evs = _ctl_eventos(a, "encerrar")
        assert [e["resultado"] for e in evs] == ["iniciado", "ok"], evs
        assert evs[-1]["motivo"] == "travado no CI" and evs[-1]["head"] == antes[:12] and evs[-1]["sujo"] == 1, evs[-1]
        assert os.path.isdir(repo) and _head(repo) == antes and os.path.exists(os.path.join(repo, "novo")), "a worktree e o trabalho não commitado seguem intactos"


def test_it_should_require_a_reason_to_encerrar():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        assert a.orq("encerrar", "ctx_w1").returncode == 2
        r = a.orq("encerrar", "ctx_w1", "--motivo", "  ")
        assert r.returncode == 1 and "motivo" in r.stderr and not _log(a, "stopped.log")


def test_it_should_only_release_when_the_dispatch_already_finished():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo, status="completed", terminal="active")
        r = a.orq("encerrar", "ctx_w1", "--motivo", "entregue e esquecido")
        assert r.returncode == 0, r.stderr
        assert not _log(a, "stopped.log") and _log(a, "released.log"), "dispatch que já terminou não leva worker-stop"


def test_it_should_leave_nothing_stopped_when_worker_stop_fails():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        r = a.orq("encerrar", "ctx_w1", "--motivo", "x", FAKE_FAIL="worker-stop")
        assert r.returncode == 1, r
        assert not _log(a, "released.log"), "sem parar não há release"
        assert [e["resultado"] for e in _ctl_eventos(a, "encerrar")] == ["iniciado", "falhou"]
        assert _ctl_eventos(a, "encerrar")[-1]["passo"] == "worker-stop"


def test_it_should_report_a_partial_encerrar_when_the_release_fails_and_keep_the_worktree():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        antes = _head(repo)
        r = a.orq("encerrar", "ctx_w1", "--motivo", "x", FAKE_FAIL="worker-release")
        assert r.returncode == 1 and "orq liberar ctx_w1" in r.stderr, r.stderr
        ev = _ctl_eventos(a, "encerrar")[-1]
        assert ev["resultado"] == "parcial" and ev["passo"] == "release" and ev["worktree_intacta"] is True, ev
        assert os.path.isdir(repo) and _head(repo) == antes


def test_it_should_relaunch_in_the_same_worktree_and_task_with_retry_of_and_the_note():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t, sujo=True)
        a = Amb(run="run_a")
        _ctl_env(a, repo, effort="medium")
        antes = _head(repo)
        r = a.orq("relancar", "ctx_w1", "--nota", "o índice novo já existe: pule a migração")
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)
        assert out["novo_dispatch"] == "ctx_term_novo2" and out["resultado"] == "ok", out
        (parar,) = _log(a, "stopped.log")
        assert parar[:3] == ["worker-stop", "--dispatch", "ctx_w1"]
        (subir,) = _log(a, "started.log")
        assert subir[:1] == ["worker-start"] and subir[subir.index("--task") + 1] == "task_w1", subir
        assert subir[subir.index("--retry-of") + 1] == "ctx_w1" and subir[subir.index("--worktree") + 1] == "id:repo_1::" + repo, subir
        assert (subir[subir.index("--model") + 1], subir[subir.index("--effort") + 1], subir[subir.index("--agent") + 1]) == ("claude-sonnet-5-5", "medium", "claude"), "mantém o perfil do worker antigo"
        assert "--spec" not in subir, "a task já existe no Orca: o spec não muda"
        (nota,) = _enviados(a)
        assert nota[nota.index("--to") + 1] == "dispatch:ctx_term_novo2" and "pule a migração" in nota[nota.index("--body") + 1], nota
        (ev,) = [e for e in _ctl_eventos(a, "relancar") if e["resultado"] == "ok"]
        assert (ev["dispatch"], ev["novo_dispatch"], ev["task"], ev["nota"]) == ("ctx_w1", "ctx_term_novo2", "task_w1", "o índice novo já existe: pule a migração"), ev
        assert ev["worktree_intacta"] is True and _head(repo) == antes and os.path.exists(os.path.join(repo, "novo"))
        assert _log(a, "released.log"), "o terminal do worker antigo é liberado depois que o novo sobe"


def test_it_should_refuse_a_relaunch_without_a_note():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        assert a.orq("relancar", "ctx_w1").returncode == 2
        r = a.orq("relancar", "ctx_w1", "--nota", " ")
        assert r.returncode == 1 and "nota" in r.stderr and not _log(a, "stopped.log")


def test_it_should_refuse_before_stopping_when_the_worktree_is_gone():
    with tempfile.TemporaryDirectory() as t:
        a = Amb(run="run_a")
        _ctl_env(a, os.path.join(t, "sumiu"))
        r = a.orq("relancar", "ctx_w1", "--nota", "x")
        assert r.returncode == 1 and "worktree" in r.stderr, r
        assert not _log(a, "stopped.log") and not _log(a, "started.log"), "a recusa vem antes de parar o worker"
        assert not _ctl_eventos(a)


def test_it_should_fall_back_to_the_old_profile_when_the_requested_one_does_not_start():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        r = a.orq("relancar", "ctx_w1", "--nota", "x", "--modelo", "claude-inexistente", "--effort", "max", FAKE_FAIL_START_MODEL="claude-inexistente")
        assert r.returncode == 0, r.stderr
        tentativas = _log(a, "started.log")
        assert [x[x.index("--model") + 1] for x in tentativas] == ["claude-inexistente", "claude-sonnet-5-5"], tentativas
        assert tentativas[1][tentativas[1].index("--effort") + 1] == "high", "a volta usa o effort de antes, não o pedido"
        evs = [e for e in _ctl_eventos(a, "relancar")]
        assert evs[-1]["resultado"] == "revertido" and evs[-1]["novo_dispatch"] == "ctx_term_novo2" and "claude-inexistente" in evs[-1]["erro"], evs[-1]
        assert "claude-inexistente" in r.stderr, "o usuário fica sabendo que o perfil pedido não subiu"


def test_it_should_keep_the_worktree_and_the_note_when_nothing_starts():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t, sujo=True)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        antes = _head(repo)
        r = a.orq("relancar", "ctx_w1", "--nota", "use a branch nova", FAKE_FAIL="worker-start")
        assert r.returncode == 1, r
        assert "orq relancar ctx_w1 --nota" in r.stderr and "use a branch nova" in r.stderr, "a mensagem traz o comando para repetir, com a nota"
        ev = _ctl_eventos(a, "relancar")[-1]
        assert ev["resultado"] == "falhou" and ev["passo"] == "worker-start" and ev["nota"] == "use a branch nova" and ev["worktree_intacta"] is True, ev
        assert not _log(a, "released.log"), "o terminal do worker antigo fica retido para inspeção"
        assert os.path.isdir(repo) and _head(repo) == antes and os.path.exists(os.path.join(repo, "novo"))
        r2 = a.orq("relancar", "ctx_w1", "--nota", "use a branch nova")  # o dispatch parado (failed) sobe de novo sem novo worker-stop
        assert r2.returncode == 0, r2.stderr
        assert len(_log(a, "stopped.log")) == 1 and [c[0] for c in _log(a, "calls.log")].count("worker-start") == 2


def test_it_should_show_the_control_history_in_orq_agentes():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        assert a.orq("interromper", "ctx_w1").returncode == 0
        assert a.orq("relancar", "ctx_w1", "--nota", "pule a migração").returncode == 0
        ags = {x["dispatch"]: x for x in json.loads(a.orq("agentes", "--json", "--todos").stdout)}
        assert [c["acao"] for c in ags["ctx_w1"]["controle"]] == ["interromper", "relancar"], ags["ctx_w1"]
        assert ags["ctx_term_novo2"]["controle"] == [c for c in ags["ctx_w1"]["controle"] if c["acao"] == "relancar"], "o novo dispatch cita o relançamento"
        texto = a.orq("agentes", "--todos").stdout
        assert "controle: interromper ok" in texto and "relancar ok" in texto and "ctx_w1" in texto, texto


def test_it_should_not_list_control_lines_for_dispatches_without_history():
    ags = orq_mod.monta_agentes([{"dispatchId": "ctx_x", "taskId": "task_x", "dispatchStatus": "dispatched", "agentTerminalHandle": "term_x"}],
                                [], [], datetime.now(timezone.utc))
    assert "controle" not in ags[0] and "controle:" not in orq_mod.texto_agentes(ags)


# ---------- review-8 (M14 a M19) ----------

def test_review8_m14_guard_com_gerente_ve_o_despacho_de_run_do_proprio_coordenador():
    a = Amb(run="run_a")
    _gerente(a)  # o gerente term_ger segura run_a; o coordenador term_coord criou run_b com run-create cru e o segura sozinho
    json.dump({}, open(os.path.join(a.home, "cursor.json"), "w"))  # o guard é o do despacho ativo, não o do modo ausente (ticket 126): sem away
    a.set("runs.json", [{"id": "run_a", "coordinator_handle": "term_ger"}, {"id": "run_b", "coordinator_handle": "term_coord"}])
    a.set("terminals.json", ["term_ger", "term_coord"])
    _workers(a, ("w_b", "run_b", "dispatched"))
    out = json.loads(_guard(a).stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny" and "run_b" in out["permissionDecisionReason"], "o Run é deste coordenador, mesmo fora do gerente"
    a.set("runs.json", [{"id": "run_a", "coordinator_handle": "term_ger"}, {"id": "run_b", "coordinator_handle": "term_outro"}])
    a.set("terminals.json", ["term_ger", "term_coord", "term_outro"])
    a.set("ativos.json", None)
    os.remove(os.path.join(a.home, "ativos.json"))
    assert _guard(a).stdout == "", "o Run de outro terminal vivo continua fora"


def _run_fora_do_gerente(a):
    """Gerente (term_ger) no run_a e o coordenador (term_coord) ligado ao run_b, com um worker rodando e um entregue em run_b."""
    _multi(a, {"run_a": "term_ger", "run_b": "term_coord"}, ["run_a"])
    a.set("tasks_run_b.json", [{"id": "t2", "status": "dispatched", "dispatch_id": "ctx_1"}])
    a.set("workers.json", [{"handle": "term_w1", "run": "run_b", "task": "t2", "dispatch": "ctx_1", "status": "dispatched"},
                           {"handle": "term_w2", "run": "run_b", "task": "t3", "dispatch": "ctx_2", "status": "completed"}])
    a.set("terminals.json", ["term_ger", "term_coord", "term_w1", "term_w2"])
    a.prompt("oi")


def test_review8_m15_a_steer_num_run_que_o_coordenador_segura_fora_do_gerente_funciona():
    a = Amb()
    _run_fora_do_gerente(a)
    r = a.orq("steer", "t2", "ajuste", "--run", "run_b")
    assert r.returncode == 0, r.stderr
    (env,) = _log(a, "sent.log")
    assert env[env.index("--run") + 1] == "run_b", env
    assert _binds(a) == {"run_a": "term_ger", "run_b": "term_coord"}, "o gerente não foi tirado do Run dele para falar do run_b"


def test_review8_m15_a_liberar_num_run_que_o_coordenador_segura_fora_do_gerente_funciona():
    a = Amb()
    _run_fora_do_gerente(a)
    r = a.orq("liberar", "ctx_2", "--run", "run_b")
    assert r.returncode == 0, r.stderr
    (rel,) = _log(a, "released.log")
    assert rel[rel.index("--dispatch") + 1] == "ctx_2"


def test_review8_m15_a_responder_num_run_que_o_coordenador_segura_fora_do_gerente_funciona():
    a = Amb()
    _run_fora_do_gerente(a)
    _inbox(a, _pergunta_ask(50, "ctx_q", run="run_b"))
    r = a.orq("responder", "msg_q50", "sim")
    assert r.returncode == 0, r.stderr
    assert len(_log(a, "replied.log")) == 1


def test_review8_m15_a_run_solto_pede_o_gerente_ligar_e_nao_o_run_use():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a"])  # run_b saiu do gerente (gerente_soltar) e ninguém o segura
    a.set("tasks_run_b.json", [{"id": "t2", "status": "dispatched", "dispatch_id": "ctx_1"}])
    a.set("workers.json", [{"handle": "term_w1", "run": "run_b", "task": "t2", "dispatch": "ctx_1", "status": "dispatched"},
                           {"handle": "term_w2", "run": "run_b", "task": "t3", "dispatch": "ctx_2", "status": "completed"}])
    a.prompt("oi")
    for r in (a.orq("steer", "t2", "ajuste", "--run", "run_b"), a.orq("liberar", "ctx_2", "--run", "run_b")):
        assert r.returncode == 1 and "orq gerente ligar --terminal term_ger --run run_b" in r.stderr and "run-use" not in r.stderr, r.stderr
    assert not _log(a, "sent.log") and not _log(a, "released.log")
    assert a.orq("gerente", "ligar", "--terminal", "term_ger", "--run", "run_b").returncode == 0  # o que a mensagem manda fazer resolve
    assert a.orq("steer", "t2", "ajuste", "--run", "run_b").returncode == 0


def test_review8_m15_b_sem_run_e_com_o_gerente_em_dois_runs_recusa_em_vez_de_sortear():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.set("tasks_run_a.json", [{"id": "t1", "status": "dispatched", "dispatch_id": "ctx_1"}])
    a.set("workers.json", [{"handle": "term_w1", "run": "run_a", "task": "t1", "dispatch": "ctx_1", "status": "dispatched"}])
    a.prompt("oi")
    r = a.orq("steer", "t1", "ajuste")
    assert r.returncode == 1 and "passe --run" in r.stderr and "run_a" in r.stderr and "run_b" in r.stderr, r.stderr
    assert not _log(a, "sent.log"), "o painel estar no run_a não pode decidir o alvo"
    r = _novo(a)
    assert r.returncode == 1 and "passe --run" in r.stderr and not _log(a, "created.log"), r.stderr
    assert a.orq("steer", "t1", "ajuste", "--run", "run_a").returncode == 0


def _gate_no_run_b(a):
    """Gerente nos dois Runs, painel parado no run_a; a decisão trava a task_b1 do run_b."""
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.prompt("oi")
    r = a.orq("pend", "add", "--id", "gate-dec", "--tipo", "decisao", "--titulo", "Sobe?", "--task", "task_b1", "--run", "run_b")
    assert r.returncode == 0, r.stderr
    a.set("binds.json", {"run_a": {"handle": "term_ger", "gen": 9}, "run_b": {"handle": None, "gen": 9}})  # o painel voltou ao run_a


def test_review8_m15_c_gate_nasce_no_run_da_task_e_resolve_com_o_painel_em_outro_run():
    a = Amb()
    _gate_no_run_b(a)
    assert json.load(open(os.path.join(a.fake, "gates_map.json"))) == {"gate_1": "run_b"}, "gate-create com o Run da task, não o em que o painel estava"
    (item,) = [e for e in a.events() if e["tipo"] == "pend" and e["op"] == "add"]
    assert item["gate_run"] == "run_b", item
    r = a.orq("pend", "done", "gate-dec", "--resposta", "Sim")
    assert r.returncode == 0 and "aviso" not in json.loads(r.stdout), r
    assert _resolucoes(a) == ["Sim"] and [e["gate"] for e in a.events() if e["tipo"] == "gate_resolvido"] == ["gate_1"]


def test_review8_m15_c_reconciliar_gates_resolve_no_run_certo_sem_gastar_as_tentativas():
    a = Amb()
    _gate_no_run_b(a)
    a.orq("pend", "done", "gate-dec", "--resposta", "Sim", FAKE_FAIL="gate-resolve")  # Orca fora do ar: fica sem gate_resolvido
    assert not _resolucoes(a)
    assert a.orq("ingest").returncode == 0
    assert _resolucoes(a) == ["Sim"], "o painel em outro Run não pode deixar o gate para sempre"


def test_review8_m15_c_gate_com_o_gerente_em_dois_runs_pede_o_run():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.prompt("oi")
    r = a.orq("pend", "add", "--id", "gate-dec", "--tipo", "decisao", "--titulo", "Sobe?", "--task", "task_b1")
    assert r.returncode == 1 and "passe --run" in r.stderr and not _gates_log(a), r.stderr


def test_review8_m16_espera_declarada_com_o_turno_encerrado_continua_rodando_e_vencida_e_travado():
    agora = datetime.now(timezone.utc)
    ws = [{"dispatchId": "ctx_1", "taskId": "task_1", "runId": "run_a", "dispatchStatus": "dispatched"}]
    det = {"ctx_1": {"agente": "claude", "desde": _iso(-3600).replace(" ", "T") + "Z"}}
    turnos = {"ctx_1": {"task": "task_1", "inicio": now_iso(-700), "fim": now_iso(-580)}}  # encerrou o turno esperando, 10 min atrás

    def msgs(fase):
        return [{"id": "m1", "type": "heartbeat", "payload": json.dumps({"taskId": "task_1", "dispatchId": "ctx_1", "phase": fase}), "created_at": _iso(-600)}]
    ag = orq_mod.monta_agentes(ws, msgs(f"esperando: fila de E2E até {_hhmm_25(50)}"), [], agora, det, turnos=turnos)[0]
    assert ag["estado"] == "rodando" and ag["espera"] == "fila de E2E" and ag["turno"] == "parado", ag
    ab = {"agentes": [ag]}
    assert orq_mod.reavalia(ab["agentes"], [], agora, turnos)[0]["estado"] == "rodando"
    assert "parado no prompt" not in orq_mod.linha_vivos([], ab, agora, turnos)
    ag = orq_mod.monta_agentes(ws, msgs(f"esperando: fila de E2E até {_hhmm_25(-5)}"), [], agora, det, turnos=turnos)[0]
    assert ag["estado"] == "travado" and ag["motivo"] == "espera vencida", ag
    assert orq_mod.reavalia([ag], [], agora, turnos)[0]["estado"] == "travado"
    ag = orq_mod.monta_agentes(ws, msgs("compilando"), [], agora, det, turnos=turnos)[0]
    assert ag["estado"] == "parado", "sem espera declarada o turno encerrado continua parado"


def test_review8_m17_resumo_sem_desde_nao_conta_o_pedido_do_proprio_resumo():
    a = _amb_resumo()
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"ts": now_iso(-5), "tipo": "entrada", "id": "e5", "origem": "usuario", "texto": "me dá o resumo do que rolou"}) + "\n")
    out = a.orq("resumo").stdout
    assert "Com você (1)" in out and "e2" in out and "tarefa task_f1" in out and "e4" in out and "pend revisar-pr" in out, out
    assert "e1" not in out and "Decisões (3)" in out and "freio-prod: liberado" in out, out
    assert "Anda (1)" in out and "Corrigir o filtro de marca" in out, out


def test_review8_m17_anda_mostra_o_worker_perguntando_e_o_travado():
    ab = {"agentes": [{"dispatch": "ctx_9", "task": "t9", "titulo": "Worker perguntando", "estado": "perguntando"},
                      {"dispatch": "ctx_8", "task": "t8", "titulo": "Worker rodando", "estado": "rodando", "fase": "testando"},
                      {"dispatch": "ctx_7", "task": "t7", "titulo": "Worker entregue", "estado": "entregue"}]}
    out = orq_mod.resumo_quatro([], ab, {"itens": []}, [], agora=datetime.now(timezone.utc))
    assert "Anda (2)" in out and "Worker perguntando [perguntando]" in out and "Worker rodando [testando]" in out and "entregue" not in out, out


def test_review8_m18_lugar_reconhece_prefixos_antes_do_git():
    a = Amb(run="run_a")
    a.prompt("oi")
    p, _ = _repo(a.tmp.name, ramo="feat/outra")
    for cmd in ("git commit -m x", "cd /x && git push", "git -C /r commit", "rtk git commit -m x", "GIT_EDITOR=true git commit --amend",
                "env git push", "command git push", "time git commit", "sudo git push"):
        assert "lugar errado" in _aviso(_lugar(a, p, cmd=cmd)), cmd
    for cmd in ("rtk git status", "echo git commit", "git log --oneline", "GIT_EDITOR=true git rebase --continue"):
        assert _aviso(_lugar(a, p, cmd=cmd)) == "", cmd


def test_review8_m18_lugar_olha_o_dash_c_e_nao_so_o_cwd():
    a = Amb(run="run_a")
    a.prompt("oi")
    p, w = _repo(a.tmp.name)  # o cwd é o checkout principal, limpo, em main
    assert _aviso(_lugar(a, p, CLAUDE_PROJECT_DIR=p)) == ""
    msg = _aviso(_lugar(a, p, cmd=f"git -C {w} commit -m x", CLAUDE_PROJECT_DIR=p))
    assert "lugar errado" in msg and os.path.realpath(w) in msg, msg


def test_review8_m18_lugar_usa_a_branch_padrao_do_origin_head():
    a = Amb(run="run_a")
    a.prompt("oi")
    p, _ = _repo(a.tmp.name, ramo="develop")
    assert "lugar errado" in _aviso(_lugar(a, p)), "sem origin/HEAD a padrão é main"
    subprocess.run(["git", "-C", p, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/develop"], check=True, capture_output=True)
    assert _aviso(_lugar(a, p)) == "", "develop é a branch padrão deste repositório: não é engano"


def _painel_tocado(a, segundos_atras=None):
    p = os.path.join(a.home, orq_mod.PAINEL_VIVO)
    if segundos_atras is not None:
        open(p, "w").close()
        os.utime(p, (time.time() - segundos_atras,) * 2)


def test_review8_m19_painel_parado_aparece_no_prompt_no_status_e_no_resumo():
    a = Amb(run="run_a")
    _gerente(a)
    a.set("terminals.json", ["term_coord", "term_ger"])  # o terminal do gerente existe: só o painel parou
    for saida in (json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"], a.orq("status").stdout, a.orq("resumo").stdout):
        assert "painel do agent manager sem carimbo" in saida, saida
    _painel_tocado(a, 200)
    for saida in (json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"], a.orq("status").stdout, a.orq("resumo").stdout):
        assert "painel do agent manager parado há 3 min" in saida, saida
    _painel_tocado(a, 20)
    for saida in (json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"], a.orq("status").stdout, a.orq("resumo").stdout):
        assert "painel do agent manager" not in saida, "carimbo de 20 s: o painel está vivo"


def test_review8_m19_sem_gerente_ou_de_outro_coordenador_nao_avisa_do_painel():
    a = Amb(run="run_a")
    assert "painel do agent manager" not in a.orq("status").stdout
    b = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_outro")
    _gerente(b)
    assert "painel do agent manager" not in b.orq("status").stdout, "o gerente.json só vale para o coordenador que o ligou"


def test_review8_m19_o_painel_toca_o_carimbo_antes_do_orq_e_com_o_orq_quebrado():
    with tempfile.TemporaryDirectory() as t:
        bin_ = os.path.join(t, "bin")
        os.makedirs(bin_)
        for nome, corpo in (("orq", "exit 1"), ("clear", ":"), ("sleep", "kill $PPID")):  # orq quebrado; o sleep mata o laço na primeira volta
            with open(os.path.join(bin_, nome), "w") as f:
                f.write("#!/bin/sh\n" + corpo + "\n")
            os.chmod(os.path.join(bin_, nome), 0o755)
        home = os.path.join(t, "orq")
        os.makedirs(home)
        subprocess.run(["sh", os.path.join(AQUI, "painel-agent-manager.sh")], env={**os.environ, "PATH": bin_ + os.pathsep + os.environ["PATH"], "ORQ_HOME": home},
                       capture_output=True, timeout=20)
        assert os.path.exists(os.path.join(home, orq_mod.PAINEL_VIVO)), "o carimbo sai do shell do painel, sem depender do orq"


def test_review8_m19_o_spec_de_worker_do_orq_manda_worktree_propria():
    skill = open(os.path.join(AQUI, "skills", "worker-routing", "SKILL.md")).read()
    assert "git worktree add" in skill and "git pull" in skill and "~/.claude/orq" in skill


# ---------- review-9 (B49 a B53 e o filtro de Runs de teste) ----------

def _sob_trava_falsa(vistos):
    """Troca a trava_gerente por uma que só conta a profundidade; devolve o que restaurar."""
    import contextlib
    orig = orq_mod.trava_gerente
    prof = [0]

    @contextlib.contextmanager
    def trava():
        prof[0] += 1
        try:
            yield
        finally:
            prof[0] -= 1
    orq_mod.trava_gerente = trava
    return orig, prof


def test_review9_b49_reconciliar_gates_confere_e_resolve_sob_a_trava_do_gerente():
    vistos = []
    orig, prof = _sob_trava_falsa(vistos)
    velhos = (orq_mod.read_events, orq_mod.run_do_coordenador, orq_mod._resolve_gate)
    try:
        orq_mod.read_events = lambda: [{"tipo": "pend", "op": "done", "gate": "g1", "gate_run": "run_b", "resposta": "x"}]
        orq_mod.run_do_coordenador = lambda run, proprio=None: vistos.append(("conferiu", prof[0])) or True
        orq_mod._resolve_gate = lambda g, r, run=None: vistos.append(("resolveu", prof[0])) or True
        orq_mod.reconciliar_gates()
    finally:
        orq_mod.trava_gerente = orig
        orq_mod.read_events, orq_mod.run_do_coordenador, orq_mod._resolve_gate = velhos
    assert vistos == [("conferiu", 1), ("resolveu", 1)], vistos


def test_review9_b49_pend_done_confere_e_resolve_o_gate_sob_a_trava_do_gerente():
    vistos = []
    orig, prof = _sob_trava_falsa(vistos)
    velhos = (orq_mod.read_events, orq_mod._mutar_pend, orq_mod.run_do_coordenador, orq_mod._resolve_gate)
    try:
        orq_mod.read_events = lambda: []
        orq_mod._mutar_pend = lambda rm, ev: {"id": "d", "gate": "g1", "gate_run": "run_b"}
        orq_mod.run_do_coordenador = lambda run, proprio=None: vistos.append(("conferiu", prof[0])) or True
        orq_mod._resolve_gate = lambda g, r, run=None: vistos.append(("resolveu", prof[0])) or True
        orq_mod.pend_done("d", "sim")
    finally:
        orq_mod.trava_gerente = orig
        orq_mod.read_events, orq_mod._mutar_pend, orq_mod.run_do_coordenador, orq_mod._resolve_gate = velhos
    assert vistos == [("conferiu", 1), ("resolveu", 1)], vistos


def test_review9_b49_anda_mostra_travado_parado_e_nao_comecou():
    agora = datetime.now(timezone.utc)

    def z(s):
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(agora.timestamp() - s))
    base = {"agente": "claude", "estado": "rodando", "titulo": "W"}
    ags = [{**base, "dispatch": "ctx_t", "task": "t1", "desde": z(3600), "ultimo_heartbeat": z(3000)},
           {**base, "dispatch": "ctx_p", "task": "t2", "desde": z(3600), "ultimo_heartbeat": z(1200), "turno_inicio": z(1100), "turno_fim": z(900)},
           {**base, "dispatch": "ctx_n", "task": "t3", "desde": z(900)}]
    assert sorted(a["estado"] for a in orq_mod.reavalia(ags, [], agora)) == ["nao_comecou", "parado", "travado"]
    out = orq_mod.resumo_quatro([], {"agentes": ags}, {"itens": []}, [], agora=agora)
    assert "Anda (3)" in out, out


def test_review9_b49_lavish_resposta_fecha_o_gate_de_um_run_que_o_coordenador_segura_fora_do_gerente():
    a = Amb()
    _run_fora_do_gerente(a)
    r = a.orq("pend", "add", "--id", "gate-dec", "--tipo", "decisao", "--titulo", "Sobe?", "--task", "task_b1", "--run", "run_b")
    assert r.returncode == 0, r.stderr
    r = a.orq("lavish-resposta", _lote(a, [{"id": "L1", "header": "gate-dec", "resposta": "Sim", "disposicao": "escolha"}]))
    assert r.returncode == 0 and "run-use" not in r.stderr and "gerente ligar" not in r.stderr, r.stderr
    assert _resolucoes(a) == ["Sim"], "o Run próprio é conferido pelo handle do coordenador, não pelo run-current do painel"


def test_review9_b50_sem_run_com_o_gerente_num_run_e_o_coordenador_noutro_pede_o_run():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_b": "term_coord"}, ["run_a"])
    a.prompt("oi")
    r = _novo(a)
    assert r.returncode == 1 and "passe --run" in r.stderr and "run_a" in r.stderr and "run_b" in r.stderr and not _log(a, "created.log"), r.stderr
    assert _novo(a, "Ticket de teste", "--run", "run_b").returncode == 0


def test_review9_b50_o_run_proprio_igual_ao_do_gerente_nao_e_ambiguo():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a"])
    a.prompt("oi")
    assert _novo(a).returncode == 0, "o coordenador não segura Run fora do gerente: o do gerente é o alvo"


def test_review9_b51_pend_add_task_num_run_que_o_coordenador_nao_comanda_recusa():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_b": None, "run_c": "term_outro"}, ["run_a"])
    a.prompt("oi")
    r = a.orq("pend", "add", "--id", "gate-dec", "--tipo", "decisao", "--titulo", "Sobe?", "--task", "task_c1", "--run", "run_c")
    assert r.returncode == 1 and "run_c" in r.stderr and not _gates_log(a), r.stderr
    assert "gate-dec" not in _ids_pend(a)


def test_review9_b52_heartbeat_de_um_run_do_coordenador_fora_do_gerente_e_absorvido_e_confirmado():
    a = Amb()
    _run_fora_do_gerente(a)
    a.caixa(_hb("lendo"), run="run_b")
    _inbox(a, _hb_inbox(a, "ctx_9", "fase-4", -5, 900))
    r = a.prompt(AVISO_B)
    assert _bloqueado(r) and "absorvidos" in json.loads(r.stdout)["reason"], r.stdout
    assert a.estados() == {"msg_1": "acked"}, "o check confirmou a entrega: o aviso de Run próprio não cai no bloqueio sem confirmar"


def test_review9_b52_hook_ask_resolve_o_gate_de_um_run_do_coordenador_fora_do_gerente():
    a = Amb()
    _run_fora_do_gerente(a)
    a.orq("pend", "add", "--id", "gate-dec", "--tipo", "decisao", "--titulo", "Sobe?", "--task", "task_b1", "--run", "run_b")
    q = [_pergunta("gate-dec", [("Sim", "x"), ("Não", "y")])]
    r = a.orq("hook", "ask", stdin=_ask(q, {q[0]["question"]: "Sim"}))
    assert r.returncode == 0 and "run-use" not in r.stdout and "gerente ligar" not in r.stdout, r
    assert _resolucoes(a) == ["Sim"], "a resposta resolve o gate na hora, sem esperar o próximo ingest"


def test_review9_b52_waiter_vigia_o_run_do_coordenador_fora_do_gerente():
    a = Amb()
    _run_fora_do_gerente(a)
    a.caixa(("question", {"taskId": "t2", "dispatchId": "ctx_1"}), run="run_b")
    out = json.loads(_waiter(a, "run_b").stdout)
    assert out.get("run") == "run_b" and [m["type"] for m in out["messages"]] == ["question"], out
    assert a.estados() == {"msg_1": "out"}, "veio pelo check do Run (entrega aberta), não por leitura do inbox"


def test_review9_b53_lugar_reconhece_rtk_proxy_git():
    a = Amb(run="run_a")
    a.prompt("oi")
    p, _ = _repo(a.tmp.name, ramo="feat/outra")
    for cmd in ("rtk proxy git commit -m x", "rtk proxy git push"):
        assert "lugar errado" in _aviso(_lugar(a, p, cmd=cmd)), cmd
    assert _aviso(_lugar(a, p, cmd="rtk proxy git status")) == ""


def test_review9_resumo_nao_conta_backlog_nem_bloqueado_de_run_de_teste():
    agora = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)

    def t(id, status, titulo=None):
        return {"id": id, "status": status, "created_at": "2026-09-29 11:00:00", "deps": "[]", "task_title": titulo or id}
    real = {"id": "run_1", "objective": "Frente A"}
    prova = {"id": "run_2", "objective": "Prova r6"}  # Run real com task de teste dentro
    de_teste = {"id": "run_3", "objective": "[teste] review-8 gate 1"}
    ab = orq_mod.monta_aberto([(real, [t("a", "ready"), t("b", "blocked")], []),
                               (prova, [t("c", "ready", "[teste] review-8 gate 1"), t("d", "blocked", "[teste] review-8 gate 2"), t("g", "ready", "Ticket 9")], []),
                               (de_teste, [t("e", "pending"), t("f", "blocked")], [])], agora)
    assert sorted(b["id"] for b in ab["backlog"]) == ["a", "g"] and [b["id"] for b in ab["bloqueado"]] == ["b"], ab


# ---- orq noite ----

def _noite(a, ate_h=2, **extra):
    """Liga o modo noite no cursor.json do ambiente; ate_h = horas a partir de agora (negativo: já passou)."""
    ate = (datetime.now(timezone.utc) + __import__("datetime").timedelta(hours=ate_h)).strftime("%Y-%m-%dT%H:%M:%SZ")
    os.makedirs(a.home, exist_ok=True)
    json.dump({"noite": {"ate": ate, "ligada_em": "2026-01-01T00:00:00Z", "max_despachos": None, "max_falhas": 3, **extra}}, open(os.path.join(a.home, "cursor.json"), "w"))


def _desp_ev(a, dispatch):
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"ts": "2026-09-30T01:00:00Z", "tipo": "despacho", "run": "run_a", "task": "t_" + dispatch, "dispatch": dispatch}) + "\n")


def _wd(a, *itens):
    """Põe worker_done no inbox falso: (dispatch, outcome, com_relatorio)."""
    ms = [{"id": f"m{i}", "run_id": "run_a", "sequence": i, "type": "worker_done", "subject": "s", "created_at": "2026-09-30T02:00:00Z",
           "payload": json.dumps({"dispatchId": d, "taskId": "t_" + d, "outcome": o, **({"reportPath": "/r.md"} if r else {})})} for i, (d, o, r) in enumerate(itens, 1)]
    a.set("inbox.json", {"ok": True, "result": {"messages": ms}})


def test_noite_despachar_recusado_depois_do_horario():
    a = Amb(run="run_a")
    _noite(a, ate_h=-1)
    r = _despachar(a)
    assert r.returncode != 0 and "horário" in r.stderr, r.stderr
    assert not _log(a, "started.log"), "não pode ter subido worker"
    assert [e["motivo"] for e in a.events() if e["tipo"] == "noite_parou"][0].startswith("passou do horário")


def test_noite_despachar_recusado_no_teto_de_despachos():
    a = Amb(run="run_a")
    _noite(a, max_despachos=2)
    _desp_ev(a, "d1"), _desp_ev(a, "d2")
    r = _despachar(a)
    assert r.returncode != 0 and "teto de 2 despachos" in r.stderr, r.stderr
    _noite(a, max_despachos=3)
    assert _despachar(a).returncode == 0, "abaixo do teto despacha"


def test_noite_despachar_recusado_na_terceira_falha_seguida():
    a = Amb(run="run_a")
    _noite(a)
    for d in ("d1", "d2", "d3"):
        _desp_ev(a, d)
    _wd(a, ("d1", "failed", False), ("d2", "failed", False))
    assert _despachar(a).returncode == 0, "duas falhas ainda despacham"
    _wd(a, ("d1", "failed", False), ("d2", "failed", False), ("d3", "failed", False))
    r = _despachar(a)
    assert r.returncode != 0 and "3 falhas seguidas" in r.stderr, r.stderr


def test_noite_falha_reportada_reinicia_a_contagem():
    f = orq_mod.falhas_seguidas
    noite = {"ligada_em": "2026-01-01T00:00:00Z"}
    evs = [{"ts": "2026-09-30T01:00:00Z", "tipo": "despacho", "dispatch": d} for d in ("d1", "d2", "d3", "d4")]
    wd = lambda d, o, rp=False: {"type": "worker_done", "payload": {"dispatchId": d, "outcome": o, **({"reportPath": "/r"} if rp else {})}}
    assert f(evs, [wd("d1", "failed"), wd("d2", "failed"), wd("d3", "failed"), wd("d4", "failed")], noite) == 4
    assert f(evs, [wd("d1", "failed"), wd("d2", "failed"), wd("d3", "failed", True), wd("d4", "failed")], noite) == 1, "failed com relatório é decisão do worker"
    assert f(evs, [wd("d1", "failed"), wd("d2", "succeeded"), wd("d3", "failed"), wd("d4", "failed")], noite) == 2, "succeeded reinicia"
    lib = evs + [{"ts": "2026-09-30T03:00:00Z", "tipo": "liberar", "dispatch": "d1"}]
    assert f(lib[:1] + lib[-1:], [], noite) == 1, "liberado sem worker_done conta"
    assert f(evs[:2], [], noite) == 0, "ainda rodando não conta"


def test_noite_desligar_libera():
    a = Amb(run="run_a")
    _noite(a, ate_h=-1)
    assert _despachar(a).returncode != 0
    r = a.orq("noite", "desligar")
    assert r.returncode == 0 and "desligado" in r.stdout, r.stderr
    assert _despachar(a).returncode == 0
    assert "modo noite desligado" in a.orq("noite").stdout


def test_noite_ligar_grava_evento_e_estado():
    a = Amb(run="run_a")
    r = a.orq("noite", "ligar", "--ate", "06:30", "--max-despachos", "5")
    assert r.returncode == 0, r.stderr
    n = json.load(open(os.path.join(a.home, "cursor.json")))["noite"]
    assert n["max_despachos"] == 5 and n["max_falhas"] == 3 and _hora_de(n["ate"]) == "06:30", n
    assert [e for e in a.events() if e["tipo"] == "noite_ligar"][0]["max_despachos"] == 5
    assert "Regras" in r.stdout and "0/5 despachos" in r.stdout, r.stdout
    assert a.orq("noite", "ligar", "--ate", "25:00").returncode != 0
    assert a.orq("noite", "ligar").returncode != 0


def _hora_de(ts):
    return orq_mod._dt(ts).astimezone().strftime("%H:%M")


def test_noite_motivo_aparece_uma_vez_em_status_e_em_agentes():
    a = Amb(run="run_a")
    _noite(a, ate_h=-1)
    _despachar(a)
    st = a.orq("status").stdout
    assert st.count("Parou de despachar") == 1 and "horário" in st, st
    ag = a.orq("agentes").stdout
    assert ag.count("Parou de despachar") == 1, ag


def test_noite_hooks_injetam_as_regras_e_desligado_nada_muda():
    a = Amb(run="run_a")
    assert "orq noite" not in a.prompt("oi").stdout and "orq noite" not in a.orq("status").stdout
    _noite(a)
    ctx = json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "[orq noite] Regras" in ctx and "AskUserQuestion" in ctx and "push" in ctx, ctx
    aviso = a.orq("hook", "prompt", stdin=json.dumps({"prompt": "You have 1 orchestration messages. Run `orca orchestration check`", "session_id": "abcdef123456"}))
    assert "[orq noite]" in aviso.stdout, "o aviso do Orca também leva as regras"
    t0 = time.time()
    orq_mod.linhas_noite(orq_mod._cursor_ro(), [])
    assert time.time() - t0 < 0.1


# ---- orq hook externas (modo noite) ----

EXTERNAS = ["git push", "git push --force origin x", "rtk git push -u origin feat/x", "git -C /tmp/r push", "cd x && git push", "gh pr merge 12 --squash",
            "gh workflow run deploy.yaml", "git commit --no-verify -m x", "git commit -n -m x", "git commit -anm x", "orca worktree rm --worktree x --force --run-hooks"]


def _externas(a, cmd, cwd=None, tool="Bash"):
    r = a.orq("hook", "externas", stdin=json.dumps({"tool_name": tool, "tool_input": {"command": cmd}, "session_id": "s1", "cwd": cwd or a.tmp.name}))
    return json.loads(r.stdout)["hookSpecificOutput"] if r.stdout.strip() else None


def test_noite_externas_nega_cada_comando_com_o_modo_ligado():
    a = Amb(run="run_a")
    _noite(a)
    for cmd in EXTERNAS:
        out = _externas(a, cmd)
        assert out and out["permissionDecision"] == "deny", cmd
        assert "orq pend add" in out["permissionDecisionReason"] and "orq noite desligar" in out["permissionDecisionReason"], cmd


def test_noite_externas_libera_com_o_modo_desligado():
    a = Amb(run="run_a")
    for cmd in EXTERNAS:
        assert _externas(a, cmd) is None, cmd
    _noite(a)
    a.orq("noite", "desligar")
    assert _externas(a, "git push") is None


def test_noite_externas_texto_entre_aspas_e_leitura_nao_disparam():
    a = Amb(run="run_a")
    _noite(a)
    for cmd in ['git commit -m "fix: never git push --force or gh pr merge"', "echo 'git push'", 'orq pend add "rodar gh workflow run depois"', "git status", "git log --oneline",
                "git pull", "git commit -m x", "git commit -am x", "gh pr view 12", "gh pr list", "gh workflow list", "orca worktree rm --worktree x --run-hooks",
                "git commit -F - <<'EOF'\nfeat: x\ngit push\nEOF", "git push-helper"]:
        assert _externas(a, cmd) is None, cmd
    assert _externas(a, "git push", tool="Edit") is None, "só Bash"


def test_noite_externas_reset_hard_so_fora_de_worktree_ligada():
    a = Amb(run="run_a")
    _noite(a)
    t = a.tmp.name
    main, wt = os.path.join(t, "main"), os.path.join(t, "wt")
    g = lambda *x: subprocess.run(["git", *x], cwd=main, check=True, capture_output=True, env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"})
    os.makedirs(main)
    g("init", "-q"), g("commit", "-q", "--allow-empty", "-m", "i"), g("worktree", "add", "-q", wt, "-b", "w")
    out = _externas(a, "git reset --hard HEAD~1", cwd=main)
    assert out and out["permissionDecision"] == "deny", "checkout principal"
    assert _externas(a, "git reset --hard HEAD~1", cwd=wt) is None, "worktree ligada (de worker)"
    out = _externas(a, f"git -C {main} reset --hard", cwd=wt)
    assert out and out["permissionDecision"] == "deny", "-C aponta para o checkout principal"
    assert _externas(a, "git reset --soft HEAD~1", cwd=main) is None


def test_noite_externas_hook_fica_abaixo_de_100_ms():
    a = Amb(run="run_a")
    _noite(a)
    ev = {"tool_name": "Bash", "tool_input": {"command": "git push"}, "session_id": "s1", "cwd": a.tmp.name}
    t0 = time.time()
    orq_mod._externas_negada(ev, orq_mod._cursor_ro())
    assert time.time() - t0 < 0.1


def _sem_git_env(a, **env):
    """Tira do ambiente do teste as variáveis de git que a sessão do desenvolvedor pode ter (GIT_CONFIG_*, GIT_TERMINAL_PROMPT)."""
    for k in [k for k in a.env if k.startswith("GIT_CONFIG_") or k == "GIT_TERMINAL_PROMPT"]:
        del a.env[k]
    a.env.update(env)


def test_noite_despachar_soma_ao_git_config_que_o_ambiente_ja_tem():
    a = Amb(run="run_a")
    _sem_git_env(a, GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="core.editor", GIT_CONFIG_VALUE_0="true")
    _noite(a)
    assert _despachar(a).returncode == 0
    env = _log(a, "started-env.log")[0]
    assert env["GIT_CONFIG_COUNT"] == "2" and env["GIT_CONFIG_KEY_0"] == "core.editor" and env["GIT_CONFIG_KEY_1"] == "commit.gpgsign" and env["GIT_CONFIG_VALUE_1"] == "false", env


def test_noite_despachar_passa_o_ambiente_sem_prompt_e_grava_no_evento():
    a = Amb(run="run_a")
    _sem_git_env(a)
    _noite(a)
    assert _despachar(a).returncode == 0
    env = _log(a, "started-env.log")[0]
    assert {k: v for k, v in env.items() if v} == {"GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "commit.gpgsign", "GIT_CONFIG_VALUE_0": "false"}, env
    assert [e for e in a.events() if e["tipo"] == "despacho"][0]["ambiente"] == ["GIT_TERMINAL_PROMPT", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"]


def test_despachar_fora_da_noite_nao_mexe_no_ambiente():
    a = Amb(run="run_a")
    _sem_git_env(a)
    assert _despachar(a).returncode == 0
    assert _log(a, "started-env.log")[0]["GIT_CONFIG_COUNT"] is None
    assert "ambiente" not in [e for e in a.events() if e["tipo"] == "despacho"][0]


# ---------- cartão da manhã e motivo de parada por dispatch (ticket 40) ----------

T0 = "2026-09-30T02:00:00Z"


def _ev(ts, tipo, **k):
    return {"ts": f"2026-09-30T{ts}Z", "tipo": tipo, **k}


def _noite_log(*extra, desligar="09:00:00", volta="08:58:00"):
    """Uma noite de 02:00 a 09:00 UTC com um despacho por motivo de parada; `extra` entra no fim do log (o log fica ordenado por ts)."""
    evs = [_ev("02:00:00", "noite_ligar", ate="2026-09-30T09:00:00Z")]
    motivos = [("d1", "entregue"), ("d2", "falhou"), ("d3", "parou: orçamento"), ("d4", "parou: decisão pendente"), ("d5", "parou: limite de uso"), ("d6", "sem worker_done")]
    for i, (d, m) in enumerate(motivos):
        evs.append(_ev(f"02:1{i}:00", "despacho", dispatch=d, task="t_" + d, run="run_a", titulo="Frente " + d))
        evs.append(_ev(f"03:1{i}:00", "fim_dispatch", dispatch=d, task="t_" + d, run="run_a", motivo=m, sujo=0, sem_push=0, caminho="/wt/" + d))
    evs += list(extra)
    if desligar:
        evs.append(_ev(desligar, "noite_desligar"))
    cur = {"noite": {"ate": "2026-09-30T09:00:00Z", "ligada_em": T0}, "gerente_volta": f"2026-09-30T{volta}Z"} if not desligar else {"gerente_volta": f"2026-09-30T{volta}Z"}
    return sorted(evs, key=lambda e: e["ts"]), cur


def _cartao(evs, cur, agora="2026-09-30T09:05:00Z", **k):
    return orq_mod.cartao_noite(evs, cur, {"itens": []}, orq_mod._dt(agora), **k)


def test_cartao_cada_motivo_de_parada_aparece_no_dispatch():
    evs, cur = _noite_log(_ev("05:00:00", "heartbeat_absorvido"), _ev("06:00:00", "heartbeat_absorvido"), _ev("07:00:00", "heartbeat_absorvido"), _ev("08:00:00", "heartbeat_absorvido"))
    txt = "\n".join(_cartao(evs, cur))
    for d, m in (("d1", "entregue"), ("d2", "falhou"), ("d3", "parou: orçamento"), ("d4", "parou: decisão pendente"), ("d5", "parou: limite de uso"), ("d6", "sem worker_done")):
        assert any(f"Frente {d}" in l and l.rstrip().endswith(m) for l in txt.splitlines()), (d, m, txt)


def test_cartao_worktree_suja_e_commit_sem_push_com_os_comandos_para_colar():
    evs, cur = _noite_log(_ev("08:00:00", "heartbeat_absorvido"))
    for e in evs:
        if e.get("dispatch") == "d2" and e["tipo"] == "fim_dispatch":
            e.update(sujo=3)
        if e.get("dispatch") == "d3" and e["tipo"] == "fim_dispatch":
            e.update(sem_push=2)
    linhas = _cartao(evs, cur)
    txt = "\n".join(linhas)
    assert "Worktrees sujas (1)" in txt and "Frente d2: 3 arquivos" in txt, txt
    assert "Sem push (1)" in txt and "Frente d3: 2 commits" in txt, txt
    assert "git -C /wt/d2 status --short" in txt and "git -C /wt/d3 log --oneline origin/main..HEAD" in txt, txt
    assert "/wt/d1" not in txt, "worktree limpa e sem commit pendente não vira comando"


def test_cartao_vivo_pega_o_estado_das_worktrees_dos_dispatches_sem_fim():
    evs, cur = _noite_log()
    evs.append(_ev("04:00:00", "despacho", dispatch="d7", task="t_d7", run="run_a", titulo="Frente d7"))
    vivos = {"d7": {"caminho": "/wt/d7", "sujo": 2, "sem_push": 0}}
    txt = "\n".join(_cartao(sorted(evs, key=lambda e: e["ts"]), cur, vivos=vivos))
    assert any("Frente d7" in l and l.rstrip().endswith("rodando") for l in txt.splitlines()), txt
    assert "Frente d7: 2 arquivos" in txt and "git -C /wt/d7 status --short" in txt, txt


def test_cartao_lacuna_maior_que_10_min_no_log():
    evs, cur = _noite_log()
    txt = "\n".join(_cartao(evs, cur))
    assert "a máquina pode ter dormido às " + _hora_de("2026-09-30T03:15:00Z") in txt, txt
    evs2, cur2 = _noite_log(*[_ev(f"{h:02d}:{m:02d}:00", "heartbeat_absorvido") for h in range(2, 9) for m in range(0, 60, 5) if (h, m) >= (2, 20)])
    assert "dormido" not in "\n".join(_cartao(evs2, cur2)), "passos de 5 min não são lacuna"


def test_cartao_gerente_vivo_ou_parado():
    evs, cur = _noite_log(_ev("08:59:00", "heartbeat_absorvido"))
    assert "Gerente: vivo" in "\n".join(_cartao(evs, cur))
    evs, cur = _noite_log(volta="05:30:00")
    assert f"Gerente: parou às {_hora_de('2026-09-30T05:30:00Z')}" in "\n".join(_cartao(evs, cur))
    evs, cur = _noite_log()
    cur.pop("gerente_volta")
    assert "Gerente: sem rodada" in "\n".join(_cartao(evs, cur))


def test_cartao_decisoes_estacionadas_e_parada_do_orcamento():
    evs, cur = _noite_log(_ev("04:00:00", "pend", op="add", pend="freio-x"), _ev("04:05:00", "pend", op="add", pend="freio-y"), _ev("06:00:00", "pend", op="done", pend="freio-y"),
                          _ev("07:00:00", "noite_parou", motivo="teto de 6 despachos da noite"))
    txt = "\n".join(orq_mod.cartao_noite(evs, cur, {"itens": [{"id": "freio-x", "tipo": "decisao", "titulo": "Liberar o freio?"}]}, orq_mod._dt("2026-09-30T09:05:00Z")))
    assert "Decisões estacionadas (1)" in txt and "freio-x" in txt and "freio-y" not in txt, txt
    assert "Parou de despachar às " + _hora_de("2026-09-30T07:00:00Z") + ": teto de 6 despachos da noite" in txt, txt


def test_cartao_cabe_em_40_linhas_com_muitos_dispatches():
    evs, cur = _noite_log()
    for i in range(30):
        d = f"x{i:02d}"
        evs.append(_ev("04:00:00", "despacho", dispatch=d, task="t_" + d, run="run_a", titulo="Frente " + d))
        evs.append(_ev("04:30:00", "fim_dispatch", dispatch=d, task="t_" + d, run="run_a", motivo="entregue", sujo=1, sem_push=1, caminho="/wt/" + d))
    evs.sort(key=lambda e: e["ts"])
    linhas = _cartao(evs, cur)
    assert len(linhas) <= 40, len(linhas)
    assert "+" in "\n".join(linhas), "o que não coube vira +N"


def test_cartao_sem_noite_no_log_diz_que_nao_houve():
    assert _cartao([_ev("01:00:00", "entrada")], {}) == ["Nenhuma noite no log: rode orq noite ligar --ate HH:MM"]


def test_cartao_primeira_linha_no_session_start_so_ate_12_h_depois_do_fim():
    evs, cur = _noite_log()
    linha = orq_mod.cartao_primeira_linha(evs, orq_mod._dt("2026-09-30T10:00:00Z"))
    assert linha and "6 despachos" in linha and "1 entregue" in linha and "orq resumo --noite" in linha, linha
    assert orq_mod.cartao_primeira_linha(evs, orq_mod._dt("2026-09-30T21:01:00Z")) is None, "12 h depois do fim o cartão sai do SessionStart"
    assert orq_mod.cartao_primeira_linha([], orq_mod._dt("2026-09-30T10:00:00Z")) is None


def _fim(a):
    return [e for e in a.events() if e["tipo"] == "fim_dispatch"]


def test_liberar_grava_fim_dispatch_entregue_e_falhou():
    a = Amb(run="run_a")
    _lib_env(a, worktree="/tmp/nao-existe")
    a.set("inbox.json", {"ok": True, "result": {"messages": [{"id": "m1", "run_id": "run_a", "sequence": 1, "type": "worker_done", "subject": "s", "created_at": "2026-09-30T02:00:00Z",
                                                            "payload": json.dumps({"dispatchId": "ctx_term_w1", "taskId": "task_w1", "outcome": "succeeded"})}]}})
    assert a.orq("liberar", "ctx_term_w1").returncode == 0
    (f,) = _fim(a)
    assert (f["dispatch"], f["task"], f["run"], f["motivo"]) == ("ctx_term_w1", "task_w1", "run_a", "entregue"), f
    b = Amb(run="run_a")
    _lib_env(b)
    b.set("inbox.json", {"ok": True, "result": {"messages": [{"id": "m1", "run_id": "run_a", "sequence": 1, "type": "worker_done", "subject": "s", "created_at": "2026-09-30T02:00:00Z",
                                                            "payload": json.dumps({"dispatchId": "ctx_term_w1", "taskId": "task_w1", "outcome": "failed"})}]}})
    assert b.orq("liberar", "ctx_term_w1").returncode == 0
    assert _fim(b)[0]["motivo"] == "falhou"


def test_liberar_sem_worker_done_grava_sem_worker_done_e_o_estado_da_worktree(tmp_path=None):
    a = Amb(run="run_a")
    wt = os.path.join(a.tmp.name, "wt")
    subprocess.run(["git", "init", "-q", wt], check=True)
    open(os.path.join(wt, "sujo.txt"), "w").write("x")
    _lib_env(a, worktree=wt, sem_done=True)
    a.set("inbox.json", {"ok": True, "result": {"messages": []}})
    assert a.orq("liberar", "ctx_term_w1").returncode == 0
    (f,) = _fim(a)
    assert f["motivo"] == "sem worker_done" and f["sujo"] == 1 and f["caminho"] == wt and f["sem_push"] == 0, f


def test_liberar_inbox_indisponivel_nao_inventa_motivo():
    a = Amb(run="run_a")
    _lib_env(a)
    r = a.orq("liberar", "ctx_term_w1", FAKE_FAIL="inbox")
    assert r.returncode == 0, r.stderr
    (f,) = _fim(a)
    assert f["motivo"] == "motivo desconhecido", f


def test_encerrar_com_parada_marca_o_motivo_no_fim_dispatch():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_w2", "run": "run_a", "task": "task_w2", "status": "dispatched"}])
    a.set("terminals.json", ["term_w2", "term_coord"])
    a.set("inbox.json", {"ok": True, "result": {"messages": []}})
    r = a.orq("encerrar", "ctx_term_w2", "--motivo", "acabou o orçamento", "--parada", "orcamento")
    assert r.returncode == 0, r.stderr
    (f,) = _fim(a)
    assert f["motivo"] == "parou: orçamento", f
    assert a.orq("encerrar", "ctx_term_w2", "--motivo", "x", "--parada", "sono").returncode != 0


def test_resumo_noite_imprime_o_cartao_e_status_nao_muda():
    a = Amb(run="run_a")
    os.makedirs(a.home, exist_ok=True)
    evs, cur = _noite_log()
    with open(os.path.join(a.home, "events.jsonl"), "w") as f:
        f.writelines(json.dumps(e) + "\n" for e in evs)
    json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
    r = a.orq("resumo", "--noite")
    assert r.returncode == 0, r.stderr
    assert r.stdout.startswith("[orq noite] Cartão da manhã") and "Frente d3" in r.stdout and len(r.stdout.splitlines()) <= 40, r.stdout
    assert "Cartão da manhã" not in a.orq("resumo").stdout


def test_session_start_injeta_so_a_primeira_linha_do_cartao_quando_a_noite_acabou_ha_pouco():
    a = Amb(run="run_a")
    os.makedirs(a.home, exist_ok=True)
    agora = datetime.now(timezone.utc)
    fim = (agora - __import__("datetime").timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ini = (agora - __import__("datetime").timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
    evs = [{"ts": ini, "tipo": "noite_ligar", "ate": fim}, {"ts": ini, "tipo": "despacho", "dispatch": "d1", "task": "t1", "run": "run_a", "titulo": "Frente"},
           {"ts": fim, "tipo": "fim_dispatch", "dispatch": "d1", "task": "t1", "run": "run_a", "motivo": "entregue"}, {"ts": fim, "tipo": "noite_desligar"}]
    with open(os.path.join(a.home, "events.jsonl"), "w") as f:
        f.writelines(json.dumps(e) + "\n" for e in evs)
    ctx = json.loads(a.orq("hook", "session", stdin=json.dumps({"session_id": "abcdef123456"})).stdout)["hookSpecificOutput"]["additionalContext"]
    assert ctx.count("Cartão da manhã") == 1 and "orq resumo --noite" in ctx and "Despachos (" not in ctx, ctx


def test_gerente_absorver_carimba_a_rodada_no_cursor():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
    _gerente(a)
    assert a.orq("gerente", "absorver").returncode == 0
    assert json.load(open(os.path.join(a.home, "cursor.json")))["gerente_volta"] >= "2026"


# ---------- PR ligado à tarefa (ticket 42) ----------

FAKE_GH = """#!/usr/bin/env python3
import json, os, sys
d = os.environ["FAKE_DIR"]
open(os.path.join(d, "gh.log"), "a").write(json.dumps(sys.argv[1:]) + "\\n")
try:
    dados = json.load(open(os.path.join(d, "gh.json")))
except OSError:
    dados = {}
if sys.argv[2] == "list":
    repo = sys.argv[sys.argv.index("--repo") + 1]
    so_abertos = "--state" in sys.argv and sys.argv[sys.argv.index("--state") + 1] == "open"
    print(json.dumps([{**v, "url": k} for k, v in dados.items() if k.startswith(f"https://github.com/{repo}/") and (v["state"] == "OPEN" or not so_abertos)])); sys.exit(0)
if sys.argv[3] not in dados:
    sys.stderr.write("no pull requests found"); sys.exit(1)
print(json.dumps(dados[sys.argv[3]]))
"""
PR1 = "https://github.com/acme/app/pull/1216"
PR2 = "https://github.com/acme/app/pull/1220"


def _gh(a, **env):
    """Um gh falso no ambiente (ORQ_GH): responde com o que o gh.json tem por URL e anota cada chamada no gh.log."""
    caminho = os.path.join(a.tmp.name, "gh")
    with open(caminho, "w") as f:
        f.write(FAKE_GH)
    os.chmod(caminho, 0o755)
    a.env.update({"ORQ_GH": caminho, "ORQ_PR_POLL_S": a.env.get("ORQ_PR_POLL_S", "0"), **env})


def _pr(a, url, state="OPEN", base="development", titulo=None, **extra):
    """extra: mergeable, headRefName, statusCheckRollup (como o gh os devolve)."""
    dados = _log_json(a, "gh.json", {})
    dados[url] = {"state": state, "mergedAt": "2026-09-30T12:00:00Z" if state == "MERGED" else None, "baseRefName": base, **({"title": titulo} if titulo else {}), **extra}
    a.set("gh.json", dados)


def _log_json(a, nome, padrao):
    try:
        return json.load(open(os.path.join(a.fake, nome)))
    except OSError:
        return padrao


def _gh_chamadas(a):
    return _log(a, "gh.log")


def _neo(a):
    """O arquivo de projeto de três ambientes (development, staging, main), achado pelo cwd: o git flow que os testes de PR sempre supuseram."""
    os.makedirs(os.path.join(a.home, "projects"), exist_ok=True)
    with open(os.path.join(a.home, "projects", "tres-ambientes.json"), "w") as f:
        json.dump({"repo": f"path:{os.getcwd()}", "ambientes": [{"branch": "development"}, {"branch": "staging"}, {"branch": "main", "producao": True}], "fluxo": "promocao"}, f)


def _prs_env(**env):
    a = Amb(run="run_a", **env)
    _neo(a)
    _gh(a)
    _pr(a, PR1)
    return a


def test_pr_ligar_lista_e_desligar():
    a = _prs_env()
    r = a.orq("pr", "ligar", "task_feat1", PR1, "--issue", "1210")
    assert r.returncode == 0, r.stderr
    item = json.loads(r.stdout)
    assert (item["task"], item["url"], item["numero"], item["base"], item["estado"], item["issue"]) == ("task_feat1", PR1, 1216, "development", "aberto", 1210), item
    (ev,) = [e for e in a.events() if e["tipo"] == "pr"]
    assert (ev["op"], ev["task"], ev["url"]) == ("ligar", "task_feat1", PR1)
    lista = a.orq("pr", "lista").stdout
    assert "task_feat1" in lista and "#1216" in lista and "development" in lista and "aberto" in lista and "issue #1210" in lista, lista
    assert "task_feat1" not in a.orq("pr", "lista", "--task", "task_outra").stdout
    r = a.orq("pr", "desligar", "task_feat1", PR1)
    assert r.returncode == 0, r.stderr
    assert "nenhum PR" in a.orq("pr", "lista").stdout
    assert [e["op"] for e in a.events() if e["tipo"] == "pr"] == ["ligar", "desligar"]


def test_pr_ligar_recusas():
    a = _prs_env()
    assert a.orq("pr", "ligar", "task_feat1", "https://example.com/x").returncode == 1
    assert a.orq("pr", "ligar", "nao-e-task", PR1).returncode == 1
    assert a.orq("pr", "ligar", "task_feat1", PR1).returncode == 0
    r = a.orq("pr", "ligar", "task_feat1", PR1)
    assert r.returncode == 1 and "já está ligado" in r.stderr, r
    r = a.orq("pr", "desligar", "task_feat1", PR2)
    assert r.returncode == 1 and "não está ligado" in r.stderr, r
    assert len(json.load(open(os.path.join(a.home, "prs.json")))["itens"]) == 1


def test_pr_ligar_guarda_o_estado_que_o_gh_ve_e_o_ja_mergeado_nao_acorda():
    a = _prs_env()
    _pr(a, PR1, "MERGED", "staging")
    item = json.loads(a.orq("pr", "ligar", "task_feat1", PR1).stdout)
    assert (item["estado"], item["base"], item["avisado"]) == ("mergeado", "staging", True), item
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    assert not [e for e in a.events() if e["tipo"] == "entrada"], "o usuário já sabe do PR que ele ligou depois do merge"


def test_pr_ligar_sem_resposta_do_gh_registra_aberto_sem_base():
    a = _prs_env()
    item = json.loads(a.orq("pr", "ligar", "task_feat1", PR2).stdout)  # o gh falso não conhece o PR2
    assert (item["estado"], item.get("base")) == ("aberto", None), item


def test_pr_poll_merge_vira_uma_entrada_uma_so_vez():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "development")
    r = a.orq("pr", "poll", "--forcar")
    assert r.returncode == 0, r.stderr
    (ent,) = [e for e in a.events() if e["tipo"] == "entrada"]
    assert ent["origem"] == "pr" and ent["ref"] == PR1 and ent["task"] == "task_feat1", ent
    assert "PR #1216 entrou em development" in ent["texto"] and "pronto para staging" in ent["texto"], ent["texto"]
    antes = len(_gh_chamadas(a))
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    assert len([e for e in a.events() if e["tipo"] == "entrada"]) == 1, "o mesmo merge não vira outra entrada"
    assert len(_gh_chamadas(a)) == antes, "PR já resolvido não volta ao gh"
    assert [e["op"] for e in a.events() if e["tipo"] == "pr"] == ["ligar", "entrou"]


def _limpar_falso(a):
    """Um limpar-mergeados falso que grava os argumentos que recebeu em limpar.args."""
    falso = os.path.join(a.fake, "limpar.py")
    with open(falso, "w") as f:
        f.write("import sys, json\nopen(%r, 'w').write(json.dumps(sys.argv[1:]))\n" % os.path.join(a.fake, "limpar.args"))
    a.env.update(ORQ_LIMPAR=falso, ORQ_LIMPAR_ATRASO_S="0")
    return os.path.join(a.fake, "limpar.args")


def _espera_arquivo(caminho, s=5):
    fim = time.time() + s
    while time.time() < fim and not os.path.exists(caminho):
        time.sleep(0.05)
    return os.path.exists(caminho)


def test_pr_poll_merge_em_main_dispara_a_limpeza_da_branch():
    a = _prs_env()
    args = _limpar_falso(a)
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "main", headRefName="fix/x")
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    assert _espera_arquivo(args), "o poll não chamou a limpeza"
    lido = json.load(open(args))
    assert lido[0] == "--repo" and lido[2:] == ["--branch", "fix/x", "--task", "task_feat1"], lido
    (ev,) = [e for e in a.events() if e["tipo"] == "pr" and e["op"] == "limpeza"]
    assert (ev["branch"], ev["task"]) == ("fix/x", "task_feat1"), ev


def test_pr_poll_merge_fora_de_main_nao_dispara_a_limpeza():
    a = _prs_env()
    args = _limpar_falso(a)
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "development", headRefName="fix/x")
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    time.sleep(0.5)
    assert not os.path.exists(args) and not [e for e in a.events() if e.get("op") == "limpeza"]


def test_pr_poll_fechado_sem_merge_vira_entrada_e_nao_sugere_o_proximo():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "CLOSED", "development")
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    (ent,) = [e for e in a.events() if e["tipo"] == "entrada"]
    assert "PR #1216 fechado sem merge" in ent["texto"] and "pronto para" not in ent["texto"], ent["texto"]
    assert [e["op"] for e in a.events() if e["tipo"] == "pr"] == ["ligar", "fechou", "fechada"]


def test_pr_poll_pr_aberto_nao_gera_entrada():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    assert not [e for e in a.events() if e["tipo"] == "entrada"]


def test_pr_poll_respeita_o_limite_de_frequencia():
    a = _prs_env(ORQ_PR_POLL_S="600")
    a.orq("pr", "ligar", "task_feat1", PR1)
    antes = len(_gh_chamadas(a))
    assert a.orq("pr", "poll").returncode == 0
    depois = len(_gh_chamadas(a))
    assert depois == antes + 1, "o primeiro poll consulta o gh"
    assert a.orq("pr", "poll").returncode == 0
    assert len(_gh_chamadas(a)) == depois, "o segundo, dentro do limite, não chama o gh"
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    assert len(_gh_chamadas(a)) == depois + 1


def test_pr_poll_sem_pr_aberto_nao_chama_o_gh():
    a = _prs_env()
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    assert _gh_chamadas(a) == []


def test_pr_poll_gh_fora_do_ar_deixa_o_pr_para_a_proxima_volta():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    a.env["ORQ_GH"] = "/nao/existe/gh"
    r = a.orq("pr", "poll", "--forcar")
    assert r.returncode == 0, r.stderr
    assert json.load(open(os.path.join(a.home, "prs.json")))["itens"][0]["estado"] == "aberto"
    assert not [e for e in a.events() if e["tipo"] == "entrada"]


def test_pr_hooks_de_prompt_e_stop_nao_chamam_o_gh():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    antes = len(_gh_chamadas(a))
    _pr(a, PR1, "MERGED")
    assert a.prompt("oi").returncode == 0
    assert a.orq("hook", "stop", stdin=json.dumps({"session_id": "abcdef123456"})).returncode == 0
    assert a.orq("hook", "session", stdin=json.dumps({"session_id": "abcdef123456"})).returncode == 0
    assert len(_gh_chamadas(a)) == antes, "só o poll fora dos hooks fala com a rede"


def _linha_pr(a):
    """A linha `PR <task>:` do `orq status` (a entrada aberta, no resumo, também cita o próximo ambiente)."""
    (linha,) = [x for x in a.orq("status").stdout.splitlines() if x.startswith("PR task_")]
    return linha


def test_pr_status_mostra_o_ambiente_e_sugere_o_proximo_sem_abrir():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    assert "task_feat1" in _linha_pr(a) and "#1216 development aberto" in _linha_pr(a)
    _pr(a, PR1, "MERGED", "development")
    a.orq("pr", "poll", "--forcar")
    out = _linha_pr(a)
    assert "#1216 development ✓" in out and "pronto para staging" in out, out
    _pr(a, PR2, "OPEN", "staging")
    a.orq("pr", "ligar", "task_feat1", PR2)
    out = _linha_pr(a)
    assert "#1220 staging aberto" in out and "pronto para" not in out, out
    _pr(a, PR2, "MERGED", "staging")
    a.orq("pr", "poll", "--forcar")
    assert "pronto para main" in _linha_pr(a)
    PR3 = PR1.replace("1216", "1230")
    _pr(a, PR3, "MERGED", "main")
    a.orq("pr", "ligar", "task_feat1", PR3)
    out = _linha_pr(a)
    assert "em main" in out and "pronto para" not in out, out


def test_pr_entrada_aparece_no_prompt_e_fecha_com_intake():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "development")
    a.orq("pr", "poll", "--forcar")
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "PR #1216 entrou em development" in ctx, ctx
    assert len(ctx.splitlines()) <= 5
    (ent,) = [e for e in a.events() if e["tipo"] == "entrada" and e.get("origem") == "pr"]
    assert a.orq("intake", ent["id"], "conversa").returncode == 1, "com obrigação aberta o intake conversa é recusado (ticket 114)"
    for chave in ("deploy", "proximo"):
        assert a.orq("feito", ent["id"], chave, "--prova", "ok").returncode == 0
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "PR #1216" not in ctx, ctx


def test_pr_aviso_digitado_no_coordenador_nao_vira_entrada_do_usuario():
    assert orq_mod.origem("orq: PR #1216 entrou em development (task_feat1): pronto para staging.") == "aviso_orq"
    a = _prs_env()
    r = a.prompt("orq: PR #1216 entrou em development (task_feat1): pronto para staging. Entrada e1.")
    assert r.returncode == 0 and not [e for e in a.events() if e.get("origem") == "usuario"]


def test_pr_gerente_digita_o_aviso_no_coordenador_uma_vez():
    a = _prs_env(ORCA_TERMINAL_HANDLE="term_ger")
    _gerente(a)
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "development")
    assert a.orq("gerente", "absorver").returncode == 0
    assert a.orq("gerente", "absorver").returncode == 0
    envios = [e for e in _log(a, "send.log") if "--text" in e]
    assert len(envios) == 1, envios
    assert envios[0][envios[0].index("--terminal") + 1] == "term_coord"
    texto = envios[0][envios[0].index("--text") + 1]
    assert orq_mod.origem(texto) == "aviso_orq" and "PR #1216 entrou em development" in texto and "pronto para staging" in texto, texto
    (ent,) = [e for e in a.events() if e["tipo"] == "entrada"]
    assert ent["id"] in texto
    assert [e["op"] for e in a.events() if e["tipo"] == "pr"] == ["ligar", "entrou", "avisado"]


def test_pr_gerente_com_coordenador_ocupado_tenta_de_novo_sem_duplicar_a_entrada():
    a = _prs_env(ORCA_TERMINAL_HANDLE="term_ger")
    _gerente(a)
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "development")
    a.set("busy.json", ["term_coord"])
    assert a.orq("gerente", "absorver").returncode == 0
    assert not [e for e in _log(a, "send.log") if "--text" in e]
    a.set("busy.json", [])
    assert a.orq("gerente", "absorver").returncode == 0
    assert len([e for e in _log(a, "send.log") if "--text" in e]) == 1
    assert len([e for e in a.events() if e["tipo"] == "entrada"]) == 1


def _wt(caminho, dias, agora, **extra):
    return {"path": caminho, "branch": "refs/heads/feat/" + os.path.basename(caminho), "lastActivityAt": int((agora.timestamp() - dias * 86400) * 1000), **extra}


def test_worktrees_paradas_lista_so_a_sem_worker_parada_ha_mais_de_3_dias_com_os_commits_fora_da_main():
    agora = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    wts = [_wt("/w/velha", 5, agora), _wt("/w/recente", 2, agora), _wt("/w/com-worker", 9, agora), _wt("/w/principal", 9, agora, isMainWorktree=True),
           _wt("/w/arquivada", 9, agora, isArchived=True)]
    fora = {"/w/velha": 4}
    r = orq_mod.worktrees_paradas(wts, {"/w/com-worker"}, agora, lambda c: fora.get(c, 0))
    assert r == [{"caminho": "/w/velha", "branch": "feat/velha", "dias": 5, "fora": 4}], r
    assert orq_mod.worktrees_paradas(wts, set(), agora, lambda c: 0)[0]["caminho"] == "/w/com-worker"  # sem o worker, ela entra


def test_worktrees_paradas_avisa_uma_vez_por_dia_no_status():
    agora = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    with tempfile.TemporaryDirectory() as d:
        orq_mod.HOME, antes = d, orq_mod.HOME
        try:
            wts = [_wt("/w/velha", 5, agora)]
            args = (agora, wts, set(), lambda c: 4)
            (linha,) = orq_mod.linhas_worktrees(*args)
            assert "Worktrees paradas (1)" in linha and "velha" in linha and "5 dias" in linha and "4 commits fora da main" in linha, linha
            assert orq_mod.linhas_worktrees(*args) == []  # o mesmo dia não repete
            assert len(orq_mod.linhas_worktrees(agora + timedelta(days=1), wts, set(), lambda c: 4)) == 1  # o dia seguinte volta
            assert orq_mod.linhas_worktrees(agora + timedelta(days=2), [], set(), lambda c: 0) == []  # nada parado: sem linha
        finally:
            orq_mod.HOME = antes



# ---------- ticket 46: PR ligado à tarefa sozinho no gh pr create ----------

def _pos_pr(a, cwd, cmd="gh pr create --base development --title x --body y", saida=PR1 + "\n", sid="abcdef123456", **env):
    """O PostToolUse do Bash do coordenador com a saída (falsa) do gh pr create."""
    ev = {"session_id": sid, "cwd": cwd, "hook_event_name": "PostToolUse", "tool_name": "Bash", "tool_input": {"command": cmd}, "tool_response": {"stdout": saida, "stderr": ""}}
    r = a.orq("hook", "prligar", stdin=json.dumps(ev), **env)
    assert r.returncode == 0, r.stderr
    return r


def _prs_json(a):
    return json.load(open(os.path.join(a.home, "prs.json")))


def _ambiente_46(**env):
    """Coordenador com gh falso e um worker cuja worktree é a `wt` (branch feat/w) do repo de teste; devolve (a, principal, worktree)."""
    a = Amb(run="run_a", **env)
    _neo(a)
    _gh(a)
    _pr(a, PR1)
    a.prompt("oi")
    p, w = _repo(a.tmp.name)
    a.set("workers.json", [{"handle": "term_w", "run": "run_a", "dispatch": "ctx_w", "worktree": w}])
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"ts": "2026-09-30T01:00:00Z", "tipo": "despacho", "run": "run_a", "task": "task_feat1", "dispatch": "ctx_w", "worktree": "current"}) + "\n")
    return a, p, w


def test_it_should_link_the_pr_to_the_task_whose_worktree_holds_the_branch():
    a, p, w = _ambiente_46()
    r = _pos_pr(a, w)
    (item,) = _prs_json(a)["itens"]
    assert (item["task"], item["url"], item["numero"], item["base"], item["estado"]) == ("task_feat1", PR1, 1216, "development", "aberto"), item
    ctx = json.loads(r.stdout)["hookSpecificOutput"]
    assert ctx["hookEventName"] == "PostToolUse" and "#1216" in ctx["additionalContext"] and "feat/w" in ctx["additionalContext"], ctx
    assert [e["op"] for e in a.events() if e["tipo"] == "pr"] == ["ligar"]


def test_it_should_find_the_task_by_the_head_flag_even_when_run_from_the_main_checkout():
    a, p, w = _ambiente_46()
    _pos_pr(a, p, cmd="gh pr create --head feat/w --base development --title x")
    (item,) = _prs_json(a)["itens"]
    assert item["task"] == "task_feat1", item
    a2, p2, w2 = _ambiente_46()
    _pos_pr(a2, p2, cmd=f"cd {w2} && gh pr create --base development")
    assert _prs_json(a2)["itens"][0]["task"] == "task_feat1"


def test_it_should_find_the_task_by_the_worktree_name_without_asking_the_orca():
    a, p, w = _ambiente_46()
    a.set("workers.json", [])  # o Orca não conhece mais o worker
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"ts": "2026-09-30T02:00:00Z", "tipo": "despacho", "run": "run_a", "task": "task_nome", "dispatch": "ctx_n", "worktree": "new-top-level", "nome": "feat/w"}) + "\n")
    _pos_pr(a, p, cmd="gh pr create --head leodiegoo/feat/w")
    assert _prs_json(a)["itens"][0]["task"] == "task_nome"


def test_it_should_list_a_pr_without_a_known_task_and_show_it_in_status():
    a, p, w = _ambiente_46()
    a.set("workers.json", [])
    r = _pos_pr(a, p, cmd="gh pr create --head feat/desconhecida --base development")
    d = _prs_json(a)
    assert d["itens"] == [] and [(x["url"], x["head"]) for x in d["sem_task"]] == [(PR1, "feat/desconhecida")], d
    assert "sem tarefa" in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    st = a.orq("status").stdout
    assert "PR sem tarefa" in st and "#1216" in st and "feat/desconhecida" in st, st
    a.orq("pr", "ligar", "task_feat1", PR1)  # ligar à mão tira o PR da lista
    assert _prs_json(a)["sem_task"] == [] and "PR sem tarefa" not in a.orq("status").stdout


def _fila_json(a):
    return json.load(open(os.path.join(a.home, "fila.json")))["passos"]


def _fila_auto_env():
    a, p, w = _ambiente_46()
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"ts": "2026-09-30T01:30:00Z", "tipo": "despacho", "run": "run_a", "task": "task_feat1", "dispatch": "ctx_w", "worktree": "current", "titulo": "Filtro por marca"}) + "\n")
    corpo = "Corrige o filtro por marca que perdia a seleção.\n\nSegundo parágrafo que não entra."
    _pr(a, PR1, base="development", body=corpo)
    _pr(a, PR2, base="staging", body=corpo)
    _pr(a, PR3, base="main", body=corpo)
    return a, w


def test_it_should_queue_the_development_and_staging_prs_of_a_task_in_one_step():
    a, p = _fila_auto_env()
    _pos_pr(a, p, saida=PR1 + "\n")
    _pos_pr(a, p, saida=PR2 + "\n")
    assert _fila_json(a) == [{"passo": 1, "nome": "Filtro por marca", "por": "Corrige o filtro por marca que perdia a seleção.", "prs": [1216, 1220], "feito": False}]


def test_it_should_open_a_main_step_that_names_the_environments_already_merged():
    a, p = _fila_auto_env()
    _pos_pr(a, p, saida=PR1 + "\n")
    _pos_pr(a, p, saida=PR2 + "\n")
    _pr(a, PR1, state="MERGED", base="development")
    _pr(a, PR2, state="MERGED", base="staging")
    a.orq("pr", "poll")
    _pos_pr(a, p, saida=PR3 + "\n")
    assert [(x["passo"], x["nome"], x["por"], x["prs"]) for x in _fila_json(a)][1] == (2, "Filtro por marca para main", "development e staging já entraram (#1216, #1220)", [1230])


def test_it_should_not_duplicate_the_queue_step_when_the_pr_is_reopened():
    a, p = _fila_auto_env()
    _pos_pr(a, p, saida=PR1 + "\n")
    _pos_pr(a, p, saida=PR1 + "\n")
    assert [x["prs"] for x in _fila_json(a)] == [[1216]]


def test_it_should_put_a_merge_branch_pr_in_the_step_of_its_feature():
    a, p = _fila_auto_env()
    _despacho_por_nome(a, "task_feat1", "feat/w", "ctx_w2")
    _pos_pr(a, p, saida=PR1 + "\n")
    _pr(a, PR2, base="staging", body="x")
    _pos_pr(a, p, cmd="gh pr create --head merge/feat/w-staging --base staging", saida=PR2 + "\n")
    assert [x["prs"] for x in _fila_json(a)] == [[1216, 1220]]


def test_it_should_keep_a_pr_without_a_task_out_of_the_queue():
    a, p = _fila_auto_env()
    a.set("workers.json", [])
    _pos_pr(a, p, cmd="gh pr create --head feat/desconhecida --base development")
    assert not os.path.exists(os.path.join(a.home, "fila.json")) and "PR sem tarefa" in a.orq("status").stdout


def test_it_should_ignore_what_is_not_a_fresh_pr_create_or_not_the_coordinator():
    a, p, w = _ambiente_46()
    assert _pos_pr(a, w, cmd="gh pr view 1216", saida=PR1).stdout == ""
    assert _pos_pr(a, w, saida="a pull request for branch already exists\n").stdout == ""
    assert _pos_pr(a, w, sid="outra_sessao").stdout == "", "sessão sem Run guardado não é o coordenador"
    assert not os.path.exists(os.path.join(a.home, "prs.json"))
    _pos_pr(a, w)
    assert _pos_pr(a, w).stdout == "", "PR já ligado não liga de novo"
    assert len(_prs_json(a)["itens"]) == 1


def test_it_should_tell_when_development_and_staging_both_merged_to_open_main():
    a, p, w = _ambiente_46()
    _pos_pr(a, w)
    _pr(a, PR2, "OPEN", "staging")
    _pos_pr(a, w, saida=PR2 + "\n")
    _pr(a, PR1, "MERGED", "development")
    a.orq("pr", "poll", "--forcar")
    _pr(a, PR2, "MERGED", "staging")
    a.orq("pr", "poll", "--forcar")
    ents = [e["texto"] for e in a.events() if e["tipo"] == "entrada" and e.get("origem") == "pr"]
    assert "abrir o de main" not in ents[0], "com o PR de staging ainda aberto, nada a abrir"
    assert "development e staging entraram" in ents[1] and "abrir o de main" in ents[1], ents
    assert "abrir o de main" in _linha_pr(a)


def test_it_should_not_tell_to_open_main_when_the_main_pr_is_already_there():
    a, p, w = _ambiente_46()
    _pr(a, PR1, "MERGED", "development")
    _pr(a, PR2, "MERGED", "staging")
    a.orq("pr", "ligar", "task_feat1", PR1)
    a.orq("pr", "ligar", "task_feat1", PR2)
    assert "abrir o de main" in _linha_pr(a)
    PR3 = PR1.replace("1216", "1230")
    _pr(a, PR3, "OPEN", "main")
    a.orq("pr", "ligar", "task_feat1", PR3)
    assert "abrir o de main" not in _linha_pr(a)


# ---------- digest e modo ausente (ticket 47) ----------

def _tk_arq(a, nn, titulo, task=None, bloqueado=None):
    os.makedirs(a.env["ORQ_ISSUES"], exist_ok=True)
    cab = f"# {nn}: {titulo}\n\nStatus: claimed\nBlocked by: {bloqueado or '(nenhum)'}\nRun: run_a\n" + (f"Task: {task}\n" if task else "")
    with open(os.path.join(a.env["ORQ_ISSUES"], f"{nn}-t.md"), "w") as f:
        f.write(cab + "\n## What to build\n\nx\n")


def _evs(a, *evs):
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.writelines(json.dumps(e) + "\n" for e in evs)


PR3 = "https://github.com/acme/app/pull/1230"


def _digest_env(**env):
    """Dois tickets (02 depende do 01), PRs ligados fora de ordem, uma pendência, um worker rodando e um log com uma janela."""
    a = Amb(run="run_a", **env)
    _neo(a)
    _gh(a)
    _tk_arq(a, "01", "Base de auth", "task_a")
    _tk_arq(a, "02", "Tela nova", "task_b", "01")
    for url, base, titulo in ((PR2, "development", "feat: tela nova"), (PR1, "development", "feat: base de auth"), (PR3, "staging", "feat: base de auth (staging)")):
        _pr(a, url, "OPEN", base, titulo)
    a.orq("pr", "ligar", "task_b", PR2)
    a.orq("pr", "ligar", "task_a", PR1, "--tag", "segurança", "--nota", "A regra de auth fica num pacote só.")
    a.orq("pr", "ligar", "task_a", PR3)
    a.set("../pendencias.json", {"itens": [{"id": "freio-prod", "tipo": "decisao", "titulo": "Escolher o freio de produção", "detalhe": "teto por pod ou sem freio"}]})
    a.set("../orq/aberto.json", _aberto_ag("rodando", titulo="Ticket 47 digest"))
    _evs(a, _ev("09:00:00", "resposta_worker", texto="ANTIGA resposta", dispatch="ctx_1"),
         _ev("10:00:00", "entrada", id="e1", origem="usuario", texto="sigo amanhã"),
         _ev("11:00:00", "resposta_worker", texto="use o índice novo", dispatch="ctx_1"),
         _ev("11:30:00", "worker_done", msg="m1", task="task_a", dispatch="ctx_1", outcome="succeeded", subject="Base de auth entregue"),
         _ev("12:00:00", "pend", op="done", pend="avisar-x", resposta="pode seguir"))
    return a


def _json_digest(a, *args):
    r = a.orq("digest", *args)
    assert r.returncode == 0, r.stderr
    return json.load(open(os.path.join(a.home, "digest", "atual.json")))


def _html(a, *args):
    r = a.orq("digest", "--html", *args)
    assert r.returncode == 0, r.stderr
    (caminho,) = [l for l in r.stdout.splitlines() if l.endswith(".html")]
    return r, open(caminho).read()


def _ev_depois(tipo, **k):
    return {"ts": "2099-01-01T00:00:00Z", "tipo": tipo, **k}


def test_digest_grava_o_contrato_v1_no_caminho_fixo():
    a = _digest_env()
    r = a.orq("digest")
    assert r.returncode == 0 and r.stdout.splitlines()[0] == os.path.join(a.home, "digest", "atual.json"), r
    d = json.load(open(r.stdout.splitlines()[0]))
    assert d["versao"] == 1 and d["geradoEm"].endswith("Z") and d["ausente"] == {"ligado": False, "desde": None}, d
    assert set(d) == {"versao", "geradoEm", "ausente", "fila", "proximoPasso", "features", "pendencias", "linha", "rodando", "tickets_orq"}, set(d)
    assert [p["nome"] for p in d["fila"]] == ["Base de auth", "Tela nova"] and [p["passo"] for p in d["fila"]] == [1, 2], d["fila"]
    assert set(d["fila"][0]) == {"passo", "nome", "por", "prs", "feito", "pronto", "avisos"} and d["fila"][0]["feito"] is False
    assert [(x["numero"], x["base"], x["estado"], x["titulo"]) for x in d["fila"][0]["prs"]] == [
        (1216, "development", "OPEN", "feat: base de auth"), (1230, "staging", "OPEN", "feat: base de auth (staging)")], d["fila"][0]["prs"]
    assert d["fila"][0]["prs"][0]["url"] == PR1
    f = {x["nome"]: x for x in d["features"]}
    assert (f["Base de auth"]["tag"], f["Base de auth"]["nota"]) == ("segurança", "A regra de auth fica num pacote só.") and f["Tela nova"]["tag"] is None, f
    assert [x["numero"] for x in f["Base de auth"]["prs"]] == [1216, 1230]
    (pend,) = d["pendencias"]
    assert pend["id"] == "freio-prod" and pend["detalhe"] == "teto por pod ou sem freio" and pend["depois"] is False, pend
    assert d["rodando"] == [{"titulo": "Ticket 47 digest", "estado": "fase-3", "desde": None}], d["rodando"]
    assert d["linha"] == [], "a linha só existe com o modo ausente ligado"
    assert "task_aaaa" not in json.dumps(d["rodando"]) and "ctx_1" not in json.dumps(d["rodando"]) and "term_1" not in json.dumps(d["rodando"])


def _tk_status(a, nn, titulo, status, bloqueado=None, task=None, dias=0):
    _tk_arq(a, nn, titulo, task, bloqueado)
    caminho = os.path.join(a.env["ORQ_ISSUES"], f"{nn}-t.md")
    txt = open(caminho).read().replace("Status: claimed", f"Status: {status}")
    open(caminho, "w").write(txt)
    t = time.time() - dias * 86400
    os.utime(caminho, (t, t))


def test_digest_lista_os_tickets_abertos_por_status_com_bloqueio_e_os_5_ultimos_resolvidos():
    a = Amb(run="run_a")
    _tk_status(a, "01", "Pronto", "ready-for-agent")
    _tk_status(a, "02", "Espera o 01", "ready-for-agent", bloqueado="01")
    _tk_status(a, "03", "Em curso", "claimed", task="task_aaaaaaaaaa")
    _tk_status(a, "04", "Bloqueio ja resolvido", "ready-for-agent", bloqueado="05")
    for n in range(5, 13):
        _tk_status(a, f"{n:02d}", f"Feito {n}", "resolved", dias=20 - n)
    _tk_status(a, "13", "Descartado", "wontfix")
    os.makedirs(os.path.join(a.home, "..", "orq"), exist_ok=True)
    a.set("../orq/aberto.json", _aberto_ag("rodando"))
    t = _json_digest(a)["tickets_orq"]
    ab = {x["num"]: x for x in t["abertos"]}
    assert sorted(ab) == ["01", "02", "03", "04"], ab
    assert [(ab[n]["grupo"], ab[n]["bloqueios"]) for n in ("01", "02", "03", "04")] == [
        ("pronto", []), ("bloqueado", ["01"]), ("andamento", []), ("pronto", [])], ab
    assert ab["03"]["task"] == "task_aaaaaaaaaa" and ab["03"]["worker"] == "fase-3" and ab["01"]["worker"] is None
    assert ab["01"]["titulo"] == "Pronto" and ab["01"]["arquivo"].endswith("01-t.md")
    assert [x["num"] for x in t["resolvidos"]] == ["12", "11", "10", "09", "08"], t["resolvidos"]
    assert re.fullmatch(r"\d{4}-\d\d-\d\d", t["resolvidos"][0]["em"])


def test_digest_sem_tickets_manda_tickets_orq_vazio():
    assert _json_digest(Amb(run="run_a"))["tickets_orq"] == {"abertos": [], "resolvidos": []}


def test_digest_pendencia_em_depois_sai_marcada_no_contrato():
    a = _digest_env()
    a.set("../pendencias.json", {"itens": [{"id": "x", "tipo": "acao", "titulo": "Adiada", "ate": "2999-01-01"}, {"id": "y", "tipo": "acao", "titulo": "Viva"}]})
    d = _json_digest(a)
    assert {p["id"]: p["depois"] for p in d["pendencias"]} == {"x": True, "y": False}, d["pendencias"]


def test_digest_reune_prs_pendencias_workers_e_respostas_na_pagina():
    a = _digest_env()
    r, h = _html(a)
    assert r.stdout.splitlines()[1].startswith(os.path.join(a.home, "digest") + os.sep) and r.stdout.splitlines()[1].endswith(".html"), r.stdout
    for trecho in ("#1216", "#1220", "#1230", "Base de auth", "Tela nova", "Escolher o freio de produção", "Ticket 47 digest", "fase-3",
                   "use o índice novo", "Base de auth entregue", "pode seguir"):
        assert trecho in h, (trecho, h[:400])
    assert "ANTIGA resposta" not in h, "sem o modo ausente a janela começa na última mensagem do usuário"


def test_digest_ordem_de_merge_segue_o_blocked_by_e_development_vem_antes_de_staging():
    a = _digest_env()
    _, h = _html(a)
    assert h.index("Base de auth") < h.index("Tela nova"), "o ticket 02 espera o 01 mesmo com o PR ligado antes"
    assert h.index("#1216") < h.index("#1230"), "development antes de staging dentro do passo"
    assert "Espera: Base de auth" in h, "o passo bloqueado diz de quem espera"
    assert "Blocked by dos tickets" in h, "a página diz que a ordem veio dos tickets"


def test_digest_ordem_pura_com_ciclo_e_transitivo_sem_travar():
    g = lambda t, ligado: {"task": t, "ligado_em": ligado, "itens": []}  # noqa: E731
    tk = lambda n, t, b: {"num": n, "task": t, "blocked_by": b, "titulo": n, "status": "claimed"}  # noqa: E731
    # c depende de b, que depende de a; b não tem PR: c ainda espera a
    ordem = orq_mod.ordem_de_merge([g("task_c", "3"), g("task_a", "2")], [tk("01", "task_a", []), tk("02", "task_b", ["01"]), tk("03", "task_c", ["02"])])
    assert [x["task"] for x in ordem] == ["task_a", "task_c"], ordem
    assert ordem[1]["espera"] == ["task_a"], ordem
    # ciclo: todos saem, na ordem do ligado_em, e a ordem avisa
    ordem = orq_mod.ordem_de_merge([g("task_y", "2"), g("task_x", "1")], [tk("01", "task_x", ["02"]), tk("02", "task_y", ["01"])])
    assert [x["task"] for x in ordem] == ["task_x", "task_y"] and all(x.get("ciclo") for x in ordem), ordem
    # sem ticket: sem dependência declarada, vale o ligado_em
    assert [x["task"] for x in orq_mod.ordem_de_merge([g("task_2", "2"), g("task_1", "1")], [])] == ["task_1", "task_2"]


def test_digest_passo_fica_feito_so_quando_entrou_em_main_ou_so_tem_fechado():
    a = _digest_env()
    _pr(a, PR2, "MERGED", "development")
    a.orq("pr", "poll", "--forcar")
    d = _json_digest(a, "--html")
    _, h = _html(a)
    assert "Próximo: pronto para staging" in h and 'class="feito"' not in h, "development entrou, falta promover: ainda não é feito"
    assert [p["feito"] for p in d["fila"]] == [False, False]
    _pr(a, PR1, "MERGED", "development")
    _pr(a, PR3, "MERGED", "main")
    a.orq("pr", "poll", "--forcar")
    d = _json_digest(a)
    _, h = _html(a)
    assert [p["feito"] for p in d["fila"]] == [True, False], d["fila"]
    assert h.count('class="feito"') == 1 and "Base de auth" in h.split('class="feito"', 1)[1].split("</li>", 1)[0]


def test_digest_desde_aceita_um_carimbo_e_escapa_o_html():
    a = _digest_env()
    _evs(a, _ev("13:00:00", "resposta_worker", texto="<script>alert(1)</script>", dispatch="ctx_1"))
    _, h = _html(a, "--desde", "2026-09-30T08:00:00Z")
    assert "ANTIGA resposta" in h and "<script>alert(1)</script>" not in h and "&lt;script&gt;" in h, h[:500]
    assert a.orq("digest", "--desde", "ontem").returncode == 1


def test_digest_nao_chama_gh_nem_orca_e_le_o_estado_dos_prs_do_poll():
    a = _digest_env()
    a.orq("pr", "poll", "--forcar")
    antes_gh, antes_orca = len(_gh_chamadas(a)), len(_log(a, "calls.log"))
    _, h = _html(a)
    assert (len(_gh_chamadas(a)), len(_log(a, "calls.log"))) == (antes_gh, antes_orca), "o digest só lê arquivos locais"
    assert "estado dos PRs é do poll das" in h


def test_digest_abrir_pede_a_aba_ao_orca_com_a_url_do_arquivo():
    a = _digest_env()
    r = a.orq("digest", "--abrir")
    (pagina,) = [l for l in r.stdout.splitlines() if l.endswith(".html")]
    (chamada,) = [c for c in _log(a, "calls.log") if c[0] == "create"]
    assert chamada[chamada.index("--url") + 1] == pathlib.Path(pagina).as_uri(), chamada


def test_digest_abrir_com_o_orca_fora_do_ar_mostra_o_caminho_e_nao_falha():
    a = _digest_env()
    r = a.orq("digest", "--abrir", FAKE_FAIL="create")
    assert r.returncode == 0 and "aviso" in r.stderr and os.path.exists(os.path.join(a.home, "digest", "atual.json")), r
    (pagina,) = [l for l in r.stdout.splitlines() if l.endswith(".html")]
    assert os.path.exists(pagina)


def test_digest_vazio_gera_o_arquivo_e_a_pagina_dizendo_que_nao_ha_nada():
    a = Amb(run="run_a")
    a.set("../pendencias.json", {"itens": []})
    d = _json_digest(a)
    assert (d["fila"], d["features"], d["pendencias"], d["linha"], d["rodando"]) == ([], [], [], [], []), d
    _, h = _html(a)
    assert h.lower().count("nada") >= 4 and "<!doctype html>" in h.lower(), h[:400]


def test_pr_ligar_guarda_titulo_tag_e_nota_para_o_digest():
    a = _prs_env()
    _pr(a, PR1, "OPEN", "development", "feat: x")
    item = json.loads(a.orq("pr", "ligar", "task_feat1", PR1, "--tag", "qualidade", "--nota", "Faz x.").stdout)
    assert (item["titulo"], item["tag"], item["nota"]) == ("feat: x", "qualidade", "Faz x."), item
    sem = json.loads(a.orq("pr", "ligar", "task_feat2", PR2).stdout)
    assert "tag" not in sem and "nota" not in sem, sem


def test_ingest_grava_o_worker_done_no_log_uma_vez_so():
    a = Amb(run="run_a")
    a.env["ORQ_NO_BG"] = "1"
    m = {"id": "msg_w1", "run_id": "run_a", "sequence": 1, "type": "worker_done", "subject": "Ticket 47 pronto", "body": "feito", "created_at": "2099-01-01T00:00:00Z",
         "payload": json.dumps({"dispatchId": "ctx_w", "taskId": "task_w", "outcome": "succeeded"})}
    _inbox(a, m)
    assert a.orq("ingest").returncode == 0
    assert a.orq("ingest").returncode == 0
    (ev,) = [e for e in a.events() if e["tipo"] == "worker_done"]
    assert (ev["msg"], ev["task"], ev["dispatch"], ev["outcome"], ev["subject"]) == ("msg_w1", "task_w", "ctx_w", "succeeded", "Ticket 47 pronto"), ev


# ---- orq fila ----

def _fila_add(a, passo="1", nome="Plano 2 para main", por="Destrava o plano 1", *prs):
    return a.orq("fila", "add", "--passo", passo, "--nome", nome, "--por", por, *(prs or ("1216", "1230")))


def test_fila_add_lista_feito_e_rm():
    a = _digest_env()
    assert "nenhum passo" in a.orq("fila", "lista").stdout
    r = _fila_add(a)
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout) == {"passo": 1, "nome": "Plano 2 para main", "por": "Destrava o plano 1", "prs": [1216, 1230], "feito": False}
    _fila_add(a, "2", "Tela", "Depende do 1", "1220")
    lista = a.orq("fila", "lista").stdout.splitlines()
    assert lista[0].startswith("1  Plano 2 para main  [a fazer]  #1216 open ? sem leitura do CI, #1230 open ? sem leitura do CI") and "Destrava o plano 1" in lista[0] and lista[1].startswith("2  Tela"), lista
    assert lista[-1] == "Próximo a mergear: nenhum passo pronto", lista
    assert a.orq("fila", "feito", "1").returncode == 0
    assert "[feito]" in a.orq("fila", "lista").stdout.splitlines()[0]
    assert a.orq("fila", "rm", "2").returncode == 0
    assert len(a.orq("fila", "lista").stdout.splitlines()) == 2  # o passo e a linha do próximo
    assert [(e["op"], e["passo"]) for e in a.events() if e["tipo"] == "fila"] == [("add", 1), ("add", 2), ("feito", 1), ("rm", 2)]


# ---- orq fila: CI, conflito e próximo passo (ticket 81) ----

def _ck(nome, conclusao="SUCCESS", status="COMPLETED"):
    return {"__typename": "CheckRun", "name": nome, "status": status, "conclusion": conclusao}


def _fila_ci(**por_pr):
    """Dois PRs (1216 e 1220) em dois passos; cada PR lê o `mergeable` e os checks dados e o poll roda uma vez."""
    a = Amb(run="run_a")
    _neo(a)
    _gh(a)
    for url, k in ((PR1, "pr1"), (PR2, "pr2")):
        _pr(a, url, "OPEN", "development", **{"mergeable": "MERGEABLE", "statusCheckRollup": [_ck("lint")], "headRefName": "feat/" + k, **por_pr.get(k, {})})
    a.orq("pr", "ligar", "task_a", PR1)
    a.orq("pr", "ligar", "task_b", PR2)
    _fila_add(a, "1", "Primeiro", "", "1216")
    _fila_add(a, "2", "Segundo", "", "1220")
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    return a


def test_fila_poll_guarda_mergeable_e_checks_com_uma_chamada_do_gh_para_todos_os_prs():
    a = _fila_ci(pr1={"statusCheckRollup": [_ck("lint", "FAILURE"), _ck("test", None, "IN_PROGRESS"), _ck("build")]})
    chamadas = [c for c in _gh_chamadas(a) if c[:2] == ["pr", "list"]]
    assert len(chamadas) == 1, chamadas  # uma chamada para os dois PRs do mesmo repositório
    ci = {i["numero"]: i["ci"] for i in json.load(open(os.path.join(a.home, "prs.json")))["itens"]}
    assert (ci[1216]["mergeable"], ci[1216]["falhas"], ci[1216]["rodando"]) == ("MERGEABLE", ["lint"], ["test"]) and ci[1216]["lido_em"] > 0, ci
    assert ci[1220]["falhas"] == [] and ci[1220]["rodando"] == []


def test_fila_lista_mostra_o_check_vermelho_pelo_nome_o_conflito_e_o_ci_rodando():
    a = _fila_ci(pr1={"statusCheckRollup": [_ck("lint", "FAILURE"), _ck("e2e", "TIMED_OUT")]}, pr2={"mergeable": "CONFLICTING", "statusCheckRollup": [_ck("test", None, "QUEUED")]})
    l1, l2, fim = a.orq("fila", "lista").stdout.splitlines()
    assert "#1216 open ✗ lint, e2e" in l1, l1
    assert "#1220 open ⚠ conflito ⏳ CI rodando" in l2, l2
    assert fim == "Próximo a mergear: nenhum passo pronto"


def _ckw(nome, workflow, conclusao="SUCCESS"):
    return {**_ck(nome, conclusao), "workflowName": workflow}


def test_fila_pr_de_main_com_falha_so_no_workflow_de_staging_fica_pronto_com_a_nota():
    a = _fila_ci(pr1={"baseRefName": "main", "statusCheckRollup": [_ckw("lint", "Web CI"), _ckw("check", "Web Deploy Staging", "FAILURE")]})
    out = a.orq("fila", "lista").stdout.splitlines()
    assert "#1216 open ✓ ℹ falha em outro ambiente: Web Deploy Staging" in out[0] and "✗" not in out[0], out
    assert out[-1] == "Próximo a mergear: passo 1 (Primeiro)", out
    ci = {i["numero"]: i["ci"] for i in json.load(open(os.path.join(a.home, "prs.json")))["itens"]}
    assert ci[1216]["falhas"] == [] and ci[1216]["outro_ambiente"] == ["Web Deploy Staging"], ci


def test_fila_pr_de_main_com_falha_no_workflow_da_propria_base_fica_vermelho():
    a = _fila_ci(pr1={"baseRefName": "main", "statusCheckRollup": [_ckw("check", "Web Deploy Staging", "FAILURE"), _ckw("check", "Web Deploy Production", "FAILURE")]})
    l1 = a.orq("fila", "lista").stdout.splitlines()[0]
    assert "#1216 open ✗ check" in l1 and "falha em outro ambiente: Web Deploy Staging" in l1, l1


def test_fila_lista_aponta_o_primeiro_passo_a_fazer_com_todos_os_prs_prontos():
    a = _fila_ci(pr1={"statusCheckRollup": [_ck("lint", "FAILURE")]})
    out = a.orq("fila", "lista").stdout.splitlines()
    assert out[-1] == "Próximo a mergear: passo 2 (Segundo)" and "#1220 open ✓" in out[1], out
    a.orq("fila", "feito", "2")
    assert a.orq("fila", "lista").stdout.splitlines()[-1] == "Próximo a mergear: nenhum passo pronto", "passo feito não é o próximo"


def test_fila_lista_leitura_velha_aparece_como_velha_e_nao_conta_como_pronta():
    a = _fila_ci()
    assert a.orq("fila", "lista").stdout.splitlines()[-1] == "Próximo a mergear: passo 1 (Primeiro)"
    arq = os.path.join(a.home, "prs.json")
    d = json.load(open(arq))
    for i in d["itens"]:
        i["ci"]["lido_em"] -= 3600
    json.dump(d, open(arq, "w"))
    out = a.orq("fila", "lista").stdout.splitlines()
    assert "leitura velha, 60 min" in out[0] and out[-1] == "Próximo a mergear: nenhum passo pronto", out


def test_fila_conflito_entre_passos_vira_aviso_no_passo_de_baixo():
    a = _fila_ci()
    repo = os.path.join(a.tmp.name, "repo")
    g = lambda *x: subprocess.run(["git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@t", *x], check=True, capture_output=True)
    os.makedirs(repo)
    g("init", "-b", "main")
    open(os.path.join(repo, "a.js"), "w").write("um\n")
    g("add", "."), g("commit", "-m", "base")
    for ramo, txt in (("feat/pr1", "dois\n"), ("feat/pr2", "tres\n")):
        g("checkout", "-b", ramo, "main")
        open(os.path.join(repo, "a.js"), "w").write(txt)
        g("commit", "-am", ramo)
    out = a.orq("fila", "lista", ORQ_REPOS=repo).stdout.splitlines()
    assert out[0].startswith("1  Primeiro") and out[1].startswith("2  Segundo"), out
    assert out[2] == "   ⚠ #1220 conflita com #1216 (passo 1): a.js", out
    g("checkout", "main"), g("checkout", "-B", "feat/pr2", "main")
    open(os.path.join(repo, "b.js"), "w").write("x\n")
    g("add", "."), g("commit", "-m", "outro arquivo")
    assert not [x for x in a.orq("fila", "lista", ORQ_REPOS=repo).stdout.splitlines() if x.startswith("   ⚠")]


def test_digest_leva_o_ci_e_o_proximo_passo_do_painel():
    a = _fila_ci(pr1={"mergeable": "CONFLICTING"})
    d = _json_digest(a)
    p1, p2 = d["fila"]
    assert (p1["pronto"], p2["pronto"], d["proximoPasso"]) == (False, True, 2), d["fila"]
    assert (p1["prs"][0]["mergeable"], p1["prs"][0]["falhas"], p1["prs"][0]["pronto"], p1["prs"][0]["velha"]) == ("CONFLICTING", [], False, False), p1


def test_fila_add_troca_o_passo_de_mesmo_numero_e_recusa_pr_nao_ligado_e_passo_inexistente():
    a = _digest_env()
    _fila_add(a)
    _fila_add(a, "1", "Outro nome", "Outro motivo", "1220")
    (p,) = json.load(open(os.path.join(a.home, "fila.json")))["passos"]
    assert (p["nome"], p["prs"]) == ("Outro nome", [1220]), p
    r = _fila_add(a, "3", "x", "y", "9999")
    assert r.returncode == 1 and "#9999" in r.stderr and "orq pr ligar" in r.stderr, r
    assert _fila_add(a, "0").returncode == 1
    assert a.orq("fila", "feito", "7").returncode == 1 and a.orq("fila", "rm", "7").returncode == 1


def test_digest_fila_declarada_vale_mais_que_a_dos_tickets_com_o_estado_real_dos_prs():
    a = _digest_env()
    _fila_add(a, "1", "Tela primeiro", "Decisão do coordenador", "1220")
    _fila_add(a, "2", "Auth depois", "Vem por último", "1216", "1230")
    d = _json_digest(a)
    assert [(p["passo"], p["nome"], p["por"]) for p in d["fila"]] == [(1, "Tela primeiro", "Decisão do coordenador"), (2, "Auth depois", "Vem por último")], d["fila"]
    assert [x["estado"] for x in d["fila"][0]["prs"]] == ["OPEN"] and d["fila"][0]["prs"][0]["titulo"] == "feat: tela nova"
    _pr(a, PR2, "MERGED", "development")
    a.orq("pr", "poll", "--forcar")
    d = _json_digest(a)
    assert [p["feito"] for p in d["fila"]] == [True, False] and d["fila"][0]["prs"][0]["estado"] == "MERGED", d["fila"]
    assert [x["nome"] for x in d["features"]] == ["Base de auth", "Tela nova"], "features seguem a ordem pelos tickets"
    _, h = _html(a)
    assert "Ordem declarada com" in h and h.index("Tela primeiro") < h.index("Auth depois")


def test_digest_passo_declarado_marcado_feito_a_mao_vale_com_pr_aberto():
    a = _digest_env()
    _fila_add(a, "1", "Auth", "x", "1216")
    a.orq("fila", "feito", "1")
    assert _json_digest(a)["fila"][0]["feito"] is True


def test_digest_passo_declarado_ignora_pr_desligado():
    a = _digest_env()
    _fila_add(a, "1", "Auth", "x", "1216", "1230")
    a.orq("pr", "desligar", "task_a", PR3)
    d = _json_digest(a)
    assert [x["numero"] for x in d["fila"][0]["prs"]] == [1216], d["fila"]


# ---- modo ausente ----

def test_ausente_ligar_e_desligar_guardam_o_estado_e_o_log():
    a = Amb(run="run_a")
    assert "desligado" in a.orq("ausente").stdout
    r = a.orq("ausente", "ligar")
    assert r.returncode == 0 and "ligado" in r.stdout and os.path.join(a.home, "digest", "atual.json") in r.stdout, r
    assert _cursor(a)["ausente"]["ligada_em"]
    r = a.orq("ausente")
    assert "ligado" in r.stdout and "desligado" not in r.stdout
    assert "desligado" in a.orq("ausente", "desligar").stdout and "ausente" not in _cursor(a)
    assert [e["tipo"] for e in a.events() if e["tipo"].startswith("ausente")] == ["ausente_ligar", "ausente_desligar"]


def test_away_alterna_e_aceita_on_off_status_sem_tirar_o_ausente():
    a = Amb(run="run_a")
    assert "away mode ligado" in a.orq("away").stdout and _cursor(a)["ausente"]["ligada_em"]
    assert "ligado" in a.orq("away", "status").stdout and "desligado" not in a.orq("away", "status").stdout
    assert "away mode ligado" in a.orq("away", "on").stdout
    r = a.orq("away")
    assert "away mode desligado" in r.stdout and "entradas na linha do tempo" in r.stdout and "localhost:8765" in r.stdout and "ausente" not in _cursor(a), r
    assert "desligado" in a.orq("away", "status").stdout
    assert not os.path.exists(os.path.join(a.fake, "calls.log")) or "8765" not in open(os.path.join(a.fake, "calls.log")).read()
    assert "ligado" in a.orq("away", "on").stdout and "ligado" in a.orq("ausente").stdout
    assert "away mode desligado" in a.orq("away", "off").stdout


def _stop(a, **ev):
    return a.orq("hook", "stop", stdin=json.dumps({"session_id": "abcdef123456", **ev}))


def _atual(a):
    return json.load(open(os.path.join(a.home, "digest", "atual.json")))


def test_ausente_stop_atualiza_o_atual_json_com_cada_resposta_do_coordenador_sem_rede():
    a = _digest_env()
    a.orq("ausente", "ligar")
    antes_gh = len(_gh_chamadas(a))
    r = _stop(a, last_assistant_message="Fechei o passo 1 e segui no passo 2.")
    assert r.returncode == 0, r
    d = _atual(a)
    assert d["ausente"]["ligado"] is True and d["ausente"]["desde"] == _cursor(a)["ausente"]["ligada_em"]
    assert [x["titulo"] for x in d["linha"]] == ["Fechei o passo 1 e segui no passo 2."] and d["linha"][0]["tipo"] == "info", d["linha"]
    assert d["linha"][0]["detalhe"] == "Fechei o passo 1 e segui no passo 2." and d["linha"][0]["ts"].endswith("Z")
    assert len(_gh_chamadas(a)) == antes_gh, "o hook não chama o gh: o estado dos PRs vem do poll"
    _stop(a, last_assistant_message="Segunda resposta")
    assert [x["detalhe"] for x in _atual(a)["linha"]] == ["Fechei o passo 1 e segui no passo 2.", "Segunda resposta"], "cada resposta vira uma entrada"
    assert [e["texto"] for e in a.events() if e["tipo"] == "resposta_coordenador"] == ["Fechei o passo 1 e segui no passo 2.", "Segunda resposta"]


def test_ausente_stop_le_a_resposta_do_transcrito_quando_o_stop_nao_a_traz():
    a = _digest_env()
    a.orq("ausente", "ligar")
    arq = os.path.join(a.tmp.name, "transcrito.jsonl")
    with open(arq, "w") as f:
        f.write(json.dumps({"type": "user", "message": {"role": "user", "content": "oi"}}) + "\n")
        f.write(json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Resposta do transcrito"}]}}) + "\n")
        f.write(json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "tool_use", "name": "Bash"}]}}) + "\n")
        f.write("linha quebrada\n")
    _stop(a, transcript_path=arq)
    assert [x["detalhe"] for x in _atual(a)["linha"]] == ["Resposta do transcrito"]
    _stop(a, transcript_path=os.path.join(a.tmp.name, "nao-existe.jsonl"))
    assert len(_atual(a)["linha"]) == 1, "sem resposta achada não há entrada nova, e o Stop não quebra"


def test_ausente_linha_guarda_so_o_que_aconteceu_desde_que_ligou():
    a = _digest_env()
    _evs(a, {"ts": "2020-01-01T00:00:00Z", "tipo": "resposta_worker", "texto": "ANTIGA de 2020", "dispatch": "ctx_1"})
    a.orq("ausente", "ligar")
    _evs(a, _ev_depois("worker_done", msg="m9", task="task_a", outcome="failed", subject="Falhou no E2E"),
         _ev_depois("pr", op="entrou", numero=1216, base="development", task="task_a"),
         _ev_depois("pr", op="fechou", numero=1220, base="development", task="task_b"),
         _ev_depois("resposta_worker", texto="use o índice", dispatch="ctx_1"))
    _stop(a)
    linha = _atual(a)["linha"]
    assert "ANTIGA de 2020" not in json.dumps(linha) and "use o índice novo" not in json.dumps(linha), "o que veio antes de ligar fica de fora"
    assert [(x["tipo"], x["titulo"]) for x in linha] == [
        ("sec", "Worker falhou: Falhou no E2E"), ("ok", "PR #1216 entrou em development"), ("sec", "PR #1220 fechado sem merge"), ("info", "Coordenador respondeu a um worker")], linha


def test_ausente_desligado_a_linha_do_contrato_volta_vazia_mesmo_com_eventos():
    a = _digest_env()
    a.orq("ausente", "ligar")
    _evs(a, _ev_depois("resposta_worker", texto="algo", dispatch="ctx_1"))
    assert _json_digest(a)["linha"]
    a.orq("ausente", "desligar")
    assert _json_digest(a)["linha"] == [] and _atual(a)["ausente"] == {"ligado": False, "desde": None}


def test_ausente_stop_nao_chama_o_orca_alem_do_que_o_stop_ja_chamava():
    com, sem = _digest_env(), _digest_env()
    com.orq("ausente", "ligar")
    n_com, n_sem = len(_log(com, "calls.log")), len(_log(sem, "calls.log"))
    _stop(com, last_assistant_message="oi"), _stop(sem, last_assistant_message="oi")
    assert len(_log(com, "calls.log")) - n_com == len(_log(sem, "calls.log")) - n_sem, "o digest do Stop não faz chamada ao Orca"


def test_ausente_desligado_o_stop_nao_gera_digest_nem_evento():
    a = _digest_env()
    assert _stop(a, last_assistant_message="oi").returncode == 0
    assert not os.path.exists(os.path.join(a.home, "digest"))
    a.orq("ausente", "ligar")
    a.orq("ausente", "desligar")
    _stop(a, last_assistant_message="oi")
    assert not os.path.exists(os.path.join(a.home, "digest")) and not [e for e in a.events() if e["tipo"] == "resposta_coordenador"]


def test_ausente_falha_do_digest_nao_derruba_o_stop_nem_o_aviso_de_entrada():
    a = _digest_env()
    a.orq("ausente", "ligar")
    open(os.path.join(a.home, "digest"), "w").write("arquivo no lugar da pasta")  # mkdir falha
    _evs(a, _ev("15:00:00", "entrada", id="e9", origem="usuario", texto="sem efeito ainda"))
    r = _stop(a, last_assistant_message="oi")
    assert r.returncode == 0 and "e9" in json.loads(r.stdout)["systemMessage"], r
    assert "digest" in a.log(), "a falha vai para o log"


def test_ausente_stop_do_worker_nao_gera_digest():
    a = _digest_env()
    a.orq("ausente", "ligar")
    a.set("workers.json", [{"handle": "term_coord", "run": "run_do_despacho"}])
    cur = _cursor(a)
    cur["papeis"] = {"abcdef123456": "worker"}
    json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
    _stop(a, last_assistant_message="oi")
    assert not os.path.exists(os.path.join(a.home, "digest"))


# ---------- ticket 48: orq retomar ----------

def _w48(handle, run="run_a", **kw):
    return {"handle": handle, "run": run, "status": "dispatched", "dispatch": "ctx_" + handle, "task": "task_" + handle, **kw}


def _turno48(a, **por_dispatch):
    json.dump({d: {"task": "task_x", "sessao": v[0], "inicio": "2026-09-30T14:00:00Z", "fim": None, **({"cwd": v[1]} if v[1] else {})} for d, v in por_dispatch.items()},
              open(os.path.join(a.home, "turnos.json"), "w"))


def _queda48(a):
    """Depois da queda: o w1 (opus) e o w3 (sem sessão gravada) perderam o terminal, o w2 tem terminal vivo e o w4 já terminou."""
    os.makedirs(a.home, exist_ok=True)
    a.wt = os.path.join(a.tmp.name, "wt")
    for n in ("w1", "w2", "w4"):
        os.makedirs(os.path.join(a.wt, n))
    a.set("workers.json", [_w48("term_w1", modelo="claude-opus-5-5"), _w48("term_w2"), _w48("term_w3"), _w48("term_w4", status="completed")])
    a.set("tasks_run_a.json", [{"id": "task_term_w1", "task_title": "Ticket 99"}, {"id": "task_term_w3", "task_title": "Sem sessão"}])
    a.set("terminals.json", ["term_coord", "term_w2"])
    _turno48(a, ctx_term_w1=("sess-w1", a.wt + "/w1"), ctx_term_w2=("sess-w2", a.wt + "/w2"), ctx_term_w4=("sess-w4", a.wt + "/w4"))


def test_ticket48_hook_do_worker_grava_sessao_e_cwd_do_lancamento_por_dispatch():
    a = Amb(run=None)
    _hook(a, "prompt", prompt=PREAMBULO_24, cwd="/wt/t24")
    t = _turnos(a)["ctx_d24"]
    assert t["sessao"] == "abcdef123456" and t["cwd"] == "/wt/t24", t
    _hook(a, "stop", cwd="/wt/t24/sub")
    _hook(a, "prompt", prompt="ajuste: use o outro arquivo", cwd="/wt/t24/sub")  # o worker deu cd: o cwd de lançamento é o que vale
    assert _turnos(a)["ctx_d24"]["cwd"] == "/wt/t24" and _turnos(a)["ctx_d24"]["sessao"] == "abcdef123456"


def test_ticket48_retomar_dry_run_lista_so_sem_worker_done_e_sem_terminal_vivo():
    a = Amb(run="run_a")
    _queda48(a)
    r = a.orq("retomar", "--dry-run", "--json")
    assert r.returncode == 0, r.stderr
    res = json.loads(r.stdout)
    assert [(w["dispatch"], w["estado"]) for w in res["workers"]] == [("ctx_term_w1", "a_retomar"), ("ctx_term_w3", "sem_sessao")], res
    w1 = res["workers"][0]
    assert (w1["sessao"], w1["cwd"], w1["modelo"], w1["titulo"]) == ("sess-w1", a.wt + "/w1", "claude-opus-5-5", "Ticket 99"), w1
    assert not _log(a, "create.log") and res["gerente"] is None
    assert "a_retomar" in a.orq("retomar", "--dry-run").stdout


def test_ticket48_retomar_cria_o_terminal_com_o_comando_certo_liga_ao_dispatch_e_confere_a_tela():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="2")
    _queda48(a)
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    r = a.orq("retomar", "--json")
    assert r.returncode == 0, r.stderr
    res = json.loads(r.stdout)
    assert [(w["dispatch"], w["estado"], w.get("novo")) for w in res["workers"]] == [("ctx_term_w1", "retomado", "term_ret1"), ("ctx_term_w3", "sem_sessao", None)], res
    (c,) = _log(a, "create.log")
    assert c[c.index("--worktree") + 1] == "path:" + a.wt + "/w1" and c[c.index("--title") + 1] == "Ticket 99 (retomado)", c
    comando = c[c.index("--command") + 1]
    assert comando.startswith("claude --resume sess-w1 --model claude-opus-5-5 --dangerously-skip-permissions 'Continue de onde parou."), comando
    assert "relatorio-final.md" in comando
    (ev,) = [e for e in a.events() if e["tipo"] == "retomada"]
    assert (ev["dispatch"], ev["terminal"], ev["anterior"], ev["sessao"], ev["cwd"]) == ("ctx_term_w1", "term_ret1", "term_w1", "sess-w1", a.wt + "/w1"), ev
    assert "head" in ev and "sujo" in ev, "o checkpoint da worktree fica no evento"
    ags = {x["dispatch"]: x for x in json.loads(a.orq("agentes", "--json").stdout)}
    assert ags["ctx_term_w1"]["terminal"] == "term_ret1", "o terminal novo é o do dispatch"
    again = json.loads(a.orq("retomar", "--dry-run", "--json").stdout)
    assert [w["dispatch"] for w in again["workers"]] == ["ctx_term_w3"], "o retomado não volta à lista"


def test_ticket48_retomar_sessao_que_nao_voltou_ou_terminal_que_nao_abriu_nao_some():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="1")
    _queda48(a)
    a.set("screens.json", {"term_ret1": ["No conversation found with session ID: sess-w1"]})
    (w1, _) = json.loads(a.orq("retomar", "--json").stdout)["workers"]
    assert w1["estado"] == "sem_atividade" and "orq relancar ctx_term_w1" in w1["aviso"], w1
    b = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="1")
    _queda48(b)
    res = json.loads(b.orq("retomar", "--json", FAKE_FAIL_CREATE_PATH="/wt/w1").stdout)
    assert res["workers"][0]["estado"] == "falhou" and "selector_not_found" in res["workers"][0]["aviso"], res
    assert not [e for e in b.events() if e["tipo"] == "retomada"]


def test_ticket48_retomar_worktree_que_sumiu_nao_sobe_terminal():
    a = Amb(run="run_a")
    _queda48(a)
    os.rmdir(a.wt + "/w1")
    (w1, w3) = json.loads(a.orq("retomar", "--json").stdout)["workers"]
    assert w1["estado"] == "sem_worktree" and "não existe" in w1["aviso"], w1
    assert "orq relancar ctx_term_w3" in w3["aviso"], w3
    assert not _log(a, "create.log")


def test_ticket48_retomar_sem_lista_de_terminais_confiavel_recusa():
    a = Amb(run="run_a")
    _queda48(a)
    open(os.path.join(a.fake, "terminals_truncados"), "w").close()
    r = a.orq("retomar")
    assert r.returncode == 1 and "nada foi retomado" in r.stderr, r
    assert not _log(a, "create.log")


def test_ticket48_retomar_sobe_o_painel_do_gerente_e_religa_os_runs():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="1")
    _queda48(a)
    a.set("workers.json", [])
    json.dump({"coordenador": "term_old", "gerente": "term_ger_old", "runs": ["run_a", "run_b"]}, open(os.path.join(a.home, "gerente.json"), "w"))
    seco = json.loads(a.orq("retomar", "--dry-run", "--json").stdout)
    assert seco["gerente"]["estado"] == "a_subir" and not _log(a, "create.log")
    res = json.loads(a.orq("retomar", "--json").stdout)
    assert res["gerente"]["novo"] == "term_ret1" and res["gerente"]["estado"] == "religado", res
    (c,) = _log(a, "create.log")
    assert c[c.index("--command") + 1] == f"sh {os.path.join(a.home, 'painel-agent-manager.sh')}" and "--worktree" not in c, c
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ret1", "runs": ["run_a", "run_b"]}
    assert {c[c.index("--id") + 1] for c in _log(a, "calls.log") if c[0] == "run-use"} == {"run_a", "run_b"}
    assert a.orq("retomar", "--dry-run").stdout.strip() == "nada a retomar", "religado, não há mais o que fazer"


def test_ticket48_gerente_vivo_de_outro_coordenador_vivo_nao_e_tocado():
    a = Amb(run="run_a")
    _queda48(a)
    a.set("workers.json", [])
    a.set("terminals.json", ["term_coord", "term_old", "term_ger_old"])
    velho = {"coordenador": "term_old", "gerente": "term_ger_old", "runs": ["run_a", "run_b"]}
    json.dump(velho, open(os.path.join(a.home, "gerente.json"), "w"))
    assert a.orq("retomar").stdout.strip() == "nada a retomar"
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == velho and not _log(a, "create.log")
    r = a.orq("gerente", "desligar")
    assert r.returncode == 1 and "não está ligado a este coordenador" in r.stderr, r


def test_ticket48_gerente_desligar_depois_da_queda_aceita_o_coordenador_novo():
    a = Amb(run="run_a")
    os.makedirs(a.home, exist_ok=True)
    a.set("terminals.json", ["term_coord"])  # o coordenador antigo (term_old) e o gerente antigo morreram
    json.dump({"coordenador": "term_old", "gerente": "term_ger_old", "runs": ["run_a", "run_b"]}, open(os.path.join(a.home, "gerente.json"), "w"))
    r = a.orq("gerente", "desligar")
    assert r.returncode == 0, r.stderr
    assert not os.path.exists(os.path.join(a.home, "gerente.json"))
    assert {c[c.index("--id") + 1] for c in _log(a, "calls.log") if c[0] == "run-use"} == {"run_a", "run_b"}


def test_ticket48_gerente_ligar_depois_da_queda_troca_coordenador_e_gerente_e_guarda_os_runs():
    a = Amb(run="run_c")
    os.makedirs(a.home, exist_ok=True)
    a.set("terminals.json", ["term_coord", "term_ger"])
    json.dump({"coordenador": "term_old", "gerente": "term_ger_old", "runs": ["run_a", "run_b"]}, open(os.path.join(a.home, "gerente.json"), "w"))
    r = a.orq("gerente", "ligar", "--terminal", "term_ger")
    assert r.returncode == 0, r.stderr
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ger", "runs": ["run_a", "run_b", "run_c"]}
    b = Amb(run="run_c")  # sem queda (o antigo segue vivo): outro gerente recomeça a lista, como antes
    os.makedirs(b.home, exist_ok=True)
    b.set("terminals.json", ["term_coord", "term_ger", "term_old", "term_ger_old"])
    json.dump({"coordenador": "term_old", "gerente": "term_ger_old", "runs": ["run_a", "run_b"]}, open(os.path.join(b.home, "gerente.json"), "w"))
    assert b.orq("gerente", "ligar", "--terminal", "term_ger").returncode == 0
    assert json.load(open(os.path.join(b.home, "gerente.json")))["runs"] == ["run_c"]



# ---------- ticket 54: --assumir troca de coordenador e de gerente sem mover o gerente.json ----------

def _gerente_de_outro_vivo(run):
    a = Amb(run=run)
    os.makedirs(a.home, exist_ok=True)
    a.set("terminals.json", ["term_coord", "term_ger", "term_old", "term_ger_old"])  # o coordenador e o gerente antigos seguem no Orca
    json.dump({"coordenador": "term_old", "gerente": "term_ger_old", "runs": ["run_a", "run_b"]}, open(os.path.join(a.home, "gerente.json"), "w"))
    return a


def test_it_should_take_over_the_manager_of_another_live_coordinator_when_asked_to_on_ligar():
    a = _gerente_de_outro_vivo("run_c")
    r = a.orq("gerente", "ligar", "--terminal", "term_ger", "--assumir")
    assert r.returncode == 0, r.stderr
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ger", "runs": ["run_a", "run_b", "run_c"]}
    assert {c[c.index("--id") + 1] for c in _log(a, "calls.log") if c[0] == "run-use"} == {"run_c"}, "só o Run pedido é religado; os herdados já estavam no gerente"


def test_it_should_take_over_the_manager_of_another_live_coordinator_when_asked_to_on_desligar():
    a = _gerente_de_outro_vivo("run_a")
    assert a.orq("gerente", "desligar").returncode == 1, "sem --assumir o coordenador de outro segue intocado"
    r = a.orq("gerente", "desligar", "--assumir")
    assert r.returncode == 0, r.stderr
    assert not os.path.exists(os.path.join(a.home, "gerente.json"))
    assert {c[c.index("--id") + 1] for c in _log(a, "calls.log") if c[0] == "run-use"} == {"run_a", "run_b"}


# ---------- ticket 54: gerente morto avisado no prompt e religado por `orq gerente subir` ----------

def _listas_de_terminais(a):
    return [c for c in _log(a, "calls.log") if c[:2] == ["list", "--limit"] or c[:1] == ["list"]]


def _contexto(a):
    return json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]


def test_it_should_warn_on_the_prompt_when_the_manager_terminal_is_gone_and_say_how_to_raise_it():
    a = Amb(run="run_a")
    _gerente(a)
    a.set("terminals.json", ["term_coord"])  # o terminal do gerente sumiu do Orca
    _painel_tocado(a, 200)
    _contexto(a)  # a primeira checagem roda fora do hook
    ctx = _contexto(a)
    assert "sumiu" in ctx and "term_ger" in ctx and "orq gerente subir" in ctx, ctx


def test_it_should_not_ask_the_orca_while_the_manager_stamp_is_fresh_nor_more_than_once_a_minute():
    a = Amb(run="run_a")
    _gerente(a)
    a.set("terminals.json", ["term_coord"])
    _painel_tocado(a, 20)
    for _ in range(3):
        assert "sumiu" not in _contexto(a)
    assert not _listas_de_terminais(a), "carimbo fresco: nada de Orca a cada prompt"
    _painel_tocado(a, 200)
    for _ in range(3):
        _contexto(a)
    assert len(_listas_de_terminais(a)) == 1, _listas_de_terminais(a)


def test_it_should_keep_the_old_stopped_message_when_the_terminal_exists_but_the_panel_stopped():
    a = Amb(run="run_a")
    _gerente(a)
    a.set("terminals.json", ["term_coord", "term_ger"])
    _painel_tocado(a, 200)
    _contexto(a)
    ctx = _contexto(a)
    assert "parado há 3 min" in ctx and "sumiu" not in ctx, ctx


def test_it_should_raise_a_new_manager_terminal_and_rebind_every_run_of_the_gerente_json():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="1")
    os.makedirs(a.home, exist_ok=True)
    a.set("terminals.json", ["term_coord"])
    json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": ["run_a", "run_b"]}, open(os.path.join(a.home, "gerente.json"), "w"))
    r = a.orq("gerente", "subir")
    assert r.returncode == 0, r.stderr
    (c,) = _log(a, "create.log")
    assert c[c.index("--command") + 1] == f"sh {os.path.join(a.home, 'painel-agent-manager.sh')}", c
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ret1", "runs": ["run_a", "run_b"]}
    assert {c[c.index("--id") + 1] for c in _log(a, "calls.log") if c[0] == "run-use"} == {"run_a", "run_b"}
    assert "sumiu" not in _contexto(a), "depois de subir o aviso some"


def test_it_should_refuse_to_raise_the_manager_while_its_terminal_still_exists():
    a = Amb(run="run_a")
    _gerente(a)
    a.set("terminals.json", ["term_coord", "term_ger"])
    r = a.orq("gerente", "subir")
    assert r.returncode == 1 and "ainda existe" in r.stderr and not _log(a, "create.log"), r
    assert a.orq("gerente", "subir", "--forcar").returncode == 0
    assert a.orq("gerente", "subir", "--forcar", ORQ_HOME=os.path.join(a.tmp.name, "vazio")).returncode == 1, "sem gerente.json não há o que subir"


# ---------- ticket 54: prligar com PRs de branches diferentes ----------

PR3 = "https://github.com/acme/app/pull/1230"
PR4 = "https://github.com/acme/app/pull/1231"


def _despacho_por_nome(a, task, nome, dispatch):
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"ts": "2026-09-30T03:00:00Z", "tipo": "despacho", "run": "run_a", "task": task, "dispatch": dispatch, "worktree": "new-top-level", "nome": nome}) + "\n")


def test_it_should_link_each_pr_of_a_loop_to_the_task_that_owns_its_own_branch():
    a, p, w = _ambiente_46()
    a.set("workers.json", [])
    _despacho_por_nome(a, "task_e2e", "fix/e2e-x", "ctx_e")
    _despacho_por_nome(a, "task_2116", "fix/2116-y", "ctx_y")
    for url, head in ((PR1, "fix/e2e-x"), (PR2, "fix/e2e-x"), (PR3, "fix/2116-y"), (PR4, "fix/2116-y")):
        _pr(a, url)
        dados = _log_json(a, "gh.json", {})
        dados[url]["headRefName"] = head
        a.set("gh.json", dados)
    _pos_pr(a, p, cmd='for h in $HEADS; do gh pr create --head "$h" --base development; done', saida="\n".join([PR1, PR2, PR3, PR4]) + "\n")
    por_task = {i["url"]: i["task"] for i in _prs_json(a)["itens"]}
    assert por_task == {PR1: "task_e2e", PR2: "task_e2e", PR3: "task_2116", PR4: "task_2116"}, por_task


def test_it_should_keep_one_literal_head_for_every_url_of_the_command():
    a, p, w = _ambiente_46()
    _pos_pr(a, p, cmd="for b in development staging; do gh pr create --head feat/w --base $b; done", saida=PR1 + "\n" + PR2 + "\n")
    assert {i["task"] for i in _prs_json(a)["itens"]} == {"task_feat1"} and len(_prs_json(a)["itens"]) == 2


# ---------- ticket 50: prligar com vários PRs, fila do E2E, aviso de PR uma vez só ----------

def test_it_should_link_every_pr_url_in_one_gh_pr_create_command():
    a, p, w = _ambiente_46()
    saida = PR1 + "\n" + PR2 + "\n"
    r = _pos_pr(a, w, cmd="for b in development staging; do gh pr create --base $b --title x; done", saida=saida)
    itens = _prs_json(a)["itens"]
    assert sorted(i["url"] for i in itens) == [PR1, PR2] and {i["task"] for i in itens} == {"task_feat1"}, itens
    ctx = json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]
    assert "#1216" in ctx and "#1220" in ctx, ctx


def test_it_should_link_only_the_urls_not_yet_linked_when_a_command_creates_two_prs():
    a, p, w = _ambiente_46()
    _pos_pr(a, w, saida=PR1 + "\n")
    _pos_pr(a, w, cmd="gh pr create --base staging", saida=PR1 + "\n" + PR2 + "\n")
    assert sorted(i["url"] for i in _prs_json(a)["itens"]) == [PR1, PR2]


def _ticket_e2e(fila, nome, pid, vivo, sessao=False, inicio=0, projeto="e2e-x", worktree="/w/feat-x"):
    d = os.path.join(fila, nome)
    os.makedirs(os.path.join(d, "pids"))
    open(os.path.join(d, "owner"), "w").write(f"pid={pid}\nworktree={worktree}\nproject={projeto}\ncommand=teste\nstarted={inicio}\nstarted_at=x\n")
    if vivo:
        open(os.path.join(d, "pids", str(os.getpid())), "w").write("lstart")
    else:
        open(os.path.join(d, "pids", "999999"), "w").write("lstart")
    if sessao:
        open(os.path.join(d, "session"), "w").close()
    open(os.path.join(d, "acquired"), "w").write(str(inicio))


def test_it_should_show_who_holds_the_e2e_queue_and_how_many_wait():
    with tempfile.TemporaryDirectory() as fila:
        assert orq_mod.fila_e2e(fila) is None and orq_mod.linha_e2e(None) == ""
        _ticket_e2e(fila, "0000000001-1", 1, vivo=True, inicio=1000)
        _ticket_e2e(fila, "0000000002-2", 2, vivo=True, inicio=1500, projeto="e2e-y")
        _ticket_e2e(fila, "0000000003-3", 3, vivo=True, inicio=1600, projeto="e2e-z")
        f = orq_mod.fila_e2e(fila, agora=1000 + 600)
        assert (f["projeto"], f["min"], f["esperam"], f["presa"]) == ("e2e-x", 10, 2, None), f
        assert "e2e-x" in orq_mod.linha_e2e(f) and "10 min" in orq_mod.linha_e2e(f) and "2 esperando" in orq_mod.linha_e2e(f) and "PRESA" not in orq_mod.linha_e2e(f)


def test_it_should_mark_the_e2e_queue_stuck_when_the_owner_died_or_the_session_has_no_test():
    with tempfile.TemporaryDirectory() as fila:
        _ticket_e2e(fila, "0000000001-1", 1, vivo=False, inicio=1000)
        _ticket_e2e(fila, "0000000002-2", 2, vivo=True, inicio=1100)
        f = orq_mod.fila_e2e(fila, agora=1100)
        assert "dono" in f["presa"] and f["esperam"] == 1 and "PRESA" in orq_mod.linha_e2e(f), f
    with tempfile.TemporaryDirectory() as fila:
        _ticket_e2e(fila, "0000000001-1", 1, vivo=False, sessao=True, inicio=1000)
        assert orq_mod.fila_e2e(fila, agora=1000 + 5 * 60)["presa"] is None, "sessão recém-aberta ainda não está presa"
        f = orq_mod.fila_e2e(fila, agora=1000 + 30 * 60)
        assert "sessão" in f["presa"] and f["min"] == 30, f


def test_it_should_tell_the_coordinator_once_when_the_e2e_queue_is_stuck():
    with tempfile.TemporaryDirectory() as home, tempfile.TemporaryDirectory() as fila:
        antes, dig = orq_mod.HOME, orq_mod.digita
        enviados = []
        orq_mod.HOME, orq_mod.digita = home, lambda h, t: enviados.append((h, t)) or "enviado"
        try:
            json.dump({"coordenador": "term_c", "gerente": "term_g", "runs": []}, open(os.path.join(home, "gerente.json"), "w"))
            _away_ligado(home)
            _ticket_e2e(fila, "0000000001-1", 1, vivo=False, sessao=True, inicio=0)
            f = orq_mod.fila_e2e(fila, agora=40 * 60)
            assert orq_mod.avisa_fila_e2e(f) and len(enviados) == 1 and "PRESA" in enviados[0][1], enviados
            assert orq_mod.avisa_fila_e2e(f) == [] and len(enviados) == 1, "o mesmo ticket não avisa de novo"
            assert orq_mod.avisa_fila_e2e(None) == []
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig


def _prs_avisar(home, avisado=False):
    json.dump({"itens": [{"task": "task_a", "url": PR1, "numero": 1216, "base": "development", "estado": "mergeado", "avisado": avisado,
                          "entrada": "e292", "texto": "PR #1216 entrou em development (task_a)"}], "sem_task": []}, open(os.path.join(home, "prs.json"), "w"))
    json.dump({"coordenador": "term_c", "gerente": "term_g", "runs": []}, open(os.path.join(home, "gerente.json"), "w"))
    _away_ligado(home)


def test_it_should_type_the_pr_notice_once_even_when_another_panel_runs_at_the_same_time():
    with tempfile.TemporaryDirectory() as home:
        antes, dig = orq_mod.HOME, orq_mod.digita
        enviados = []

        def digita(h, t):
            enviados.append(t)
            if len(enviados) == 1:
                assert orq_mod.pr_avisar() == [], "o segundo painel chega no meio do envio do primeiro"
            return "enviado"
        orq_mod.HOME, orq_mod.digita = home, digita
        try:
            _prs_avisar(home)
            assert len(orq_mod.pr_avisar()) == 1 and len(enviados) == 1, enviados
            assert orq_mod.pr_avisar() == [] and len(enviados) == 1
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig


def test_it_should_not_retype_the_pr_notice_after_a_restart_that_lost_the_flag():
    with tempfile.TemporaryDirectory() as home:
        antes, dig = orq_mod.HOME, orq_mod.digita
        enviados = []
        orq_mod.HOME, orq_mod.digita = home, lambda h, t: enviados.append(t) or "enviado"
        try:
            _prs_avisar(home)
            orq_mod.append_event({"tipo": "pr", "op": "avisado", "task": "task_a", "url": PR1, "numero": 1216})
            assert orq_mod.pr_avisar() == [] and enviados == [], "o log já diz que foi avisado"
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig


def test_it_should_retry_the_pr_notice_when_the_coordinator_was_busy():
    with tempfile.TemporaryDirectory() as home:
        antes, dig = orq_mod.HOME, orq_mod.digita
        respostas = iter(["ocupado", "enviado"])
        orq_mod.HOME, orq_mod.digita = home, lambda h, t: next(respostas)
        try:
            _prs_avisar(home)
            assert orq_mod.pr_avisar() == [] and len(orq_mod.pr_avisar()) == 1
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig


# ---------- ticket 82: o aviso não cai no meio do que o usuário digita ----------

def _usuario_falou(home, minutos):
    """Grava o prompt do usuário `minutos` atrás no events.jsonl."""
    ts = (datetime.now(timezone.utc) - timedelta(minutes=minutos)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(os.path.join(home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"ts": ts, "tipo": "entrada", "origem": "usuario", "id": "e1", "texto": "oi"}) + "\n")


def _fila_presa(fila):
    _ticket_e2e(fila, "0000000001-1", 1, vivo=False, sessao=True, inicio=0)
    return orq_mod.fila_e2e(fila, agora=40 * 60)


def test_it_should_not_type_the_notices_while_the_user_talks_to_the_coordinator():
    with tempfile.TemporaryDirectory() as home, tempfile.TemporaryDirectory() as fila:
        antes, dig = orq_mod.HOME, orq_mod.digita
        enviados = []
        orq_mod.HOME, orq_mod.digita = home, lambda h, t: enviados.append(t) or "enviado"
        try:
            _prs_avisar(home)
            _usuario_falou(home, 2)
            assert len(orq_mod.pr_avisar()) == 1 and orq_mod.avisa_fila_e2e(_fila_presa(fila)), "adiado já é entrega: o estado marca avisado"
            assert enviados == [], f"o coordenador tem gente: {enviados}"
            assert orq_mod.pr_avisar() == [] and orq_mod.avisos_entregar() == [] and enviados == []
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig


def test_it_should_show_the_untyped_notices_once_in_the_next_prompt_context():
    a = Amb()
    with tempfile.TemporaryDirectory() as fila:
        antes, dig = orq_mod.HOME, orq_mod.digita
        orq_mod.HOME, orq_mod.digita = a.home, lambda h, t: _nao_digita()
        try:
            os.makedirs(a.home, exist_ok=True)
            json.dump({"coordenador": "term_c", "gerente": "term_g", "runs": []}, open(os.path.join(a.home, "gerente.json"), "w"))
            _prs_avisar(a.home)
            _usuario_falou(a.home, 1)
            assert orq_mod.avisa_fila_e2e(_fila_presa(fila)) and len(orq_mod.pr_avisar()) == 1
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "não foram digitados" in ctx and "PRESA" in ctx, ctx
    assert "PR #1216" not in ctx, "o PR aparece na linha PR: do resumo (a entrada), a fila não o repete"
    ctx2 = json.loads(a.prompt("mais uma").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "não foram digitados" not in ctx2 and "PRESA" not in ctx2, "o aviso sai uma vez só"


def _nao_digita():
    raise AssertionError("o coordenador tem gente: nada é digitado")


def test_it_should_type_a_queued_notice_only_after_the_coordinator_is_idle_for_n_minutes():
    with tempfile.TemporaryDirectory() as home, tempfile.TemporaryDirectory() as fila:
        antes, dig = orq_mod.HOME, orq_mod.digita
        enviados = []
        orq_mod.HOME, orq_mod.digita = home, lambda h, t: enviados.append(t) or "enviado"
        try:
            json.dump({"coordenador": "term_c", "gerente": "term_g", "runs": []}, open(os.path.join(home, "gerente.json"), "w"))
            _away_ligado(home)
            _usuario_falou(home, 2)
            assert orq_mod.avisa_fila_e2e(_fila_presa(fila)) and enviados == []
            assert orq_mod.avisos_entregar() == [] and enviados == [], "2 min: ainda tem gente"
            os.remove(os.path.join(home, "events.jsonl"))
            _usuario_falou(home, 30)
            assert orq_mod.avisos_entregar() and len(enviados) == 1 and "PRESA" in enviados[0], enviados
            assert orq_mod.avisos_entregar() == [] and len(enviados) == 1, "o aviso digitado sai da fila"
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig


def test_it_should_keep_the_queued_notice_when_the_idle_coordinator_is_busy_or_has_a_draft():
    with tempfile.TemporaryDirectory() as home, tempfile.TemporaryDirectory() as fila:
        antes, dig = orq_mod.HOME, orq_mod.digita
        respostas = iter(["rascunho", "enviado"])
        orq_mod.HOME, orq_mod.digita = home, lambda h, t: next(respostas)
        try:
            json.dump({"coordenador": "term_c", "gerente": "term_g", "runs": []}, open(os.path.join(home, "gerente.json"), "w"))
            _away_ligado(home)
            _usuario_falou(home, 2)
            orq_mod.avisa_fila_e2e(_fila_presa(fila))
            os.remove(os.path.join(home, "events.jsonl"))
            _usuario_falou(home, 30)
            assert orq_mod.avisos_entregar() == [] and len(orq_mod.avisos_entregar()) == 1
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig


def test_ticket86_aviso_de_pressao_da_maquina_nao_e_digitado_com_o_usuario_no_coordenador():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Vivo", SONNET)])
    a.maquina(carga=40)
    _usuario_falou(a.home, 1)
    _desp79(a, "Ticket 05", 1)
    for _ in range(2):
        assert a.orq("gerente", "absorver").returncode == 0
    assert not _log(a, "send.log"), "o coordenador tem gente: nada é digitado"
    assert [e["tipo"] for e in a.events() if e["tipo"] == "maquina_aviso"] == ["maquina_aviso"], "adiado é entrega: um aviso por episódio"
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "não foram digitados" in ctx and "máquina sob pressão" in ctx, ctx


def test_ticket86_gerente_absorver_acorda_o_coordenador_parado_ha_mais_de_dois_minutos_e_adia_antes_disso():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}), run="run_b")
    _usuario_falou(a.home, 5)  # fora da janela de 2 min, dentro dos 10 do aviso comum
    assert a.orq("gerente", "absorver").returncode == 0
    (env,) = _log(a, "send.log")
    assert "--run run_b" in env[env.index("--text") + 1], "worker_done acorda o coordenador sem esperar 10 min"


def test_ticket86_gerente_absorver_nao_digita_o_aviso_enquanto_o_usuario_fala_com_o_coordenador():
    """O texto que cortou a digitação em 01/10 era o aviso do próprio gerente (`--terminal <gerente>`), digitado direto pelo gerente_absorver."""
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a", "run_b"])
    a.caixa(("worker_done", {"taskId": "task_1", "dispatchId": "ctx_1"}), run="run_b")
    _usuario_falou(a.home, 1)
    r = a.orq("gerente", "absorver")
    assert r.returncode == 0, r.stderr
    assert _log(a, "send.log") == [], "o coordenador tem gente: o aviso do gerente não é digitado"
    assert "avisado ao coordenador" in r.stdout, r.stdout
    a.orq("gerente", "absorver")
    assert _log(a, "send.log") == [], "adiado já é entrega: a volta seguinte não o repete"
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "não foram digitados" in ctx and "check --run run_b" in ctx, ctx


def test_it_should_not_type_when_the_second_read_finds_text_in_the_box():
    antes, orca_ = orq_mod.terminal_livre, orq_mod.orca
    chamadas, envios = [], []
    orq_mod.orca = lambda *a, **k: envios.append(a) or {"send": {"prompt": {"observation": "unsupported"}}}
    try:
        respostas = iter([None, "rascunho"])
        orq_mod.terminal_livre = lambda h: chamadas.append(h) or next(respostas)
        assert orq_mod.digita("term_c", "orq: PR #1") == "rascunho" and len(chamadas) == 2 and envios == [], "o usuário começou a digitar entre as leituras"
        respostas = iter([None, None])
        assert orq_mod.digita("term_c", "orq: PR #1") == "enviado" and len(envios) == 1
    finally:
        orq_mod.terminal_livre, orq_mod.orca = antes, orca_


def test_it_should_not_type_into_a_busy_worker_when_the_second_read_finds_a_draft():
    orca_ = orq_mod.orca
    telas = iter([{"tail": ["esc to interrupt"]}, {"tail": ["esc to interrupt"], "draft": "estou escrevendo"}])
    envios = []
    orq_mod.orca = lambda *a, **k: envios.append(a) or {"terminal": next(telas)} if a[0] == "read" else envios.append(a) or {}
    try:
        assert orq_mod.digita_ocupado("term_w", "orq: ajuste") == "ocupado" and not any(x[0] == "send" for x in envios)
        telas = iter([{"tail": ["esc to interrupt"]}, {"tail": ["esc to interrupt"]}])
        assert orq_mod.digita_ocupado("term_w", "orq: ajuste") == "ocupado_digitado"
    finally:
        orq_mod.orca = orca_


def test_it_should_not_record_the_typed_orq_notices_as_user_prompts():
    for t in ("orq: PR #1 entrou em staging", "orq: Fila do E2E: x PRESA", "orq: uso do plano, semana em 93%", "orq: worker task_a pergunta na tela"):
        assert orq_mod.origem(t) == "aviso_orq", t
    assert orq_mod.origem("orq: faça isso") == "usuario"


def test_it_should_notify_on_macos_only_when_the_config_turns_it_on():
    with tempfile.TemporaryDirectory() as home, tempfile.TemporaryDirectory() as fila:
        antes, dig, env = orq_mod.HOME, orq_mod.digita, os.environ.get("ORQ_OSASCRIPT")
        log = os.path.join(home, "osascript.log")
        fake = os.path.join(home, "osascript")
        open(fake, "w").write(f"#!/bin/sh\necho \"$@\" >> {log}\n")
        os.chmod(fake, 0o755)
        os.environ["ORQ_OSASCRIPT"] = fake
        orq_mod.HOME, orq_mod.digita = home, lambda h, t: _nao_digita()
        try:
            _usuario_falou(home, 1)
            cfg = {"coordenador": "term_c", "gerente": "term_g", "runs": []}
            json.dump(cfg, open(os.path.join(home, "gerente.json"), "w"))
            orq_mod.avisa_fila_e2e(_fila_presa(fila))
            assert not os.path.exists(log), "desligada por padrão"
            os.remove(os.path.join(home, "e2e-aviso.json"))
            json.dump({**cfg, "notificar_macos": True}, open(os.path.join(home, "gerente.json"), "w"))
            orq_mod.avisa_fila_e2e(orq_mod.fila_e2e(fila, agora=40 * 60))
            assert "display notification" in open(log).read() and "PRESA" in open(log).read()
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig
            os.environ.pop("ORQ_OSASCRIPT", None) if env is None else os.environ.__setitem__("ORQ_OSASCRIPT", env)


# ---------- ticket 51: orçamento de uso do plano e pausa por prioridade ----------

FIXTURE_HUD = os.path.join(AQUI, "fixtures", "hud-stdin.json")


def _uso51(a, semana=None, cinco_h=None, idade_s=0, sem_reset=3 * 86400, cinco_reset=3600):
    """Grava um quadro do HUD (stdin.<sessão>.json) a partir da fixture, com os percentuais dados e `idade_s` de idade."""
    d = json.load(open(FIXTURE_HUD))
    agora = time.time()
    if semana is not None:
        d["rate_limits"]["seven_day"] = {"used_percentage": semana, "resets_at": int(agora + sem_reset)}
    if cinco_h is not None:
        d["rate_limits"]["five_hour"] = {"used_percentage": cinco_h, "resets_at": int(agora + cinco_reset)}
    os.makedirs(a.env["ORQ_HUD_CACHE"], exist_ok=True)
    f = os.path.join(a.env["ORQ_HUD_CACHE"], "stdin.sessao-1.json")
    json.dump(d, open(f, "w"))
    os.utime(f, (agora - idade_s, agora - idade_s))


def test_ticket51_uso_le_o_rate_limits_do_quadro_do_hud_sem_chamar_o_orca():
    a = Amb()
    _uso51(a, semana=93, cinco_h=86)
    u = json.loads(a.orq("uso", "--json").stdout)
    assert (u["uso"]["semana"], u["uso"]["cinco_h"], u["nivel"]) == (93, 86, "pausa"), u
    assert "semana em 93% (limiar 92%), vira em 2d" in u["motivo"], u
    assert not _log(a, "calls.log"), "ler o uso não fala com o Orca"
    _uso51(a, semana=80, cinco_h=50)
    assert json.loads(a.orq("uso", "--json").stdout)["nivel"] == "ok"
    _uso51(a, semana=86, cinco_h=50)
    assert json.loads(a.orq("uso", "--json").stdout)["nivel"] == "avisa"


def test_ticket51_janela_que_ja_virou_vale_zero_e_quadro_velho_nao_vale():
    a = Amb()
    _uso51(a, semana=95, sem_reset=-60, cinco_h=10)
    assert json.loads(a.orq("uso", "--json").stdout)["nivel"] == "ok", "a semana virou depois do quadro"
    _uso51(a, semana=95, idade_s=3600)
    u = json.loads(a.orq("uso", "--json").stdout)
    assert u["uso"] is None and u["nivel"] == "desconhecido", u


def test_ticket51_limiares_vem_do_uso_json():
    a = Amb()
    os.makedirs(a.home, exist_ok=True)
    json.dump({"semana_pausa": 97}, open(os.path.join(a.home, "uso.json"), "w"))
    _uso51(a, semana=95, cinco_h=10)
    assert json.loads(a.orq("uso", "--json").stdout)["nivel"] == "avisa"


def test_ticket51_despachar_recusa_acima_do_limiar_da_semana_e_da_janela_de_5h():
    a = Amb(run="run_a")
    _uso51(a, semana=93, cinco_h=10)
    r = _despachar(a)
    assert r.returncode == 1 and "uso do plano" in r.stderr and "semana em 93%" in r.stderr and "orq pausar" in r.stderr, r
    assert not _log(a, "started.log"), "nada foi despachado"
    _uso51(a, semana=50, cinco_h=91)
    r = _despachar(a)
    assert r.returncode == 1 and "janela de 5 h em 91%" in r.stderr and "janela virar" in r.stderr, r
    assert not _log(a, "started.log")
    _uso51(a, semana=90, cinco_h=89)
    assert _despachar(a).returncode == 0, "abaixo dos limiares de pausa e de segurar o despacho sai"
    assert not [c for c in _log(a, "calls.log") if c[0] == "worker-start"][:0] and len(_log(a, "started.log")) == 1


def test_ticket51_gerente_avisa_o_coordenador_uma_vez_por_nivel_e_janela():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
    _gerente(a)
    _uso51(a, semana=93)
    assert a.orq("gerente", "absorver").returncode == 0
    a.orq("gerente", "absorver")
    (env,) = _log(a, "send.log")
    assert env[env.index("--terminal") + 1] == "term_coord"
    assert "uso do plano, semana em 93%" in env[env.index("--text") + 1] and "orq pausar" in env[env.index("--text") + 1]
    assert [e["nivel"] for e in a.events() if e["tipo"] == "uso_aviso"] == ["pausa"]
    _uso51(a, semana=50)  # voltou ao normal: o aviso reabre
    a.orq("gerente", "absorver")
    _uso51(a, semana=94)
    a.orq("gerente", "absorver")
    assert len(_log(a, "send.log")) == 2, "cruzou de novo: avisa de novo"


def test_ticket51_prioridade_no_despacho_e_padrao_pela_frente():
    a = Amb(run="run_a")
    assert _despachar(a, "--prioridade", "1").returncode == 0
    assert [e.get("prioridade") for e in a.events() if e["tipo"] == "despacho"] == [1]
    r = _despachar(a, "--prioridade", "4")
    assert r.returncode == 2 and "invalid choice" in r.stderr, r
    p = orq_mod.prioridade_padrao
    assert (p("Segurança: rotacionar segredos"), p("Deploy em produção"), p("Failover 31"), p("Painel do orq"), p("Diagnóstico de cache"), p("Ticket 07")) == (1, 1, 3, 3, 3, 2)


def _pausa51(a):
    """Quatro workers vivos no run_a: segurança (P1, implementando), failover (P3, implementando), ticket 07 (P2, investigando) e painel (P3, em review)."""
    os.makedirs(a.home, exist_ok=True)
    a.wt = os.path.join(a.tmp.name, "wt")
    nomes = {"s": "Segurança: rotacionar segredos", "f": "Failover 31", "i": "Ticket 07", "p": "Painel do orq"}
    for n in nomes:
        os.makedirs(os.path.join(a.wt, n))
    a.set("workers.json", [_w48("term_" + n, modelo="claude-opus-5-5") for n in nomes])
    a.set("tasks_run_a.json", [{"id": "task_term_" + n, "task_title": t, "status": "dispatched", "dispatch_id": "ctx_term_" + n, "created_at": _iso(-900)} for n, t in nomes.items()])
    a.set("terminals.json", ["term_coord", *("term_" + n for n in nomes)])
    _turno48(a, **{"ctx_term_" + n: ("sess-" + n, a.wt + "/" + n) for n in nomes})
    _inbox(a, _hbi(1, "ctx_term_s", "implementing", -30), _hbi(2, "ctx_term_f", "implementing", -30), _hbi(3, "ctx_term_i", "investigating", -30),
           _hbi(4, "ctx_term_p", "reviewing", -30))


def _escreve_pausa51(a, *nomes, depois=1.0):
    """O worker obedece: depois de `depois` s escreve o PAUSA.md novo na worktree dele."""
    n0 = len(_log(a, "send.log"))

    def escreve():
        fim = time.time() + 30  # só depois do steer de cada worker: a linha de base do PAUSA.md é tirada antes dele, e sob carga 1 s pode vir antes
        while len(_log(a, "send.log")) < n0 + len(nomes) and time.time() < fim:
            time.sleep(0.05)
        time.sleep(depois)
        for n in nomes:
            f = os.path.join(a.wt, n, "PAUSA.md")
            open(f, "w").write("parei em X; próximo passo Y\n")
            os.utime(f, (time.time() + 5, time.time() + 5))
    t = ThreadPoolExecutor(1)
    return t.submit(escreve)


def test_ticket51_pausar_sem_argumento_pausa_baixa_e_investigando_e_poupa_review_e_alta():
    a = Amb(run="run_a", ORQ_PAUSA_ESPERA_S="6", ORQ_PAUSA_POLL_S="0.2")
    _pausa51(a)
    seco = json.loads(a.orq("pausar", "--dry-run", "--json").stdout)
    assert sorted((w["task"], w["estado"]) for w in seco["pausados"]) == [("task_term_f", "a_pausar"), ("task_term_i", "a_pausar")], seco
    assert [(w["task"], w["fase"]) for w in seco["preservados"]] == [("task_term_p", "reviewing")], "P3 em verificação final fica"
    assert not _enviados(a) and not _log(a, "close.log")
    f = _escreve_pausa51(a, "f", "i")
    r = a.orq("pausar", "--json")
    f.result()
    assert r.returncode == 0, r.stderr
    res = json.loads(r.stdout)
    assert sorted((w["task"], w["estado"], w["prioridade"]) for w in res["pausados"]) == [("task_term_f", "pausado", 3), ("task_term_i", "pausado", 2)], res
    assert {x[x.index("--to") + 1] for x in _enviados(a)} == {"dispatch:ctx_term_f", "dispatch:ctx_term_i"}
    assert all("PAUSA.md" in x[x.index("--body") + 1] for x in _enviados(a))
    assert sorted(c[c.index("--terminal") + 1] for c in _log(a, "close.log")) == ["term_f", "term_i"], "só os pausados perdem o terminal"
    p = _cursor(a)["pausados"]
    assert set(p) == {"ctx_term_f", "ctx_term_i"} and (p["ctx_term_f"]["sessao"], p["ctx_term_f"]["cwd"], p["ctx_term_f"]["modelo"]) == ("sess-f", a.wt + "/f", "claude-opus-5-5"), p
    assert {e["dispatch"] for e in a.events() if e["tipo"] == "pausa_plano"} == {"ctx_term_f", "ctx_term_i"}


def test_ticket51_pausar_ate_prioridade_e_por_task_e_sem_pausa_md_mantem_o_terminal():
    a = Amb(run="run_a", ORQ_PAUSA_ESPERA_S="1.5", ORQ_PAUSA_POLL_S="0.2")
    _pausa51(a)
    r = json.loads(a.orq("pausar", "--ate-prioridade", "2", "--dry-run", "--json").stdout)
    assert sorted(w["task"] for w in r["pausados"]) == ["task_term_f", "task_term_i"], "P2 e P3; a review P3 continua poupada"
    r = json.loads(a.orq("pausar", "--ate-prioridade", "1", "--dry-run", "--json").stdout)
    assert sorted(w["task"] for w in r["pausados"]) == ["task_term_f", "task_term_i", "task_term_s"]
    r = json.loads(a.orq("pausar", "task_term_p", "--dry-run", "--json").stdout)
    assert [w["task"] for w in r["pausados"]] == ["task_term_p"] and r["preservados"] == [], "a task nomeada vale sozinha, até em review"
    assert a.orq("pausar", "task_inexistente").returncode == 1
    velho = os.path.join(a.wt, "f", "PAUSA.md")
    open(velho, "w").write("de ontem\n")
    os.utime(velho, (1, 1))
    res = json.loads(a.orq("pausar", "task_term_f", "--json").stdout)
    assert [(w["estado"]) for w in res["pausados"]] == ["sem_pausa_md"], "PAUSA.md antigo não vale"
    assert not _log(a, "close.log") and not os.path.exists(os.path.join(a.home, "cursor.json")), "sem arquivo novo o worker segue com o terminal"


def test_ticket51_pausar_nao_pausa_worker_sem_sessao_gravada():
    a = Amb(run="run_a", ORQ_PAUSA_ESPERA_S="1")
    _pausa51(a)
    json.dump({}, open(os.path.join(a.home, "turnos.json"), "w"))
    a.set("tasks_run_a.json", [{"id": "task_term_f", "task_title": "Failover 31", "status": "dispatched", "dispatch_id": "ctx_term_f", "created_at": _iso(-900)}])
    res = json.loads(a.orq("pausar", "task_term_f", "--json").stdout)
    assert [w["estado"] for w in res["pausados"]] == ["sem_sessao"] and not _enviados(a) and not _log(a, "close.log"), res


def test_ticket51_retomar_pausados_sobe_com_resume_e_so_os_pausados_e_o_retomar_comum_os_ignora():
    a = Amb(run="run_a", ORQ_PAUSA_ESPERA_S="6", ORQ_PAUSA_POLL_S="0.2", ORQ_RETOMAR_ESPERA_S="2")
    _pausa51(a)
    json.dump({"max_caros": 4}, open(os.path.join(a.home, "maquina.json"), "w"))  # os quatro workers da fixture são Opus: aqui a ordem é o que se confere, não o teto de caros
    f = _escreve_pausa51(a, "f", "i")
    assert a.orq("pausar").returncode == 0
    f.result()
    a.set("terminals.json", ["term_coord", "term_s", "term_p"])  # f e i fecharam
    assert a.orq("retomar", "--dry-run").stdout.strip() == "nada a retomar", "o crash-recovery não revive o que o orçamento pausou"
    a.set("screens.json", {"term_ret1": ["esc to interrupt"], "term_ret2": ["esc to interrupt"]})
    _uso51(a, semana=95)
    r = a.orq("retomar", "--pausados")
    assert r.returncode == 1 and "uso do plano ainda alto" in r.stderr and not _log(a, "create.log"), r
    _uso51(a, semana=70, cinco_h=20)
    r = a.orq("retomar", "--pausados", "--json")
    assert r.returncode == 0, r.stderr
    res = json.loads(r.stdout)["workers"]
    assert [(w["task"], w["estado"]) for w in res] == [("task_term_i", "retomado"), ("task_term_f", "retomado")], "a de prioridade mais alta (P2) sobe primeiro"
    c = _log(a, "create.log")
    assert c[0][c[0].index("--worktree") + 1] == "path:" + a.wt + "/i"
    comando = c[0][c[0].index("--command") + 1]
    assert comando.startswith("claude --resume sess-i --model claude-opus-5-5 --dangerously-skip-permissions 'O uso do plano voltou"), comando
    assert not _cursor(a).get("pausados") and {e["dispatch"] for e in a.events() if e["tipo"] == "pausa_fim"} == {"ctx_term_f", "ctx_term_i"}
    assert [e["terminal"] for e in a.events() if e["tipo"] == "retomada"] == ["term_ret1", "term_ret2"]
    assert a.orq("retomar", "--pausados").stdout.strip() == "nada a retomar"


def test_ticket51_orq_prioridade_troca_a_prioridade_e_aparece_em_agentes_status_e_digest():
    a = Amb(run="run_a")
    _pausa51(a)
    ags = _agentes(a)
    assert {d: x["prioridade"] for d, x in ags.items()} == {"ctx_term_s": 1, "ctx_term_f": 3, "ctx_term_i": 2, "ctx_term_p": 3}, "padrão pela frente"
    assert "P1 task_term_s" in a.orq("agentes").stdout
    r = a.orq("prioridade", "task_term_f", "1")
    assert r.returncode == 0, r.stderr
    assert _agentes(a)["ctx_term_f"]["prioridade"] == 1, "a troca vale sobre o padrão"
    assert a.orq("prioridade", "task_term_f", "3").returncode == 0 and _agentes(a)["ctx_term_f"]["prioridade"] == 3, "a última troca vale"
    r = a.orq("prioridade", "xyz", "2")
    assert r.returncode == 1 and "não é um id de task" in r.stderr, r
    assert a.orq("prioridade", "task_term_f", "5").returncode == 2
    a.orq("prioridade", "task_term_p", "1")
    # digest: 'rodando' sai da prioridade mais alta para a mais baixa
    a.orq("ingest", "--refresh")
    a.orq("digest")
    rod = json.load(open(os.path.join(a.home, "digest", "atual.json")))["rodando"]
    assert [r.get("prioridade") for r in rod] == sorted(r.get("prioridade") for r in rod) and rod[0]["prioridade"] == 1 and len(rod) == 4, rod
    assert "P1 " in a.orq("status").stdout


def test_ticket51_orq_pausar_respeita_a_prioridade_trocada():
    a = Amb(run="run_a")
    _pausa51(a)
    a.orq("prioridade", "task_term_f", "1")  # o failover virou urgente
    a.orq("prioridade", "task_term_s", "3")  # e a segurança deixou de ser
    r = json.loads(a.orq("pausar", "--dry-run", "--json").stdout)
    assert sorted(w["task"] for w in r["pausados"]) == ["task_term_i", "task_term_s"], r


def test_ticket51_prioridade_1_passa_pela_janela_de_5h_mas_nao_pela_pausa_da_semana():
    a = Amb(run="run_a")
    _uso51(a, semana=50, cinco_h=95)
    assert _despachar(a).returncode == 1
    assert _despachar(a, "--prioridade", "1").returncode == 0, "urgente passa pela janela de 5 h"
    _uso51(a, semana=95, cinco_h=10)
    r = _despachar(a, "--prioridade", "1")
    assert r.returncode == 1 and "semana" in r.stderr, "a pausa da semana vale para todas"


# ---------- ticket 52: pergunta ou permissão presa no terminal do worker ----------

def _tela52(nome):
    return open(os.path.join(AQUI, "fixtures", nome), encoding="utf-8").read().splitlines()


def test_ticket52_tela_pergunta_reconhece_permissao_askuserquestion_e_trust():
    p = orq_mod.tela_pergunta(_tela52("tela-permissao.txt"))
    assert p["tipo"] == "permissao" and "Do you want to proceed?" in p["texto"] and "Dangerous rm" in p["texto"], p
    assert [o[0] for o in p["opcoes"]] == [1, 2, 3] and p["opcoes"][0][1] == "Yes", p
    q = orq_mod.tela_pergunta(_tela52("tela-askuserquestion.txt"))
    assert q["tipo"] == "pergunta" and "Qual escopo" in q["texto"], q
    assert [o[0] for o in q["opcoes"]] == [1, 2, 3, 4], q
    t = orq_mod.tela_pergunta(_tela52("tela-trust.txt"))
    assert t["tipo"] == "trust" and [o[0] for o in t["opcoes"]] == [1, 2] and t["opcoes"][1][1] == "No, exit", t


def test_ticket52_tela_pergunta_ignora_lista_no_historico_e_tela_ociosa():
    assert orq_mod.tela_pergunta(_tela52("tela-lista-no-historico.txt")) is None
    assert orq_mod.tela_pergunta([]) is None and orq_mod.tela_pergunta(None) is None
    assert orq_mod.tela_pergunta(["Do you want to proceed?", "  1. Yes", "  2. No"]) is None, "sem o cursor ❯ não é um menu aberto"
    assert orq_mod.tela_pergunta(_tela52("tela-permissao.txt") + ["saída nova"] * 6) is None, "o menu já foi respondido e a tela seguiu"


def _tela_no_gerente52(a, tela="tela-permissao.txt"):
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    a.set("workers.json", [_w48("term_w1", agente="claude")])
    _turno48(a, ctx_term_w1=("sess-w1", None))
    a.set("screens.json", {"term_w1": _tela52(tela)})


def test_ticket52_gerente_avisa_o_coordenador_uma_vez_e_o_agentes_mostra_a_pergunta():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _tela_no_gerente52(a)
    r = a.orq("gerente", "absorver")
    assert r.returncode == 0 and "pergunta na tela (permissao)" in r.stdout, r
    (env,) = _avisos_enviados(a)
    texto = env[env.index("--text") + 1]
    assert "Do you want to proceed?" in texto and "1) Yes" in texto and "orq responder-tela task_term_w1 <opção>" in texto, texto
    a.orq("gerente", "absorver")
    assert len(_avisos_enviados(a)) == 1, "o mesmo menu não é avisado de novo"
    (ev,) = [e for e in a.events() if e["tipo"] == "pergunta_tela"]
    assert ev["dispatch"] == "ctx_term_w1" and ev["menu"] == "permissao", ev
    ag = json.loads(a.orq("agentes", "--json", "--run", "run_a", ORCA_TERMINAL_HANDLE="term_coord").stdout)
    w = next(x for x in ag if x["dispatch"] == "ctx_term_w1")
    assert w["estado"] == "perguntando" and w["pergunta"]["tipo"] == "permissao", w
    txt = a.orq("agentes", "--run", "run_a", ORCA_TERMINAL_HANDLE="term_coord").stdout
    assert "PERGUNTA NA TELA (permissao)" in txt and "orq responder-tela task_term_w1" in txt, txt
    a.set("screens.json", {"term_w1": ["● seguindo"]})  # respondido: o evento fecha
    a.orq("gerente", "absorver")
    a.set("screens.json", {"term_w1": _tela52("tela-permissao.txt")})
    a.orq("gerente", "absorver")
    assert len(_avisos_enviados(a)) == 2, "o mesmo texto volta a ser avisado depois que sumiu da tela"


def test_ticket52_gerente_com_coordenador_ocupado_nao_digita_e_tenta_na_proxima_volta():
    a = Amb(ORCA_TERMINAL_HANDLE="term_ger")
    _tela_no_gerente52(a, "tela-trust.txt")
    a.set("busy.json", ["term_coord"])
    a.orq("gerente", "absorver")
    assert _avisos_enviados(a) == [] and not [e for e in a.events() if e["tipo"] == "pergunta_tela"]
    a.set("busy.json", [])
    a.orq("gerente", "absorver")
    assert len(_avisos_enviados(a)) == 1


def test_ticket52_responder_tela_digita_a_opcao_com_enter_e_registra_quem_respondeu():
    a = Amb(run="run_a")
    _tela_no_gerente52(a)
    r = a.orq("responder-tela", "task_term_w1", "2", ORCA_TERMINAL_HANDLE="term_coord")
    assert r.returncode == 0, r.stderr
    (env,) = [e for e in _log(a, "send.log") if e[e.index("--terminal") + 1] == "term_w1"]
    assert env[env.index("--text") + 1] == "2" and "--enter" in env, env
    (c,) = [e for e in a.events() if e["tipo"] == "controle" and e["acao"] == "responder-tela"]
    assert c["por"] == "term_coord" and c["opcao"].startswith("2) Yes, and") and c["resultado"] == "ok" and "Do you want to proceed?" in c["pergunta"], c
    r = a.orq("responder-tela", "task_term_w1", "no, and", ORCA_TERMINAL_HANDLE="term_coord")
    assert r.returncode == 0 and _log(a, "send.log")[-1][_log(a, "send.log")[-1].index("--text") + 1] == "3", "o começo do rótulo também vale"


def test_ticket52_responder_tela_recusa_sem_menu_aberto_ou_com_opcao_que_nao_existe():
    a = Amb(run="run_a")
    _tela_no_gerente52(a)
    r = a.orq("responder-tela", "task_term_w1", "9", ORCA_TERMINAL_HANDLE="term_coord")
    assert r.returncode != 0 and "não existe no menu" in r.stderr and not _log(a, "send.log"), r
    a.set("screens.json", {"term_w1": ["● trabalhando"]})
    r = a.orq("responder-tela", "task_term_w1", "1", ORCA_TERMINAL_HANDLE="term_coord")
    assert r.returncode != 0 and "não mostra um menu" in r.stderr and not _log(a, "send.log"), "um número digitado no prompt comum viraria mensagem ao worker"
    r = a.orq("responder-tela", "task_nao_existe", "1", ORCA_TERMINAL_HANDLE="term_coord")
    assert r.returncode != 0 and "nada a responder" in r.stderr


def test_ticket52_hook_guard_recusa_askuserquestion_so_no_worker_e_rapido():
    a = Amb(run="run_a")
    a.prompt(PREAMBULO)  # esta sessão é de worker
    t = time.time()
    r = _guard(a)
    dt = time.time() - t
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert r.returncode == 0 and out["permissionDecision"] == "deny" and "escalation" in out["permissionDecisionReason"] and "orchestration ask" in out["permissionDecisionReason"], r
    assert not _calls(a, "worker-list") and not _calls(a, "run-current"), "o worker nem chama o Orca"
    assert dt < 0.5, dt  # o teto do ticket é 100 ms do hook; o resto é a partida do python no subprocess
    t = time.perf_counter()
    orq_mod.guard_worker()
    assert time.perf_counter() - t < 0.1
    c = Amb(run="run_a")  # coordenador sem despacho ativo: a caixa passa
    assert _guard(c).stdout == ""
    assert _guard(a, tool="Bash").stdout == "", "só o AskUserQuestion"


def test_ticket52_retomar_leva_o_jeito_de_escalar_na_mensagem_de_continuacao():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="2", ORCA_TERMINAL_HANDLE="term_coord")
    _queda48(a)
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    assert a.orq("retomar", "--json").returncode == 0
    (c,) = _log(a, "create.log")
    comando = c[c.index("--command") + 1]
    assert "orca orchestration send" in comando and "--type escalation" in comando and "term_coord" in comando, comando
    assert "--to run:run_a" in comando and "--task-id task_term_w1" in comando and "--dispatch-id ctx_term_w1" in comando, comando
    assert "${VAR:?}" in comando


def test_ticket52_tela_tem_15_linhas_de_folga_para_o_prompt_de_permissao():
    assert orq_mod.TELA_LINHAS >= 25 and len(_tela52("tela-permissao.txt")) <= orq_mod.TELA_LINHAS


# ---------- ticket 73: adaptador de harness (claude | codex) ----------

def test_ticket73_o_resume_do_claude_sai_igual_ao_de_antes_do_adaptador():
    r = orq_mod.HARNESS["claude"]["resume"]
    assert orq_mod.shlex.join(r("sess 1", "claude-opus-5-5", "high", "Continue, it's ok")) == \
        "claude --resume 'sess 1' --model claude-opus-5-5 --dangerously-skip-permissions 'Continue, it'\"'\"'s ok'"
    assert orq_mod.shlex.join(r("s", None, None, "m")) == "claude --resume s --dangerously-skip-permissions m", "sem modelo não leva --model"


def test_ticket73_a_tela_de_cada_harness_usa_os_padroes_dele_e_harness_sem_adaptador_nao_tem_menu():
    p = orq_mod.tela_pergunta(_tela52("tela-permissao.txt"), "claude")
    assert p == orq_mod.tela_pergunta(_tela52("tela-permissao.txt")), "o padrão é o claude"
    assert orq_mod.tela_pergunta(_tela52("tela-permissao.txt"), "cursor") is None, "sem adaptador o orq não afirma nada da tela"
    assert set(orq_mod.HARNESS) >= {"claude"} and "claude" in orq_mod.HARNESSES



def _codex(nome, **campos):
    """Payload real de hook do Codex (fixtures/codex-hook-<nome>.json, capturado do codex-cli 0.159.2) com os campos trocados."""
    return {**json.load(open(os.path.join(AQUI, "fixtures", f"codex-hook-{nome}.json"))), **campos}


def _hook_codex(a, kind, ev, **env):
    r = a.orq("hook", kind, "codex", stdin=json.dumps(ev), **env)
    assert r.returncode == 0, r.stderr
    return r


def test_ticket73_hook_do_worker_codex_grava_o_turno_com_o_harness_e_o_transcrito():
    a = Amb(run=None)
    ev = _codex("userpromptsubmit", prompt=PREAMBULO_24, session_id="thr_1")
    _hook_codex(a, "prompt", ev)
    t = _turnos(a)["ctx_d24"]
    assert (t["sessao"], t["harness"], t["cwd"]) == ("thr_1", "codex", "/wt/repo-captura") and t["transcrito"].endswith("-01a0f802-a07c-7f81-86a2-f094ee020c7a.jsonl"), t
    _hook_codex(a, "stop", _codex("stop", session_id="thr_1"))
    assert _turnos(a)["ctx_d24"]["fim"], "o Stop do Codex fecha o turno"
    _hook(a, "prompt", prompt=PREAMBULO_24.replace("ctx_d24", "ctx_c24"), session_id="s_claude")
    assert _turnos(a)["ctx_c24"]["harness"] == "claude", "sem o argumento o hook é do Claude, como os instalados hoje"


def test_ticket73_hook_com_harness_desconhecido_sai_0_e_o_exemplo_do_codex_instala_cada_hook_com_o_argumento():
    a = Amb(run=None)
    r = a.orq("hook", "prompt", "cursor", stdin="{}")
    assert r.returncode == 0 and not r.stdout and "argumentos inválidos" in a.log(), "fail-open: no Codex a saída 2 bloquearia o prompt"
    cfg = json.load(open(os.path.join(AQUI, "codex.hooks.example.json")))["hooks"]
    cmds = {h["command"] for g in cfg.values() for x in g for h in x["hooks"]}
    for k in ("prompt", "stop", "session", "lugar", "externas", "prligar"):
        assert f"python3 ~/.claude/orq/orq.py hook {k} codex" in cmds, k
    assert "apply_patch" in next(x["matcher"] for x in cfg["PreToolUse"] if "hook lugar" in json.dumps(x))
    assert not [c for c in cmds if "hook guard" in c or "hook ask" in c], "o Codex não tem AskUserQuestion"


def test_ticket73_lugar_no_codex_le_o_caminho_do_apply_patch_e_a_casa_vem_do_primeiro_prompt():
    a = Amb(run="run_a")
    p, w = _repo(a.tmp.name)
    _hook_codex(a, "prompt", _codex("userpromptsubmit", session_id="abcdef123456", prompt="oi", cwd=p))  # coordenador Codex na casa p
    patch = "*** Begin Patch\n*** Update File: sub/a.txt\n@@\n-x\n+y\n*** End Patch"
    os.makedirs(os.path.join(w, "sub"))
    r = _hook_codex(a, "lugar", _codex("pre-apply-patch", session_id="abcdef123456", cwd=w, tool_input={"command": patch}))
    msg = _aviso(r)
    assert "lugar errado" in msg and os.path.realpath(w) in msg, msg
    b = Amb(run="run_a")
    pb, wb = _repo(b.tmp.name)
    _hook_codex(b, "prompt", _codex("userpromptsubmit", session_id="abcdef123456", prompt="oi", cwd=wb))  # coordenador que mora na worktree
    assert _aviso(_hook_codex(b, "lugar", _codex("pre-apply-patch", session_id="abcdef123456", cwd=wb, tool_input={"command": patch.replace("sub/", "")}))) == ""
    assert _aviso(_hook_codex(b, "lugar", _codex("pre-bash", session_id="abcdef123456", cwd=wb, tool_input={"command": "git commit -m x"}))) == ""


def test_ticket73_prligar_no_codex_le_a_url_da_saida_em_texto():
    a, p, w = _ambiente_46()
    ev = _codex("post-bash", session_id="abcdef123456", cwd=w, tool_input={"command": "gh pr create --base development --title x --body y"}, tool_response=PR1 + "\n")
    r = _hook_codex(a, "prligar", ev)
    (item,) = _prs_json(a)["itens"]
    assert item["task"] == "task_feat1" and "#1216" in json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"], item


def test_ticket73_worker_codex_com_turno_gravado_tem_estado_e_agente_sem_adaptador_segue_unknown():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_x", "run": "run_a", "status": "dispatched", "desde": _iso(-3000), "agente": "codex"},
                           {"handle": "term_y", "run": "run_a", "status": "dispatched", "desde": _iso(-3000), "agente": "cursor"}])
    os.makedirs(a.home, exist_ok=True)
    fim = now_iso(-300)
    json.dump({d: {"task": "t", "harness": "codex", "inicio": now_iso(-600), "fim": fim} for d in ("ctx_term_x", "ctx_term_y")}, open(os.path.join(a.home, "turnos.json"), "w"))
    ag = _agentes(a)
    assert ag["ctx_term_x"]["turno"] == "parado" and ag["ctx_term_y"]["turno"] == "unknown", ag



def test_ticket73_despachar_com_agente_codex_sobe_o_worker_codex_e_grava_o_agente_no_evento():
    a = Amb(run="run_a")
    r = _despachar(a, "--agente", "codex", "--modelo", "gpt-6-sol", "--effort", "max", spec=_spec(a, "# Ticket 05\n\nFaça X.\n"))
    assert r.returncode == 0, r.stderr
    (arg,) = _log(a, "started.log")
    assert (arg[arg.index("--agent") + 1], arg[arg.index("--model") + 1], arg[arg.index("--effort") + 1]) == ("codex", "gpt-6-sol", "max"), arg
    (ev,) = [e for e in a.events() if e["tipo"] == "despacho"]
    assert ev["agente"] == "codex", ev
    b = Amb(run="run_a")
    assert _despachar(b).returncode == 0
    (ev,) = [e for e in b.events() if e["tipo"] == "despacho"]
    assert ev["agente"] == "claude", "sem --agente o worker é Claude, como antes"


def test_ticket73_despachar_recusa_effort_que_o_harness_nao_tem_sem_criar_task():
    a = Amb(run="run_a")
    r = _despachar(a, "--effort", "ultra")
    assert r.returncode != 0 and "ultra" in r.stderr and "claude" in r.stderr, r.stderr
    assert _despachar(a, "--agente", "codex", "--modelo", "gpt-6-sol", "--effort", "ultra").returncode == 0, "o Codex tem ultra"
    assert a.orq("despachar", "--run", "run_a", "--agente", "cursor", "--titulo", "x", "--spec-arquivo", _spec(a), "--modelo", "m", "--effort", "low").returncode != 0
    assert len(_log(a, "started.log")) == 1


def test_ticket73_worker_routing_tem_a_tabela_do_codex_com_a_fonte_astra_so_em_low_e_medium_e_sem_terra():
    txt = open(os.path.join(AQUI, "skills", "worker-routing", "SKILL.md")).read()
    assert "gpt-6-luna" in txt and "gpt-6-sol" in txt and "--agente codex" in txt, "a tabela do Codex e o jeito de despachar"
    assert "learn.chatgpt.com/docs/models" in txt, "a fonte da equivalência"
    terra = [l for l in txt.splitlines() if "terra" in l.lower()]
    assert terra and all("não" in l.lower() for l in terra), terra
    astra = [l for l in txt.splitlines() if "astra" in l.lower()]  # só low e medium, no lugar do Opus xhigh e max
    assert astra and all("low" in l.lower() or "medium" in l.lower() or "não" in l.lower() for l in astra), astra
    assert not [l for l in astra if "astra" in l.lower() and ("astra` high" in l.lower() or "astra` xhigh" in l.lower())], astra


def test_ticket73_mensagens_ao_worker_nao_citam_o_claude():
    for m in (orq_mod.MSG_ESCALAR, orq_mod.MSG_PAUSA, orq_mod.MSG_CONTINUE, orq_mod.MSG_VOLTA):
        assert "claude" not in m.lower(), m



def _queda73(a, turno=True):
    """Depois da queda: um worker Codex (gpt-6-sol xhigh) perdeu o terminal; `turno` diz se os hooks dele gravaram a sessão."""
    os.makedirs(a.home, exist_ok=True)
    a.wt = os.path.join(a.tmp.name, "wt")
    os.makedirs(os.path.join(a.wt, "c1"))
    a.set("workers.json", [_w48("term_c1", agente="codex", modelo="gpt-6-sol", effort="xhigh")])
    a.set("tasks_run_a.json", [{"id": "task_term_c1", "task_title": "Ticket 73"}])
    a.set("terminals.json", ["term_coord"])
    if turno:
        json.dump({"ctx_term_c1": {"task": "task_term_c1", "sessao": "thr-c1", "inicio": "2026-10-01T14:00:00Z", "fim": None, "harness": "codex", "cwd": a.wt + "/c1"}},
                  open(os.path.join(a.home, "turnos.json"), "w"))


def test_ticket73_retomar_worker_codex_sobe_com_codex_resume_modelo_e_effort():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="2")
    _queda73(a)
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    res = json.loads(a.orq("retomar", "--json").stdout)
    assert [(w["dispatch"], w["estado"]) for w in res["workers"]] == [("ctx_term_c1", "retomado")], res
    (c,) = _log(a, "create.log")
    comando = c[c.index("--command") + 1]
    assert comando.startswith("codex resume thr-c1 -m gpt-6-sol -c 'model_reasoning_effort=\"xhigh\"' --dangerously-bypass-approvals-and-sandbox 'Continue de onde parou."), comando


def test_ticket73_retomar_sem_sessao_gravada_acha_a_sessao_pelo_indice_do_orca():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="2")
    _queda73(a, turno=False)
    a.set("search.json", [
        {"agent": "claude", "sessionId": "coord", "cwd": "/x", "evidence": {"role": "tool"}, "source": {"filePath": "/x/coord.jsonl"}},  # o coordenador cita o dispatch
        {"agent": "codex", "sessionId": "thr-achada", "cwd": a.wt + "/c1", "evidence": {"role": "user"}, "source": {"filePath": "/r/rollout-thr-achada.jsonl"}}])
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    (w,) = json.loads(a.orq("retomar", "--json").stdout)["workers"]
    assert (w["estado"], w["sessao"], w["cwd"]) == ("retomado", "thr-achada", a.wt + "/c1"), w
    assert any(x[:1] == ["ctx_term_c1"] and "--agent" in x for x in _log(a, "calls.log")), "a busca é pelo dispatch, no agente do worker"
    b = Amb(run="run_a")
    _queda73(b, turno=False)
    (w,) = json.loads(b.orq("retomar", "--dry-run", "--json").stdout)["workers"]
    assert w["estado"] == "sem_sessao", "sem hit no índice continua sem sessão"


def test_ticket73_prompts_humanos_e_steer_lido_no_rollout_do_codex():
    a = Amb(run="run_a")
    os.makedirs(a.home, exist_ok=True)
    arq = os.path.join(a.tmp.name, "rollout-thr_73.jsonl")
    linhas = open(os.path.join(AQUI, "fixtures", "codex-rollout-worker.jsonl")).read()
    open(arq, "w").write(linhas)
    json.dump({"ctx_x73": {"task": "task_t73", "sessao": "thr_73", "inicio": "2026-10-01T15:08:39Z", "fim": None, "harness": "codex", "transcrito": arq}},
              open(os.path.join(a.home, "turnos.json"), "w"))
    with EmProcesso(a):
        assert orq_mod._prompts_humanos_do_worker("ctx_x73") == 0, "contexto do ambiente, preâmbulo e aviso do Orca não são humanos"
        assert orq_mod.lido_no_transcrito("ctx_x73", "msg_steer73") is True and orq_mod.lido_no_transcrito("ctx_x73", "msg_outra") is False
        humano = {"timestamp": "2026-10-01T15:11:00.000Z", "type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "pare e me explique"}]}}
        open(arq, "a").write(json.dumps(humano) + "\n")
        assert orq_mod._prompts_humanos_do_worker("ctx_x73") == 1


def test_ticket73_pausar_e_retomar_pausados_de_um_worker_codex_usam_o_resume_dele():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="2")
    _queda73(a)
    _cursor_set = os.path.join(a.home, "cursor.json")
    json.dump({"pausados": {"ctx_term_c1": {"task": "task_term_c1", "run": "run_a", "titulo": "Ticket 73", "prioridade": 2, "modelo": "gpt-6-sol",
                                             "effort": "xhigh", "agente": "codex", "sessao": "thr-c1", "cwd": a.wt + "/c1", "terminal": "term_c1"}}}, open(_cursor_set, "w"))
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    r = a.orq("retomar", "--pausados", "--forcar", "--json")
    assert r.returncode == 0, r.stderr
    (c,) = _log(a, "create.log")
    assert c[c.index("--command") + 1].startswith("codex resume thr-c1 -m gpt-6-sol -c 'model_reasoning_effort=\"xhigh\"'"), c



def _conta73(a, codex_semana=None, codex_5h=None, claude_semana=None):
    """O rateLimits do `orca account list`: resetsAt em milissegundos, como o Orca real."""
    agora = time.time()
    j = lambda p, minutos, dt: {"usedPercent": p, "windowMinutes": minutos, "resetsAt": int((agora + dt) * 1000)} if p is not None else None
    a.set("account.json", {"codex": {"provider": "codex", "session": j(codex_5h, 300, 3600), "weekly": j(codex_semana, 10080, 2 * 86400 + 60), "status": "ok"},
                           "claude": {"provider": "claude", "session": None, "weekly": j(claude_semana, 10080, 3 * 86400), "status": "ok"}})


def test_ticket73_uso_do_codex_vem_do_orca_account_list():
    a = Amb()
    _conta73(a, codex_semana=93, codex_5h=10)
    u = json.loads(a.orq("uso", "--agente", "codex", "--json").stdout)
    assert (u["uso"]["semana"], u["uso"]["cinco_h"], u["nivel"]) == (93, 10, "pausa") and "vira em 2d" in u["motivo"], u
    _conta73(a, codex_semana=40)
    u = json.loads(a.orq("uso", "--agente", "codex", "--json").stdout)
    assert (u["uso"]["cinco_h"], u["nivel"]) == (None, "ok"), "o Codex sem janela de 5 h só tem a semana"


def test_ticket73_uso_do_claude_sem_quadro_do_hud_cai_no_orca_e_com_hud_fresco_nao_chama_o_orca():
    a = Amb()
    _conta73(a, claude_semana=88)
    u = json.loads(a.orq("uso", "--json").stdout)
    assert (u["uso"]["semana"], u["nivel"]) == (88, "avisa"), u
    b = Amb()
    _uso51(b, semana=50, cinco_h=10)
    assert json.loads(b.orq("uso", "--json").stdout)["uso"]["semana"] == 50 and not _log(b, "calls.log")


def test_ticket73_despachar_confere_a_cota_do_harness_escolhido():
    a = Amb(run="run_a")
    _uso51(a, semana=40, cinco_h=10)
    _conta73(a, codex_semana=95)
    r = _despachar(a, "--agente", "codex", "--modelo", "gpt-6-sol", "--effort", "low")
    assert r.returncode != 0 and "Codex" in r.stderr and "95%" in r.stderr, r.stderr
    assert _despachar(a).returncode == 0, "a cota do Claude segue livre"
    assert len(_log(a, "started.log")) == 1



def test_ticket73_tela_do_codex_reconhece_o_trust_e_nao_ve_menu_na_tela_ociosa_nem_na_ocupada():
    t = orq_mod.tela_pergunta(_tela52("tela-codex-trust.txt"), "codex")
    assert t["tipo"] == "trust" and [o[0] for o in t["opcoes"]] == [1, 2] and t["opcoes"][0][1] == "Trust and continue", t
    assert orq_mod.tela_pergunta(_tela52("tela-codex-trust.txt"), "claude") is None, "o cursor › é do Codex"
    for f in ("tela-codex-ocioso.txt", "tela-codex-ocupado.txt"):
        assert orq_mod.tela_pergunta(_tela52(f), "codex") is None, f
    h = orq_mod.tela_pergunta(["  Hooks need review", "  3 hooks are new or changed", "› 1. Review hooks", "  2. Trust all and continue", "  3. Continue without trusting"], "codex")
    assert h["tipo"] == "hooks" and len(h["opcoes"]) == 3, h


def test_ticket73_tela_do_codex_com_terminal_em_segundo_plano_e_espera_e_o_resume_que_falhou_e_reconhecido():
    esp = orq_mod.HARNESS["codex"]["tela"]["espera"]
    m = esp.search("\n".join(_tela52("tela-codex-ocupado.txt")))
    assert m and m.group(0).strip() == "1 background terminal running", m
    assert not esp.search("\n".join(_tela52("tela-codex-ocioso.txt")))
    falha = "ERROR: No saved session found with ID 01a0ffff-0000-7000-8000-000000000000. Run `codex resume` without an ID to choose f"
    assert any(f in falha for f in orq_mod.HARNESS["codex"]["tela"]["falha"])


def test_ticket73_responder_tela_de_worker_codex_digita_a_opcao_do_menu_dele():
    a = Amb(run="run_a")
    a.set("workers.json", [{"handle": "term_x", "run": "run_a", "status": "dispatched", "task": "task_x", "agente": "codex"}])
    os.makedirs(a.home, exist_ok=True)
    json.dump({"ctx_term_x": {"task": "task_x", "sessao": "thr", "inicio": now_iso(-60), "fim": None, "harness": "codex"}}, open(os.path.join(a.home, "turnos.json"), "w"))
    a.set("screens.json", {"term_x": _tela52("tela-codex-trust.txt")})
    r = a.orq("responder-tela", "task_x", "Trust")
    assert r.returncode == 0, r.stderr
    (num, enter) = _log(a, "send.log")
    assert num[num.index("--text") + 1] == "1" and "--enter" not in num and "--enter" in enter and "--text" not in enter, \
        "o menu do Codex só confirma com o Enter num send separado (conferido no Orca real em 01/10)"



def test_ticket73_telas_le_o_worker_codex_rodando_com_os_padroes_dele():
    orig = orq_mod.orca
    orq_mod.orca = lambda *a, **k: {"terminal": {"tail": _tela52("tela-codex-ocupado.txt")}}
    try:
        ws = [{"dispatchId": "ctx_c", "agentTerminalHandle": "term_c", "dispatchStatus": "dispatched"}]
        assert orq_mod._telas(ws, {"ctx_c": {"agente": "codex"}}) == {"ctx_c": "1 background terminal running (tela)"}
    finally:
        orq_mod.orca = orig



def test_ticket73_despachar_codex_confia_a_raiz_do_repositorio_no_config_do_codex_sem_estragar_o_que_ja_tem():
    a = Amb(run="run_a")
    cfg = os.path.join(a.tmp.name, "config.toml")
    open(cfg, "w").write('model = "gpt-6-luna"\n\n[projects."/ja/confiada"]\ntrust_level = "trusted"\n')
    r = _despachar(a, "--agente", "codex", "--modelo", "gpt-6-sol", "--effort", "low", ORQ_CODEX_CONFIG=cfg)
    assert r.returncode == 0, r.stderr
    raiz = os.path.dirname(subprocess.run(["git", "-C", AQUI, "rev-parse", "--path-format=absolute", "--git-common-dir"], capture_output=True, text=True).stdout.strip())
    import tomllib
    d = tomllib.loads(open(cfg).read())
    assert d["model"] == "gpt-6-luna" and d["projects"]["/ja/confiada"]["trust_level"] == "trusted", d
    assert d["projects"][os.path.realpath(raiz)]["trust_level"] == "trusted", d
    antes = open(cfg).read()
    assert _despachar(a, "--agente", "codex", "--modelo", "gpt-6-sol", "--effort", "low", ORQ_CODEX_CONFIG=cfg).returncode == 0
    assert open(cfg).read() == antes, "pasta já confiada não é gravada de novo"
    b = Amb(run="run_a")
    cfg_b = os.path.join(b.tmp.name, "config.toml")
    assert _despachar(b, ORQ_CODEX_CONFIG=cfg_b).returncode == 0 and not os.path.exists(cfg_b), "worker Claude não mexe no Codex"


def test_ticket73_confiar_codex_grava_a_worktree_e_recusa_config_que_nao_e_toml():
    with tempfile.TemporaryDirectory() as d:
        cfg = os.path.join(d, "config.toml")
        antes, log_antes = orq_mod.CODEX_CONFIG, orq_mod.LOG
        orq_mod.CODEX_CONFIG, orq_mod.LOG = cfg, os.path.join(d, "orq.log")
        try:
            assert orq_mod.confiar_codex(d + '/wt "x"') == [d + '/wt "x"']
            import tomllib
            assert tomllib.loads(open(cfg).read())["projects"][d + '/wt "x"']["trust_level"] == "trusted"
            open(cfg, "w").write("isto = não é toml [")
            assert orq_mod.confiar_codex(d + "/outra") == [] and open(cfg).read() == "isto = não é toml [", "config quebrado fica como estava"
        finally:
            orq_mod.CODEX_CONFIG, orq_mod.LOG = antes, log_antes


def test_ticket73_auditar_respostas_de_coordenador_codex_diz_que_nao_ha_o_que_auditar():
    a = Amb(run="run_a")
    _hook_codex(a, "prompt", _codex("userpromptsubmit", session_id="thr_coord", prompt="oi"))
    r = a.orq("auditar-respostas")
    assert r.returncode != 0 and "Codex" in r.stderr and "AskUserQuestion" in r.stderr, r.stderr


# ---------- ticket 79: orçamento da máquina e fila de despacho ----------

SONNET, OPUS = "claude-sonnet-5-5", "claude-opus-5-5"


def _frota79(a, vivos=(), mortos=()):
    """No run_a, um worker vivo por (título, modelo) em `vivos` (term_v0…, com terminal) e um caído por (título, modelo) em `mortos` (term_m0…, sem terminal, com sessão e pasta)."""
    os.makedirs(a.home, exist_ok=True)
    a.wt = os.path.join(a.tmp.name, "wt")
    ws, ts, sessoes = [], [], {}
    for pref, lista in (("v", vivos), ("m", mortos)):
        for n, (titulo, modelo) in enumerate(lista):
            h = f"term_{pref}{n}"
            ws.append(_w48(h, modelo=modelo))
            ts.append({"id": "task_" + h, "task_title": titulo, "status": "dispatched", "dispatch_id": "ctx_" + h, "created_at": _iso(-900)})
            if pref == "m":
                os.makedirs(os.path.join(a.wt, h))
                sessoes["ctx_" + h] = ("sess-" + h, os.path.join(a.wt, h))
    a.set("workers.json", ws)
    a.set("tasks_run_a.json", ts)
    a.set("terminals.json", ["term_coord", *(f"term_v{n}" for n in range(len(vivos)))])
    if sessoes:
        _turno48(a, **sessoes)


def _libera79(a, *handles):
    """Os workers entregaram: o dispatch fecha e o terminal some, a vaga abre."""
    a.set("workers.json", [{**w, "status": "completed"} if w["handle"] in handles else w for w in json.load(open(os.path.join(a.fake, "workers.json")))])
    a.set("terminals.json", [h for h in json.load(open(os.path.join(a.fake, "terminals.json"))) if h not in handles])


def _desp79(a, titulo, prio, modelo=SONNET):
    return a.orq("despachar", "--run", "run_a", "--titulo", titulo, "--spec-arquivo", _spec(a), "--modelo", modelo, "--effort", "medium", "--prioridade", str(prio))


def _fila79(a):
    return json.load(open(os.path.join(a.home, "fila-despacho.json")))["itens"] if os.path.exists(os.path.join(a.home, "fila-despacho.json")) else []


def _titulos_iniciados79(a):
    return [c[c.index("--task-title") + 1] for c in _log(a, "started.log")]


def _painel79():
    return Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")


def test_ticket79_maquina_json_ausente_usa_os_padroes_e_o_set_ajusta_e_valida():
    a = Amb(run="run_a")
    cfg = json.loads(a.orq("maquina", "--json").stdout)["config"]
    assert (cfg["max_workers"], cfg["max_e2e"], cfg["max_caros"]) == (4, 1, 2) and cfg["modelos_caros"] == ["claude-opus-*", "gpt-6-astra*", "gpt-6-sol*"], cfg
    assert not os.path.exists(os.path.join(a.home, "maquina.json")), "ler não grava"
    assert json.loads(a.orq("maquina", "set", "max_workers", "6").stdout)["max_workers"] == 6
    assert json.loads(a.orq("maquina", "set", "modelos_caros", '["claude-opus-*"]').stdout)["modelos_caros"] == ["claude-opus-*"]
    for ruim in (("max_workers", "muitos"), ("max_workers", "true"), ("nada", "1"), ("modelos_caros", "3")):
        r = a.orq("maquina", "set", *ruim)
        assert r.returncode == 1 and r.stderr.startswith("orq:"), (ruim, r)
    assert json.load(open(os.path.join(a.home, "maquina.json"))) == {"max_workers": 6, "modelos_caros": ["claude-opus-*"]}, "o que foi recusado não grava"
    json.dump({"max_workers": "x", "max_caros": 3}, open(os.path.join(a.home, "maquina.json"), "w"))
    cfg = json.loads(a.orq("maquina", "--json").stdout)["config"]
    assert (cfg["max_workers"], cfg["max_caros"]) == (4, 3), "valor de tipo errado vale como ausente"


def test_ticket79_despachar_com_o_orcamento_cheio_enfileira_e_nao_sobe_worker():
    a = Amb(run="run_a")
    _frota79(a, vivos=[(f"Vivo {n}", SONNET) for n in range(4)])
    r = _desp79(a, "Ticket 05", 2)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert (out["estado"], out["posicao"], out["prioridade"]) == ("enfileirado", 1, 2) and "4/4 workers vivos" in out["motivo"] and "dispatchId" not in out, out
    assert "orq fila-despacho lista" in r.stderr, "o aviso diz como ver a fila"
    assert not _log(a, "started.log"), "nada subiu"
    (it,) = _fila79(a)
    assert (it["tipo"], it["run"], it["titulo"], it["modelo"], it["effort"], it["prioridade"]) == ("despacho", "run_a", "Ticket 05", SONNET, "medium", 2), it
    assert open(it["spec_arquivo"]).read() == open(_spec(a)).read() and it["spec_arquivo"].startswith(a.home), "o spec fica numa cópia em ORQ_HOME"
    assert _desp79(a, "Ticket 05", 2).returncode == 0 and len(_fila79(a)) == 1, "o mesmo pedido não entra duas vezes"
    assert "P2 despacho Ticket 05" in a.orq("fila-despacho", "lista").stdout
    assert [e["op"] for e in a.events() if e["tipo"] == "despacho_fila"] == ["entrou"]
    assert a.orq("fila-despacho", "rm", "fd_que_nao_existe").returncode == 1
    assert a.orq("fila-despacho", "rm", it["id"]).returncode == 0 and not _fila79(a) and not os.path.exists(it["spec_arquivo"])
    assert a.orq("fila-despacho", "lista").stdout.strip() == "fila de despacho vazia"


def test_ticket79_vaga_aberta_sobe_o_p1_antes_do_p2_e_um_por_volta_do_painel():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[(f"Vivo {n}", SONNET) for n in range(4)])
    assert _desp79(a, "Segunda", 2).returncode == 0 and _desp79(a, "Primeira", 1).returncode == 0 and _desp79(a, "Terceira", 2).returncode == 0
    assert [i["titulo"] for i in sorted(_fila79(a), key=lambda i: (i["prioridade"], i["ts"]))] == ["Primeira", "Segunda", "Terceira"]
    a.orq("gerente", "absorver")
    assert not _log(a, "started.log"), "sem vaga o painel não sobe nada"
    _libera79(a, "term_v0", "term_v1")
    r = a.orq("gerente", "absorver")
    assert r.returncode == 0, r.stderr
    assert _titulos_iniciados79(a) == ["Primeira"], "duas vagas, mas o painel sobe um por volta: o P1 antes do P2"
    assert "Primeira subiu" in r.stdout
    a.set("terminals.json", [*json.load(open(os.path.join(a.fake, "terminals.json"))), "term_novo5"])  # o terminal do worker novo (o Orca falso não o cria)
    a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Primeira", "Segunda"], "a segunda vaga leva o próximo da fila; o worker novo já ocupa a primeira"
    a.set("terminals.json", [*json.load(open(os.path.join(a.fake, "terminals.json"))), "term_novo6"])
    a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Primeira", "Segunda"] and [i["titulo"] for i in _fila79(a)] == ["Terceira"], "duas vagas ocupadas por eles: a Terceira espera"


def test_ticket79_o_painel_sobe_pela_ordem_p1_p2_e_empate_pelo_mais_antigo():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[(f"Vivo {n}", SONNET) for n in range(4)])
    for t, p in (("Segunda", 2), ("Primeira", 1), ("Terceira", 2)):
        assert _desp79(a, t, p).returncode == 0
    _libera79(a, "term_v0", "term_v1", "term_v2", "term_v3")
    for _ in range(3):
        assert a.orq("gerente", "absorver").returncode == 0
    assert _titulos_iniciados79(a) == ["Primeira", "Segunda", "Terceira"] and not _fila79(a)
    assert [e["op"] for e in a.events() if e["tipo"] == "despacho_fila"].count("subiu") == 3


def test_ticket79_retomar_com_10_caidos_e_max_workers_4_sobe_4_por_prioridade_e_enfileira_6():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="1")
    mortos = [("Segurança a", SONNET), ("Failover 1", SONNET), ("Ticket 1", SONNET), ("Failover 2", SONNET), ("Ticket 2", SONNET),
              ("Failover 3", SONNET), ("Segurança b", SONNET), ("Ticket 3", SONNET), ("Failover 4", SONNET), ("Ticket 4", SONNET)]
    _frota79(a, mortos=mortos)
    a.set("screens.json", {f"term_ret{n}": ["esc to interrupt"] for n in range(1, 5)})
    seco = json.loads(a.orq("retomar", "--dry-run", "--json").stdout)["workers"]
    assert sorted(w["estado"] for w in seco).count("a_retomar") == 4 and sorted(w["estado"] for w in seco).count("a_enfileirar") == 6 and not _fila79(a) and not _log(a, "create.log")
    r = a.orq("retomar", "--json")
    assert r.returncode == 0, r.stderr
    res = json.loads(r.stdout)["workers"]
    assert [w["titulo"] for w in res] == [t for t, _ in mortos], "a resposta segue a ordem do Orca"
    subiram = [c[c.index("--title") + 1] for c in _log(a, "create.log")]
    assert subiram == ["Segurança a (retomado)", "Segurança b (retomado)", "Ticket 1 (retomado)", "Ticket 2 (retomado)"], "os dois P1 e os dois primeiros P2"
    fila = _fila79(a)
    assert sorted(i["titulo"] for i in fila) == sorted(["Ticket 3", "Ticket 4", "Failover 1", "Failover 2", "Failover 3", "Failover 4"]) and {i["tipo"] for i in fila} == {"retomada"}
    assert {(i["titulo"], i["prioridade"]) for i in fila} >= {("Ticket 3", 2), ("Failover 1", 3)}
    assert all("4/4 workers vivos" in w["aviso"] for w in res if w["estado"] == "enfileirado")
    again = json.loads(a.orq("retomar", "--dry-run", "--json").stdout)["workers"]
    assert [w["estado"] for w in again].count("a_enfileirar") == 6, "os 4 retomados contam como vivos; os outros seguem esperando"


def test_ticket79_a_fila_de_retomada_sobe_por_prioridade_quando_abre_vaga_e_descarta_o_que_ja_voltou():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Vivo", SONNET)] * 4, mortos=[("Failover 1", SONNET), ("Ticket 1", SONNET), ("Ticket 2", SONNET)])
    a.set("workers.json", json.load(open(os.path.join(a.fake, "workers.json"))))
    a.set("screens.json", {f"term_ret{n}": ["esc to interrupt"] for n in range(1, 4)})
    a.set("terminals.json", ["term_coord", "term_v0", "term_v1", "term_v2", "term_v3"])
    assert [w["estado"] for w in json.loads(a.orq("retomar", "--json").stdout)["workers"]] == ["enfileirado"] * 3
    _libera79(a, "term_v0")
    a.orq("gerente", "absorver")
    assert [c[c.index("--title") + 1] for c in _log(a, "create.log")] == ["Ticket 1 (retomado)"], "o P2 mais antigo antes do P3"
    assert sorted(i["titulo"] for i in _fila79(a)) == ["Failover 1", "Ticket 2"]
    _libera79(a, "term_v1", "term_v2")
    a.set("workers.json", [{**w, "status": "completed"} if w["handle"] == "term_m2" else w for w in json.load(open(os.path.join(a.fake, "workers.json")))])  # Ticket 2 terminou sozinho
    r = a.orq("gerente", "absorver")
    assert "Ticket 2 saiu" in r.stdout and "Failover 1 retomado" in r.stdout, r.stdout
    assert [c[c.index("--title") + 1] for c in _log(a, "create.log")] == ["Ticket 1 (retomado)", "Failover 1 (retomado)"] and not _fila79(a)


def test_ticket79_sob_pressao_alta_nada_sobe_e_o_aviso_sai_uma_vez_por_episodio():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Vivo", SONNET)])
    a.maquina(carga=40)
    r = _desp79(a, "Ticket 05", 1)
    out = json.loads(r.stdout)
    assert out["estado"] == "enfileirado" and "carga 40 (máximo 12)" in out["motivo"], out
    assert not _log(a, "started.log"), "com vaga de sobra, mas a máquina em pressão: enfileira"
    for _ in range(3):
        assert a.orq("gerente", "absorver").returncode == 0
    assert not _log(a, "started.log") and len(_fila79(a)) == 1
    (env,) = _log(a, "send.log")
    texto = env[env.index("--text") + 1]
    assert env[env.index("--terminal") + 1] == "term_coord" and "máquina sob pressão" in texto and "carga 40" in texto and "1 na fila de despacho" in texto, texto
    assert "orq pausar task_term_v0" in texto, "propõe pausar o único worker vivo"
    assert not _log(a, "close.log"), "propor não é pausar"
    assert [e["tipo"] for e in a.events() if e["tipo"] == "maquina_aviso"] == ["maquina_aviso"]
    a.maquina(mem_livre_mb=1000, livre_pct=8)
    a.orq("gerente", "absorver")
    assert len(_log(a, "send.log")) == 1, "o mesmo episódio de pressão não avisa de novo, mesmo com outro motivo"
    a.maquina()
    r = a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Ticket 05"] and not _fila79(a), "a pressão passou: o item sobe"
    a.maquina(carga=40)
    a.orq("gerente", "absorver")
    assert len(_log(a, "send.log")) == 2, "pressão nova, aviso novo"


def test_ticket79_pressao_por_memoria_livre_e_por_percentual_livre():
    a = Amb(run="run_a")
    for kw, trecho in (({"mem_livre_mb": 2000}, "memória livre 2000 MB (mínimo 3072 MB)"), ({"livre_pct": 9}, "memória livre 9% (mínimo 15%)")):
        a.maquina(**kw)
        out = json.loads(_desp79(a, "Ticket 05", 2).stdout)
        assert out["estado"] == "enfileirado" and trecho in out["motivo"], out
        for i in _fila79(a):
            a.orq("fila-despacho", "rm", i["id"])
    a.maquina()
    assert json.loads(_desp79(a, "Ticket 05", 2).stdout)["dispatchId"], "máquina folgada: sobe"


def _sem_processos():
    """Um ORQ_PROCESSOS com a lista vazia: o `orq` não chama o ps real."""
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    f.write("[]")
    f.close()
    return f.name


def test_ticket79_pressao_com_pausar_sob_pressao_ligado_o_gerente_pausa_o_de_menor_prioridade_sozinho():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", ORQ_PAUSA_ESPERA_S="6", ORQ_PAUSA_POLL_S="0.2", ORQ_PROCESSOS=_sem_processos())  # sem o ps real: com a máquina carregada ele passa de 1 s e o PAUSA.md deixa de ser novo
    _gerente(a)
    _pausa51(a)
    a.maquina(carga=40)
    a.orq("gerente", "absorver")
    (env,) = _log(a, "send.log")
    assert "orq pausar task_term_f" in env[env.index("--text") + 1] and not _log(a, "close.log"), "sem a regra, só propõe: o failover P3, e não o P3 em review"
    a.orq("maquina", "set", "pausar_sob_pressao", "true")
    a.maquina()
    a.orq("gerente", "absorver")  # volta a normal: reabre o aviso
    a.maquina(carga=40)
    f = _escreve_pausa51(a, "f")
    r = a.orq("gerente", "absorver")
    f.result()
    assert r.returncode == 0, r.stderr
    assert [c[c.index("--terminal") + 1] for c in _log(a, "close.log")] == ["term_f"] and "pausa automática: task_term_f pausado" in r.stdout, r.stdout


def test_ticket79_modelo_caro_com_o_teto_cheio_enfileira_e_o_barato_sobe_e_quando_o_opus_libera_o_da_fila_sobe():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Opus 1", OPUS), ("Opus 2", OPUS)])
    out = json.loads(_desp79(a, "Terceiro opus", 1, OPUS).stdout)
    assert out["estado"] == "enfileirado" and "2/2 workers caros" in out["motivo"], out
    out = json.loads(_desp79(a, "Um sonnet", 2, SONNET).stdout)
    assert out["dispatchId"] and out["taskId"], "o barato continua subindo com vaga geral"
    assert _titulos_iniciados79(a) == ["Um sonnet"] and [i["titulo"] for i in _fila79(a)] == ["Terceiro opus"]
    a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Um sonnet"], "dois Opus ainda rodando"
    ws = json.load(open(os.path.join(a.fake, "workers.json")))
    a.set("workers.json", ws)
    _libera79(a, "term_v0")
    r = a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Um sonnet", "Terceiro opus"] and not _fila79(a), r.stdout
    assert [c[c.index("--model") + 1] for c in _log(a, "started.log")] == [SONNET, OPUS]


def test_ticket79_nunca_rebaixa_o_modelo_o_opus_espera_na_fila_e_sobe_com_o_mesmo_modelo_e_effort():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Opus 1", OPUS), ("Opus 2", OPUS)])
    r = a.orq("despachar", "--run", "run_a", "--titulo", "Opus que espera", "--spec-arquivo", _spec(a), "--modelo", OPUS, "--effort", "xhigh", "--prioridade", "1")
    assert json.loads(r.stdout)["estado"] == "enfileirado" and not _log(a, "started.log"), r
    (it,) = _fila79(a)
    assert (it["modelo"], it["effort"]) == (OPUS, "xhigh"), "a fila guarda o pedido como veio"
    for _ in range(3):
        a.orq("gerente", "absorver")
    assert not _log(a, "started.log"), "o orq não troca por um barato para subir antes: espera"
    assert (_fila79(a)[0]["modelo"], _fila79(a)[0]["effort"]) == (OPUS, "xhigh")
    _libera79(a, "term_v1")
    a.orq("gerente", "absorver")
    (arg,) = _log(a, "started.log")
    assert (arg[arg.index("--model") + 1], arg[arg.index("--effort") + 1], arg[arg.index("--task-title") + 1]) == (OPUS, "xhigh", "Opus que espera"), arg
    assert not _fila79(a)


def test_ticket79_o_opus_bloqueado_da_fila_nao_trava_o_barato_de_prioridade_menor_e_o_padrao_do_modelo_e_glob():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Opus 1", OPUS), ("Opus 2", OPUS), ("Sonnet 1", SONNET), ("Sonnet 2", SONNET)])
    assert json.loads(_desp79(a, "Opus P1", 1, OPUS).stdout)["estado"] == "enfileirado"
    assert json.loads(_desp79(a, "Sonnet P3", 3, SONNET).stdout)["estado"] == "enfileirado"
    _libera79(a, "term_v2")
    a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Sonnet P3"] and [i["titulo"] for i in _fila79(a)] == ["Opus P1"], "uma vaga geral, mas os caros estão cheios"
    m = orq_mod.modelo_caro
    assert (m("claude-opus-5-5"), m("gpt-6-astra"), m("gpt-6-sol"), m("GPT-6-SOL"), m("claude-sonnet-5-5"), m("gpt-6-luna"), m(None)) == (True, True, True, True, False, False, False)
    cfg = json.loads(a.orq("maquina", "set", "modelos_caros", '["claude-sonnet-*"]').stdout)
    assert orq_mod.modelo_caro("claude-sonnet-5-5", cfg) and not orq_mod.modelo_caro("claude-opus-5-5", cfg), "a lista de padrões é configurável"


def test_ticket79_max_caros_ajustavel_e_retomar_conta_os_caros_igual():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="1")
    _frota79(a, vivos=[("Opus 1", OPUS), ("Opus 2", OPUS)], mortos=[("Opus caído", OPUS), ("Sonnet caído", SONNET)])
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    res = {w["titulo"]: w for w in json.loads(a.orq("retomar", "--json").stdout)["workers"]}
    assert (res["Opus caído"]["estado"], res["Sonnet caído"]["estado"]) == ("enfileirado", "retomado"), res
    assert "2/2 workers caros" in res["Opus caído"]["aviso"] and [i["titulo"] for i in _fila79(a)] == ["Opus caído"]
    b = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="1")
    _frota79(b, vivos=[("Opus 1", OPUS), ("Opus 2", OPUS)], mortos=[("Opus caído", OPUS)])
    b.orq("maquina", "set", "max_caros", "3")
    b.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    assert json.loads(b.orq("retomar", "--json").stdout)["workers"][0]["estado"] == "retomado", "max_caros ajustado para 3"


def test_ticket79_retomar_pausados_respeita_o_teto_e_deixa_o_resto_pausado():
    a = Amb(run="run_a", ORQ_RETOMAR_ESPERA_S="1")
    _frota79(a, vivos=[(f"Vivo {n}", SONNET) for n in range(3)])
    os.makedirs(os.path.join(a.wt, "p"))
    json.dump({"pausados": {f"ctx_p{n}": {"task": f"task_p{n}", "run": "run_a", "titulo": f"Pausado {n}", "prioridade": 2, "agente": "claude", "modelo": SONNET, "effort": "medium",
                                         "sessao": f"s{n}", "cwd": os.path.join(a.wt, "p"), "terminal": f"term_p{n}"} for n in range(2)}}, open(os.path.join(a.home, "cursor.json"), "w"))
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    res = json.loads(a.orq("retomar", "--pausados", "--json").stdout)["workers"]
    assert [w["estado"] for w in res] == ["retomado", "sem_vaga"] and "3/4" not in res[1]["aviso"] and "4/4 workers vivos" in res[1]["aviso"], res
    assert list(_cursor(a)["pausados"]) == ["ctx_p1"], "o que não coube continua pausado"


def test_ticket79_relancar_conta_igual_o_opus_a_mais_e_recusado_sem_parar_ninguem():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _ctl_env(a, repo)
        ws = json.load(open(os.path.join(a.fake, "workers.json")))
        a.set("workers.json", ws + [_w48("term_v0", modelo=OPUS), _w48("term_v1", modelo=OPUS)])
        a.set("terminals.json", ["term_coord", "term_w1", "term_v0", "term_v1"])
        r = a.orq("relancar", "ctx_w1", "--nota", "x", "--modelo", OPUS, "--effort", "high")
        assert r.returncode == 1 and "2/2 workers caros" in r.stderr and "nada foi parado" in r.stderr, r
        assert not _log(a, "stopped.log") and not _log(a, "started.log")
        r = a.orq("relancar", "ctx_w1", "--nota", "x")
        assert r.returncode == 0, r.stderr
        assert _log(a, "stopped.log") and _log(a, "started.log"), "o mesmo modelo barato troca um por um: o teto não conta o próprio"
        b = Amb(run="run_a")
        _ctl_env(b, repo, modelo=OPUS)
        wb = json.load(open(os.path.join(b.fake, "workers.json")))
        b.set("workers.json", wb + [_w48("term_v0", modelo=OPUS)])
        b.set("terminals.json", ["term_coord", "term_w1", "term_v0"])
        assert b.orq("relancar", "ctx_w1", "--nota", "x", "--modelo", OPUS, "--effort", "high").returncode == 0, "o Opus que sai libera a vaga de caro para o Opus novo"


def test_ticket79_status_painel_e_digest_mostram_vagas_ocupadas_livres_e_a_fila():
    a = Amb(run="run_a")
    _pausa51(a)
    out = json.loads(_desp79(a, "Ticket 06", 2).stdout)
    assert out["estado"] == "enfileirado"
    a.orq("ingest", "--refresh")
    ab = json.load(open(os.path.join(a.home, "aberto.json")))["maquina"]
    assert (ab["max_workers"], ab["ocupadas"], ab["livres"], ab["caros"]) == (4, 4, 0, 4) and [i["titulo"] for i in ab["fila"]] == ["Ticket 06"], ab
    st = a.orq("status").stdout
    assert "Máquina: 4/4 workers (4/2 caros), 0 vagas livres; 1 na fila de despacho: P2 Ticket 06" in st, st
    a.orq("digest", "--html")
    d = json.load(open(os.path.join(a.home, "digest", "atual.json")))
    assert "maquina" not in d, "o contrato do digest não muda"
    pagina = open(os.path.join(a.home, "digest", [f for f in os.listdir(os.path.join(a.home, "digest")) if f.endswith(".html")][0])).read()
    assert "Máquina: 4/4 vagas ocupadas, 0 livres, 1 na fila de despacho" in pagina


def test_ticket79_digest_nao_conta_o_worker_hibernado_nas_vagas_ocupadas():
    agentes = [{"titulo": "vivo", "estado": "rodando", "desde": None}, {"titulo": "dormindo", "estado": "hibernado", "desde": None}]
    agora = datetime.now(timezone.utc)
    d = orq_mod.monta_digest([], {}, {}, {"agentes": agentes}, [], {"passos": []}, "", agora, maquina={"max_workers": 4})
    assert [r["titulo"] for r in d["rodando"]] == ["vivo", "dormindo"], "o hibernado segue listado"
    assert (d["maquina"]["ocupadas"], d["maquina"]["livres"]) == (1, 3), d["maquina"]


def test_ticket79_status_sem_worker_sem_fila_e_sem_pressao_nao_diz_nada_da_maquina():
    a = Amb(run="run_a")
    assert "Máquina:" not in a.orq("status").stdout
    a.maquina(carga=30)
    assert "PRESSÃO ALTA: carga 30 (máximo 12)" in a.orq("status").stdout


def test_ticket79_item_da_fila_que_falha_tres_vezes_sai_e_o_segurado_pelo_uso_espera():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[(f"Vivo {n}", SONNET) for n in range(4)])
    assert _desp79(a, "Ticket 05", 2).returncode == 0
    _libera79(a, "term_v0")
    _uso51(a, semana=95)
    r = a.orq("gerente", "absorver")
    assert "segue esperando" in r.stdout and "uso do plano" in r.stdout, r.stdout
    (it,) = _fila79(a)
    assert it["falhas"] == 0 and it["nao_antes"] > time.time(), "o uso do plano segura sem contar como falha"
    n = len([e for e in a.events() if e["tipo"] == "uso_parou"])
    a.orq("gerente", "absorver")
    assert len([e for e in a.events() if e["tipo"] == "uso_parou"]) == n, "dentro da espera nem tenta de novo"
    _uso51(a, semana=10)
    it["nao_antes"] = 0
    json.dump({"itens": [it]}, open(os.path.join(a.home, "fila-despacho.json"), "w"))
    a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Ticket 05"]
    b = _painel79()
    _gerente(b)
    _frota79(b, vivos=[(f"Vivo {n}", SONNET) for n in range(4)])
    assert _desp79(b, "Ticket 07", 2).returncode == 0
    _libera79(b, "term_v0")
    (it,) = _fila79(b)
    json.dump({"itens": [{**it, "run": "run_que_nao_e_do_gerente"}]}, open(os.path.join(b.home, "fila-despacho.json"), "w"))
    for _ in range(3):
        b.orq("gerente", "absorver")
    assert not _fila79(b) and [e["op"] for e in b.events() if e["tipo"] == "despacho_fila"][-1] == "desistiu" and not _log(b, "started.log")


def test_ticket79_leitura_real_do_sistema_tem_as_quatro_medidas_no_macos():
    if sys.platform != "darwin":
        return
    antes = os.environ.pop("ORQ_MAQUINA_LEITURA", None)
    try:
        l = orq_mod.maquina_ler()
    finally:
        if antes:
            os.environ["ORQ_MAQUINA_LEITURA"] = antes
    assert l["mem_livre_mb"] > 0 and 0 <= l["livre_pct"] <= 100 and l["carga"] >= 0 and l["ncpu"] >= 1, l
    assert set(l["rss_mb"]) == {"claude", "codex", "node", "docker"} and all(v >= 0 for v in l["rss_mb"].values()), l


# ---------- ticket 84: pausar encerra os processos em segundo plano do worker ----------

SNAP84 = "/bin/zsh -c source /Users/leo/.claude/shell-snapshots/snapshot-zsh-1.sh && eval '%s'"


def _procs84(a, teimoso=False):
    """Dois claude (f e i) com a pasta de cada um; f tem um shell de teste com um node abaixo e um monitor (`until`) que, com `teimoso`, ignora o SIGTERM; i tem o seu."""
    cl = "claude --dangerously-skip-permissions --model claude-opus-5-5"
    ps = [{"pid": 7, "ppid": 1, "rss": 1000, "args": "/usr/bin/outro-programa", "cwd": None},
          {"pid": 100, "ppid": 1, "rss": 600_000, "args": cl, "cwd": a.wt + "/f"},
          {"pid": 101, "ppid": 100, "rss": 150_000, "args": "node /opt/mcp/server.js", "cwd": None},
          {"pid": 150, "ppid": 100, "rss": 5000, "args": SNAP84 % "python3 -m pytest test_orq.py", "cwd": None},
          {"pid": 151, "ppid": 150, "rss": 5000, "args": "python3 -m pytest test_orq.py", "cwd": None},
          {"pid": 152, "ppid": 100, "rss": 5000, "args": SNAP84 % "until false; do sleep 1; done", "cwd": None, **({"ignora_term": True} if teimoso else {})},
          {"pid": 200, "ppid": 1, "rss": 600_000, "args": cl, "cwd": a.wt + "/i"},
          {"pid": 250, "ppid": 200, "rss": 5000, "args": SNAP84 % "bash scripts/e2e-infra.sh test", "cwd": None}]
    a.set("../procs.json", ps)
    a.env["ORQ_PROCESSOS"] = os.path.join(a.tmp.name, "procs.json")


def _pausa84(teimoso=False):
    a = Amb(run="run_a", ORQ_PAUSA_ESPERA_S="6", ORQ_PAUSA_POLL_S="0.2", ORQ_ENCERRA_ESPERA_S="1")
    _pausa51(a)
    _procs84(a, teimoso)
    f = _escreve_pausa51(a, "f")
    r = a.orq("pausar", "task_term_f", "--json")
    f.result()
    assert r.returncode == 0, r.stderr
    return a, json.loads(r.stdout)["pausados"][0], {p["pid"] for p in json.load(open(os.path.join(a.tmp.name, "procs.json")))}


def test_ticket84_pausar_encerra_os_filhos_do_worker_e_preserva_o_que_esta_fora_da_arvore():
    a, w, vivos = _pausa84()
    assert w["estado"] == "pausado" and {p["pid"] for p in w["encerrados"]} == {150, 151, 152}, w
    assert vivos == {7, 100, 101, 200, 250}, "o agente e o MCP dele (o close leva), o outro worker e o programa alheio ficam"
    assert all(p["sinal"] == "TERM" for p in w["encerrados"])


def test_ticket84_filho_que_ignora_sigterm_recebe_sigkill():
    a, w, vivos = _pausa84(teimoso=True)
    assert {p["pid"]: p["sinal"] for p in w["encerrados"]} == {150: "TERM", 151: "TERM", 152: "KILL"}, w
    assert 152 not in vivos and 250 in vivos


def test_ticket84_o_evento_da_pausa_lista_o_que_saiu():
    a, w, _ = _pausa84(teimoso=True)
    ev = next(e for e in a.events() if e["tipo"] == "pausa_plano")
    assert sorted((p["pid"], p["sinal"]) for p in ev["encerrados"]) == [(150, "TERM"), (151, "TERM"), (152, "KILL")], ev
    assert "pytest" in next(p for p in ev["encerrados"] if p["pid"] == 151)["args"]


def test_ticket84_aviso_de_pressao_mostra_quantos_processos_em_segundo_plano_cada_worker_tem():
    a = _painel79()
    _gerente(a)
    _pausa51(a)
    _procs84(a)
    a.maquina(carga=40)
    assert _desp79(a, "Ticket 05", 1).returncode == 0
    a.orq("gerente", "absorver")
    ev = next(e for e in a.events() if e["tipo"] == "maquina_aviso")
    assert ev["filhos"] == {"task_term_f": 3, "task_term_i": 1}, ev
    texto = next(c[c.index("--text") + 1] for c in _log(a, "send.log") if c[c.index("--terminal") + 1] == "term_coord")
    assert "task_term_f 3" in texto and "task_term_i 1" in texto, texto


# ---------- ticket 60: hibernar worker ocioso e acordar quando precisar ----------

HIB60 = {"ORQ_HIBERNA_VOLTA_S": "0", "ORQ_HIBERNA_RSS_ESPERA_S": "1", "ORQ_RETOMAR_ESPERA_S": "2"}
FILHO60 = "/bin/zsh -c source /Users/leo/.claude/shell-snapshots/snapshot-zsh-1.sh && eval 'bash scripts/e2e-infra.sh test'"


def _z60(minutos):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - minutos * 60))


def _procs60(a, extra=(), sem=()):
    """Um claude (600000 KB) com um servidor MCP filho (150000 KB) por worker, no cwd da worktree dele, mais um processo que não é agente."""
    ps = [{"pid": 7, "ppid": 1, "rss": 99_999_999, "args": "/usr/bin/outro-programa", "cwd": None}]
    for i, n in enumerate(a.nomes60):
        if n in sem:
            continue
        ps += [{"pid": 100 + i * 10, "ppid": 1, "rss": 600_000, "args": "claude --dangerously-skip-permissions --model claude-opus-5-5", "cwd": a.wt + "/" + n, "terminal": "term_" + n},
               {"pid": 101 + i * 10, "ppid": 100 + i * 10, "rss": 150_000, "args": "node /opt/mcp/server.js", "cwd": None}]
    a.set("../procs.json", ps + list(extra))


def _hib60(a, parado_min=20, nomes=("w1",), **kw):
    """Workers claude parados no prompt há `parado_min` min (hooks: turno fechado) no run_a, com a tela ociosa e os processos de cada um. `kw` vai ao worker-list."""
    os.makedirs(a.home, exist_ok=True)
    a.wt, a.nomes60 = os.path.join(a.tmp.name, "wt"), nomes
    for n in nomes:
        os.makedirs(os.path.join(a.wt, n))
    a.set("workers.json", [_w48("term_" + n, agente="claude", modelo="claude-opus-5-5", **kw) for n in nomes])
    a.set("tasks_run_a.json", [{"id": "task_term_" + n, "task_title": "Ticket " + n, "status": kw.get("status", "dispatched"), "dispatch_id": "ctx_term_" + n, "created_at": _iso(-3600)} for n in nomes])
    a.set("terminals.json", ["term_coord", "term_ger", *("term_" + n for n in nomes)])
    json.dump({"ctx_term_" + n: {"task": "task_term_" + n, "sessao": "sess-" + n, "inicio": _z60(parado_min + 5), "fim": _z60(parado_min), "harness": "claude", "cwd": a.wt + "/" + n}
               for n in nomes}, open(os.path.join(a.home, "turnos.json"), "w"))
    a.set("screens.json", {"term_" + n: _tela52("tela-claude-ocioso.txt") for n in nomes} | {"term_ret1": ["esc to interrupt"], "term_ret2": ["esc to interrupt"]})
    a.env["ORQ_PROCESSOS"] = os.path.join(a.tmp.name, "procs.json")
    _procs60(a)


def _no_gerente60(a, **kw):
    """O agent manager ligado (term_ger, run_a) e a volta dele: `orq gerente absorver`."""
    _multi(a, {"run_a": "term_ger"}, ["run_a"])
    _hib60(a, **kw)
    return lambda: a.orq("gerente", "absorver")


def _hibernados60(a):
    try:
        return _cursor(a).get("hibernados") or {}
    except OSError:
        return {}


def test_ticket60_ocioso_ha_mais_de_n_min_hiberna_fecha_o_terminal_guarda_a_sessao_e_mede_o_rss():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    volta = _no_gerente60(a, parado_min=20)
    r = volta()
    assert r.returncode == 0 and "task_term_w1: hibernado (ocioso no prompt, ~732 MB)" in r.stdout, (r.stdout, r.stderr)
    assert [c[c.index("--terminal") + 1] for c in _log(a, "close.log")] == ["term_w1"]
    h = _hibernados60(a)["ctx_term_w1"]
    assert (h["sessao"], h["cwd"], h["modelo"], h["task"], h["run"], h["agente"], h["entregue"], h["motivo"]) == \
        ("sess-w1", a.wt + "/w1", "claude-opus-5-5", "task_term_w1", "run_a", "claude", False, "ocioso no prompt"), h
    ev = next(e for e in a.events() if e["tipo"] == "hibernar")
    assert (ev["dispatch"], ev["rss_antes_mb"], ev["rss_depois_mb"], ev["rss_liberado_mb"]) == ("ctx_term_w1", 732, 0, 732), "o RSS do claude e do MCP, não o do outro programa"
    assert h["rss_liberado_mb"] == 732
    assert not _log(a, "create.log"), "hibernar não sobe nada"
    volta()
    assert len(_log(a, "close.log")) == 1, "o hibernado não é fechado de novo"


def test_ticket60_ocioso_ha_menos_de_n_min_nao_hiberna_e_o_n_e_configuravel():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    volta = _no_gerente60(a, parado_min=10)
    assert volta().returncode == 0 and not _log(a, "close.log") and not _hibernados60(a), "10 min < 15"
    json.dump({"min": 5}, open(os.path.join(a.home, "hibernar.json"), "w"))
    volta()
    assert list(_hibernados60(a)) == ["ctx_term_w1"], "hibernar.json baixou o N para 5"
    b = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", ORQ_HIBERNA_MIN="30", **HIB60)
    _no_gerente60(b, parado_min=20)()
    assert not _hibernados60(b), "ORQ_HIBERNA_MIN=30 e o worker parado há 20 min"
    assert (orq_mod.HIBERNA_MIN, orq_mod.HIBERNA_EXTERNA_MIN) == (15, 2), "padrões do ticket"


def test_ticket60_nao_hiberna_com_shell_still_running_spinner_pergunta_presa_rascunho_ocupado_ou_processo_filho():
    def tela(nome):
        return lambda a: a.set("screens.json", {"term_w1": _tela52(nome)})
    casos = {
        "shell still running": tela("tela-claude-shell.txt"),
        "spinner": tela("tela-claude-spinner.txt"),
        "pergunta presa": tela("tela-permissao.txt"),
        "rascunho": lambda a: a.set("drafts.json", {"term_w1": "estava digitando"}),
        "no meio do turno": lambda a: a.set("busy.json", ["term_w1"]),
        "processo filho vivo": lambda a: _procs60(a, extra=[{"pid": 150, "ppid": 100, "rss": 5000, "args": FILHO60, "cwd": None}]),
        "sem prova do processo": lambda a: _procs60(a, sem=("w1",)),
    }
    for nome, prepara in casos.items():
        a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
        volta = _no_gerente60(a, parado_min=40)
        prepara(a)
        r = volta()
        assert r.returncode == 0, (nome, r.stderr)
        assert not _log(a, "close.log") and not _hibernados60(a), f"hibernou com {nome}"
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    volta = _no_gerente60(a, parado_min=40)
    _procs60(a, extra=[{"pid": 150, "ppid": 100, "rss": 5000, "args": FILHO60, "cwd": None}])
    volta()
    _procs60(a)  # o E2E acabou
    volta()
    assert list(_hibernados60(a)) == ["ctx_term_w1"], "sem o filho, a volta seguinte hiberna"


def test_ticket60_coordenador_gerente_e_terminal_que_nao_e_worker_do_orq_nunca_hibernam():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    volta = _no_gerente60(a, parado_min=40, nomes=("coord", "ger", "x", "w1"))  # term_coord e term_ger como workers ociosos; term_x sem hook do orq
    t = json.load(open(os.path.join(a.home, "turnos.json")))
    del t["ctx_term_x"]
    json.dump(t, open(os.path.join(a.home, "turnos.json"), "w"))
    r = volta()
    assert r.returncode == 0, r.stderr
    assert list(_hibernados60(a)) == ["ctx_term_w1"], "só o worker do orq hiberna"
    assert [c[c.index("--terminal") + 1] for c in _log(a, "close.log")] == ["term_w1"]
    for alvo in ("task_term_coord", "task_term_ger"):  # nem à mão
        r = a.orq("hibernar", alvo, "--run", "run_a", "--forcar")
        assert r.returncode == 1 and "coordenador ou o gerente" in r.stderr, (alvo, r.stderr)


def test_ticket60_espera_externa_conhecida_pendencia_pr_ou_ticket_bloqueado_hiberna_antes_do_n():
    pend = {"itens": [{"id": "teto-pod", "tipo": "decisao", "titulo": "Teto por pod?", "task": "task_term_w1"}]}
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    volta = _no_gerente60(a, parado_min=5)
    volta()
    assert not _hibernados60(a), "parado há 5 min sem nada que ele espere: fica"
    json.dump(pend, open(a.env["ORQ_PENDENCIAS"], "w"))
    assert "esperando: pendência teto-pod" in volta().stdout
    assert _hibernados60(a)["ctx_term_w1"]["motivo"] == "esperando: pendência teto-pod"
    b = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    _gh(b)
    _pr(b, PR1)
    volta = _no_gerente60(b, parado_min=5)
    assert b.orq("pr", "ligar", "task_term_w1", PR1).returncode == 0
    assert "esperando: PR #1216 esperando merge" in volta().stdout
    c = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    os.makedirs(c.env["ORQ_ISSUES"])
    open(os.path.join(c.env["ORQ_ISSUES"], "01-base.md"), "w").write("# 01: Base\n\nStatus: claimed\nBlocked by: (nenhum)\n\n## What to build\n\nx\n")
    open(os.path.join(c.env["ORQ_ISSUES"], "02-feature.md"), "w").write("# 02: Feature\n\nStatus: claimed\nBlocked by: 01\nTask: task_term_w1\n\n## What to build\n\nx\n")
    volta = _no_gerente60(c, parado_min=5)
    assert "esperando: ticket 02 bloqueado por 01" in volta().stdout


def test_ticket60_entregue_e_sem_liberar_hiberna_e_o_liberar_tira_da_lista():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    volta = _no_gerente60(a, parado_min=20, status="completed", terminal="retained")
    assert _agentes(a, "--run", "run_a", ORCA_TERMINAL_HANDLE="term_coord")["ctx_term_w1"]["estado"] == "entregue"
    assert "entregue e sem liberar" in volta().stdout
    h = _hibernados60(a)["ctx_term_w1"]
    assert h["entregue"] is True and [c[c.index("--terminal") + 1] for c in _log(a, "close.log")] == ["term_w1"]
    assert _agentes(a, "--run", "run_a", ORCA_TERMINAL_HANDLE="term_coord")["ctx_term_w1"]["estado"] == "hibernado", "o terminal fechado não o faz sumir: ele não foi liberado"
    r = a.orq("liberar", "ctx_term_w1", "--run", "run_a", ORCA_TERMINAL_HANDLE="term_ger")
    assert r.returncode == 0, r.stderr
    assert not _hibernados60(a), "liberado não volta"


def test_ticket60_steer_num_hibernado_acorda_com_resume_e_entrega_a_mensagem():
    a = Amb(run="run_a", **HIB60)
    _hib60(a)
    r = a.orq("hibernar", "task_term_w1", "--run", "run_a")
    assert r.returncode == 0 and "hibernado (manual)" in r.stdout and "RSS 732 -> 0 MB" in r.stdout, (r.stdout, r.stderr)
    assert _log(a, "close.log") and not _log(a, "create.log")
    r = a.orq("steer", "task_term_w1", "Faça o rebase na main antes do push")
    assert r.returncode == 0, r.stderr
    (c,) = _log(a, "create.log")
    assert c[c.index("--worktree") + 1] == "path:" + a.wt + "/w1"
    comando = c[c.index("--command") + 1]
    assert comando.startswith("claude --resume sess-w1 --model claude-opus-5-5 --dangerously-skip-permissions '"), comando
    assert "Faça o rebase na main antes do push" in comando and "hibernado" in comando and "--type escalation" in comando and "--task-id task_term_w1" in comando, comando
    assert not _enviados(a), "nada vai ao terminal morto: o resume leva a mensagem"
    assert not _hibernados60(a)
    tipos = [e["tipo"] for e in a.events()]
    assert tipos.count("acordar") == 1 and tipos.count("retomada") == 1 and "steer" in tipos, tipos
    (ev,) = [e for e in a.events() if e["tipo"] == "steer"]
    assert ev["acordado"] == "retomado" and ev["task"] == "task_term_w1", ev
    ag = _agentes(a, "--run", "run_a")["ctx_term_w1"]
    assert ag["terminal"] == "term_ret1" and ag["estado"] != "hibernado", "o terminal novo é o do dispatch"


def test_ticket60_responder_a_um_hibernado_responde_e_acorda():
    a = Amb(run="run_a", **HIB60)
    _hib60(a)
    assert a.orq("hibernar", "ctx_term_w1", "--run", "run_a").returncode == 0
    _inbox(a, {**_pergunta_ask(50, "ctx_term_w1"), "type": "escalation"})
    r = a.orq("responder", "msg_q50", "use o índice composto")
    assert r.returncode == 0, r.stderr
    (chamada,) = _log(a, "replied.log")
    assert chamada[:5] == ["reply", "--id", "msg_q50", "--body", "use o índice composto"], chamada
    (c,) = _log(a, "create.log")
    comando = c[c.index("--command") + 1]
    assert "claude --resume sess-w1" in comando and "use o índice composto" in comando and "msg_q50" in comando, comando
    (ev,) = [e for e in a.events() if e["tipo"] == "resposta_worker"]
    assert ev["acordado"] == "retomado" and not _hibernados60(a), ev


def test_ticket60_pendencia_respondida_e_pr_mergeado_acordam_pelo_gerente_mas_o_entregue_nao():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    _gh(a)
    _pr(a, PR1)
    volta = _no_gerente60(a, parado_min=20, nomes=("w1", "w2"))
    json.dump({"itens": [{"id": "teto-pod", "tipo": "decisao", "titulo": "Teto por pod?", "task": "task_term_w2"}]}, open(a.env["ORQ_PENDENCIAS"], "w"))
    assert a.orq("pr", "ligar", "task_term_w1", PR1).returncode == 0
    volta()
    assert set(_hibernados60(a)) == {"ctx_term_w1", "ctx_term_w2"}
    assert not _log(a, "create.log"), "nada o acorda sozinho"
    _pr(a, PR1, state="MERGED", base="development")
    r = volta()
    assert "task_term_w1: acordado (o PR #1216 entrou em development)" in r.stdout, (r.stdout, r.stderr)
    assert set(_hibernados60(a)) == {"ctx_term_w2"}
    assert a.orq("pend", "done", "teto-pod", "--resposta", "usar 4").returncode == 0
    r = volta()
    assert "task_term_w2: acordado (a pendência teto-pod foi respondida: usar 4)" in r.stdout, r.stdout
    c1, c2 = _log(a, "create.log")
    assert "claude --resume sess-w1" in c1[c1.index("--command") + 1] and "PR #1216 entrou em development" in c1[c1.index("--command") + 1]
    assert "claude --resume sess-w2" in c2[c2.index("--command") + 1] and "usar 4" in c2[c2.index("--command") + 1]
    assert not _hibernados60(a)
    e = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger", **HIB60)
    _gh(e)
    _pr(e, PR1)
    volta = _no_gerente60(e, parado_min=20, status="completed", terminal="retained")
    e.orq("pr", "ligar", "task_term_w1", PR1)
    volta()
    _pr(e, PR1, state="MERGED")
    volta()
    assert list(_hibernados60(e)) == ["ctx_term_w1"] and not _log(e, "create.log"), "quem já entregou não volta sozinho por causa do merge"


def test_ticket60_o_estado_sobrevive_a_uma_queda_e_o_retomar_nao_sobe_hibernados_sem_motivo():
    a = Amb(run="run_a", **HIB60)
    _hib60(a, nomes=("w1", "w2"))
    assert a.orq("hibernar", "task_term_w1", "--run", "run_a").returncode == 0
    a.set("terminals.json", ["term_coord"])  # a queda levou o terminal do w2 também
    r = a.orq("retomar", "--dry-run", "--json")
    assert r.returncode == 0, r.stderr
    assert [(w["task"], w["estado"]) for w in json.loads(r.stdout)["workers"]] == [("task_term_w2", "a_retomar")], "o w1 hibernado fica de fora"
    assert "ctx_term_w1" in _hibernados60(a), "o cursor.json guardou a sessão"
    ag = _agentes(a, "--run", "run_a")["ctx_term_w1"]
    assert ag["estado"] == "hibernado", "depois da queda ele continua hibernado, não 'sem terminal'"
    assert a.orq("acordar", "task_term_w1").returncode == 0 and len(_log(a, "create.log")) == 1


def test_ticket60_agentes_e_digest_mostram_hibernado_desde_hh_mm_e_a_economia():
    a = Amb(run="run_a", **HIB60)
    _hib60(a)
    assert a.orq("hibernar", "task_term_w1", "--run", "run_a").returncode == 0
    ag = _agentes(a, "--run", "run_a")["ctx_term_w1"]
    assert ag["estado"] == "hibernado" and ag["hibernado_desde"] and ag["motivo_hibernado"] == "manual", ag
    hora = orq_mod._hora_local(ag["hibernado_desde"])
    txt = a.orq("agentes", "--run", "run_a").stdout
    assert f"hibernado desde {hora} (manual)" in txt and "orq acordar task_term_w1" in txt, txt
    assert "Hibernados: 1, ~732 MB de RSS liberados" in txt, txt
    assert a.orq("ingest", "--refresh").returncode == 0 and a.orq("digest").returncode == 0
    rod = json.load(open(os.path.join(a.home, "digest", "atual.json")))["rodando"]
    assert [r["estado"] for r in rod] == [f"hibernado desde {hora}"], rod


def test_ticket60_hibernar_e_acordar_a_mao_com_as_recusas_e_o_forcar():
    a = Amb(run="run_a", **HIB60)
    _hib60(a)
    _procs60(a, extra=[{"pid": 150, "ppid": 100, "rss": 5000, "args": FILHO60, "cwd": None}])
    r = a.orq("hibernar", "task_term_w1", "--run", "run_a", "--forcar")
    assert r.returncode == 1 and "processo filho vivo" in r.stderr and not _log(a, "close.log"), "nem --forcar passa processo filho"
    _procs60(a, sem=("w1",))
    r = a.orq("hibernar", "task_term_w1", "--run", "run_a")
    assert r.returncode == 1 and "--forcar" in r.stderr and not _log(a, "close.log"), r.stderr
    assert a.orq("hibernar", "task_term_w1", "--run", "run_a", "--forcar").returncode == 0 and _log(a, "close.log")
    r = a.orq("hibernar", "task_term_w1", "--run", "run_a")
    assert r.returncode == 1 and "hibernado" in r.stderr, "já hibernado"
    assert a.orq("hibernar", "task_nao_existe", "--run", "run_a").returncode == 1
    r = a.orq("acordar", "task_term_w1", "--texto", "o PR passou no CI")
    assert r.returncode == 0 and "retomado" in r.stdout, (r.stdout, r.stderr)
    (c,) = _log(a, "create.log")
    assert "o PR passou no CI" in c[c.index("--command") + 1]
    r = a.orq("acordar", "task_term_w1")
    assert r.returncode == 1 and "não está hibernado" in r.stderr, r.stderr


def test_ticket60_o_resume_adia_a_proxima_hibernacao_e_o_criterio_puro():
    cfg = {"min": 15, "externa_min": 2}
    agora = datetime.now(timezone.utc)
    base = {"estado": "parado", "turno": "parado", "task": "task_t", "turno_inicio": _z60(40), "turno_fim": _z60(20)}
    m = orq_mod.motivo_hibernar
    assert m(base, agora, cfg) == "ocioso no prompt"
    assert m(base, agora, cfg, acordada=_z60(1)) is None, "acordado há 1 min: o turno novo ainda não está no turnos.json"
    assert m(base, agora, cfg, acordada=_z60(30)) == "ocioso no prompt"
    assert m({**base, "turno_fim": _z60(10)}, agora, cfg) is None
    assert m({**base, "turno_fim": _z60(10)}, agora, cfg, pend=[{"id": "p", "task": "task_t"}]) == "esperando: pendência p"
    assert m({**base, "turno_inicio": _z60(5)}, agora, cfg) is None, "turno aberto depois do fim"
    for estado in ("travado", "perguntando", "nao_comecou", "hibernado", "liberado"):
        assert m({**base, "estado": estado}, agora, cfg) is None, estado
    assert m({**base, "tela": "1 shell still running (tela)"}, agora, cfg) is None
    assert m({**base, "turno": "aberto"}, agora, cfg) is None
    assert m({**base, "estado": "entregue", "turno": "unknown"}, agora, cfg) == "entregue e sem liberar"
    assert m({**base, "estado": "entregue", "turno": "unknown", "retido": "external_terminal"}, agora, cfg) is None


def test_ticket60_rss_e_processo_filho_sobre_a_lista_de_processos():
    ps = [{"pid": 1, "ppid": 0, "rss": 9_000_000, "args": "/sbin/launchd", "cwd": None},
          {"pid": 10, "ppid": 1, "rss": 512_000, "args": "claude --model x", "cwd": "/wt/a"}, {"pid": 11, "ppid": 10, "rss": 256_000, "args": "node mcp.js", "cwd": None},
          {"pid": 12, "ppid": 11, "rss": 256_000, "args": "node filho-do-mcp.js", "cwd": None},
          {"pid": 20, "ppid": 1, "rss": 1_024_000, "args": "codex --yolo", "cwd": "/wt/b"}]
    assert orq_mod.rss_agentes_mb(ps) == 2000 and orq_mod.rss_agentes_mb(None) is None
    assert orq_mod._processo_do_worker(ps, "/wt/a", "claude") == ([10], False)
    assert orq_mod._processo_do_worker(ps + [{"pid": 30, "ppid": 12, "rss": 1, "args": FILHO60, "cwd": None}], "/wt/a", "claude") == ([10], True), "o filho do filho conta"
    assert orq_mod._processo_do_worker(ps, "/wt/outra", "claude") == ([], None)
    assert orq_mod._processo_do_worker(ps, "/wt/b", "codex") == ([20], None), "o Codex não tem padrão de filho: sem prova"
    assert orq_mod._processo_do_worker(None, "/wt/a", "claude") == ([], None)


# ---------- ticket 78: orq retro (coletor determinístico de sinais de falha) ----------

def _ev78(tipo, ts, **kw):
    return {"ts": ts, "tipo": tipo, **kw}


def _eventos78():
    """Uma semana de fixture com pelo menos um caso de cada sinal de eventos; os de transcrito e de PR vêm de outros testes."""
    d = {"run": "run_x", "task": "task_a", "dispatch": "ctx_a"}
    return [
        _ev78("despacho", "2026-09-29T10:00:00Z", **d, titulo="orq: coisa A", modelo="claude-sonnet-5-5", effort="high", worktree="/wt/a"),
        _ev78("despacho", "2026-09-29T10:05:00Z", run="run_x", task="task_b", dispatch="ctx_b", titulo="orq: coisa B", modelo="claude-opus-5-5", effort="xhigh", worktree="/wt/b"),
        _ev78("nao_iniciou", "2026-09-29T10:06:00Z", run="run_x", task="task_b", dispatch="ctx_b", terminal="term_b"),
        _ev78("controle", "2026-09-29T10:07:00Z", acao="relancar", resultado="iniciado", run="run_x", task="task_b", dispatch="ctx_b", nota="a sessão abriu mas nunca recebeu o spec"),
        _ev78("controle", "2026-09-29T10:08:00Z", acao="relancar", resultado="falhou", run="run_x", task="task_b", dispatch="ctx_b", erro="cannot retry"),
        _ev78("controle", "2026-09-29T10:09:00Z", acao="interromper", resultado="ok", run="run_x", task="task_a", dispatch="ctx_a"),
        _ev78("steer", "2026-09-29T11:00:00Z", **d, texto="use outra chave", msg_id="msg_1"),
        _ev78("steer", "2026-09-29T11:10:00Z", **d, texto="outro ajuste", msg_id="msg_2"),
        _ev78("steer_fim", "2026-09-29T11:20:00Z", **d, msg_id="msg_2", motivo="lido", fonte="transcrito"),
        _ev78("steer_reentrega", "2026-09-29T11:05:00Z", **d, msg_id="msg_1", tentativa=1),
        _ev78("resposta_worker", "2026-09-29T12:00:00Z", **d, msg_id="msg_9", texto="serve a leitura padrão"),
        _ev78("worker_done", "2026-09-29T13:00:00Z", **d, msg="msg_d1", outcome="failed", subject="não deu"),
        _ev78("worker_done", "2026-09-29T13:01:00Z", run="run_x", task="task_b", dispatch="ctx_b", msg="msg_d2", outcome="succeeded", subject="pronto"),
        _ev78("entrega", "2026-09-29T13:02:00Z", run="run_x", task="task_b", dispatch="ctx_b", msg="msg_d2", avisos=[f"entrega sem commit: árvore suja em {orq_mod.ORQ_INSTALL}"]),
        _ev78("fim_dispatch", "2026-09-29T13:10:00Z", run="run_x", task="task_a", dispatch="ctx_a", motivo="sem worker_done", caminho="/wt/a", sujo=3, sem_push=0),
        _ev78("fim_dispatch", "2026-09-29T13:11:00Z", run="run_x", task="task_b", dispatch="ctx_b", motivo="entregue", caminho="/wt/b", sujo=0, sem_push=2),
        _ev78("gate_aviso", "2026-09-29T14:00:00Z", abertas=["e1", "e2"], sessao="s1"),
        _ev78("gate_aviso", "2026-09-29T14:05:00Z", abertas=["e2"], sessao="s1"),
        _ev78("intake", "2026-09-29T14:10:00Z", entrada="e1", efeito="descartado", nota="já tratado"),
        _ev78("intake", "2026-09-29T14:11:00Z", entrada="e2", efeito="tarefa", ref="task_a"),
        _ev78("alerta", "2026-09-29T15:00:00Z", alerta="steer_nao_lido", **d),
        _ev78("binding_perdido", "2026-09-29T15:30:00Z", run="run_x", sessao="s1"),
        _ev78("entrada", "2026-09-29T13:20:00Z", origem="usuario", texto="não, era o outro arquivo", id="e7"),
        _ev78("entrada", "2026-09-29T20:00:00Z", origem="usuario", texto="não sei se vale, o que acha?", id="e8"),  # longe de qualquer entrega: não é correção
    ]


def _sinais78(r):
    return {k: v["n"] for k, v in r["sinais"].items()}


def test_ticket78_retro_conta_e_agrupa_cada_sinal_dos_eventos():
    r = orq_mod.retro_coleta(_eventos78(), "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z")
    n = _sinais78(r)
    assert n["nao_iniciou"] == 1 and n["retry"] == 1 and n["controle_falhou"] == 1 and n["intervencao"] == 1, n
    assert n["steer_sem_leitura"] == 1 and n["steer_reentregue"] == 1, n  # msg_2 foi lido; msg_1 não
    assert n["pergunta_de_worker"] == 1 and n["worker_falhou"] == 1 and n["sem_entrega"] == 1, n
    assert n["liberado_sujo"] == 1 and n["liberado_sem_push"] == 1 and n["entrega_com_aviso"] == 1 and n["checkout_em_uso"] == 1, n
    assert n["entrada_sem_tratamento"] == 2 and n["intake_descartado"] == 1 and n["alerta"] == 1 and n["binding_perdido"] == 1, n  # e2 duas vezes conta uma
    assert n["correcao_do_usuario"] == 1, n
    por = r["sinais"]["nao_iniciou"]["casos"][0]
    assert por["titulo"] == "orq: coisa B" and por["modelo"] == "claude-opus-5-5" and por["effort"] == "xhigh", por
    assert "nunca recebeu o spec" in por["detalhe"], por  # o porquê vem da nota do relançamento
    assert por["ponteiro"].startswith("events.jsonl"), por
    assert r["falhas"] == sum(v for v in n.values() if v is not None) and r["falhas"] > 10, r["falhas"]
    m = r["por_modelo"]
    assert m["claude-opus-5-5/xhigh"]["despachos"] == 1 and m["claude-opus-5-5/xhigh"]["retry"] == 1, m
    assert m["claude-sonnet-5-5/high"]["worker_falhou"] == 1 and m["claude-sonnet-5-5/high"]["pergunta_de_worker"] == 1, m


def test_ticket78_retro_steer_depois_da_entrega_diz_que_o_worker_entregou_sem_o_ajuste():
    ev = [_ev78("steer", "2026-09-29T11:00:00Z", task="t", dispatch="ctx_z", texto="x", msg_id="msg_z"),
          _ev78("worker_done", "2026-09-29T11:09:00Z", task="t", dispatch="ctx_z", outcome="succeeded")]
    caso = orq_mod.retro_coleta(ev, "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z")["sinais"]["steer_sem_leitura"]["casos"][0]
    assert "entregou" in caso["detalhe"], caso


def test_ticket78_retro_respeita_a_janela_e_o_filtro_de_projeto():
    r = orq_mod.retro_coleta(_eventos78(), "2026-09-30T00:00:00Z", "2026-10-01T00:00:00Z")
    assert r["falhas"] == 0 and all(v["n"] == 0 for k, v in r["sinais"].items() if v["n"] is not None), r
    p = orq_mod.retro_coleta(_eventos78(), "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z", projeto="/wt/b")
    assert _sinais78(p)["nao_iniciou"] == 1 and _sinais78(p)["sem_entrega"] == 0 and _sinais78(p)["liberado_sujo"] == 0, _sinais78(p)


def test_ticket78_retro_correcao_so_conta_logo_depois_de_uma_entrega_e_com_a_palavra_certa():
    base = [_ev78("worker_done", "2026-09-29T10:00:00Z", task="t", dispatch="ctx_c", outcome="succeeded")]
    def n(texto, ts):
        ev = base + [_ev78("entrada", ts, origem="usuario", texto=texto, id="e1")]
        return orq_mod.retro_coleta(ev, "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z")["sinais"]["correcao_do_usuario"]["n"]
    assert n("Ops, errei o nome", "2026-09-29T10:10:00Z") == 1
    assert n("ajuste: troque o título", "2026-09-29T10:10:00Z") == 1
    assert n("Não era isso", "2026-09-29T10:10:00Z") == 1
    assert n("ótimo, siga", "2026-09-29T10:10:00Z") == 0
    assert n("não era isso", "2026-09-29T12:00:00Z") == 0, "duas horas depois já não é reação à entrega"
    assert n("nota: o ajuste ficou bom", "2026-09-29T10:10:00Z") == 0, "só a primeira palavra conta"


def _transcrito78(d, sessao, chamadas, cwd="/wt/a"):
    """Um transcrito do Claude Code com uma linha de assistente por chamada de ferramenta, mais uma fala que só cita os comandos."""
    os.makedirs(os.path.join(d, "proj"), exist_ok=True)
    with open(os.path.join(d, "proj", sessao + ".jsonl"), "w") as f:
        f.write(json.dumps({"type": "assistant", "cwd": cwd, "message": {"content": [{"type": "text", "text": "não vou rodar git push nem editar ~/.agents/skills/x"}]}}) + "\n")
        for nome, entrada in chamadas:
            f.write(json.dumps({"type": "assistant", "cwd": cwd, "timestamp": "2026-09-29T10:30:00Z", "message": {"content": [{"type": "tool_use", "name": nome, "input": entrada}]}}) + "\n")


def test_ticket78_retro_acha_as_regras_do_usuario_violadas_no_transcrito_do_worker():
    with tempfile.TemporaryDirectory() as d:
        _transcrito78(d, "sess1", [
            ("Bash", {"command": "git push origin feat/x"}),
            ("Bash", {"command": 'git commit -m "feat: x\n\nCo-Authored-By: Claude <noreply@anthropic.com>"'}),
            ("Edit", {"file_path": "/Users/leo/.agents/skills/retro/SKILL.md", "old_string": "a", "new_string": "b"}),
            ("Bash", {"command": "gh pr merge 12 --squash"}),
            ("Bash", {"command": "git merge feat/outra", "cwd": "x"}),
            ("Bash", {"command": "git status && git commit -m 'fix: ok'"}),
            ("Read", {"file_path": "/Users/leo/.agents/AGENTS.md"}),
        ], cwd=orq_mod.ORQ_INSTALL)
        ev = [_ev78("despacho", "2026-09-29T10:00:00Z", run="r", task="task_a", dispatch="ctx_a", titulo="orq: A", modelo="m", effort="high", worktree="/wt/a")]
        turnos = {"ctx_a": {"task": "task_a", "sessao": "sess1", "inicio": "2026-09-29T10:00:00Z"}}
        r = orq_mod.retro_coleta(ev, "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z", turnos=turnos, projetos=d)
        casos = r["sinais"]["regra_violada"]["casos"]
        regras = sorted(c["regra"] for c in casos)
        assert regras == ["agents_global", "checkout_em_uso_do_orq", "producao", "push_de_worker", "trailer"], regras
        assert all("sess1.jsonl" in c["ponteiro"] and c["dispatch"] == "ctx_a" for c in casos), casos
        sem = orq_mod.retro_coleta(ev, "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z")
        assert sem["sinais"]["regra_violada"]["n"] is None, "sem transcritos o sinal é 'não consultado', não zero"


def test_ticket78_retro_regra_so_vale_no_comando_de_verdade_nao_em_heredoc_nem_em_texto_entre_aspas():
    with tempfile.TemporaryDirectory() as d:
        _transcrito78(d, "sess2", [
            ("Bash", {"command": "python3 - <<'E'\ns = 'git push origin x; gh pr merge 1'\nE"}),
            ("Bash", {"command": 'echo "git push" && grep -n \'gh workflow run\' README.md'}),
            ("Bash", {"command": "cat > /tmp/x <<'E'\ngit commit -m y Co-Authored-By: z\nE"}),
        ])
        _transcrito78(d, "sess3", [
            ("Bash", {"command": "cd " + orq_mod.ORQ_INSTALL + " && git merge feat/x"}),
            ("Bash", {"command": "git commit -m \"$(cat <<'EOF'\nfeat: x\n\nCo-Authored-By: Claude <n@a.com>\nEOF\n)\""}),
            ("Bash", {"command": "ln -s ~/.claude/orq/skills/x ~/.agents/skills/x"}),
        ], cwd="/wt/a")
        ev = [_ev78("despacho", "2026-09-29T10:00:00Z", run="r", task="ta", dispatch="ctx_1", titulo="A", modelo="m", effort="high"),
              _ev78("despacho", "2026-09-29T10:01:00Z", run="r", task="tb", dispatch="ctx_2", titulo="B", modelo="m", effort="high")]
        turnos = {"ctx_1": {"sessao": "sess2"}, "ctx_2": {"sessao": "sess3"}}
        casos = orq_mod.retro_coleta(ev, "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z", turnos=turnos, projetos=d)["sinais"]["regra_violada"]["casos"]
        assert [(c["dispatch"], c["regra"]) for c in casos if c["dispatch"] == "ctx_1"] == [], casos
        assert sorted(c["regra"] for c in casos if c["dispatch"] == "ctx_2") == ["agents_global", "checkout_em_uso_do_orq", "trailer"], casos


def test_ticket78_retro_pr_com_check_vermelho_e_review_pedindo_mudanca():
    ev = [_ev78("pr", "2026-09-30T10:00:00Z", op="ligar", task="task_a", url="https://github.com/o/r/pull/7", numero=7, base="development", estado="aberto"),
          _ev78("pr", "2026-09-30T10:01:00Z", op="ligar", task="task_a", url="https://github.com/o/r/pull/8", numero=8, base="staging", estado="aberto")]
    def checks(url):
        return {"falhos": ["MCP typecheck"], "revisao": ""} if url.endswith("/7") else {"falhos": [], "revisao": "CHANGES_REQUESTED"}
    r = orq_mod.retro_coleta(ev, "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z", pr_checks=checks)
    assert r["sinais"]["pr_ci_vermelho"]["n"] == 1 and "MCP typecheck" in r["sinais"]["pr_ci_vermelho"]["casos"][0]["detalhe"], r["sinais"]["pr_ci_vermelho"]
    assert r["sinais"]["pr_ci_vermelho"]["casos"][0]["ponteiro"] == "https://github.com/o/r/pull/7"
    assert r["sinais"]["pr_pediu_mudanca"]["n"] == 1
    assert orq_mod.retro_coleta(ev, "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z")["sinais"]["pr_ci_vermelho"]["n"] is None, "sem gh: não consultado"
    sem_resposta = orq_mod.retro_coleta(ev, "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z", pr_checks=lambda u: None)
    assert sem_resposta["sinais"]["pr_ci_vermelho"]["n"] == 0, "gh sem resposta num PR não inventa falha"


def test_ticket78_orq_retro_periodo_sem_falhas_diz_zero_por_extenso():
    a = Amb()
    r = a.orq("retro", "--desde", "2026-09-29", "--ate", "2026-10-01", "--sem-gh", "--sem-transcritos")
    assert r.returncode == 0, r.stderr
    assert "falhas no período: 0" in r.stdout, r.stdout
    assert "nao_iniciou" in r.stdout and "correcao_do_usuario" in r.stdout, "cada sinal aparece, mesmo com zero"
    linha = next(l for l in r.stdout.splitlines() if l.startswith("nao_iniciou"))
    assert linha.split()[1] == "0", linha
    assert "n/d" in next(l for l in r.stdout.splitlines() if l.startswith("regra_violada")), "o que não foi consultado não vira zero"


def test_ticket78_orq_retro_com_eventos_imprime_ponteiros_json_e_compara_com_a_rodada_gravada():
    a = Amb()
    os.makedirs(a.home)
    with open(os.path.join(a.home, "events.jsonl"), "w") as f:
        for e in _eventos78():
            f.write(json.dumps(e) + "\n")
    args = ("retro", "--desde", "2026-09-29", "--ate", "2026-10-01", "--sem-gh", "--sem-transcritos")
    r = a.orq(*args)
    assert r.returncode == 0, r.stderr
    assert "events.jsonl:" in r.stdout and "orq: coisa B" in r.stdout and "claude-opus-5-5/xhigh" in r.stdout, r.stdout
    j = json.loads(a.orq(*args, "--json").stdout)
    assert j["sinais"]["nao_iniciou"]["n"] == 1 and j["falhas"] > 10, j["falhas"]
    linha_ponteiro = int(j["sinais"]["nao_iniciou"]["casos"][0]["ponteiro"].split(":")[1])
    assert json.loads(open(os.path.join(a.home, "events.jsonl")).read().splitlines()[linha_ponteiro - 1])["tipo"] == "nao_iniciou", "o ponteiro é a linha do log"
    assert a.orq(*args, "--gravar").returncode == 0
    snaps = os.listdir(os.path.join(a.home, "retro"))
    assert len(snaps) == 1, snaps
    r2 = a.orq("retro", "--desde", "2026-10-01", "--ate", "2026-10-08", "--sem-gh", "--sem-transcritos")
    assert "antes" in r2.stdout and "nao_iniciou" in r2.stdout, r2.stdout
    assert next(l for l in r2.stdout.splitlines() if l.startswith("nao_iniciou")).split()[1:3] == ["0", "1"], "esta rodada 0, a gravada 1"
    assert a.orq("retro", "--desde", "isto-nao-e-data").returncode != 0


def test_ticket78_digest_mostra_as_ultimas_rodadas_do_retro_so_quando_existem():
    a = Amb()
    d0 = json.load(open(a.orq("digest").stdout.splitlines()[0]))
    assert "retro" not in d0, "sem rodada gravada o contrato v1 fica como estava"
    os.makedirs(os.path.join(a.home, "retro"))
    json.dump({"desde": "2026-09-22T00:00:00Z", "ate": "2026-09-29T00:00:00Z", "falhas": 4, "metricas": {"nao_iniciou": 1, "regra_violada": None}}, open(os.path.join(a.home, "retro", "2026-09-29T0000.json"), "w"))
    d1 = json.load(open(a.orq("digest").stdout.splitlines()[0]))
    assert d1["retro"] == [{"ate": "2026-09-29T00:00:00Z", "falhas": 4, "metricas": {"nao_iniciou": 1, "regra_violada": None}}], d1.get("retro")


def _statusline(a, hud_saida="X"):
    os.makedirs(a.home, exist_ok=True)
    hud = os.path.join(a.home, "hud-falso.sh")
    open(hud, "w").write(f"cat >/dev/null\nprintf '%s\\n' '{hud_saida}'\n")
    env = {**os.environ, "ORQ_HOME": a.home, "ORQ_HUD": hud}
    return subprocess.run(["sh", os.path.join(AQUI, "statusline.sh")], input="{}", capture_output=True, text=True, env=env)


def test_ausente_ligar_cria_o_marcador_e_desligar_apaga():
    a = Amb(run="run_a")
    marca = os.path.join(a.home, "estado", "away")
    a.orq("ausente", "ligar")
    assert os.path.exists(marca) and len(open(marca).read()) == 5, marca
    a.orq("ausente", "desligar")
    assert not os.path.exists(marca)


def test_statusline_imprime_o_hud_igual_desligado_e_com_o_segmento_ligado():
    a = Amb(run="run_a")
    r = _statusline(a)
    assert r.returncode == 0 and r.stdout == "X\n", r
    a.orq("ausente", "ligar")
    r = _statusline(a)
    assert r.returncode == 0 and r.stdout.startswith("X ") and "away desde " in r.stdout and r.stdout.endswith("\n"), r
    assert _statusline(a, "A\nB").stdout.split("\n")[1] == "B"


def test_statusline_sai_0_e_imprime_o_hud_se_o_marcador_estiver_ilegivel():
    a = Amb(run="run_a")
    os.makedirs(os.path.join(a.home, "estado"))
    os.mkdir(os.path.join(a.home, "estado", "away"))  # diretório no lugar do arquivo: cat falha
    r = _statusline(a)
    assert r.returncode == 0 and r.stdout == "X\n", r


def _repo_vivo55(branches):
    """Repositório temporário no formato da instalação (orq.py, orqlib.py, scripts/integrar.py) com um branch por item de `branches`
    ({nome: {arquivo: texto}}). Devolve (vivo, ambiente) com o ORQ_WT_DIR de teste."""
    t = tempfile.mkdtemp()
    vivo = os.path.join(t, "orq")
    os.makedirs(os.path.join(vivo, "scripts"))
    for f in ("orq.py", "orqlib.py", "falha_segura.py"):
        shutil.copy(os.path.join(AQUI, f), vivo)
    shutil.copy(os.path.join(AQUI, "scripts", "integrar.py"), os.path.join(vivo, "scripts"))
    open(os.path.join(vivo, "nota.txt"), "w").write("a\nb\nc\n")
    env = {**os.environ, "ORQ_WT_DIR": os.path.join(t, "orq-wt"), "ORQ_TESTES": "true", "ORQ_LOG": os.path.join(t, "orq.log"),
           "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    g = lambda *a, cwd=vivo: subprocess.run(["git", *a], cwd=cwd, env=env, capture_output=True, text=True, check=True)  # noqa: E731
    g("init", "-q", "-b", "main")
    g("add", "-A")
    g("commit", "-qm", "base")
    for nome, arquivos in branches.items():
        g("checkout", "-qb", nome, "main")
        for arq, txt in arquivos.items():
            open(os.path.join(vivo, arq), "w").write(txt)
        g("add", "-A")
        g("commit", "-qm", nome)
    g("checkout", "-q", "main")
    return vivo, env, g


def test_ticket55_conflito_na_integracao_nao_toca_o_orq_instalado():
    vivo, env, g = _repo_vivo55({"um": {"nota.txt": "a\num\nc\n"}, "dois": {"nota.txt": "a\ndois\nc\n"}})
    antes = g("rev-parse", "HEAD").stdout
    r = subprocess.run([sys.executable, os.path.join(vivo, "scripts", "integrar.py"), "um", "dois"], cwd=vivo, env=env, capture_output=True, text=True)
    assert r.returncode != 0 and "conflito" in r.stdout + r.stderr, r.stdout + r.stderr
    assert g("rev-parse", "HEAD").stdout == antes and g("status", "--porcelain").stdout == "", "a main viva não anda nem suja"
    wt = os.path.join(env["ORQ_WT_DIR"], "integra-um-dois")
    assert "<<<<<<<" in open(os.path.join(wt, "nota.txt")).read(), "o conflito vive só na worktree"
    # o conflito simulado também no orqlib.py da worktree: o orq instalado segue importando
    open(os.path.join(wt, "orqlib.py"), "a").write("\n<<<<<<< HEAD\nx\n=======\ny\n>>>>>>> dois\n")
    ok = subprocess.run([sys.executable, os.path.join(vivo, "orq.py"), "--help"], capture_output=True, text=True)
    assert ok.returncode == 0 and "SyntaxError" not in ok.stderr, ok.stderr
    assert subprocess.run([sys.executable, "-c", "import ast,sys; ast.parse(open(sys.argv[1]).read())", os.path.join(wt, "orqlib.py")],
                          capture_output=True).returncode != 0, "a worktree de fato quebrou"


def test_ticket55_main_so_avanca_por_fast_forward_depois_dos_testes():
    vivo, env, g = _repo_vivo55({"um": {"um.txt": "1\n"}, "dois": {"dois.txt": "2\n"}})
    integrar = os.path.join(vivo, "scripts", "integrar.py")
    antes = g("rev-parse", "HEAD").stdout
    r = subprocess.run([sys.executable, integrar, "um", "dois"], cwd=vivo, env={**env, "ORQ_TESTES": "false"}, capture_output=True, text=True)
    assert r.returncode != 0 and g("rev-parse", "HEAD").stdout == antes, "teste vermelho: a main fica onde estava"
    shutil.rmtree(os.path.join(env["ORQ_WT_DIR"], "integra-um-dois"))
    g("worktree", "prune")
    g("branch", "-D", "integra/um-dois")
    r = subprocess.run([sys.executable, integrar, "um", "dois"], cwd=vivo, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    assert os.path.exists(os.path.join(vivo, "um.txt")) and os.path.exists(os.path.join(vivo, "dois.txt")), "a main viva recebeu os dois"
    assert g("rev-list", "--merges", "-n1", "HEAD").stdout.strip(), "o merge foi feito na worktree e chegou por fast-forward"
    assert not os.path.exists(os.path.join(env["ORQ_WT_DIR"], "integra-um-dois")), "worktree removida depois de entrar"


def test_ticket55_avancar_recusa_marcador_de_conflito_esquecido():
    vivo, env, g = _repo_vivo55({"um": {"nota.txt": "a\num\nc\n"}, "dois": {"nota.txt": "a\ndois\nc\n"}})
    integrar = os.path.join(vivo, "scripts", "integrar.py")
    subprocess.run([sys.executable, integrar, "um", "dois"], cwd=vivo, env=env, capture_output=True, text=True)
    wt = os.path.join(env["ORQ_WT_DIR"], "integra-um-dois")
    g("add", "nota.txt", cwd=wt)  # "resolveu" sem tirar os marcadores
    g("commit", "-qm", "merge", cwd=wt)
    antes = g("rev-parse", "HEAD").stdout
    r = subprocess.run([sys.executable, integrar, "--avancar", wt], cwd=vivo, env=env, capture_output=True, text=True)
    assert r.returncode != 0 and "marcador" in r.stdout + r.stderr and g("rev-parse", "HEAD").stdout == antes, r.stdout + r.stderr
    open(os.path.join(wt, "nota.txt"), "w").write("a\num e dois\nc\n")
    g("commit", "-qam", "resolve", cwd=wt)
    r = subprocess.run([sys.executable, integrar, "--avancar", wt], cwd=vivo, env=env, capture_output=True, text=True)
    assert r.returncode == 0 and open(os.path.join(vivo, "nota.txt")).read() == "a\num e dois\nc\n", r.stdout + r.stderr


def test_ticket55_hook_com_import_falho_sai_0_sem_saida_e_grava_o_log():
    t = tempfile.mkdtemp()
    for f in ("orq.py", "precompact.py", "falha_segura.py"):
        shutil.copy(os.path.join(AQUI, f), t)
    open(os.path.join(t, "orqlib.py"), "w").write("<<<<<<< HEAD\nx = 1\n=======\nx = 2\n>>>>>>> outro\n")
    log = os.path.join(t, "orq.log")
    env = {**os.environ, "ORQ_LOG": log, "ORCA_TERMINAL_HANDLE": "term_x"}
    for cmd in (["orq.py", "hook", "session"], ["orq.py", "hook", "prligar"], ["precompact.py"], ["precompact.py", "retomar"]):
        r = subprocess.run([sys.executable, os.path.join(t, cmd[0]), *cmd[1:]], input="{}", env=env, capture_output=True, text=True)
        assert (r.returncode, r.stdout, r.stderr) == (0, "", ""), (cmd, r.returncode, r.stdout, r.stderr)
    assert open(log).read().count("import falhou") == 4, open(log).read()
    # o hook de limpeza importa o orq da instalação (HOME/.claude/orq)
    casa = os.path.join(t, "casa")
    os.makedirs(os.path.join(casa, ".claude", "orq"))
    for f in ("orq.py", "orqlib.py", "falha_segura.py"):
        shutil.copy(os.path.join(t, f), os.path.join(casa, ".claude", "orq"))
    r = subprocess.run([sys.executable, LIMPAR], input="{}", env={**env, "HOME": casa}, capture_output=True, text=True)
    assert (r.returncode, r.stdout, r.stderr) == (0, "", ""), (r.returncode, r.stdout, r.stderr)
    # fora de um hook o erro continua alto: quem digita `orq status` precisa ver a causa
    r = subprocess.run([sys.executable, os.path.join(t, "orq.py"), "status"], env=env, capture_output=True, text=True)
    assert r.returncode != 0 and "SyntaxError" in r.stderr, r.stderr


# ---------- ticket 85: a pressão separa o que é do orq do que não é, e o Run do orq é isento ----------

def _p85(pid, ppid, cpu, args, rss=100_000):
    return {"pid": pid, "ppid": ppid, "cpu": cpu, "rss": rss, "args": args}


# o worker (claude e o node dele) e o que roda na máquina fora do orq
_WORKER85 = lambda cpu: [_p85(100, 1, 3, "/opt/homebrew/bin/claude --model x"), _p85(101, 100, cpu, "node /x/vitest.js")]
_FORA85 = [_p85(200, 1, 146, "/System/Library/Frameworks/CoreServices.framework/Metadata.framework/Support/mds_stores"),
           _p85(201, 1, 50, "/Applications/OrbStack.app/Contents/Frameworks/OrbStack Helper.app/Contents/MacOS/OrbStack Helper"),
           _p85(202, 1, 27, "/Applications/OrbStack.app/Contents/MacOS/OrbStack"), _p85(203, 1, 4, "/usr/bin/ssh")]


def _texto_aviso85(a):
    (env,) = _log(a, "send.log")
    return env[env.index("--text") + 1]


def test_ticket85_carga_vinda_de_fora_segura_o_despacho_lista_os_culpados_e_nao_sugere_pausa():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Vivo", SONNET)])
    a.orq("maquina", "set", "pausar_sob_pressao", "true")
    a.maquina(carga=14.2, processos=[*_WORKER85(13), *_FORA85])
    out = json.loads(_desp79(a, "Ticket 05", 2).stdout)
    assert out["estado"] == "enfileirado" and "carga 14.2 (máximo 12)" in out["motivo"] and "mds_stores 146%, OrbStack 77%" in out["motivo"], out
    a.orq("gerente", "absorver")
    texto = _texto_aviso85(a)
    assert "mds_stores 146%, OrbStack 77%" in texto and "fora do orq" in texto and "não alivia" in texto, texto
    assert "orq pausar" not in texto and not _log(a, "close.log"), "nem sugere nem pausa sozinho, mesmo com pausar_sob_pressao ligado"
    assert not _log(a, "started.log") and len(_fila79(a)) == 1, "o despacho novo segue seguro"
    assert [(e["tipo"], e["causa"]) for e in a.events() if e["tipo"] == "maquina_aviso"] == [("maquina_aviso", "fora")]
    assert "carga por dono: orq 16% de CPU" in a.orq("maquina").stdout and "fora do orq 227%" in a.orq("maquina").stdout


def test_ticket85_carga_vinda_dos_workers_sugere_pausar_o_de_menor_prioridade():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Vivo", SONNET)])
    a.maquina(carga=14.2, processos=[*_WORKER85(600), *_FORA85])
    assert json.loads(_desp79(a, "Ticket 05", 2).stdout)["estado"] == "enfileirado"
    a.orq("gerente", "absorver")
    texto = _texto_aviso85(a)
    assert "orq pausar task_term_v0" in texto and "fora do orq" not in texto, texto
    assert [e["causa"] for e in a.events() if e["tipo"] == "maquina_aviso"] == ["orq"]


def test_ticket85_abaixo_do_limite_nada_acontece_mesmo_com_processo_de_fora_pesado():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Vivo", SONNET)])
    a.maquina(carga=3.0, processos=[*_WORKER85(13), *_FORA85])
    assert json.loads(_desp79(a, "Ticket 05", 2).stdout)["dispatchId"], "sobe"
    a.orq("gerente", "absorver")
    assert not _log(a, "send.log") and not [e for e in a.events() if e["tipo"] == "maquina_aviso"]


def test_ticket85_o_total_conta_o_e2e_e_o_que_sobe_abaixo_dos_agentes_como_do_orq():
    o = orq_mod.maquina_origem([_p85(100, 1, 3, "/opt/homebrew/bin/codex"), _p85(101, 100, 40, "node mcp"), _p85(300, 1, 90, "bash scripts/e2e-infra.sh start"),
                                _p85(301, 300, 10, "mongod"), *_FORA85])
    assert (o["orq_cpu"], o["fora_cpu"]) == (143, 227) and [x["nome"] for x in o["fora_por_cpu"]] == ["mds_stores", "OrbStack", "ssh"], o
    assert orq_mod.maquina_origem(None) is None and orq_mod.maquina_origem([]) is None


def _runs85(a, objetivo):
    a.set("runs.json", [{"id": "run_a", "objective": objetivo}])


def test_ticket85_run_isento_sobe_sob_pressao_e_o_de_outro_run_enfileira():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Vivo", SONNET)])
    a.maquina(carga=40)
    _runs85(a, "Neo-jobs: insert diário")
    assert json.loads(_desp79(a, "Ticket 05", 2).stdout)["estado"] == "enfileirado", "Run de outra frente enfileira"
    _runs85(a, "Orquestrador: registro de tarefas")
    assert json.loads(_desp79(a, "Ticket 06", 2).stdout)["dispatchId"], "Run do orq sobe com a carga alta"
    assert _titulos_iniciados79(a) == ["Ticket 06"]
    a.orq("maquina", "set", "runs_isentos", '["run_a"]')
    _runs85(a, "Neo-jobs: insert diário")
    assert json.loads(_desp79(a, "Ticket 07", 2).stdout)["dispatchId"], "o id do Run também vale como padrão"


def test_ticket85_run_isento_sobe_sem_vaga_mas_o_piso_de_memoria_o_segura():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("V", SONNET)] * 4)
    _runs85(a, "Orquestrador: x")
    assert json.loads(_desp79(a, "Ticket 05", 2).stdout)["dispatchId"], "4/4 workers vivos e o Run isento sobe"
    a.maquina(mem_livre_mb=800, livre_pct=5)
    out = json.loads(_desp79(a, "Ticket 06", 2).stdout)
    assert out["estado"] == "enfileirado" and "piso de segurança (1024 MB)" in out["motivo"], out
    a.maquina(mem_livre_mb=2000, livre_pct=60)
    assert json.loads(_desp79(a, "Ticket 07", 2).stdout)["dispatchId"], "abaixo do mínimo mole, acima do piso: sobe"


def test_ticket85_run_isento_respeita_o_max_caros_e_o_opus_enfileira_mesmo_sob_pressao():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Opus 1", OPUS), ("Opus 2", OPUS)])
    _runs85(a, "Orquestrador: x")
    out = json.loads(_desp79(a, "Terceiro opus", 1, OPUS).stdout)
    assert out["estado"] == "enfileirado" and "2/2 workers caros" in out["motivo"], out
    a.maquina(carga=40)
    assert json.loads(_desp79(a, "Ticket 05", 2).stdout)["dispatchId"], "o barato do mesmo Run sobe com a carga alta e 2/4 workers"
    assert [i["titulo"] for i in _fila79(a)] == ["Terceiro opus"]
    a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Ticket 05"] and len(_fila79(a)) == 1, "sob pressão o gerente drena só o que o max_caros deixa: o opus segue na fila"


def test_ticket85_fila_do_run_isento_sobe_pelo_gerente_mesmo_sob_pressao():
    a = _painel79()
    _gerente(a)
    _frota79(a, vivos=[("Vivo", SONNET)])
    a.maquina(carga=40)
    _runs85(a, "Neo-jobs")
    _desp79(a, "Ticket 05", 2)
    _runs85(a, "Orquestrador: x")
    a.orq("gerente", "absorver")
    assert _titulos_iniciados79(a) == ["Ticket 05"] and not _fila79(a), "o item do Run isento sai da fila com a pressão alta"


# ---------- grupos e secondmates (ticket 80) ----------

GRUPOS_T = {"orq": {"projetos": ["/h/.claude/orq", "/h/.claude/dashboard"], "prefixos": ["orq:"]},
            "trabalho": {"projetos": ["/h/dev/web", "/h/dev/api"], "prefixos": ["web:", "api:"]},
            "pessoal": {"projetos": ["/h/dev/dbq"], "prefixos": ["dbq:"]}}


def test_grupo_de_roteia_pelo_explicito_pelo_titulo_e_pelo_cwd():
    g = orq_mod.grupo_de
    assert g(GRUPOS_T, titulo="orq: secondmate por grupo")[0] == "orq"
    assert g(GRUPOS_T, titulo="ORQ: maiúscula")[0] == "orq"
    assert g(GRUPOS_T, titulo="corrigir o filtro", cwd="/h/dev/api/src")[0] == "trabalho"
    assert g(GRUPOS_T, titulo="corrigir o filtro", cwd="/h/dev/api-velha")[0] is None  # prefixo de caminho não é pasta de dentro
    assert g(GRUPOS_T, titulo="dbq: x", cwd="/h/dev/web")[0] == "pessoal"  # o título vence o cwd
    assert g(GRUPOS_T, titulo="web: x", grupo="orq")[0] == "orq"  # o explícito vence tudo
    nome, motivo = g(GRUPOS_T, titulo="sem prefixo", cwd="/tmp")
    assert nome is None and "coordenador" in motivo
    try:
        g(GRUPOS_T, grupo="nenhum")
        assert False, "grupo inexistente passou"
    except ValueError as e:
        assert "nenhum" in str(e)


def test_ticket119_dentro_casa_por_identidade_de_arquivo_e_cai_na_string_se_a_pasta_sumiu():
    with tempfile.TemporaryDirectory() as t:
        real = os.path.join(os.path.realpath(t), "proj")
        os.makedirs(os.path.join(real, "src"))
        elo = os.path.join(os.path.realpath(t), "elo")
        os.symlink(real, elo)
        assert orq_mod._dentro(os.path.join(elo, "src"), real)  # symlink
        assert orq_mod._dentro(real, elo)
        assert orq_mod._dentro("/tmp", "/private/tmp") or not os.path.samefile("/tmp", "/private/tmp")
        assert orq_mod._dentro(os.path.join(elo, "src"), os.path.join(elo))
        assert not orq_mod._dentro(os.path.join(real, ".."), real)
        assert not orq_mod._dentro(t, real)
        sumida = os.path.join(real, "sumiu")
        assert orq_mod._dentro(os.path.join(sumida, "x"), sumida)  # fallback por string
        assert not orq_mod._dentro(os.path.join(real, "outro"), sumida)
        nome, _ = orq_mod.grupo_de({"g": {"projetos": [real]}}, cwd=os.path.join(elo, "src"))
        assert nome == "g"


def test_grupo_de_ambiguo_fica_com_o_coordenador():
    gs = {**GRUPOS_T, "painel": {"projetos": ["/h/.claude/dashboard"], "prefixos": ["orq: painel"]}}
    nome, motivo = orq_mod.grupo_de(gs, titulo="orq: painel novo")
    assert nome is None and "orq" in motivo and "painel" in motivo, motivo
    nome, motivo = orq_mod.grupo_de(gs, cwd="/h/.claude/dashboard/web")
    assert nome is None and "ambíguo" in motivo, motivo


def _grupo(a, nome="orq", **cfg):
    os.makedirs(os.path.join(a.home, "groups"), exist_ok=True)
    with open(os.path.join(a.home, "groups", f"{nome}.json"), "w") as f:
        json.dump({"projetos": [a.home], "prefixos": [f"{nome}:"], "modelo": "claude-sonnet-5-5", **cfg}, f)


def test_orq_grupos_lista_e_roteia_e_pula_arquivo_ruim():
    a = Amb()
    _grupo(a)
    with open(os.path.join(a.home, "groups", "quebrado.json"), "w") as f:
        f.write("{nao é json")
    r = a.orq("grupos")
    assert r.returncode == 0 and "orq" in r.stdout and "quebrado" not in r.stdout, r.stdout + r.stderr
    r = json.loads(a.orq("grupos", "--titulo", "orq: ticket pequeno").stdout)
    assert r["grupo"] == "orq" and r["mate"] is None, r  # grupo sem mate aberto: o coordenador despacha ele mesmo
    assert "quebrado" in a.log()


T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _z(seg):
    return (T0 + timedelta(seconds=seg)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _pedido(corr="p1", prazo=120, entregue=10):
    evs = [{"tipo": "mate_pedido", "corr": corr, "grupo": "orq", "texto": "x", "prazo": prazo, "ts": _z(0)}]
    if entregue is not None:
        evs.append({"tipo": "mate_entregue", "corr": corr, "ts": _z(entregue)})
    return evs


def _estado(evs, mates, seg):
    return {p["corr"]: p["estado"] for p in orq_mod.mate_pendentes(evs, mates, T0 + timedelta(seconds=seg))}


def _linhas_mate(a):
    return [x for x in a.orq("status").stdout.splitlines() if x.startswith("mate ")]


def test_orq_status_mostra_uma_linha_por_mate_trabalhando_caido_e_com_pedido():
    a = Amb()
    _grupo(a)
    _grupo(a, "dados")  # grupo sem mate: sem linha
    assert _linhas_mate(a) == []
    _mate_vivo(a)
    assert _linhas_mate(a) == ["mate orq: trabalhando (term_mate)"]
    a.set("terminals.json", ["term_coord"])  # o terminal do mate sumiu
    assert _linhas_mate(a) == ["mate orq: caiu (term_mate)"]
    a.set("terminals.json", ["term_mate", "term_coord"])
    with open(os.path.join(a.home, "events.jsonl"), "w") as f:
        f.write(json.dumps({"tipo": "mate_pedido", "corr": "p1", "grupo": "orq", "texto": "x", "prazo": 120, "ts": "2026-10-01T12:00:00Z"}) + "\n")
    assert _linhas_mate(a) == ["mate orq: trabalhando (term_mate) | pedidos: p1 a_entregar"]


def test_mate_pendentes_conta_o_prazo_do_fim_do_turno_que_recebeu_o_pedido():
    evs = _pedido()
    assert _estado(_pedido(entregue=None), {}, 5) == {"p1": "a_entregar"}
    # o mate entrou no turno depois da entrega e não terminou: um turno longo não estoura o prazo
    no_turno = {"orq": {"turnos": [[_z(1), _z(5)], [_z(11), None]]}}
    assert _estado(evs, no_turno, 1000) == {"p1": "aguardando"}
    fechou = {"orq": {"turnos": [[_z(11), _z(400)]]}}
    assert _estado(evs, fechou, 400 + 119) == {"p1": "aguardando"}  # o prazo conta do fim do turno, não da entrega
    assert _estado(evs, fechou, 400 + 121) == {"p1": "reenviar"}
    # o turno nem começou depois da entrega: conta da entrega
    assert _estado(evs, {"orq": {"turnos": [[_z(1), _z(2)]]}}, 10 + 121) == {"p1": "reenviar"}
    # turnos seguintes (aviso do Orca, heartbeat) não empurram o prazo: vale o primeiro depois da entrega
    assert _estado(evs, {"orq": {"turnos": [[_z(11), _z(30)], [_z(400), _z(410)]]}}, 500) == {"p1": "reenviar"}
    # turno sem fim (o Stop não rodou) não segura o pedido para sempre
    assert _estado(evs, {"orq": {"turnos": [[_z(11), None]]}}, 11 + orq_mod.TURNO_ABERTO_TETO_S + 1) == {"p1": "reenviar"}
    # uma repostagem, depois uma escalada, depois nada (nunca em laço)
    rep = [*evs, {"tipo": "mate_reenvio", "corr": "p1", "ts": _z(600)}]
    depois = {"orq": {"turnos": [[_z(11), _z(30)], [_z(601), _z(700)]]}}
    assert _estado(rep, depois, 700 + 60) == {"p1": "aguardando"}
    assert _estado(rep, depois, 700 + 121) == {"p1": "escalar"}
    esc = [*rep, {"tipo": "mate_escalado", "corr": "p1", "ts": _z(900)}]
    assert _estado(esc, depois, 5000) == {"p1": "escalado"}


def test_mate_pendentes_resolve_so_com_a_resposta_correlacionada():
    evs = _pedido()
    fechou = {"orq": {"turnos": [[_z(11), _z(20)]]}}
    outra = [*evs, {"tipo": "entrada", "id": "e9", "origem": "mate", "mate": "orq", "corr": "p7", "ts": _z(15)}]
    assert _estado(outra, fechou, 1000) == {"p1": "reenviar"}  # resposta de outro pedido não conta
    sem_corr = [*evs, {"tipo": "entrada", "id": "e9", "origem": "mate", "mate": "orq", "ts": _z(15)}]
    assert _estado(sem_corr, fechou, 1000) == {"p1": "reenviar"}  # subida sem corr (um resumo) também não
    certa = [*evs, {"tipo": "entrada", "id": "e9", "origem": "mate", "mate": "orq", "corr": "p1", "ts": _z(15)}]
    assert _estado(certa, fechou, 1000) == {}
    # prazo 0: não espera resposta, só a entrega
    assert _estado(_pedido(prazo=0), fechou, 9999) == {}
    assert _estado(_pedido(prazo=0, entregue=None), fechou, 9999) == {"p1": "a_entregar"}


def _mate_vivo(a, terminal="term_mate"):
    a.set("terminals.json", [terminal, "term_coord"])
    with open(os.path.join(a.home, "cursor.json"), "w") as f:
        json.dump({"mates": {"orq": {"terminal": terminal, "sessao": "sess-mate", "cwd": a.home, "turnos": []}}, "ausente": {"ligada_em": "2026-10-01T12:00:00Z"}}, f)


def test_mate_pedir_digita_no_mate_e_a_subida_volta_como_entrada_do_coordenador():
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    r = a.orq("mate", "pedir", "orq", "--texto", "despache o ticket 77")
    assert r.returncode == 0, r.stderr
    p = json.loads(r.stdout)
    assert p["corr"] == "p1" and p["entrega"] == "enviado", p
    send = [json.loads(x) for x in open(os.path.join(a.fake, "send.log"))]
    texto = next(c[c.index("--text") + 1] for c in send if "--text" in c)
    assert "term_mate" in send[0] and texto.startswith("orq ▸ pedido p1") and "orq mate subir --corr p1" in texto, texto
    assert orq_mod.origem(texto) == "aviso_orq"  # no mate o pedido não vira entrada: quem o cobra é o prazo
    # o mate responde: vira entrada do coordenador (origem mate) e resolve o pedido
    r = a.orq("mate", "subir", "--tipo", "resposta", "--corr", "p1", "--texto", "despachado, ctx_x", ORQ_MATE="orq", ORCA_TERMINAL_HANDLE="term_mate")
    assert r.returncode == 0, r.stderr
    ent = next(e for e in a.events() if e.get("tipo") == "entrada")
    assert ent["origem"] == "mate" and ent["mate"] == "orq" and ent["corr"] == "p1" and "grupo" not in ent, ent
    assert [e["id"] for e in orq_mod.abertas(a.events())] == [ent["id"]]  # aberta para o coordenador
    assert orq_mod.mate_pendentes(a.events(), {}, datetime.now(timezone.utc)) == []
    # corr que não existe é recusado: a resposta não some calada
    r = a.orq("mate", "subir", "--tipo", "resposta", "--corr", "p9", "--texto", "x", ORQ_MATE="orq", ORCA_TERMINAL_HANDLE="term_mate")
    assert r.returncode != 0 and "p9" in r.stderr, r.stderr


def test_mate_responde_a_decisao_e_fecha_a_entrada_que_subiu():
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    sub = json.loads(a.orq("mate", "subir", "--tipo", "decisao", "--texto", "integrar t77 na main?", ORQ_MATE="orq", ORCA_TERMINAL_HANDLE="term_mate").stdout)
    r = a.orq("mate", "pedir", "orq", "--texto", "sim, integre", "--responde", sub["id"], "--prazo", "0")
    assert r.returncode == 0, r.stderr
    intake = [e for e in a.events() if e.get("tipo") == "intake"]
    assert intake and intake[-1]["entrada"] == sub["id"] and intake[-1]["efeito"] == "mate" and intake[-1]["ref"] == "p1", intake
    assert orq_mod.abertas(a.events()) == []
    r = a.orq("intake", sub["id"], "mate", "p8")
    assert r.returncode != 0 and "p8" in r.stderr  # o efeito mate confere o pedido


def test_entrada_digitada_no_mate_nao_aparece_para_o_coordenador():
    a = Amb()
    a.prompt("trabalho do mate", ORQ_MATE="orq")
    a.prompt("trabalho do coordenador")
    ents = [e for e in a.events() if e.get("tipo") == "entrada"]
    assert [e.get("grupo") for e in ents] == ["orq", None], ents
    assert [e["texto"] for e in orq_mod.abertas(a.events())] == ["trabalho do coordenador"]
    assert [e["texto"] for e in orq_mod.abertas(a.events(), grupo="orq")] == ["trabalho do mate"]
    cur = _cursor(a)
    t = cur["mates"]["orq"]["turnos"]
    assert cur["mates"]["orq"]["sessao"] == "abcdef123456" and len(t) == 1 and t[0][0] and t[0][1] is None, cur.get("mates")
    a.orq("hook", "stop", stdin=json.dumps({"session_id": "abcdef123456"}), ORQ_MATE="orq")
    assert _cursor(a)["mates"]["orq"]["turnos"][-1][1], _cursor(a)["mates"]


def test_mate_volta_entrega_reenvia_uma_vez_escala_e_avisa_o_coordenador():
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    with open(os.path.join(a.home, "gerente.json"), "w") as f:
        json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": []}, f)
    a.set("busy.json", ["term_mate"])  # o mate está no meio do turno: o pedido não é digitado
    p = json.loads(a.orq("mate", "pedir", "orq", "--texto", "status do t77", "--prazo", "1").stdout)
    assert p["entrega"] == "ocupado", p
    a.set("busy.json", [])
    with EmProcesso(a):
        assert any("p1 entregue" in x for x in orq_mod.mate_volta())
        cur = _cursor(a)
        cur["mates"]["orq"]["turnos"] = [[_z(-10), _z(-5)]]  # o turno que recebeu o pedido acabou há tempo (T0 fica no passado)
        json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
        evs = [e for e in a.events() if e.get("tipo") != "mate_entregue"] + [{"tipo": "mate_entregue", "corr": "p1", "ts": _z(-20)}]
        with open(os.path.join(a.home, "events.jsonl"), "w") as f:
            f.write("".join(json.dumps(e) + "\n" for e in evs))
        assert any("p1 reenviado" in x for x in orq_mod.mate_volta())
        evs = [e if e.get("tipo") != "mate_reenvio" else {**e, "ts": _z(-15)} for e in a.events()]  # o turno -10..-5 veio depois da repostagem
        with open(os.path.join(a.home, "events.jsonl"), "w") as f:
            f.write("".join(json.dumps(e) + "\n" for e in evs))
        assert any("p1 escalado" in x for x in orq_mod.mate_volta())
        assert not any("p1" in x for x in orq_mod.mate_volta())  # escalado uma vez só
    send = [json.loads(x) for x in open(os.path.join(a.fake, "send.log"))]
    alvos = [c[c.index("--terminal") + 1] for c in send if "--text" in c]
    textos = [c[c.index("--text") + 1] for c in send if "--text" in c]
    assert alvos == ["term_mate", "term_mate", "term_coord"], alvos
    assert "de novo" in textos[1] and "p1" in textos[2] and "não respondeu" in textos[2], textos


def test_mate_volta_avisa_a_subida_e_o_mate_morto_uma_vez():
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    with open(os.path.join(a.home, "gerente.json"), "w") as f:
        json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": []}, f)
    a.orq("mate", "subir", "--tipo", "pr", "--texto", "PR pronto: t77", ORQ_MATE="orq", ORCA_TERMINAL_HANDLE="term_mate")
    with EmProcesso(a):
        assert any("subida" in x for x in orq_mod.mate_volta())
        assert orq_mod.mate_volta() == []  # avisada uma vez
        a.set("terminals.json", ["term_coord"])  # o terminal do mate sumiu
        assert any("caiu" in x for x in orq_mod.mate_volta())
        assert orq_mod.mate_volta() == []
    textos = [c[c.index("--text") + 1] for c in (json.loads(x) for x in open(os.path.join(a.fake, "send.log"))) if "--text" in c]
    assert textos[0].startswith("orq ▸ mate orq subiu") and "PR pronto" in textos[0], textos
    assert "orq mate abrir orq" in textos[1], textos


def test_mate_abrir_sobe_com_orq_mate_no_ambiente_e_retoma_a_sessao():
    a = Amb()
    _grupo(a, regras="/h/regras-orq.md")
    a.set("terminals.json", ["term_coord"])
    a.set("screens.json", {"term_ret1": ["❯ ", "  ⏵⏵ bypass permissions on (shift+tab to cycle)"], "term_ret2": ["esc to interrupt"]})
    r = a.orq("mate", "abrir", "orq")
    assert r.returncode == 0, r.stderr
    cria = [json.loads(x) for x in open(os.path.join(a.fake, "create.log"))]
    cmd = cria[0][cria[0].index("--command") + 1]
    # o Orca só cria terminal em worktree que conhece (o ~/.claude/orq não é uma): abre no checkout atual e entra na pasta do grupo. O `claude '<prompt>'`
    # num terminal do Orca roda não interativo e sai depois do turno (sdk-cli, visto em 01/10): o claude abre sem prompt e o charter é digitado
    assert cmd == f"cd {a.home}; ORQ_MATE=orq claude --model claude-sonnet-5-5 --dangerously-skip-permissions", cmd
    assert "--worktree" not in cria[0], cria[0]
    assert not _env_claude(cria), "env VAR=x claude roda não interativo num terminal do Orca (ticket 106)"
    texto = next(c[c.index("--text") + 1] for c in (json.loads(x) for x in open(os.path.join(a.fake, "send.log"))) if "--text" in c)
    assert "secondmate do grupo orq" in texto and "/h/regras-orq.md" in texto and "orq mate subir" in texto and "\n" not in texto, texto
    assert _cursor(a)["mates"]["orq"]["terminal"] == "term_ret1"
    r = a.orq("mate", "abrir", "orq")
    assert r.returncode != 0 and "aberto" in r.stderr  # um mate por grupo
    cur = _cursor(a)
    cur["mates"]["orq"]["sessao"] = "sess-mate"
    json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
    a.set("terminals.json", ["term_coord"])  # caiu
    a.set("screens.json", {"term_ret2": ["esc to interrupt"]})
    assert a.orq("mate", "abrir", "orq").returncode == 0
    cmd = [json.loads(x) for x in open(os.path.join(a.fake, "create.log"))][1]
    cmd = cmd[cmd.index("--command") + 1]
    assert cmd == f"cd {a.home}; ORQ_MATE=orq claude --resume sess-mate --model claude-sonnet-5-5 --dangerously-skip-permissions", cmd
    textos = [c[c.index("--text") + 1] for c in (json.loads(x) for x in open(os.path.join(a.fake, "send.log"))) if "--text" in c]
    assert len(textos) == 2 and "terminal caiu" in textos[1], textos


def _env_claude(criados):
    """Os comandos de `orca terminal create` que lançam o claude ou o codex atrás de `env VAR=…`."""
    return [c[c.index("--command") + 1] for c in criados if re.search(r"\benv\s+\S+=\S*\s.*\b(?:claude|codex)\b", c[c.index("--command") + 1])]


def test_ticket106_nenhum_comando_do_orq_lanca_o_agente_atras_de_env():
    # num terminal do Orca (fish), `env VAR=x claude …` roda não interativo (sdk-cli; sem prompt, o erro de --print), com ou sem prompt na linha. Conferido em
    # 01/10 (Claude Code 2.1.287): `claude 'p'`, `VAR=x claude 'p'` e `claude --resume <sessão> 'p'` ficam interativos depois do primeiro turno
    assert _env_claude([["--command", "cd /x; env ORQ_MATE=orq claude --model m"]]) and not _env_claude([["--command", "cd /x; ORQ_MATE=orq claude --model m"]])
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "orqlib.py")).read()
    assert not re.search(r'\[\s*"env"\s*,', src), "um comando montado como [\"env\", …] sobe o agente atrás de env"
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    a.set("workers.json", [])
    a.set("terminals.json", ["term_coord"])
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    assert a.orq("retomar").returncode == 0
    cria = _log(a, "create.log")
    assert cria and not _env_claude(cria), cria


def test_mate_nao_abre_pergunta_no_terminal():
    a = Amb()
    _grupo(a)
    ev = {"session_id": "abcdef123456", "tool_name": "AskUserQuestion", "tool_input": {"questions": []}}
    r = a.orq("hook", "guard", stdin=json.dumps(ev), ORQ_MATE="orq")
    out = json.loads(r.stdout)
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny" and "orq mate subir --tipo decisao" in out["hookSpecificOutput"]["permissionDecisionReason"], out


def test_retomar_sobe_o_mate_que_caiu_pela_sessao():
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    a.set("workers.json", [])
    a.set("terminals.json", ["term_coord"])  # a queda levou o terminal do mate
    a.set("screens.json", {"term_ret1": ["esc to interrupt"]})
    r = a.orq("retomar", "--dry-run")
    assert "mate orq: a_retomar" in r.stdout and not os.path.exists(os.path.join(a.fake, "create.log")), r.stdout + r.stderr
    r = a.orq("retomar")
    assert "mate orq: retomado -> term_ret1" in r.stdout, r.stdout + r.stderr
    cria = json.loads(open(os.path.join(a.fake, "create.log")).readline())
    assert "--resume sess-mate" in cria[cria.index("--command") + 1]


def test_despacho_enfileirado_pelo_mate_drena_com_o_handle_do_mate():
    a = Amb()
    os.makedirs(a.home, exist_ok=True)
    with open(os.path.join(a.home, "gerente.json"), "w") as f:
        json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": []}, f)
    with EmProcesso(a):
        os.environ.update(ORQ_MATE="orq", ORCA_TERMINAL_HANDLE="term_mate")
        r = orq_mod._enfileirar_despacho("sem vaga", "run_mate", "orq: t", "# orq: t\n", "claude-sonnet-5-5", "medium", None, None, None, None, None, 2, "claude")
        it = next(i for i in orq_mod.fila_despacho_itens() if i["id"] == r["fila"])
        assert it["coord"] == "term_mate" and it["mate"] == "orq", it
        del os.environ["ORQ_MATE"]
        os.environ["ORCA_TERMINAL_HANDLE"] = "term_ger"
        visto, antes = {}, orq_mod.despachar
        orq_mod.despachar = lambda *x, **k: visto.update(h=os.environ.get("ORCA_TERMINAL_HANDLE"), m=os.environ.get("ORQ_MATE")) or {"dispatchId": "ctx_1"}
        try:
            orq_mod._sobe_da_fila(it)
        finally:
            orq_mod.despachar = antes
        assert visto == {"h": "term_mate", "m": "orq"}, visto
        assert os.environ["ORCA_TERMINAL_HANDLE"] == "term_ger" and "ORQ_MATE" not in os.environ


def test_subida_do_mate_com_o_usuario_no_coordenador_espera_o_proximo_prompt():
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    with open(os.path.join(a.home, "gerente.json"), "w") as f:
        json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": []}, f)
    a.prompt("algo no mate", ORQ_MATE="orq")  # quem digita no mate não torna o coordenador ativo
    a.orq("mate", "subir", "--tipo", "decisao", "--texto", "integrar?", ORQ_MATE="orq", ORCA_TERMINAL_HANDLE="term_mate")
    with EmProcesso(a):
        assert not orq_mod.coordenador_ativo()
        a.prompt("o usuário fala com o coordenador")
        assert any("avisada" in x for x in orq_mod.mate_volta())
        assert orq_mod.mate_volta() == []
    assert not os.path.exists(os.path.join(a.fake, "send.log")), "digitou por cima do usuário"
    assert "subiu" in _cursor(a)["avisos"][0]["texto"], _cursor(a).get("avisos")


def test_mate_abrir_fecha_o_terminal_quando_a_sessao_nao_volta():
    a = Amb(ORQ_RETOMAR_ESPERA_S="0.5", ORQ_MATE_ESPERA_S="0.5")
    _grupo(a)
    _mate_vivo(a)
    a.set("terminals.json", ["term_coord"])
    a.set("screens.json", {"term_ret1": ["No conversation found with session ID: sess-mate"]})
    r = a.orq("mate", "abrir", "orq")
    assert r.returncode != 0 and "não voltou" in r.stderr, r.stderr
    assert "term_ret1" in open(os.path.join(a.fake, "close.log")).read()  # o shell que sobrou não recebe pedido
    m = _cursor(a)["mates"]["orq"]
    assert m.get("terminal") is None and m.get("sessao") is None, m
    a.set("screens.json", {"term_ret2": ["⏵⏵ bypass permissions on"]})
    assert a.orq("mate", "abrir", "orq").returncode == 0
    cmd = [json.loads(x) for x in open(os.path.join(a.fake, "create.log"))][1]
    assert "--resume" not in cmd[cmd.index("--command") + 1]  # o segundo abre com o charter
    a.set("terminals.json", ["term_coord"])
    r = a.orq("mate", "abrir", "orq")  # o claude não chega ao prompt (term_ret3 sem a caixa na tela): fecha em vez de deixar um shell
    assert r.returncode != 0 and "não chegou ao prompt" in r.stderr, r.stderr
    assert "term_ret3" in open(os.path.join(a.fake, "close.log")).read()


def test_mate_pedir_responde_invalido_nao_grava_e_texto_vai_numa_linha():
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    a.prompt("entrada do usuário")
    r = a.orq("mate", "pedir", "orq", "--texto", "x", "--responde", "e1")
    assert r.returncode != 0 and "nada foi gravado" in r.stderr, r.stderr
    assert not any(e.get("tipo") == "mate_pedido" for e in a.events())
    p = json.loads(a.orq("mate", "pedir", "orq", "--texto", "linha um\nlinha dois\n\nfim").stdout)
    texto = next(c[c.index("--text") + 1] for c in (json.loads(x) for x in open(os.path.join(a.fake, "send.log"))) if "--text" in c)
    assert "\n" not in texto and "linha um linha dois fim" in texto, texto
    assert next(e for e in a.events() if e.get("tipo") == "mate_pedido")["texto"] == "linha um\nlinha dois\n\nfim"
    ent = next(e for e in a.events() if e.get("tipo") == "mate_entregue")
    assert ent["ts"] <= orq_mod.now(), ent  # o ts é o de antes do digita
    # o mate de outro grupo não responde este pedido
    _grupo(a, "outro")
    r = a.orq("mate", "subir", "--tipo", "resposta", "--corr", p["corr"], "--texto", "x", ORQ_MATE="outro", ORCA_TERMINAL_HANDLE="term_x")
    assert r.returncode != 0 and "outro" in r.stderr, r.stderr


def test_mate_volta_nao_reavisa_subida_velha_com_mais_de_cinquenta():
    a = Amb()
    _grupo(a)
    _mate_vivo(a)
    with open(os.path.join(a.home, "gerente.json"), "w") as f:
        json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": []}, f)
    with open(os.path.join(a.home, "events.jsonl"), "w") as f:
        f.write("".join(json.dumps({"tipo": "entrada", "id": f"e{i}", "origem": "mate", "mate": "orq", "tipo_mate": "resumo", "texto": f"r{i}", "ts": _z(i)}) + "\n"
                        for i in range(1, 56)))
    with EmProcesso(a):
        for _ in range(55):
            orq_mod.mate_volta()
        assert orq_mod.mate_volta() == []
    textos = [c[c.index("--text") + 1] for c in (json.loads(x) for x in open(os.path.join(a.fake, "send.log"))) if "--text" in c]
    assert len(textos) == 55 and len(set(textos)) == 55, len(textos)


def test_prompt_no_mate_nao_esvazia_os_avisos_do_coordenador_e_o_stop_nao_vai_ao_digest():
    a = Amb()
    os.makedirs(a.home, exist_ok=True)
    with open(os.path.join(a.home, "cursor.json"), "w") as f:
        json.dump({"avisos": [{"texto": "orq: PR #1 entrou", "ts": _z(0), "contexto": True}]}, f)
    a.prompt("oi, mate", ORQ_MATE="orq")
    assert _cursor(a).get("avisos"), "o mate levou o aviso do coordenador"
    a.orq("ausente", "ligar")
    a.orq("hook", "stop", stdin=json.dumps({"session_id": "abcdef123456", "last_assistant_message": "feito"}), ORQ_MATE="orq")
    assert not any(e.get("tipo") == "resposta_coordenador" for e in a.events())


def test_fila_do_mate_drena_com_o_terminal_atual_do_mate():
    a = Amb()
    _grupo(a)
    _mate_vivo(a, terminal="term_mate_novo")  # o mate caiu e voltou depois de enfileirar
    with EmProcesso(a):
        visto, antes = {}, orq_mod.despachar
        orq_mod.despachar = lambda *x, **k: visto.update(h=os.environ.get("ORCA_TERMINAL_HANDLE")) or {"dispatchId": "ctx_1"}
        try:
            orq_mod._sobe_da_fila({"tipo": "despacho", "run": "run_m", "titulo": "orq: t", "modelo": "m", "effort": "low", "prioridade": 2, "mate": "orq", "coord": "term_mate_velho"})
        finally:
            orq_mod.despachar = antes
    assert visto["h"] == "term_mate_novo", visto


# ---- ticket 94: arquivos de projeto (ORQ_HOME/projects/<nome>.json) ----

def _projeto(a, nome, dado):
    os.makedirs(os.path.join(a.home, "projects"), exist_ok=True)
    with open(os.path.join(a.home, "projects", f"{nome}.json"), "w") as f:
        f.write(dado if isinstance(dado, str) else json.dumps(dado))


def test_ticket94_projetos_lista_cada_arquivo_com_repo_harness_e_grupo():
    a = Amb(run="run_a")
    _projeto(a, "meu-app", {"repo": "path:/r/meu-app", "harness": "codex", "grupo": "confi"})
    _projeto(a, "neo-api", {"repo": "name:neo-api"})
    r = a.orq("projetos")
    assert r.returncode == 0, r.stderr
    linhas = r.stdout.splitlines()
    assert any(l.startswith("meu-app") and "path:/r/meu-app" in l and "codex" in l and "confi" in l for l in linhas), r.stdout
    assert any(l.startswith("neo-api") and "name:neo-api" in l and "claude" in l for l in linhas), "sem harness no arquivo vale claude: " + r.stdout
    js = json.loads(a.orq("projetos", "--json").stdout)
    assert {p["nome"] for p in js} == {"meu-app", "neo-api"} and next(p for p in js if p["nome"] == "neo-api")["harness"] == "claude", js


def test_ticket94_projetos_sem_pasta_diz_que_nao_ha_projeto_e_sai_0():
    a = Amb(run="run_a")
    r = a.orq("projetos")
    assert r.returncode == 0 and "nenhum projeto" in r.stdout, r
    assert json.loads(a.orq("projetos", "--json").stdout) == []


def test_ticket94_projetos_mostra_o_arquivo_invalido_com_o_motivo_sem_derrubar_a_lista():
    a = Amb(run="run_a")
    _projeto(a, "bom", {"repo": "path:/r/bom"})
    _projeto(a, "quebrado", "{nao e json")
    _projeto(a, "sem-repo", {"harness": "claude"})
    _projeto(a, "harness-ruim", {"repo": "path:/r/x", "harness": "gemini"})
    r = a.orq("projetos")
    assert r.returncode == 0, r.stderr
    for nome, motivo in (("quebrado", "json"), ("sem-repo", "repo"), ("harness-ruim", "gemini")):
        assert any(l.startswith(nome) and "inválido" in l and motivo in l for l in r.stdout.splitlines()), (nome, r.stdout)
    assert any(l.startswith("bom") and "inválido" not in l for l in r.stdout.splitlines()), r.stdout


def test_ticket94_despachar_sem_agente_usa_o_harness_do_projeto():
    a = Amb(run="run_a")
    _projeto(a, "p", {"repo": "path:/r/p", "harness": "codex"})
    r = _despachar(a, "--projeto", "p", "--modelo", "gpt-6-sol")
    assert r.returncode == 0, r.stderr
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--agent") + 1] == "codex", arg
    (ev,) = [e for e in a.events() if e["tipo"] == "despacho"]
    assert ev["agente"] == "codex", ev


def test_ticket94_despachar_com_agente_ganha_do_harness_do_projeto():
    a = Amb(run="run_a")
    _projeto(a, "p", {"repo": "path:/r/p", "harness": "codex"})
    assert _despachar(a, "--projeto", "p", "--agente", "claude").returncode == 0
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--agent") + 1] == "claude", arg


def test_ticket94_despachar_projeto_sem_harness_ou_sem_projeto_segue_claude():
    a = Amb(run="run_a")
    _projeto(a, "p", {"repo": "path:/r/p"})
    assert _despachar(a, "--projeto", "p").returncode == 0
    assert _despachar(a).returncode == 0  # nenhum projeto contém o cwd dos testes
    assert [x[x.index("--agent") + 1] for x in _log(a, "started.log")] == ["claude", "claude"]


def test_ticket94_despachar_projeto_inexistente_ou_invalido_recusa_sem_criar_task():
    a = Amb(run="run_a")
    _projeto(a, "ruim", {"harness": "codex"})
    for nome in ("nao-existe", "ruim"):
        r = _despachar(a, "--projeto", nome)
        assert r.returncode != 0 and nome in r.stderr, (nome, r.stderr)
    assert not _log(a, "started.log"), "nada subiu"


def test_ticket94_projeto_por_pasta_pega_o_repo_que_contem_o_cwd_e_o_mais_especifico_ganha():
    ps = {"geral": {"repo": "path:/r/mono"}, "web": {"repo": "path:/r/mono/web", "harness": "codex"}, "outro": {"repo": "name:x"}}
    assert orq_mod.projeto_por_pasta(ps, "/r/mono/web/app") == "web"
    assert orq_mod.projeto_por_pasta(ps, "/r/mono/api") == "geral"
    assert orq_mod.projeto_por_pasta(ps, "/r/monolito") is None, "prefixo de texto não é pasta contida"
    assert orq_mod.projeto_por_pasta(ps, "/fora") is None


# ---- ticket 95: o Run guarda o projeto e o despacho usa --repo ----

def _repo95(a, nome="alvo"):
    """Um repositório git de verdade fora do cwd dos testes: o alvo do projeto."""
    p = os.path.join(a.tmp.name, nome)
    os.makedirs(p)
    subprocess.run(["git", "-C", p, "init", "-q"], check=True)
    return os.path.realpath(p)


def _confiadas95(cfg):
    import tomllib
    return set(tomllib.loads(open(cfg).read()).get("projects", {})) if os.path.exists(cfg) else set()


def test_ticket95_run_projeto_grava_o_evento_e_recusa_projeto_que_nao_existe_ou_esta_invalido():
    a = Amb(run="run_a")
    _projeto(a, "p", {"repo": "path:/r/p"})
    _projeto(a, "ruim", {"harness": "codex"})
    r = a.orq("run", "projeto", "p", "--run", "run_a")
    assert r.returncode == 0, r.stderr
    (ev,) = [e for e in a.events() if e["tipo"] == "run_projeto"]
    assert (ev["run"], ev["projeto"]) == ("run_a", "p"), ev
    for nome in ("nao-existe", "ruim"):
        r = a.orq("run", "projeto", nome, "--run", "run_a")
        assert r.returncode != 0 and nome in r.stderr, (nome, r.stderr)
    assert len([e for e in a.events() if e["tipo"] == "run_projeto"]) == 1, "o recusado não grava"


def test_ticket95_despachar_no_run_com_projeto_passa_repo_e_worktree_nova_e_grava_o_projeto_no_evento():
    a = Amb(run="run_a")
    _projeto(a, "p", {"repo": "path:/r/p", "harness": "codex"})
    assert a.orq("run", "projeto", "p", "--run", "run_a").returncode == 0
    r = _despachar(a)
    assert r.returncode == 0, r.stderr
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--repo") + 1] == "path:/r/p" and arg[arg.index("--worktree") + 1] == "new-top-level", arg
    assert arg[arg.index("--agent") + 1] == "codex", "o harness também vem do projeto do Run"
    (ev,) = [e for e in a.events() if e["tipo"] == "despacho"]
    assert ev["projeto"] == "p" and ev["worktree"] == "new-top-level", ev


def test_ticket95_projeto_explicito_ganha_do_projeto_do_run_e_o_ultimo_run_projeto_vale():
    a = Amb(run="run_a")
    for nome in ("um", "dois", "tres"):
        _projeto(a, nome, {"repo": f"path:/r/{nome}"})
    assert a.orq("run", "projeto", "um", "--run", "run_a").returncode == 0
    assert a.orq("run", "projeto", "dois", "--run", "run_a").returncode == 0
    assert _despachar(a).returncode == 0
    assert _despachar(a, "--projeto", "tres").returncode == 0
    assert [c[c.index("--repo") + 1] for c in _log(a, "started.log")] == ["path:/r/dois", "path:/r/tres"]


def test_ticket95_projeto_de_outro_run_e_run_sem_projeto_despacham_como_hoje():
    a = Amb(run="run_a")
    _projeto(a, "p", {"repo": "path:/r/p"})
    assert a.orq("run", "projeto", "p", "--run", "run_b").returncode == 0
    assert _despachar(a).returncode == 0
    (arg,) = _log(a, "started.log")
    assert "--repo" not in arg and "--worktree" not in arg, arg
    (ev,) = [e for e in a.events() if e["tipo"] == "despacho"]
    assert "projeto" not in ev, ev


def test_ticket95_run_com_projeto_recusa_worktree_current_e_arquivo_que_sumiu_sem_subir_worker():
    a = Amb(run="run_a")
    _projeto(a, "p", {"repo": "path:/r/p"})
    assert a.orq("run", "projeto", "p", "--run", "run_a").returncode == 0
    r = _despachar(a, "--worktree", "current")
    assert r.returncode != 0 and "new-top-level" in r.stderr, r.stderr
    os.remove(os.path.join(a.home, "projects", "p.json"))
    r = _despachar(a)
    assert r.returncode != 0 and "run_a" in r.stderr, "o Run aponta para um projeto que não existe mais: não cai no cwd"
    assert not _log(a, "started.log"), "nada subiu"


def test_ticket95_codex_confia_a_raiz_do_repo_do_projeto_e_nao_a_do_cwd():
    a = Amb(run="run_a")
    alvo = _repo95(a)
    _projeto(a, "p", {"repo": "path:" + os.path.join(alvo, "sub", ".."), "harness": "codex"})
    cfg = os.path.join(a.tmp.name, "config.toml")
    assert a.orq("run", "projeto", "p", "--run", "run_a").returncode == 0
    r = _despachar(a, ORQ_CODEX_CONFIG=cfg)
    assert r.returncode == 0, r.stderr
    confiadas = _confiadas95(cfg)
    assert alvo in confiadas, confiadas
    raiz_cwd = os.path.realpath(os.path.dirname(subprocess.run(["git", "-C", AQUI, "rev-parse", "--path-format=absolute", "--git-common-dir"],
                                                                capture_output=True, text=True).stdout.strip()))
    assert raiz_cwd not in confiadas, "a pasta do cwd não é o repositório do worker"


def test_ticket95_seletor_id_ou_name_acha_a_pasta_pelo_orca_repo_list_para_confiar_no_codex():
    a = Amb(run="run_a")
    alvo = _repo95(a)
    a.set("repos.json", [{"id": "rid1", "path": alvo, "displayName": "alvo"}, {"id": "rid2", "path": "/outro", "displayName": "outro"}])
    cfg = os.path.join(a.tmp.name, "config.toml")
    for sel in ("id:rid1", "name:alvo"):
        _projeto(a, "p", {"repo": sel, "harness": "codex"})
        assert a.orq("run", "projeto", "p", "--run", "run_a").returncode == 0
        if os.path.exists(cfg):
            os.remove(cfg)
        assert _despachar(a, ORQ_CODEX_CONFIG=cfg).returncode == 0
        assert alvo in _confiadas95(cfg), (sel, _confiadas95(cfg))
    assert [c[c.index("--repo") + 1] for c in _log(a, "started.log")] == ["id:rid1", "name:alvo"]
    _projeto(a, "p", {"repo": "id:inexistente", "harness": "codex"})
    assert _despachar(a, ORQ_CODEX_CONFIG=cfg).returncode == 0, "seletor sem pasta não derruba o despacho"


def test_ticket95_worker_claude_com_projeto_nao_mexe_no_config_do_codex():
    a = Amb(run="run_a")
    _projeto(a, "p", {"repo": "path:" + _repo95(a)})
    cfg = os.path.join(a.tmp.name, "config.toml")
    assert a.orq("run", "projeto", "p", "--run", "run_a").returncode == 0
    assert _despachar(a, ORQ_CODEX_CONFIG=cfg).returncode == 0 and not os.path.exists(cfg)


def test_ticket95_pedido_enfileirado_leva_o_projeto_e_o_worktree_para_o_gerente_subir_no_repo_certo():
    a = _painel79()
    _gerente(a)
    _projeto(a, "p", {"repo": "path:/r/p"})
    _frota79(a, vivos=[(f"Vivo {n}", SONNET) for n in range(4)])
    out = json.loads(a.orq("despachar", "--run", "run_a", "--titulo", "Ticket 05", "--spec-arquivo", _spec(a), "--modelo", SONNET, "--effort", "medium",
                           "--projeto", "p").stdout)
    assert out["estado"] == "enfileirado", out
    (it,) = _fila79(a)
    assert (it["projeto"], it["worktree"]) == ("p", "new-top-level"), it
    _libera79(a, "term_v0")
    r = a.orq("gerente", "absorver")  # o gerente roda sem o --projeto e fora do repo: o projeto vem do item
    assert r.returncode == 0 and "Ticket 05 subiu" in r.stdout, r.stdout + r.stderr
    (arg,) = _log(a, "started.log")
    assert arg[arg.index("--repo") + 1] == "path:/r/p" and arg[arg.index("--worktree") + 1] == "new-top-level", arg


# ---------- orq perguntar: decisão pelo Lavish nos dois harnesses (ticket 75) ----------

LAVISH_FALSO = """#!/usr/bin/env python3
import os, sys
if sys.argv[1] == "poll":
    sys.stdout.write(open(os.environ["FAKE_POLL"]).read() if os.environ.get("FAKE_POLL") else "session:\\n  status: ended\\n")
    sys.exit(0)
print('session:\\n  url: "http://127.0.0.1:4387/session/abc"\\n  status: opened')
"""


def _perguntar(a, poll=None, *args, **env):
    bin_ = os.path.join(a.tmp.name, "lavish")
    with open(bin_, "w") as f:
        f.write(LAVISH_FALSO)
    os.chmod(bin_, 0o755)
    if poll is not None:
        env["FAKE_POLL"] = _lote(a, poll, "poll-perg.json")
    return a.orq("perguntar", "--id", "badge", "--pergunta", "Qual badge?", "--opcao", "A: ponto", "--opcao", "B: texto", *args, ORQ_LAVISH=bin_, **env)


def _todas_chamadas(a):
    return [json.loads(x) for x in open(os.path.join(a.fake, "calls.log"))]


def test_perguntar_monta_a_pagina_com_as_opcoes_e_a_recomendada_e_abre_no_orca():
    a = Amb()
    r = _perguntar(a, None, "--recomendada", "2", "--sem-poll")
    assert r.returncode == 0, r.stderr
    saida = json.loads(r.stdout)
    pagina = open(saida["pagina"]).read()
    assert 'value="A: ponto"' in pagina and 'value="B: texto"' in pagina and "Qual badge?" in pagina
    assert pagina.count('class="rec"') == 1 and pagina.index("B: texto") < pagina.index('class="rec"'), "só a recomendada leva a marca"
    assert " checked" not in pagina, "nenhuma opção pré-selecionada: Enter solto não decide pelo usuário"
    assert saida["url"] == "http://127.0.0.1:4387/session/abc" and saida["efeito"] == "aberta"
    assert "badge" in _ids_pend(a), "a decisão nasce como pendência"
    assert any("http://127.0.0.1:4387/session/abc" in " ".join(c) for c in _todas_chamadas(a)), "a aba abre no browser do Orca"


def test_perguntar_com_escolha_fecha_a_pendencia_e_grava_o_evento():
    a = Amb()
    r = _perguntar(a, [{"id": "perg-badge", "header": "badge", "resposta": "A: ponto", "disposicao": "escolha"}])
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["efeito"] == "fechou"
    assert "badge" not in _ids_pend(a)
    (ev,) = [e for e in a.events() if e["tipo"] == "resposta_lavish"]
    assert ev["header"] == "badge" and ev["resposta"] == "A: ponto" and ev["fechou"] == ["badge"]


def test_perguntar_com_resposta_vazia_ou_sessao_encerrada_deixa_a_pendencia_aberta_com_aviso():
    a = Amb()
    r = _perguntar(a, [{"id": "perg-badge", "header": "badge", "resposta": "", "disposicao": "escolha"}])
    assert "badge" in _ids_pend(a) and json.loads(r.stdout)["efeito"] == "aberta"
    assert "orq pend done badge" in r.stderr, r.stderr
    b = Amb()
    r = _perguntar(b)  # o poll volta com a sessão encerrada, sem lote
    assert r.returncode == 0 and "badge" in _ids_pend(b) and json.loads(r.stdout)["efeito"] == "aberta"
    assert "terminou sem resposta" in r.stderr, r.stderr
    c = Amb()
    r = _perguntar(c, None, "--espera-min", "0.0005", FAKE_POLL="/nao/existe")  # poll quebrado não fecha nada
    assert "badge" in _ids_pend(c)


def test_perguntar_recusa_menos_de_duas_opcoes_e_pendencia_que_nao_e_decisao():
    a = Amb()
    r = a.orq("perguntar", "--id", "x", "--pergunta", "p", "--opcao", "só uma")
    assert r.returncode != 0 and "duas" in r.stderr
    r = a.orq("perguntar", "--id", "avisar-x", "--pergunta", "p", "--opcao", "a", "--opcao", "b", "--sem-poll")
    assert r.returncode != 0 and "não decisão" in r.stderr


def _hooks_codex_fixture(t, confiar):
    """hooks.json com um hook alheio (grupo 0) e dois do orq (grupos 1 e 2); `confiar` são as chaves `evento:grupo:hook` que o config.toml confia."""
    hj = os.path.join(t, "hooks.json")
    json.dump({"hooks": {"Stop": [{"hooks": [{"type": "command", "command": "graphify hook-check"}]},
                                  {"hooks": [{"type": "command", "command": "python3 ~/.claude/orq/orq.py hook stop codex"}]}],
                         "SessionStart": [{"hooks": [{"type": "command", "command": "python3 ~/.claude/orq/precompact.py retomar"}]}]}}, open(hj, "w"))
    cfg = os.path.join(t, "codex-config.toml")
    open(cfg, "w").write("[hooks.state]\n" + "".join(f'[hooks.state."{hj}:{k}"]\ntrusted_hash = "sha256:ab"\n' for k in confiar))
    return hj, cfg


def test_ticket77_orq_status_avisa_dos_hooks_do_orq_nao_confiados_no_codex_e_cala_quando_confiados():
    a = Amb(run=None)
    hj, cfg = _hooks_codex_fixture(a.tmp.name, ["stop:1:0"])  # falta o session_start:0:0
    r = a.orq("status")
    assert "hooks do orq não confiados no Codex: rode /hooks" in r.stdout and "(1 hook;" in r.stdout, r.stdout
    assert "hooks do orq não confiados" in a.orq("agentes").stdout
    _hooks_codex_fixture(a.tmp.name, ["stop:1:0", "session_start:0:0"])
    assert "não confiados no Codex" not in a.orq("status").stdout
    assert "não confiados no Codex" not in a.orq("agentes").stdout
    open(cfg, "a").write(f'[hooks.state."{hj}:stop:1:0"]\nenabled = false\n')  # chave repetida: o TOML não lê, o trust some
    assert "não confiados no Codex" in a.orq("status").stdout, "config que não lê vale não confiado"


def test_ticket77_sem_hook_do_orq_no_hooks_json_nao_ha_aviso():
    a = Amb(run=None)
    assert "não confiados no Codex" not in a.orq("status").stdout, "sem hooks.json o Codex não está ligado ao orq"


def test_ticket77_o_preambulo_do_coordenador_traz_o_aviso_na_primeira_linha():
    a = Amb(run="run_a")
    _hooks_codex_fixture(a.tmp.name, [])
    assert _ctx_session(a).splitlines()[0].startswith("⚠ hooks do orq não confiados no Codex: rode /hooks")


def test_ticket77_o_instalador_acrescenta_no_fim_e_nunca_reordena_os_grupos_existentes():
    exemplo = {"hooks": {"Stop": [{"hooks": [{"type": "command", "command": "python3 ~/.claude/orq/orq.py hook stop codex"}]}],
                         "SessionStart": [{"hooks": [{"type": "command", "command": "novo-do-orq"}]}]}}
    atual = {"hooks": {"SessionStart": [{"hooks": [{"command": "a"}]}, {"hooks": [{"command": "b"}]}],
                       "Stop": [{"hooks": [{"command": "graphify hook-check"}]}]}}
    novo, add = orq_mod.mesclar_hooks_codex(atual, exemplo)
    assert novo["hooks"]["SessionStart"][:2] == atual["hooks"]["SessionStart"], "os grupos existentes ficam nas mesmas posições"
    assert novo["hooks"]["SessionStart"][2]["hooks"][0]["command"] == "novo-do-orq" and add == [("Stop", 1), ("SessionStart", 2)]
    assert novo["hooks"]["Stop"][0] == atual["hooks"]["Stop"][0]
    again, add2 = orq_mod.mesclar_hooks_codex(novo, exemplo)
    assert add2 == [] and again == novo, "rodar de novo não duplica"
    vazio, add3 = orq_mod.mesclar_hooks_codex({}, exemplo)
    assert len(add3) == 2 and vazio["hooks"]["Stop"] == exemplo["hooks"]["Stop"]
    assert atual["hooks"]["Stop"] == [{"hooks": [{"command": "graphify hook-check"}]}], "a entrada não é mutada"


# ---------- ticket 97: `orq iniciar` liga o coordenador que já está aberto, em qualquer harness ----------

def _hooks_do_harness(a, agente, confiar=True):
    """Instala no Amb os hooks do orq do harness (o exemplo do repositório); no Codex, com o trust de todos em [hooks.state] quando `confiar`."""
    exemplo = os.path.join(os.path.dirname(ORQ), "settings.hooks.example.json" if agente == "claude" else "codex.hooks.example.json")
    if agente == "claude":
        shutil.copy(exemplo, a.env["ORQ_CLAUDE_SETTINGS"])
        return
    shutil.copy(exemplo, a.env["ORQ_CODEX_HOOKS"])
    hooks = json.load(open(exemplo))["hooks"]
    chaves = [f"{a.env['ORQ_CODEX_HOOKS']}:{re.sub(r'(?<!^)(?=[A-Z])', '_', ev).lower()}:{g}:{h}" for ev, gs in hooks.items() for g, grupo in enumerate(gs) for h, _ in enumerate(grupo["hooks"])]
    open(a.env["ORQ_CODEX_CONFIG"], "w").write("[hooks.state]\n" + "".join(f'[hooks.state."{k}"]\ntrusted_hash = "sha256:ab"\n' for k in chaves if confiar))


def _amb97(agente="claude", **env):
    a = Amb(run=None, ORQ_CLAUDE_SETTINGS=os.path.join(tempfile.mkdtemp(), "settings.json"), **env)
    a.set("terminals.json", ["term_coord"])
    _hooks_do_harness(a, agente)
    return a


def test_ticket97_it_should_check_the_hooks_bind_a_new_run_raise_the_manager_and_print_the_status_in_claude():
    a = _amb97("claude")
    r = a.orq("iniciar", "--agente", "claude", "--objetivo", "Frente X")
    assert r.returncode == 0, r.stderr
    chamadas = _log(a, "calls.log")
    (rc,) = [c for c in chamadas if c[0] == "run-create"]
    assert rc[rc.index("--objective") + 1] == "Frente X"
    (c,) = _log(a, "create.log")  # só o terminal do gerente: nenhum outro coordenador
    assert c[c.index("--command") + 1] == f"sh {os.path.join(a.home, 'painel-agent-manager.sh')}", c
    assert not [x for x in chamadas if x[0] == "worker-start"]
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ret1", "runs": ["run_novo"]}
    assert "harness: claude" in r.stdout and "run_novo" in r.stdout and "term_ret1" in r.stdout, r.stdout
    assert "hooks: ok" in r.stdout and "Run" in r.stdout.split("hooks: ok", 1)[1], "o status do orq vem depois da conferência"


def test_ticket97_it_should_do_the_same_in_codex_with_the_trusted_hooks():
    a = _amb97("codex")
    r = a.orq("iniciar", "--agente", "codex", "--objetivo", "Frente Y")
    assert r.returncode == 0, r.stderr
    assert "harness: codex" in r.stdout and "hooks: ok" in r.stdout and "não confiados" not in r.stdout, r.stdout
    assert json.load(open(os.path.join(a.home, "gerente.json")))["runs"] == ["run_novo"]
    assert len(_log(a, "create.log")) == 1


def test_ticket97_it_should_warn_but_go_on_when_codex_hooks_are_installed_and_not_trusted():
    a = _amb97("codex")
    _hooks_do_harness(a, "codex", confiar=False)
    r = a.orq("iniciar", "--agente", "codex", "--objetivo", "Frente Y")
    assert r.returncode == 0 and "hooks do orq não confiados no Codex: rode /hooks" in r.stdout, r
    assert os.path.exists(os.path.join(a.home, "gerente.json"))


def test_ticket97_it_should_refuse_before_touching_the_orca_when_a_hook_is_missing():
    a = _amb97("claude")
    cfg = json.load(open(a.env["ORQ_CLAUDE_SETTINGS"]))
    cfg["hooks"]["Stop"] = []
    json.dump(cfg, open(a.env["ORQ_CLAUDE_SETTINGS"], "w"))
    r = a.orq("iniciar", "--agente", "claude", "--objetivo", "Frente X")
    assert r.returncode == 1 and "Stop: orq.py hook stop" in r.stderr, r
    assert not [c for c in _log(a, "calls.log") if c[0] in ("run-create", "run-use")] and not _log(a, "create.log")
    sem = _amb97("claude")
    os.remove(sem.env["ORQ_CLAUDE_SETTINGS"])
    assert sem.orq("iniciar", "--agente", "claude", "--objetivo", "x").returncode == 1, "sem settings.json não há hook nenhum"


def test_ticket97_it_should_need_an_objective_when_there_is_no_run_and_reuse_the_bound_one_otherwise():
    a = _amb97("claude")
    r = a.orq("iniciar", "--agente", "claude")
    assert r.returncode == 1 and "--objetivo" in r.stderr and not _log(a, "create.log"), r
    a.set("run.json", {"id": "run_a", "handle": "term_coord"})  # o coordenador já comanda um Run
    r = a.orq("iniciar", "--agente", "claude")
    assert r.returncode == 0, r.stderr
    assert not [c for c in _log(a, "calls.log") if c[0] == "run-create"]
    assert json.load(open(os.path.join(a.home, "gerente.json")))["runs"] == ["run_a"]


def test_ticket97_it_should_bind_an_existing_run_with_run_and_stay_idempotent_on_the_second_call():
    a = _amb97("claude")
    assert a.orq("iniciar", "--agente", "claude", "--run", "run_b").returncode == 0
    assert not [c for c in _log(a, "calls.log") if c[0] == "run-create"]
    assert a.orq("iniciar", "--agente", "claude", "--run", "run_b").returncode == 0
    assert len(_log(a, "create.log")) == 1, "o gerente vivo é reaproveitado, não sobe outro"
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ret1", "runs": ["run_b"]}


def test_ticket97_it_should_keep_the_manager_runs_and_raise_a_new_terminal_when_the_old_one_is_gone():
    a = _amb97("claude")
    json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": ["run_a"]}, open((os.makedirs(a.home, exist_ok=True), os.path.join(a.home, "gerente.json"))[1], "w"))
    assert a.orq("iniciar", "--agente", "claude", "--run", "run_b").returncode == 0
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ret1", "runs": ["run_a", "run_b"]}


def test_ticket97_it_should_refuse_another_live_coordinators_manager_without_assumir_and_take_it_with_assumir():
    a = _amb97("claude")
    a.set("terminals.json", ["term_coord", "term_outro", "term_ger"])
    json.dump({"coordenador": "term_outro", "gerente": "term_ger", "runs": ["run_a"]}, open((os.makedirs(a.home, exist_ok=True), os.path.join(a.home, "gerente.json"))[1], "w"))
    r = a.orq("iniciar", "--agente", "claude", "--run", "run_b")
    assert r.returncode == 1 and "--assumir" in r.stderr and "term_outro" in r.stderr and not _log(a, "create.log"), r
    assert a.orq("iniciar", "--agente", "claude", "--run", "run_b", "--assumir").returncode == 0
    assert json.load(open(os.path.join(a.home, "gerente.json"))) == {"coordenador": "term_coord", "gerente": "term_ger", "runs": ["run_a", "run_b"]}
    assert not _log(a, "create.log"), "o gerente vivo de quem foi assumido continua"


def test_ticket97_it_should_find_the_harness_from_the_ancestor_processes_and_ask_for_agente_when_there_is_none():
    a = _amb97("codex")
    procs = os.path.join(a.tmp.name, "ps.json")
    json.dump([{"pid": os.getpid(), "ppid": 2000, "rss": 1, "cpu": 0, "args": "/bin/zsh -c orq", "cwd": None},
               {"pid": 2000, "ppid": 1999, "rss": 1, "cpu": 0, "args": "/opt/homebrew/bin/codex --foo", "cwd": None},
               {"pid": 1999, "ppid": 1, "rss": 1, "cpu": 0, "args": "-fish", "cwd": None}], open(procs, "w"))
    r = a.orq("iniciar", "--objetivo", "Frente Z", ORQ_PROCESSOS=procs)
    assert r.returncode == 0 and "harness: codex" in r.stdout, r
    sem = _amb97("claude")
    json.dump([{"pid": os.getpid(), "ppid": 1, "rss": 1, "cpu": 0, "args": "/bin/zsh", "cwd": None}], open(procs, "w"))
    r = sem.orq("iniciar", "--objetivo", "Frente Z", ORQ_PROCESSOS=procs, CLAUDECODE="1")
    assert r.returncode == 1 and "--agente" in r.stderr, "o ambiente herdado não decide sozinho"


# ---------- ticket 87: orq passar, um worker continua em outro harness na mesma worktree ----------

def _passagem_env(a, repo, **w):
    """Um worker Claude (Sonnet high) rodando na worktree; o Claude está no limite da semana e o Codex em 40%."""
    _ctl_env(a, repo, **w)
    _conta73(a, codex_semana=40, claude_semana=95)


def _flag(cmd, nome):
    return cmd[cmd.index(nome) + 1]


def test_it_should_hand_a_claude_worker_over_to_codex_in_the_same_worktree_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t, sujo=True)
        a = Amb(run="run_a")
        _passagem_env(a, repo)
        antes = _head(repo)
        assert a.orq("steer", "task_w1", "use a branch nova").returncode == 0
        a.set("../pendencias.json", {"itens": [{"id": "indice-novo", "tipo": "decisao", "titulo": "Qual índice criar?", "task": "task_w1"}, {"id": "outra", "tipo": "decisao", "titulo": "de outra task", "task": "task_x"}]})
        r = a.orq("passar", "ctx_w1", "--para", "codex")
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)
        assert out["novo_dispatch"] == "ctx_term_novo2" and out["resultado"] == "ok", out
        (parar,) = _log(a, "stopped.log")
        assert parar[:3] == ["worker-stop", "--dispatch", "ctx_w1"]
        (subir,) = _log(a, "started.log")
        assert (_flag(subir, "--task"), _flag(subir, "--retry-of"), _flag(subir, "--agent")) == ("task_w1", "ctx_w1", "codex"), subir
        assert (_flag(subir, "--model"), _flag(subir, "--effort")) == ("gpt-6-luna", "xhigh"), "Sonnet high vira Luna xhigh (tabela do worker-routing)"
        assert _flag(subir, "--worktree") == "id:repo_1::" + repo and "--spec" not in subir, subir
        texto = open(os.path.join(repo, "PASSAGEM.md"), encoding="utf-8").read()
        assert texto.splitlines()[0] == "<!-- orq-passagem v1 de=ctx_w1 para=codex -->", texto
        titulos = ["## Próximo passo", "## Perguntas abertas", "## Decisões já tomadas", "## Estado do git", "## Relatório parcial", "## Fim do transcrito",
                   "## Onde está o resto", "## Como agir"]
        pos = [texto.index(x) for x in titulos]
        assert pos == sorted(pos), "ação antes da prosa: as seções seguem a ordem do desenho"
        assert "use a branch nova" in texto[pos[2]:pos[3]], "o steer da task é decisão já tomada"
        assert "indice-novo: Qual índice criar?" in texto[pos[1]:pos[2]] and "de outra task" not in texto, "pergunta aberta é a decisão pendente da própria task"
        assert "novo" in texto[pos[3]:pos[4]] and antes[:12] in texto[pos[3]:pos[4]], "o estado do git traz o head e os caminhos sujos"
        nota = _enviados(a)[-1]
        assert nota[nota.index("--to") + 1] == "dispatch:ctx_term_novo2" and "PASSAGEM.md" in _flag(nota, "--body"), nota
        (ev,) = [e for e in a.events() if e["tipo"] == "passagem"]
        assert (ev["de"], ev["agente_de"], ev["para"], ev["agente_para"], ev["head"], ev["sujo"], ev["escrito_por"]) == \
            ("ctx_w1", "claude", "ctx_term_novo2", "codex", antes[:12], 1, "orq"), ev
        assert ev["pacote"] == hashlib.sha256(texto.encode("utf-8")).hexdigest()[:12], ev
        assert ev["aceita"] is True, "o hook do worker novo registrou o primeiro turno"
        assert _head(repo) == antes and os.path.exists(os.path.join(repo, "novo"))
        assert "PASSAGEM.md" not in subprocess.run(["git", "-C", repo, "status", "--porcelain"], capture_output=True, text=True).stdout, "o pacote não vai no commit do worker novo"
        assert _log(a, "released.log"), "o terminal do worker antigo é liberado depois que o novo sobe"
        (ctl,) = [e for e in _ctl_eventos(a, "passar") if e["resultado"] == "ok"]
        assert ctl["novo_dispatch"] == "ctx_term_novo2", ctl


def test_it_should_map_the_profile_between_harnesses_and_refuse_what_has_no_equivalent_handover():
    perfil = orq_mod._perfil_da_passagem
    assert perfil("claude-opus-5-5", "high", "codex") == ("gpt-6-sol", "high")
    assert perfil("claude-opus-5-5", "xhigh", "codex") == ("gpt-6-astra", "low")
    assert perfil("gpt-6-sol", "high", "claude") == ("claude-opus-5-5", "high")
    assert perfil("gpt-6-luna", "low", "claude") == ("claude-sonnet-5-5", "low")
    for modelo, effort, para in (("claude-haiku-4-5-20251001", "high", "codex"), ("claude-sonnet-5-5", "ultra", "codex"), ("gpt-6-astra", "high", "claude"), (None, None, "codex")):
        try:
            perfil(modelo, effort, para)
        except ValueError as e:
            assert "--modelo" in str(e), e
        else:
            raise AssertionError(f"{modelo}/{effort} não tem equivalente em {para}")


def test_it_should_use_the_explicit_profile_when_given_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _passagem_env(a, repo, modelo="claude-haiku-4-5-20251001")
        assert a.orq("passar", "ctx_w1", "--para", "codex", "--modelo", "gpt-6-sol").returncode == 1, "modelo e effort vão juntos"
        assert a.orq("passar", "ctx_w1", "--para", "codex", "--modelo", "gpt-6-sol", "--effort", "enorme").returncode == 1, "effort que o harness não tem"
        assert not _log(a, "stopped.log")
        r = a.orq("passar", "ctx_w1", "--para", "codex", "--modelo", "gpt-6-sol", "--effort", "medium")
        assert r.returncode == 0, r.stderr
        (subir,) = _log(a, "started.log")
        assert (_flag(subir, "--model"), _flag(subir, "--effort")) == ("gpt-6-sol", "medium"), subir


def test_it_should_refuse_before_stopping_anything_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _passagem_env(a, repo)
        r = a.orq("passar", "ctx_w1", "--para", "claude")
        assert r.returncode == 1 and "já é claude" in r.stderr, r
        assert a.orq("passar", "ctx_w1").returncode == 2, "--para é obrigatório"
        _conta73(a, codex_semana=95)
        r = a.orq("passar", "ctx_w1", "--para", "codex")
        assert r.returncode == 1 and "Codex" in r.stderr, "o Codex também está acima do limiar de pausa"
        a2 = Amb(run="run_a")
        _passagem_env(a2, os.path.join(t, "sumiu"))
        r = a2.orq("passar", "ctx_w1", "--para", "codex")
        assert r.returncode == 1 and "worktree" in r.stderr, r
        for amb in (a, a2):
            assert not _log(amb, "stopped.log") and not _log(amb, "started.log") and not [e for e in amb.events() if e["tipo"] in ("passagem", "controle")]
        assert not os.path.exists(os.path.join(repo, "PASSAGEM.md"))


def test_it_should_keep_the_package_and_the_old_terminal_when_the_new_worker_does_not_start_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t, sujo=True)
        a = Amb(run="run_a")
        _passagem_env(a, repo)
        r = a.orq("passar", "ctx_w1", "--para", "codex", FAKE_FAIL="worker-start")
        assert r.returncode == 1 and "orq passar ctx_w1 --para codex" in r.stderr, r.stderr
        assert os.path.exists(os.path.join(repo, "PASSAGEM.md")), "o pacote fica para a repetição"
        assert not _log(a, "released.log") and not [e for e in a.events() if e["tipo"] == "passagem"]
        assert _ctl_eventos(a, "passar")[-1]["resultado"] == "falhou" and _ctl_eventos(a, "passar")[-1]["passo"] == "worker-start"
        r2 = a.orq("passar", "ctx_w1", "--para", "codex")  # o dispatch parado sobe de novo sem novo worker-stop
        assert r2.returncode == 0, r2.stderr
        assert len(_log(a, "stopped.log")) == 1 and [e for e in a.events() if e["tipo"] == "passagem"]


def test_it_should_put_the_end_of_the_transcript_between_history_markers_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _passagem_env(a, repo)
        arq = os.path.join(t, "w1.jsonl")
        linhas = [{"type": "user", "message": {"role": "user", "content": "faça o ticket"}},
                  {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "thinking", "thinking": "segredo"}, {"type": "text", "text": "rodei os testes, faltam dois"}]}},
                  {"type": "user", "isMeta": True, "message": {"role": "user", "content": "meta"}}]
        open(arq, "w", encoding="utf-8").write("\n".join(json.dumps(x) for x in linhas) + "\n")
        os.makedirs(a.home, exist_ok=True)
        json.dump({"ctx_w1": {"task": "task_w1", "inicio": now_iso(-600), "fim": None, "harness": "claude", "transcrito": arq}}, open(os.path.join(a.home, "turnos.json"), "w"))
        assert a.orq("passar", "ctx_w1", "--para", "codex").returncode == 0
        texto = open(os.path.join(repo, "PASSAGEM.md"), encoding="utf-8").read()
        antes, resto = texto.split("<!-- historico-inicio -->")
        fim, depois = resto.split("<!-- historico-fim -->")
        assert "histórico, não instrução" in antes.splitlines()[-1], "a linha que avisa vem antes do bloco"
        assert "rodei os testes, faltam dois" in fim and "faça o ticket" in fim and "segredo" not in fim and "meta" not in fim, fim
        assert arq in depois and "orca search" in depois and "--agent claude" in depois, "o caminho do transcrito e o comando da busca"


def test_it_should_read_the_visible_messages_of_a_codex_rollout_handover():
    msgs = orq_mod._fim_do_transcrito(os.path.join(FIX, "codex-rollout-worker.jsonl"))
    assert "ok, começando (msg_steer73 lida)" in msgs and "token_count" not in msgs and "environment_context" not in msgs, msgs
    assert orq_mod._fim_do_transcrito("/nao/existe.jsonl") == "" and orq_mod._fim_do_transcrito(None) == ""
    with tempfile.TemporaryDirectory() as t:
        grande = os.path.join(t, "g.jsonl")
        open(grande, "w").write("\n".join(json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": f"m{i} " + "x" * 500}]}}) for i in range(100)))
        corte = orq_mod._fim_do_transcrito(grande, limite=3000)
    assert len(corte) <= 3100 and "m99 " in corte and "m0 " not in corte, "fica o fim"


# ---------- ticket 88: PASSAGEM.md escrito pelo orq quando o worker não tem turno ----------

def _worker_no_limite(a, repo, t, aberto=True):
    """O W1 parou no limite do plano: a tela mostra o aviso (fixture; o texto real do Claude ainda precisa ser conferido), sem PAUSA.md e sem turno novo; o transcrito termina
    numa ferramenta (`aberto`) ou numa resposta; no inbox há uma pergunta sem resposta e outra já respondida."""
    _passagem_env(a, repo)
    a.set("screens.json", {"term_w1": open(os.path.join(FIX, "tela-claude-limite.txt"), encoding="utf-8").read().splitlines()})
    arq = os.path.join(t, "w1.jsonl")
    linhas = [{"type": "user", "message": {"role": "user", "content": "faça o ticket"}},
              {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "thinking", "thinking": "segredo"}, {"type": "text", "text": "criei o índice, falta rodar a suíte"}]}}]
    linhas.append({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "tool_use", "name": "Bash", "input": {"command": "pytest"}}]}} if aberto
                  else {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "pronto para a suíte"}]}})
    open(arq, "w", encoding="utf-8").write("\n".join(json.dumps(x) for x in linhas) + "\n")
    a.set("search.json", [{"agent": "claude", "sessionId": "sess-w1", "cwd": repo, "source": {"filePath": arq}, "evidence": {"role": "user"}}])
    q = lambda i, seq, tipo, de, para, assunto: {"id": i, "run_id": "run_a", "type": tipo, "subject": assunto, "body": "detalhe de " + assunto, "sequence": seq,  # noqa: E731
                                                  "from_handle": de, "to_handle": para, "created_at": "2026-10-01T10:00:00Z", "payload": json.dumps({"dispatchId": "ctx_w1", "taskId": "task_w1"})}
    a.set("inbox.json", {"result": {"messages": [q("msg_q1", 1, "question", "dispatch:ctx_w1", "run:run_a", "Qual índice criar?"),
                                                 q("msg_r1", 2, "reply", "term_coord", "dispatch:ctx_w1", "re"),
                                                 q("msg_q2", 3, "question", "dispatch:ctx_w1", "run:run_a", "Posso apagar a coleção velha?")]}})
    with open(os.path.join(a.home, "events.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": now_iso(-300), "tipo": "resposta_worker", "msg_id": "msg_q1", "dispatch": "ctx_w1", "texto": "o índice parcial"}) + "\n")
    return arq


def test_it_should_write_the_package_for_a_worker_with_no_turn_within_the_deadline_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t, sujo=True)
        a = Amb(run="run_a")
        os.makedirs(a.home, exist_ok=True)
        arq = _worker_no_limite(a, repo, t)
        antes = _head(repo)
        t0 = time.monotonic()
        r = a.orq("passagem", "ctx_w1")
        assert r.returncode == 0, r.stderr
        assert time.monotonic() - t0 <= 20 and json.loads(r.stdout)["segundos"] <= 20, "o pacote sai em até 20 s"
        texto = open(os.path.join(repo, "PASSAGEM.md"), encoding="utf-8").read()
        assert texto.splitlines()[0] == "<!-- orq-passagem v1 de=ctx_w1 para=codex -->", "o outro harness é o padrão"
        titulos = ["## Próximo passo", "## Perguntas abertas", "## Decisões já tomadas", "## Estado do git", "## Relatório parcial", "## Fim do transcrito",
                   "## Onde está o resto", "## Como agir"]
        pos = [texto.index(x) for x in titulos]
        assert pos == sorted(pos), "as seções seguem a ordem da 2.3"
        secao = lambda i: texto[pos[i]:pos[i + 1] if i + 1 < len(pos) else None]  # noqa: E731
        assert "parou sem fechar o turno" in secao(0) and "Desconhecido" in secao(0), "o transcrito termina numa ferramenta e não há nota do worker"
        assert "Posso apagar a coleção velha?" in secao(1) and "Qual índice criar?" not in secao(1), "só a pergunta sem resposta"
        assert "Qual índice criar? → o índice parcial" in secao(2), "a pergunta respondida vira decisão"
        assert antes[:12] in secao(3) and "arquivos sujos, 1" in secao(3) and "novo" in secao(3), secao(3)
        assert "criei o índice, falta rodar a suíte" in secao(4), "sem relatório do worker, vale a última resposta visível dele"
        assert "segredo" not in texto and arq in secao(6) and "claude --resume sess-w1" in secao(6), secao(6)
        assert "não está na fila do E2E" in secao(7)
        assert not _log(a, "stopped.log") and not _log(a, "started.log") and not _enviados(a), "o worker não é parado, relançado nem consultado"
        assert _head(repo) == antes and not [e for e in a.events() if e["tipo"] == "passagem"]
        (ctl,) = _ctl_eventos(a, "passagem")
        assert ctl["resultado"] == "ok" and ctl["de"] == "claude" and ctl["para"] == "codex" and len(ctl["pacote"]) == 12, ctl


def test_it_should_not_say_the_turn_was_cut_when_the_transcript_ends_in_an_answer_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        os.makedirs(a.home, exist_ok=True)
        _worker_no_limite(a, repo, t, aberto=False)
        assert a.orq("passagem", "ctx_w1", "--para", "codex").returncode == 0
        texto = open(os.path.join(repo, "PASSAGEM.md"), encoding="utf-8").read()
        assert "parou sem fechar o turno" not in texto and "pronto para a suíte" in texto


def test_it_should_write_the_package_even_when_a_source_is_slow_or_down_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t, sujo=True)
        a = Amb(run="run_a")
        os.makedirs(a.home, exist_ok=True)
        _worker_no_limite(a, repo, t)
        t0 = time.monotonic()
        r = a.orq("passagem", "ctx_w1", FAKE_SLEEP_CMD="inbox:60")
        assert r.returncode == 0, r.stderr
        assert time.monotonic() - t0 <= 20, "o inbox parado não segura o pacote além do prazo"
        texto = open(os.path.join(repo, "PASSAGEM.md"), encoding="utf-8").read()
        assert "não li o inbox do Run" in texto, "a fonte que falhou é dita no pacote"
        assert "criei o índice" in texto and "novo" in texto, "o resto do pacote sai igual"


def test_it_should_say_when_the_old_worker_holds_the_e2e_queue_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        os.makedirs(a.home, exist_ok=True)
        _worker_no_limite(a, repo, t)
        fila = os.path.join(t, "queue", "0001-" + str(os.getpid()))
        os.makedirs(os.path.join(fila, "pids"))
        open(os.path.join(fila, "owner"), "w").write(f"pid={os.getpid()}\nworktree={repo}\nproject=produto\ncommand=test-e2e\nstarted={int(time.time())}\n")
        open(os.path.join(fila, "pids", str(os.getpid())), "w").close()
        assert a.orq("passagem", "ctx_w1", E2E_LOCK_DIR=os.path.dirname(fila)).returncode == 0
        texto = open(os.path.join(repo, "PASSAGEM.md"), encoding="utf-8").read()
        assert "segura a fila do E2E" in texto and "lock-release" in texto, texto[-700:]


def test_it_should_refuse_a_passagem_to_the_same_harness_or_without_worktree_handover():
    with tempfile.TemporaryDirectory() as t:
        repo, _ = _repo_git(t)
        a = Amb(run="run_a")
        _passagem_env(a, repo)
        r = a.orq("passagem", "ctx_w1", "--para", "claude")
        assert r.returncode == 1 and "outro harness" in r.stderr and not os.path.exists(os.path.join(repo, "PASSAGEM.md")), r
        a2 = Amb(run="run_a")
        _passagem_env(a2, os.path.join(t, "sumiu"))
        r = a2.orq("passagem", "ctx_w1")
        assert r.returncode == 1 and "worktree" in r.stderr, r


def test_ticket91_aviso_de_pausa_propoe_passar_cada_worker_a_pausar_quando_o_outro_harness_tem_folga():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
    _pausa51(a)
    _gerente(a)
    _uso51(a, semana=93)
    _conta73(a, codex_semana=40)
    assert a.orq("gerente", "absorver").returncode == 0
    (env,) = _log(a, "send.log")
    txt = env[env.index("--text") + 1]
    assert "orq passar ctx_term_f --para codex" in txt and "orq passar ctx_term_i --para codex" in txt, txt
    assert "ctx_term_s" not in txt and "ctx_term_p" not in txt, "alta e em review não seriam pausados"


def test_ticket91_aviso_de_pausa_sem_folga_no_outro_harness_ou_sem_numero_dele_segue_so_pausar():
    for conta in (dict(codex_semana=86), dict(codex_semana=40, codex_5h=95), {}):
        a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
        _pausa51(a)
        _gerente(a)
        _uso51(a, semana=93)
        _conta73(a, **conta)
        a.orq("gerente", "absorver")
        env = _log(a, "send.log")[0]  # o do Claude vem primeiro; o do Codex, se houver, é outro aviso
        assert "orq passar" not in env[env.index("--text") + 1] and "orq pausar" in env[env.index("--text") + 1], conta

# ticket 92: relatorio-final.md como worker_done de reserva e passagem aberta no status

def _reserva92(a, relatorio="# Entrega\n\nfeito", fim=-300, mtime=-400, inicio=-600, cwd=True, done=False):
    """Um worker sem worker_done: turnos.json com cwd, `relatorio-final.md` na worktree e, se `done`, o worker_done dele já no log."""
    _ingest_env(a)
    wt = os.path.join(a.tmp.name, "wt92")
    os.makedirs(wt)
    arq = os.path.join(wt, "relatorio-final.md")
    if relatorio is not None:
        open(arq, "w").write(relatorio)
        os.utime(arq, (time.time() + mtime, time.time() + mtime))
    json.dump({"ctx_92": {"task": "task_92", "inicio": now_iso(inicio), "fim": now_iso(fim) if fim is not None else None, "harness": "claude",
                          **({"cwd": wt} if cwd else {})}}, open(os.path.join(a.home, "turnos.json"), "w"))
    linhas = [{"ts": now_iso(-900), "tipo": "despacho", "run": "run_a", "task": "task_92", "dispatch": "ctx_92", "titulo": "t"}]
    if done:
        linhas.append({"ts": now_iso(-100), "tipo": "worker_done", "msg": "msg_real", "run": "run_a", "task": "task_92", "dispatch": "ctx_92", "outcome": "succeeded", "subject": "x"})
    open(os.path.join(a.home, "events.jsonl"), "w").write("".join(json.dumps(e) + "\n" for e in linhas))
    return arq


def test_ticket_92_relatorio_final_sem_worker_done_vira_worker_done_e_entrada():
    a = Amb()
    arq = _reserva92(a)
    assert a.orq("ingest").returncode == 0
    (wd,) = [e for e in a.events() if e["tipo"] == "worker_done" and e.get("dispatch") == "ctx_92"]
    assert wd["dispatch"] == "ctx_92" and wd["task"] == "task_92" and wd["run"] == "run_a" and wd["origem"] == "relatorio-final"
    (ent,) = [e for e in _entradas(a, "relatorio_worker") if e.get("task") == "task_92"]
    assert ent["caminho"] == arq and ent["task"] == "task_92" and ent["ref"] == wd["msg"]
    a.orq("ingest")
    assert len([e for e in a.events() if e["tipo"] == "worker_done" and e.get("dispatch") == "ctx_92"]) == 1, "o segundo ingest não repete"
    assert len([e for e in _entradas(a, "relatorio_worker") if e.get("task") == "task_92"]) == 1


def test_ticket_92_relatorio_final_nao_vale_com_worker_done_real_turno_aberto_ou_arquivo_velho():
    for campos in ({"done": True}, {"fim": None}, {"mtime": -700}, {"cwd": False}, {"relatorio": None}):
        a = Amb()
        _reserva92(a, **campos)
        assert a.orq("ingest").returncode == 0
        assert [e for e in a.events() if e.get("origem") == "relatorio-final"] == [], campos
        assert [e for e in _entradas(a, "relatorio_worker") if e.get("task") == "task_92"] == [], campos


def test_ticket_92_worker_done_real_depois_da_reserva_nao_duplica():
    a = Amb()
    _reserva92(a)
    a.orq("ingest")
    ib = _fix("inbox.json")
    ib["result"]["messages"].append({**ib["result"]["messages"][0], "id": "msg_tardio", "sequence": 990, "run_id": "run_a", "type": "worker_done",
                                     "created_at": "2099-01-01T00:00:00Z", "payload": json.dumps({"taskId": "task_92", "dispatchId": "ctx_92", "outcome": "succeeded", "reportPath": "/x/r.md"})})
    a.set("inbox.json", ib)
    a.orq("ingest")
    assert len([e for e in a.events() if e["tipo"] == "worker_done" and e.get("dispatch") == "ctx_92"]) == 1
    assert len([e for e in _entradas(a, "relatorio_worker") if e.get("task") == "task_92"]) == 1, "só a entrada da reserva: a do worker_done tardio é descartada"


def _passagem92(a, minutos, aceita=False, inicio_novo=False):
    os.makedirs(a.home, exist_ok=True)
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - minutos * 60))
    open(os.path.join(a.home, "events.jsonl"), "w").write(json.dumps(
        {"ts": ts, "tipo": "passagem", "de": "ctx_A", "agente_de": "claude", "para": "ctx_B", "agente_para": "codex", "aceita": aceita, "task": "task_92", "run": "run_a"}) + "\n")
    json.dump({"ctx_B": {"task": "task_92", "inicio": now_iso(-60), "fim": None}} if inicio_novo else {}, open(os.path.join(a.home, "turnos.json"), "w"))


def test_ticket_92_status_mostra_passagem_aberta_ha_mais_de_15_min():
    a = Amb()
    _passagem92(a, 22)
    out = a.orq("status").stdout
    assert "Passagens abertas" in out and "ctx_B" in out and "ctx_A" in out and "22 min" in out and "claude→codex" in out


def test_ticket_92_status_cala_passagem_recente_aceita_ou_com_turno_novo():
    for minutos, campos in ((5, {}), (30, {"aceita": True}), (30, {"inicio_novo": True})):
        a = Amb()
        _passagem92(a, minutos, **campos)
        assert "Passagens abertas" not in a.orq("status").stdout, (minutos, campos)



# ---------- ticket 105: o orq destrava sozinho o que fica preso ----------

def _tk105(a, num, titulo, status="ready-for-agent", bloqueado="(nenhum)", task=None, run="run_a", extra=""):
    """Escreve `NN-t.md` em ORQ_ISSUES com o cabeçalho de um ticket do /to-tickets; `extra` são linhas a mais do cabeçalho (Modelo, Effort)."""
    os.makedirs(a.env["ORQ_ISSUES"], exist_ok=True)
    with open(os.path.join(a.env["ORQ_ISSUES"], f"{num}-t.md"), "w") as f:
        f.write(f"# {num}: {titulo}\n\nStatus: {status}\nBlocked by: {bloqueado}\nRun: {run}\n" + (f"Task: {task}\n" if task else "") + extra +
                "\n## What to build\n\nx\n\n## Acceptance criteria\n\n- [ ] y\n")


def _tasks105(a, *tasks, run="run_a"):
    a.set(f"tasks_{run}.json", [{"id": i, "task_title": "x", "status": s, "dispatch_id": None, "deps": "[]"} for i, s in tasks])


def _cab105(a, num):
    return _lido(a, num).split("\n## ")[0]


MODELO105 = "Modelo: claude-sonnet-5-5\nEffort: medium\n"


def _caso_87(a):
    """O caso real de 01/10: o 88, o 91 e o 93 ficaram 'Blocked by: 87' depois do 87 resolvido."""
    _tk105(a, "87", "Orq passar de harness", "claimed", task="task_87")
    _tk105(a, "88", "Passagem escrita", bloqueado="87", task="task_88", extra=MODELO105)
    _tk105(a, "91", "Segurança do limiar", bloqueado="87", task="task_91")
    _tk105(a, "93", "Painel de passagem", bloqueado="87", task="task_93", extra=MODELO105)
    _tk105(a, "90", "Ainda aberto", "claimed", task="task_90")
    _tk105(a, "95", "Espera dois", bloqueado="87, 90", task="task_95", extra=MODELO105)
    _tk105(a, "96", "Já em andamento", "claimed", bloqueado="87", task="task_96", extra=MODELO105)
    _tasks105(a, ("task_87", "dispatched"), ("task_88", "blocked"), ("task_91", "pending"), ("task_93", "blocked"), ("task_90", "dispatched"), ("task_95", "blocked"), ("task_96", "blocked"))


def test_ticket105_fechar_o_87_libera_o_88_o_91_e_o_93_e_tira_o_87_de_todos_os_blocked_by():
    a = Amb(run="run_a")
    _caso_87(a)
    r = a.orq("ticket", "fechar", "87", "--answer", "feito")
    assert r.returncode == 0, r.stderr
    for n in ("88", "91", "93", "96"):
        assert "Blocked by: (nenhum)" in _cab105(a, n), (n, _cab105(a, n))
    assert "Blocked by: 90" in _cab105(a, "95"), "o que ainda tem outro bloqueio aberto só perde o 87"
    out = json.loads(r.stdout)
    assert [(x["ticket"], x["prioridade"]) for x in out["liberados"]] == [("91", 1), ("88", 2), ("93", 3)], "liberado = ficou sem bloqueio e ainda é ready-for-agent; a ordem é a de prioridade"
    ev = [e for e in a.events() if e["tipo"] == "ticket" and e["op"] == "fechar"][-1]
    assert [x["ticket"] for x in ev["liberados"]] == ["91", "88", "93"], "o orq status lê do evento"


def test_ticket105_liberado_p1_ou_p2_com_modelo_entra_na_fila_de_despacho_e_p3_nunca():
    a = Amb(run="run_a")
    _caso_87(a)
    r = a.orq("ticket", "fechar", "87", "--answer", "feito")
    (it,) = _fila79(a)
    assert (it["tipo"], it["ticket"], it["run"], it["modelo"], it["effort"], it["prioridade"]) == ("despacho", "88", "run_a", "claude-sonnet-5-5", "medium", 2), it
    por = {x["ticket"]: x for x in json.loads(r.stdout)["liberados"]}
    assert por["88"]["fila"] == it["id"] and not por["93"].get("fila"), "o P3 tem Modelo e nem assim entra na fila"
    assert not por["91"].get("fila") and "91" in r.stderr and "Modelo:" in r.stderr, "P1 sem Modelo/Effort só avisa"
    assert not _log(a, "started.log"), "o ticket só entra na fila: quem sobe é o gerente"


def test_ticket105_task_blocked_do_liberado_vira_ready_e_a_pending_fica():
    a = Amb(run="run_a")
    _caso_87(a)
    a.orq("ticket", "fechar", "87", "--answer", "feito")
    feitos = {x[x.index("--id") + 1]: x[x.index("--status") + 1] for x in _log(a, "updated.log")}
    assert feitos == {"task_87": "completed", "task_88": "ready", "task_93": "ready", "task_96": "ready"}, "o 91 está pending (o Orca a libera) e o 95 ainda tem o 90"
    tasks = {t["id"]: t["status"] for t in json.load(open(os.path.join(a.fake, "tasks_run_a.json")))}
    assert tasks["task_91"] == "pending" and tasks["task_95"] == "blocked", tasks


def test_ticket105_status_mostra_os_liberados_ate_o_ticket_ser_despachado():
    a = Amb(run="run_a")
    _caso_87(a)
    a.orq("ticket", "fechar", "87", "--answer", "feito")
    assert "liberados: 91, 88, 93 (P1, P2, P3)" in a.orq("status").stdout, a.orq("status").stdout
    _tk105(a, "88", "Passagem escrita", "claimed", task="task_88", extra=MODELO105)  # o 88 subiu
    assert "liberados: 91, 93 (P1, P3)" in a.orq("status").stdout
    a.orq("ticket", "fechar", "91", "--answer", "ok")
    assert "liberados: 93 (P3)" in a.orq("status").stdout, "o ticket fechado também sai"


def test_ticket105_fechar_sem_dependentes_nao_muda_nada_alem_do_que_fazia():
    a = Amb(run="run_a")
    _tk105(a, "05", "Sozinho", "claimed", task="task_05")
    _tasks105(a, ("task_05", "dispatched"))
    out = json.loads(a.orq("ticket", "fechar", "05", "--answer", "ok").stdout)
    assert out["liberados"] == [] and "liberados:" not in a.orq("status").stdout
    assert [x[x.index("--status") + 1] for x in _log(a, "updated.log")] == ["completed"]


def test_ticket105_doctor_tasks_completa_a_task_de_ticket_resolvido_e_lista_a_sem_ticket():
    a = Amb(run="run_a")
    _tk105(a, "59", "Painel antigo", "resolved", task="task_b8e395095fb8")
    _tk105(a, "60", "Painel novo", "claimed", task="task_vivo")
    _tasks105(a, ("task_b8e395095fb8", "blocked"), ("task_vivo", "blocked"), ("task_b5a1d9de7e48", "pending"), ("task_pronta", "ready"), ("task_feita", "completed"))
    a.set("runs.json", [{"id": "run_a"}])
    r = a.orq("doctor", "tasks", "--json")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert [(x["task"], x["ticket"]) for x in out["completadas"]] == [("task_b8e395095fb8", "59")], out
    assert [(x["task"], x["status"], x["run"]) for x in out["sem_ticket"]] == [("task_b5a1d9de7e48", "pending", "run_a")], "ready sem ticket é despacho normal"
    (arg,) = _log(a, "updated.log")
    assert arg[arg.index("--id") + 1] == "task_b8e395095fb8" and arg[arg.index("--status") + 1] == "completed", arg
    assert json.loads(arg[arg.index("--result") + 1]) == {"ticket": "59", "supersededBy": "ticket 59"}, arg
    tasks = {t["id"]: t["status"] for t in json.load(open(os.path.join(a.fake, "tasks_run_a.json")))}
    assert tasks == {"task_b8e395095fb8": "completed", "task_vivo": "blocked", "task_b5a1d9de7e48": "pending", "task_pronta": "ready", "task_feita": "completed"}, tasks
    txt = a.orq("doctor", "tasks").stdout
    assert "sem ticket: task_b5a1d9de7e48" in txt and "task_b8e395095fb8" not in txt, "a segunda rodada não acha mais nada para completar"
    assert "completada: task_b8e395095fb8 (ticket 59" in orq_mod.texto_doctor_tasks(out)


def test_ticket105_doctor_tasks_dry_run_so_lista():
    a = Amb(run="run_a")
    _tk105(a, "59", "Painel antigo", "resolved", task="task_old")
    _tasks105(a, ("task_old", "blocked"))
    a.set("runs.json", [{"id": "run_a"}])
    out = json.loads(a.orq("doctor", "tasks", "--dry-run", "--json").stdout)
    assert [x["task"] for x in out["completadas"]] == ["task_old"] and not _log(a, "updated.log"), "dry-run diz o que faria e não escreve"


def _aguardando105(a, com_fila=True):
    """O t80 do caso real: parado há 12 min no prompt, esperando o integrador levar a branch dele para a main."""
    a.set("workers.json", [{"handle": "term_t80", "run": "run_a", "status": "dispatched", "desde": _iso(-3000), "agente": "claude"}])
    os.makedirs(a.home, exist_ok=True)
    json.dump({"ctx_term_t80": {"task": "task_term_t80", "inicio": now_iso(-1200), "fim": now_iso(-720)}}, open(os.path.join(a.home, "turnos.json"), "w"))
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"tipo": "despacho", "run": "run_a", "task": "task_term_t80", "dispatch": "ctx_term_t80", "ticket": "80", "titulo": "Secondmate por grupo"}) + "\n")
    if com_fila:
        assert a.orq("integrar", "fila", "add", "feat/secondmate-por-grupo", "80").returncode == 0


def test_ticket105_worker_na_fila_do_integrador_fica_aguardando_integracao_e_nao_parado():
    a = Amb(run="run_a")
    _aguardando105(a, com_fila=False)
    assert _agentes(a)["ctx_term_t80"]["estado"] == "parado" and 'orq steer task_term_t80' in a.orq("agentes").stdout, "sem a fila é o parado de sempre"
    _aguardando105(a)
    t80 = _agentes(a)["ctx_term_t80"]
    assert t80["estado"] == "aguardando_integracao" and t80["integracao"]["branch"] == "feat/secondmate-por-grupo" and t80["integracao"]["ticket"] == "80", t80
    txt = a.orq("agentes").stdout
    assert "aguardando integração" in txt and "feat/secondmate-por-grupo" in txt and "orq steer" not in txt, txt
    assert a.orq("integrar", "fila", "rm", "80").returncode == 0
    assert _agentes(a)["ctx_term_t80"]["estado"] == "parado", "saiu da fila: volta ao critério normal"


def test_ticket105_o_cache_do_aberto_tambem_vira_aguardando_integracao_sem_sugerir_o_steer():
    agora = datetime.now(timezone.utc)
    ab = _aberto_ag("parado", agente="claude", desde=now_iso(-3000), turno_inicio=now_iso(-1200), turno_fim=now_iso(-720), idade_s=720)
    task = ab["agentes"][0]["task"]
    evs = [{"tipo": "despacho", "task": task, "dispatch": ab["agentes"][0]["dispatch"], "ticket": "80"}]
    fila = {"80": {"branch": "feat/secondmate-por-grupo", "ticket": "80"}}
    (r,) = orq_mod.reavalia(ab["agentes"], evs, agora, integracao=fila)
    assert r["estado"] == "aguardando_integracao" and r["integracao"]["branch"] == "feat/secondmate-por-grupo", r
    (r2,) = orq_mod.reavalia(ab["agentes"], evs, agora, integracao={})
    assert r2["estado"] == "parado", "fila vazia: o critério normal"
    a = Amb(run="run_a")  # o orq status (o hook do prompt) lê o aberto.json e a fila do disco
    os.makedirs(a.home, exist_ok=True)
    json.dump(ab, open(os.path.join(a.home, "aberto.json"), "w"))
    json.dump({"ctx_1": {"task": task, "inicio": now_iso(-1200), "fim": now_iso(-720)}}, open(os.path.join(a.home, "turnos.json"), "w"))
    open(os.path.join(a.home, "events.jsonl"), "w").write("".join(json.dumps(e) + "\n" for e in evs))
    assert 'Parado no prompt: ' + task in a.orq("status").stdout, "sem a fila o status pede o steer"
    assert a.orq("integrar", "fila", "add", "feat/secondmate-por-grupo", "80").returncode == 0
    txt = a.orq("status").stdout
    assert "Parado no prompt" not in txt and "orq steer" not in txt and "aguardando integração de feat/secondmate-por-grupo" in txt, txt


def test_ticket105_fila_do_integrador_lista_add_e_rm():
    a = Amb(run="run_a")
    assert "vazia" in a.orq("integrar", "fila", "lista").stdout
    assert a.orq("integrar", "fila", "add", "feat/a", "80").returncode == 0
    assert a.orq("integrar", "fila", "add", "feat/a", "80").returncode == 0, "repetir não duplica"
    assert a.orq("integrar", "fila", "add", "feat/b", "81").returncode == 0
    fila = json.loads(a.orq("integrar", "fila", "lista", "--json").stdout)
    assert [(i["branch"], i["ticket"]) for i in fila] == [("feat/a", "80"), ("feat/b", "81")], fila
    assert a.orq("integrar", "fila", "rm", "80").returncode == 0
    assert [i["ticket"] for i in json.loads(a.orq("integrar", "fila", "lista", "--json").stdout)] == ["81"]
    r = a.orq("integrar", "fila", "rm", "99")
    assert r.returncode == 1 and "99" in r.stderr, "ticket que não está na fila é erro"


def test_ticket105_aguardando_integracao_e_espera_conhecida_para_o_hibernar():
    cfg = {"min": 15, "externa_min": 2}
    agora = datetime.now(timezone.utc)
    ag = {"estado": "aguardando_integracao", "turno": "parado", "task": "task_t", "turno_inicio": _z60(40), "turno_fim": _z60(10),
          "integracao": {"branch": "feat/x", "ticket": "80"}}
    assert orq_mod.motivo_hibernar(ag, agora, cfg) == "esperando: integração de feat/x (ticket 80)", "espera algo que o orq conhece: hiberna depois de externa_min"
    assert orq_mod.motivo_hibernar({**ag, "turno_fim": _z60(1)}, agora, cfg) is None


def _servico105(a, ciclo=True):
    """O integrador: despachado como serviço; o Orca revoga a capability depois do primeiro worker_done e o dispatch aparece completed com o terminal aberto."""
    a.set("workers.json", [{"handle": "term_int", "run": "run_a", "status": "completed", "terminal": "active", "desde": _iso(-9000), "agente": "claude"}])
    a.caixa(("worker_done", {"taskId": "task_term_int", "dispatchId": "ctx_term_int"}))
    os.makedirs(a.home, exist_ok=True)
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        f.write(json.dumps({"tipo": "despacho", "run": "run_a", "task": "task_term_int", "dispatch": "ctx_term_int", "titulo": "Integrador", "servico": True}) + "\n")


def test_ticket105_despachar_servico_marca_o_dispatch_e_o_evento():
    a = Amb(run="run_a")
    r = _despachar(a, "--servico")
    assert r.returncode == 0, r.stderr
    (ev,) = [e for e in a.events() if e["tipo"] == "despacho"]
    assert ev["servico"] is True, ev
    b = Amb(run="run_a")
    assert _despachar(b).returncode == 0
    assert "servico" not in [e for e in b.events() if e["tipo"] == "despacho"][0], "o despacho comum não carrega a marca"


def test_ticket105_servico_entregue_nunca_aparece_como_entregue_sem_liberar_e_mostra_o_ultimo_ciclo():
    a = Amb(run="run_a")
    _servico105(a)
    s = _agentes(a)["ctx_term_int"]
    assert s["estado"] == "servico" and s.get("ciclo") is None, s
    assert "serviço, nenhum ciclo ainda" in a.orq("agentes").stdout and "orq liberar" not in a.orq("agentes").stdout
    assert "Entregues sem liberar" not in json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]
    r = a.orq("ciclo", "feito", "--dispatch", "ctx_term_int", "--hash", "abc1234", "--nota", "integrou 74, 59 e 79")
    assert r.returncode == 0, r.stderr
    s = _agentes(a)["ctx_term_int"]
    assert s["estado"] == "servico" and s["ciclo"]["hash"] == "abc1234" and s["ciclo"]["nota"] == "integrou 74, 59 e 79", s
    txt = a.orq("agentes").stdout
    assert "serviço, último ciclo" in txt and "abc1234" in txt and "integrou 74, 59 e 79" in txt and "orq liberar" not in txt, txt
    (ev,) = [e for e in a.events() if e["tipo"] == "ciclo"]
    assert (ev["dispatch"], ev["hash"], ev["nota"]) == ("ctx_term_int", "abc1234", "integrou 74, 59 e 79")
    assert not _log(a, "sent.log") and not _log(a, "released.log"), "o ciclo não fala com o Orca: o worker não tem mais capability"


def test_ticket105_ciclo_feito_recusa_dispatch_que_nao_e_servico():
    a = Amb(run="run_a")
    _agentes_env(a)
    r = a.orq("ciclo", "feito", "--dispatch", "ctx_term_r1", "--hash", "abc")
    assert r.returncode == 1 and "serviço" in r.stderr and not [e for e in a.events() if e["tipo"] == "ciclo"], r.stderr


def test_ticket105_servico_nao_entra_na_hibernacao_do_entregue():
    cfg = {"min": 15, "externa_min": 2}
    ag = {"estado": "servico", "turno": "unknown", "task": "t", "turno_inicio": _z60(90), "turno_fim": _z60(80)}
    assert orq_mod.motivo_hibernar(ag, datetime.now(timezone.utc), cfg) is None


def _gerente105(a, voltas=None):
    _gerente(a)
    g = json.load(open(os.path.join(a.home, "gerente.json")))
    if voltas is not None:
        g["voltas_s"] = voltas
    json.dump(g, open(os.path.join(a.home, "gerente.json"), "w"))


def _ctx105(a):
    return json.loads(a.prompt("oi").stdout)["hookSpecificOutput"]["additionalContext"]


def test_ticket105_volta_lenta_nao_vira_painel_parado_o_limite_acompanha_a_media_das_voltas():
    a = Amb(run="run_a")
    a.set("terminals.json", ["term_coord", "term_ger"])
    _gerente105(a, voltas=[50, 55, 60])  # média 55 s: 3x = 165 s
    _painel_tocado(a, 100)
    ctx = _ctx105(a)
    assert "parado há" not in ctx and "painel lento (55 s por volta)" in ctx, ctx
    _painel_tocado(a, 200)
    assert "painel do agent manager parado há 3 min" in _ctx105(a), "acima de 3x a média é parado de verdade"
    _painel_tocado(a, 20)
    assert "painel do agent manager" not in _ctx105(a)


def test_ticket105_sem_voltas_gravadas_o_limite_continua_90_s():
    a = Amb(run="run_a")
    a.set("terminals.json", ["term_coord", "term_ger"])
    _gerente105(a)
    _painel_tocado(a, 75)
    assert "painel lento" in _ctx105(a) and "parado há" not in _ctx105(a), "entre 60 e 90 s é lento"
    _painel_tocado(a, 100)
    assert "painel do agent manager parado há 1 min" in _ctx105(a)


def test_ticket105_gerente_absorver_toca_o_carimbo_e_grava_a_duracao_da_volta():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
    _gerente105(a, voltas=[1.0] * 20)
    _painel_tocado(a, 500)
    r = a.orq("gerente", "absorver", FAKE_SLEEP="0.05")
    assert r.returncode == 0, r.stderr
    assert time.time() - os.path.getmtime(os.path.join(a.home, orq_mod.PAINEL_VIVO)) < 5, "a volta toca o carimbo, e não só o shell do painel"
    voltas = json.load(open(os.path.join(a.home, "gerente.json")))["voltas_s"]
    assert len(voltas) == orq_mod.VOLTAS_LEMBRADAS and voltas[-1] > 0 and voltas[-1] < 30, "guarda as últimas voltas, a mais nova no fim"
    assert json.load(open(os.path.join(a.home, "gerente.json")))["runs"] == ["run_a"], "o resto do gerente.json fica"


def test_ticket105_a_volta_toca_o_carimbo_depois_de_cada_run_e_nao_so_no_fim():
    a = Amb(run="run_a", ORCA_TERMINAL_HANDLE="term_ger")
    _gerente(a)
    json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": ["run_a", "run_b"]}, open(os.path.join(a.home, "gerente.json"), "w"))
    _painel_tocado(a, 500)
    t0 = time.time()
    a.orq("gerente", "absorver", FAKE_SLEEP="0.5", FAKE_FAIL_RUN="run_b")  # o run_a leva ~1 s (run-use e check); o run_b falha e a volta cai sem chegar ao fim
    assert os.path.getmtime(os.path.join(a.home, orq_mod.PAINEL_VIVO)) - t0 > 0.9, "o carimbo foi tocado de novo depois do primeiro Run, no meio da volta"


def _entregue105(a):
    """O caso do t77: o worker já mandou o worker_done e o Orca ainda lista o dispatch como dispatched, com a sessão guardada."""
    a.set("workers.json", [{"handle": "term_e", "run": "run_a", "status": "dispatched", "desde": _iso(-900), "agente": "claude"},
                           {"handle": "term_v", "run": "run_a", "status": "dispatched", "desde": _iso(-900), "agente": "claude"}])
    _inbox(a, {**_msg(-60, 700, tipo="worker_done"), "payload": json.dumps({"taskId": "task_term_e", "dispatchId": "ctx_term_e", "outcome": "succeeded"}), "from_handle": "dispatch:ctx_term_e"})


def test_ticket105_pausar_dispatch_entregue_diz_liberar_e_sai_na_hora_sem_mandar_steer():
    a = Amb(run="run_a")
    _entregue105(a)
    t0 = time.time()
    r = a.orq("pausar", "ctx_term_e", ORQ_PAUSA_ESPERA_S="300")
    assert time.time() - t0 < 20, "não espera o PAUSA.md"
    assert r.returncode == 1 and "orq liberar ctx_term_e" in r.stderr and "entreg" in r.stderr, (r.stdout, r.stderr)
    assert not _log(a, "sent.log"), "nem steer nem espera"


def test_ticket105_pausar_por_criterio_pula_o_entregue_e_segue_com_os_outros():
    a = Amb(run="run_a")
    _entregue105(a)
    out = json.loads(a.orq("pausar", "--dry-run", "--json", "--ate-prioridade", "2").stdout)
    estados = {x["dispatch"]: x["estado"] for x in out["pausados"]}
    assert estados["ctx_term_e"] == "entregue" and estados.get("ctx_term_v") != "entregue", estados
    assert "orq liberar ctx_term_e" in [x for x in out["pausados"] if x["dispatch"] == "ctx_term_e"][0]["aviso"]


def test_ticket105_ticket_novo_em_outro_run_liga_o_run_e_religa_o_que_estava():
    a = Amb(run="run_a")  # o coordenador está no Run A e o ticket é do Run B
    r = _novo(a, "No Run B", "--run", "run_b")
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["run"] == "run_b"
    assert json.load(open(os.path.join(a.fake, "run.json")))["id"] == "run_a", "o Run A volta a ser o ligado"
    usos = [c[c.index("--id") + 1] for c in _log(a, "calls.log") if c[0] == "run-use"]
    assert usos == ["run_b", "run_a"], usos
    assert [t["task_title"] for t in json.load(open(os.path.join(a.fake, "tasks_run_b.json")))] == ["No Run B"]


def test_ticket105_liberar_em_outro_run_liga_o_run_confirma_o_ack_e_religa_o_anterior():
    b = Amb(run="run_b")  # liberar um dispatch do Run A com o coordenador no B
    _lib_env(b)
    out = json.loads(b.orq("liberar", "ctx_term_w1").stdout)
    assert out["estado"] == "released" and "ack" not in out["aviso"], out
    assert b.estados()["msg_1"] == "acked", "com o Run ligado o ack acontece"
    assert json.load(open(os.path.join(b.fake, "run.json")))["id"] == "run_b"


def test_ticket105_com_o_gerente_ligado_o_run_solto_continua_pedindo_o_gerente_ligar():
    a = Amb()
    _multi(a, {"run_a": "term_ger", "run_b": None}, ["run_a"])
    a.set("tasks_run_b.json", [{"id": "t2", "status": "dispatched", "dispatch_id": "ctx_1"}])
    a.prompt("oi")
    r = a.orq("steer", "t2", "ajuste", "--run", "run_b")
    assert r.returncode == 1 and "orq gerente ligar --terminal term_ger --run run_b" in r.stderr, "religar o terminal do coordenador o tiraria do gerente"
    assert not [c for c in _log(a, "calls.log") if c[0] == "run-use"]


# ---------- ticket 114: cada aviso gera as obrigações que implica ----------

PR_MAIN = PR1.replace("1216", "1282")


def _merge_main(a, issue="2045"):
    """Liga o PR de main à task_feat1 (com --issue, se houver), o gh o vê mergeado e o poll cria a entrada. Devolve o id dela."""
    _pr(a, PR_MAIN, "OPEN", "main")
    assert a.orq("pr", "ligar", "task_feat1", PR_MAIN, *(["--issue", issue] if issue else [])).returncode == 0
    _pr(a, PR_MAIN, "MERGED", "main")
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    (ent,) = [e for e in a.events() if e["tipo"] == "entrada" and e.get("origem") == "pr"]
    return ent["id"]


def _obrig(a, e=None):
    return {o["chave"]: o["texto"] for o in orq_mod.obrigacoes_abertas(a.events(), e)}


def _ctx(r):
    return json.loads(r.stdout)["hookSpecificOutput"]["additionalContext"]


def test_ticket114_merge_em_main_com_issue_gera_as_obrigacoes_e_elas_aparecem_no_preambulo():
    a = _prs_env()
    e = _merge_main(a)
    ob = _obrig(a, e)
    assert set(ob) == {"deploy", "comentario", "limpeza"}, ob
    assert "#2045" in ob["comentario"] and "produção" in ob["deploy"], ob
    ctx = _ctx(a.prompt("e agora?"))
    assert f"A fazer por você: {e} →" in ctx and "#2045" in ctx and "orq feito" in ctx, ctx
    assert len(ctx.splitlines()) <= 5, ctx
    aviso = a.prompt(f"orq: PR #1282 entrou em main (task_feat1, issue #2045): em main. Entrada {e}.")
    assert f"A fazer por você: {e} →" in _ctx(aviso), "o aviso digitado pelo painel já traz o que ele pede"
    outro = a.prompt("orq: Fila do E2E parada há 40 min.")
    assert "A fazer por você" not in (outro.stdout or ""), "só o aviso de PR carrega as obrigações"
    for k in range(4):  # avisos antigos não somem com obrigações abertas: elas ficam fora do teto da linha extra
        with open(os.path.join(a.home, "events.jsonl"), "a") as f:
            f.write(json.dumps({"ts": "2026-09-30T10:00:00Z", "tipo": "obrigacao", "op": "nova", "entrada": f"e9{k}", "chave": "deploy", "texto": "x" * 150}) + "\n")
    linha = next(l for l in _ctx(a.prompt("e agora?")).splitlines() if "A fazer por você" in l)
    assert linha.index("PR: ") < linha.index("A fazer por você") and "e93 → deploy" in linha, linha


def test_ticket114_pr_sem_issue_nao_gera_a_obrigacao_de_comentar():
    a = _prs_env()
    ob = _obrig(a, _merge_main(a, issue=None))
    assert "comentario" not in ob and "deploy" in ob, ob


def test_ticket114_issue_e_ticket_vem_do_cabecalho_do_ticket_da_task_e_fechar_o_ticket_cumpre_a_obrigacao():
    a = _prs_env()
    os.makedirs(a.env["ORQ_ISSUES"], exist_ok=True)
    with open(os.path.join(a.env["ORQ_ISSUES"], "07-feature.md"), "w") as f:
        f.write("# 07: Feature\n\nStatus: claimed\nBlocked by: (nenhum)\nRun: run_a\nTask: task_feat1\nissue: #2045\n\n## What to build\n\nX\n")
    e = _merge_main(a, issue=None)
    ob = _obrig(a, e)
    assert "#2045" in ob.get("comentario", "") and "07" in ob.get("ticket", ""), ob
    assert a.orq("ticket", "fechar", "07", "--answer", "entregue").returncode == 0
    assert "ticket" not in _obrig(a, e), "o orq cumpre sozinho o que consegue e só registra"
    (f,) = [x for x in a.events() if x["tipo"] == "obrigacao" and x["op"] == "feito"]
    assert f["chave"] == "ticket" and "07" in f["prova"], f


def test_ticket114_merge_em_development_pede_o_proximo_pr_e_o_deploy():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "development")
    a.orq("pr", "poll", "--forcar")
    ob = _obrig(a)
    assert set(ob) == {"deploy", "proximo"} and "staging" in ob["proximo"], ob


def test_ticket114_intake_conversa_ou_descartado_e_recusado_com_obrigacao_aberta():
    a = _prs_env()
    e = _merge_main(a)
    for efeito in ("conversa", "descartado"):
        r = a.orq("intake", e, efeito, "--nota", "visto")
        assert r.returncode == 1 and "obrigação" in r.stderr and "orq feito" in r.stderr and "comentario" in r.stderr, r
    assert not [x for x in a.events() if x["tipo"] == "intake"]


def test_ticket114_feito_com_prova_fecha_a_obrigacao_e_a_ultima_fecha_a_entrada():
    a = _prs_env()
    e = _merge_main(a)
    assert a.orq("feito", e, "comentario").returncode == 2, "sem --prova não fecha"
    r = a.orq("feito", e, "nao-existe", "--prova", "x")
    assert r.returncode == 1 and "deploy" in r.stderr, r
    r = a.orq("feito", e, "comentario", "--prova", "https://github.com/acme/app/issues/2045#issuecomment-1")
    assert r.returncode == 0, r.stderr
    assert "comentario" not in _obrig(a, e) and e in {x["id"] for x in orq_mod.abertas(a.events())}
    assert a.orq("feito", e, "comentario", "--prova", "de novo").returncode == 1, "já fechada"
    a.orq("feito", e, "deploy", "--prova", "v776")
    a.orq("feito", e, "limpeza", "--prova", "branch e worktree removidas")
    assert not _obrig(a, e) and e not in {x["id"] for x in orq_mod.abertas(a.events())}, "sem obrigação aberta a entrada se fecha"
    assert "A fazer por você" not in _ctx(a.prompt("e agora?"))


def test_ticket114_adiar_cria_um_ticket_com_o_motivo():
    a = _prs_env()
    e = _merge_main(a)
    r = a.orq("adiar", e, "deploy", "--motivo", "deploy de produção só amanhã")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    txt = _lido(a, out["ticket"])
    assert "deploy de produção só amanhã" in txt and e in txt and "## Acceptance criteria" in txt, txt
    assert "deploy" not in _obrig(a, e)
    (ad,) = [x for x in a.events() if x["tipo"] == "obrigacao" and x["op"] == "adiada"]
    assert ad["ticket"] == out["ticket"] and ad["motivo"] == "deploy de produção só amanhã", ad
    assert a.orq("adiar", e, "limpeza").returncode == 2, "sem --motivo não adia"


def test_ticket114_o_stop_avisa_uma_vez_da_obrigacao_velha():
    a = _prs_env(ORQ_OBRIGACAO_MIN="0")
    e = _merge_main(a)
    m1 = json.loads(_stop(a).stdout)["systemMessage"]
    assert "Obrigação aberta" in m1 and f"{e} comentario" in m1 and "orq feito" in m1, m1
    m2 = json.loads(_stop(a).stdout or "{}").get("systemMessage", "")
    assert "Obrigação aberta" not in m2, "uma vez só: não bloqueia em loop"
    for chave in ("deploy", "comentario", "limpeza"):
        a.orq("feito", e, chave, "--prova", "ok")
    with open(os.path.join(a.home, "events.jsonl"), "a") as f:
        for n in range(6):  # seis obrigações velhas: o Stop cita quatro e só essas contam como cobradas
            f.write(json.dumps({"ts": "2026-09-30T10:00:00Z", "tipo": "obrigacao", "op": "nova", "entrada": "e90", "chave": f"k{n}", "texto": f"t{n}"}) + "\n")
    m3 = json.loads(_stop(a).stdout)["systemMessage"]
    assert "e90 k3" in m3 and "e90 k4" not in m3 and "+2 no próximo Stop" in m3, m3
    m4 = json.loads(_stop(a).stdout)["systemMessage"]
    assert "e90 k4" in m4 and "e90 k5" in m4 and "e90 k0" not in m4, "a que não foi citada não se perde"
    b = _prs_env(ORQ_OBRIGACAO_MIN="10")
    _merge_main(b)
    assert "Obrigação aberta" not in json.loads(_stop(b).stdout or "{}").get("systemMessage", ""), "a obrigação nova ainda não é cobrada"


def test_ticket114_o_mesmo_fluxo_roda_com_o_payload_de_hook_do_codex():
    a = _prs_env(ORQ_OBRIGACAO_MIN="0")
    e = _merge_main(a)
    r = _hook_codex(a, "prompt", _codex("userpromptsubmit", session_id="abcdef123456", prompt="e agora?"))
    assert f"A fazer por você: {e} →" in _ctx(r), r.stdout
    m = json.loads(_hook_codex(a, "stop", _codex("stop", session_id="abcdef123456")).stdout)["systemMessage"]
    assert "Obrigação aberta" in m and f"{e} deploy" in m, m
    for chave in ("deploy", "comentario", "limpeza"):
        assert a.orq("feito", e, chave, "--prova", "ok").returncode == 0
    assert "A fazer por você" not in _ctx(_hook_codex(a, "prompt", _codex("userpromptsubmit", session_id="abcdef123456", prompt="e agora?")))


def test_ticket107_without_away_mode_no_notice_is_typed_in_the_coordinator_and_all_reach_the_next_prompt():
    a = Amb()
    with tempfile.TemporaryDirectory() as fila:
        antes, dig = orq_mod.HOME, orq_mod.digita
        orq_mod.HOME, orq_mod.digita = a.home, lambda h, t: _nao_digita()
        try:
            os.makedirs(a.home, exist_ok=True)
            json.dump({"coordenador": "term_c", "gerente": "term_g", "runs": []}, open(os.path.join(a.home, "gerente.json"), "w"))
            _usuario_falou(a.home, 60)  # prompt antigo: a regra do ticket 86 não adiaria
            assert orq_mod.avisa_fila_e2e(_fila_presa(fila))
            assert orq_mod.avisa_coordenador("term_c", "orq: wake de teste", minutos=orq_mod.WAKE_OCIOSO_MIN) == "adiado"
            assert orq_mod.avisos_entregar() == []
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig
    ctx = json.loads(a.prompt("e agora?").stdout)["hookSpecificOutput"]["additionalContext"]
    assert "PRESA" in ctx and "wake de teste" in ctx, ctx


def test_ticket107_with_away_mode_the_notice_is_typed_keeping_the_old_guards():
    with tempfile.TemporaryDirectory() as home:
        antes, dig = orq_mod.HOME, orq_mod.digita
        enviados = []
        orq_mod.HOME, orq_mod.digita = home, lambda h, t: enviados.append(t) or "enviado"
        try:
            orq_mod.ausente_ligar()
            _usuario_falou(home, 1)
            assert orq_mod.avisa_coordenador("term_c", "orq: a") == "adiado" and enviados == []
            os.remove(os.path.join(home, "events.jsonl"))
            assert orq_mod.avisa_coordenador("term_c", "orq: b") == "enviado" and enviados == ["orq: b"]
        finally:
            orq_mod.HOME, orq_mod.digita = antes, dig


# ---- ticket 115: ambientes (branches) declarados por projeto ----

def _repo_remoto(a, padrao):
    """Um repo git com origin/HEAD apontando para `padrao`; devolve a pasta."""
    d = os.path.join(a.tmp.name, "repo-" + padrao)
    os.makedirs(d)
    g = lambda *x: subprocess.run(["git", "-C", d, *x], check=True, capture_output=True, env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                                                                                         "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"})
    g("init", "-q", "-b", padrao)
    g("commit", "-q", "--allow-empty", "-m", "x")
    g("update-ref", f"refs/remotes/origin/{padrao}", "HEAD")
    g("symbolic-ref", "refs/remotes/origin/HEAD", f"refs/remotes/origin/{padrao}")
    return d


def _status_pr(a, cwd=None):
    (linha,) = [x for x in a.orq("status", cwd=cwd).stdout.splitlines() if x.startswith("PR task_")]
    return linha


def _entrou(a, url, base, cwd=None):
    _pr(a, url, "MERGED", base)
    assert a.orq("pr", "ligar", "task_feat1", url, cwd=cwd).returncode == 0
    assert a.orq("pr", "poll", "--forcar", cwd=cwd).returncode == 0


def test_ticket115_projeto_com_dev_e_main_diz_pronto_para_main_depois_do_pr_de_dev():
    a = Amb(run="run_a")
    _gh(a)
    _projeto(a, "app", {"repo": f"path:{os.getcwd()}", "ambientes": [{"branch": "dev"}, {"branch": "main", "producao": True}], "fluxo": "promocao"})
    _entrou(a, PR1, "dev")
    linha = _status_pr(a)
    assert "pronto para main (dev entrou: abrir o de main)" in linha and "staging" not in linha, linha
    _entrou(a, PR2, "main")
    assert "em main" in _status_pr(a) and "pronto para" not in _status_pr(a)


def test_ticket115_projeto_so_com_main_e_fluxo_direto_nao_pede_pr_de_outro_ambiente():
    a = Amb(run="run_a")
    _gh(a)
    _projeto(a, "app", {"repo": f"path:{os.getcwd()}", "ambientes": [{"branch": "main", "producao": True}], "fluxo": "direto"})
    _entrou(a, PR1, "release")  # um PR para outra base não promove nada
    assert "pronto para" not in _status_pr(a), _status_pr(a)
    _entrou(a, PR2, "main")
    assert "em main" in _status_pr(a) and "pronto para" not in _status_pr(a)


def test_ticket115_fluxo_direto_com_ambientes_declarados_so_conta_a_producao():
    a = Amb(run="run_a")
    _gh(a)
    _projeto(a, "app", {"repo": f"path:{os.getcwd()}", "ambientes": [{"branch": "development"}, {"branch": "main", "producao": True}], "fluxo": "direto"})
    _entrou(a, PR1, "development")
    assert "pronto para" not in _status_pr(a), _status_pr(a)


def test_ticket115_projeto_sem_bloco_usa_a_branch_padrao_do_remoto_com_fluxo_direto():
    a = Amb(run="run_a")
    _gh(a)
    d = _repo_remoto(a, "trunk")
    _projeto(a, "app", {"repo": f"path:{d}"})
    _entrou(a, PR1, "development", cwd=d)
    assert "pronto para" not in _status_pr(a, d), "sem bloco só a branch padrão do remoto conta: " + _status_pr(a, d)
    _entrou(a, PR2, "trunk", cwd=d)
    assert "em trunk" in _status_pr(a, d), _status_pr(a, d)


def test_ticket115_projeto_de_tres_ambientes_segue_com_os_textos_de_hoje():
    a = _prs_env()
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "development")
    a.orq("pr", "poll", "--forcar")
    assert "#1216 development ✓ → pronto para staging" in _linha_pr(a), _linha_pr(a)
    _entrou(a, PR2, "staging")
    assert "pronto para main (development e staging entraram: abrir o de main)" in _linha_pr(a), _linha_pr(a)
    _, h = _html_com_pr(a)
    assert '<span><span class="dot d"></span> development</span><span><span class="dot s"></span> staging</span><span><span class="dot p"></span> main</span>' in h
    assert "Dentro de cada passo, development antes de staging." in h


def _html_com_pr(a):
    r = a.orq("digest", "--html")
    assert r.returncode == 0, r.stderr
    (caminho,) = [l for l in r.stdout.splitlines() if l.endswith(".html")]
    return r, open(caminho).read()


def test_ticket115_digest_de_projeto_dev_main_leva_a_legenda_do_projeto():
    a = Amb(run="run_a")
    _gh(a)
    _projeto(a, "app", {"repo": f"path:{os.getcwd()}", "ambientes": [{"branch": "dev"}, {"branch": "main", "producao": True}]})
    _entrou(a, PR1, "dev")
    _, h = _html_com_pr(a)
    assert '<span class="dot d"></span> dev</span><span><span class="dot p"></span> main</span>' in h and "staging" not in h and "Dentro de cada passo" not in h, h[h.index("legend"):][:300]


def test_ticket115_projetos_valida_os_ambientes_e_lista_o_fluxo():
    a = Amb(run="run_a")
    _projeto(a, "bom", {"repo": "path:/r/bom", "ambientes": [{"branch": "dev"}, {"branch": "main", "producao": True}]})
    _projeto(a, "malformado", {"repo": "path:/r/m", "ambientes": ["dev"]})
    _projeto(a, "duas-producoes", {"repo": "path:/r/d", "ambientes": [{"branch": "a", "producao": True}, {"branch": "b", "producao": True}]})
    _projeto(a, "fluxo-ruim", {"repo": "path:/r/f", "fluxo": "cascata"})
    out = a.orq("projetos").stdout.splitlines()
    assert any(l.startswith("bom") and "ambientes dev > main (promocao, produção main)" in l and "inválido" not in l for l in out), out
    for nome, motivo in (("malformado", "ambientes"), ("duas-producoes", "produção"), ("fluxo-ruim", "cascata")):
        assert any(l.startswith(nome) and "inválido" in l and motivo in l for l in out), (nome, out)
    js = {p["nome"]: p for p in json.loads(a.orq("projetos", "--json").stdout)}
    assert js["bom"]["ambientes"] == ["dev", "main"] and js["bom"]["producao"] == "main" and js["bom"]["fluxo"] == "promocao", js["bom"]


def test_ticket115_worktree_nova_nasce_da_producao_do_projeto_que_declara_ambientes():
    a = Amb(run="run_a")
    _projeto(a, "app", {"repo": "path:/r/app", "ambientes": [{"branch": "dev"}, {"branch": "release", "producao": True}]})
    _projeto(a, "sem-bloco", {"repo": "path:/r/sem"})
    assert _despachar(a, "--projeto", "app").returncode == 0
    assert _despachar(a, "--projeto", "app", "--base-branch", "origin/dev").returncode == 0
    assert _despachar(a, "--projeto", "sem-bloco").returncode == 0
    base = lambda arg: arg[arg.index("--base-branch") + 1] if "--base-branch" in arg else None
    assert [base(x) for x in _log(a, "started.log")] == ["origin/release", "origin/dev", None]


def _vira_mergeado(a, url, base):
    """Liga o PR aberto e só depois o dá como mergeado: a entrada e as obrigações nascem na passagem do poll."""
    _pr(a, url, "OPEN", base, headRefName="fix/x")
    assert a.orq("pr", "ligar", "task_feat1", url).returncode == 0
    _pr(a, url, "MERGED", base, headRefName="fix/x")
    assert a.orq("pr", "poll", "--forcar").returncode == 0


def _projeto_dev_trunk(a):
    _projeto(a, "app", {"repo": f"path:{os.getcwd()}", "ambientes": [{"branch": "dev"}, {"branch": "trunk", "producao": True}], "fluxo": "promocao"})


def test_ticket115_obrigacoes_do_merge_leem_a_producao_e_os_ambientes_do_projeto_e_nao_main():
    a = Amb(run="run_a")
    _gh(a)
    _limpar_falso(a)
    _projeto_dev_trunk(a)
    _vira_mergeado(a, PR1, "dev")
    textos = [o["texto"] for o in orq_mod.obrigacoes_abertas(a.events())]
    assert "conferir o deploy de dev" in textos and any("trunk" in t for t in textos), textos
    assert not any("produção" in t for t in textos), "dev não é a produção do projeto"
    _vira_mergeado(a, PR2, "trunk")
    textos = [o["texto"] for o in orq_mod.obrigacoes_abertas(a.events())]
    assert "conferir o deploy de produção (quave-one)" in textos and "conferir que a branch e a worktree saíram" in textos, textos


def test_ticket115_um_projeto_cuja_producao_e_main_em_tres_ambientes_pede_o_deploy_de_producao_so_em_main():
    a = _prs_env()
    _limpar_falso(a)
    a.orq("pr", "ligar", "task_feat1", PR1)
    _pr(a, PR1, "MERGED", "main", headRefName="fix/x")
    assert a.orq("pr", "poll", "--forcar").returncode == 0
    assert "deploy" in _obrig(a) and "produção" in _obrig(a)["deploy"] and "ticket" not in _obrig(a), _obrig(a)


def test_ticket115_o_merge_na_producao_do_projeto_dispara_a_limpeza_e_o_merge_em_outro_ambiente_nao():
    a = Amb(run="run_a")
    _gh(a)
    args = _limpar_falso(a)
    _projeto_dev_trunk(a)
    _vira_mergeado(a, PR1, "dev")
    time.sleep(0.5)
    assert not os.path.exists(args), "dev não encerra a branch"
    _vira_mergeado(a, PR2, "trunk")
    assert _espera_arquivo(args), "o merge em trunk (a produção do projeto) tem de chamar a limpeza"


def test_ticket115_fluxo_diz_a_producao_e_os_ambientes_do_projeto_que_contem_o_repo():
    a = Amb(run="run_a")
    _projeto_dev_trunk(a)
    fx = json.loads(a.orq("fluxo", "--repo", os.getcwd(), "--json").stdout)
    assert (fx["producao"], fx["ambientes"], fx["declarado"]) == ("trunk", ["dev", "trunk"], True), fx
    d = _repo_remoto(a, "padrao-x")
    fx = json.loads(a.orq("fluxo", "--repo", d, "--json").stdout)
    assert (fx["producao"], fx["ambientes"], fx["declarado"]) == ("padrao-x", ["padrao-x"], False), fx


def test_ticket115_o_limpar_mergeados_le_a_producao_do_projeto_e_nao_fixa_main():
    import importlib.util
    spec = importlib.util.spec_from_file_location("limpar_mergeados", os.path.join(AQUI, "scripts", "limpar-mergeados.py"))
    lm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lm)
    a = Amb(run="run_a")
    _projeto_dev_trunk(a)
    antes = {k: os.environ.get(k) for k in ("ORQ_HOME", "ORQ_FINAL_BASE", "ORQ_PROTECTED_BRANCHES")}
    os.environ["ORQ_HOME"] = a.home
    os.environ.pop("ORQ_FINAL_BASE", None)
    os.environ.pop("ORQ_PROTECTED_BRANCHES", None)
    lm.ORQ = os.path.join(AQUI, "orq.py")
    try:
        fx = lm.fluxo_do_repo(os.getcwd())
        assert (fx["producao"], fx["ambientes"]) == ("trunk", ["dev", "trunk"]), fx
        assert lm.is_final({"baseRefName": "trunk"}, "feat/x", fx) and not lm.is_final({"baseRefName": "main"}, "feat/x", fx)
        assert lm.is_final({"baseRefName": "dev"}, "merge/feat-dev", fx) and not lm.is_final({"baseRefName": "dev"}, "feat/x", fx)
        os.environ["ORQ_FINAL_BASE"] = "release"
        assert lm.fluxo_do_repo(os.getcwd())["producao"] == "release", "a variável força a base final"
    finally:
        for k, v in antes.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
    codigo = open(os.path.join(AQUI, "scripts", "limpar-mergeados.py")).read().split("def self_test():")[0]
    assert not re.search(r"""["'](?:main|development|staging)["']""", codigo.split("def self_test_git")[0]), "o script lê os ambientes do projeto"


def test_ticket115_nenhum_nome_de_ambiente_fica_fixo_no_codigo():
    codigo = open(os.path.join(AQUI, "orqlib.py")).read()
    assert not re.search(r"development|staging|AMBIENTES|DIGEST_BASE", codigo), re.findall(r".*(?:development|staging|AMBIENTES|DIGEST_BASE).*", codigo)
    fixos = [l for l in codigo.splitlines() if re.search(r"""["']main["']""", l) and not l.startswith("BRANCH_SEM_REMOTO")]
    assert not fixos, fixos  # a branch padrão sem remoto é uma constante só


# ---------- ticket 126: com o away ligado, decisão vira pendência e o coordenador segue com o desbloqueado ----------

def _stop126(a, **ev):
    r = a.orq("hook", "stop", stdin=json.dumps({"session_id": "abcdef123456", **ev}))
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout) if r.stdout.strip() else {}


def test_ticket126_com_away_o_hook_nega_askuserquestion_e_manda_para_orq_pend_add():
    a = Amb(run="run_a")
    assert _guard(a).stdout == "", "away desligado: a caixa passa"
    a.orq("away", "on")
    out = json.loads(_guard(a).stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny" and "away ligado" in out["permissionDecisionReason"] and "orq pend add --tipo decisao" in out["permissionDecisionReason"], out
    assert _guard(a, tool="Bash").stdout == "", "só o AskUserQuestion"
    a.orq("away", "off")
    assert _guard(a).stdout == "", "desligou: a caixa volta a passar"


def test_ticket126_perguntar_com_away_grava_a_pendencia_com_o_link_e_volta_sem_esperar():
    a = Amb()
    a.orq("away", "on")
    t = time.time()
    r = _perguntar(a, None, FAKE_POLL="/nao/existe", ORQ_PERGUNTAR_MIN="0.5")  # um poll que fosse chamado quebraria ou esperaria 30 s
    assert r.returncode == 0, r.stderr
    saida = json.loads(r.stdout)
    assert time.time() - t < 10 and saida["efeito"] == "aberta" and any("away" in x for x in saida["avisos"]), saida
    (item,) = [i for i in json.load(open(a.env["ORQ_PENDENCIAS"]))["itens"] if i["id"] == "badge"]
    assert item["tipo"] == "decisao" and item["link"] == "http://127.0.0.1:4387/session/abc", item
    assert not os.path.exists(os.path.join(a.home, "perguntar", "badge.poll")), "não esperou o poll"


def test_ticket126_stop_com_away_bloqueia_com_ticket_ready_e_vaga_e_cita_o_ticket():
    a = Amb(run="run_a")
    _tk105(a, "88", "Passagem escrita", task="task_88", extra=MODELO105)
    assert _stop126(a) == {}, "away desligado: nunca bloqueia por isso"
    a.orq("away", "on")
    out = _stop126(a)
    assert out["decision"] == "block" and "88" in out["reason"] and "orq despachar --ticket 88" in out["reason"], out


def test_ticket126_stop_com_away_deixa_parar_sem_trabalho_desbloqueado():
    a = Amb(run="run_a")
    a.orq("away", "on")
    assert _stop126(a) == {}
    _tk105(a, "93", "Painel de passagem", task="task_93", extra=MODELO105)  # P3 nunca sobe sozinho
    _tk105(a, "91", "Sem modelo", task="task_91")
    _tk105(a, "90", "Bloqueado", bloqueado="93", task="task_90", extra=MODELO105)
    _tk105(a, "89", "Em andamento", status="claimed", task="task_89", extra=MODELO105)
    assert _stop126(a) == {}


def test_ticket126_stop_nao_prende_o_coordenador_no_mesmo_bloqueio_para_sempre():
    a = Amb(run="run_a")
    _tk105(a, "88", "Passagem escrita", task="task_88", extra=MODELO105)
    a.orq("away", "on")
    blocos = [bool(_stop126(a).get("decision")) for _ in range(orq_mod.AWAY_BLOQUEIOS + 2)]
    assert blocos == [True] * orq_mod.AWAY_BLOQUEIOS + [False] * 2, blocos


def _tk126(num, estado="ready-for-agent", modelo="claude-sonnet-5-5", bloqueado=(), task="t"):
    return {"num": num, "titulo": f"ticket {num}", "status": estado, "blocked_by": list(bloqueado), "task": task, "run": "run_a", "modelo": modelo, "effort": "medium"}


def test_ticket126_proximo_sem_usuario_entrega_sem_integrar_e_ciclo_sem_push():
    ag = {"dispatch": "d1", "task": "t1", "estado": "entregue"}
    ev = [{"tipo": "despacho", "dispatch": "d1", "task": "t1", "ticket": "50"}]
    prox = lambda **k: orq_mod.proximo_sem_usuario(**{"tks": [_tk126("50", "claimed")], "ags": [ag], "integracao": {}, "fila": [], "events": ev, "cfg": orq_mod.maquina_cfg(), "sem_push": 0, **k})
    assert "50" in prox() and "orq integrar fila add" in prox(), "entregue, ticket aberto e fora da fila do integrador"
    assert prox(integracao={"50": {"ticket": "50"}}) is None, "já espera o integrador"
    assert prox(tks=[_tk126("50", "resolved")]) is None, "ticket fechado: integrado"
    assert prox(ags=[{**ag, "estado": "liberado"}]) is None
    ciclo = ev + [{"tipo": "ciclo", "dispatch": "dI", "hash": "abc1234"}]
    assert prox(ags=[], events=ciclo, sem_push=2) and "abc1234" in prox(ags=[], events=ciclo, sem_push=2) and "push" in prox(ags=[], events=ciclo, sem_push=2)
    assert prox(ags=[], events=ciclo, sem_push=0) is None and prox(ags=[], events=ciclo, sem_push=None) is None, "sem commit a enviar, ou sem saber: deixa parar"
    assert prox(ags=[], events=ev, sem_push=2) is None, "sem ciclo do integrador não há o que auditar"


def test_ticket126_proximo_sem_usuario_ticket_ready_pede_prioridade_modelo_e_vaga():
    base = {"ags": [], "integracao": {}, "events": [], "sem_push": 0, "cfg": {**orq_mod.maquina_cfg(), "max_workers": 2}}
    prox = lambda tks, **k: orq_mod.proximo_sem_usuario(tks=tks, fila=[], **{**base, **k})
    assert "orq despachar --ticket 07" in prox([_tk126("07")])
    assert prox([_tk126("07", bloqueado=["06"])]) is None and prox([_tk126("07", modelo=None)]) is None
    cheio = [{"dispatch": f"d{i}", "estado": "rodando", "modelo": "claude-sonnet-5-5"} for i in range(2)]
    assert prox([_tk126("07")], ags=cheio) is None, "sem slot livre"
    hib = [{**x, "estado": "hibernado"} for x in cheio]
    assert prox([_tk126("07")], ags=hib), "worker hibernado não ocupa slot"
    assert orq_mod.proximo_sem_usuario(tks=[_tk126("07")], fila=[{"ticket": "07"}], **base) is None, "já está na fila de despacho"


def test_ticket126_maquina_ocupacao_nao_conta_o_worker_hibernado():
    antes = (orq_mod._terminais_vivos, orq_mod._workers_todos, orq_mod._hibernados)
    try:
        orq_mod._terminais_vivos = lambda: None  # sem lista de terminais todo `dispatched` contava como vivo
        orq_mod._workers_todos = lambda: [{"dispatchId": "dH", "dispatchStatus": "dispatched", "agentTerminalHandle": "term_h"},
                                          {"dispatchId": "dV", "dispatchStatus": "dispatched", "agentTerminalHandle": "term_v"}]
        orq_mod._hibernados = lambda: {"dH": {"desde": "x"}}
        orq_mod._detalhes = lambda faltam: {w["dispatchId"]: {"modelo": "claude-sonnet-5-5"} for w in faltam}
        assert set(orq_mod.maquina_ocupacao()["vivos"]) == {"dV"}
    finally:
        orq_mod._terminais_vivos, orq_mod._workers_todos, orq_mod._hibernados = antes


def test_ticket126_away_off_lista_as_pendencias_abertas_na_ausencia_decisoes_primeiro_com_o_link():
    a = Amb(run="run_a")
    a.orq("away", "on")
    a.orq("pend", "add", "--id", "aviso-1", "--tipo", "avisar", "--titulo", "Avisar o time")
    r = a.orq("pend", "add", "--id", "badge", "--tipo", "decisao", "--titulo", "Qual badge?", "--link", "http://127.0.0.1:4387/session/abc")
    assert r.returncode == 0, r.stderr
    a.orq("pend", "add", "--id", "feita", "--tipo", "decisao", "--titulo", "Já respondida")
    a.orq("pend", "done", "feita")
    out = a.orq("away", "off").stdout
    assert "away mode desligado" in out and "badge" in out and "http://127.0.0.1:4387/session/abc" in out and "aviso-1" in out, out
    assert out.index("badge") < out.index("aviso-1"), "decisões primeiro"
    assert "freio-prod" not in out and "feita" not in out, "só o aberto desde que ligou"
    assert "pendência" not in a.orq("away", "off").stdout, "desligado de novo: sem lista"


# ---- ticket 125: orq projeto add registra no Orca com o orca.yaml gerado; o mate roda no projeto do Orca

def _repo_git_de_projeto(a, nome="app", arquivos=()):
    pasta = os.path.join(a.tmp.name, nome)
    os.makedirs(pasta)
    subprocess.run(["git", "init", "-q", "-b", "main", pasta], check=True)
    for arq in arquivos:
        os.makedirs(os.path.dirname(os.path.join(pasta, arq)), exist_ok=True)
        open(os.path.join(pasta, arq), "w").write("")
    return os.path.realpath(pasta)


def _chamadas_repo(a):
    return [c[:-1] for c in _log(a, "calls.log") if c[0] in ("add", "set-base-ref")]  # sem o --json que o orca() acrescenta


TRUST_CLAUDE = "python3 ~/.claude/scripts/trust-cwd.py"


def test_ticket125_add_sem_registro_registra_no_orca_grava_o_projeto_e_o_orca_yaml_so_com_o_que_o_repo_tem():
    a = Amb()
    repo = _repo_git_de_projeto(a, arquivos=["package-lock.json"])
    r = a.orq("projeto", "add", repo)
    assert r.returncode == 0, r.stderr
    assert _chamadas_repo(a) == [["add", "--path", repo], ["set-base-ref", "--repo", f"path:{repo}", "--ref", "origin/main"]], _chamadas_repo(a)
    assert json.load(open(os.path.join(a.home, "projects", "app.json")))["repo"] == f"path:{repo}"
    y = open(os.path.join(repo, "orca.yaml")).read()
    assert TRUST_CLAUDE in y and "npm ci" in y and ".scratch" in y, y
    # só package-lock.json: sem Meteor, E2E, graphify nem o script de setup de worktree
    for ausente in ("meteor", ".meteor", "e2e", "graphify", "setup-worktree", "docker"):
        assert ausente not in y, (ausente, y)
    assert orq_mod.orca_yaml_ler(y)[0]["setup"][0] == TRUST_CLAUDE


def test_ticket125_cada_bloco_entra_so_pelo_marcador_que_o_repo_tem():
    a = Amb()
    tudo = _repo_git_de_projeto(a, "tudo", ["pnpm-lock.yaml", "scripts/setup-worktree.sh", "graphify-out/graph.json", "web/.meteor/release", "docker-compose.e2e.yml"])
    open(os.path.join(tudo, "scripts", "e2e-infra.sh"), "w").write("case $1 in\n  destroy) :;;\nesac\n")
    y = orq_mod.orca_yaml_montar(tudo, "claude", {})[0]
    assert "pnpm install" in y and "sh scripts/setup-worktree.sh" in y and "graphify update" in y, y
    assert "rm -rf web/.meteor/local web/_build" in y and "sh scripts/e2e-infra.sh destroy" in y, y
    nada = _repo_git_de_projeto(a, "nada")
    y = orq_mod.orca_yaml_montar(nada, "claude", {})[0]
    assert TRUST_CLAUDE in y and "install" not in y and "meteor" not in y and "e2e" not in y, y


def test_ticket125_projeto_pode_ligar_e_desligar_blocos_e_acrescentar_comandos():
    a = Amb()
    repo = _repo_git_de_projeto(a, arquivos=["package-lock.json", "web/.meteor/release"])
    cfg = {"blocos": {"meteor": False, "install": False, "graphify": True}, "setup_extra": ["make bootstrap"], "archive_extra": ["rm -rf .cache"]}
    y = orq_mod.orca_yaml_montar(repo, "claude", cfg)[0]
    assert "npm ci" not in y and "meteor" not in y and "graphify update" in y, y
    setup, archive = (orq_mod.orca_yaml_ler(y)[0][k] for k in ("setup", "archive"))
    assert setup[-1] == "make bootstrap" and archive[-1] == "rm -rf .cache", (setup, archive)
    for ruim in ({"blocos": {"docker": True}}, {"blocos": {"meteor": "sim"}}, {"setup_extra": "x"}, {"setup_extra": [1]}):
        try:
            orq_mod.orca_yaml_montar(repo, "claude", ruim)
        except ValueError:
            continue
        raise AssertionError(f"aceitou {ruim}")


def test_ticket125_orca_yaml_existente_nao_e_sobrescrito_e_mostra_o_diff():
    a = Amb()
    repo = _repo_git_de_projeto(a, arquivos=["package-lock.json"])
    antigo = "scripts:\n  setup: |\n    echo meu\n"
    open(os.path.join(repo, "orca.yaml"), "w").write(antigo)
    r = a.orq("projeto", "add", repo)
    assert r.returncode == 0, r.stderr
    assert open(os.path.join(repo, "orca.yaml")).read() == antigo
    assert "-    echo meu" in r.stdout and "+    npm ci" in r.stdout and "--substituir-orca-yaml" in r.stdout, r.stdout
    assert ["add", "--path", repo] in _chamadas_repo(a), "o registro no Orca segue valendo"
    r = a.orq("projeto", "add", repo, "--substituir-orca-yaml")
    assert r.returncode == 0 and "npm ci" in open(os.path.join(repo, "orca.yaml")).read()


def test_ticket125_a_parte_fixa_entra_sempre_inclusive_na_proposta_do_agente_e_no_codex():
    a = Amb()
    repo = _repo_git_de_projeto(a, arquivos=["package-lock.json"])
    prop = os.path.join(a.tmp.name, "prop.yaml")
    open(prop, "w").write("scripts:\n  setup: |\n    make deps\n  archive: |\n    make clean\n")
    r = a.orq("projeto", "add", repo, "--orca-yaml", prop, "--harness", "codex")
    assert r.returncode == 0, r.stderr
    setup, archive = (orq_mod.orca_yaml_ler(open(os.path.join(repo, "orca.yaml")).read())[0][k] for k in ("setup", "archive"))
    assert setup[0] == "orq projeto confiar" and setup[1].startswith("main=$(git worktree list") and setup[-1] == "make deps", setup
    assert archive[0].startswith("main=$(git worktree list") and archive[-1] == "make clean", archive
    assert "npm ci" not in "\n".join(setup), "a proposta do agente troca a detecção"
    assert json.load(open(os.path.join(a.home, "projects", "app.json")))["harness"] == "codex"


def test_ticket125_proposta_com_yaml_invalido_ou_chave_desconhecida_e_recusada_antes_de_gravar():
    a = Amb()
    repo = _repo_git_de_projeto(a, arquivos=["package-lock.json"])
    for i, ruim in enumerate(("scripts:\n  setup: echo a: b\n", "scripts:\n  setup: |\n    ok\nfoo: 1\n", "scripts:\n  teardown: |\n    x\n",
                              "scripts:\n\tsetup: x\n", "isto nao e yaml", "scripts:\n  setup:\n", "scripts:\n  setup: |\n    a\n  setup: |\n    b\n")):
        prop = os.path.join(a.tmp.name, f"p{i}.yaml")
        open(prop, "w").write(ruim)
        r = a.orq("projeto", "add", repo, "--orca-yaml", prop)
        assert r.returncode != 0, (ruim, r.stdout)
        assert not os.path.exists(os.path.join(repo, "orca.yaml")) and not os.path.exists(os.path.join(a.home, "projects")) and not _chamadas_repo(a), ruim


def test_ticket125_repo_ja_registrado_no_orca_nao_registra_de_novo():
    a = Amb()
    repo = _repo_git_de_projeto(a, arquivos=["package-lock.json"])
    a.set("repos.json", [{"id": "r9", "path": repo, "displayName": "app"}])
    assert a.orq("projeto", "add", repo).returncode == 0
    assert _chamadas_repo(a) == [], _chamadas_repo(a)


def test_ticket125_dry_run_mostra_o_orca_yaml_e_nao_grava_nem_registra_nada():
    a = Amb()
    repo = _repo_git_de_projeto(a, arquivos=["package-lock.json"])
    r = a.orq("projeto", "add", repo, "--dry-run")
    assert r.returncode == 0 and TRUST_CLAUDE in r.stdout and "npm ci" in r.stdout, r
    assert not os.path.exists(os.path.join(repo, "orca.yaml")) and not os.path.exists(os.path.join(a.home, "projects")) and not _chamadas_repo(a)


def test_ticket125_add_de_url_clona_e_pasta_que_nao_e_repo_ou_nome_de_outro_repo_recusa():
    a = Amb()
    origem = _repo_git_de_projeto(a, "origem", ["package-lock.json"])
    subprocess.run(["git", "-C", origem, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "x"], check=True)
    destino = os.path.join(a.tmp.name, "clone")
    r = a.orq("projeto", "add", f"file://{origem}", "--destino", destino, "--nome", "clonado")
    assert r.returncode == 0, r.stderr
    assert os.path.isfile(os.path.join(destino, "orca.yaml")) and json.load(open(os.path.join(a.home, "projects", "clonado.json")))["repo"] == f"path:{os.path.realpath(destino)}"
    solta = os.path.join(a.tmp.name, "solta")
    os.makedirs(solta)
    assert a.orq("projeto", "add", solta).returncode != 0
    outro = _repo_git_de_projeto(a, "outro")
    r = a.orq("projeto", "add", outro, "--nome", "clonado")
    assert r.returncode != 0 and "clonado" in r.stderr, r


def test_ticket125_mate_abrir_abre_o_terminal_no_projeto_do_grupo_registrado_no_orca():
    a = Amb()
    repo, outro = _repo_git_de_projeto(a, "p1"), _repo_git_de_projeto(a, "p2")
    a.set("repos.json", [{"id": "r1", "path": repo, "displayName": "p1"}, {"id": "r2", "path": outro, "displayName": "p2"}])
    a.set("terminals.json", ["term_coord"])
    a.set("screens.json", {"term_ret1": ["❯ ", "  ⏵⏵ bypass permissions on (shift+tab to cycle)"]})
    _grupo(a, projetos=[repo, outro])
    assert a.orq("mate", "abrir", "orq").returncode == 0
    cria = _log(a, "create.log")[0]
    assert cria[cria.index("--worktree") + 1] == f"path:{repo}", cria  # vários projetos: o primeiro
    assert cria[cria.index("--command") + 1].startswith(f"cd {repo}; "), cria
    a.set("terminals.json", ["term_coord"])
    a.set("screens.json", {"term_ret2": ["❯ ", "  ⏵⏵ bypass permissions on (shift+tab to cycle)"]})
    cur = _cursor(a)
    cur["mates"] = {}
    json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
    _grupo(a, projetos=[repo, outro], projeto_mate=outro)
    assert a.orq("mate", "abrir", "orq").returncode == 0
    cria = _log(a, "create.log")[1]
    assert cria[cria.index("--worktree") + 1] == f"path:{outro}", cria  # projeto_mate ganha


def test_ticket125_mate_abrir_sem_o_projeto_no_orca_segue_no_checkout_atual():
    a = Amb()
    repo = _repo_git_de_projeto(a, "p1")
    a.set("terminals.json", ["term_coord"])
    a.set("screens.json", {"term_ret1": ["❯ ", "  ⏵⏵ bypass permissions on (shift+tab to cycle)"]})
    _grupo(a, projetos=[repo])
    assert a.orq("mate", "abrir", "orq").returncode == 0
    assert "--worktree" not in _log(a, "create.log")[0]


# ---------- limpeza de PR fechado sem merge (ticket 104) ----------

def _fechado_env(**env):
    """Um origin bare, o checkout `repo` (main, development, staging), a branch feat/x com um commit empurrado e uma worktree dela com relatorio-final.md; o projeto aponta para o repo."""
    a = Amb(run="run_a", **{"ORQ_FECHADO_DIAS": "1", **env})
    a.env["ORQ_RELATORIOS"] = os.path.join(a.tmp.name, "relatorios")
    base = a.tmp.name
    a.origin, a.repo, a.wt = (os.path.join(base, n) for n in ("origin.git", "repo", "wt-x"))
    g = lambda *args, cwd=None: subprocess.run(["git", "-C", cwd or a.repo, "-c", "user.name=t", "-c", "user.email=t@t", *args], check=True, capture_output=True, text=True).stdout.strip()
    a.g = g
    subprocess.run(["git", "init", "-q", "--bare", a.origin], check=True)
    subprocess.run(["git", "clone", "-q", a.origin, a.repo], check=True, capture_output=True)
    g("checkout", "-q", "-b", "main")
    g("commit", "-q", "--allow-empty", "-m", "x")
    for amb in ("development", "staging"):
        g("branch", amb)
    g("push", "-q", "origin", "main", "development", "staging")
    g("worktree", "add", "-q", "-b", "feat/x", a.wt)
    os.makedirs(os.path.join(a.wt, ".scratch/x"))
    open(os.path.join(a.wt, ".scratch/x/relatorio-final.md"), "w").write("feito")
    g("commit", "-q", "--allow-empty", "-m", "trabalho", cwd=a.wt)
    g("push", "-q", "origin", "feat/x", cwd=a.wt)
    a.set("worktrees.json", [{"path": a.wt, "branch": "refs/heads/feat/x", "repo": a.repo}])
    os.makedirs(os.path.join(a.home, "projects"), exist_ok=True)
    with open(os.path.join(a.home, "projects", "p.json"), "w") as f:
        json.dump({"repo": f"path:{a.repo}", "ambientes": [{"branch": "development"}, {"branch": "staging"}, {"branch": "main", "producao": True}], "fluxo": "promocao"}, f)
    _gh(a)
    return a


def _fechados(a, *urls, estados=("CLOSED", "CLOSED"), head="feat/x"):
    for u in urls:
        _pr(a, u, "OPEN", "development", headRefName=head)
        assert a.orq("pr", "ligar", "task_feat1", u, cwd=a.repo).returncode == 0
    for u, est in zip(urls, estados):
        _pr(a, u, est, "development", headRefName=head)
    r = a.orq("pr", "poll", "--forcar", cwd=a.repo)
    assert r.returncode == 0
    return r.stdout


def _ramos(a):
    return {"local": "feat/x" in a.g("branch", "--list", "feat/x"), "remota": bool(a.g("ls-remote", "--heads", "origin", "feat/x"))}


def test_it_should_mark_the_task_closed_and_clean_it_after_the_grace_period():
    a = _fechado_env()
    _fechados(a, PR1, PR2)
    assert [e["op"] for e in a.events() if e["tipo"] == "pr" and e["op"] == "fechada"] == ["fechada"], "marca uma vez, quando o último PR fecha"
    assert a.orq("pr", "poll", "--forcar", cwd=a.repo).stdout.count("limpa") == 0 and _ramos(a) == {"local": True, "remota": True}, "dentro do prazo nada sai"
    r = a.orq("limpar", "--fechados", "--dry-run", cwd=a.repo)
    assert "limparia feat/x" in r.stdout and "#1216, #1220" in r.stdout and _ramos(a) == {"local": True, "remota": True} and os.path.isdir(a.wt), r
    r = a.orq("limpar", "--fechados", cwd=a.repo)
    assert r.returncode == 0 and "Restore branch" in r.stdout, r
    assert _ramos(a) == {"local": False, "remota": False} and not os.path.exists(a.wt)
    (rm,) = _log(a, "wtrm.log")
    assert "--run-hooks" in rm["args"] and rm["relatorios"] == ["wt-x-.scratch-x-relatorio-final.md"], "o relatório é guardado antes de tirar a worktree"
    (ev,) = [e for e in a.events() if e.get("op") == "limpou_fechado"]
    assert ev["removidos"] == ["local", "remota"] and "Restore branch" in ev["restaurar"], ev


def test_it_should_not_clean_a_task_with_an_open_pr():
    a = _fechado_env()
    _fechados(a, PR1, PR2, estados=("CLOSED", "OPEN"))
    r = a.orq("limpar", "--fechados", cwd=a.repo)
    assert "nenhuma task" in r.stdout and _ramos(a) == {"local": True, "remota": True} and os.path.isdir(a.wt), r
    assert not [e for e in a.events() if e.get("op") in ("fechada", "limpou_fechado")]


def test_it_should_never_touch_an_environment_branch_or_a_branch_with_an_open_pr():
    a = _fechado_env()
    _fechados(a, PR1, estados=("CLOSED",), head="development")
    assert "branch de ambiente" in a.orq("limpar", "--fechados", cwd=a.repo).stdout
    assert a.g("ls-remote", "--heads", "origin", "development") and "development" in a.g("branch", "--list", "development")
    b = _fechado_env()  # outro PR aberto usa a branch como head (outra task): nada sai
    _fechados(b, PR1, estados=("CLOSED",))
    _pr(b, PR2, "OPEN", "development", headRefName="feat/x")
    assert "tem PR aberto" in b.orq("limpar", "--fechados", cwd=b.repo).stdout and _ramos(b) == {"local": True, "remota": True}


def test_it_should_only_preview_the_automatic_cleanup_until_the_first_real_one():
    a = _fechado_env(ORQ_FECHADO_DIAS="0")
    out = _fechados(a, PR1, estados=("CLOSED",))
    assert "limparia feat/x" in out and _ramos(a) == {"local": True, "remota": True}, "a primeira vez é só prévia"
    assert "limparia" not in a.orq("pr", "poll", "--forcar", cwd=a.repo).stdout, "a prévia sai uma vez por task"
    assert a.orq("limpar", "--fechados", cwd=a.repo).returncode == 0 and _ramos(a) == {"local": False, "remota": False}
    assert os.path.exists(os.path.join(a.home, "limpar-fechados.json"))


# ---------- ticket 127: o mate ocioso dorme, o pedido o acorda ----------

AGORA127 = datetime(2026, 10, 1, 15, 0, 0, tzinfo=timezone.utc)


def _m127(fim_min=11, aberto_min=None, **kw):
    """O registro do mate no cursor: turno que acabou há `fim_min` min em relação a AGORA127 (None: sem turno)."""
    ts = lambda min_: (AGORA127 - timedelta(minutes=min_)).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    return {"terminal": "term_mate", "sessao": "sess-mate", "runs": ["run_m"],
            "turnos": [[ts(fim_min + 5), ts(fim_min)]] if fim_min is not None else [],
            **({"aberto_em": ts(aberto_min)} if aberto_min is not None else {}), **kw}


def test_ticket127_ocioso_so_com_turno_fechado_sem_pedido_e_sem_worker_vivo():
    sem = lambda: False  # noqa: E731
    com = lambda: True  # noqa: E731
    assert orq_mod.mate_situacao(_m127(11), AGORA127, [], sem) == "ocioso há 11 min"
    assert orq_mod.mate_situacao(_m127(9), AGORA127, [], sem) == "trabalhando", "turno fechado há menos de 10 min"
    assert orq_mod.mate_situacao(_m127(11), AGORA127, [{"corr": "p1"}], sem) == "trabalhando", "pedido aberto do coordenador"
    assert orq_mod.mate_situacao(_m127(11), AGORA127, [], com) == "trabalhando", "worker do Run dele vivo (ou entrega sem integrar)"
    aberto = _m127(11)
    aberto["turnos"][-1][1] = None
    assert orq_mod.mate_situacao(aberto, AGORA127, [], sem) == "trabalhando", "turno ainda aberto"
    assert orq_mod.mate_situacao(_m127(None), AGORA127, [], sem) == "trabalhando", "sem turno nem abertura gravados não há como medir"
    assert orq_mod.mate_situacao(_m127(40, aberto_min=3), AGORA127, [], sem) == "trabalhando", "o resume zera o relógio: o turno antigo não vale"
    assert orq_mod.mate_situacao(_m127(40, aberto_min=12), AGORA127, [], sem) == "ocioso há 12 min"
    assert orq_mod.mate_situacao(_m127(30, terminal=None, dormiu="2026-10-01T14:40:00Z"), AGORA127, [], sem) == "dormindo"
    assert orq_mod.mate_situacao(_m127(11), AGORA127, [], sem, minimo=15) == "trabalhando", "ORQ_MATE_OCIOSO_MIN"
    chamado = []
    assert orq_mod.mate_situacao(_m127(9), AGORA127, [], lambda: chamado.append(1)) == "trabalhando" and not chamado, "o worker só é consultado com o resto ocioso"


def _amb127(fim_min, prontos=(), grupo_cfg=True, **mate):
    """Um mate vivo com o turno fechado há `fim_min` min (relógio real), a tela ociosa e o coordenador no gerente.json; `prontos`: tickets `ready` do grupo."""
    a = Amb(run="run_m", **HIB60)
    _grupo(a)
    cur = {"mates": {"orq": {"terminal": "term_mate", "sessao": "sess-mate", "cwd": a.home, "runs": ["run_m"],
                             "turnos": [[_z60(fim_min + 5), _z60(fim_min)]], **mate}}}
    json.dump(cur, open(os.path.join(a.home, "cursor.json"), "w"))
    json.dump({"coordenador": "term_coord", "gerente": "term_ger", "runs": []}, open(os.path.join(a.home, "gerente.json"), "w"))
    a.set("terminals.json", ["term_coord", "term_ger", "term_mate"])
    a.set("screens.json", {"term_mate": _tela52("tela-claude-ocioso.txt"), "term_ret1": ["❯ ", "  ⏵⏵ bypass permissions on (shift+tab to cycle)"]})
    a.set("workers.json", [])
    os.makedirs(a.env["ORQ_ISSUES"], exist_ok=True)
    for n in prontos:
        with open(os.path.join(a.env["ORQ_ISSUES"], f"{n}-x.md"), "w") as f:
            f.write(f"# {n}: orq: ticket {n}\n\nStatus: ready\nBlocked by: (nenhum)\n\n## What to build\nx\n\n## Acceptance criteria\n- x\n")
    return a


def test_ticket127_ocioso_por_20_min_sem_ticket_pronto_o_gerente_hiberna_e_grava_mate_dormiu():
    a = _amb127(25)
    with EmProcesso(a):
        r = orq_mod.mates_dormir()
    assert r == ["mate orq: dormiu (ocioso há 25 min)"], r
    assert [c[c.index("--terminal") + 1] for c in _log(a, "close.log")] == ["term_mate"]
    m = _cursor(a)["mates"]["orq"]
    assert m["terminal"] is None and m["sessao"] == "sess-mate" and m["dormiu"], m
    ev = next(e for e in a.events() if e["tipo"] == "mate_dormiu")
    assert (ev["grupo"], ev["terminal"], ev["sessao"]) == ("orq", "term_mate", "sess-mate"), ev
    assert not _log(a, "create.log")
    with EmProcesso(a):
        assert orq_mod.mates_dormir() == [], "dormindo não dorme de novo"
    assert len(_log(a, "close.log")) == 1
    with EmProcesso(a):
        assert not any("caiu" in x for x in orq_mod.mate_volta()), "dormir de propósito não é queda"


def test_ticket127_ocioso_ha_menos_de_20_min_ou_com_tela_ocupada_nao_hiberna():
    a = _amb127(15)
    with EmProcesso(a):
        assert orq_mod.mates_dormir() == []
    assert not _log(a, "close.log") and _cursor(a)["mates"]["orq"]["terminal"] == "term_mate"
    b = _amb127(25)
    b.set("screens.json", {"term_mate": ["✻ Pensando… (esc to interrupt)"]})
    with EmProcesso(b):
        assert orq_mod.mates_dormir() == []
    assert not _log(b, "close.log"), "tela com spinner: a próxima volta confere de novo"


def test_ticket127_com_ticket_pronto_do_grupo_avisa_o_coordenador_uma_vez_e_nao_hiberna():
    a = _amb127(25, prontos=("104", "117"))
    with EmProcesso(a):
        r = orq_mod.mates_dormir()
        assert r == ["mate orq: ocioso há 25 min, coordenador avisado (104, 117 prontos)"], r
        assert orq_mod.mates_dormir() == [], "avisado uma vez por ociosidade"
    assert not _log(a, "close.log") and _cursor(a)["mates"]["orq"]["terminal"] == "term_mate"
    avisos = [x["texto"] for x in _cursor(a)["avisos"]]  # sem o modo ausente o aviso espera no contexto do próximo prompt do coordenador (ticket 82)
    assert len(avisos) == 1 and "mate orq ocioso, tem 104 e 117 prontos" in avisos[0], avisos
    # sem vaga livre na máquina o ticket pronto não ocupa o mate: ele dorme
    b = _amb127(25, prontos=("104",))
    json.dump({"max_workers": 0}, open(os.path.join(b.home, "maquina.json"), "w"))
    with EmProcesso(b):
        assert orq_mod.mates_dormir() == ["mate orq: dormiu (ocioso há 25 min)"]


def test_ticket127_worker_vivo_ou_pedido_aberto_do_mate_o_mantem_acordado():
    a = _amb127(25)
    a.set("workers.json", [_w48("term_w1", run="run_m", agente="claude", modelo="claude-opus-5-5")])
    a.set("tasks_run_m.json", [{"id": "task_term_w1", "task_title": "x", "status": "dispatched", "dispatch_id": "ctx_term_w1", "created_at": _iso(-3600)}])
    a.set("terminals.json", ["term_coord", "term_ger", "term_mate", "term_w1"])
    a.set("screens.json", {"term_mate": _tela52("tela-claude-ocioso.txt"), "term_w1": _tela52("tela-claude-ocioso.txt")})
    with EmProcesso(a):
        assert orq_mod.mates_dormir() == []
    assert not _log(a, "close.log")
    b = _amb127(25)
    b.orq("mate", "pedir", "orq", "--texto", "status", "--prazo", "0")
    with EmProcesso(b):
        # prazo 0 não espera resposta; com prazo o pedido sem resposta segura o mate
        assert orq_mod.mates_dormir() == ["mate orq: dormiu (ocioso há 25 min)"]
    c = _amb127(25)
    c.orq("mate", "pedir", "orq", "--texto", "status", "--prazo", "600")
    with EmProcesso(c):
        assert orq_mod.mates_dormir() == []
    assert not _log(c, "close.log")


def test_ticket127_mate_pedir_com_o_mate_dormindo_retoma_a_sessao_e_entrega_o_pedido():
    a = _amb127(25, terminal=None, dormiu=_z60(5))
    a.set("terminals.json", ["term_coord", "term_ger"])
    r = a.orq("mate", "pedir", "orq", "--texto", "despache o ticket 77")
    assert r.returncode == 0, r.stderr
    p = json.loads(r.stdout)
    assert p["corr"] == "p1" and p["entrega"] == "enviado", p
    cmd = _log(a, "create.log")[0]
    cmd = cmd[cmd.index("--command") + 1]
    assert cmd == f"cd {a.home}; ORQ_MATE=orq claude --resume sess-mate --model claude-sonnet-5-5 --dangerously-skip-permissions", cmd
    textos = [c[c.index("--text") + 1] for c in _log(a, "send.log") if "--text" in c]
    assert len(textos) == 2 and "dormiu" in textos[0] and textos[1].startswith("orq ▸ pedido p1") and "despache o ticket 77" in textos[1], textos
    m = _cursor(a)["mates"]["orq"]
    assert m["terminal"] == "term_ret1" and not m.get("dormiu") and m["aberto_em"], m
    assert any(e["tipo"] == "mate_acordou" and e["grupo"] == "orq" for e in a.events())
    # o relógio de ociosidade parte da abertura: o turno antigo (25 min) não põe o mate de volta para dormir
    with EmProcesso(a):
        assert orq_mod.mates_dormir() == []


def test_ticket127_retomar_nao_acorda_o_mate_que_dormiu_de_proposito():
    a = _amb127(25, terminal=None, dormiu=_z60(5))
    a.set("terminals.json", ["term_coord", "term_ger"])
    r = a.orq("retomar", "--dry-run")
    assert r.returncode == 0 and "a_retomar" not in r.stdout and not _log(a, "create.log"), (r.stdout, r.stderr)


def test_ticket127_grupos_e_status_mostram_trabalhando_ocioso_e_dormindo():
    a = _amb127(3)
    assert "mate trabalhando (term_mate" in a.orq("grupos").stdout
    assert "mate orq: trabalhando (term_mate)" in a.orq("status").stdout
    b = _amb127(12)
    assert "mate ocioso há 12 min (term_mate" in b.orq("grupos").stdout
    assert "mate orq: ocioso há 12 min (term_mate)" in b.orq("status").stdout
    c = _amb127(30, terminal=None, dormiu=_z60(4))
    c.set("terminals.json", ["term_coord", "term_ger"])
    assert "mate dormindo" in c.orq("grupos").stdout and "mate orq: dormindo" in c.orq("status").stdout
    assert "caiu" not in c.orq("status").stdout


def test_ticket127_orq_mate_dormir_a_mao_e_a_recusa_com_pedido_aberto():
    a = _amb127(1)
    r = a.orq("mate", "dormir", "orq")
    assert r.returncode == 0 and json.loads(r.stdout)["grupo"] == "orq", (r.stdout, r.stderr)
    assert [c[c.index("--terminal") + 1] for c in _log(a, "close.log")] == ["term_mate"]
    b = _amb127(1)
    b.orq("mate", "pedir", "orq", "--texto", "x", "--prazo", "600")
    r = b.orq("mate", "dormir", "orq")
    assert r.returncode != 0 and "pedido" in r.stderr and not _log(b, "close.log"), (r.stdout, r.stderr)


if __name__ == "__main__":
    filtro = sys.argv[1] if len(sys.argv) > 1 else ""
    testes = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f) and filtro in n]
    falhas = []
    for nome, fn in testes:
        try:
            fn()
            print(f"ok      {nome}")
        except Exception as e:  # noqa: BLE001 - o relatório mostra todas as falhas de uma vez
            falhas.append(nome)
            print(f"FALHOU  {nome}: {type(e).__name__}: {str(e)[-400:]!r}")
    print(f"{len(testes) - len(falhas)}/{len(testes)} testes passaram")
    sys.exit(1 if falhas else 0)

