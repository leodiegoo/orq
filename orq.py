#!/usr/bin/env python3
"""orq: registro de entradas do orquestrador (fatias 1, 3, 4, 6, 7, 8 e 9, e as correções dos reviews). Só stdlib. Desenho: docs/design.md

ORQ_PENDENCIAS troca o pendencias.json, ORQ_HOME troca o diretório de dados, ORQ_ORCA o binário do Orca, ORQ_LOG o log, ORQ_TRANSCRITOS a pasta dos transcritos do coordenador, ORQ_PROJETOS a pasta `projects` do Claude Code (transcritos dos workers), ORQ_NO_BG=1 desliga o refresh em segundo plano, ORQ_ISSUES a pasta dos tickets e ORQ_MAPA o mapa (desenho.md).
"""
import argparse
import contextlib
import fcntl
import glob
import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

HOME = os.environ.get("ORQ_HOME") or os.path.expanduser("~/.claude/orq")
ORCA = os.environ.get("ORQ_ORCA") or "orca"
LOG = os.environ.get("ORQ_LOG") or os.path.expanduser("~/.claude/logs/orq.log")
PEND = os.environ.get("ORQ_PENDENCIAS") or os.path.expanduser("~/.claude/dashboard/data/pendencias.json")
EFEITOS = ("tarefa", "steer", "pend", "decisao", "conversa", "descartado")
HOOK_TIMEOUT = 3
TIPOS_PEND = ("acao", "decisao", "avisar")
PEND_IDADE_DIAS = 14  # pendência sem mexer há mais de 14 dias sai da vista e vai para "Depois"
ID_HEADER = 12  # o header do AskUserQuestion aceita até 12 caracteres
SUSPEITA_S = 3  # resposta a menos de 3 s de uma notificação vista pelo hook de prompt
JANELA_ORCA_S = 5  # mensagem entregue pelo Orca a ±5 s da resposta (inbox do Orca)
JANELA_AUDITORIA_S = 2  # orq auditar-respostas: entrega a até 2 s da resposta, no transcrito
TRANSCRITOS = os.environ.get("ORQ_TRANSCRITOS") or os.path.expanduser("~/.claude/projects/" + re.sub(r"[^A-Za-z0-9]", "-", os.getcwd()))  # o Claude Code nomeia a pasta do projeto com o cwd, cada caractere fora de [A-Za-z0-9] vira "-"
PROJETOS = os.environ.get("ORQ_PROJETOS") or os.path.expanduser("~/.claude/projects")  # onde o Claude Code guarda o transcrito de cada sessão, inclusive a do worker (M9)
TRANSCRITO_DIAS = 3  # o orq liberar só procura o transcrito do worker entre os arquivos mexidos nos últimos 3 dias
LEITURA_INICIAL = 200_000  # bytes do começo do transcrito onde está o prompt do despacho
MAX_TENTATIVAS = 3  # falha transitória do ingest: a mensagem/run é tentada até 3 vezes antes de ser descartada
INICIO = "2026-09-29T15:00:00Z"  # ponto de partida da primeira execução do ingest: nada anterior vira entrada
MAX_ITENS = 20  # itens de ação por relatório; o resto vira um "ler" só
ALERTA_H = 24  # o alerta de scout fica no resumo por 24 h
ASK_GUARD_TTL = 10  # segundos que a lista de despachos ativos vale para o hook guard (o Orca leva ~130 ms por página)
PAGINAS_ATIVOS = 3  # o worker-list vem dos mais novos para os mais velhos: 300 despachos bastam para achar um ativo
SEM_LIGACAO = "term_00000000-0000-0000-0000-000000000000"  # handle que o Orca não conhece: sem Run ligado, o worker-list dá o escopo all
TRAVADO_S = 15 * 60  # dispatch rodando sem heartbeat há mais que isto está travado: aparece no resumo e no painel com o orq steer sugerido
ESPERA_TETO_S = 60 * 60  # heartbeat `esperando: <motivo>` sem `até HH:MM` vale por tanto tempo; depois disso o dispatch é travado por "espera vencida"
ESPERA_FASE = re.compile(r"^\s*esperando:\s*(.*?)(?:\s+até\s+(\d{1,2}):(\d{2}))?\s*$", re.I)
STEER_LEITURA_S = 90  # ajuste que o dispatch não leu (`read` no inbox do Orca ou o id no transcrito do worker) tanto tempo depois do envio, ou da última redigitação, é reentregue
STEER_TENTATIVAS = 3  # redigitações do aviso ao worker parado; sem leitura STEER_LEITURA_S depois da terceira, vira o alerta "steer não lido"
STEER_TRANSCRITO_BYTES = 4_000_000  # o fim do transcrito do worker onde se procura o id da mensagem do steer
STEER_JANELA_S = 30 * 60  # steer mais velho que isto sai do acompanhamento: o inbox de 200 mensagens já não o alcança
NAO_COMECOU_S = 120  # dispatch aberto sem nenhum turno registrado tanto tempo depois do despacho não começou (o worker-start que ficou sem Enter)
PARADO_S = 60  # o turno terminou há tanto tempo e nada mais veio: o worker parou no prompt (o limiar evita chamar de parado a folga entre dois turnos)
TURNOS = "turnos.json"  # {dispatch: {task, sessao, inicio, fim}}: o que os hooks prompt e stop do worker gravam, sem chamar o Orca
TURNOS_DIAS = 7  # turno mais velho que isto sai do turnos.json na próxima gravação
AGENTE_COM_HOOK = "claude"  # só o Claude Code roda os hooks do orq: de outro agente o orq não sabe se parou
CONTROLE_LINHAS = 5  # entradas do histórico de controle que o `orq agentes` mostra por dispatch (as mais novas)
ORDEM_AGENTES = {"travado": 0, "nao_comecou": 1, "parado": 2, "perguntando": 3, "rodando": 4, "entregue": 5, "liberado": 6}
ID_DISPATCH = re.compile(r"--dispatch-id (ctx_\w+)")  # no preâmbulo de despacho do Orca, nos comandos que o worker roda
ID_TASK = re.compile(r"Your task ID is: (task_\w+)")
HB_JANELA_S = 120  # aviso que chega logo depois de um lote de heartbeats absorvido encontra a caixa vazia: também é bloqueado
HB_LOTES = 4  # lotes de heartbeat seguidos que um só aviso confirma (o --ack devolve o próximo lote)
AVISO_RUN = re.compile(r"orchestration check --run (run_\w+)")
RESUMO_PEDIDO_S = 60  # a entrada do usuário mais nova que isso é o pedido do próprio `orq resumo`
ANDA = ("rodando", "perguntando", "travado", "parado", "nao_comecou")  # estados que aparecem em "Anda" do `orq resumo`
PAINEL_VIVO = "gerente-vivo"  # o painel do agent manager toca este arquivo a cada volta (painel-agent-manager.sh), fora do orq
PAINEL_PARADO_S = 60
GERENTE = "gerente.json"  # {coordenador, gerente, runs}: o coordenador fala com o Orca pelo terminal do agent manager
RUN_PARADO_MIN = float(os.environ.get("ORQ_RUN_PARADO_MIN") or 30)  # Run sem task aberta nem mensagem por tanto tempo sai do gerente
RUN_RECENTE_H = 24  # Run sem trabalho aberto só aparece no resumo até 24 h depois da última atividade; depois vai para o arquivo (`orq runs --todos`)
RUN_TESTE = re.compile(r"teste|descart[aá]vel", re.I)  # objetivo de Run de teste: nunca aparece por padrão
GERENTE_PRESO_S = float(os.environ.get("ORQ_GERENTE_PRESO_S") or 120)  # o painel fica no Run do aviso até o coordenador confirmar, ou por este prazo
_DESCONHECIDO = object()  # "ainda não perguntei ao Orca qual é o Run ligado"
MUTA_RUN = {"worker-start", "send", "check", "reply", "task-create", "task-update"}  # o Orca só aceita estes do terminal ligado ao Run do --run
AVISO_FINAL = re.compile(r"You have \d+ orchestration messages?\. Run `orca orchestration check --run run_\w+(?: --terminal [\w-]+)?`\.?\s*$")  # o aviso do Orca no fim do prompt (B26)  # o aviso do Orca cita o Run: "Run `orca orchestration check --run <r>`."
ESPERA_RELATORIO_S = 3600  # o Orca marca a automation de terminal como completed antes de o agente gravar o relatório: espera até 1 h pelo arquivo
ESCOLHA_LAVISH = ("escolha", "escolhida", "decidido", "manter", "trocar")  # disposicao de um item do Lavish que fecha a decisão (com resposta); o resto é resposta livre
ISSUES = os.environ.get("ORQ_ISSUES") or os.path.expanduser("~/.claude/orquestrador-plan/issues")  # um arquivo por ticket: NN-<slug>.md
MAPA = os.environ.get("ORQ_MAPA") or os.path.expanduser("~/.claude/orquestrador-plan/desenho.md")  # mapa do plano (opcional): Destino, Notes, Decisões até aqui, Não especificado; ver docs/design.md
STATUS_NOVO = "ready-for-agent"  # o papel de triagem de docs/agents/triage-labels.md: ticket completo, pronto para um agente
STATUS_ANDAMENTO = "claimed"  # o despachar --ticket põe este status: o ticket já tem worker (B24)
STATUS_FECHADO = "resolved"
LINHAS_SESSAO = 12  # o SessionStart injeta o orq status e os tickets abertos em até 12 linhas, contando a do caminho do mapa
FEITO_LAVISH = "feito"  # disposicao (ou resposta com `escolha`) de "já fiz": fecha a pendência de qualquer tipo
JA_FEZ = "ja-fez"  # id/header do item que lista as pendências já feitas, como o multiSelect do AskUserQuestion
CONVERSAR_LAVISH = "__conversar"  # a resposta que a página manda com a disposicao "conversar": adiamento, não texto do usuário
MARCA_LAVISH = "\n\nContext data:\n"  # o lavish-axi poll põe o data do queuePrompt depois disso, dentro do texto do prompt
_STRING_JSON = re.compile(r'"(?:[^"\\\n]|\\.)*"')
# o detector do próprio Orca (findOrcaDispatchPreambleStart): linha de abertura opcional, <pasted_content> opcional, e o preâmbulo;
# hosts sem a linha de abertura mandam o preâmbulo puro
DESPACHO_SEM_ABERTURA = re.compile(r"(?:<pasted_content\b[^>]*>\s*)?You are working inside Orca, a multi-agent IDE\.")
TITULOS_ACAO = re.compile(r"^#{1,6}\s*(?:\d+\.\s*)?(?:Itens de ação|O que fazer hoje|O que precisa de ação)\s*$", re.I)


# ---------- puras ----------

def origem(prompt):
    """usuario | notificacao | orca | comando | resumo | despacho."""
    p = (prompt or "").lstrip()
    if p.startswith("<task-notification"):
        return "notificacao"
    if re.match(r"You have \d+ orchestration", p):
        return "orca"
    if p.startswith(("<command-", "<local-command", "/compact")):
        return "comando"
    if p.startswith("This session is being continued"):
        return "resumo"
    if p.startswith("Please carry out this task from my Orca coordinator") or DESPACHO_SEM_ABERTURA.match(p):
        return "despacho"
    return "usuario"


def separa_aviso(prompt):
    """(texto sem o aviso do Orca no fim, se havia): o Orca digita o aviso na linha em que o usuário escrevia, e o resto pode ser digitação pela metade (B26)."""
    p = prompt or ""
    m = AVISO_FINAL.search(p)
    return (p[:m.start()].rstrip(), True) if m else (p, False)


def abertas(events):
    """Entradas sem nenhum intake com o mesmo id, na ordem em que entraram."""
    fechadas = {e.get("entrada") for e in events if e.get("tipo") == "intake"}
    return [e for e in events if e.get("tipo") == "entrada" and e.get("id") and e["id"] not in fechadas]


def marcadas(resposta, opcoes):
    """Separa a resposta de um multiSelect em (labels marcados, texto livre). O AskUserQuestion junta tudo com ', '.

    ponytail: casa por substring, do label mais longo ao mais curto; texto livre que contenha um label exato conta como marcado.
    """
    resto, achadas = resposta, []
    for o in sorted(opcoes, key=lambda o: -len(o.get("label") or "")):
        label = o.get("label") or ""
        if label and label in resto:
            achadas.append(label)
            resto = resto.replace(label, "", 1)
    return achadas, resto.strip(' ,"')


def suspeitas(events, agora):
    """Headers com resposta_suspeita ainda sem confirmação.

    Sai com uma resposta do mesmo header, com uma resposta de verdade da mesma sessão em outra pergunta (a pergunta refeita
    com outro header), com pend done do mesmo id, ou depois de ALERTA_H horas.
    """
    abertas_ = {}
    for e in events:
        t = e.get("tipo")
        if t == "resposta_suspeita":
            if not e.get("ts") or (agora - _dt(e["ts"])).total_seconds() < ALERTA_H * 3600:
                abertas_[e.get("header")] = e
        elif t == "resposta":
            for h, s in list(abertas_.items()):
                mesma_sessao = s.get("sessao") and s.get("sessao") == e.get("sessao") and (not s.get("ask") or s.get("ask") != e.get("ask"))
                if h == e.get("header") or mesma_sessao:
                    del abertas_[h]
        elif t == "pend" and e.get("op") == "done":
            abertas_.pop(e.get("pend"), None)
    return list(abertas_)


def so_heartbeats(msgs):
    """Há mensagem e todas são heartbeat. Tipo desconhecido, ausente ou item que não é objeto conta como não-heartbeat: na dúvida, passa."""
    return bool(msgs) and all(isinstance(m, dict) and m.get("type") == "heartbeat" for m in msgs)


def sinal_de_vida(m):
    """{msg, task, dispatch, fase, ts} de um heartbeat do Orca (a fase vem no payload)."""
    p = _payload(m)
    return {"msg": m.get("id"), "task": p.get("taskId"), "dispatch": p.get("dispatchId"), "fase": p.get("phase"), "ts": m.get("created_at")}


def espera_declarada(fase, ts):
    """(motivo, prazo) de um heartbeat `esperando: <motivo> [até HH:MM]`, ou None para fase comum.

    `HH:MM` é hora local, a próxima depois do heartbeat; sem ele o prazo é o heartbeat mais ESPERA_TETO_S.
    """
    m = ESPERA_FASE.match(fase or "")
    ts = _ts(ts) if isinstance(ts, str) else ts
    if not m or not ts:
        return None
    if m.group(2) is None:
        return m.group(1), ts + timedelta(seconds=ESPERA_TETO_S)
    local = ts.astimezone()
    prazo = local.replace(hour=int(m.group(2)) % 24, minute=int(m.group(3)) % 60, second=0, microsecond=0)
    return m.group(1), (prazo if prazo >= local else prazo + timedelta(days=1)).astimezone(timezone.utc)


def _vivo_ou_travado(fase, ts, idade, agora):
    """(estado, espera, motivo) de um dispatch aberto pelo último heartbeat: esperando declarado dentro do prazo não é travado; vencido, é "espera vencida"."""
    esp = espera_declarada(fase, ts)
    if esp and agora <= esp[1]:
        return "rodando", esp[0], None
    if esp:
        return "travado", None, "espera vencida"
    return ("travado" if idade is not None and idade > TRAVADO_S else "rodando"), None, None


def sinais_de_vida(events):
    """{dispatch: último heartbeat (msg, task, fase, ts, run)} a partir dos eventos heartbeat_absorvido (Run ligado) e heartbeat_visto (outro Run)."""
    out = {}
    for e in events:
        if e.get("tipo") in ("heartbeat_absorvido", "heartbeat_visto"):
            for h in e.get("heartbeats") or []:
                if isinstance(h, dict) and h.get("dispatch"):
                    out[h["dispatch"]] = {**h, "run": e.get("run")}
    return out


def _ts(x):
    """datetime de um carimbo (Orca ou ISO); None se faltar ou for ilegível."""
    try:
        return _dt(x) if x else None
    except (AttributeError, ValueError):
        return None


def _z(x):
    """O carimbo em ISO com Z, ou None."""
    d = _ts(x)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ") if d else None


def turno_do_dispatch(t, agente, desde, ultimo_hb, agora):
    """(turno, desde_quando) do que os hooks do worker dizem de um dispatch aberto.

    `nao_comecou`: nenhum turno registrado e nenhum heartbeat NAO_COMECOU_S depois do despacho (prova de vida vale mais que a falta de registro, como no
    worker que já rodava quando os hooks entraram); `parado`: o último turno terminou há PARADO_S ou mais e nenhum
    heartbeat veio depois; `aberto`: turno em andamento (ou terminado há pouco). `unknown` é o que não dá para afirmar (agente sem o hook do orq,
    agente que o Orca não informou, ainda dentro da janela): nunca vale como ocioso. `t` é {inicio, fim} do turnos.json; `desde` e `ultimo_hb` são datetimes.
    """
    if agente != AGENTE_COM_HOOK:
        return "unknown", None
    inicio, fim = _ts(_dict(t).get("inicio")), _ts(_dict(t).get("fim"))
    if not inicio:
        return ("nao_comecou", desde) if desde and not ultimo_hb and (agora - desde).total_seconds() >= NAO_COMECOU_S else ("unknown", None)
    if fim and fim >= inicio and not (ultimo_hb and ultimo_hb > fim):
        return ("parado", fim) if (agora - fim).total_seconds() >= PARADO_S else ("aberto", None)
    return "aberto", None


def _dispatch_da_msg(m):
    """O dispatch que mandou a mensagem: payload.dispatchId, senão o from_handle `dispatch:<id>`."""
    d = _payload(m).get("dispatchId")
    h = str(m.get("from_handle") or "")
    return d or (h[len("dispatch:"):] if h.startswith("dispatch:") else None)


def perguntas_abertas(msgs):
    """{dispatch: mensagem} de question/escalation de worker ainda sem resposta.

    Respondida é a que tem, depois dela, uma mensagem de outro remetente na mesma thread ou para o dispatch (ou para quem perguntou).
    """
    ms = sorted((m for m in msgs if isinstance(m, dict) and isinstance(m.get("sequence"), int)), key=lambda m: m["sequence"])
    out = {}
    for i, q in enumerate(ms):
        d = _dispatch_da_msg(q)
        if q.get("type") not in ("question", "escalation") or not d:
            continue
        resp = any(_dispatch_da_msg(m) != d and (m.get("thread_id") == q.get("id") or m.get("to_handle") in (f"dispatch:{d}", q.get("from_handle")))
                   for m in ms[i + 1:])
        if resp:
            out.pop(d, None)
        else:
            out[d] = q
    return out


def ultimos_sinais(events, msgs):
    """{dispatch: {fase, ts}} do heartbeat mais novo de cada dispatch: os heartbeat_absorvido do log e os heartbeats do inbox."""
    out = {d: {"fase": h.get("fase"), "ts": h.get("ts")} for d, h in sinais_de_vida(events).items()}
    for m in msgs:
        d = _dispatch_da_msg(m) if isinstance(m, dict) and m.get("type") == "heartbeat" else None
        if d:
            sinal = sinal_de_vida(m)
            novo, velho = _ts(sinal["ts"]), _ts((out.get(d) or {}).get("ts"))
            if novo and (not velho or novo > velho):
                out[d] = {"fase": sinal["fase"], "ts": sinal["ts"]}
    return out


def _sem_terminal(w, liberados, vivos):
    """Dispatch que já terminou e não tem mais terminal a liberar: passou por `orq liberar` (evento `liberar`) ou o terminal não está no `orca terminal list`.

    `vivos` None (o Orca não respondeu) não prova nada: só o evento vale.
    """
    return w.get("dispatchStatus") != "dispatched" and (w.get("dispatchId") in liberados or (vivos is not None and w.get("agentTerminalHandle") not in vivos))


def monta_agentes(workers, msgs, events, agora, detalhes=None, vivos=None, turnos=None):
    """Pura: uma linha por dispatch do worker-list, com o estado (rodando, travado, nao_comecou, parado, perguntando, entregue ou liberado).

    Dispatched sem pergunta aberta é `nao_comecou` ou `parado` quando os turnos dos hooks do worker dizem (turno_do_dispatch); senão `travado` quando o
    último heartbeat (ou, sem nenhum, o despacho) tem mais de TRAVADO_S; completed com o terminal released é `liberado`, o resto é `entregue`
    (worker_done dado, terminal ainda aberto). `detalhes` é {dispatch: titulo, modelo, desde, agente}; `vivos` são os handles do `orca terminal list`;
    `turnos` é o turnos.json (None: sem dado, o turno fica `unknown`).
    """
    sinais, perguntas, detalhes, humanos = ultimos_sinais(events, msgs), perguntas_abertas(msgs), detalhes or {}, _interacao_registrada(events)
    liberados = _liberados(events)
    avisos_entrega = {e.get("dispatch"): e["avisos"] for e in events if e.get("tipo") == "entrega" and e.get("avisos")}
    controles = {}
    for e in events:
        if e.get("tipo") == "controle" and e.get("resultado") != "iniciado":
            c = {k: e[k] for k in ("ts", "acao", "resultado", "dispatch", "novo_dispatch", "motivo", "nota") if e.get(k)}
            for d in {e.get("dispatch"), e.get("novo_dispatch")} - {None}:
                controles.setdefault(d, []).append(c)
    out = []
    for w in workers:
        d = w.get("dispatchId")
        det, sinal = detalhes.get(d) or {}, sinais.get(d) or {}
        ref = _ts(sinal.get("ts")) or _ts(det.get("desde"))  # o último heartbeat; sem nenhum, o despacho
        idade = int((agora - ref).total_seconds()) if ref else None
        t = _dict((turnos or {}).get(d))
        turno = "unknown"
        if w.get("dispatchStatus") == "dispatched":
            if turnos is not None:
                turno, quando = turno_do_dispatch(t, det.get("agente"), _ts(det.get("desde")), _ts(sinal.get("ts")), agora)
                if quando:
                    idade = int((agora - quando).total_seconds())
            estado, espera, motivo = _vivo_ou_travado(sinal.get("fase"), sinal.get("ts"), idade, agora)
            estado = "perguntando" if d in perguntas else turno if turno in ("nao_comecou", "parado") and not (espera or motivo) else estado  # espera declarada (dentro do prazo ou vencida) vale mais que o turno encerrado (M16)
        else:
            espera = motivo = None
            estado, idade = ("liberado" if w.get("terminalState") == "released" or _sem_terminal(w, liberados, vivos) else "entregue"), None
        ag = {"dispatch": d, "task": w.get("taskId"), "run": w.get("runId"), "titulo": det.get("titulo"), "modelo": det.get("modelo"),
              "terminal": w.get("agentTerminalHandle"), "estado": estado, "fase": sinal.get("fase"), "ultimo_heartbeat": _z(sinal.get("ts")),
              "desde": _z(det.get("desde")), "idade_s": idade, "agente": det.get("agente"), "turno": turno,
              "turno_inicio": _z(t.get("inicio")), "turno_fim": _z(t.get("fim"))}
        if espera:
            ag["espera"] = espera
        if motivo and estado == "travado":
            ag["motivo"] = motivo
        if avisos_entrega.get(d):
            ag["entrega"] = avisos_entrega[d]
        if controles.get(d):
            ag["controle"] = controles[d][-CONTROLE_LINHAS:]
        retido = _retencao(w, humanos)
        if estado == "entregue" and retido:
            ag["retido"] = retido
        out.append(ag)
    return sorted(out, key=lambda a: ORDEM_AGENTES[a["estado"]])


def reavalia(agentes_, events, agora, turnos=None):
    """Os agentes do cache do aberto.json com rodando/travado/nao_comecou/parado refeito pelo heartbeat mais novo do log e, com `turnos`, pelo turnos.json (o cache atrasa até um prompt)."""
    sinais = sinais_de_vida(events)
    liberados = _liberados(events) | {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("estado") == "released"}
    out = []
    for ag in agentes_:
        ag = dict(ag)
        if ag.get("estado") in ("entregue", "rodando", "travado", "nao_comecou", "parado") and ag.get("dispatch") in liberados:
            ag["estado"] = "liberado"  # o orq liberar depois do cache (M13); só existe liberar depois do worker_done, então vale mesmo com cache anterior a ele (B38)
        if ag.get("estado") in ("rodando", "travado", "nao_comecou", "parado"):
            h = sinais.get(ag.get("dispatch")) or {}
            if _ts(h.get("ts")) and (not _ts(ag.get("ultimo_heartbeat")) or _ts(h["ts"]) > _ts(ag["ultimo_heartbeat"])):
                ag["fase"], ag["ultimo_heartbeat"] = h.get("fase"), _z(h["ts"])
            ref = _ts(ag.get("ultimo_heartbeat")) or _ts(ag.get("desde"))
            ag["idade_s"] = int((agora - ref).total_seconds()) if ref else None
            ag["estado"], espera, motivo = _vivo_ou_travado(ag.get("fase"), ag.get("ultimo_heartbeat"), ag["idade_s"], agora)
            ag.pop("espera", None), ag.pop("motivo", None)
            if espera:
                ag["espera"] = espera
            if motivo:
                ag["motivo"] = motivo
            t = _dict(turnos.get(ag.get("dispatch"))) if turnos is not None else {"inicio": ag.get("turno_inicio"), "fim": ag.get("turno_fim")}
            ag["turno"], quando = turno_do_dispatch(t, ag.get("agente"), _ts(ag.get("desde")), _ts(ag.get("ultimo_heartbeat")), agora)
            if ag["turno"] in ("nao_comecou", "parado") and not (espera or motivo):  # quem espera de propósito (fila de E2E, CI) encerra o turno e não está parado; vencida, é travado (M16)
                ag["estado"], ag["idade_s"] = ag["turno"], int((agora - quando).total_seconds())
        out.append(ag)
    return out


def linha_vivos(events, aberto, agora=None, turnos=None):
    """'Vivos: task fase HH:MM, …' com o estado de cada dispatch rodando, travado, não começado, parado ou perguntando, e os entregues sem liberar; '' se não há nada.

    Usa os agentes do cache (aberto.json, fatia 8) com o estado refeito pelo log; cache sem `agentes` cai nos `andamento` com a última fase.
    """
    agora = agora or datetime.now(timezone.utc)
    if isinstance((aberto or {}).get("agentes"), list):
        ags = reavalia(aberto["agentes"], events, agora, turnos)
        vivos = [a for a in ags if a["estado"] in ("travado", "nao_comecou", "parado", "perguntando", "rodando")]
        sem_liberar = sum(a["estado"] == "entregue" and not a.get("retido") for a in ags)
        itens = []
        for a in vivos[:3]:
            nome = f"{(a.get('task') or '?')[:9]}… "
            itens.append(nome + (f"TRAVADO há {(a.get('idade_s') or 0) // 60} min" + (f" ({a['motivo']})" if a.get("motivo") else "") if a["estado"] == "travado"
                                 else f"NÃO COMEÇOU há {(a.get('idade_s') or 0) // 60} min" if a["estado"] == "nao_comecou"
                                 else f"parado no prompt há {(a.get('idade_s') or 0) // 60} min" if a["estado"] == "parado" else "pergunta" if a["estado"] == "perguntando"
                                 else f"{a.get('fase') or '?'} {_hora_local(a['ultimo_heartbeat'])}" if a.get("ultimo_heartbeat") else "sem heartbeat"))
        return ((" Vivos: " + ", ".join(itens) + (f" +{len(vivos) - 3}" if len(vivos) > 3 else "") + ".") if vivos else "") + \
            (f" Entregues sem liberar {sem_liberar} (orq agentes)." if sem_liberar else "")
    andamento = (aberto or {}).get("andamento") or []
    if not andamento:
        return ""
    sinais = sinais_de_vida(events)
    itens = []
    for a in andamento[:3]:
        h = sinais.get(a.get("dispatch"))
        itens.append(f"{(a.get('task') or '?')[:9]}… " + (f"{h.get('fase') or '?'} {_hora_local(h.get('ts'))}" if h else "sem heartbeat"))
    return " Vivos: " + ", ".join(itens) + (f" +{len(andamento) - 3}" if len(andamento) > 3 else "") + "."


def _cita(texto, n=40):
    t = " ".join((texto or "").split())
    return t if len(t) <= n else t[: n - 1] + "…"


def alertas_recentes(events, agora, agentes_=None):
    """Alertas das últimas ALERTA_H horas, um por (task, tipo): scout sem relatório e steer não lido.

    Some quando algo posterior o trata: `orq alerta visto <task>` (alerta_visto), o `orq liberar` do dispatch da task ou um intake que cita a task (B30).
    `agentes_` (já reavaliados): a task cujo agente está `liberado` também some, mesmo liberada fora do `orq liberar` (B39); o steer não lido some
    também com o worker `entregue` (o ajuste ficou sem sentido).
    """
    out = {}

    def trata(task, so=None):
        for k in [k for k in out if k[0] == task and so in (None, k[1])]:
            del out[k]

    for e in events:
        if e.get("tipo") == "alerta" and (not e.get("ts") or (agora - _dt(e["ts"])).total_seconds() < ALERTA_H * 3600):
            out[(e.get("task"), e.get("alerta"))] = e
        elif e.get("tipo") in ("alerta_visto", "liberar"):
            trata(e.get("task"))
        elif e.get("tipo") == "intake":
            trata(e.get("ref"))
    for a in agentes_ or []:
        if a.get("estado") == "liberado":
            trata(a.get("task"))
        elif a.get("estado") == "entregue":
            trata(a.get("task"), "steer_nao_lido")
    return list(out.values())


def steers_abertos(events, agora):
    """{msg_id: {steer, tentativas, ultima}} dos steers ainda sem fim (lido, encerrado ou alerta) e dentro de STEER_JANELA_S.

    `tentativas` são as redigitações do aviso (steer_reentrega); `ultima` é o instante do steer ou da última redigitação, de onde correm os STEER_LEITURA_S.
    """
    out = {}
    for e in events:
        m = e.get("msg_id")
        if e.get("tipo") == "steer" and m and _ts(e.get("ts")):
            out[m] = {"steer": e, "tentativas": 0, "ultima": _ts(e["ts"])}
        elif e.get("tipo") == "steer_reentrega" and m in out and _ts(e.get("ts")):
            out[m]["tentativas"] += 1
            out[m]["ultima"] = _ts(e["ts"])
        elif e.get("tipo") == "steer_fim" or (e.get("tipo") == "alerta" and e.get("alerta") == "steer_nao_lido"):
            out.pop(m, None)
    return {m: s for m, s in out.items() if (agora - _ts(s["steer"]["ts"])).total_seconds() < STEER_JANELA_S}


def _relatorios(abertas_):
    """Entradas de relatório em aberto agrupadas por fonte (todos os worker_done num grupo só), na ordem em que entraram."""
    grupos = {}
    for e in abertas_:
        if e.get("origem") in ("relatorio", "relatorio_worker"):
            grupos.setdefault("worker_done com relatório" if e["origem"] == "relatorio_worker" else e.get("fonte") or e["id"], []).append(e)
    return grupos


def livres(events, pendencias):
    """Headers de decisão ainda aberta cuja última resposta foi texto livre (não fecha nada): o usuário pode não ter decidido."""
    abertas_ = {i.get("id") for i in (pendencias or {}).get("itens", []) if i.get("tipo") == "decisao"}
    ult = {}
    for e in events:
        if e.get("tipo") in ("resposta", "resposta_lavish") and e.get("header") in abertas_:
            ult[e["header"]] = e.get("livre")
    return [h for h, livre in ult.items() if livre]


def ultima_livre(events, id_):
    """Texto da última resposta livre dada ao header `id_` (AskUserQuestion ou Lavish) desde o último `pend add` dele; None se não há.

    É o que o `pend done` sem --resposta manda ao gate: sem isso o worker destravado recebe "fechada sem resposta" e perde o que o usuário escreveu.
    """
    txt = None
    for e in events:
        if e.get("tipo") == "pend" and e.get("op") == "add" and e.get("pend") == id_:
            txt = None
        elif e.get("tipo") in ("resposta", "resposta_lavish") and e.get("header") == id_ and e.get("livre") and e.get("resposta"):
            txt = e["resposta"] + (f" (nota: {e['nota']})" if e.get("nota") else "")
    return txt


def aviso_gate(gate, run):
    return f"gate {gate} do Run {run} fica pendente até o coordenador comandar o Run: {dica_ligar(run)}"


def gates_pendentes(events):
    """[(gate, Run)] de decisão já fechada cujo gate o Orca ainda não resolveu (sem `gate_resolvido` no log).

    Gate recusado MAX_TENTATIVAS vezes está abandonado (reconciliar_gates desiste dele): fica fora, porque a linha não teria ação possível.
    O aberto.json ainda lista o gate pendente do Orca.
    """
    recusas = [e.get("gate") for e in events if e.get("tipo") == "gate_falha"]
    resolvidos = {e.get("gate") for e in events if e.get("tipo") == "gate_resolvido"} | {g for g in recusas if recusas.count(g) >= MAX_TENTATIVAS}
    return list({e["gate"]: e.get("gate_run") for e in events
                 if e.get("tipo") == "pend" and e.get("op") == "done" and e.get("gate") and e["gate"] not in resolvidos}.items())


def cursor_recuperado(cursor, agora):
    """A marca de recuperação do cursor.json (ts + cópia) se for das últimas ALERTA_H horas; senão None."""
    r = (cursor or {}).get("recuperado")
    if isinstance(r, dict) and r.get("ts") and (agora - _dt(r["ts"])).total_seconds() < ALERTA_H * 3600:
        return r
    return None


def aviso_recuperado(r):
    return f"cursor.json estava ilegível: reconstruído do events.jsonl (cópia em {r.get('copia')}); papéis e ingest recomeçaram."


def aviso_painel(agora=None):
    """Aviso de que o painel do agent manager parou, ou None: com o gerente ligado a este coordenador, os avisos do Orca vão para o terminal do
    gerente e só o painel os repassa; o carimbo `gerente-vivo` é do shell do painel, então vale mesmo com o orq.py quebrado (M19)."""
    g = _gerente_cfg()
    if not g or g.get("coordenador") != os.environ.get("ORCA_TERMINAL_HANDLE"):
        return None
    try:
        idade = (agora or time.time()) - os.path.getmtime(_path(PAINEL_VIVO))
    except OSError:
        return f"painel do agent manager sem carimbo ({PAINEL_VIVO}): ele não subiu ou roda o script antigo; nenhum aviso de worker chega enquanto isso"
    if idade <= PAINEL_PARADO_S:
        return None
    return f"painel do agent manager parado há {int(idade // 60)} min: nenhum aviso de worker chega; reinicie painel-agent-manager.sh no terminal dele"


def _extra(events, todas, agora, pendencias=None, cursor=None, aberto=None, turnos=None, painel=None):
    """A linha única para o que não é mensagem do usuário: painel parado, cursor recuperado, resposta suspeita ou livre, alerta de scout e relatórios sem triar."""
    partes = [painel] if painel else []
    rec = cursor_recuperado(cursor, agora)
    if rec:
        partes.append("[aviso] " + aviso_recuperado(rec))
    sus = suspeitas(events, agora)
    if sus:
        partes.append("; ".join(f"resposta suspeita em {h}: confirme" for h in sus[:2]) + ", refaça a pergunta.")
    liv = livres(events, pendencias)
    if liv:
        partes.append("; ".join(f'resposta livre em {h}: se decidiu, feche com orq pend done {shlex.quote(h)} --resposta "<o que foi decidido>"' for h in liv[:2]) + ".")
    gp = gates_pendentes(events)
    if gp:
        partes.append("Gates de decisão fechada ainda pendentes: " + "; ".join(f"{g} (Run {r}: {dica_ligar(r)})" if r else g for g, r in gp[:2])
                      + (f" +{len(gp) - 2}" if len(gp) > 2 else "") + ".")
    ags = reavalia((aberto or {}).get("agentes") or [], events, agora, turnos)
    for estado, rotulo in (("travado", "Travado"), ("nao_comecou", "Não começou"), ("parado", "Parado no prompt")):
        parados = [a for a in ags if a["estado"] == estado]
        for a in parados[:2]:
            min_ = (a.get("idade_s") or 0) // 60
            detalhe = (f"{a.get('fase') or 'sem fase'}, sem heartbeat há {min_} min" + (f", {a['motivo']}" if a.get("motivo") else "") if estado == "travado" else f"sem turno {min_} min depois do despacho"
                       if estado == "nao_comecou" else f"há {min_} min")
            partes.append(f"{rotulo}: {a.get('task')} ({detalhe}): " + f'orq steer {a.get("task")} "<ajuste>" --run {a.get("run")}.')
        if len(parados) > 2:
            partes.append(f"+{len(parados) - 2} {rotulo.lower()}.")
    alertas = alertas_recentes(events, agora, ags)
    for a in alertas[:2]:
        if a.get("alerta") == "steer_nao_lido":
            partes.append(f"Alerta: steer não lido em {a.get('task')} depois de {STEER_TENTATIVAS} avisos ao terminal parado: olhe o worker (orq agentes).")
            continue
        titulo = re.sub(r"^\s*\[scout\]\s*", "", a.get("titulo") or "", flags=re.I)
        partes.append(f"Alerta: scout {_cita(titulo)!r} concluído sem reportPath ({(a.get('task') or '')[:14]}).")
    if len(alertas) > 2:
        partes.append(f"+{len(alertas) - 2} alertas.")
    grupos = _relatorios(todas)
    if grupos:
        def grupo(fonte, es):
            ids = ",".join(e["id"] for e in es) if len(es) <= 3 else f"{es[0]['id']}-{es[-1]['id']}"
            caminhos = list(dict.fromkeys(e["caminho"] for e in es if e.get("caminho")))
            onde = "/".join((caminhos[-1] if caminhos else "").split("/")[-2:])  # o do relatório mais novo
            return (f"{_cita(fonte, 38)} ×{len(es)} [{ids}]" + (f" {len(caminhos)} relatórios, o mais novo {onde}" if len(caminhos) > 1
                                                                 else f" {onde}" if onde else ""))
        lista = [grupo(f, es) for f, es in list(grupos.items())[:3]]
        partes.append("Relatórios sem triar: " + "; ".join(lista) + (f" +{len(grupos) - 3} fontes" if len(grupos) > 3 else "") + ".")
    linha = " ".join(partes)
    return linha if len(linha) <= 560 else linha[:559] + "…"


def _linha_runs(aberto):
    """" Runs: <objetivo> (N abertas), ..." dos Runs visíveis do aberto.json; vazio no cache antigo, sem a chave."""
    rs = aberto.get("runs") or []
    return " Runs: " + ", ".join(f"{_cita(x['objetivo'], 30)} ({x['abertas']} abertas)" for x in rs[:3]) + (f" +{len(rs) - 3}" if len(rs) > 3 else "") + "." if rs else ""


def resumo(events, aberto, pendencias, entrada=None, agora=None, cursor=None, turnos=None, painel=None):
    """No máximo 5 linhas: entrada e o que está sem efeito, uma linha extra (suspeita, alerta, relatórios), aberto no Orca, pendências, como dar efeito."""
    todas = abertas(events)
    sem = [e for e in todas if e["id"] != (entrada or {}).get("id") and e.get("origem", "usuario") == "usuario"]
    quem = f"entrada {entrada['id']} (usuário). " if entrada else ""
    lista = ", ".join(f"{e['id']} ({_cita(e.get('texto'))!r})" for e in sem[:3]) + (f" +{len(sem) - 3}" if len(sem) > 3 else "")
    l1 = f"[orq] {quem}Sem efeito: {lista or 'nenhum'}."
    agora = agora or datetime.now(timezone.utc)
    extra = _extra(events, todas, agora, pendencias, cursor, aberto, turnos, painel)
    if aberto:
        bl = aberto["backlog"]
        velho = f" ({bl[0]['id'][:9]}… {_cita(bl[0]['titulo'])!r}, {bl[0]['dias']} d)" if bl else ""
        falhas = aberto.get("falhas") or []
        sem_leitura = f" {len(falhas)} Run{'s' if len(falhas) > 1 else ''} sem leitura." if falhas else ""
        l2 = (f"Aberto (cache de {_hora_local(aberto.get('ts'))}): backlog {len(bl)}{velho}, rodando {aberto['rodando']}, "
              f"bloqueado {len(aberto['bloqueado'])}, gates {len(aberto['gates'])}.{sem_leitura}{linha_vivos(events, aberto, agora, turnos)}"
              f"{_linha_runs(aberto)}")
    else:
        l2 = "Aberto: cache ainda não existe (refresh em andamento)."
    todos = (pendencias or {}).get("itens", [])
    itens = [i for i in todos if not pend_depois(i, agora.astimezone().date())]
    l3 = (f"Com você: {len(itens)} ({sum(i.get('tipo') == 'decisao' for i in itens)} decisões)."
          + (f" Depois: {len(todos) - len(itens)}." if len(todos) > len(itens) else ""))
    l4 = "Efeito: orq intake <e> tarefa <task>|steer <task>|pend <id>|decisao <id>|conversa|descartado --nota <motivo>"
    return "\n".join([l1, *([extra] if extra else []), l2, l3, l4])


def _lim(itens, n, fmt):
    """Até n itens formatados, mais `+k` do que sobrou."""
    return [fmt(i) for i in itens[:n]] + ([f"+{len(itens) - n}"] if len(itens) > n else [])


def resumo_quatro(events, aberto, pendencias, ts, desde=None, agora=None, painel=None):
    """`orq resumo`: as quatro partes desde `desde` (padrão: a última mensagem do usuário, sem contar o pedido do próprio resumo) e as decisões, até ~20 linhas.

    Com você = pendências fora do Depois; Entrou = entradas da janela e o efeito de cada; Anda = workers rodando e a fase;
    Vem = tickets prontos (Blocked by todos resolvidos) e bloqueados. `ts` são os tickets já lidos.
    """
    agora = agora or datetime.now(timezone.utc)
    users = [e["ts"] for e in events if e.get("tipo") == "entrada" and e.get("origem", "usuario") == "usuario" and e.get("ts")]
    if users and 0 <= (agora - _dt(users[-1])).total_seconds() < RESUMO_PEDIDO_S:
        users.pop()  # o hook de prompt grava o pedido do resumo antes de o `orq resumo` rodar: a janela começa na mensagem anterior (M17)
    desde = desde or (users[-1] if users else "")
    na_janela = [e for e in events if (e.get("ts") or "") >= desde]
    efeito = {e["entrada"]: e for e in events if e.get("tipo") == "intake"}
    itens = [i for i in (pendencias or {}).get("itens", []) if not pend_depois(i, agora.astimezone().date())]
    entrou = [e for e in na_janela if e.get("tipo") == "entrada" and e.get("id")]
    rodando = [a for a in reavalia((aberto or {}).get("agentes") or [], events, agora) if a.get("estado") in ANDA]  # o perguntando também anda (M17)
    resolvidos = {t["num"] for t in ts if t["status"] == STATUS_FECHADO}
    abertos = [t for t in ts if t["status"] not in (STATUS_FECHADO, STATUS_ANDAMENTO)]
    prontos = [t for t in abertos if set(t["blocked_by"]) <= resolvidos]
    travados = [t for t in abertos if t not in prontos]
    dec = []
    for e in na_janela:
        if e.get("tipo") in ("resposta", "resposta_lavish") and e.get("resposta"):
            dec.append(f"{e.get('header') or e.get('item')}: {_cita(e['resposta'], 60)}")
        elif e.get("tipo") == "pend" and e.get("op") == "done" and e.get("resposta"):
            dec.append(f"{e['pend']}: {_cita(e['resposta'], 60)}")

    def efeito_de(e):
        i = efeito.get(e["id"])
        return f"{e['id']} ({e.get('origem', 'usuario')}) {_cita(e.get('texto'), 40)!r} -> " + (f"{i['efeito']}{' ' + i['ref'] if i.get('ref') else ''}" if i else "sem efeito")

    def bloq(t):
        falta = [n for n in t["blocked_by"] if n not in resolvidos]
        return f"{t['num']} {_cita(t['titulo'], 40)} (espera {', '.join(falta)})"

    def parte(nome, lst, vazio, fmt, n=5):
        return [f"{nome} ({len(lst)}):", *("  " + x for x in _lim(lst, n, fmt))] if lst else [f"{nome}: {vazio}"]

    entregas = [f"{_cita(e.get('task'), 14)}: {a}" for e in na_janela if e.get("tipo") == "entrega" for a in e.get("avisos") or []]
    vem = [f"pronto: {t['num']} {_cita(t['titulo'], 40)}" for t in prontos[:5]] + [f"bloqueado: {bloq(t)}" for t in travados[:5]]
    return "\n".join([
        f"[orq resumo] desde {_hora_local(desde) if desde else 'o início'}",
        *([f"[aviso] {painel}"] if painel else []),
        *parte("Com você", itens, "nada", lambda i: f"{i['id']}  {i.get('tipo')}  {_cita(i.get('titulo'), 50)}"),
        *parte("Entrou", entrou, "nada", efeito_de, 6),
        *(parte("Entrega sem prova", entregas, "", lambda x: x, 4) if entregas else []),
        *parte("Anda", rodando, "ninguém rodando", lambda a: f"{_cita(a.get('titulo') or a.get('task'), 40)} [{(a.get('fase') or 'sem fase') if a['estado'] == 'rodando' else a['estado']}]"),
        *(["Vem:", *("  " + x for x in vem)] if vem else ["Vem: nenhum ticket aberto"]),
        *parte("Decisões", dec, "nenhuma", lambda d: d, 6),
    ])


def itens_de_acao(md):
    """Itens numerados (lista ou tabela) da seção de ação do relatório; None se não há seção ou ela não tem itens.

    Aceita "Itens de ação" e os títulos antigos "O que fazer hoje" e "O que precisa de ação".
    """
    itens, dentro, cerca = [], False, False
    for linha in md.splitlines():
        if linha.lstrip().startswith("```"):
            cerca = not cerca
            continue
        if cerca:
            continue
        if re.match(r"#{1,6}(\s|$)", linha):  # "#101" sem espaço não é título em Markdown
            if dentro:
                break
            dentro = bool(TITULOS_ACAO.match(linha.strip()))
            continue
        m = dentro and (re.match(r"\s*\d+[.)]\s+(.+)", linha) or re.match(r"\s*\|\s*\d+\s*\|\s*([^|]+?)\s*\|", linha))
        if m:
            itens.append(" ".join(re.sub(r"[*`]", "", m.group(1)).split())[:300])
    return itens or None


def caminho_do_relatorio(content, base, quando_ms=None):
    """O .scratch/….md citado no texto da automation, absoluto (relativo a `base`, o repo do run).

    Com vários, vale o que tem a data do run (UTC ou local) no nome; sem data que case, o último citado.
    """
    cands = list(dict.fromkeys(re.findall(r"([^\s`'\"()]*\.scratch/[^\s`'\"()]+?\.md)", content or "")))
    if not cands:
        return None
    c = cands[-1]
    if quando_ms:
        t = datetime.fromtimestamp(quando_ms / 1000, timezone.utc)
        datas = {t.strftime("%Y-%m-%d"), t.astimezone().strftime("%Y-%m-%d")}
        c = next((x for x in reversed(cands) if any(d in os.path.basename(x) for d in datas)), c)
    return c if c.startswith("/") or not base else os.path.join(base, c)


# ---------- E/S ----------

def log(msg):
    try:
        os.makedirs(os.path.dirname(LOG), exist_ok=True)
        with open(LOG, "a") as f:
            f.write(f"{now()} {msg}\n")
    except OSError:
        pass


def _dt(ts):
    """Data do Orca ("2026-09-29 12:47:56", UTC sem fuso; aceita ISO com Z) como datetime com fuso."""
    d = datetime.fromisoformat(ts.replace("Z", "+00:00").replace(" ", "T"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _hora_local(ts):
    """HH:MM local de um carimbo UTC; "?" se não houver."""
    try:
        return _dt(ts).astimezone().strftime("%H:%M")
    except (AttributeError, ValueError):
        return "?"


def dias_desde(ts, agora):
    """Idade em dias de um created_at do Orca."""
    return (agora - _dt(ts)).days


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


TIMEOUT_ORCA = float(os.environ.get("ORQ_ORCA_TIMEOUT") or 2.5)  # os testes sobem um processo Python por chamada e usam mais (B31)


def orca(*args, timeout=TIMEOUT_ORCA, area="orchestration", sem_terminal=False, como=None, run=None, env_extra=None):
    """Único ponto que toca o Orca (`orca <area> ...`). Devolve `result` ou levanta RuntimeError.

    Com sem_terminal o Orca chama-o de um terminal sem ligação (SEM_LIGACAO): `worker-list` deixa de ser escopado a um Run e lista todos.
    Tirar a variável não serve: sem ela o Orca cai no Run do coordenador ativo (`scope.source` bound, conferido em 29/09 com o Orca de verdade).

    Com o agent manager segurando o Run do comando (`run`, ou o --run de um comando de MUTA_RUN), liga o gerente a ele antes, sob a trava:
    o Orca liga um Run por terminal. `run-use` no Run já ligado não muda nada. Run que o gerente não segura vai pelo handle do próprio
    coordenador, o dono do Run fora do gerente (M15)."""
    alvo = None if sem_terminal or como else run or _run_do_comando(area, args)
    liga = alvo if alvo and _do_gerente(alvo) else None
    if alvo and not liga and runs_do_gerente():
        como = os.environ.get("ORCA_TERMINAL_HANDLE")
    if liga:
        with trava_gerente():
            _orca("run-use", "--id", liga, timeout=timeout, h=handle_orca())
            return _orca(*args, timeout=timeout, area=area, h=handle_orca(), env_extra=env_extra)
    return _orca(*args, timeout=timeout, area=area, h=SEM_LIGACAO if sem_terminal else como or handle_orca(), env_extra=env_extra)


def _run_do_comando(area, args):
    """O Run do --run de um comando que exige o terminal ligado; senão None."""
    if area != "orchestration" or not args or args[0] not in MUTA_RUN or "--run" not in args:
        return None
    return args[args.index("--run") + 1]


def runs_do_gerente():
    """Os Runs do agent manager ligado por ESTE coordenador; [] sem gerente (o waiter lê a caixa de todos)."""
    g = _gerente_cfg()
    return g["runs"] if g and g.get("coordenador") == os.environ.get("ORCA_TERMINAL_HANDLE") else []


def _do_gerente(run):
    """O Run é de um agent manager ligado por ESTE coordenador?"""
    g = _gerente_cfg()
    return bool(g) and g.get("coordenador") == os.environ.get("ORCA_TERMINAL_HANDLE") and run in g["runs"]


def _run_proprio():
    """O Run ligado ao terminal do próprio coordenador (com o gerente ligado, o `run-current` sem `como` é onde o painel parou, M15)."""
    return _run_atual_id(como=os.environ.get("ORCA_TERMINAL_HANDLE") if runs_do_gerente() else None)


def run_do_coordenador(run, proprio=_DESCONHECIDO):
    """O coordenador comanda o Run: o agent manager dele o segura, ou é o Run ligado ao terminal do próprio coordenador (`proprio`, se o
    chamador já o perguntou ao Orca). O `run-current` do gerente não serve, troca a cada volta do painel (M15)."""
    return _do_gerente(run) or (_run_proprio() if proprio is _DESCONHECIDO else proprio) == run


def run_padrao(run=None):
    """O Run alvo de um comando: `run`, senão o ligado ao coordenador (o único do gerente, se o próprio terminal não segura nenhum).
    O gerente em mais de um Run não tem Run padrão: o `run-current` dele é sorteado pelo revezamento do painel (M15)."""
    if run:
        return run
    g = runs_do_gerente()
    if len(g) > 1:
        raise ValueError(f"o agent manager segura {len(g)} Runs ({', '.join(g)}): passe --run")
    proprio = _run_proprio()
    if proprio and g and proprio not in g:  # o coordenador comanda dois Runs: sem --run não há alvo óbvio (B50)
        raise ValueError(f"o coordenador comanda {len(g) + 1} Runs ({', '.join([*g, proprio])}): passe --run")
    return proprio or (g[0] if g else None)


def dica_ligar(run):
    """O que rodar para o coordenador voltar a comandar o Run: religar o gerente a ele, ou o run-use quando não há gerente."""
    g = _gerente_cfg()
    if g and g.get("coordenador") == os.environ.get("ORCA_TERMINAL_HANDLE"):
        return f"rode orq gerente ligar --terminal {g['gerente']} --run {run}"
    return f"rode run-use --id {run}"


def _gerente_cfg():
    """gerente.json como {coordenador, gerente, runs}; {} se ausente ou de forma errada. O formato do ticket 17 (`run`) vale como runs=[run]."""
    g = _dict(_read_json(_path(GERENTE)))
    if not isinstance(g.get("gerente"), str):
        return {}
    runs = g["runs"] if isinstance(g.get("runs"), list) else [g["run"]] if g.get("run") else []
    return {**g, "runs": [r for r in runs if isinstance(r, str)]}


_TRAVA_GERENTE = [0]  # profundidade da trava neste processo


@contextlib.contextmanager
def trava_gerente():
    """Ligar o gerente a um Run, ler e confirmar a caixa dele é uma seção só, entre processos (painel, hooks, waiter, orq): o Orca liga um Run por
    terminal e cancela a entrega (consumer_fenced) de quem perde a ligação entre o check e o ack. Reentrante; sem gerente.json não trava."""
    if _TRAVA_GERENTE[0] or not os.path.exists(_path(GERENTE)):
        yield
        return
    with _trava("gerente.lock"):
        _TRAVA_GERENTE[0] = 1
        try:
            yield
        finally:
            _TRAVA_GERENTE[0] = 0


def _orca(*args, timeout, h, area="orchestration", env_extra=None):
    """Uma chamada ao binário do Orca com o handle `h` na variável ORCA_TERMINAL_HANDLE."""
    env = {**os.environ, "ORCA_TERMINAL_HANDLE": h} if h and h != os.environ.get("ORCA_TERMINAL_HANDLE") else None
    if env_extra:
        env = {**(env or os.environ), **env_extra}
    p = subprocess.run([ORCA, area, *args, "--json"], capture_output=True, text=True, timeout=timeout, env=env)
    out = json.loads(p.stdout)
    if not out.get("ok"):
        raise RuntimeError((out.get("error") or {}).get("message") or "orca falhou")
    return out["result"]


def handle_orca():
    """O handle com que este terminal fala com o Orca: o do agent manager quando este é o coordenador que o ligou (`orq gerente ligar`),
    senão o próprio. O Orca identifica quem chama pela variável ORCA_TERMINAL_HANDLE e só avisa (digita "You have N orchestration message")
    o terminal ligado ao Run; um terminal sem agente não recebe aviso nenhum."""
    meu = os.environ.get("ORCA_TERMINAL_HANDLE")
    g = _read_json(_path(GERENTE))
    return g["gerente"] if isinstance(g, dict) and meu and g.get("coordenador") == meu and g.get("gerente") else meu


def _path(nome):
    return os.path.join(HOME, nome)


def _read_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def _dict(x):
    """O JSON lido à mão pode ter a forma errada (null, lista): o que não é objeto vale como ausente."""
    return x if isinstance(x, dict) else {}


def _cursor_ro():
    """cursor.json para quem só lê (hooks, resumo): sempre um dict, mesmo com a raiz errada. Quem grava recupera o arquivo em _read_cursor."""
    return _dict(_read_json(_path("cursor.json")))


def _sub(c, chave):
    """c[chave] como dict gravável: se estiver com outra forma, é recomeçado."""
    if not isinstance(c.get(chave), dict):
        c[chave] = {}
    return c[chave]


def _read_cursor():
    """cursor.json para quem vai regravá-lo (com o cursor.lock): ausente é {}; ilegível vira cópia e recomeço (_recuperar_cursor)."""
    try:
        with open(_path("cursor.json")) as f:
            d = json.load(f)
    except FileNotFoundError:
        return {}
    except ValueError as e:
        return _recuperar_cursor(str(e))
    return d if isinstance(d, dict) else _recuperar_cursor("a raiz não é um objeto")


def _recuperar_cursor(motivo):
    """Guarda o cursor.json ilegível ao lado (cursor.json.corrompido-<hora>) e devolve um cursor novo com a marca `recuperado`.

    Sem o contador `entrada`, _grava_evento reconstrói os ids do events.jsonl (maior eN + 1); o ingest não repete o que o log já tem.
    Papéis e Runs por sessão se perdem: sessão de worker com Run ligado volta a valer como coordenador até o próximo despacho.
    """
    copia = "cursor.json.corrompido-" + re.sub(r"\D", "", now())
    os.replace(_path("cursor.json"), _path(copia))
    log(f"cursor.json ilegível ({motivo}): cópia em {copia}, cursor reconstruído do events.jsonl")
    return {"recuperado": {"ts": now(), "copia": copia}}


def _write_json(path, data, indent=None):
    """tmp + rename no mesmo diretório: quem lê (fs.watch do painel) nunca vê arquivo pela metade, e falha não deixa tmp."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


@contextlib.contextmanager
def _trava(nome):
    """flock exclusivo em HOME/<nome>. Ordem fixa quando há duas: pend.lock e depois cursor.lock."""
    os.makedirs(HOME, exist_ok=True)
    with open(_path(nome), "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


@contextlib.contextmanager
def _sem_alarme():
    """Segura o SIGALRM do teto do hook até o fim de uma gravação em dois passos (arquivo e evento); o alarme chega depois.

    Só cobre a gravação: as travas são tomadas antes, fora daqui, para um lock preso ainda ser cortado pelo teto.
    """
    antes = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, antes)


def read_events():
    out = []
    try:
        with open(_path("events.jsonl")) as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    except OSError:
        pass
    return out


def _grava_evento(ev, novo_id=False):
    """Acrescenta uma linha; cursor.lock já tomado. Com novo_id, o próximo eN é max(cursor, maior id do log) + 1:
    se o cursor.json se perder (apagado ou recuperado), os ids não recomeçam e não colidem com intakes antigos."""
    if novo_id:
        cur = _read_cursor()
        n = cur["entrada"] if isinstance(cur.get("entrada"), int) else None
        # o log só é varrido quando o contador se perdeu (cursor apagado): varrer 50 mil linhas a cada entrada custaria ~90 ms
        maior = 0 if n is not None else max(
            (int(m.group(1)) for e in read_events() if (m := re.fullmatch(r"e(\d+)", str(e.get("id") or "")))), default=0)
        cur["entrada"] = max(n or 0, maior) + 1
        _write_json(_path("cursor.json"), cur)
        ev["id"] = f"e{cur['entrada']}"
    ev = {"ts": now(), **ev}
    with open(_path("events.jsonl"), "a") as f:
        f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    return ev


def append_event(ev, novo_id=False):
    """Acrescenta uma linha ao events.jsonl sob o cursor.lock (ids sequenciais com novo_id)."""
    with _trava("cursor.lock"):
        return _grava_evento(ev, novo_id)


# ---------- aberto.json ----------

ABERTA = ("ready", "pending", "dispatched", "blocked")


def resumo_run(r, tasks, gerente=()):
    """Um Run como o `orq runs` e o aberto.json o mostram. `ultima` é a atividade mais recente das tasks (criação ou conclusão), ou a criação do
    Run: o updated_at do Orca não serve, o próprio painel o mexe a cada run-use."""
    datas = [d for d in (_ts(r.get("created_at")), *(_ts(t.get(k)) for t in tasks for k in ("created_at", "completed_at"))) if d]
    return {"id": r["id"], "objetivo": r.get("objective") or "", "abertas": sum(t.get("status") in ABERTA for t in tasks),
            "concluidas": sum(t.get("status") == "completed" for t in tasks), "gerente": r["id"] in gerente,
            "ultima": max(datas).strftime("%Y-%m-%dT%H:%M:%SZ") if datas else None}


def runs_visiveis(runs, agora, todos=False):
    """Runs com trabalho aberto ou com atividade nas últimas RUN_RECENTE_H h; os de teste só com `todos`."""
    def vale(x):
        ultima = _ts(x.get("ultima"))
        return x["abertas"] or (ultima and agora - ultima < timedelta(hours=RUN_RECENTE_H))
    return [x for x in runs if todos or (vale(x) and not RUN_TESTE.search(x["objetivo"]))]


def monta_aberto(dados, agora):
    """Pura: [(run, tasks, gates_pendentes)] de todos os Runs -> o aberto (backlog, rodando, bloqueado, gates, falhas).

    Cancelada (completed com result.cancelado) é completed e não entra em nada. deps_faltando são as deps ainda não completed.
    Um Run com dado quebrado vai para `falhas` e para o log; os outros seguem.
    """
    ab = {"ts": now(), "backlog": [], "rodando": 0, "andamento": [], "bloqueado": [], "gates": [], "falhas": [], "runs": []}
    for r, tasks, gates in dados:
        try:
            backlog, rodando, andamento, bloqueado = [], 0, [], []
            feitas = {t["id"] for t in tasks if t["status"] == "completed"}
            for t in tasks:
                item = {"id": t["id"], "run": r["id"], "objetivo": r.get("objective") or "",
                        "titulo": t.get("task_title") or _cita(t.get("spec"), 50), "dias": dias_desde(t["created_at"], agora)}
                if t["status"] in ("ready", "pending"):
                    deps = t.get("deps") or []
                    deps = json.loads(deps) if isinstance(deps, str) else deps
                    backlog.append({**item, "deps_faltando": [d for d in deps if d not in feitas]})
                elif t["status"] == "dispatched":
                    rodando += 1
                    andamento.append({"task": t["id"], "dispatch": t.get("dispatch_id"), "run": r["id"], "titulo": item["titulo"]})
                elif t["status"] == "blocked":
                    bloqueado.append(item)
            gates_ = [{"id": g.get("id"), "run": r["id"], "task": g.get("task_id"), "objetivo": r.get("objective") or "",
                       "pergunta": g.get("question") or "", "dias": dias_desde(g["created_at"], agora) if g.get("created_at") else 0}
                      for g in gates]
        except Exception as e:  # noqa: BLE001 - um Run quebrado não derruba o cache dos outros
            log(f"refresh: Run {r.get('id')}: {type(e).__name__}: {e}")
            ab["falhas"].append(r.get("id"))
            continue
        ab["runs"].append(resumo_run(r, tasks, runs_do_gerente()))
        teste = RUN_TESTE.search(r.get("objective") or "")  # Run de teste, ou task "[teste] ..." num Run real: fora do backlog e do bloqueado (B54)
        backlog = [i for i in backlog if not teste and not RUN_TESTE.search(i["titulo"])]
        bloqueado = [i for i in bloqueado if not teste and not RUN_TESTE.search(i["titulo"])]
        ab["backlog"] += backlog
        ab["rodando"] += rodando
        ab["andamento"] += andamento
        ab["bloqueado"] += bloqueado
        ab["gates"] += gates_
    ab["backlog"].sort(key=lambda i: -i["dias"])
    ab["runs"] = runs_visiveis(ab["runs"], agora)  # o resto fica no arquivo: orq runs --todos
    return ab


def _todos_os_runs():
    """run-list seguindo o nextCursor (o Orca pagina); ponytail: teto de 50 páginas de 100."""
    runs, cursor = [], None
    for _ in range(50):
        res = orca("run-list", "--limit", "100", *(["--cursor", cursor] if cursor else []), timeout=20)
        runs += res["runs"]
        cursor = res.get("nextCursor")
        if not cursor:
            break
    return runs


def runs_lista(todos=False):
    """O `orq runs`: todos os Runs do Orca (run-list + task-list), com o filtro de fim de vida, mais recentes primeiro."""
    def por_run(r):
        return resumo_run(r, orca("task-list", "--run", r["id"], timeout=20)["tasks"], runs_do_gerente())
    with ThreadPoolExecutor(8) as ex:
        rs = list(ex.map(por_run, _todos_os_runs()))
    return sorted(runs_visiveis(rs, datetime.now(timezone.utc), todos), key=lambda x: x["ultima"] or "", reverse=True)


def texto_runs(rs):
    """Uma linha por Run: id, objetivo, abertas/concluídas, gerente e última atividade."""
    return "\n".join(f"{x['id']}  {_cita(x['objetivo'], 50)}  {x['abertas']} abertas/{x['concluidas']} concluídas  "
                     f"{'no gerente' if x['gerente'] else 'fora do gerente'}  última {_hora_local(x['ultima'])}" for x in rs) or "nenhum Run com trabalho aberto"


def refresh_aberto():
    """Lê todos os Runs (task-list + gates pendentes) e grava o cache. Um refresh por vez; Run que falha vai para `falhas`."""
    os.makedirs(HOME, exist_ok=True)
    with open(_path("refresh.lock"), "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return None

        def por_run(r):
            try:
                tasks = orca("task-list", "--run", r["id"], timeout=20)["tasks"]
                gates = orca("gate-list", "--run", r["id"], "--status", "pending", timeout=20)["gates"]
                return r, tasks, gates
            except Exception as e:  # noqa: BLE001
                log(f"refresh: Run {r.get('id') if isinstance(r, dict) else r}: {type(e).__name__}: {e}")
                return r, None, None

        runs = _todos_os_runs()
        with ThreadPoolExecutor(8) as ex:
            dados = list(ex.map(por_run, runs))
        ab = monta_aberto([d for d in dados if d[1] is not None], datetime.now(timezone.utc))
        ab["falhas"] += [d[0].get("id") if isinstance(d[0], dict) else str(d[0]) for d in dados if d[1] is None]
        try:
            ab["agentes"] = agentes()  # sem os liberados; se o Orca falhar, o cache fica sem a chave e o resumo cai nos `andamento`
        except Exception as e:  # noqa: BLE001
            log(f"refresh: agentes: {type(e).__name__}: {e}")
        _write_json(_path("aberto.json"), ab)
        return ab


def refresh_bg(refresh=True):
    """ingest em segundo plano; com refresh, refaz também o aberto.json (150 chamadas do Orca): o aviso do Orca só precisa do inbox."""
    if os.environ.get("ORQ_NO_BG"):
        return
    subprocess.Popen([sys.executable, os.path.abspath(__file__), "ingest", *(["--refresh"] if refresh else [])], stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


# ---------- ingest: relatórios de automation e worker_done com relatório ----------

def _cursor_mut(fn):
    """Lê o cursor.json, aplica fn(cursor) e grava, sob o mesmo flock dos ids de entrada."""
    with _trava("cursor.lock"):
        cur = _read_cursor()
        fn(cur)
        _write_json(_path("cursor.json"), cur)


def _turnos_ro():
    """turnos.json para quem só lê: sempre um dict."""
    return _dict(_read_json(_path(TURNOS)))


def _turnos_mut(fn):
    """Lê o turnos.json, aplica fn(turnos) e grava sob o turnos.lock, a menos que fn devolva False (nada mudou)."""
    with _trava("turnos.lock"):
        turnos = _turnos_ro()
        if fn(turnos) is not False:
            _write_json(_path(TURNOS), turnos)


def registra_turno(kind, ev):
    """Hooks prompt e stop de uma sessão de worker: grava o início ou o fim do turno do dispatch dela em turnos.json. Não chama o Orca.

    O preâmbulo de despacho traz o dispatch e a task e abre o registro; os prompts seguintes (steer, aviso) reabrem o mais novo da sessão, e o Stop o fecha.
    Slash command não é turno. Sem dispatch conhecido para a sessão não grava nada: o estado dela fica `unknown`.
    """
    sid, agora, prompt = ev.get("session_id") or "", now(), ev.get("prompt") or ""
    org = origem(prompt)
    if kind == "prompt" and org == "comando":
        return
    dispatch = task = None
    if kind == "prompt" and org == "despacho":
        d, t = ID_DISPATCH.search(prompt), ID_TASK.search(prompt)
        dispatch, task = d and d.group(1), t and t.group(1)
        if not dispatch:
            return  # preâmbulo sem dispatch (colado à mão, formato novo): o estado fica unknown

    def grava(turnos):
        d = dispatch or next((k for k in reversed(turnos) if _dict(turnos[k]).get("sessao") == sid), None)  # o mais novo é o último da ordem de inserção
        if d is None:
            return False
        if kind == "prompt":
            antigo = _dict(turnos.pop(d, None))  # o pop leva o dispatch reaberto para o fim da ordem
            turnos[d] = {"task": task or antigo.get("task"), "sessao": sid, "inicio": agora, "fim": None}
        else:
            turnos[d]["fim"] = agora
        corte = _ts(agora) - timedelta(days=TURNOS_DIAS)
        for k in [k for k, v in turnos.items() if (_ts(_dict(v).get("inicio")) or corte) < corte]:
            del turnos[k]

    _turnos_mut(grava)


def relatorio_do_dia(base, criado_ms):
    """O único .scratch/*/*.md com a data do run no nome e gravado entre o começo do run e ESPERA_RELATORIO_S depois; None com zero ou vários.

    É o que sobra quando o texto da automation não cita o arquivo. ponytail: vários candidatos do mesmo dia não são desempatados.
    """
    if not base or not criado_ms:
        return None
    t = datetime.fromtimestamp(criado_ms / 1000, timezone.utc)
    datas = {t.strftime("%Y-%m-%d"), t.astimezone().strftime("%Y-%m-%d")}
    achados = []
    for arq in glob.glob(os.path.join(glob.escape(base), ".scratch", "*", "*.md")):
        try:
            gravado = os.path.getmtime(arq)
        except OSError:
            continue
        if any(d in os.path.basename(arq) for d in datas) and criado_ms / 1000 - 60 <= gravado <= criado_ms / 1000 + ESPERA_RELATORIO_S:
            achados.append(arq)
    return achados[0] if len(achados) == 1 else None


def caminho_do_run(r):
    """O arquivo do relatório do run: o citado no snapshot (o da data do run, se vários) ou, com snapshot que não o cita, o do dia."""
    snap = r.get("outputSnapshot") or {}
    base = (r.get("runContext") or {}).get("path")
    return caminho_do_relatorio(snap.get("content"), base, snap.get("capturedAt") or r.get("createdAt")) \
        or (relatorio_do_dia(base, r.get("createdAt")) if snap else None)


def entradas_do_run(r):
    """Uma entrada por item de ação do relatório do run; sem seção (ou sem arquivo) vira um item "ler <arquivo>".

    None enquanto o relatório não existe: a automation de terminal vira `completed` antes de o agente gravar o arquivo (o snapshot chega 6 a
    13 min depois). Sem snapshot, snapshot que não cita arquivo ou arquivo citado que ainda não existe, o run espera ESPERA_RELATORIO_S desde
    o createdAt; depois disso vale o que houver, e o pior caso é o item "ler o resultado do run".
    """
    fonte = r.get("title") or r["id"]
    caminho = caminho_do_run(r)
    cedo = time.time() * 1000 - (r.get("createdAt") or 0) < ESPERA_RELATORIO_S * 1000
    itens = None
    if caminho:
        try:
            with open(caminho) as f:
                itens = itens_de_acao(f.read())
        except FileNotFoundError as e:
            if cedo:
                return None
            log(f"ingest: relatório ilegível {caminho}: {type(e).__name__}: {e}")
        except (OSError, ValueError) as e:  # ValueError inclui UnicodeDecodeError
            log(f"ingest: relatório ilegível {caminho}: {type(e).__name__}: {e}")
    elif cedo:
        return None
    ler = f"ler {os.path.basename(caminho)}" if caminho else "ler o resultado do run (sem arquivo de relatório)"
    itens = itens or [ler]
    if len(itens) > MAX_ITENS:
        itens = itens[:MAX_ITENS] + [f"{ler} (mais {len(itens) - MAX_ITENS} itens)"]
    return [{"tipo": "entrada", "origem": "relatorio", "texto": t, "fonte": fonte, "ref": r["id"], "item": i,
             **({"caminho": caminho} if caminho else {})} for i, t in enumerate(itens, 1)]


def completar_relatorios(runs, eventos):
    """Run gravado só com o item "ler o resultado" (o ingest o viu antes do arquivo existir): quando o caminho aparece, entram os itens do
    relatório e a entrada velha, se ainda aberta, é descartada. Devolve quantos runs foram completados."""
    sem, com = {}, set()
    for e in eventos:
        if e.get("tipo") == "entrada" and e.get("origem") == "relatorio" and e.get("ref"):
            if e.get("caminho"):
                com.add(e["ref"])
            else:
                sem.setdefault(e["ref"], []).append(e.get("id"))
    fechadas = {e.get("entrada") for e in eventos if e.get("tipo") == "intake"}
    n = 0
    for r in runs:
        try:
            if r.get("id") not in sem or r["id"] in com or r.get("status") != "completed":
                continue
            caminho = caminho_do_run(r)
            novas = entradas_do_run(r) if caminho and os.path.isfile(caminho) else None
            if not novas:
                continue
            for e in novas:
                append_event(e, novo_id=True)
            for id_ in sem[r["id"]]:
                if id_ not in fechadas:
                    append_event({"tipo": "intake", "entrada": id_, "efeito": "descartado", "nota": f"substituída pelos itens de {os.path.basename(caminho)}"})
            n += 1
        except Exception as e:  # noqa: BLE001 - um run venenoso não trava os outros
            log(f"ingest automations: completar {r.get('id') if isinstance(r, dict) else r}: {type(e).__name__}: {e}")
    return n


def ingest_automations():
    """Run completed novo de qualquer automation -> entradas de origem relatorio.

    Não repete o que o cursor já viu nem o que o próprio events.jsonl já tem (se o cursor se perder); o `auto_desde` anda até o
    maior carimbo ingerido, então a lista de ids vistos não precisa de teto para valer.
    Run cujo relatório ainda não existe (entradas_do_run devolve None) fica para a próxima rodada e segura o `auto_desde`, como uma falha.
    Run já gravado sem caminho ganha os itens quando o arquivo aparece (completar_relatorios).
    """
    ing = _read_cursor()["ingest"]
    desde = _dt(ing.get("auto_desde") or ing["desde"]).timestamp()  # auto_desde anda; `desde` é o ponto de partida fixo (a inbox também usa)
    vistos = set(ing["runs"]) | {e.get("ref") for e in read_events() if e.get("tipo") == "entrada" and e.get("origem") == "relatorio"}
    novos, maior, falhou, tent = 0, desde, False, dict(ing.get("tentativas") or {})
    todos = sorted(orca("runs", area="automations", timeout=20)["runs"], key=lambda r: r.get("createdAt") or 0)
    eventos = read_events()
    for r in todos:
        try:
            quando = ((r.get("outputSnapshot") or {}).get("capturedAt") or r.get("createdAt") or 0) / 1000
            if r["id"] in vistos or r.get("status") != "completed" or quando <= desde:
                continue
            entradas = entradas_do_run(r)
            if entradas is None:
                falhou = True
                continue
            for e in entradas:
                append_event(e, novo_id=True)
            _cursor_mut(lambda c, i=r["id"]: c["ingest"].__setitem__("runs", (c["ingest"]["runs"] + [i])[-200:]))
        except Exception as e:  # noqa: BLE001 - um run venenoso não pode travar os seguintes
            chave = f"auto:{r.get('id') or r.get('createdAt') if isinstance(r, dict) else r}"
            tent[chave] = tent.get(chave, 0) + 1
            log(f"ingest automations: run {chave[5:]} ({tent[chave]}/{MAX_TENTATIVAS}): {type(e).__name__}: {e}")
            falhou = falhou or tent[chave] < MAX_TENTATIVAS  # o auto_desde não passa de um run que ainda vai ser tentado
            continue
        novos += 1
        if not falhou:
            maior = max(maior, quando)
    if maior > desde or tent != (ing.get("tentativas") or {}):
        def grava(c):
            if maior > desde:
                c["ingest"]["auto_desde"] = datetime.fromtimestamp(maior, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if tent:
                c["ingest"]["tentativas"] = {**(c["ingest"].get("tentativas") or {}), **tent}
        _cursor_mut(grava)
    return novos + completar_relatorios(todos, eventos)


def _payload(m):
    p = m.get("payload")
    try:
        p = json.loads(p) if isinstance(p, str) else p
    except ValueError:
        p = None
    return p if isinstance(p, dict) else {}


class _Transitorio(Exception):
    """O Orca não respondeu (timeout, recusa, saída ilegível): a mensagem não tem nada de errado e volta na próxima rodada."""


SHA_RE = re.compile(r"\b(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])[0-9a-f]{7,40}\b")
PR_RE = re.compile(r"https://github\.com/[\w.-]+/[\w.-]+/pull/\d+")


def _git(repo, *args):
    """Saída do git em `repo`, ou None se ele falhar, não existir ou demorar (sem rede: só git local)."""
    try:
        r = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True, timeout=15)
    except (subprocess.TimeoutExpired, OSError):
        return None
    return r.stdout if r.returncode == 0 else None


def confere_entrega(texto, repos, pr_commits=None):
    """Avisos da prova de entrega de um worker_done: commits citados que não existem em nenhum de `repos` e árvore suja onde o commit está.

    Sem sha no texto não há o que provar (lista vazia). `pr_commits(url)` devolve os oids do PR (None: sem gh ou sem resposta, e então não avisa).
    O aviso não bloqueia o worker; só marca a entrega.
    """
    shas = list(dict.fromkeys(SHA_RE.findall(texto or "")))
    avisos, sujos = [], []
    for sha in shas:
        onde = next((r for r in repos if _git(r, "cat-file", "-e", sha + "^{commit}") is not None), None)
        if not onde:
            avisos.append(f"entrega sem commit: {sha} não existe no repositório do worker")
        elif onde not in sujos and _git(onde, "status", "--porcelain").strip():
            sujos.append(onde)
            avisos.append(f"entrega sem commit: árvore suja em {onde}")
    for url in dict.fromkeys(PR_RE.findall(texto or "")):
        oids = pr_commits(url) if pr_commits else None
        faltam = [s for s in shas if oids is not None and not any(o.startswith(s) for o in oids)]
        if faltam:
            avisos.append(f"entrega sem commit: o PR {url} não tem {', '.join(faltam)}")
    return avisos


def _pr_commits(url):
    """Os oids dos commits do PR pelo gh, ou None se o gh não existe, falha ou não há rede."""
    try:
        r = subprocess.run(["gh", "pr", "view", url, "--json", "commits", "-q", ".commits[].oid"], capture_output=True, text=True, timeout=20)
    except (subprocess.TimeoutExpired, OSError):
        return None
    return r.stdout.split() if r.returncode == 0 else None


def _repos_do_worker(m, p):
    """O worktree do dispatch (worker-list) e depois os repositórios de ORQ_REPOS (padrão ~/.claude/orq)."""
    repos = []
    try:
        for w in _workers_todos(m["run_id"]):
            if w.get("dispatchId") == p.get("dispatchId"):
                wt = ((w.get("resource") or {}).get("worktreeId") or "").split("::", 1)[-1]
                repos += [wt] if wt.startswith("/") else []
    except Exception as e:  # noqa: BLE001
        log(f"entrega: worker-list falhou ({type(e).__name__}: {e}); só ORQ_REPOS")
    return repos + [r for r in os.environ.get("ORQ_REPOS", os.path.expanduser("~/.claude/orq")).split(":") if r]


def _prova_de_entrega(m, p):
    """worker_done com sha no texto -> evento `entrega` com os avisos, se houver."""
    texto = f"{m.get('subject') or ''}\n{m.get('body') or ''}"
    if not SHA_RE.search(texto):
        return
    avisos = confere_entrega(texto, _repos_do_worker(m, p), _pr_commits)
    if avisos:
        append_event({"tipo": "entrega", "dispatch": p.get("dispatchId"), "task": p.get("taskId"), "run": m["run_id"], "msg": m["id"], "avisos": avisos})


def _ingest_msg(m, desde, ja, titulos):
    """Uma mensagem da inbox -> 1 se virou entrada. Levanta o que a mensagem tiver de errado; quem chama isola.

    _Transitorio é falha do Orca (o task-list do título do scout), que vale nova tentativa; o resto é da mensagem e é descartado.
    """
    if m.get("type") != "worker_done" or _dt(m["created_at"]) <= desde or m["id"] in ja:
        return 0
    p = _payload(m)
    _prova_de_entrega(m, p)
    if p.get("reportPath"):
        append_event({"tipo": "entrada", "origem": "relatorio_worker", "texto": m.get("subject") or "", "fonte": f"worker {m.get('subject') or ''}",
                      "caminho": p["reportPath"], "ref": m["id"], "run": m["run_id"], "task": p.get("taskId")}, novo_id=True)
        return 1
    log(f"ingest: worker_done {m['id']} sem reportPath (task {p.get('taskId')}, {m.get('subject')})")
    if p.get("outcome") != "succeeded":  # scout que falhou não tem relatório e não é alerta
        return 0
    if m["run_id"] not in titulos:
        try:
            tasks = orca("task-list", "--run", m["run_id"], timeout=20)["tasks"]
        except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:
            raise _Transitorio(f"{type(e).__name__}: {e}")
        titulos[m["run_id"]] = {t["id"]: t.get("task_title") or "" for t in tasks}
    titulo = titulos[m["run_id"]].get(p.get("taskId"), "")
    if "[scout]" in titulo.lower():
        append_event({"tipo": "alerta", "alerta": "scout_sem_relatorio", "task": p.get("taskId"), "run": m["run_id"], "msg": m["id"], "titulo": titulo})
    return 0


def ingest_inbox():
    """worker_done novo com reportPath -> entrada relatorio_worker; scout sem reportPath -> alerta; o resto só no log.

    Cada mensagem é isolada (uma venenosa vai para o log e o cursor passa dela); o que já está no events.jsonl não repete.
    Falha do Orca (_Transitorio) para a rodada antes da mensagem, que volta na próxima; depois de MAX_TENTATIVAS ela é descartada.
    """
    ing = _read_cursor()["ingest"]
    desde, ultimo, novos, titulos = _dt(ing["desde"]), ing["inbox_seq"], 0, {}
    avancou, tent = ultimo, dict(ing.get("tentativas") or {})
    eventos = read_events()
    ja = {e.get("ref") for e in eventos if e.get("origem") == "relatorio_worker"} | {e.get("msg") for e in eventos if e.get("tipo") == "alerta"}
    msgs = sorted((m for m in orca("inbox", "--limit", "200", timeout=20)["messages"] if isinstance(m.get("sequence"), int)),
                  key=lambda m: m["sequence"])
    if ultimo and msgs and msgs[0]["sequence"] > ultimo + 1:
        log(f"ingest inbox: janela perdida, mensagens {ultimo + 1} a {msgs[0]['sequence'] - 1} saíram das últimas 200")
    for m in msgs:
        if m["sequence"] <= ultimo:
            continue
        try:
            novos += _ingest_msg(m, desde, ja, titulos)
        except _Transitorio as e:
            chave = f"msg:{m.get('id')}"
            tent[chave] = tent.get(chave, 0) + 1
            if tent[chave] < MAX_TENTATIVAS:
                log(f"ingest inbox: mensagem {m.get('id')} adiada ({tent[chave]}/{MAX_TENTATIVAS}): {e}")
                break
            log(f"ingest inbox: mensagem {m.get('id')} descartada depois de {MAX_TENTATIVAS} tentativas: {e}")
        except Exception as e:  # noqa: BLE001
            log(f"ingest inbox: mensagem {m.get('id')} descartada: {type(e).__name__}: {e}")
        avancou = m["sequence"]
    if avancou > ultimo or tent != (ing.get("tentativas") or {}):
        def grava(c):
            c["ingest"]["inbox_seq"] = max(c["ingest"].get("inbox_seq", 0), avancou)
            if tent:
                c["ingest"]["tentativas"] = {**(c["ingest"].get("tentativas") or {}), **tent}
        _cursor_mut(grava)
    return novos


def ingest():
    """Automations e inbox -> entradas, e gates de pendência já fechada -> resolvidos. Devolve (entradas novas, gates resolvidos), ou None se
    outro ingest está rodando; cada fonte falha sozinha e vai para o log."""
    os.makedirs(HOME, exist_ok=True)
    with open(_path("ingest.lock"), "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return None
        # primeira execução: grava o ponto de partida, e só o que for posterior a ele entra
        _cursor_mut(lambda c: c.__setitem__("ingest", {"desde": INICIO, "inbox_seq": 0, "runs": [], **_dict(c.get("ingest"))}))
        total = gates = 0
        for nome, fn in (("automations", ingest_automations), ("inbox", ingest_inbox), ("gates", reconciliar_gates)):
            try:
                if nome == "gates":
                    gates = fn()
                else:
                    total += fn()
            except Exception as e:  # noqa: BLE001
                log(f"ingest {nome}: {type(e).__name__}: {e}")
        return total, gates


# ---------- pendencias.json (único escritor) ----------

def _load_pend():
    """Itens do pendencias.json; arquivo ausente é vazio, arquivo quebrado levanta ValueError (nunca é sobrescrito)."""
    try:
        with open(PEND) as f:
            d = json.load(f)
    except FileNotFoundError:
        return {"itens": []}
    except ValueError as e:
        raise ValueError(f"pendencias.json ilegível: {e}")
    if not isinstance(d, dict) or not isinstance(d.get("itens"), list):
        raise ValueError("pendencias.json sem a lista itens")
    return d


def _mutar_pend(fn, evento=None):
    """Lê, aplica fn(itens) e grava com tmp + rename, tudo sob flock: dois orq ao mesmo tempo não perdem item.

    Com `evento(resultado)`, o evento entra no mesmo passo: as duas gravações ficam fora do alcance do alarme do hook.
    """
    with contextlib.ExitStack() as pilha:
        pilha.enter_context(_trava("pend.lock"))
        if evento:
            pilha.enter_context(_trava("cursor.lock"))
        d = _load_pend()
        out = fn(d["itens"])
        d["atualizadoEm"] = datetime.now().astimezone().isoformat(timespec="seconds")
        with _sem_alarme():
            _write_json(PEND, d, indent=2)
            if evento:
                _grava_evento(evento(out))
    return out


def _resolve_gate(gate, resolucao, run=None):
    """Resolve o gate do Orca ligado a uma decisão e grava `gate_resolvido`; True se resolveu. `run` é o Run do gate (`gate_run`): o orca()
    liga o gerente a ele, ou usa o handle do coordenador que o segura, e a conferência de que o coordenador o comanda cabe na mesma trava.

    A falha vai para o log e não desfaz o fechamento da pendência. Sem o `gate_resolvido` no log, o próximo ingest tenta de novo
    (reconciliar_gates), e é isso que cobre o alarme de 3 s que vence depois da gravação. Recusa do Orca (gate já resolvido, fora do
    Run ligado) grava `gate_falha`; depois de MAX_TENTATIVAS recusas o gate deixa de ser tentado.
    """
    try:
        with trava_gerente():
            orca("gate-resolve", "--id", gate, "--resolution", resolucao, run=run)
    except RuntimeError as e:
        log(f"gate-resolve {gate}: recusado: {e}")
        append_event({"tipo": "gate_falha", "gate": gate, "erro": str(e)})
        return False
    except Exception as e:  # noqa: BLE001
        log(f"gate-resolve {gate}: {type(e).__name__}: {e}")
        return False
    append_event({"tipo": "gate_resolvido", "gate": gate})
    return True


def reconciliar_gates():
    """`pend done` de decisão com gate que ficou sem `gate_resolvido` (alarme de 3 s, Orca fora): resolve agora. Devolve quantos."""
    eventos = read_events()
    feitos = {e.get("gate") for e in eventos if e.get("tipo") == "gate_resolvido"}
    recusas = [e.get("gate") for e in eventos if e.get("tipo") == "gate_falha"]
    feitos |= {g for g in recusas if recusas.count(g) >= MAX_TENTATIVAS}
    n = 0
    for e in eventos:
        g = e.get("gate")
        if e.get("tipo") == "pend" and e.get("op") == "done" and g and g not in feitos:
            run = e.get("gate_run")
            with trava_gerente():  # a conferência e o gate-resolve na mesma ligação: o painel trocando de Run no meio gastaria as tentativas (M15)
                if run and not run_do_coordenador(run):  # o Orca só resolve gate do Run que o coordenador comanda; a recusa gastaria as tentativas
                    continue
                feitos.add(g)
                n += _resolve_gate(g, e.get("resposta") or "fechada sem resposta", run)
    return n


def _run_atual_id(como=None):
    """Id do Run ligado ao terminal que chama (ou ao handle `como`), ou None."""
    return (orca("run-current", como=como)["run"] or {}).get("id")


def pend_depois(item, hoje=None):
    """Motivo de a pendência estar em "Depois" (data futura, espera, envelhecida) ou None se está viva.

    `ate` no futuro esconde; `ate` vencida ou de hoje devolve a pendência à vista, mesmo esperando alguém ou envelhecida. Decisão com
    gate trava uma task e nunca envelhece para fora da vista.
    """
    hoje = hoje or datetime.now().date()
    if item.get("ate"):
        return f"até {item['ate']}" if item["ate"] > hoje.isoformat() else None
    if item.get("espera"):
        return f"esperando {item['espera']}"
    if item.get("gate"):
        return None
    dias = (hoje - datetime.fromisoformat(item["desde"]).date()).days if item.get("desde") else 0
    return f"parada há {dias} d" if dias > PEND_IDADE_DIAS else None


def pend_lista(todas=False, hoje=None):
    """Linhas de `orq pend lista`: as vivas, e com `todas` também as de Depois com o motivo."""
    itens = _load_pend()["itens"]
    linhas = []
    for depois in ((False, True) if todas else (False,)):
        for i in itens:
            motivo = pend_depois(i, hoje)
            if bool(motivo) == depois:
                linhas.append(f"{i['id']}  {i.get('tipo')}  {i.get('titulo')}" + (f"  [Depois: {motivo}]" if motivo else ""))
    return linhas


def pend_add(id_, tipo, titulo, detalhe=None, frente=None, link=None, comando=None, espera=None, task=None, ate=None, run=None):
    """Acrescenta uma pendência do usuário (formato do painel; `espera` opcional) e registra o evento.

    Com `task`, a decisão trava a task: cria o gate no Orca, no Run da task (`run`, senão o do coordenador; run_padrao recusa o gerente com
    vários Runs), e guarda o id na pendência (o hook ask o resolve).
    """
    id_, titulo = (id_ or "").strip(), (titulo or "").strip()
    if not id_ or not titulo:
        raise ValueError("pend add pede --id e --titulo")
    if tipo not in TIPOS_PEND:
        raise ValueError(f"tipo inválido: {tipo} (use {'|'.join(TIPOS_PEND)})")
    if tipo == "decisao" and len(id_) > ID_HEADER:
        raise ValueError(f"id de decisão tem até {ID_HEADER} caracteres para caber no header do AskUserQuestion ({id_!r} tem {len(id_)})")
    if tipo == "decisao" and espera:
        raise ValueError("decisão não tem espera: espera é pendência que aguarda terceiro e não vira pergunta")
    if ate:
        try:
            datetime.strptime(ate, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"--ate pede AAAA-MM-DD ({ate!r})")
    if task and tipo != "decisao":
        raise ValueError("--task só vale para decisão (o gate trava a task até a resposta)")
    gate = gate_run = None
    if task:
        run = run_padrao(run)
        if not run_do_coordenador(run):  # o gate-create não leva --run: fora do que o coordenador comanda nasceria no Run errado (B51)
            raise ValueError(f"o coordenador não comanda o Run {run}: {dica_ligar(run)}")
        try:
            res = orca("gate-create", "--task", task, "--question", titulo, run=run)
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            raise ValueError(f"gate-create falhou, a pendência não foi criada: {e}")
        g = res.get("gate") or res
        gate = g.get("id")
        if not gate:
            raise ValueError("gate-create não devolveu o id do gate, a pendência não foi criada")
        gate_run = g.get("run_id") or g.get("runId") or res.get("run_id") or run  # o gate nasce no Run pedido

    def add(itens):
        if any(i.get("id") == id_ for i in itens):
            raise ValueError(f"pendência {id_} já existe")
        item = {"id": id_, "tipo": tipo, "titulo": titulo}
        for k, v in (("detalhe", detalhe), ("frente", frente)):
            if v:
                item[k] = v
        item["desde"] = datetime.now().date().isoformat()
        for k, v in (("link", link), ("comando", comando), ("espera", espera), ("ate", ate), ("gate", gate), ("gate_run", gate_run)):
            if v:
                item[k] = v
        itens.append(item)
        return item

    try:
        return _mutar_pend(add, lambda item: {"tipo": "pend", "op": "add", "pend": id_, "pend_tipo": tipo, "titulo": titulo,
                                               **({"gate": gate} if gate else {}), **({"gate_run": gate_run} if gate and gate_run else {})})
    except Exception:
        if gate:
            _resolve_gate(gate, "cancelada: a pendência não foi criada", gate_run)
        raise


def pend_done(id_, resposta=None, atual=_DESCONHECIDO):
    """Fecha (remove) uma pendência aberta e resolve o gate dela, se houver. `resposta` fica no evento e na resolução.

    Sem `resposta`, vale a última resposta livre dada ao header (AskUserQuestion ou Lavish): o texto do usuário chega ao worker destravado.
    O gate é do Run em que nasceu (`gate_run`): se o coordenador não comanda esse Run o Orca o recusaria, então não tenta, e o item
    devolvido traz `aviso`; o próximo ingest resolve quando o coordenador voltar a comandá-lo. `atual` é o Run ligado ao terminal próprio, se o chamador já o sabe.
    """
    resposta = resposta or ultima_livre(read_events(), id_)

    def rm(itens):
        for i, item in enumerate(itens):
            if item.get("id") == id_:
                return itens.pop(i)
        raise ValueError(f"pendência {id_} não existe em pendencias.json")

    item = _mutar_pend(rm, lambda it: {"tipo": "pend", "op": "done", "pend": id_, **({"resposta": resposta} if resposta else {}),
                                       **({"gate": it["gate"]} if it.get("gate") else {}),
                                       **({"gate_run": it["gate_run"]} if it.get("gate") and it.get("gate_run") else {})})
    if item.get("gate"):
        run = item.get("gate_run")
        with trava_gerente():  # conferir e resolver na mesma ligação (M15)
            if run and not run_do_coordenador(run, atual):
                log(f"pend done {id_}: {aviso_gate(item['gate'], run)}")
                return {**item, "aviso": aviso_gate(item["gate"], run)}
            _resolve_gate(item["gate"], resposta or "fechada sem resposta", run)
    return item


def estado(entrada=None):
    events, cur = read_events(), _cursor_ro()
    txt = resumo(events, _read_json(_path("aberto.json")), _read_json(PEND), entrada, cursor=cur, turnos=_turnos_ro(), painel=aviso_painel())
    return "\n".join([txt, *linhas_noite(cur, events)])


# ---------- modo noite: orçamento e disjuntor ----------

NOITE_FALHAS = 3  # falhas seguidas que fecham o despacho


def noite_ativa(cur):
    """O estado do modo noite no cursor.json ({ate, ligada_em, max_despachos, max_falhas}) ou None se desligado."""
    n = _dict(cur).get("noite")
    return n if isinstance(n, dict) and n.get("ate") else None


def _desde_noite(events, noite, tipo):
    return [e for e in events if e.get("tipo") == tipo and (e.get("ts") or "") >= noite["ligada_em"]]


def falhas_seguidas(events, msgs, noite):
    """Falhas seguidas no fim dos despachos desde `noite.ligada_em`, pelos worker_done da inbox e pelos `liberar` do log.

    Conta: worker_done `failed` sem reportPath, ou dispatch liberado sem worker_done (o worker morreu). Reinicia: `succeeded`, ou `failed`
    com reportPath (o worker explicou que não dá: decisão dele, não do ambiente). Dispatch ainda rodando não conta nem reinicia.
    """
    done = {}
    for m in msgs:
        p = _payload(m)
        if m.get("type") == "worker_done" and p.get("dispatchId"):
            done[p["dispatchId"]] = p
    liberados = {e.get("dispatch") for e in events if e.get("tipo") == "liberar"}
    n = 0
    for e in _desde_noite(events, noite, "despacho"):
        p = done.get(e.get("dispatch"))
        if p is None:
            n += e.get("dispatch") in liberados
        elif p.get("outcome") == "failed" and not p.get("reportPath"):
            n += 1
        else:
            n = 0
    return n


def noite_motivo(noite, events, msgs, agora):
    """Por que o `orq despachar` deve recusar (horário, teto de despachos ou falhas seguidas), ou None."""
    if agora >= _dt(noite["ate"]):
        return f"passou do horário da noite ({_hora_local(noite['ate'])})"
    teto = noite.get("max_despachos")
    if teto is not None and len(_desde_noite(events, noite, "despacho")) >= teto:
        return f"teto de {teto} despachos da noite"
    falhas = noite.get("max_falhas") or NOITE_FALHAS
    if msgs is not None and falhas_seguidas(events, msgs, noite) >= falhas:
        return f"{falhas} falhas seguidas de worker"
    return None


def noite_checar(agora=None):
    """Recusa o despacho com ValueError e grava `noite_parou` se o modo noite estourou o orçamento. Desligado: não faz nada."""
    noite = noite_ativa(_cursor_ro())
    if not noite:
        return
    events = read_events()
    try:
        msgs = orca("inbox", "--limit", "200", timeout=20)["messages"]
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:  # sem a inbox só as falhas seguidas ficam de fora: horário e teto valem
        log(f"noite: inbox falhou ({type(e).__name__}: {e}); falhas seguidas não conferidas")
        msgs = None
    motivo = noite_motivo(noite, events, msgs, agora or datetime.now(timezone.utc))
    if motivo:
        append_event({"tipo": "noite_parou", "motivo": motivo})
        raise ValueError(f"modo noite: {motivo}; nada foi despachado. Deixe a decisão em orq pend add ou rode orq noite desligar")


def linhas_noite(cur, events):
    """As duas linhas que o coordenador lê com o modo noite ligado (regras; orçamento ou motivo da parada). Vazio com o modo desligado."""
    noite = noite_ativa(cur)
    if not noite:
        return []
    parou = next((e for e in reversed(_desde_noite(events, noite, "noite_parou"))), None)
    teto = noite.get("max_despachos")
    gasto = f"{len(_desde_noite(events, noite, 'despacho'))}/{teto if teto is not None else '∞'} despachos, até {_hora_local(noite['ate'])}"
    return ["[orq noite] Regras: sem AskUserQuestion (estacione a decisão com orq pend add e siga no que é independente), sem push nem merge, "
            "pare de despachar ao estourar o orçamento.",
            f"[orq noite] {'Parou de despachar: ' + parou['motivo'] + f' ({gasto}); orq noite desligar libera.' if parou else 'Orçamento: ' + gasto + f', {noite.get('max_falhas') or NOITE_FALHAS} falhas seguidas fecham.'}"]


def noite_ligar(ate, max_despachos=None, max_falhas=NOITE_FALHAS, agora=None):
    """Liga o modo noite até o próximo HH:MM local. Devolve o estado gravado."""
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", ate or "")
    if not m or int(m[1]) > 23 or int(m[2]) > 59:
        raise ValueError(f"--ate espera HH:MM (recebi {ate!r})")
    if (max_despachos is not None and max_despachos < 1) or max_falhas < 1:
        raise ValueError("--max-despachos e --max-falhas pedem um número maior que 0")
    agora = (agora or datetime.now(timezone.utc)).astimezone()
    fim = agora.replace(hour=int(m[1]), minute=int(m[2]), second=0, microsecond=0)
    fim += timedelta(days=1) if fim <= agora else timedelta(0)
    noite = {"ate": fim.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "ligada_em": now(), "max_despachos": max_despachos, "max_falhas": max_falhas}
    _cursor_mut(lambda c: c.__setitem__("noite", noite))
    append_event({"tipo": "noite_ligar", **{k: v for k, v in noite.items() if k != "ligada_em"}})
    return noite


def noite_desligar():
    """Desliga o modo noite; devolve se estava ligado."""
    ligado = bool(noite_ativa(_cursor_ro()))
    _cursor_mut(lambda c: c.pop("noite", None))
    if ligado:
        append_event({"tipo": "noite_desligar"})
    return ligado


# ---- modo noite: ações externas e ambiente do worker ----

NOITE_GIT_CONFIG = ("commit.gpgsign", "false")  # sem assinatura: o pinentry não tem quem responda de madrugada


def noite_ambiente(base=None):
    """O ambiente extra do worker na noite: git sem prompt de credencial e sem assinatura. Soma ao GIT_CONFIG_COUNT que já existe, não o
    sobrescreve. Devolve {variável: valor}."""
    base = os.environ if base is None else base
    n = int(base.get("GIT_CONFIG_COUNT") or 0) if str(base.get("GIT_CONFIG_COUNT") or "0").isdigit() else 0
    return {"GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_COUNT": str(n + 1), f"GIT_CONFIG_KEY_{n}": NOITE_GIT_CONFIG[0], f"GIT_CONFIG_VALUE_{n}": NOITE_GIT_CONFIG[1]}


_EXT_PRE = r"(?:^|[;&|(\n]\s*)(?:(?:\w+=\S*|rtk(?:\s+proxy)?|env|command|time|sudo)\s+)*"  # só em posição de comando, como o worker-routing-guard
_EXT_GIT = _EXT_PRE + r"git((?:\s+-\S+(?:\s+\S+)?)*)\s+"
_EXT_GH = _EXT_PRE + r"gh(?:\s+-\S+(?:\s+\S+)?)*\s+"
EXTERNAS_NOITE = [  # (o que é negado, regex sobre o comando sem aspas nem heredoc)
    ("git push", re.compile(_EXT_GIT + r"push(?![-\w])")),
    ("gh pr merge", re.compile(_EXT_GH + r"pr\s+merge(?![-\w])")),
    ("gh workflow run (deploy)", re.compile(_EXT_GH + r"workflow\s+run(?![-\w])")),
    ("git commit --no-verify", re.compile(_EXT_GIT + r"commit(?![-\w])[^;&|\n]*?\s(?:--no-verify|-[aeiopqsuvz]*n[a-zA-Z]*)(?=\s|$)")),
    ("orca worktree rm --force", re.compile(_EXT_PRE + r"orca\s+worktree\s+rm(?![-\w])[^;&|\n]*\s(?:--force|-f)(?![-\w])")),
]
_EXT_RESET = re.compile(_EXT_GIT + r"reset(?![-\w])[^;&|\n]*\s--hard(?![-\w])")


def _sem_texto(cmd):
    """O comando sem corpo de heredoc nem texto entre aspas: o que está ali (uma mensagem de commit, um echo) não é comando."""
    cmd = re.sub(r"<<-?\s*([\'\"]?)(\w+)\1([^\n]*)\n.*?\n[ \t]*\2[ \t]*(?=\n|$)", r"\3", cmd, flags=re.S)
    return re.sub(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'', '""', cmd)


def _externas_negada(ev, cur):
    """O nome da ação externa que o Bash de `ev` faria com o modo noite ligado em `cur`, ou None. `git reset --hard` só vale dentro de
    worktree ligada (a de um worker); no checkout principal perde o trabalho de todos. Só lê o cursor e, no reset, o git local."""
    if (ev.get("tool_name") != "Bash" or not isinstance(ev.get("tool_input"), dict) or not isinstance(ev["tool_input"].get("command"), str)
            or "--help" in ev["tool_input"]["command"]):
        return None
    cmd = _sem_texto(ev["tool_input"]["command"])
    achado = next((nome for nome, rx in EXTERNAS_NOITE if rx.search(cmd)), None)
    m = None if achado else _EXT_RESET.search(cmd)
    if not (achado or m) or not noite_ativa(cur):
        return None
    if achado:
        return achado
    d = ev.get("cwd") or os.getcwd()
    c = LUGAR_GIT_C.search(m.group(1) or "")
    if c:
        d = os.path.join(d, os.path.expanduser(c.group(1).strip("'\"")))
    gd, comum = ((_git(d, "rev-parse", "--absolute-git-dir", "--git-common-dir") or "").split() + ["", ""])[:2]
    if not gd or os.path.realpath(gd) != os.path.realpath(os.path.join(d, comum)):
        return None  # fora de repositório ou numa worktree ligada
    return "git reset --hard no checkout principal"


def hook_externas(ev, run):
    """PreToolUse de Bash, em toda sessão (worker incluído): com o modo noite ligado nega push, merge de PR, deploy, commit sem hook,
    `orca worktree rm --force` e `git reset --hard` no checkout principal. A mensagem diz para estacionar e como desligar. Desligado, nada muda."""
    nome = _externas_negada(ev, _cursor_ro())
    if not nome:
        return None
    motivo = (f"{MARCA} modo noite: `{nome}` é ação externa ou contorna um hook, e ninguém está olhando. Estacione a decisão com `orq pend add` e siga no que "
              "é independente. Se o usuário voltou e autorizou, `orq noite desligar` libera. Commit que falha no pre-commit não se contorna: repare o que o hook apontou.")
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": motivo}}


# ---------- coordenador x worker ----------

def _papeis():
    return _dict(_cursor_ro().get("papeis"))


def coordenador(ev):
    """O Run desta sessão se ela coordena (Run ligado e sessão que não é de worker); senão None. Nada grava em worker sem despacho.

    O papel de worker vem do preâmbulo de despacho, que é o primeiro prompt de todo worker (origem `despacho`): vale antes de qualquer
    Run ligado e fica em cursor.json (papeis), então um worker que roda run-create continua worker. Numa sessão que já tem Run registrado
    (coordenador) o preâmbulo não a vira worker. O `worker-list` do Orca não serve
    de sinal: sem --run ele lista só o Run ligado ao terminal, e o Run que o worker criou nunca o tem. Sessão sem preâmbulo e sem papel
    guardado é coordenador quando há Run ligado.
    """
    if not os.environ.get("ORCA_TERMINAL_HANDLE"):
        return None
    sid = ev.get("session_id") or ""
    if origem(ev.get("prompt")) == "despacho":
        if _papeis().get(sid) == "worker":
            return None
        if not _dict(_cursor_ro().get("runs")).get(sid):
            def grava(c):
                _sub(c, "papeis")[sid] = "worker"

            _cursor_mut(grava)
            return None
        # o preâmbulo é o primeiro prompt de todo worker: numa sessão que já coordenava, foi o usuário quem o colou (para perguntar algo)
        log(f"preâmbulo de despacho numa sessão que já coordena ({sid[:8]}): segue como coordenador")
    if _papeis().get(sid) == "worker":
        return None
    run = orca("run-current")["run"]
    if run is None:
        return None
    lembrar_run(sid, run["id"])
    return run


# ---------- hooks ----------

def lembrar_run(sid, run_id):
    """Guarda em cursor.json o último Run visto por session_id (só de coordenador); grava só quando muda."""
    if _dict(_cursor_ro().get("runs")).get(sid) == run_id:
        return
    _cursor_mut(lambda c: _sub(c, "runs").__setitem__(sid, run_id))


def binding_perdido(ev, ultimo):
    """Sessão que já teve Run e agora vem com run-current null (hibernação ou resume): registra e avisa."""
    append_event({"tipo": "binding_perdido", "run": ultimo, "sessao": (ev.get("session_id") or "")[:8]})
    log(f"binding_perdido: sessão {(ev.get('session_id') or '')[:8]} sem Run, último {ultimo}")
    ctx = f"[orq] binding perdido: rode run-use --id {ultimo}"
    return {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit", "additionalContext": ctx}}


def marcar_chegada(org):
    """Guarda em cursor.json a última mensagem de sistema (orca | notificacao) e a hora: o hook ask compara com a resposta."""
    _cursor_mut(lambda c: c.__setitem__("chegada", {"origem": org, "t": time.time()}))


def confirmar_lotes(run_id, res):
    """`res` é a resposta de um `check` que consome. Confirma (--ack) os lotes seguidos que são só heartbeat, até HB_LOTES, e grava
    um evento heartbeat_absorvido. Devolve (evento ou None, resposta do primeiro lote que não é só heartbeat ou do último).

    O --ack confirma o lote e já traz o próximo. Um lote que não é só heartbeat não é confirmado: fica na entrega em aberto, que o Orca
    repete no check do coordenador. Confirmar de novo o mesmo lote é inofensivo no Orca (repete a entrega atual, conferido num Run de
    teste), então o hook e o waiter (orca-wait-runs.py) podem chamar isto ao mesmo tempo sem double-ack nem perder mensagem; no máximo o
    evento sai duas vezes, e o painel guarda só o último sinal de cada dispatch.
    """
    sinais, entregas = [], []
    for _ in range(HB_LOTES):
        msgs = res.get("messages") or []
        if not res.get("deliveryId") or not so_heartbeats(msgs):
            break
        sinais += [sinal_de_vida(m) for m in msgs]
        entregas.append(res["deliveryId"])
        res = orca("check", "--run", run_id, "--ack", res["deliveryId"])
    if not entregas:
        return None, res
    with _trava("cursor.lock"), _sem_alarme():  # já confirmado: o evento e a marca do lote entram juntos, fora do alcance do alarme
        ev = _grava_evento({"tipo": "heartbeat_absorvido", "run": run_id, "entregas": entregas, "heartbeats": sinais})
        cur = _read_cursor()
        _sub(cur, "hb_absorvido")[run_id] = time.time()
        _write_json(_path("cursor.json"), cur)
    return ev, res


def absorver_heartbeats(run_id, pendentes=None):
    """Se TODAS as mensagens não confirmadas do Run são heartbeat, consome, confirma e devolve o evento gravado; senão None.

    Só lê com --peek antes: com qualquer mensagem que não seja heartbeat (ou de tipo desconhecido) nada é consumido nem confirmado.
    `pendentes` são as mensagens que o chamador já leu com --peek.
    """
    if pendentes is None:
        pendentes = orca("check", "--run", run_id, "--peek")["messages"]
    if not so_heartbeats(pendentes):
        return None
    with trava_gerente():  # o check e o ack na mesma ligação do gerente ao Run
        return confirmar_lotes(run_id, orca("check", "--run", run_id))[0]


def aviso_atrasado(run_id):
    """O último lote de heartbeats do Run foi absorvido há menos de HB_JANELA_S? Então um aviso com a caixa vazia é do lote que já saiu."""
    t = _dict(_cursor_ro().get("hb_absorvido")).get(run_id)
    return isinstance(t, (int, float)) and 0 <= time.time() - t < HB_JANELA_S


# Marca fixa no começo de tudo que o Claude Code mostra ao usuário (reason, systemMessage, permissionDecisionReason): o usuário
# distingue na hora o que veio do orq. Emoji, não cor ANSI: ver docs/design.md. additionalContext (só o coordenador lê) não leva marca.
MARCA = "🟣 orq ·"


def _run_curto(run_id):
    return run_id if len(run_id) <= 15 else run_id[:10] + "…"


def bloqueio_de_outro_run(run_id):
    """Aviso de um Run que este coordenador não coordena: check, --peek e --ack dão consumer_fenced, então o sinal é lido no inbox (que cobre
    todos os Runs) e nada é consumido nem confirmado. Bloqueia se as mensagens ainda não lidas (`read` 0) para o Run são todas heartbeat, sem
    limite de tempo (M11: um heartbeat novo não pode esconder um worker_done antigo sem ack); grava heartbeat_visto. Qualquer outra mensagem
    não lida, ou nenhuma, deixa passar. As mensagens saem num lote quando o coordenador faz run-use.
    """
    nao_lidas = [x for x in orca("inbox", "--limit", "200")["messages"]
                 if isinstance(x, dict) and x.get("to_handle") == f"run:{run_id}" and not x.get("read")]
    if not so_heartbeats(nao_lidas):
        return None
    append_event({"tipo": "heartbeat_visto", "run": run_id, "heartbeats": [sinal_de_vida(x) for x in nao_lidas]})
    return {"decision": "block", "reason": f"{MARCA} {len(nao_lidas)} sinal(is) de vida do Run {_run_curto(run_id)} vistos, não são para o coordenador"}


def bloqueio_de_heartbeat(ev, run):
    """Aviso do Orca ("You have N orchestration message. Run `orca orchestration check --run <r>`") que só traz heartbeat: absorve e bloqueia.

    Devolve a saída do UserPromptSubmit que impede o processamento, ou None (o prompt passa). Passa quando o aviso não cita Run, cita um
    Run diferente do ligado (o check daria consumer_fenced), há qualquer mensagem que não seja heartbeat ou a caixa está vazia sem lote
    recente. Um erro do Orca sobe para o chamador, que também deixa passar.
    """
    m = AVISO_RUN.search(ev.get("prompt") or "")
    if not m:
        return None
    alvo = m.group(1)
    if alvo != run["id"] and not run_do_coordenador(alvo):
        return bloqueio_de_outro_run(alvo)
    pendentes = orca("check", "--run", alvo, "--peek")["messages"]  # Run do agent manager: o orca() liga o gerente a ele, e o check cru do coordenador vale
    if not pendentes:
        if not aviso_atrasado(alvo):
            return None
        return {"decision": "block", "reason": f"{MARCA} aviso de sinais de vida já absorvidos ({_run_curto(alvo)})"}
    ev = absorver_heartbeats(alvo, pendentes)
    if not ev:
        return None
    return {"decision": "block", "reason": f"{MARCA} {len(ev['heartbeats'])} sinais de vida absorvidos ({_run_curto(alvo)})"}


def hook_prompt(ev, run):
    org = origem(ev.get("prompt"))
    texto, com_aviso = separa_aviso(ev.get("prompt")) if org == "usuario" else (ev.get("prompt") or "", False)
    if com_aviso and not texto.strip():
        org = "orca"
    if org == "orca":
        try:
            bloqueio = bloqueio_de_heartbeat(ev, run)
        except TimeoutError:
            raise  # o teto de 3 s do hook vale para o hook inteiro
        except Exception as e:  # noqa: BLE001 - fail-open: sem saber, o aviso passa e o coordenador é acordado
            log(f"heartbeat: {type(e).__name__}: {e}")
            bloqueio = None
        if bloqueio:
            return bloqueio
    if org in ("orca", "notificacao"):
        marcar_chegada(org)
    elif com_aviso:
        marcar_chegada("orca")
    if org == "orca":
        refresh_bg(refresh=False)  # o aviso do Orca é o sinal de mensagem nova: ingere o inbox já, sem refazer o aberto.json (um heartbeat por 100 s)
    if org != "usuario":
        ln = linhas_noite(_cursor_ro(), read_events()) if org in ("orca", "notificacao") else []  # a noite acorda o coordenador por aviso, não por usuário
        return {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit", "additionalContext": "\n".join(ln)}} if ln else None
    entrada = append_event({"tipo": "entrada", "origem": "usuario", "texto": texto[:2000], "sessao": (ev.get("session_id") or "")[:8],
                            **({"com_aviso": True} if com_aviso else {})}, novo_id=True)
    ctx = estado(entrada)
    refresh_bg()
    return {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit", "additionalContext": ctx}}


def hook_stop(ev, run):
    # modo aviso (fatia 1): nunca bloqueia; a fatia 5 troca o systemMessage por decision=block com stop_hook_active
    sem = abertas(read_events())
    if not sem:
        return None
    ids = [e["id"] for e in sem]
    append_event({"tipo": "gate_aviso", "abertas": ids, "sessao": (ev.get("session_id") or "")[:8]})
    rec = cursor_recuperado(_cursor_ro(), datetime.now(timezone.utc))
    citadas = ", ".join(f"{e['id']} ({_cita(e.get('texto'))!r})" for e in sem[:3]) + (f" +{len(sem) - 3}" if len(sem) > 3 else "")
    return {"systemMessage": f"{MARCA} {len(ids)} entrada(s) sem efeito: {citadas}. Use: orq intake <e> <efeito> [ref]"
                             + (f" [aviso] {aviso_recuperado(rec)}" if rec else "")}


def _ask_dados(ev):
    """(questions, answers) do PostToolUse de AskUserQuestion. As answers vêm indexadas pelo texto da pergunta,
    no tool_response (o toolUseResult do transcrito) ou, se o harness as puser lá, no tool_input."""
    ti = ev.get("tool_input") if isinstance(ev.get("tool_input"), dict) else {}
    tr = ev.get("tool_response") if isinstance(ev.get("tool_response"), dict) else {}
    return ti.get("questions") or tr.get("questions") or [], tr.get("answers") or ti.get("answers") or {}


def _ask_notas(ev):
    """Notas do usuário por pergunta (annotations[pergunta].notes), no tool_response ou, se o harness as puser lá, no tool_input."""
    ti = ev.get("tool_input") if isinstance(ev.get("tool_input"), dict) else {}
    tr = ev.get("tool_response") if isinstance(ev.get("tool_response"), dict) else {}
    return _dict(tr.get("annotations") or ti.get("annotations"))


def _chegada_recente():
    c = _dict(_cursor_ro().get("chegada"))
    return c if isinstance(c.get("t"), (int, float)) and 0 <= time.time() - c["t"] < SUSPEITA_S else None


def _ao_coordenador(m):
    """A mensagem tem por destino o mailbox de um Run (`run:<id>`)? É a que o Orca avisa no terminal do coordenador; as para
    `dispatch:` e `term_` vão a um worker."""
    return str(m.get("to_handle") or "run:").startswith("run:")


def _mensagem_recente():
    """Mensagem que o Orca entregou ao coordenador a ±JANELA_ORCA_S do agora (inbox do Orca, não do nosso hook), ou None.

    É o sinal de que o aviso "You have N orchestration message" foi digitado no terminal perto da resposta. Vale a de qualquer Run:
    o Orca avisa o terminal também de Runs a que ele não está mais ligado, e o usuário trabalha com um Run por frente.
    """
    agora = time.time()
    for m in orca("inbox", "--limit", "20")["messages"]:
        if _ao_coordenador(m) and abs(agora - _dt(m.get("delivered_at") or m["created_at"]).timestamp()) <= JANELA_ORCA_S:
            return m
    return None


def _recomendada(resposta, q):
    """A resposta é só a opção recomendada: a primeira, ou a que traz "(Recomendado)"/"(Recommended)" no label?"""
    ops = q.get("options") or []
    achadas, resto = marcadas(resposta, ops)
    if not ops or resto or len(achadas) != 1:
        return False
    return achadas[0] == ops[0].get("label") or bool(re.search(r"\((?:recomendad[oa]|recommended)\b[^)]*\)", achadas[0], re.I))


def hook_ask(ev, run):
    """Grava cada pergunta respondida e fecha a pendência do header (ou, no `ja-fez`, as marcadas).

    Só uma opção marcada fecha: texto livre ("Other", ou opção com texto junto) é gravado com `livre` e a pendência segue aberta,
    com o aviso "resposta livre em <header>: feche com orq pend done se decidiu".

    Resposta suspeita (não fecha nada): o hook de prompt viu um aviso do Orca colado nela, o texto dela é o próprio aviso, ou o Orca
    entregou uma mensagem ao Run a ±5 s e a resposta é a opção recomendada, que é o que o Enter no widget escolhe.
    """
    qs, answers = _ask_dados(ev)
    notas = _ask_notas(ev)
    if not answers:
        # sem answers: o usuário dispensou a pergunta, ou o formato do payload mudou; o log distingue os dois casos
        log(f"ask sem answers: tool_input={sorted((ev.get('tool_input') or {}))} tool_response={sorted(ev['tool_response']) if isinstance(ev.get('tool_response'), dict) else type(ev.get('tool_response')).__name__}")
        return None
    chegada = _chegada_recente()
    recente = None if chegada else _mensagem_recente()  # falha do Orca aqui: nada foi gravado ainda, o hook só loga
    sessao, ask = (ev.get("session_id") or "")[:8], ev.get("tool_use_id")
    suspeitas_, livres_, avisos = [], [], []
    for q in qs:
        resposta = answers.get(q.get("question"))
        if resposta is None:
            continue
        header, base = q.get("header") or "", {"header": q.get("header") or "", "pergunta": q.get("question"), "resposta": resposta,
                                                "sessao": sessao, "ask": ask}
        texto = origem(resposta) in ("orca", "notificacao")
        if chegada or texto or (recente and _recomendada(resposta, q)):
            motivo = chegada["origem"] if chegada else "texto" if texto else "inbox"
            append_event({"tipo": "resposta_suspeita", **base, "chegada": motivo, **({"msg": recente["id"]} if motivo == "inbox" else {})})
            suspeitas_.append(header)
            continue
        itens = {i.get("id"): i for i in _load_pend()["itens"]}
        marcadas_, resto = marcadas(resposta, q.get("options") or [])
        nota = str(_dict(notas.get(q.get("question"))).get("notes") or "").strip()  # opção marcada com nota ("só se X") não é opção limpa
        livre = bool(resto) or not marcadas_ or bool(nota)
        if header == "ja-fez":
            ids = [m.group(1) for o in q.get("options") or [] if (o.get("label") in marcadas_)
                   for m in [re.match(r"\[([^\]]+)\]", o.get("description") or "")] if m]
        else:  # só decisão fecha pelo header, e só com uma opção marcada
            decisao = (itens.get(header) or {}).get("tipo") == "decisao"
            ids = [header] if decisao and not livre else []
            if decisao and livre:
                livres_.append(header)
        fechou = [i for i in ids if i in itens]
        append_event({"tipo": "resposta", **base, **({"livre": True} if livre else {}), **({"nota": nota} if nota else {}),
                      **({"fechou": fechou} if fechou else {})})
        for i in fechou:
            try:
                feito = pend_done(i, resposta if header != "ja-fez" else None, _DESCONHECIDO if runs_do_gerente() else run["id"])
            except ValueError as e:  # outro orq fechou no meio: as próximas perguntas seguem
                log(f"ask: {e}")
            else:
                if feito.get("aviso"):
                    avisos.append(feito["aviso"])
    ctx = []
    if suspeitas_:
        ctx.append("; ".join(f"[orq] resposta suspeita em {h}: confirme" for h in suspeitas_) +
                   " (chegou junto de uma mensagem do Orca; a pendência continua aberta). Refaça a pergunta antes de agir.")
    if livres_:
        ctx.append("; ".join(f'[orq] resposta livre em {h}: se decidiu, feche com orq pend done {shlex.quote(h)} --resposta "<o que foi decidido>"' for h in livres_) +
                   " (a pendência e o gate continuam abertos).")
    if avisos:
        ctx.append("; ".join(f"[orq] {a}" for a in avisos) + ".")
    return {"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": " ".join(ctx)}} if ctx else None


def despachos_ativos():
    """[{run, task, dispatch}] dos despachos com dispatchStatus `dispatched` em qualquer Run, com cache de ASK_GUARD_TTL segundos.

    Uma chamada sem o terminal ligado lista todos os Runs (`scope.source` all), mais nova primeiro, em vez de run-list + um worker-list por
    Run (cerca de 70 chamadas). Se o Orca devolver a lista escopada, levanta: o guard falha aberto em vez de olhar só um Run.
    Despacho de Run que outro terminal vivo coordena não conta (_de_outro_coordenador); o cache guarda a lista já filtrada.
    ponytail: só as PAGINAS_ATIVOS páginas mais novas; despacho `dispatched` mais velho que 300 outros está morto, e ask-guard.off libera.
    """
    cache = _read_json(_path("ativos.json"))
    if isinstance(cache, dict) and isinstance(cache.get("ativos"), list) and isinstance(cache.get("t"), (int, float)) \
            and 0 <= time.time() - cache["t"] < ASK_GUARD_TTL:
        return cache["ativos"]
    ativos = [{"run": w.get("runId"), "task": w.get("taskId"), "dispatch": w.get("dispatchId")}
              for w in _workers_todos() if w.get("dispatchStatus") == "dispatched"]
    ativos = _de_outro_coordenador(ativos)
    _write_json(_path("ativos.json"), {"t": time.time(), "ativos": ativos})
    return ativos


def _de_outro_coordenador(ativos):
    """Tira os despachos de Run coordenado por outro terminal que ainda existe: o Orca avisa o coordenador ligado ao Run, então eles não
    são deste terminal. Coordenador fechado (terminal_handle_stale) não avisa ninguém: o despacho continua valendo. Erro do Orca sobe."""
    meu = {os.environ.get("ORCA_TERMINAL_HANDLE"), handle_orca()}  # com o agent manager ligado, o Run dele e o que o coordenador segura são deste coordenador (M14)
    dono = {}
    for run in {a["run"] for a in ativos if a.get("run")}:
        h = (orca("run-show", "--id", run)["run"] or {}).get("coordinator_handle")
        if h and h not in meu:
            dono[run] = h
    vivos = {}
    for h in set(dono.values()):
        try:
            orca("show", "--terminal", h, area="terminal")
            vivos[h] = True
        except RuntimeError as e:
            if "terminal_handle_stale" not in str(e):
                raise
            vivos[h] = False
    return [a for a in ativos if not vivos.get(dono.get(a.get("run")))]


def hook_guard(ev, run):
    """PreToolUse de AskUserQuestion, só no coordenador: com despacho ativo em qualquer Run a caixa é recusada e a decisão vai pelo Lavish.

    Fail-open (run_hook): se o Orca não responder, a caixa passa e o erro vai para o log.
    """
    if ev.get("tool_name") != "AskUserQuestion" or os.path.exists(_path("ask-guard.off")):
        return None
    ativos = despachos_ativos()
    if not ativos:
        return None
    lista = ", ".join(f"{(a.get('task') or '?')[:18]} ({a.get('run')}, {a.get('dispatch')})" for a in ativos[:3]) + (f" +{len(ativos) - 3}" if len(ativos) > 3 else "")
    motivo = (f"{MARCA} {len(ativos)} despacho{'s' if len(ativos) > 1 else ''} ativo{'s' if len(ativos) > 1 else ''} ({lista}). Com worker rodando, a decisão do "
              "usuário vai pelo Lavish, não pelo AskUserQuestion: monte a página com lavish-axi (lote com itens id, header igual ao id da pendência, "
              'resposta e disposicao; só a disposicao "escolha" (ou "manter"/"trocar") com resposta fecha a decisão, "livre", "adiar" e "conversar" '
              "a deixam aberta), rode `lavish-axi poll <arquivo.html>` e grave a saída do poll, crua, com `orq lavish-resposta <arquivo>` "
              "(`-` lê da entrada padrão). O AskUserQuestion só vale sem worker ativo. Despacho preso? "
              "`orca orchestration worker-stop --dispatch <id>` ou `touch ~/.claude/orq/ask-guard.off`.")
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": motivo}}


LUGAR_ESCRITA = re.compile(r"(^|[;&|(]\s*)((\w+=\S*|rtk(?:\s+proxy)?|env|command|time|sudo)\s+)*git((?:\s+-\S+(?:\s+\S+)?)*)\s+(commit|push)\b")  # M18: prefixos antes do git
LUGAR_GIT_C = re.compile(r"\s-C\s+(\S+)")


def hook_lugar(ev, run):
    """PreToolUse de Bash/Edit/Write, só no coordenador: AVISA (não bloqueia) escrita no lugar errado, no checkout principal fora da
    branch padrão ou com o cwd numa worktree que não é a do coordenador (CLAUDE_PROJECT_DIR). Só olha comando de escrita: git commit/push e edição de arquivo."""
    ferr, ti = ev.get("tool_name"), ev.get("tool_input") or {}
    if ferr == "Bash":
        m = LUGAR_ESCRITA.search(ti.get("command") or "")
        if not m:
            return None
        d = ev.get("cwd") or os.getcwd()
        c = LUGAR_GIT_C.search(m.group(4) or "")  # `git -C <dir> commit` escreve em <dir>, não no cwd
        if c:
            d = os.path.join(d, os.path.expanduser(c.group(1).strip("'\"")))
            d = d if os.path.isdir(d) else ev.get("cwd") or os.getcwd()
    elif ferr in ("Edit", "Write", "MultiEdit", "NotebookEdit"):
        d = os.path.dirname(ti.get("file_path") or ti.get("notebook_path") or "")
        d = d if os.path.isdir(d) else ev.get("cwd") or os.getcwd()
    else:
        return None
    gd, comum, topo = ((_git(d, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel") or "").split() + ["", "", ""])[:3]
    if not gd:
        return None
    comum = os.path.realpath(os.path.join(d, comum))
    if os.path.realpath(gd) != comum:  # worktree ligada: só é engano se não é onde o coordenador mora
        casa = os.environ.get("CLAUDE_PROJECT_DIR")
        if casa and os.path.realpath(casa) == os.path.realpath(topo):
            return None
        aviso = f"o cwd está na worktree {topo}, que parece ser de um worker"
    else:
        ramo = (_git(d, "rev-parse", "--abbrev-ref", "HEAD") or "").strip()
        padrao = (_git(d, "symbolic-ref", "-q", "--short", "refs/remotes/origin/HEAD") or "main").strip().split("/", 1)[-1]
        if ramo in (padrao, "main", "master"):
            return None
        aviso = f"o checkout principal está em {ramo}, não em {padrao}"
    msg = f"{MARCA} lugar errado? {aviso}. Confira `git status --short --branch` e o cwd antes de escrever (aviso, nada foi bloqueado)."
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": msg}}


def hook_session(ev, run):
    """SessionStart: injeta o estado e os tickets abertos para a sessão nova retomar sem que ninguém conte nada."""
    if not os.path.exists(_path("aberto.json")):
        refresh_bg()  # sem cache o resumo diz "refresh em andamento": pede o refresh (B27)
    return {"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": contexto_sessao()}}


HOOKS = {"prompt": hook_prompt, "stop": hook_stop, "ask": hook_ask, "guard": hook_guard, "session": hook_session, "lugar": hook_lugar, "externas": hook_externas}


def run_hook(kind):
    """Só no coordenador (Run ligado e terminal que não é de worker). Fail-open: qualquer exceção vira exit 0 e uma linha no log."""
    def estouro(*_):
        raise TimeoutError(f"hook {kind} passou de {HOOK_TIMEOUT}s")
    try:
        signal.signal(signal.SIGALRM, estouro)
        signal.alarm(HOOK_TIMEOUT)
        if not os.environ.get("ORCA_TERMINAL_HANDLE"):
            return 0  # fora do Orca não há Run nem terminal: nem chama o Orca nem enche o log
        ev = json.load(sys.stdin)
        if kind == "externas":  # todo Bash, de qualquer sessão: só lê o cursor, sem Orca
            out = hook_externas(ev, None)
            if out:
                print(json.dumps(out, ensure_ascii=False))
            return 0
        if kind == "lugar":  # a cada Bash/Edit: sem Orca, o coordenador é a sessão que já tem Run guardado e não é worker
            sid = ev.get("session_id") or ""
            if _dict(_cursor_ro().get("runs")).get(sid) and _papeis().get(sid) != "worker":
                out = hook_lugar(ev, None)
                if out:
                    print(json.dumps(out, ensure_ascii=False))
            return 0
        run = coordenador(ev)
        if run is None:
            sid = ev.get("session_id") or ""
            if kind in ("prompt", "stop") and _papeis().get(sid) == "worker":
                registra_turno(kind, ev)  # o worker só grava o turno: sem Orca, sem Run
            ultimo = _dict(_cursor_ro().get("runs")).get(sid)
            if ultimo and kind == "guard" and _papeis().get(sid) != "worker":
                run = {"id": ultimo}  # binding perdido (hibernação, resume): a sessão que já coordenou continua com a caixa travada
            else:
                if ultimo and kind == "prompt" and origem(ev.get("prompt")) == "usuario":
                    print(json.dumps(binding_perdido(ev, ultimo), ensure_ascii=False))
                return 0
        out = HOOKS[kind](ev, run)
        if out:
            print(json.dumps(out, ensure_ascii=False))
    except Exception as e:
        log(f"hook {kind}: {type(e).__name__}: {e}")
    finally:
        signal.alarm(0)
    return 0


# ---------- comandos ----------

def intake(e, efeito, ref=None, run=None, nota=None):
    """Grava o efeito de uma entrada. Levanta ValueError quando a referência não existe."""
    if efeito not in EFEITOS:
        raise ValueError(f"efeito inválido: {efeito} (use {'|'.join(EFEITOS)})")
    eventos = read_events()
    alvo_e = next((x for x in eventos if x.get("id") == e and x.get("tipo") == "entrada"), None)
    if not alvo_e:
        raise ValueError(f"entrada {e} não existe")
    if efeito == "conversa" and alvo_e.get("origem") in ("relatorio", "relatorio_worker"):
        raise ValueError(f"entrada {e} é item de relatório ({_cita(alvo_e.get('texto'))!r}, {alvo_e.get('fonte')}): conversa não a trata; "
                         "use tarefa, steer, pend, decisao ou descartado --nota <motivo>")
    ev = {"tipo": "intake", "entrada": e, "efeito": efeito}
    if efeito in ("tarefa", "steer"):
        if not ref:
            raise ValueError(f"{efeito} pede o id da task")
        alvo = run_padrao(run)
        if not alvo:
            raise ValueError("sem Run ligado: passe --run")
        if efeito == "tarefa" and not run_do_coordenador(alvo):
            print(f"aviso: task-create --run {alvo} é recusado (consumer_fenced) porque o coordenador não comanda esse Run; "
                  f"para criar task nele, {dica_ligar(alvo)} antes (run-create e run-use tiram o coordenador do Run anterior).",
                  file=sys.stderr)
        if not any(t["id"] == ref for t in orca("task-list", "--run", alvo, timeout=20)["tasks"]):
            raise ValueError(f"task {ref} não existe no Run {alvo}")
        ev.update(ref=ref, run=alvo)
    elif efeito in ("pend", "decisao"):
        if not ref:
            raise ValueError(f"{efeito} pede o id da pendência")
        # uma decisão respondida já saiu do arquivo: vale também a pendência que o registro viu ser criada
        no_arquivo = any(i.get("id") == ref for i in (_read_json(PEND, {}) or {}).get("itens", []))
        no_log = any(x.get("tipo") == "pend" and x.get("op") == "add" and x.get("pend") == ref for x in eventos)
        if not (no_arquivo or no_log):
            raise ValueError(f"pendência {ref} não existe em pendencias.json nem no registro de eventos")
        ev["ref"] = ref
    if nota:
        ev["nota"] = nota
    append_event(ev)
    # o eco diz qual entrada acabou de ser fechada: quem fecha pelo id errado vê o texto
    return {**ev, "origem": alvo_e.get("origem") or "usuario", "texto": _cita(alvo_e.get("texto")), **({"fonte": alvo_e["fonte"]} if alvo_e.get("fonte") else {})}


def _itens_lavish(doc):
    """Itens (dicts com id) de qualquer lista `items` do que o `lavish-axi poll` devolveu: data.items solto, dentro de uma lista de prompts,
    aninhado ou dentro do texto de um prompt, depois de "Context data:" (é assim que o queuePrompt entrega o `data`).

    Um id repetido vale pela última ocorrência (o usuário mandou de novo)."""
    achados = {}

    def anda(x):
        if isinstance(x, dict):
            itens = x.get("items")
            if isinstance(itens, list):
                for i in itens:
                    if isinstance(i, dict) and i.get("id") is not None:
                        achados.pop(str(i["id"]), None)
                        achados[str(i["id"])] = i
            for v in x.values():
                if not isinstance(v, list) or v is not itens:
                    anda(v)
        elif isinstance(x, list):
            for v in x:
                anda(v)
        elif isinstance(x, str) and MARCA_LAVISH in x:
            with contextlib.suppress(ValueError):
                anda(json.JSONDecoder().raw_decode(x.split(MARCA_LAVISH, 1)[1].lstrip())[0])
    anda(doc)
    return list(achados.values())


def _itens_do_poll(texto):
    """Itens do lote a partir do arquivo: JSON (com data.items ou prompts) ou a saída crua do `lavish-axi poll`, que é TOON.

    No TOON o texto do prompt é uma string entre aspas no formato JSON, com o `data` como JSON escapado depois de "Context data:"; cada
    string entre aspas é decodificada e as que trazem a marca são lidas."""
    try:
        doc = json.loads(texto)
    except ValueError:
        doc = []
        for m in _STRING_JSON.finditer(texto):
            with contextlib.suppress(ValueError):
                doc.append(json.loads(m.group(0)))
    return _itens_lavish(doc)


def lavish_resposta(caminho):
    """Grava a resposta do usuário vinda do Lavish: a saída do `lavish-axi poll` (crua, ou o JSON dela; `-` lê a entrada padrão), com o lote
    de itens id, header, resposta, disposicao.

    Um evento `resposta_lavish` por item; o mesmo lote duas vezes não duplica (hash dos itens, então a saída crua e o JSON extraído dela são
    o mesmo lote). O `header` fecha a pendência de decisão quando a disposicao é uma escolha explícita (ESCOLHA_LAVISH) e há resposta; texto
    livre, adiamento (`adiar`, `conversar`) e pendência de ação ou aviso ficam abertos, como no AskUserQuestion (review 2, M3). O Run ligado é
    perguntado e a pendência fechada antes de o evento ser gravado: se o Orca falha, nada foi registrado e rodar de novo refaz (review 4, M8).
    Devolve {"lote", "itens": [{"item", "efeito"}], "avisos": [...]}.
    """
    if caminho == "-":
        bruto = sys.stdin.buffer.read()
    else:
        with open(caminho, "rb") as f:
            bruto = f.read()
    itens = _itens_do_poll(bruto.decode("utf-8", "replace"))
    if not itens:
        raise ValueError("nenhum item em Context data nem em data.items: isto não é a saída do lavish-axi poll")
    lote = hashlib.sha1(json.dumps(itens, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:10]
    ja = {(e.get("lote"), e.get("item")) for e in read_events() if e.get("tipo") == "resposta_lavish"}
    saida, avisos, atual = [], [], _DESCONHECIDO
    for it in itens:
        id_, header = str(it["id"]), str(it.get("header") or "")
        resposta, disp = str(it.get("resposta") or "").strip(), str(it.get("disposicao") or it.get("disposition") or "").strip()
        if disp == "conversar" or resposta == CONVERSAR_LAVISH:  # "vamos conversar" é adiamento: o marcador não é resposta do usuário
            resposta, disp = "", "conversar"
        if (lote, id_) in ja:
            saida.append({"item": id_, "efeito": "repetido"})
            continue
        pends = {i.get("id"): i for i in _load_pend()["itens"]}
        pend = pends.get(header)
        agregado = JA_FEZ in (id_, header)
        # "já fiz": a disposição `feito`, ou uma escolha explícita com a resposta "feito" (o lote de 29/09) ou no item `ja-fez`; texto livre, `adiar` e `conversar` não
        feito = disp == FEITO_LAVISH or (disp in ESCOLHA_LAVISH and bool(resposta) and (agregado or resposta.lower() == FEITO_LAVISH))
        explicita = feito or (disp in ESCOLHA_LAVISH and bool(resposta))
        sem_ids = feito and agregado and not it.get("ids")
        if sem_ids:  # B22: o texto livre não diz quais fechar ("feito o A, menos o B"); só a lista estruturada `ids` vale
            feito, explicita = False, False
            avisos.append(f'{header}: sem `ids` (lista estruturada) nada foi fechado; feche com orq pend done <id> --resposta "feito"')
        if feito and agregado:  # os ids das pendências feitas, na lista `ids` da página
            alvos = [i for i in pends if i in {str(x) for x in it.get("ids") or []}]
        elif feito:
            alvos = [header] if pend is not None else []
        else:
            alvos = [header] if explicita and pend is not None and pend.get("tipo") == "decisao" else []
        fechou = []
        for alvo in alvos:
            gr = pends[alvo].get("gate_run")
            if gr and atual is _DESCONHECIDO and not _do_gerente(gr):
                atual = _run_proprio()  # antes de gravar: o Orca fora do ar não deixa a pendência fechada sem o evento
            try:
                feito_ = pend_done(alvo, None if feito else resposta, atual)
            except ValueError as e:  # outro orq fechou no meio
                log(f"lavish-resposta: {e}")
                continue
            fechou.append(alvo)
            if feito_.get("aviso"):
                avisos.append(feito_["aviso"])
        append_event({"tipo": "resposta_lavish", "item": id_, "header": header, "resposta": resposta, "disposicao": disp, "lote": lote,
                      **({} if explicita else {"livre": True}), **({"fechou": fechou} if fechou else {})})
        if fechou:
            saida.append({"item": id_, "efeito": "fechou", "pend": fechou[0] if len(fechou) == 1 else fechou})
        elif pend is not None and pend.get("tipo") == "decisao":
            avisos.append(f'resposta livre em {header}: se decidiu, feche com orq pend done {shlex.quote(header)} --resposta "<o que foi decidido>"')
            saida.append({"item": id_, "efeito": "aberta"})
        else:
            saida.append({"item": id_, "efeito": "so registrada"})
    return {"lote": lote, "itens": saida, "avisos": avisos}


OCIOSO_MS = int(os.environ.get("ORQ_OCIOSO_MS") or 2000)  # quanto esperar o tui-idle de um terminal antes de dizer que ele está ocupado
STEER_ESPERA_S = float(os.environ.get("ORQ_STEER_ESPERA_S") or 2)  # o Orca digita o próprio aviso no worker ocupado: dá-lhe tempo antes de achar que ninguém avisou


def terminal_livre(handle):
    """None se o agente do terminal encerrou o turno (tui-idle) e a caixa dele está sem rascunho; senão o motivo: `ocupado` ou `rascunho`.

    Texto digitado num Claude Code no meio do turno fica parado na caixa (o Enter não submete, conferido em 29/09), e por cima de um
    rascunho do usuário viraria um prompt misturado: nos dois casos quem chama espera a próxima volta."""
    try:
        orca("wait", "--terminal", handle, "--for", "tui-idle", "--timeout-ms", str(OCIOSO_MS), area="terminal", timeout=OCIOSO_MS / 1000 + TIMEOUT_ORCA)
    except (RuntimeError, subprocess.TimeoutExpired):
        return "ocupado"
    try:
        rascunho = ((orca("read", "--terminal", handle, "--limit", "1", area="terminal").get("terminal") or {}).get("draft") or "").strip()
    except (RuntimeError, subprocess.TimeoutExpired):
        return None  # sem ler a caixa: o tui-idle já disse que dá para digitar
    return "rascunho" if rascunho else None


def digita(handle, texto):
    """Digita `texto` + Enter no agente do terminal, uma vez. Devolve `enviado`, `ocupado`/`rascunho` (nada foi digitado: repita depois) ou
    `falhou` (o Orca recusou: nada foi digitado). O tui-idle sozinho engana (satisfeito no começo do turno e por uns 20 s de turno, conferido
    no Orca real em 29/09): quem barra de verdade é o `agent_prompt_blocked` do send, que o Orca devolve com o agente no meio do turno. Se o Orca observa a submissão e não viu o turno começar, manda um Enter sozinho (numa caixa
    vazia não faz nada); timeout do send conta como enviado, porque o texto pode ter saído e repetir empilharia."""
    if motivo := terminal_livre(handle):
        return motivo
    espera = ("--wait-submit", "3")
    try:
        res = orca("send", "--terminal", handle, "--text", texto, "--enter", *espera, area="terminal", timeout=3 + TIMEOUT_ORCA)
    except subprocess.TimeoutExpired:
        return "enviado"
    except RuntimeError as e:
        return "ocupado" if "agent_prompt_blocked" in str(e) else "falhou"
    prompt = (res.get("send") or {}).get("prompt") or {}
    if prompt.get("observation") == "supported" and not set(prompt.get("stages") or []) - {"input_accepted"}:
        with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired):
            orca("send", "--terminal", handle, "--enter", *espera, area="terminal", timeout=3 + TIMEOUT_ORCA)
    return "enviado"


def _terminal_do_dispatch(run, dispatch):
    """agentTerminalHandle do dispatch no worker-list do Run, ou None (sem linha, ou o Orca falhou)."""
    try:
        return next((w.get("agentTerminalHandle") for w in _workers_todos(run) if w.get("dispatchId") == dispatch), None)
    except (RuntimeError, KeyError):
        return None


def _aviso_worker(handle):
    """O aviso no formato do Orca, digitado no terminal de um worker que ele não avisou."""
    return f"You have 1 orchestration message. Run `orca orchestration check --terminal {handle}`."


def lido_no_transcrito(dispatch, msg_id):
    """True se o transcrito da sessão do worker cita a mensagem (o `check` dele a trouxe), False se não, None sem transcrito.

    O `read` do inbox só vira 1 com o `check --ack`, e o worker que lê por `check --terminal` sem ack o deixa em 0 (conferido no Orca real em 29/09:
    o worker leu, respondeu "recebi" e a linha seguiu com read 0). O Orca não expõe a entrega em aberto (tabela `deliveries`), então a leitura
    se prova pelo id no fim do transcrito, achado pela sessão que o hook prompt gravou em turnos.json."""
    sid = _dict(_turnos_ro().get(dispatch)).get("sessao")
    for arq in glob.glob(os.path.join(PROJETOS, "*", f"{glob.escape(sid)}.jsonl")) if sid else []:
        try:
            with open(arq, "rb") as f:
                f.seek(max(0, f.seek(0, 2) - STEER_TRANSCRITO_BYTES))
                return msg_id.encode() in f.read()
        except OSError as e:
            log(f"transcrito {arq}: {type(e).__name__}: {e}")
    return None


def _orca_avisou(msg_id):
    """A mensagem tem `delivered_at` no inbox: o Orca digitou o aviso dele no terminal do worker (conferido em 29/09: a resposta do gerente a um
    worker preso no `ask` e o steer que o Orca não avisou ficam sem ele)."""
    try:
        return bool(msg_id) and any(m.get("id") == msg_id and m.get("delivered_at") for m in orca("inbox", "--limit", "20")["messages"] if isinstance(m, dict))
    except (RuntimeError, subprocess.TimeoutExpired):
        return False


def reentrega_steers(agora=None):
    """Uma volta do gerente sobre os steers abertos: as linhas do painel.

    Só toca o Orca com um steer vencido (STEER_LEITURA_S depois do envio ou da última redigitação). Mensagem com `read` no inbox ou citada no
    transcrito do worker (lido_no_transcrito): `steer_fim` (lido).
    Dispatch que já entregou: `steer_fim` (encerrado). Worker `parado` (turno encerrado, pelos hooks): redigita o aviso com `digita`, que não digita
    por cima de um turno em andamento nem de rascunho e por isso não gasta a tentativa. Depois de STEER_TENTATIVAS redigitações sem leitura grava o
    alerta `steer_nao_lido` (resumo e `orq agentes`) e o steer sai do acompanhamento. Worker ocupado não recebe nada, como no steer."""
    agora = agora or datetime.now(timezone.utc)
    vencidos = {m: s for m, s in steers_abertos(read_events(), agora).items() if (agora - s["ultima"]).total_seconds() >= STEER_LEITURA_S}
    if not vencidos:
        return []
    msgs = {m.get("id"): m for m in orca("inbox", "--limit", "200", timeout=20)["messages"] if isinstance(m, dict)}
    linhas, ags = [], None
    for m, s in vencidos.items():
        st, linha = s["steer"], msgs.get(m)
        base = {"msg_id": m, "task": st.get("task"), "dispatch": st.get("dispatch"), "run": st.get("run")}
        if linha is None:
            continue  # o inbox já não mostra a mensagem: sem prova de leitura nem de falta dela
        fonte = "orca" if linha.get("read") else "transcrito" if lido_no_transcrito(st.get("dispatch"), m) else None
        if fonte:
            append_event({"tipo": "steer_fim", **base, "motivo": "lido", "fonte": fonte})
            continue
        ags = ags if ags is not None else {a["dispatch"]: a for a in agentes()}
        ag = ags.get(st.get("dispatch"))
        if not ag or ag["estado"] in ("entregue", "liberado"):
            append_event({"tipo": "steer_fim", **base, "motivo": "encerrado"})
        elif s["tentativas"] >= STEER_TENTATIVAS:
            append_event({"tipo": "alerta", "alerta": "steer_nao_lido", **base})
            linhas.append(f"{st.get('task')}: steer não lido depois de {s['tentativas']} avisos (alerta gravado)")
        elif ag["estado"] == "parado" and digita(ag["terminal"], _aviso_worker(ag["terminal"])) == "enviado":
            append_event({"tipo": "steer_reentrega", **base, "tentativa": s["tentativas"] + 1})
            linhas.append(f"{st.get('task')}: steer não lido, aviso redigitado ({s['tentativas'] + 1}/{STEER_TENTATIVAS})")
    return linhas


def responder(msg_id, texto):
    """Responde a mensagem de um worker (`orca orchestration reply`) pelo handle do gerente, ligando antes o Run da mensagem.

    O Orca só deixa responder o terminal ligado ao Run da mensagem (consumer_fenced com o gerente em outro Run, visto em 29/09): o run vem da linha
    do inbox e o `orca()` liga o gerente a ele. Run que nem o gerente nem o coordenador seguram é recusado com o `run-use` que falta."""
    linha = next((x for x in orca("inbox", "--limit", "200", timeout=20)["messages"] if isinstance(x, dict) and x.get("id") == msg_id), None)
    if not linha:
        raise ValueError(f"mensagem {msg_id} não está entre as 200 mais novas do inbox")
    alvo = linha.get("run_id")
    fenced = f"o coordenador precisa comandar o Run da mensagem: {dica_ligar(alvo)}"
    if not run_do_coordenador(alvo):
        raise ValueError(f"a mensagem é do Run {alvo}; {fenced}")
    try:
        res = orca("reply", "--id", msg_id, "--body", texto, "--run", alvo, timeout=10)
    except RuntimeError as e:
        raise ValueError(fenced if "consumer_fenced" in str(e) else str(e))
    return append_event({"tipo": "resposta_worker", "msg_id": msg_id, "run": alvo, "dispatch": _dispatch_da_msg(linha), "texto": texto,
                         "resposta_id": (res.get("message") or res).get("id")})


PEDIDO_TITULO = "## Pedido do usuário"


def _texto_da_entrada(entrada):
    """Texto literal da entrada no events.jsonl (o hook grava até 2000 caracteres), ou None sem `entrada`. ValueError se não existe."""
    if not entrada:
        return None
    x = next((x for x in read_events() if x.get("id") == entrada and x.get("tipo") == "entrada"), None)
    if not x:
        raise ValueError(f"entrada {entrada} não existe")
    return x.get("texto") or ""


def steer(task, texto, run=None, entrada=None):
    """Manda `texto` ao dispatch da task (send --to dispatch:<id>) e registra.

    Recusa task que não está dispatched, entrada inexistente e Run que não é o ligado ao coordenador (o send daria consumer_fenced).
    """
    pedido = _texto_da_entrada(entrada)
    alvo = run_padrao(run)
    if not alvo:
        raise ValueError("sem Run ligado: passe --run e rode run-use --id <r>")
    fenced = f"o coordenador precisa comandar o Run do worker: {dica_ligar(alvo)}"
    if not run_do_coordenador(alvo):
        raise ValueError(f"a task está em {alvo}; {fenced}")
    t = next((t for t in orca("task-list", "--run", alvo, timeout=20)["tasks"] if t["id"] == task), None)
    if not t:
        raise ValueError(f"task {task} não existe no Run {alvo}")
    if t.get("status") != "dispatched" or not t.get("dispatch_id"):
        raise ValueError(f"task {task} está {t.get('status')}, não dispatched: sem worker para receber o ajuste")
    try:
        res = orca("send", "--run", alvo, "--to", f"dispatch:{t['dispatch_id']}", "--subject", "Ajuste",
                   "--body", texto if pedido is None else f"{texto}\n\n{PEDIDO_TITULO} (acréscimo)\n{pedido}",
                   "--priority", "high", timeout=10)
    except RuntimeError as e:
        raise ValueError(fenced if "consumer_fenced" in str(e) else str(e))
    msg = res.get("message") or res
    # O Orca digita o próprio aviso (a linha ganha `delivered_at`) no worker que ele alcança, e no que encerrou o turno parado no prompt às vezes não
    # (visto em 29/09): só esse recebe o aviso daqui, e um aviso que o Orca acabou de digitar não se repete.
    time.sleep(STEER_ESPERA_S)
    handle = _terminal_do_dispatch(alvo, t["dispatch_id"])
    entrega = "orca" if _orca_avisou(msg.get("id")) else digita(handle, _aviso_worker(handle)) if handle else "sem_terminal"
    ev = append_event({"tipo": "steer", "task": task, "dispatch": t["dispatch_id"], "run": alvo, "texto": texto, "msg_id": msg.get("id"),
                       **({"pedido": pedido} if pedido is not None else {}), **({"aviso_terminal": entrega} if entrega != "ocupado" else {})})
    if entrada:
        intake(entrada, "steer", task, run=alvo)
    return ev


# ---------- tickets em arquivo ----------

_NUM_ARQ = re.compile(r"^(\d{2,})-.+\.md$")
_ACEITE = re.compile(r"^##\s+(?:Acceptance criteria|Critérios de aceite)\s*$", re.I | re.M)
_QUE_CONSTRUIR = re.compile(r"^##\s+What to build\s*$", re.I | re.M)


def _cabecalho(txt):
    """O texto antes da primeira seção `## `: título, Status, Blocked by, Run e Task."""
    return re.split(r"^## ", txt, maxsplit=1, flags=re.M)[0]


def _campo(cab, nome):
    m = re.search(rf"^{re.escape(nome)}:[ \t]*(.*?)[ \t]*$", cab, re.M)
    return m.group(1) if m else None


def _trocar_campo(txt, nome, valor):
    """Troca a linha `Nome: valor` do cabeçalho; sem ela, a linha entra depois das outras."""
    cab = _cabecalho(txt)
    if _campo(cab, nome) is None:
        novo = cab.rstrip("\n") + f"\n{nome}: {valor}\n\n"
    else:
        novo = re.sub(rf"^{re.escape(nome)}:.*$", lambda _: f"{nome}: {valor}", cab, count=1, flags=re.M)
    return novo + txt[len(cab):]


def _escrever(caminho, txt):
    """Troca o arquivo de uma vez (arquivo temporário na mesma pasta e rename): quem lê nunca vê um ticket pela metade."""
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(caminho), prefix=".tk-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(txt)
        os.replace(tmp, caminho)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp)
        raise


def le_ticket(caminho):
    """{num, arquivo, titulo, status, blocked_by, run, task} do cabeçalho de um ticket `NN-slug.md`; levanta OSError ou ValueError se ilegível."""
    with open(caminho, encoding="utf-8") as f:
        txt = f.read()
    cab = _cabecalho(txt)
    titulo = re.match(r"#[ \t]*(?:\d+[ \t]*:[ \t]*)?(.+)", cab)
    if not titulo:
        raise ValueError(f"{caminho} não começa com o título (# NN: Título)")
    return {"num": os.path.basename(caminho).split("-")[0].zfill(2), "arquivo": caminho, "titulo": titulo.group(1).strip(),
            "status": _campo(cab, "Status") or "?", "blocked_by": [n.zfill(2) for n in re.findall(r"\d+", _campo(cab, "Blocked by") or "")],
            "run": _campo(cab, "Run") or None, "task": _campo(cab, "Task") or None}


def tickets():
    """Todos os tickets de ISSUES, por número. Ticket ilegível vai para o log e fica de fora: o resumo da sessão não cai por causa dele."""
    try:
        nomes = sorted((n for n in os.listdir(ISSUES) if _NUM_ARQ.match(n)), key=lambda n: int(n.split("-")[0]))
    except OSError:
        return []
    achados = []
    for n in nomes:
        try:
            achados.append(le_ticket(os.path.join(ISSUES, n)))
        except (OSError, ValueError) as e:
            log(f"ticket {n}: {type(e).__name__}: {e}")
    return achados


def _slug(titulo):
    s = unicodedata.normalize("NFKD", titulo).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")[:48].strip("-") or "ticket"


def linha_ticket(t):
    bloq = f"; Blocked by: {', '.join(t['blocked_by'])}" if t["blocked_by"] else ""
    return f"{t['num']} {_cita(t['titulo'], 60)} ({t['status']}{bloq})"


def ticket_novo(titulo, spec_arquivo, blocked_by=None, run=None):
    """Cria `ISSUES/NN-<slug>.md` a partir do título e do arquivo de spec, e a task do Orca (`--task-title` igual ao título, `--spec` curto que
    aponta para o arquivo, `--deps` com as tasks dos Blocked by ainda abertos). A `task_id` fica no ticket, que é a única fonte do conteúdo.

    O spec traz "## What to build" (senão o texto todo vira essa seção) e "## Acceptance criteria" (obrigatório). Sem a task o ticket não fica:
    se o `task-create` falha, o arquivo é desfeito.
    """
    titulo = " ".join((titulo or "").split())
    if not titulo:
        raise ValueError("ticket sem título")
    try:
        with open(os.path.expanduser(spec_arquivo), encoding="utf-8") as f:
            corpo = f.read().strip()
    except (OSError, ValueError) as e:
        raise ValueError(f"não consegui ler {spec_arquivo}: {getattr(e, 'strerror', None) or e}")
    if not _ACEITE.search(corpo):
        raise ValueError("o spec precisa da seção '## Acceptance criteria' (o formato do /to-tickets)")
    if corpo.startswith("# "):
        corpo = corpo.split("\n", 1)[1].strip() if "\n" in corpo else ""  # o título mora no cabeçalho do ticket
    if not _QUE_CONSTRUIR.search(corpo):
        corpo = f"## What to build\n\n{corpo}"
    existentes = {t["num"]: t for t in tickets()}
    bloqueios = list(dict.fromkeys(n.zfill(2) for n in re.findall(r"\d+", blocked_by or "")))
    alvo = run_padrao(run)
    deps, avisos = [], []
    for n in bloqueios:
        t = existentes.get(n)
        if not t:
            raise ValueError(f"ticket {n} não existe em {ISSUES}: Blocked by só aceita ticket que já foi criado")
        if t["status"] == STATUS_FECHADO or not t["task"]:  # a task do blocker resolvido já está completed
            continue
        if alvo and t["run"] and t["run"] != alvo:  # o Orca recusa --deps de task de outro Run (B23): o bloqueio fica só na linha Blocked by
            avisos.append(f"ticket {n} é do Run {t['run']}, não de {alvo}: o Blocked by ficou só na linha do ticket, sem --deps na task")
        else:
            deps.append(t["task"])
    if not alvo:
        raise ValueError("sem Run ligado: passe --run e rode run-use --id <r>")
    if not run_do_coordenador(alvo):
        raise ValueError(f"o ticket é para o Run {alvo}, que o coordenador não comanda: o Orca recusa task-create em outro Run (consumer_fenced); {dica_ligar(alvo)}")
    os.makedirs(ISSUES, exist_ok=True)
    with _trava("ticket.lock"):  # B25: dois `ticket novo` ao mesmo tempo não escolhem o mesmo número
        try:
            maior = max(int(n.split("-")[0]) for n in os.listdir(ISSUES) if _NUM_ARQ.match(n))
        except ValueError:
            maior = 0
        num = f"{maior + 1:02d}"
        caminho = os.path.join(ISSUES, f"{num}-{_slug(titulo)}.md")
        txt = f"# {num}: {titulo}\n\nStatus: {STATUS_NOVO}\nBlocked by: {', '.join(bloqueios) or '(nenhum)'}\nRun: {alvo}\n\n{corpo}\n"
        with open(caminho, "x", encoding="utf-8") as f:
            f.write(txt)
    try:
        res = orca("task-create", "--spec", f"Leia e execute o ticket {caminho}", "--task-title", titulo, "--run", alvo,
                   *(["--deps", json.dumps(deps)] if deps else []), timeout=20)
        task = (res.get("task") or res).get("id")
        if not task:
            raise RuntimeError(f"task-create sem id na resposta: {json.dumps(res)[:300]}")
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(caminho)
        raise
    _escrever(caminho, _trocar_campo(txt, "Task", task))
    append_event({"tipo": "ticket", "op": "novo", "ticket": num, "task": task, "run": alvo, "titulo": titulo, **({"deps": deps} if deps else {})})
    return {"ticket": num, "arquivo": caminho, "task": task, "run": alvo, **({"aviso": "; ".join(avisos)} if avisos else {})}


def ticket_fechar(numero, answer):
    """Grava `## Answer` (o texto, ou o conteúdo do arquivo se `answer` é um caminho), põe `Status: resolved` e completa a task no Orca se ela
    ainda estiver aberta. O arquivo é a verdade: falha do Orca vira aviso e o ticket fica resolvido. Devolve {ticket, status, task, task_fechada, aviso}."""
    n = str(numero).strip().zfill(2)
    t = next((t for t in tickets() if t["num"] == n), None)
    if not t:
        raise ValueError(f"ticket {n} não existe em {ISSUES}")
    if t["status"] == STATUS_FECHADO:
        raise ValueError(f"ticket {n} já está {STATUS_FECHADO}")
    caminho = os.path.expanduser(answer or "")
    resposta = (open(caminho, encoding="utf-8").read() if answer and os.path.isfile(caminho) else answer or "").strip()
    if not resposta:
        raise ValueError("--answer vazio: diga o que resolveu o ticket (texto ou arquivo)")
    with open(t["arquivo"], encoding="utf-8") as f:
        txt = f.read()
    _escrever(t["arquivo"], _trocar_campo(txt, "Status", STATUS_FECHADO).rstrip("\n") + f"\n\n## Answer\n\n{resposta}\n")
    fechada, aviso = False, ""
    if t["task"] and t["run"]:
        try:
            tk = next((x for x in orca("task-list", "--run", t["run"], timeout=20)["tasks"] if x["id"] == t["task"]), None)
            if tk and tk.get("status") not in ("completed", "failed"):
                orca("task-update", "--id", t["task"], "--status", "completed", "--run", t["run"], "--result", json.dumps({"ticket": n}), timeout=20)
                fechada = True
                if tk.get("status") == "dispatched":
                    aviso = f"a task {t['task']} estava dispatched: confira se o worker ainda roda (orq agentes)"
        except Exception as e:  # noqa: BLE001 - o ticket já está resolvido: a task fecha à mão
            aviso = f"task {t['task']} não fechada ({e}): {dica_ligar(t['run'])} e orca orchestration task-update --id {t['task']} --status completed"
    append_event({"tipo": "ticket", "op": "fechar", "ticket": n, "task": t["task"], "task_fechada": fechada, **({"aviso": aviso} if aviso else {})})
    return {"ticket": n, "status": STATUS_FECHADO, "arquivo": t["arquivo"], "task": t["task"], "task_fechada": fechada, "aviso": aviso}


def contexto_sessao():
    """O que uma sessão nova do coordenador lê ao começar: o `orq status` (até 5 linhas), os tickets abertos e o caminho do mapa, em até
    LINHAS_SESSAO linhas. Tickets que não cabem viram `+N abertos`."""
    linhas = estado().splitlines()[:LINHAS_SESSAO - 2]
    abertos = [t for t in tickets() if t["status"] != STATUS_FECHADO]
    sobra = LINHAS_SESSAO - len(linhas) - 1  # a linha do mapa fica reservada
    if not abertos:
        linhas.append("Tickets abertos: nenhum.")
    else:
        cabem = len(abertos) if len(abertos) <= sobra - 1 else sobra - 2  # o cabeçalho e, se sobra ticket, a linha do +N
        linhas += [f"Tickets abertos ({len(abertos)}):", *(f"- {linha_ticket(t)}" for t in abertos[:cabem])]
        if cabem < len(abertos):
            linhas.append(f"+{len(abertos) - cabem} abertos (orq ticket lista)")
    linhas.append(f"Mapa: {MAPA}; tickets em {ISSUES}")
    return "\n".join(linhas)


PREFIXO_PROVA = "Prova r5"  # as tasks de prova do review-5 não entram na lista padrão


def _workers_todos(run=None):
    """worker-list, mais novo primeiro, PAGINAS_ATIVOS páginas: de todos os Runs (sem o terminal ligado, `scope.source` all) ou só de `run`.

    Se o Orca devolver a lista escopada ao Run ligado (num terminal de worker ele liga o Run do dispatch mesmo sem ORCA_TERMINAL_HANDLE),
    levanta em vez de olhar só um Run: quem chama passa --run. ponytail: 300 workers bastam; o resto é histórico.
    """
    ws, cursor = [], None
    for _ in range(PAGINAS_ATIVOS):
        res = orca("worker-list", "--limit", "100", *(["--run", run] if run else []), *(["--cursor", cursor] if cursor else []), sem_terminal=not run)
        fonte = (res.get("scope") or {}).get("source")
        if fonte != ("flag" if run else "all"):
            raise RuntimeError(f"worker-list veio escopado ({fonte}): a lista não cobre todos os Runs; passe --run <r>")
        ws += res["workers"]
        cursor = (res.get("page") or {}).get("nextCursor")
        if not cursor:
            break
    return ws


def _retencao(w, humanos=frozenset()):
    """Por que o Orca reteve o terminal do worker (external_terminal…); `sem_recurso` no dispatch de contexto, que não tem terminal próprio.

    `user_takeover` só conta como retenção nos dispatches em `humanos` (o `orq liberar` achou prompt de usuário no transcrito do worker):
    o Orca marca o takeover em qualquer entrada do xterm, sem humano, então sozinho ele não prova nada.
    """
    res = w.get("resource")
    motivo = (res.get("retainedReason") if isinstance(res, dict) else "sem_recurso") or None
    return None if motivo == "user_takeover" and w.get("dispatchId") not in humanos else motivo


def _interacao_registrada(events):
    """Os dispatches em que o `orq liberar` achou prompt de usuário no transcrito do worker."""
    return {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("interacao")}


def _liberados(events):
    """Os dispatches que o `orq liberar` liberou e cujo terminal fechou (`fechado`; o retido continua aberto e segue a regra de _retencao)."""
    return {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("fechado")}


def _terminais_vivos():
    """Os handles do `orca terminal list`; None se o Orca falhar (a lista sai sem esse corte)."""
    try:
        r = orca("list", "--limit", "1000", area="terminal", timeout=10)
        if r.get("truncated"):
            log("agentes: terminal list truncado; sem prova de quem morreu")
            return None  # lista cortada não prova que um terminal sumiu (B37)
        return {t.get("handle") for t in r["terminals"]}
    except Exception as e:  # noqa: BLE001
        log(f"agentes: terminal list: {type(e).__name__}: {e}")
        return None


def _ativo(w):
    """Worker que ainda pede atenção: rodando, ou terminal não liberado."""
    return w.get("dispatchStatus") == "dispatched" or w.get("terminalState") != "released"


def _fundo(d, *chaves):
    for c in chaves:
        d = d.get(c) if isinstance(d, dict) else None
    return d


def _detalhes(ws):
    """{dispatch: {titulo, modelo, desde, agente}}: task_title do task-list de cada Run e modelo/dispatchedAt/agente do worker-show (só de quem não foi liberado).

    Falha isolada de um Run ou de um dispatch deixa os campos vazios e vai para o log: a lista sai mesmo assim.
    """
    def titulos(r):
        try:
            return {t["id"]: t.get("task_title") for t in orca("task-list", "--run", r, timeout=20)["tasks"]}
        except Exception as e:  # noqa: BLE001
            log(f"agentes: task-list {r}: {type(e).__name__}: {e}")
            return {}

    def mostra(w):
        try:
            res = orca("worker-show", "--dispatch", w["dispatchId"], timeout=10)
            return w["dispatchId"], {"modelo": _fundo(res, "worker", "startOptions", "launch", "requested", "model"), "desde": _fundo(res, "dispatch", "dispatchedAt"),
                                     "agente": _fundo(res, "worker", "startOptions", "agent")}
        except Exception as e:  # noqa: BLE001
            log(f"agentes: worker-show {w.get('dispatchId')}: {type(e).__name__}: {e}")
            return w["dispatchId"], {}

    runs = sorted({w["runId"] for w in ws if w.get("runId")})
    with ThreadPoolExecutor(8) as ex:
        tits = dict(zip(runs, ex.map(titulos, runs)))
        vivos = dict(ex.map(mostra, [w for w in ws if _ativo(w)]))
    return {w["dispatchId"]: {"titulo": tits.get(w.get("runId"), {}).get(w.get("taskId")), **vivos.get(w["dispatchId"], {})} for w in ws}


def agentes(run=None, todos=False, agora=None):
    """O estado de cada dispatch: worker-list de todos os Runs (ou de `run`) mais o inbox. Liberados, sem terminal, retidos pelo Orca e as tasks "Prova r5" só com `todos`."""
    ws, events, vivos = _workers_todos(run), read_events(), _terminais_vivos()
    if not todos:
        liberados = _liberados(events)
        ws = [w for w in ws if not _sem_terminal(w, liberados, vivos)]  # os retidos por motivo do Orca (external_terminal…) e o user_takeover com prompt humano não têm ação possível: só aparecem com --todos
        humanos = _interacao_registrada(events)
        ws = [w for w in ws if _ativo(w) and (w.get("dispatchStatus") == "dispatched" or not _retencao(w, humanos))]
    msgs = orca("inbox", "--limit", "200", timeout=20)["messages"]
    agora = agora or datetime.now(timezone.utc)
    ags = monta_agentes(ws, msgs, events, agora, _detalhes(ws), vivos, _turnos_ro())
    nao_lidos = {e.get("dispatch") for e in alertas_recentes(events, agora, ags) if e.get("alerta") == "steer_nao_lido"}
    for a in ags:
        if a["dispatch"] in nao_lidos:
            a["alerta"] = "steer não lido"
    return ags if todos else [a for a in ags if not (a.get("titulo") or "").startswith(PREFIXO_PROVA)]


def _txt_controle(c, dispatch):
    """`relancar ok 14:02 (novo ctx_…)`: uma entrada do histórico de controle; quem lê o dispatch novo vê de qual veio."""
    ref = f" (de {c['dispatch']})" if c.get("novo_dispatch") == dispatch else f" (novo {c['novo_dispatch']})" if c.get("novo_dispatch") else ""
    return f"{c['acao']} {c['resultado']} {_hora_local(c.get('ts'))}{ref}" + (f" [{_cita(c['motivo'], 40)}]" if c.get("motivo") else "")


def texto_agentes(ags):
    """Uma linha por dispatch, com o que fazer logo abaixo do travado (orq steer) e do entregue sem liberar (orq liberar)."""
    if not ags:
        return "nenhum dispatch"
    linhas = []
    for a in ags:
        ref = a.get("ultimo_heartbeat") or a.get("desde")
        hb = (f"{a.get('fase') or '-'} {_hora_local(a['ultimo_heartbeat'])} (há {a['idade_s'] // 60} min)" if a.get("ultimo_heartbeat") and a.get("idade_s") is not None
              else f"sem heartbeat (desde {_hora_local(ref)})" if ref and a["estado"] in ("rodando", "travado", "perguntando") else "")
        if a["estado"] == "nao_comecou":
            hb = f"não começou: nenhum turno {a['idade_s'] // 60} min depois do despacho ({_hora_local(a.get('desde'))})"
        elif a["estado"] == "parado":
            hb = f"parado no prompt há {a['idade_s'] // 60} min"
        linhas.append(f"{a['estado']:<11} {a['task']}  {_cita(a.get('titulo') or '?', 36)}  {a.get('modelo') or '?'}  {a['terminal']}  {hb}".rstrip())
        for av in a.get("entrega") or []:
            linhas.append(f"            AVISO: {av}")
        if a.get("controle"):
            linhas.append("            controle: " + "; ".join(_txt_controle(c, a["dispatch"]) for c in a["controle"]))
        if a.get("alerta"):
            linhas.append(f"            ALERTA: {a['alerta']} (o worker não leu o ajuste depois de {STEER_TENTATIVAS} avisos; um check sem --ack esconde as mensagens novas)")
        if a["estado"] in ("travado", "nao_comecou", "parado"):
            linhas.append(f'            -> orq steer {a["task"]} "<ajuste>" --run {a["run"]}')
        elif a["estado"] == "entregue":
            linhas.append(f"            -> orq liberar {a['dispatch']}" if not a.get("retido") else f"            retido: {a['retido']} (o orq liberar não fecha)")
    return "\n".join(linhas)


def _ack_do_dispatch(run_id, dispatch):
    """Confirma as entregas pendentes do Run que são só deste dispatch (worker_done, heartbeat…), até 10 lotes; devolve (entregas, aviso).

    Lote com mensagem de outro dispatch não é confirmado: o Orca o entrega junto e confirmá-lo perderia a do outro worker. Sem ligação ao
    Run (consumer_fenced) o ack é pulado e a liberação segue.
    """
    entregas = []
    try:
        with trava_gerente():  # a ligação do gerente ao Run não muda entre o check e o ack
            res = orca("check", "--run", run_id)
            for _ in range(10):
                msgs = res.get("messages") or []
                if not res.get("deliveryId") or not msgs:
                    break
                if any(_dispatch_da_msg(m) != dispatch for m in msgs):
                    return entregas, ("ack pulado: o lote tem mensagem de outro dispatch, que o coordenador ainda não leu" if not entregas else "")
                entregas.append(res["deliveryId"])
                res = orca("check", "--run", run_id, "--ack", res["deliveryId"])
    except RuntimeError as e:
        return entregas, f"ack pulado ({e}): {dica_ligar(run_id)} e confirme depois"
    return entregas, ""


def _texto_do_prompt(conteudo):
    """O texto de um prompt do transcrito: a string, ou os blocos `text` (tool_result e imagem não são texto digitado)."""
    if isinstance(conteudo, str):
        return conteudo
    if isinstance(conteudo, list):
        return "\n".join(b.get("text") or "" for b in conteudo if isinstance(b, dict) and b.get("type") == "text")
    return ""


def _prompts_humanos_do_worker(dispatch):
    """Quantos prompts de usuário a sessão do worker recebeu além do despacho, do Orca e das notificações; None se o transcrito não aparece.

    O transcrito do worker é o arquivo de `projects/*` (mexido nos últimos TRANSCRITO_DIAS) cujo primeiro prompt é o preâmbulo de despacho e
    cita o dispatch. O Orca não sabe dizer se houve humano: ele marca `user_takeover` em qualquer entrada do xterm.
    ponytail: lê o arquivo do worker inteiro uma vez por `orq liberar`; transcrito de dezenas de MB custa alguns segundos.
    """
    alvo, limite = dispatch.encode(), time.time() - TRANSCRITO_DIAS * 86400
    for arq in glob.glob(os.path.join(PROJETOS, "*", "*.jsonl")):
        try:
            if os.path.getmtime(arq) < limite:
                continue
            with open(arq, "rb") as f:
                cabeca = f.read(LEITURA_INICIAL)
            if alvo not in cabeca:
                continue
            primeiro = None
            for linha in cabeca.splitlines():
                try:
                    e = json.loads(linha)
                except ValueError:
                    continue
                if e.get("type") == "user" and not e.get("isMeta") and _texto_do_prompt(_dict(e.get("message")).get("content")).strip():
                    primeiro = _texto_do_prompt(e["message"]["content"])
                    break
            if primeiro is None or origem(primeiro) != "despacho" or dispatch not in primeiro:
                continue
            humanos = 0
            with open(arq, "rb") as f:
                for linha in f:
                    if b'"type":"user"' not in linha and b'"type": "user"' not in linha:
                        continue
                    try:
                        e = json.loads(linha)
                    except ValueError:
                        continue
                    texto = _texto_do_prompt(_dict(e.get("message")).get("content")).strip()
                    if e.get("type") != "user" or e.get("isMeta") or not texto or texto.startswith("<system-reminder>"):
                        continue
                    if origem(texto) not in ("despacho", "orca", "notificacao", "resumo"):
                        humanos += 1
            return humanos
        except OSError as e:
            log(f"transcrito {arq}: {type(e).__name__}: {e}")
    return None


def _pode_fechar(handle, run_id, dispatch):
    """(sim, motivo, humano): o terminal retido é do worker deste dispatch e o orq pode fechá-lo? Relê o worker-list depois do release.

    A linha é a do próprio dispatch (M10), e o terminal não pode estar em uso por outro dispatch ainda rodando (terminal reaproveitado). Fecha o
    terminal `owned` que o Orca reteve sem dizer por quê, e o `user_takeover` de terminal criado por este dispatch cujo transcrito de sessão não
    tem nenhum prompt de usuário (M9); `humano` é True quando o transcrito tem. Qualquer outro motivo (user_requested, external_terminal,
    identity_unproven, reused_terminal, configured_tab…) é o Orca dizendo que o terminal não é só do worker, e o dispatch de contexto
    (`orchestration dispatch`) usa terminal de outra sessão. Nunca fecha o terminal do próprio coordenador nem o coordenador do Run.
    """
    if not handle:
        return False, "sem handle de terminal", False
    if handle == os.environ.get("ORCA_TERMINAL_HANDLE"):
        return False, "é o terminal deste coordenador", False
    if (orca("run-show", "--id", run_id)["run"] or {}).get("coordinator_handle") == handle:
        return False, "é o coordenador do Run", False
    ws = _workers_todos(run_id)
    linha = next((w for w in ws if w.get("dispatchId") == dispatch), None)
    if not linha or linha.get("agentTerminalHandle") != handle:
        return False, "o dispatch não aparece mais no worker-list com esse terminal", False
    outro = next((w for w in ws if w.get("agentTerminalHandle") == handle and w.get("dispatchId") != dispatch and w.get("dispatchStatus") == "dispatched"), None)
    if outro:
        return False, f"o terminal foi reaproveitado pelo dispatch {outro.get('dispatchId')}, que ainda roda", False
    res = linha.get("resource") if isinstance(linha.get("resource"), dict) else {}
    motivo = res.get("retainedReason")
    if res.get("ownershipState") == "owned" and not motivo:
        return True, "", False
    if res.get("ownershipState") == "user_owned" and motivo in (None, "user_takeover"):
        if res.get("originDispatchId") != dispatch or res.get("ownerDispatchId") != dispatch:
            return False, "o Orca o reteve como user_takeover e o terminal não nasceu deste dispatch", False
        humanos = _prompts_humanos_do_worker(dispatch)
        if humanos is None:
            return False, "o Orca o reteve como user_takeover e o transcrito do worker não foi achado: sem prova de que ninguém digitou nele", False
        if humanos:
            return False, f"o Orca o reteve como user_takeover e o transcrito do worker tem {humanos} prompt(s) de usuário", True
        return True, "", False
    return False, f"o Orca o reteve como {motivo or res.get('ownershipState') or 'desconhecido'}, não é só do worker", False


def liberar(dispatch, run=None):
    """ack pendente do dispatch, worker-release e, se o estado vier `retained`, `orca terminal close` do terminal do worker. Grava um evento.

    Só age em dispatch que aparece no worker-list (de todos os Runs, ou de `run`); recusa o que ainda está rodando.
    """
    w = next((w for w in _workers_todos(run) if w.get("dispatchId") == dispatch), None)
    if not w:
        raise ValueError(f"dispatch {dispatch} não aparece no worker-list: o orq só libera worker de Run do Orca")
    if w.get("dispatchStatus") == "dispatched":
        raise ValueError(f"dispatch {dispatch} ainda está rodando: espere o worker_done ou use worker-stop")
    run_id, handle, avisos = w.get("runId"), w.get("agentTerminalHandle"), []
    if dispatch in _liberados(read_events()):
        return {"tipo": "liberar", "dispatch": dispatch, "task": w.get("taskId"), "run": run_id, "terminal": handle, "estado": w.get("terminalState"),
                "fechado": True, "ack": 0, "aviso": "já liberado"}  # repetir não manda worker-release nem terminal close (M13)
    entregas, aviso = _ack_do_dispatch(run_id, dispatch)
    if aviso:
        avisos.append(aviso)
    try:
        estado = orca("worker-release", "--dispatch", dispatch, timeout=30, run=run_id).get("state")
    except RuntimeError as e:
        if "consumer_fenced" in str(e):
            raise ValueError(f"o coordenador precisa comandar o Run {run_id} para liberar o dispatch: {dica_ligar(run_id)}")
        raise
    fechado, humano = False, False
    if estado == "retained":
        try:
            pode, motivo, humano = _pode_fechar(handle, run_id, dispatch)
            if pode:
                orca("close", "--terminal", handle, area="terminal")
                fechado = True
            else:
                avisos.append(f"terminal {handle} mantido: {motivo}")
        except RuntimeError as e:
            avisos.append(f"terminal close de {handle} falhou: {e}")
    elif estado == "release_pending":
        avisos.append("release_pending: o Orca ainda está liberando; repita orq liberar depois")
    ev = {"tipo": "liberar", "dispatch": dispatch, "task": w.get("taskId"), "run": run_id, "terminal": handle, "estado": estado, "fechado": fechado, "ack": entregas,
          **({"interacao": True} if humano else {})}
    append_event({**ev, **({"aviso": "; ".join(avisos)} if avisos else {})})
    refresh_bg()  # o resumo do próximo prompt já sai sem o dispatch liberado (M13)
    return {**ev, "aviso": "; ".join(avisos)}


# ---------- controle do worker: interromper, encerrar e relançar (ticket 32) ----------

def _controle(acao, w, resultado, **campos):
    """Grava o evento `controle` do dispatch `w` (do worker-list): o histórico que o `orq agentes` mostra. Campo vazio não entra."""
    return append_event({"tipo": "controle", "acao": acao, "dispatch": w.get("dispatchId"), "task": w.get("taskId"), "run": w.get("runId"), "resultado": resultado,
                         **{k: v for k, v in campos.items() if v not in (None, "")}})


def _worker_do_dispatch(dispatch, run=None):
    w = next((w for w in _workers_todos(run) if w.get("dispatchId") == dispatch), None)
    if not w:
        raise ValueError(f"dispatch {dispatch} não aparece no worker-list: o orq só controla worker de Run do Orca")
    return w


def _checkpoint(dispatch):
    """A worktree do dispatch (caminho, head, arquivos sujos) e o perfil com que o worker subiu (agente, modelo, effort), pelo worker-show.

    O caminho vem do terminal ou, sem ele, do `worktreeId` (`<repo>::<caminho>`). `head` e `sujo` são None se o caminho não é um repositório git legível."""
    res = orca("worker-show", "--dispatch", dispatch, timeout=10)
    wid = _fundo(res, "worker", "worktreeId")
    caminho = _fundo(res, "terminal", "worktreePath") or (wid.split("::", 1)[1] if isinstance(wid, str) and "::" in wid else None)
    pedido = _fundo(res, "worker", "startOptions", "launch", "requested") or {}
    head, sujo = (_git(caminho, "rev-parse", "HEAD"), _git(caminho, "status", "--porcelain")) if caminho and os.path.isdir(caminho) else (None, None)
    return {"worktree_id": wid, "caminho": caminho, "agente": _fundo(res, "worker", "startOptions", "agent"), "modelo": pedido.get("model"), "effort": pedido.get("effort"),
            "head": (head or "").strip()[:12] or None, "sujo": len(sujo.splitlines()) if sujo is not None else None}


def _intacta(cp):
    """A worktree do checkpoint ainda existe e o commit em que estava continua no histórico dela (o worker pode ter commitado antes de parar)."""
    c = cp.get("caminho")
    return bool(c) and os.path.isdir(c) and (not cp.get("head") or _git(c, "merge-base", "--is-ancestor", cp["head"], "HEAD") is not None)


def interromper(dispatch, run=None):
    """Manda o interrupt do Orca ao terminal do worker que está rodando (`terminal send --interrupt`) e grava o evento.

    O worker segue vivo e o Claude Code não confirma o cancelamento: quem chama confere com `orq agentes` ou `orca terminal read`."""
    w = _worker_do_dispatch(dispatch, run)
    if w.get("dispatchStatus") != "dispatched":
        raise ValueError(f"dispatch {dispatch} não está rodando ({w.get('dispatchStatus')}): não há turno a interromper")
    handle = w.get("agentTerminalHandle")
    if not handle:
        raise ValueError(f"dispatch {dispatch} não tem terminal de agente")
    try:
        orca("send", "--terminal", handle, "--interrupt", area="terminal")
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _controle("interromper", w, "falhou", terminal=handle, erro=str(e))
        raise
    return _controle("interromper", w, "ok", terminal=handle, aviso="interrupt enviado, sem confirmação de que o turno parou")


def _parar(acao, w, base):
    """worker-stop do dispatch que ainda roda (o Orca fecha o terminal do agente e nunca apaga a worktree); dispatch que já terminou não leva nada."""
    if w.get("dispatchStatus") != "dispatched":
        return
    try:
        orca("worker-stop", "--dispatch", w["dispatchId"], timeout=30, run=w.get("runId"))
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _controle(acao, w, "falhou", passo="worker-stop", erro=str(e), **base)
        raise


def encerrar(dispatch, motivo, run=None):
    """worker-stop (se ainda roda) e depois o `liberar`, com o motivo no log. Parar não volta atrás: se o release falha, o worker fica parado, o
    terminal retido e a worktree onde estava (evento `parcial`), e `orq liberar` termina o serviço."""
    if not (motivo or "").strip():
        raise ValueError("encerrar pede --motivo: ele fica no log e no orq agentes")
    w = _worker_do_dispatch(dispatch, run)
    try:
        cp = _checkpoint(dispatch)
    except RuntimeError as e:
        log(f"encerrar: worker-show {dispatch}: {e}")
        cp = {}
    base = {"motivo": motivo.strip(), "terminal": w.get("agentTerminalHandle"), "head": cp.get("head"), "sujo": cp.get("sujo")}
    _controle("encerrar", w, "iniciado", **base)
    _parar("encerrar", w, base)
    try:
        lib = liberar(dispatch, w.get("runId"))
    except (RuntimeError, ValueError, subprocess.TimeoutExpired) as e:
        _controle("encerrar", w, "parcial", passo="release", erro=str(e), worktree_intacta=_intacta(cp), **base)
        raise RuntimeError(f"o worker parou, mas o release falhou ({e}); a worktree segue em {cp.get('caminho') or '?'}: rode orq liberar {dispatch}")
    return _controle("encerrar", w, "ok", worktree_intacta=_intacta(cp), aviso=lib.get("aviso"), **base)


def relancar(dispatch, nota, modelo=None, effort=None, run=None):
    """Para o worker e sobe outro na MESMA worktree e task (`worker-start --task --retry-of`), com o perfil do antigo ou o de --modelo/--effort.

    Tudo o que pode recusar vem antes do worker-stop (worktree, Run ligado, task, perfil). Depois dele, a volta atrás é a que existe: se o perfil
    pedido não sobe, sobe o de antes (`revertido`); se nada sobe, o terminal antigo fica retido, a worktree intacta e a mensagem traz o comando para
    repetir com a nota (`falhou`). O spec de uma task do Orca não muda, então a nota vai como o primeiro ajuste do worker novo (`orq steer`).
    O worker novo usa a política de setup do Orca para worktree existente (sem novo setup)."""
    nota = (nota or "").strip()
    if not nota:
        raise ValueError("relancar pede --nota: o que mudou para o worker novo")
    if bool(modelo) != bool(effort):
        raise ValueError("--modelo e --effort vão juntos: o effort de um modelo não vale para outro")
    w = _worker_do_dispatch(dispatch, run)
    run_id, task = w.get("runId"), w.get("taskId")
    cp = _checkpoint(dispatch)
    if not cp["caminho"] or not os.path.isdir(cp["caminho"]):
        raise ValueError(f"a worktree do dispatch {dispatch} não existe ({cp['caminho'] or 'o Orca não disse onde'}): nada foi parado")
    pedido, antigo = (modelo or cp["modelo"], effort or cp["effort"]), (cp["modelo"], cp["effort"])
    if not all(pedido):
        raise ValueError("o Orca não informou o modelo e o effort do worker antigo: passe --modelo e --effort")
    if not run_do_coordenador(run_id):
        raise ValueError(f"o worker é do Run {run_id}, que o coordenador não comanda: {dica_ligar(run_id)}")
    if not any(t["id"] == task for t in orca("task-list", "--run", run_id, timeout=20)["tasks"]):
        raise ValueError(f"task {task} não existe no Run {run_id}")
    base = {"nota": nota, "terminal": w.get("agentTerminalHandle"), "worktree": cp["caminho"], "head": cp["head"], "sujo": cp["sujo"]}
    _controle("relancar", w, "iniciado", modelo=pedido[0], effort=pedido[1], **base)
    _parar("relancar", w, {**base, "modelo": pedido[0], "effort": pedido[1]})
    seletor = f"id:{cp['worktree_id']}" if cp["worktree_id"] else f"path:{cp['caminho']}"
    res, erro, subiu = None, None, None
    for perfil in [pedido, *([antigo] if pedido != antigo and all(antigo) else [])]:
        try:
            res = orca("worker-start", "--run", run_id, "--task", task, "--retry-of", dispatch, "--worktree", seletor, "--agent", cp["agente"] or "claude",
                       "--model", perfil[0], "--effort", perfil[1], timeout=180)
            subiu = perfil
            break
        except subprocess.TimeoutExpired:
            erro = "worker-start passou de 180 s sem resposta: o worker pode ter subido, confira com orq agentes"
            break  # repetir empilharia um segundo worker na mesma worktree
        except RuntimeError as e:
            erro = erro or str(e)
    if not res or not res.get("dispatchId"):
        _controle("relancar", w, "falhou", passo="worker-start", erro=erro, modelo=pedido[0], effort=pedido[1], worktree_intacta=_intacta(cp), **base)
        raise RuntimeError(f"nenhum worker subiu ({erro}): o dispatch {dispatch} está parado, o terminal dele retido e a worktree intacta em {cp['caminho']}. "
                           f"Repita com: orq relancar {dispatch} --nota {shlex.quote(nota)}")
    novo, avisos = res["dispatchId"], []
    if subiu != pedido:
        avisos.append(f"o perfil pedido ({pedido[0]}/{pedido[1]}) não subiu ({erro}); o worker novo usa o de antes ({subiu[0]}/{subiu[1]})")
    for passo, fn, volta in ((f"nota não entregue", lambda: steer(task, f"Relançado depois de {dispatch}. O que mudou: {nota}", run_id), f"orq steer {task} <nota>"),
                             ("terminal do worker antigo não liberado", lambda: liberar(dispatch, run_id), f"orq liberar {dispatch}")):
        try:
            fn()
        except (RuntimeError, ValueError, subprocess.TimeoutExpired) as e:
            avisos.append(f"{passo} ({e}): rode {volta}")
    return _controle("relancar", w, "ok" if subiu == pedido else "revertido", novo_dispatch=novo, modelo=subiu[0], effort=subiu[1],
                     erro=erro if subiu != pedido else None, worktree_intacta=_intacta(cp), aviso="; ".join(avisos), **base)


def despachar(run, titulo, spec_arquivo, modelo, effort, worktree=None, name=None, base_branch=None, entrada=None, ticket=None):
    """worker-start (com --model e --effort, o que o hook worker-routing-guard exige) + evento `despacho` + intake da entrada.

    Devolve os ids e o comando do waiter; não espera nada. Recusa antes de criar a task o que o Orca recusaria depois.

    Com `ticket` (o número de um `orq ticket novo`) o worker sobe na task que o ticket já criou (`worker-start --task`), sem título nem spec: o
    ticket é o conteúdo. Com o modo noite ligado, recusa depois do horário, do teto de despachos ou das falhas seguidas (noite_checar).
    """
    noite_checar()
    if (name or base_branch) and worktree != "new-top-level":
        raise ValueError("--name e --base-branch só valem com --worktree new-top-level (o Orca recusa criar worktree em current)")
    tk = None
    if ticket:
        if titulo or spec_arquivo:
            raise ValueError("--ticket traz o título e o conteúdo: não combine com --titulo nem --spec-arquivo")
        n = str(ticket).strip().zfill(2)
        tk = next((t for t in tickets() if t["num"] == n), None)
        if not tk:
            raise ValueError(f"ticket {n} não existe em {ISSUES}")
        if tk["status"] == STATUS_FECHADO:
            raise ValueError(f"ticket {n} já está {STATUS_FECHADO}")
        if not tk["task"]:
            raise ValueError(f"ticket {n} não tem task: crie-o com orq ticket novo")
        if tk["run"] and tk["run"] != run:
            raise ValueError(f"o ticket {n} é do Run {tk['run']} e o despacho é para {run}: a task só despacha no Run onde nasceu")
        titulo, spec = tk["titulo"], None
    elif not (titulo and spec_arquivo):
        raise ValueError("passe --ticket <NN>, ou --titulo e --spec-arquivo")
    else:
        try:
            with open(os.path.expanduser(spec_arquivo)) as f:
                spec = f.read()
        except OSError as e:
            raise ValueError(f"não consegui ler {spec_arquivo}: {e.strerror}")
    pedido = _texto_da_entrada(entrada)
    _adotar(run)
    if not run_do_coordenador(run):
        raise ValueError(f"o despacho é para o Run {run}, que o coordenador não comanda: {dica_ligar(run)}")
    if spec is not None and not spec.lstrip().startswith("#"):
        spec = f"# {titulo}\n\n{spec}"  # o Claude Code tira o nome da aba do começo do prompt
    if spec is not None and pedido is not None:  # o pedido literal fica no topo, separado do que o coordenador escreveu; o review mede contra ele
        cabeca, _, resto = spec.partition("\n")
        spec = (f"{cabeca}\n\n{PEDIDO_TITULO}\n{pedido}\n\nO que o coordenador escreveu abaixo não o substitui: o pronto se confere contra este pedido.\n\n"
                f"{resto.lstrip(chr(10))}")
    ambiente = noite_ambiente() if noite_ativa(_cursor_ro()) else None  # na noite o worker sobe sem prompt de git (credencial, pinentry)
    args = ["worker-start", "--run", run, *(["--task", tk["task"]] if tk else ["--spec", spec, "--task-title", titulo]),
            "--agent", "claude", "--model", modelo, "--effort", effort]
    for flag, val in (("--worktree", worktree), ("--name", name), ("--base-branch", base_branch)):
        if val:
            args += [flag, val]
    try:
        res = orca(*args, timeout=180, env_extra=ambiente)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"worker-start passou de 180 s sem resposta: o worker pode ter subido, confira com orq agentes --run {run}")
    task, dispatch = res.get("taskId"), res.get("dispatchId")
    terminal = next((e.get("id") for e in res.get("effects") or [] if e.get("kind") == "terminal" and e.get("role") == "agent"), None)
    if not (task and dispatch):
        raise RuntimeError(f"worker-start sem taskId/dispatchId na resposta: {json.dumps(res)[:300]}")
    if terminal:
        try:
            orca("rename", "--terminal", terminal, "--title", titulo, area="terminal")
        except Exception as e:  # noqa: BLE001 - o título da aba é conforto: falhar não desfaz o despacho
            log(f"despachar: rename do terminal {terminal}: {type(e).__name__}: {e}")
    ev = {"tipo": "despacho", "run": run, "task": task, "dispatch": dispatch, "titulo": titulo, "modelo": modelo, "effort": effort, "terminal": terminal,
          **({"worktree": worktree} if worktree else {}), **({"nome": name} if name else {}), **({"entrada": entrada} if entrada else {}),
          **({"ticket": tk["num"]} if tk else {}), **({"ambiente": list(ambiente)} if ambiente else {})}
    append_event(ev)
    out = {"dispatchId": dispatch, "taskId": task, "run": run, "terminal": terminal, "espera": f"python3 ~/.claude/scripts/orca-wait-runs.py {run}"}
    if tk:  # B24: o ticket despachado deixa de ser "pronto para agente", senão uma sessão nova o despacharia de novo
        try:
            with open(tk["arquivo"], encoding="utf-8") as f:
                _escrever(tk["arquivo"], _trocar_campo(f.read(), "Status", STATUS_ANDAMENTO))
        except OSError as e:
            out["aviso"] = f"o worker subiu mas o ticket {tk['num']} não foi marcado {STATUS_ANDAMENTO} ({e.strerror}): edite o Status à mão"
    if entrada:
        out["entrada"] = entrada
        try:
            intake(entrada, "tarefa", task, run=run)
        except Exception as e:  # noqa: BLE001 - o worker já subiu: o intake vira aviso com o comando para repetir
            out["aviso"] = f"intake não gravado ({e}): rode orq intake {entrada} tarefa {task} --run {run}"
    return out


def _sessao_do_coordenador(sessao):
    """Caminho do transcrito: o id dado (ou prefixo dele) ou, sem id, a última sessão de coordenador registrada em cursor.json."""
    if not sessao:
        runs = _dict(_cursor_ro().get("runs"))
        if not runs:
            raise ValueError("nenhuma sessão de coordenador registrada em cursor.json: passe --sessao <id>")
        sessao = list(runs)[-1]
    achados = sorted(f for f in os.listdir(TRANSCRITOS) if f.endswith(".jsonl") and f.startswith(sessao)) if os.path.isdir(TRANSCRITOS) else []
    if len(achados) != 1:
        raise ValueError(f"transcrito de {sessao!r} em {TRANSCRITOS}: {'nenhum' if not achados else 'mais de um'}")
    return os.path.join(TRANSCRITOS, achados[0])


def respostas_so_recomendada(caminho):
    """(total de perguntas respondidas, [(quando, pergunta, resposta)] das que escolheram só a recomendada) do transcrito.

    A resposta vem no `toolUseResult` da linha do tool_result (answers indexadas pelo texto da pergunta, questions com as opções);
    o carimbo da linha é a hora em que o widget foi respondido. Pergunta dispensada não tem `answers`.
    """
    total, achadas = 0, []
    with open(caminho) as f:
        for linha in f:
            if '"answers"' not in linha:  # o transcrito tem dezenas de MB: só decodifica o que pode ter resposta
                continue
            try:
                d = json.loads(linha)
            except ValueError:
                continue
            tr = d.get("toolUseResult")
            if not isinstance(tr, dict) or not isinstance(tr.get("answers"), dict) or not d.get("timestamp"):
                continue
            for q in tr.get("questions") or []:
                resposta = tr["answers"].get(q.get("question"))
                if resposta is None:
                    continue
                total += 1
                if _recomendada(resposta, q):
                    achadas.append((_dt(d["timestamp"]), q, resposta))
    return total, achadas


def auditar_respostas(sessao=None):
    """Markdown com as respostas que escolheram só a opção recomendada a até JANELA_AUDITORIA_S de uma entrega do Orca ao coordenador.

    Só leitura: lê o transcrito da sessão e o inbox de todos os Runs, e não grava nada. Quem confere é o usuário, com a lista na mão.
    """
    caminho = _sessao_do_coordenador(sessao)
    total, achadas = respostas_so_recomendada(caminho)
    msgs = [m for m in orca("inbox", "--limit", "5000", timeout=60)["messages"] if _ao_coordenador(m)]
    entregas = [(_dt(m.get("delivered_at") or m["created_at"]), m) for m in msgs]
    suspeitas_ = []
    for quando, q, resposta in achadas:
        perto = [m for t, m in entregas if abs((quando - t).total_seconds()) <= JANELA_AUDITORIA_S]
        if perto:
            suspeitas_.append((quando, q, resposta, perto))
    suspeitas_.sort(key=lambda x: x[0])

    def cel(t):
        return str(t).replace("|", "\\|").replace("\n", " ")

    def hora(t):
        return t.astimezone().strftime("%d/%m/%Y %H:%M:%S")

    desde = min((t for t, _ in entregas), default=None)
    linhas = [
        f"# Respostas suspeitas da sessão {os.path.basename(caminho)[:-6]}", "",
        f"Gerado por `orq auditar-respostas`, só leitura. {total} perguntas respondidas no transcrito, {len(achadas)} escolheram só a opção "
        f"recomendada, {len(suspeitas_)} delas a até {JANELA_AUDITORIA_S} s de uma mensagem que o Orca entregou ao terminal do coordenador.", "",
        "Critério: resposta = só a opção recomendada (a primeira, ou a que traz \"(Recomendado)\" no label) e uma mensagem com destino "
        "`run:<id>` (qualquer Run) com `delivered_at` a até 2 s da hora do transcrito. O aviso \"You have N orchestration message\" "
        "é digitado no terminal nessa hora e pode ter escolhido a opção no lugar do usuário. Heartbeat de worker também dispara o aviso, "
        "então há coincidência sem que o aviso tenha respondido: a lista serve para o usuário conferir a decisão, não prova nada sozinha. "
        "Horas no fuso local; \"+N\" na última coluna são outras mensagens na mesma janela.", "",
        f"O inbox do Orca cobre {len(entregas)} mensagens para o coordenador" + (f", a mais antiga de {hora(desde)}." if desde else "."), "",
        "| Data e hora (local) | Header | Pergunta | Resposta | Mensagem do Orca |", "|---|---|---|---|---|",
    ]
    for quando, q, resposta, perto in suspeitas_:
        m = perto[0]
        msg = f"`{m['id']}` {m.get('type')} \"{_cita(m.get('subject'), 40)}\" ({m.get('run_id')}, {hora(_dt(m.get('delivered_at') or m['created_at']))[-8:]})"
        linhas.append(f"| {hora(quando)} | {cel(q.get('header'))} | {cel(_cita(q.get('question'), 80))} | {cel(_cita(resposta, 70))} | "
                      f"{cel(msg)}{f' +{len(perto) - 1}' if len(perto) > 1 else ''} |")
    if not suspeitas_:
        linhas.append("| nenhuma | | | | |")
    return "\n".join(linhas) + "\n"


# ---------- agent manager em terminal próprio ----------

def _adotar(run):
    """Run novo do coordenador (o `run-create` cru o liga a ele, e o Orca lhe manda o heartbeat) entra no agent manager antes do worker-start.

    Só adota Run sem coordenador, do próprio coordenador ou já do gerente: o de outro terminal fica como está e o Orca o recusa."""
    g = _gerente_cfg()
    meu = os.environ.get("ORCA_TERMINAL_HANDLE")
    if not g or g.get("coordenador") != meu or run in g["runs"]:
        return
    if (orca("run-show", "--id", run)["run"] or {}).get("coordinator_handle") in (None, meu, g["gerente"]):
        gerente_ligar(g["gerente"], [run])

def gerente_ligar(terminal, runs=None):
    """Liga os Runs ao terminal do agent manager (`run-use` com o handle dele) e grava o gerente.json: daí em diante o orq deste coordenador fala
    com o Orca por esse handle, e os avisos do Orca (heartbeat incluído) vão para o terminal do agent manager, não para o coordenador.

    Soma aos Runs que o mesmo gerente já tem (sem duplicar); outro terminal de gerente recomeça a lista. Sem `runs`, entra o Run ligado ao
    coordenador (o do `run-create` que acabou de rodar). O Orca liga um Run por terminal: só o último fica ligado, o painel os reveza."""
    meu = os.environ.get("ORCA_TERMINAL_HANDLE")
    if not meu:
        raise ValueError("fora de um terminal do Orca (sem ORCA_TERMINAL_HANDLE)")
    if terminal == meu:
        raise ValueError("o agent manager não pode ser o próprio terminal do coordenador")
    runs = [runs] if isinstance(runs, str) else list(runs or [])
    if not runs:
        atual = (orca("run-current", como=meu)["run"] or {}).get("id")  # o Run que o run-create ou o run-use acabou de ligar a este terminal
        runs = [atual] if atual else []
    if not runs:
        raise ValueError("sem Run ligado: passe --run <r>")
    try:
        orca("show", "--terminal", terminal, area="terminal")
    except RuntimeError as e:
        raise ValueError(f"terminal {terminal} não existe no Orca ({e})") from e
    antes = _gerente_cfg()
    ja = antes["runs"] if antes.get("coordenador") == meu and antes.get("gerente") == terminal else []
    todos = list(dict.fromkeys([*ja, *runs]))
    _write_json(_path(GERENTE), {"coordenador": meu, "gerente": terminal, "runs": todos})
    try:
        with trava_gerente():
            for r in runs:
                orca("run-use", "--id", r)  # já pelo handle do gerente
    except Exception:
        _write_json(_path(GERENTE), antes) if antes else os.remove(_path(GERENTE))
        raise
    return append_event({"tipo": "gerente", "op": "ligar", "terminal": terminal, "run": runs[-1], "runs": todos})


def gerente_desligar(run=None):
    """Devolve ao terminal do coordenador (`run-use` com o handle próprio) o Run `run`, ou todos, e tira do gerente.json.

    Sem `run` o arquivo é apagado. O coordenador segura um Run só (um por terminal): fica com o que o gerente tinha ligado, e os outros
    ficam sem coordenador até um `run-use`."""
    g = _gerente_cfg()
    meu = os.environ.get("ORCA_TERMINAL_HANDLE")
    if not g or g.get("coordenador") != meu:
        raise ValueError("agent manager não está ligado a este coordenador")
    if run and run not in g["runs"]:
        raise ValueError(f"o Run {run} não está ligado ao agent manager ({', '.join(g['runs']) or 'nenhum'})")
    if run:
        devolvidos, resto = [run], [r for r in g["runs"] if r != run]
    else:
        try:
            atual = (orca("run-current")["run"] or {}).get("id")
        except RuntimeError:  # terminal do gerente fechado: vale a ordem gravada no ligar
            atual = None
        devolvidos, resto = [*(r for r in g["runs"] if r != atual), *([atual] if atual in g["runs"] else [])], []
    with trava_gerente():
        if resto:
            _write_json(_path(GERENTE), {**{k: v for k, v in g.items() if k != "run"}, "runs": resto})
        else:
            os.remove(_path(GERENTE))
        for r in devolvidos:
            orca("run-use", "--id", r, como=meu)
    return append_event({"tipo": "gerente", "op": "desligar", "terminal": g.get("gerente"), "run": devolvidos[-1] if devolvidos else None, "runs": devolvidos})


def _avisos_gerente():
    """gerente-aviso.json como {run: {vistos, ts}}: os ids das mensagens (fora heartbeat) que o painel já avisou ao coordenador, por Run, e a
    hora do último aviso. O id vale mesmo depois da confirmação: entrega que o coordenador já leu não é avisada de novo. O formato antigo
    (`chave`) não vale."""
    return {r: v for r, v in _dict(_read_json(_path("gerente-aviso.json"))).items() if isinstance(v, dict)}


VISTOS_MAX = 50  # ids lembrados por Run


def _absorver_run(run, liga):
    """Um Run do agent manager: liga o gerente a ele (com `liga`), consome a caixa e confirma os lotes só de heartbeat (confirmar_lotes, o
    mesmo do hook). Devolve (linha do painel, mensagens que sobraram para o coordenador ou None). Tudo sob a trava: a entrega de um Run só
    se confirma na mesma ligação em que foi lida."""
    with trava_gerente():
        if liga:
            orca("run-use", "--id", run)
        ev, res = confirmar_lotes(run, orca("check", "--run", run))
    msgs = res.get("messages") or []
    linha = f"{run}: {len(ev['heartbeats']) if ev else 0} heartbeat(s) absorvido(s)"
    return linha, (None if not msgs or so_heartbeats(msgs) else msgs)


def _run_parado(run, sobrou):
    """Motivo para soltar o Run do gerente, ou None: sem task aberta, sem mensagem (`sobrou`) e sem atividade há RUN_PARADO_MIN minutos."""
    if sobrou:
        return None
    r = resumo_run((orca("run-show", "--id", run)["run"] or {"id": run}) | {"id": run}, orca("task-list", "--run", run)["tasks"])
    ultima = _ts(r["ultima"])
    if r["abertas"] or not ultima or datetime.now(timezone.utc) - ultima < timedelta(minutes=RUN_PARADO_MIN):
        return None
    return f"sem task aberta nem mensagem há mais de {RUN_PARADO_MIN:g} min (última atividade {r['ultima']})"


def gerente_soltar(parados):
    """Tira do gerente.json os Runs {run: motivo}, um evento `soltar` por Run. O último Run fica (sem Run o gerente vira o formato do ticket 17).
    `orq despachar` num Run solto o religa (_adotar)."""
    with trava_gerente():
        g = _gerente_cfg()
        for r, motivo in parados.items():
            if r in g["runs"] and len(g["runs"]) > 1:
                g["runs"].remove(r)
                _write_json(_path(GERENTE), {k: v for k, v in g.items() if k != "run"})
                append_event({"tipo": "gerente", "op": "soltar", "terminal": g["gerente"], "run": r, "motivo": motivo})


def gerente_absorver():
    """Uma volta do painel do agent manager, no terminal dele: percorre os Runs ligados (um `run-use` por Run, o Orca liga um por terminal),
    absorve heartbeat e, quando sobra um lote com outra coisa, digita no coordenador um aviso no formato do Orca, uma vez por lote de
    mensagens, sem confirmar nada: o coordenador lê a entrega com `check --terminal <gerente>`. Devolve uma linha por Run para o painel.

    O check cru do coordenador só vale com o gerente ligado ao Run do aviso: a volta termina nele, e as seguintes só o revisitam (sem sair
    dele) até o coordenador confirmar a entrega, ou por GERENTE_PRESO_S."""
    g = _gerente_cfg()
    if not g or g.get("gerente") != os.environ.get("ORCA_TERMINAL_HANDLE"):
        return "agent manager desligado (orq gerente ligar --terminal <este terminal>, no coordenador)"
    liga = bool(g["runs"])  # gerente.json do ticket 17 (sem runs): o Run é o ligado ao terminal, sem revezar
    runs = g["runs"] or [r for r in [(orca("run-current")["run"] or {}).get("id")] if r]
    if not runs:
        return "agent manager sem Run ligado: rode orq gerente ligar de novo no coordenador"
    avisos, agora = _avisos_gerente(), time.time()
    presos = [r for r in runs if r in avisos and 0 <= agora - avisos[r].get("ts", 0) < GERENTE_PRESO_S][:1]
    linhas, pendentes = [], {}
    for r in [*presos, *(x for x in runs if x not in presos)]:
        linha, msgs = _absorver_run(r, liga)
        vistos = set(avisos.get(r, {}).get("vistos") or [])
        ids = sorted(str(m.get("id")) for m in msgs) if msgs else []
        if msgs:
            pendentes[r] = ids
            tipos = ", ".join(sorted({str(m.get("type")) for m in msgs}))
            linha += f"; {tipos} esperando o coordenador" if set(ids) <= vistos else f"; {tipos} esperando o coordenador ficar livre"
        elif r in avisos:
            avisos[r]["ts"] = 0  # a caixa esvaziou: o coordenador confirmou, o Run deixa de ser o preso
        linhas.append(linha)
        if msgs and r in presos:
            break  # preso e ainda esperando: não sai do Run
    novos = [r for r, ids in pendentes.items() if not set(ids) <= set(avisos.get(r, {}).get("vistos") or [])]
    if pendentes and liga:
        with trava_gerente():  # a volta termina no Run da entrega que o coordenador vai ler
            orca("run-use", "--id", next(iter(pendentes)))
    for r in novos:
        n = len(pendentes[r])
        texto = f"You have {n} orchestration message{'s' if n > 1 else ''}. Run `orca orchestration check --run {r} --terminal {g['gerente']}`."
        if digita(g["coordenador"], texto) != "enviado":
            break  # coordenador no meio do turno ou com rascunho (ou o Orca recusou): nada foi digitado, a próxima volta tenta
        vistos = [*(avisos.get(r, {}).get("vistos") or []), *pendentes[r]]
        avisos[r] = {"vistos": vistos[-VISTOS_MAX:], "ts": time.time()}
        _write_json(_path("gerente-aviso.json"), avisos)  # a cada aviso: falha em outro Run depois dele não o repete
        linhas = [x.replace("esperando o coordenador ficar livre", "avisado ao coordenador") if x.startswith(f"{r}:") else x for x in linhas]
    if avisos != _avisos_gerente():
        _write_json(_path("gerente-aviso.json"), avisos)
    if liga:
        parados = {}
        for r in runs:
            try:
                motivo = _run_parado(r, r in pendentes or r in avisos and avisos[r].get("ts"))
            except RuntimeError:  # Orca fora do ar: não solta nada
                continue
            if motivo:
                parados[r] = motivo
        gerente_soltar(parados)
        linhas += [f"{r}: solto do gerente ({m})" for r, m in parados.items()]
    try:
        linhas += reentrega_steers()
    except Exception as e:  # noqa: BLE001 - o painel não cai por causa do acompanhamento dos steers; a próxima volta tenta
        log(f"steers: {type(e).__name__}: {e}")
    return "\n".join(linhas)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="orq")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("hook").add_argument("kind", choices=list(HOOKS))
    i = sub.add_parser("intake")
    i.add_argument("entrada")
    i.add_argument("efeito")
    i.add_argument("ref", nargs="?")
    i.add_argument("--run")
    i.add_argument("--nota")
    p = sub.add_parser("pend").add_subparsers(dest="op", required=True)
    pa = p.add_parser("add")
    pa.add_argument("--id", required=True)
    pa.add_argument("--tipo", required=True, choices=TIPOS_PEND)
    pa.add_argument("--titulo", required=True)
    for k in ("detalhe", "frente", "link", "comando", "espera", "task", "ate"):
        pa.add_argument(f"--{k}")
    pa.add_argument("--run", help="o Run da task do gate (obrigatório com o agent manager em mais de um Run)")
    pl = p.add_parser("lista", help="as pendências vivas; --todas inclui as de Depois")
    pl.add_argument("--todas", action="store_true")
    pd = p.add_parser("done")
    pd.add_argument("id")
    pd.add_argument("--resposta")
    st = sub.add_parser("steer")
    st.add_argument("task")
    st.add_argument("texto")
    st.add_argument("--run")
    st.add_argument("--entrada")
    sub.add_parser("steers", help="reentrega o aviso dos ajustes que o worker parado não leu e grava o alerta na terceira falha (o painel do gerente já faz)")
    rp = sub.add_parser("responder", help="responde a pergunta de um worker pelo gerente, ligando o Run da mensagem antes")
    rp.add_argument("msg_id")
    rp.add_argument("texto")
    sub.add_parser("status")
    rs = sub.add_parser("resumo", help="as quatro partes (com você, entrou, anda, vem) e as decisões desde a última mensagem do usuário")
    rs.add_argument("--desde", help="carimbo ISO (AAAA-MM-DDTHH:MM:SSZ) em vez da última mensagem do usuário")
    al = sub.add_parser("alerta", help="trata um alerta de scout sem reportPath").add_subparsers(dest="op", required=True)
    al.add_parser("visto").add_argument("task")
    ag = sub.add_parser("agentes", help="o estado de cada dispatch em todos os Runs")
    ag.add_argument("--json", action="store_true")
    ag.add_argument("--run", help="só os dispatches deste Run (obrigatório se o worker-list vier escopado)")
    ag.add_argument("--todos", action="store_true", help="inclui os liberados e os retidos pelo Orca (user_takeover…)")
    li = sub.add_parser("liberar", help="ack pendente, worker-release e terminal close se vier retained")
    li.add_argument("dispatch")
    li.add_argument("--run")
    it = sub.add_parser("interromper", help="manda o interrupt ao terminal do worker que roda (o worker segue vivo)")
    it.add_argument("dispatch")
    it.add_argument("--run")
    en = sub.add_parser("encerrar", help="worker-stop (se ainda roda) e release, com o motivo no log")
    en.add_argument("dispatch")
    en.add_argument("--motivo", required=True)
    en.add_argument("--run")
    rl = sub.add_parser("relancar", help="para o worker e sobe outro na mesma worktree e task (--retry-of), com a nota do que mudou")
    rl.add_argument("dispatch")
    rl.add_argument("--nota", required=True)
    rl.add_argument("--modelo", help="troca o modelo (vai junto com --effort); sem ele, o do worker antigo")
    rl.add_argument("--effort")
    rl.add_argument("--run")
    nt = sub.add_parser("noite", help="modo noite: orq noite ligar --ate HH:MM [--max-despachos N] [--max-falhas 3] | desligar | (sem op: estado)")
    nt.add_argument("op", nargs="?", choices=["ligar", "desligar"])
    nt.add_argument("--ate")
    nt.add_argument("--max-despachos", type=int)
    nt.add_argument("--max-falhas", type=int, default=NOITE_FALHAS)
    de = sub.add_parser("despachar", help="worker-start com modelo e effort, evento e intake")
    de.add_argument("--run", required=True)
    de.add_argument("--titulo")
    de.add_argument("--spec-arquivo")
    de.add_argument("--ticket", help="número de um ticket criado por orq ticket novo: o worker sobe na task dele (no lugar de --titulo e --spec-arquivo)")
    de.add_argument("--modelo", required=True)
    de.add_argument("--effort", required=True)
    de.add_argument("--worktree", choices=["current", "new-top-level"])
    de.add_argument("--name")
    de.add_argument("--base-branch")
    de.add_argument("--entrada")
    tk = sub.add_parser("ticket", help="tickets em arquivo (ISSUES/NN-slug.md) com a task no Orca").add_subparsers(dest="op", required=True)
    tn = tk.add_parser("novo", help="cria o arquivo e a task a partir de um título e de um arquivo de spec")
    tn.add_argument("--titulo", required=True)
    tn.add_argument("--spec-arquivo", required=True)
    tn.add_argument("--blocked-by", help="números dos tickets que bloqueiam este, separados por vírgula")
    tn.add_argument("--run", help="Run da task (o ligado ao coordenador, por padrão)")
    tf = tk.add_parser("fechar", help="grava o Answer, põe resolved e completa a task")
    tf.add_argument("numero")
    tf.add_argument("--answer", required=True, help="texto ou caminho de um arquivo")
    tl = tk.add_parser("lista", help="os tickets abertos (--todos inclui os resolvidos)")
    tl.add_argument("--todos", action="store_true")
    tl.add_argument("--json", action="store_true")
    sub.add_parser("lavish-resposta").add_argument("arquivo", help="saída do `lavish-axi poll` (crua, ou o JSON dela); `-` lê a entrada padrão")
    sub.add_parser("auditar-respostas").add_argument("--sessao", help="id (ou prefixo) da sessão; sem ele, a última sessão de coordenador do cursor.json")
    ge = sub.add_parser("gerente", help="agent manager em terminal próprio: os avisos do Orca vão para ele, não para o coordenador").add_subparsers(dest="op", required=True)
    gl = ge.add_parser("ligar", help="no coordenador: liga Runs ao terminal do agent manager (soma aos que ele já tem)")
    gl.add_argument("--terminal", required=True)
    gl.add_argument("--run", action="append", help="repita para ligar vários; sem --run entra o Run ligado ao coordenador")
    gd = ge.add_parser("desligar", help="no coordenador: devolve um Run (--run) ou todos a este terminal")
    gd.add_argument("--run")
    ge.add_parser("absorver", help="no terminal do agent manager: confirma heartbeat e avisa o coordenador do resto, Run por Run")
    ru = sub.add_parser("runs", help="os Runs com trabalho aberto ou recentes (--todos: o arquivo e os de teste)")
    ru.add_argument("--todos", action="store_true")
    ru.add_argument("--json", action="store_true")
    sub.add_parser("ingest").add_argument("--refresh", action="store_true", help="depois do ingest, refaz o aberto.json")
    a = ap.parse_args(argv)
    if a.cmd == "hook":
        return run_hook(a.kind)
    try:
        if a.cmd == "intake":
            print(json.dumps(intake(a.entrada, a.efeito, a.ref, a.run, a.nota), ensure_ascii=False))
        elif a.cmd == "pend":
            if a.op == "add":
                print(json.dumps(pend_add(a.id, a.tipo, a.titulo, a.detalhe, a.frente, a.link, a.comando, a.espera, a.task, a.ate, a.run), ensure_ascii=False))
            elif a.op == "lista":
                print("\n".join(pend_lista(a.todas)) or "nenhuma pendência")
            else:
                feito = pend_done(a.id, a.resposta)
                print(json.dumps(feito, ensure_ascii=False))
                if feito.get("aviso"):
                    print(f"aviso: {feito['aviso']}", file=sys.stderr)
        elif a.cmd == "steer":
            print(json.dumps(steer(a.task, a.texto, a.run, a.entrada), ensure_ascii=False))
        elif a.cmd == "steers":
            print("\n".join(reentrega_steers()) or "nenhum ajuste a reentregar")
        elif a.cmd == "responder":
            print(json.dumps(responder(a.msg_id, a.texto), ensure_ascii=False))
        elif a.cmd == "alerta":
            print(json.dumps(append_event({"tipo": "alerta_visto", "task": a.task}), ensure_ascii=False))
        elif a.cmd == "status":
            print(estado())
        elif a.cmd == "resumo":
            if a.desde:
                _dt(a.desde)  # ValueError vira exit 1
            print(resumo_quatro(read_events(), _read_json(_path("aberto.json")), _read_json(PEND), tickets(), a.desde and _dt(a.desde).strftime("%Y-%m-%dT%H:%M:%SZ"), painel=aviso_painel()))
        elif a.cmd == "agentes":
            ags = agentes(a.run, a.todos)
            parada = [l for l in linhas_noite(_cursor_ro(), read_events()) if "Parou de despachar" in l]
            print(json.dumps(ags, ensure_ascii=False) if a.json else "\n".join([texto_agentes(ags), *parada]))
        elif a.cmd == "runs":
            rs = runs_lista(a.todos)
            print(json.dumps(rs, ensure_ascii=False) if a.json else texto_runs(rs))
        elif a.cmd == "liberar":
            r = liberar(a.dispatch, a.run)
            print(json.dumps(r, ensure_ascii=False))
            if r["aviso"]:
                print(f"aviso: {r['aviso']}", file=sys.stderr)
        elif a.cmd in ("interromper", "encerrar", "relancar"):
            r = interromper(a.dispatch, a.run) if a.cmd == "interromper" else encerrar(a.dispatch, a.motivo, a.run) if a.cmd == "encerrar" \
                else relancar(a.dispatch, a.nota, a.modelo, a.effort, a.run)
            print(json.dumps(r, ensure_ascii=False))
            if r.get("aviso"):
                print(f"aviso: {r['aviso']}", file=sys.stderr)
        elif a.cmd == "noite":
            if a.op == "ligar":
                noite_ligar(a.ate, a.max_despachos, a.max_falhas)
            elif a.op == "desligar":
                print("modo noite desligado" if noite_desligar() else "modo noite já estava desligado")
                return 0
            print("\n".join(linhas_noite(_cursor_ro(), read_events())) or "modo noite desligado")
        elif a.cmd == "despachar":
            r = despachar(a.run, a.titulo, a.spec_arquivo, a.modelo, a.effort, a.worktree, a.name, a.base_branch, a.entrada, a.ticket)
            print(json.dumps(r, ensure_ascii=False))
            if r.get("aviso"):
                print(f"aviso: {r['aviso']}", file=sys.stderr)
        elif a.cmd == "ticket":
            if a.op == "novo":
                print(json.dumps(ticket_novo(a.titulo, a.spec_arquivo, a.blocked_by, a.run), ensure_ascii=False))
            elif a.op == "fechar":
                r = ticket_fechar(a.numero, a.answer)
                print(json.dumps(r, ensure_ascii=False))
                if r["aviso"]:
                    print(f"aviso: {r['aviso']}", file=sys.stderr)
            else:
                ts = [t for t in tickets() if a.todos or t["status"] != STATUS_FECHADO]
                print(json.dumps(ts, ensure_ascii=False) if a.json else "\n".join(linha_ticket(t) for t in ts) or "nenhum ticket aberto")
        elif a.cmd == "lavish-resposta":
            r = lavish_resposta(a.arquivo)
            print(json.dumps(r, ensure_ascii=False))
            for x in r["avisos"]:
                print(f"aviso: {x}", file=sys.stderr)
        elif a.cmd == "gerente":
            if a.op == "ligar":
                print(json.dumps(gerente_ligar(a.terminal, a.run), ensure_ascii=False))
            elif a.op == "desligar":
                print(json.dumps(gerente_desligar(a.run), ensure_ascii=False))
            else:
                print(gerente_absorver())
        elif a.cmd == "auditar-respostas":
            print(auditar_respostas(a.sessao), end="")
        else:
            n = ingest()
            print("ingest em andamento por outro processo" if n is None else
                  f"ingest: {n[0]} entrada(s) nova(s)" + (f", {n[1]} gate(s) resolvido(s)" if n[1] else ""))
            if a.refresh:
                ab = refresh_aberto()
                print("refresh em andamento por outro processo" if ab is None else
                      f"aberto.json: backlog {len(ab['backlog'])}, rodando {ab['rodando']}, bloqueado {len(ab['bloqueado'])}, gates {len(ab['gates'])}")
    except Exception as e:  # noqa: BLE001 - erro esperado (recusa) só sai na tela; o ingest e o inesperado vão também para o log
        print(f"orq: {e}" if isinstance(e, (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired)) else f"orq: {type(e).__name__}: {e}", file=sys.stderr)
        if a.cmd == "ingest" or not isinstance(e, (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired)):
            log(f"{a.cmd}{' --refresh' if getattr(a, 'refresh', False) else ''}: {type(e).__name__}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
