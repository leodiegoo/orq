#!/usr/bin/env python3
"""orq: registro de entradas do orquestrador (fatias 1, 3, 4, 6, 7, 8 e 9, e as correções dos reviews). Só stdlib. Desenho: docs/design.md

ORQ_PENDENCIAS troca o pendencias.json, ORQ_HOME troca o diretório de dados, ORQ_ORCA o binário do Orca, ORQ_LOG o log, ORQ_TRANSCRITOS a pasta dos transcritos do coordenador, ORQ_PROJETOS a pasta `projects` do Claude Code (transcritos dos workers), ORQ_NO_BG=1 desliga o refresh em segundo plano, ORQ_ISSUES a pasta dos tickets e ORQ_MAPA o mapa (desenho.md).
"""
import argparse
import contextlib
import fcntl
import glob
import html
import json
import os
import pathlib
import re
import shlex
import signal
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timedelta, timezone


def ThreadPoolExecutor(n):  # import tardio: concurrent.futures custa ~11 ms e só o painel e o ingest usam (ticket 49)
    from concurrent.futures import ThreadPoolExecutor as _T
    return _T(n)

HOME = os.environ.get("ORQ_HOME") or os.path.expanduser("~/.claude/orq")
ORCA = os.environ.get("ORQ_ORCA") or "orca"
GH = os.environ.get("ORQ_GH") or "gh"
LOG = os.environ.get("ORQ_LOG") or os.path.expanduser("~/.claude/logs/orq.log")
PEND = os.environ.get("ORQ_PENDENCIAS") or os.path.expanduser("~/.claude/dashboard/data/pendencias.json")
EFEITOS = ("tarefa", "steer", "pend", "decisao", "conversa", "descartado", "mate")
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
TELA_TETO_S = 45 * 60  # shell/monitor em execução na tela sem heartbeat por tanto tempo: deixa de ser espera e vira travado ("shell sem heartbeat")
TELA_ESPERA = re.compile(r"(?:\d+\s+)?(?:shell|monitor)s?\s+still\s+running", re.I)  # o rodapé do Claude Code com o turno encerrado esperando um processo em segundo plano
TELA_LINHAS = 30  # linhas do fim da tela lidas por terminal (o rodapé fica nas últimas; o prompt de permissão com o comando e o aviso passa de 15)
TELA_OPCAO = re.compile(r"^\s*([❯>])?\s*(\d{1,2})\.\s+(\S.*?)\s*$")  # `❯ 1. Yes`: a opção de um menu do Claude Code, com o cursor na escolhida
TELA_PERGUNTAS = (("trust", re.compile(r"trust (?:this|the files in this) folder|Is this a project you (?:created|trust)", re.I)),
                  ("permissao", re.compile(r"Do you want to \w+|Yes, and don't ask again", re.I)),
                  ("pergunta", re.compile(r"Enter to select|Type something|Chat about this", re.I)))  # o AskUserQuestion aberto
TELA_RODAPE_MAX = 4  # linhas não vazias depois das opções (rodapé do menu) para o menu ainda contar como aberto
ESPERA_FASE = re.compile(r"^\s*(?:esperando:|waiting\b[:\s.…-]*)\s*(.*?)(?:\s+até\s+(\d{1,2}):(\d{2}))?\s*$", re.I)
STEER_LEITURA_S = 90  # ajuste que o dispatch não leu (`read` no inbox do Orca ou o id no transcrito do worker) tanto tempo depois do envio, ou da última redigitação, é reentregue
STEER_TENTATIVAS = 3  # redigitações do aviso ao worker parado; sem leitura STEER_LEITURA_S depois da terceira, vira o alerta "steer não lido"
STEER_TRANSCRITO_BYTES = 4_000_000  # o fim do transcrito do worker onde se procura o id da mensagem do steer
STEER_JANELA_S = 30 * 60  # steer mais velho que isto sai do acompanhamento: o inbox de 200 mensagens já não o alcança
NAO_COMECOU_S = 120  # dispatch aberto sem nenhum turno registrado tanto tempo depois do despacho não começou (o worker-start que ficou sem Enter)
PARADO_S = 60  # o turno terminou há tanto tempo e nada mais veio: o worker parou no prompt (o limiar evita chamar de parado a folga entre dois turnos)
TURNOS = "turnos.json"  # {dispatch: {task, sessao, inicio, fim}}: o que os hooks prompt e stop do worker gravam, sem chamar o Orca
TURNOS_DIAS = 7  # turno mais velho que isto sai do turnos.json na próxima gravação
CONTROLE_LINHAS = 5  # entradas do histórico de controle que o `orq agentes` mostra por dispatch (as mais novas)
ORDEM_AGENTES = {"travado": 0, "nao_comecou": 1, "parado": 2, "perguntando": 3, "rodando": 4, "entregue": 5, "hibernado": 6, "encerrado": 7, "liberado": 8}
INICIO_ESPERA_S = float(os.environ.get("ORQ_INICIO_ESPERA_S") or 8)  # quanto o `orq despachar` espera o prompt do spec entrar no worker, antes e depois do Enter
ID_DISPATCH = re.compile(r"--dispatch-id (ctx_\w+)")  # no preâmbulo de despacho do Orca, nos comandos que o worker roda
ID_TASK = re.compile(r"Your task ID is: (task_\w+)")
HB_JANELA_S = 120  # aviso que chega logo depois de um lote de heartbeats absorvido encontra a caixa vazia: também é bloqueado
HB_LOTES = 4  # lotes de heartbeat seguidos que um só aviso confirma (o --ack devolve o próximo lote)
AVISO_RUN = re.compile(r"orchestration check --run (run_\w+)")
RESUMO_PEDIDO_S = 60  # a entrada do usuário mais nova que isso é o pedido do próprio `orq resumo`
ANDA = ("rodando", "perguntando", "travado", "parado", "nao_comecou")  # estados que aparecem em "Anda" do `orq resumo`
PAINEL_VIVO = "gerente-vivo"  # o painel do agent manager toca este arquivo a cada volta (painel-agent-manager.sh), fora do orq
PAINEL_PARADO_S = 60
PAINEL_CHECAGEM = "gerente-checagem.json"  # {ts, terminal, morto}: o que o último `orq gerente checar` (fora do hook) viu no Orca; o hook só o lê
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
PRS = "prs.json"  # {itens: [{task, url, numero, base, estado, ligado_em, avisado, ...}], ultimo_poll}: os PRs de cada feature, ligados por `orq pr ligar`
PR_POLL_S = float(os.environ.get("ORQ_PR_POLL_S") or 120)  # intervalo mínimo entre dois polls do gh (fora dos hooks); `orq pr poll --forcar` ignora
PR_GH_S = 15  # tempo de cada `gh pr view`
LEITURA_VELHA_MIN = float(os.environ.get("ORQ_LEITURA_VELHA_MIN") or 10)  # minutos: o CI e o conflito lidos há mais que isso aparecem como leitura velha
CHECK_FALHO = {"FAILURE", "TIMED_OUT", "CANCELLED", "ACTION_REQUIRED", "STARTUP_FAILURE", "ERROR"}
WT_PARADA_D = 3  # worktree sem worker e sem atividade há mais que isto entra na linha do `orq status`
PR_VISIVEL_D = 7  # feature com todos os PRs resolvidos há mais que isto sai do `orq status`
AMBIENTES = ("development", "staging", "main")  # a ordem da promoção por feature branch
TITULOS_ACAO = re.compile(r"^#{1,6}\s*(?:\d+\.\s*)?(?:Itens de ação|O que fazer hoje|O que precisa de ação)\s*$", re.I)
TELA_FALHA = ("No conversation found", "command not found")  # o claude --resume não achou a sessão, ou o comando nem existe


# ---------- harness (claude | codex) ----------
# O que muda de um agente para o outro, numa tabela: o comando de resume (o launch é do Orca, `worker-start --agent`) e os padrões da tela.
# Agente fora da tabela não tem hook do orq nem tela lida: o estado dele fica `unknown`. Desenho: ~/.claude/orquestrador-plan/orq-claude-e-codex.md
HARNESS = {
    "claude": {
        "resume": lambda sessao, modelo, effort, msg: ["claude", "--resume", sessao, *(["--model", modelo] if modelo else []),
                                                       "--dangerously-skip-permissions", msg],
        "tela": {"opcao": TELA_OPCAO, "cursor": "❯", "perguntas": TELA_PERGUNTAS, "espera": TELA_ESPERA, "falha": TELA_FALHA},
        "abrir": lambda modelo, effort, msg: ["claude", *(["--model", modelo] if modelo else []), "--dangerously-skip-permissions", msg],  # o mate (ticket 80)
        "efforts": ("low", "medium", "high", "xhigh", "max"),
        "filho": re.compile(r"/shell-snapshots/"),  # o comando do Bash tool (E2E, teste, build, shell em segundo plano) sobe como `zsh -c source ~/.claude/shell-snapshots/…`, filho do claude
    },
}
# o Codex numera as opções com `›` (ou `>`) no cursor; o trust da pasta e o modal dos hooks não confiados são os menus que param um worker dele
TELA_OPCAO_CODEX = re.compile(r"^\s*([›>])?\s*(\d{1,2})\.\s+(\S.*?)\s*$")
HARNESS["codex"] = {
    "resume": lambda sessao, modelo, effort, msg: ["codex", "resume", sessao, *(["-m", modelo] if modelo else []),
                                                   *(["-c", f'model_reasoning_effort="{effort}"'] if effort else []),
                                                   "--dangerously-bypass-approvals-and-sandbox", msg],
    "abrir": lambda modelo, effort, msg: ["codex", *(["-m", modelo] if modelo else []), *(["-c", f'model_reasoning_effort="{effort}"'] if effort else []),
                                          "--dangerously-bypass-approvals-and-sandbox", msg],
    "tela": {"opcao": TELA_OPCAO_CODEX, "cursor": "›", "enter_separado": True,  # o número com Enter no mesmo send não confirma o menu (01/10)
             "perguntas": (("trust", re.compile(r"Trust this folder\?|Do you trust the contents of this directory", re.I)),
                           ("hooks", re.compile(r"Hooks? need review|hooks? (?:are|is) new or changed", re.I))),
             "espera": re.compile(r"\d+\s+background terminals?\s+running", re.I),  # `• Working (9s • esc to interrupt) · 1 background terminal running`
             "falha": ("No saved session found", "command not found")},
    "efforts": ("low", "medium", "high", "xhigh", "max", "ultra"),  # ~/.codex/models_cache.json: o ultra só nos modelos que o têm (o Orca recusa no Luna)
}
HARNESSES = tuple(HARNESS)
CODEX_CONFIG = os.environ.get("ORQ_CODEX_CONFIG") or os.path.join(os.environ.get("CODEX_HOME") or os.path.expanduser("~/.codex"), "config.toml")
PATCH_ARQUIVO = re.compile(r"^\*\*\* (?:Add|Update|Delete) File: (.+?)\s*$", re.M)  # o caminho de cada arquivo que o apply_patch do Codex mexe


# ---------- puras ----------

AVISOS_ORQ = ("orq: PR ", "orq: Fila do E2E", "orq: uso do plano", "orq: worker ", "orq ▸ pedido ", "orq ▸ mate ")  # o que o painel digita no coordenador (avisa_coordenador)


def origem(prompt):
    """usuario | notificacao | orca | comando | resumo | despacho | aviso_orq (a linha que o painel digita quando um PR ligado é resolvido)."""
    p = (prompt or "").lstrip()
    if p.startswith("<task-notification"):
        return "notificacao"
    if re.match(r"You have \d+ orchestration", p):
        return "orca"
    if p.startswith(("<command-", "<local-command", "/compact")):
        return "comando"
    if p.startswith("This session is being continued"):
        return "resumo"
    if p.startswith(AVISOS_ORQ):
        return "aviso_orq"
    if p.startswith("Please carry out this task from my Orca coordinator") or DESPACHO_SEM_ABERTURA.match(p):
        return "despacho"
    return "usuario"


def separa_aviso(prompt):
    """(texto sem o aviso do Orca no fim, se havia): o Orca digita o aviso na linha em que o usuário escrevia, e o resto pode ser digitação pela metade (B26)."""
    p = prompt or ""
    m = AVISO_FINAL.search(p)
    return (p[:m.start()].rstrip(), True) if m else (p, False)


def abertas(events, grupo=None):
    """Entradas sem nenhum intake com o mesmo id, na ordem em que entraram. Só as de quem lê: as do mate do `grupo` (padrão: o ORQ_MATE do ambiente)
    ou, sem grupo, as do coordenador. A entrada digitada no mate leva `grupo`; a que o mate sobe (origem mate) não, e é do coordenador (ticket 80)."""
    g = grupo or os.environ.get("ORQ_MATE") or None
    fechadas = {e.get("entrada") for e in events if e.get("tipo") == "intake"}
    return [e for e in events if e.get("tipo") == "entrada" and e.get("id") and e["id"] not in fechadas and e.get("grupo") == g]


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
    motivo = m.group(1) or "esperando"
    if m.group(2) is None:
        return motivo, ts + timedelta(seconds=ESPERA_TETO_S)
    local = ts.astimezone()
    prazo = local.replace(hour=int(m.group(2)) % 24, minute=int(m.group(3)) % 60, second=0, microsecond=0)
    return motivo, (prazo if prazo >= local else prazo + timedelta(days=1)).astimezone(timezone.utc)


def _vivo_ou_travado(fase, ts, idade, agora, tela=None, interrompido=False):
    """(estado, espera, motivo) de um dispatch aberto pelo último heartbeat: esperando declarado dentro do prazo não é travado; vencido, é "espera vencida".

    Sem espera declarada: `interrompido` (o coordenador pausou o worker) e `tela` (shell/monitor em execução no rodapé) também são espera, esta só até
    TELA_TETO_S sem heartbeat. `idade` é a do último heartbeat (ou do despacho).
    """
    esp = espera_declarada(fase, ts)
    if esp and agora <= esp[1]:
        return "rodando", esp[0], None
    if esp:
        return "travado", None, "espera vencida"
    if interrompido:
        return "rodando", "interrompido pelo coordenador", None
    if tela:
        return ("travado", None, "shell sem heartbeat") if idade is not None and idade > TELA_TETO_S else ("rodando", tela, None)
    return ("travado" if idade is not None and idade > TRAVADO_S else "rodando"), None, None


def interrompidos(events):
    """{dispatch: ts} do último `orq interromper` bem-sucedido de cada dispatch."""
    return {e["dispatch"]: e.get("ts") for e in events if e.get("tipo") == "controle" and e.get("acao") == "interromper" and e.get("resultado") == "ok" and e.get("dispatch")}


def _pausado(ts_interrupt, ultimo_hb, t):
    """O interromper vale até o worker dar sinal depois dele: heartbeat ou novo turno (prompt) mais novos que o interrupt o encerram."""
    i = _ts(ts_interrupt)
    return bool(i) and not (ultimo_hb and _ts(ultimo_hb) > i) and not (_ts(_dict(t).get("inicio")) and _ts(t["inicio"]) > i)


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
    if agente not in HARNESS:
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


def monta_agentes(workers, msgs, events, agora, detalhes=None, vivos=None, turnos=None, telas=None, perguntas_tela=None, hibernados=None):
    """Pura: uma linha por dispatch do worker-list, com o estado (rodando, travado, nao_comecou, parado, perguntando, entregue ou liberado).

    Dispatched sem pergunta aberta é `nao_comecou` ou `parado` quando os turnos dos hooks do worker dizem (turno_do_dispatch); senão `travado` quando o
    último heartbeat (ou, sem nenhum, o despacho) tem mais de TRAVADO_S; completed com o terminal released é `liberado`, o resto é `entregue`
    (worker_done dado, terminal ainda aberto). `detalhes` é {dispatch: titulo, modelo, desde, agente}; `vivos` são os handles do `orca terminal list`;
    `turnos` é o turnos.json (None: sem dado, o turno fica `unknown`); `telas` é {dispatch: motivo} do que a tela do terminal mostra esperando;
    `perguntas_tela` é {dispatch: tela_pergunta} dos menus esperando resposta humana no terminal (o worker vira `perguntando`, com a `pergunta` na linha);
    `hibernados` é o cursor.json `hibernados` ({dispatch: {desde, motivo, …}}): o worker vira `hibernado` (terminal fechado de propósito, sessão guardada).
    """
    sinais, perguntas, detalhes, humanos = ultimos_sinais(events, msgs), perguntas_abertas(msgs), detalhes or {}, _interacao_registrada(events)
    liberados, pausas, telas, nao_iniciou = _liberados(events), interrompidos(events), telas or {}, _nao_iniciou(events)
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
        turno, idade_hb = "unknown", idade
        if w.get("dispatchStatus") == "dispatched":
            if turnos is not None:
                turno, quando = turno_do_dispatch(t, det.get("agente"), _ts(det.get("desde")), _ts(sinal.get("ts")), agora)
                if quando:
                    idade = int((agora - quando).total_seconds())
            pausa = _pausado(pausas.get(d), sinal.get("ts"), t)
            estado, espera, motivo = _vivo_ou_travado(sinal.get("fase"), sinal.get("ts"), idade_hb, agora, telas.get(d), pausa)
            estado = "perguntando" if d in perguntas or (perguntas_tela or {}).get(d) else turno if turno in ("nao_comecou", "parado") and not (espera or motivo) else estado  # espera declarada (dentro do prazo ou vencida) vale mais que o turno encerrado (M16)
            if d in nao_iniciou and not (t.get("inicio") or sinal):  # o despachar viu o prompt não entrar: não espera NAO_COMECOU_S
                estado = "nao_comecou"
        else:
            espera = motivo = None
            feito = any(m.get("type") == "worker_done" and _payload(m).get("dispatchId") == d for m in msgs or [])
            estado, idade = ("liberado" if w.get("terminalState") == "released" or _sem_terminal(w, liberados, vivos) else "entregue" if feito else "encerrado"), None
        ag = {"dispatch": d, "task": w.get("taskId"), "run": w.get("runId"), "titulo": det.get("titulo"), "modelo": det.get("modelo"),
              "effort": det.get("effort"), "terminal": w.get("agentTerminalHandle"), "estado": estado, "fase": sinal.get("fase"), "ultimo_heartbeat": _z(sinal.get("ts")),
              "desde": _z(det.get("desde")), "idade_s": idade, "agente": det.get("agente"), "turno": turno,
              "turno_inicio": _z(t.get("inicio")), "turno_fim": _z(t.get("fim"))}
        if espera:
            ag["espera"] = espera
        if (perguntas_tela or {}).get(d) and w.get("dispatchStatus") == "dispatched":
            ag["pergunta"] = perguntas_tela[d]
        if telas.get(d):
            ag["tela"], ag["tela_ts"] = telas[d], agora.strftime("%Y-%m-%dT%H:%M:%SZ")
        if motivo and estado == "travado":
            ag["motivo"] = motivo
        if avisos_entrega.get(d):
            ag["entrega"] = avisos_entrega[d]
        if controles.get(d):
            ag["controle"] = controles[d][-CONTROLE_LINHAS:]
        retido = _retencao(w, humanos)
        if estado == "entregue" and retido:
            ag["retido"] = retido
        if d in (hibernados or {}):
            ag.update(estado="hibernado", idade_s=None, hibernado_desde=_dict(hibernados[d]).get("desde"), motivo_hibernado=_dict(hibernados[d]).get("motivo"))
            for k in ("espera", "pergunta", "tela", "tela_ts", "motivo", "retido"):
                ag.pop(k, None)
        out.append(ag)
    return sorted(out, key=lambda a: ORDEM_AGENTES[a["estado"]])


def _nao_iniciou(events):
    """Os dispatches que o `orq despachar` marcou com `nao_iniciou` (o prompt do spec não entrou, nem depois do Enter)."""
    return {e.get("dispatch") for e in events if e.get("tipo") == "nao_iniciou"}


def reavalia(agentes_, events, agora, turnos=None):
    """Os agentes do cache do aberto.json com rodando/travado/nao_comecou/parado refeito pelo heartbeat mais novo do log e, com `turnos`, pelo turnos.json (o cache atrasa até um prompt)."""
    sinais = sinais_de_vida(events)
    liberados = _liberados(events) | {e.get("dispatch") for e in events if e.get("tipo") == "liberar" and e.get("estado") == "released"}
    pausas, out = interrompidos(events), []
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
            t = _dict(turnos.get(ag.get("dispatch"))) if turnos is not None else {"inicio": ag.get("turno_inicio"), "fim": ag.get("turno_fim")}
            tela = ag.get("tela") if ag.get("tela") and _ts(ag.get("tela_ts")) and not (_ts(ag.get("ultimo_heartbeat")) and _ts(ag["ultimo_heartbeat"]) > _ts(ag["tela_ts"])) \
                and not (_ts(t.get("inicio")) and _ts(t["inicio"]) > _ts(ag["tela_ts"])) else None  # a tela lida só vale até o worker dar sinal depois dela
            ag["estado"], espera, motivo = _vivo_ou_travado(ag.get("fase"), ag.get("ultimo_heartbeat"), ag["idade_s"], agora, tela,
                                                            _pausado(pausas.get(ag.get("dispatch")), ag.get("ultimo_heartbeat"), t))
            ag.pop("espera", None), ag.pop("motivo", None)
            if espera:
                ag["espera"] = espera
            if motivo:
                ag["motivo"] = motivo
            ag["turno"], quando = turno_do_dispatch(t, ag.get("agente"), _ts(ag.get("desde")), _ts(ag.get("ultimo_heartbeat")), agora)
            if ag["turno"] in ("nao_comecou", "parado") and not (espera or motivo):  # quem espera de propósito (fila de E2E, CI) encerra o turno e não está parado; vencida, é travado (M16)
                ag["estado"], ag["idade_s"] = ag["turno"], int((agora - quando).total_seconds())
            elif ag.get("dispatch") in _nao_iniciou(events) and not (ag.get("turno_inicio") or h):
                ag["estado"] = "nao_comecou"
        out.append(ag)
    return out


def linha_vivos(events, aberto, agora=None, turnos=None):
    """'Vivos: task fase HH:MM, …' com o estado de cada dispatch rodando, travado, não começado, parado ou perguntando, e os entregues sem liberar; '' se não há nada.

    Usa os agentes do cache (aberto.json, fatia 8) com o estado refeito pelo log; cache sem `agentes` cai nos `andamento` com a última fase.
    """
    agora = agora or datetime.now(timezone.utc)
    if isinstance((aberto or {}).get("agentes"), list):
        ags = reavalia(aberto["agentes"], events, agora, turnos)
        vivos = sorted((a for a in ags if a["estado"] in ("travado", "nao_comecou", "parado", "perguntando", "rodando")), key=lambda a: a.get("prioridade") or 2)
        sem_liberar = sum(a["estado"] == "entregue" and not a.get("retido") for a in ags)
        itens = []
        for a in vivos[:3]:
            nome = f"{'P' + str(a['prioridade']) + ' ' if a.get('prioridade') else ''}{(a.get('task') or '?')[:9]}… "
            itens.append(nome + (f"TRAVADO há {(a.get('idade_s') or 0) // 60} min" + (f" ({a['motivo']})" if a.get("motivo") else "") if a["estado"] == "travado"
                                 else f"NÃO COMEÇOU há {(a.get('idade_s') or 0) // 60} min" if a["estado"] == "nao_comecou"
                                 else f"parado no prompt há {(a.get('idade_s') or 0) // 60} min" if a["estado"] == "parado" else "pergunta" if a["estado"] == "perguntando"
                                 else f"{a.get('fase') or '?'} {_hora_local(a['ultimo_heartbeat'])}" if a.get("ultimo_heartbeat") else "sem heartbeat"))
            if a["estado"] == "rodando" and a.get("espera") and a["espera"] not in (a.get("fase") or ""):
                itens[-1] += f" (esperando: {a['espera']})"
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
        idade = None
    if idade is not None and idade <= PAINEL_PARADO_S:
        return None
    ck = _dict(_read_json(_path(PAINEL_CHECAGEM)))
    if ck.get("morto") and ck.get("terminal") == g.get("gerente"):
        return (f"o terminal do agent manager ({g['gerente']}) sumiu do Orca: nenhum aviso de worker chega e os worker_done ficam na caixa. "
                "Suba de novo com: orq gerente subir")
    if idade is None:
        return f"painel do agent manager sem carimbo ({PAINEL_VIVO}): ele não subiu ou roda o script antigo; nenhum aviso de worker chega enquanto isso"
    return f"painel do agent manager parado há {int(idade // 60)} min: nenhum aviso de worker chega; reinicie painel-agent-manager.sh no terminal dele"


def checar_gerente_bg(agora=None):
    """No prompt do coordenador: carimbo `gerente-vivo` velho (ou ausente) e a última checagem com mais de PAINEL_PARADO_S, pede ao Orca em segundo plano
    se o terminal do gerente ainda existe (`orq gerente checar`). O hook não espera o Orca: o aviso sai no prompt seguinte, pelo aviso_painel."""
    g = _gerente_cfg()
    if not g or g.get("coordenador") != os.environ.get("ORCA_TERMINAL_HANDLE"):
        return
    agora = agora or time.time()
    try:
        if agora - os.path.getmtime(_path(PAINEL_VIVO)) <= PAINEL_PARADO_S:
            return
    except OSError:
        pass
    try:
        if agora - os.path.getmtime(_path(PAINEL_CHECAGEM)) <= PAINEL_PARADO_S:
            return
    except OSError:
        _write_json(_path(PAINEL_CHECAGEM), {"ts": agora, "terminal": g["gerente"], "morto": False})
    os.utime(_path(PAINEL_CHECAGEM), (agora, agora))  # marca a checagem já: prompts seguidos não disparam outra
    _orq_cli("gerente", "checar")


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
    prs = [e for e in todas if e.get("origem") == "pr"]
    if prs:
        partes.append("PR: " + "; ".join(f"{e['texto']} [{e['id']}]" for e in prs[:2]) + (f" +{len(prs) - 2}" if len(prs) > 2 else "") + ".")
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


def ultima_do_usuario(events, agora):
    """O carimbo da última mensagem do usuário, sem contar o pedido do próprio resumo ou digest (feito há menos de RESUMO_PEDIDO_S); "" sem nenhuma."""
    users = [e["ts"] for e in events if e.get("tipo") == "entrada" and e.get("origem", "usuario") == "usuario" and e.get("ts")]
    if users and 0 <= (agora - _dt(users[-1])).total_seconds() < RESUMO_PEDIDO_S:
        users.pop()  # o hook de prompt grava o pedido antes de o comando rodar: a janela começa na mensagem anterior (M17)
    return users[-1] if users else ""


def resumo_quatro(events, aberto, pendencias, ts, desde=None, agora=None, painel=None):
    """`orq resumo`: as quatro partes desde `desde` (padrão: a última mensagem do usuário, sem contar o pedido do próprio resumo) e as decisões, até ~20 linhas.

    Com você = pendências fora do Depois; Entrou = entradas da janela e o efeito de cada; Anda = workers rodando e a fase;
    Vem = tickets prontos (Blocked by todos resolvidos) e bloqueados. `ts` são os tickets já lidos.
    """
    agora = agora or datetime.now(timezone.utc)
    desde = desde or ultima_do_usuario(events, agora)
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
    import tempfile
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
        ab["maquina"] = maquina_painel(ab.get("agentes"))
        _write_json(_path("aberto.json"), ab)
        return ab


def refresh_bg(refresh=True):
    """ingest em segundo plano; com refresh, refaz também o aberto.json (150 chamadas do Orca): o aviso do Orca só precisa do inbox."""
    if os.environ.get("ORQ_NO_BG"):
        return
    subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "orq.py"), "ingest", *(["--refresh"] if refresh else [])], stdin=subprocess.DEVNULL,
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


def registra_turno(kind, ev, harness="claude"):
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
            turnos[d] = {"task": task or antigo.get("task"), "sessao": sid, "inicio": agora, "fim": None, "harness": harness,
                         **({"cwd": antigo.get("cwd") or ev.get("cwd")} if antigo.get("cwd") or ev.get("cwd") else {}),
                         **({"transcrito": ev["transcript_path"]} if ev.get("transcript_path") else {})}  # o cwd é o do lançamento: o `claude --resume` só acha a sessão nele (ticket 48)
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
        r = subprocess.run([GH, "pr", "view", url, "--json", "commits", "-q", ".commits[].oid"], capture_output=True, text=True, timeout=20)
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
                      "caminho": p["reportPath"], "ref": m["id"], "run": m["run_id"], "task": p.get("taskId"), **_grupo_do_run(m["run_id"])}, novo_id=True)
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


def _registra_worker_done(m):
    """Um evento `worker_done` por mensagem: quem entregou, com que resultado e o assunto. É o que o digest lê, sem chamar o Orca."""
    p = _payload(m)
    append_event({"tipo": "worker_done", "msg": m["id"], "run": m.get("run_id"), "task": p.get("taskId"), "dispatch": p.get("dispatchId"),
                  "outcome": p.get("outcome"), "subject": m.get("subject") or ""})


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
    feitos = {e.get("msg") for e in eventos if e.get("tipo") == "worker_done"}  # o digest lê daqui o que cada worker entregou
    msgs = sorted((m for m in orca("inbox", "--limit", "200", timeout=20)["messages"] if isinstance(m.get("sequence"), int)),
                  key=lambda m: m["sequence"])
    if ultimo and msgs and msgs[0]["sequence"] > ultimo + 1:
        log(f"ingest inbox: janela perdida, mensagens {ultimo + 1} a {msgs[0]['sequence'] - 1} saíram das últimas 200")
    for m in msgs:
        if m["sequence"] <= ultimo:
            continue
        try:
            if m.get("type") == "worker_done" and _dt(m["created_at"]) > desde and m["id"] not in feitos:
                _registra_worker_done(m)
                feitos.add(m["id"])
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
        for k, v in (("link", link), ("comando", comando), ("espera", espera), ("ate", ate), ("gate", gate), ("gate_run", gate_run), ("task", task)):
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

    item = _mutar_pend(rm, lambda it: {"tipo": "pend", "op": "done", "pend": id_, **({"resposta": resposta} if resposta else {}), **({"task": it["task"]} if it.get("task") else {}),
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


# ---------- PR ligado à tarefa ----------

_ESTADO_PR = {"aberto": "aberto", "mergeado": "✓", "fechado": "fechado"}


def _pr_estado(url):
    """{state, mergedAt, baseRefName} do PR pelo gh, ou None se o gh não existe, falha, demora ou não há rede."""
    try:
        r = subprocess.run([GH, "pr", "view", url, "--json", "state,mergedAt,baseRefName,title"], capture_output=True, text=True, timeout=PR_GH_S)
        d = json.loads(r.stdout) if r.returncode == 0 else None
    except (subprocess.TimeoutExpired, OSError, ValueError):
        return None
    return d if isinstance(d, dict) else None


def _pr_lista_gh(urls):
    """{url: dados do gh} dos PRs de `urls`, com uma chamada `gh pr list` por repositório (nunca uma por PR). PR que a lista não traz (mais velho que o
    limite) cai no `gh pr view`. gh fora do ar: o repositório inteiro fica sem leitura, sem chamar o gh de novo por PR."""
    por_repo = {}
    for u in urls:
        por_repo.setdefault(u.rsplit("/pull/", 1)[0].removeprefix("https://github.com/"), []).append(u)
    vistos = {}
    for repo, us in por_repo.items():
        try:
            r = subprocess.run([GH, "pr", "list", "--repo", repo, "--state", "all", "--limit", "300", "--json",
                                "url,state,mergedAt,baseRefName,headRefName,title,mergeable,statusCheckRollup"], capture_output=True, text=True, timeout=PR_GH_S)
            lista = json.loads(r.stdout) if r.returncode == 0 else None
        except (subprocess.TimeoutExpired, OSError, ValueError):
            lista = None
        if not isinstance(lista, list):
            continue
        achados = {x.get("url"): x for x in lista if isinstance(x, dict)}
        for u in us:
            vistos[u] = achados.get(u) or _pr_estado(u)
    return vistos


def _ci_do_gh(visto, agora):
    """{mergeable, falhas, rodando, lido_em} do que o gh viu de um PR, ou None se a resposta não traz CI nem mergeable (gh sem resposta, `pr view`)."""
    if "mergeable" not in visto and "statusCheckRollup" not in visto:
        return None
    falhas, rodando = [], []
    for c in visto.get("statusCheckRollup") or []:
        if not isinstance(c, dict):
            continue
        nome = c.get("name") or c.get("context") or "?"
        if c.get("__typename") == "StatusContext" or "context" in c:  # status antigo: só `state`
            est = c.get("state")
            falhas += [nome] if est in CHECK_FALHO else []
            rodando += [nome] if est in ("PENDING", "EXPECTED") else []
        elif c.get("status") != "COMPLETED":
            rodando.append(nome)
        elif c.get("conclusion") in CHECK_FALHO:
            falhas.append(nome)
    return {"mergeable": visto.get("mergeable") or "UNKNOWN", "falhas": falhas, "rodando": rodando, "lido_em": agora}


def _estado_do_gh(s):
    """`mergeado`, `fechado` ou None (aberto, ou sem resposta) para o que o gh devolveu."""
    return "mergeado" if s.get("state") == "MERGED" or s.get("mergedAt") else "fechado" if s.get("state") == "CLOSED" else None


def _prs_ro():
    d = _dict(_read_json(_path(PRS)))
    return {**d, "itens": [i for i in d.get("itens") or [] if isinstance(i, dict) and i.get("task") and i.get("url")]}


def _mutar_prs(fn):
    """Lê o prs.json, aplica fn(dados) e grava com tmp + rename, sob o pr.lock. Só comandos e o painel escrevem: os hooks nunca."""
    with _trava("pr.lock"):
        d = _prs_ro()
        out = fn(d)
        _write_json(_path(PRS), d, indent=2)
    return out


def pr_proximo(itens):
    """O próximo ambiente da feature, só como sugestão (`pronto para staging`, ou `em main` no fim), ou None.

    Vem do PR mergeado mais adiante em development, staging e main. Com um PR da feature ainda aberto o próximo já está a caminho: None.
    Com development e staging mergeados e nenhum PR aberto ou em main, o aviso diz que falta só o de main. Nunca abre o PR.
    O PR `merge/<feature>-<ambiente>` tem o ambiente como base e conta como entrada nele."""
    if any(i["estado"] == "aberto" for i in itens):
        return None
    indices = [AMBIENTES.index(i["base"]) for i in itens if i["estado"] == "mergeado" and i.get("base") in AMBIENTES]
    if not indices:
        return None
    if max(indices) == len(AMBIENTES) - 1:
        return "em main"
    if max(indices) == len(AMBIENTES) - 2 and AMBIENTES[0] in {i["base"] for i in itens if i["estado"] == "mergeado"}:
        return f"pronto para {AMBIENTES[-1]} ({' e '.join(AMBIENTES[:-1])} entraram: abrir o de {AMBIENTES[-1]})"
    return f"pronto para {AMBIENTES[max(indices) + 1]}"


def _seg_pr(i):
    return f"#{i.get('numero')} {i.get('base') or '?'} {_ESTADO_PR.get(i.get('estado'), i.get('estado'))}"


def pr_ligar(task, url, issue=None, tag=None, nota=None):
    """Liga um PR à task (a mesma feature tem um por ambiente). Consulta o gh uma vez, sem obrigar: sem resposta o PR entra `aberto` e sem base,
    e o poll completa. PR já mergeado ou fechado entra resolvido e avisado: quem o liga já sabe. Não confere a task no Orca (sem rede aqui)."""
    if not re.fullmatch(r"task_\w+", task or ""):
        raise ValueError(f"task inválida: {task!r} (use o id, task_…)")
    if not PR_RE.fullmatch(url or ""):
        raise ValueError(f"URL de PR inválida: {url!r} (https://github.com/<org>/<repo>/pull/<n>)")
    visto = _pr_estado(url) or {}

    def add(d):
        ja = next((i for i in d["itens"] if i["url"] == url), None)
        if ja:
            raise ValueError(f"PR #{ja.get('numero')} já está ligado à task {ja['task']}")
        estado = _estado_do_gh(visto) or "aberto"
        item = {"task": task, "url": url, "numero": int(url.rsplit("/", 1)[1]), "base": visto.get("baseRefName"), "estado": estado,
                "ligado_em": now(), "avisado": estado != "aberto", **({"resolvido_em": now()} if estado != "aberto" else {}),
                **({"issue": int(issue)} if issue else {}), **({"titulo": visto["title"]} if visto.get("title") else {}),
                **({"tag": tag} if tag else {}), **({"nota": nota} if nota else {})}
        d["itens"].append(item)
        d["sem_task"] = [x for x in d.get("sem_task") or [] if x.get("url") != url]
        append_event({"tipo": "pr", "op": "ligar", "task": task, "url": url, "numero": item["numero"], "base": item["base"], "estado": estado,
                      **({"issue": item["issue"]} if issue else {})})
        return item

    return _mutar_prs(add)


def pr_orfao(url, head=None):
    """Guarda o PR cuja branch não tem task conhecida na lista `sem_task` (o `orq status` mostra até alguém ligar). PR já conhecido: None."""
    if not PR_RE.fullmatch(url or ""):
        raise ValueError(f"URL de PR inválida: {url!r} (https://github.com/<org>/<repo>/pull/<n>)")

    def add(d):
        if any(i["url"] == url for i in d["itens"]) or any(x.get("url") == url for x in d.get("sem_task") or []):
            return None
        item = {"url": url, "numero": int(url.rsplit("/", 1)[1]), "head": head, "em": now()}
        d["sem_task"] = [*(d.get("sem_task") or []), item]
        append_event({"tipo": "pr", "op": "sem_task", "url": url, "numero": item["numero"], "head": head})
        return item

    return _mutar_prs(add)


PR_DESPACHOS = 12  # quantos despachos recentes o pr_auto confere pela worktree (um worker-show cada)


def _caminho_do_worker(res):
    """O caminho da worktree de um `worker-show`: o do terminal ou, sem ele, o do `worktreeId` (`<repo>::<caminho>`)."""
    wid = _fundo(res, "worker", "worktreeId")
    return _fundo(res, "terminal", "worktreePath") or (wid.split("::", 1)[1] if isinstance(wid, str) and "::" in wid else None)


def task_do_ramo(head, wt, events):
    """A task do despacho dono da branch `head`, ou None. Primeiro pelo `--name` da worktree nova (sem rede); depois pela worktree onde a
    branch está (`wt`), que o Orca diz em worker-show, nos PR_DESPACHOS despachos mais recentes. Duas tasks na mesma worktree (`current`): a mais nova."""
    desp = [e for e in reversed(events) if e.get("tipo") == "despacho" and e.get("task")]
    for e in desp:
        nome = e.get("nome")
        if nome and head and (head == nome or head.endswith("/" + nome)):
            return e["task"]
    alvo = os.path.realpath(wt) if wt else None
    for e in desp[:PR_DESPACHOS] if alvo else []:
        try:
            caminho = _caminho_do_worker(orca("worker-show", "--dispatch", e["dispatch"], timeout=10))
        except Exception:  # noqa: BLE001 - dispatch que o Orca não conhece mais não impede os outros
            continue
        if caminho and os.path.realpath(caminho) == alvo:
            return e["task"]
    return None


def _head_do_pr(url):
    """A branch de origem do PR pelo `gh pr view`, ou None (gh fora do ar, PR que não existe)."""
    try:
        r = subprocess.run([GH, "pr", "view", url, "--json", "headRefName"], capture_output=True, text=True, timeout=PR_GH_S)
        return (json.loads(r.stdout).get("headRefName") or None) if r.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired, ValueError, AttributeError):
        return None


def pr_auto(url, head=None, wt=None, cwd=None):
    """Liga o PR à task dona da branch (task_do_ramo) ou, sem dona, o põe em `sem_task`. Devolve o item; PR já conhecido: None.

    Sem `head` (o hook não soube a branch deste PR, como no laço de `gh pr create` com variável) ela vem do `gh pr view`, e a worktree dela do `cwd`."""
    d = _prs_ro()
    if any(i["url"] == url for i in d["itens"]) or any(x.get("url") == url for x in d.get("sem_task") or []):
        return None
    if not head:
        head = _head_do_pr(url)
        wt = wt or (_worktrees_por_ramo(cwd).get(head) if head and cwd else None)
    task = task_do_ramo(head, wt, read_events())
    return pr_ligar(task, url) if task else pr_orfao(url, head)


def pr_desligar(task, url):
    def rm(d):
        for n, i in enumerate(d["itens"]):
            if i["task"] == task and i["url"] == url:
                append_event({"tipo": "pr", "op": "desligar", "task": task, "url": url, "numero": i.get("numero")})
                return d["itens"].pop(n)
        raise ValueError(f"o PR {url} não está ligado à task {task}")

    return _mutar_prs(rm)


def pr_lista(task=None):
    """Linhas de `orq pr lista`: task, PR com base e estado, issue e URL."""
    return [f"{i['task']}  {_seg_pr(i)}" + (f"  (issue #{i['issue']})" if i.get("issue") else "") + f"  {i['url']}"
            for i in _prs_ro()["itens"] if not task or i["task"] == task]


def _pr_antigo(itens, agora):
    """Todos os PRs da feature resolvidos há mais de PR_VISIVEL_D dias: ela sai do `orq status` e do digest."""
    return all(i["estado"] != "aberto" and i.get("resolvido_em") and (agora - _dt(i["resolvido_em"])).days > PR_VISIVEL_D for i in itens)


def linhas_pr(agora=None):
    """Uma linha por feature para o `orq status`: os PRs com o ambiente de cada um e a sugestão do próximo. Feature com tudo resolvido há
    mais de PR_VISIVEL_D dias sai. Só lê o prs.json."""
    agora = agora or datetime.now(timezone.utc)
    por_task = {}
    for i in _prs_ro()["itens"]:
        por_task.setdefault(i["task"], []).append(i)
    linhas = []
    for task, itens in por_task.items():
        if _pr_antigo(itens, agora):
            continue
        prox = pr_proximo(itens)
        linhas.append(f"PR {task}: " + " · ".join(_seg_pr(i) for i in itens) + (f" → {prox}" if prox else ""))
    linhas += [f"PR sem tarefa: #{x.get('numero')} ({x.get('head') or '?'}) {x['url']}: `orq pr ligar <task> {x['url']}`"
               for x in _prs_ro().get("sem_task") or [] if isinstance(x, dict) and x.get("url")]
    return linhas


def worktrees_paradas(wts, ocupadas, agora, fora):
    """Pura: as worktrees (lista do `orca worktree list`) sem worker vivo (caminho fora de `ocupadas`) e sem atividade há mais de WT_PARADA_D dias,
    a mais velha primeiro, com `fora(caminho)` = commits fora do origin/main. A principal e as arquivadas não entram."""
    out = []
    for w in wts:
        ult = w.get("lastActivityAt")
        if w.get("isMainWorktree") or w.get("isArchived") or w["path"] in ocupadas or not ult:
            continue
        dias = (agora - datetime.fromtimestamp(ult / 1000, timezone.utc)).days
        if dias > WT_PARADA_D:
            out.append({"caminho": w["path"], "branch": (w.get("branch") or "").removeprefix("refs/heads/") or os.path.basename(w["path"]),
                        "dias": dias, "fora": fora(w["path"])})
    return sorted(out, key=lambda x: -x["dias"])


def worktrees_ocupadas():
    """Os caminhos das worktrees com worker ainda não liberado (dispatched, ou terminal não released). Na dúvida o worker conta como vivo."""
    return {p for w in _workers_todos() if w.get("dispatchStatus") == "dispatched" or w.get("terminalState") != "released"
            for p in [((w.get("resource") or {}).get("worktreeId") or "").split("::", 1)[-1]] if p.startswith("/")}


def _commits_fora(caminho):
    n = _git(caminho, "rev-list", "--count", "origin/main..HEAD")
    return int(n) if n and n.strip().isdigit() else 0


def linhas_worktrees(agora=None, wts=None, ocupadas=None, fora=None):
    """A linha do `orq status` com as worktrees paradas, uma vez por dia (worktrees-aviso.json guarda o dia). Orca fora do ar: sem linha."""
    agora = agora or datetime.now(timezone.utc)
    dia, arq = agora.astimezone().strftime("%Y-%m-%d"), _path("worktrees-aviso.json")
    if _dict(_read_json(arq)).get("dia") == dia:
        return []
    try:
        ps = worktrees_paradas(wts if wts is not None else orca("list", "--limit", "1000", area="worktree", timeout=15)["worktrees"],
                               worktrees_ocupadas() if ocupadas is None else ocupadas, agora, fora or _commits_fora)
    except Exception as e:  # noqa: BLE001
        log(f"status: worktrees paradas: {type(e).__name__}: {e}")
        return []
    if not ps:
        return []
    _write_json(arq, {"dia": dia})
    item = lambda x: f"{x['branch']} ({x['dias']} dias, {x['fora']} commit{'s' if x['fora'] != 1 else ''} fora da main)"
    return [f"Worktrees paradas ({len(ps)}): " + "; ".join(_lim(ps, 3, item))]


E2E_SESSAO_MIN = 15  # minutos que uma sessão de `e2e-infra.sh start` pode ficar sem processo de teste vivo antes de a fila contar como presa


def _pid_vivo(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # existe, é de outro usuário
    except (ValueError, OverflowError):
        return False
    return True


def fila_e2e(fila=None, agora=None, limite_min=E2E_SESSAO_MIN):
    """A fila global do E2E do repositório de produto (scripts/e2e-lock.sh: um ticket `<ordem>-<pid>` por chegada em ~/.cache/<projeto>-e2e/queue), só lida.

    Devolve None com a fila vazia, senão {ticket, worktree, projeto, comando, min, esperam, presa}. O dono é o primeiro ticket; `presa` é o
    motivo quando ele não anda: nenhum pid do ticket vivo e sem sessão (dono morto, o próximo a esperar o limparia), ou sessão aberta por
    `start` sem processo de teste vivo há mais de `limite_min` minutos. Limite: não olha o Docker, então uma stack aberta de propósito
    por mais de `limite_min` sem teste também aparece como presa."""
    import glob  # só aqui: fora do topo para não pesar nos hooks
    fila = fila or os.environ.get("E2E_LOCK_DIR") or next(iter(sorted(glob.glob(os.path.join(os.environ.get("XDG_CACHE_HOME") or os.path.expanduser("~/.cache"), "*-e2e", "queue")))), "")
    agora = time.time() if agora is None else agora
    tickets = []
    for nome in sorted(os.listdir(fila)) if os.path.isdir(fila) else []:
        d = os.path.join(fila, nome)
        try:
            owner = dict(l.rstrip("\n").split("=", 1) for l in open(os.path.join(d, "owner")) if "=" in l)
            pids = os.listdir(os.path.join(d, "pids")) if os.path.isdir(os.path.join(d, "pids")) else []
            ini = int(open(os.path.join(d, "acquired")).read().strip()) if os.path.exists(os.path.join(d, "acquired")) else int(owner.get("started") or agora)
        except (OSError, ValueError):
            continue  # ticket ainda sendo escrito ou ilegível
        tickets.append({"nome": nome, "owner": owner, "vivo": any(_pid_vivo(x) for x in pids), "sessao": os.path.exists(os.path.join(d, "session")), "ini": ini})
    if not tickets:
        return None
    dono, resto = tickets[0], tickets[1:]
    minutos = max(0, int((agora - dono["ini"]) // 60))
    presa = None
    if not dono["vivo"] and not dono["sessao"]:
        presa = f"o dono (pid {dono['owner'].get('pid')}) morreu e o ticket ficou"
    elif not dono["vivo"] and minutos > limite_min:
        presa = f"sessão aberta por start sem processo de teste vivo há {minutos} min"
    o = dono["owner"]
    return {"ticket": dono["nome"], "worktree": os.path.basename(o.get("worktree", "")), "projeto": o.get("project"), "comando": o.get("command"),
            "min": minutos, "esperam": sum(1 for t in resto if t["vivo"] or t["sessao"]), "presa": presa}


def linha_e2e(f):
    """A linha do `orq status` para a fila do E2E (`fila_e2e`); vazia com a fila vazia."""
    if not f:
        return ""
    txt = f"Fila do E2E: {f['worktree']} ({f['projeto']}) segura há {f['min']} min, {f['esperam']} esperando"
    return txt + (f". PRESA: {f['presa']}; `scripts/e2e-infra.sh lock-release` no repositório do produto solta" if f["presa"] else "")


def avisa_fila_e2e(f=None):
    """Digita no coordenador uma vez por ticket que a fila do E2E está presa (e2e-aviso.json guarda o ticket). Coordenador ocupado: a próxima volta tenta."""
    g, f = _gerente_cfg(), f or fila_e2e()
    if not g or not g.get("coordenador") or not f or not f["presa"]:
        return []
    arq = _path("e2e-aviso.json")
    if _dict(_read_json(arq)).get("ticket") == f["ticket"]:
        return []
    if avisa_coordenador(g["coordenador"], f"orq: {linha_e2e(f)}.") not in ("enviado", "adiado"):
        return []
    _write_json(arq, {"ticket": f["ticket"]})
    return [f"fila do E2E presa ({f['ticket']}): aviso digitado no coordenador"]


def _aplica_prs(d, vistos, agora):
    """Passa ao estado novo cada PR aberto que o gh viu mergeado ou fechado: um evento `pr` e uma entrada `pr` por PR, uma vez só (o `ref` da
    entrada é a URL, então repetir a passagem não a duplica). Devolve as linhas do que mudou."""
    d["ultimo_poll"] = agora
    ja = {e.get("ref") for e in read_events() if e.get("origem") == "pr"}
    linhas = []
    for i in d["itens"]:
        visto = _dict(vistos.get(i["url"]))
        novo = _estado_do_gh(visto)
        if i["estado"] == "aberto" and not novo:
            ci = _ci_do_gh(visto, agora)
            i.update(**({"ci": ci} if ci else {}), **({"head": visto["headRefName"]} if visto.get("headRefName") else {}))
        if i["estado"] != "aberto" or not novo:
            i["base"] = visto.get("baseRefName") or i.get("base") if i["estado"] == "aberto" else i.get("base")
            continue
        i.update(estado=novo, base=visto.get("baseRefName") or i.get("base"), resolvido_em=now())
        prox = pr_proximo([x for x in d["itens"] if x["task"] == i["task"]])
        onde = f"{i['task']}" + (f", issue #{i['issue']}" if i.get("issue") else "")
        texto = f"PR #{i['numero']} entrou em {i['base']} ({onde})" + (f": {prox}" if prox else "") if novo == "mergeado" \
            else f"PR #{i['numero']} fechado sem merge (base {i['base']}, {onde})"
        append_event({"tipo": "pr", "op": "entrou" if novo == "mergeado" else "fechou", "task": i["task"], "url": i["url"], "numero": i["numero"],
                      "base": i["base"], **({"proximo": prox} if prox else {})})
        if i["url"] in ja:
            i["avisado"] = True
            continue
        ent = append_event({"tipo": "entrada", "origem": "pr", "texto": texto, "fonte": f"PR #{i['numero']}", "ref": i["url"], "task": i["task"]}, novo_id=True)
        i.update(entrada=ent["id"], texto=texto, avisado=False)
        linhas.append(f"{i['task']}: {texto}")
    return linhas


def pr_poll(agora=None, forcar=False):
    """Pergunta ao gh pelos PRs abertos e grava os que entraram ou foram fechados (_aplica_prs). Devolve as linhas do que mudou.

    Roda fora dos hooks (o painel do gerente e `orq pr poll`), no máximo a cada PR_POLL_S (`forcar` ignora), sem PR aberto nem chama o gh, e
    um poll por vez (lock não bloqueante). Uma chamada `gh pr list` por repositório traz o estado, o `mergeable` e os checks de todos os PRs ligados
    (guardados em `ci`). gh sem resposta deixa o PR aberto para a próxima rodada. Limite: a volta do painel espera o gh, PR_GH_S por repositório."""
    agora = time.time() if agora is None else agora
    os.makedirs(HOME, exist_ok=True)
    with open(_path("pr-poll.lock"), "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return []
        d = _prs_ro()
        urls = [i["url"] for i in d["itens"] if i["estado"] == "aberto"]
        if not urls or (not forcar and agora - (d.get("ultimo_poll") or 0) < PR_POLL_S):
            return []
        vistos = _pr_lista_gh(urls)
        return _mutar_prs(lambda d: _aplica_prs(d, vistos, agora))


def pr_avisar():
    """Digita no coordenador uma linha `orq: PR #N entrou em <base> …` por PR resolvido ainda não avisado, uma vez (o painel do gerente chama a
    cada volta). Coordenador ocupado ou com rascunho: nada é digitado e a próxima volta tenta. Sem gerente ligado não há quem digite: a
    entrada já está no log e aparece no prompt seguinte. Devolve as linhas do painel."""
    g = _gerente_cfg()
    if not g or not g.get("coordenador"):
        return []
    linhas = []
    for i in [x for x in _prs_ro()["itens"] if x["estado"] != "aberto" and not x.get("avisado") and x.get("entrada")]:
        def reserva(d, url=i["url"], valor=True):
            """Marca/desmarca o aviso sob o pr.lock; devolve False se já estava marcado (outro painel ou um restart chegou antes)."""
            for x in d["itens"]:
                if x["url"] == url:
                    if valor and (x.get("avisado") or any(e.get("op") == "avisado" and e.get("url") == url for e in read_events())):
                        x["avisado"] = True
                        return False
                    x["avisado"] = valor
            return True

        # reserva antes de digitar: dois painéis (ou um restart no meio da volta) não digitam o mesmo aviso duas vezes
        if not _mutar_prs(reserva):
            continue
        if avisa_coordenador(g["coordenador"], f"orq: {i['texto']}. Entrada {i['entrada']}.", contexto=False) not in ("enviado", "adiado"):  # o "PR: …" do resumo já mostra
            _mutar_prs(lambda d, f=reserva: f(d, valor=False))  # nada foi digitado: a próxima volta tenta
            break
        append_event({"tipo": "pr", "op": "avisado", "task": i["task"], "url": i["url"], "numero": i["numero"]})
        linhas.append(f"{i['task']}: aviso do PR #{i['numero']} digitado no coordenador")
    return linhas


def _pergunta_aberta(events):
    """{dispatch: evento pergunta_tela} dos menus da tela que o gerente já avisou e ainda não sumiram (o `pergunta_tela_fim` depois dele fecha)."""
    abertas = {}
    for e in events:
        if e.get("tipo") == "pergunta_tela" and e.get("dispatch"):
            abertas[e["dispatch"]] = e
        elif e.get("tipo") == "pergunta_tela_fim":
            abertas.pop(e.get("dispatch"), None)
    return abertas


def telas_avisar():
    """Uma volta do gerente sobre as telas dos workers: prompt de permissão, AskUserQuestion ou "trust this folder" preso no terminal de um worker
    vira uma linha digitada no coordenador, uma vez por menu (evento `pergunta_tela`, com a pergunta e as opções), com o `orq responder-tela` a rodar.
    Coordenador ocupado ou com rascunho: nada é digitado e a próxima volta tenta. O menu que sumiu da tela fecha o evento (`pergunta_tela_fim`),
    então o mesmo texto volta a ser avisado se reaparecer. Só lê as telas de quem tem turno nos hooks do worker (Claude Code). Devolve as linhas do painel."""
    g = _gerente_cfg()
    if not g or not g.get("coordenador"):
        return []
    turnos, ws, hib = _turnos_ro(), [], _hibernados()
    for r in g["runs"]:
        ws += [w for w in _workers_todos(r) if w.get("dispatchId") in turnos and w.get("dispatchId") not in hib]
    lidas = _ler_telas(ws, {w["dispatchId"]: {"agente": _dict(turnos[w["dispatchId"]]).get("harness") or "claude"} for w in ws})
    abertas, linhas = _pergunta_aberta(read_events()), []
    for w in ws:
        d, p = w["dispatchId"], (lidas.get(w["dispatchId"]) or {}).get("pergunta")
        if not p and d in abertas:
            append_event({"tipo": "pergunta_tela_fim", "dispatch": d})
        if not p or (d in abertas and abertas[d].get("texto") == p["texto"] and abertas[d].get("opcoes") == p["opcoes"]):
            continue
        ops = " ".join(f"{n}) {r}" for n, r in p["opcoes"])
        aviso = f"orq: worker {w.get('taskId')} pergunta na tela ({p['tipo']}): {p['texto']} Opções: {ops}. Responda com: orq responder-tela {w.get('taskId')} <opção>."
        if avisa_coordenador(g["coordenador"], re.sub(r"\s+", " ", aviso)) not in ("enviado", "adiado"):
            break
        append_event({"tipo": "pergunta_tela", "dispatch": d, "task": w.get("taskId"), "run": w.get("runId"), "terminal": w.get("agentTerminalHandle"), "menu": p["tipo"], "texto": p["texto"], "opcoes": p["opcoes"]})
        linhas.append(f"{w.get('taskId')}: pergunta na tela ({p['tipo']}) avisada ao coordenador")
    return linhas


def estado(entrada=None):
    events, cur = read_events(), _cursor_ro()
    txt = resumo(events, _read_json(_path("aberto.json")), _read_json(PEND), entrada, cursor=cur, turnos=_turnos_ro(), painel=aviso_painel())
    return "\n".join([txt, *linhas_noite(cur, events)])


# ---------- digest e modo ausente ----------

DIGEST = "digest"  # ORQ_HOME/digest/<AAAA-MM-DD>.html: um arquivo por dia, sempre no mesmo lugar
DIGEST_LINHAS = 100  # entradas de `linha`; as mais antigas ficam só no log (a página diz "+N antes")
FILA = "fila.json"  # {passos: [{passo, nome, por, prs: [números], feito}]}: a ordem de merge que o coordenador declara com `orq fila`
ESTADO_GH = {"aberto": "OPEN", "mergeado": "MERGED", "fechado": "CLOSED"}  # o estado do PR no contrato do digest
TRANSCRITO_FIM = 400_000  # bytes do fim do transcrito onde o Stop procura a última resposta do coordenador
RESPOSTA_MAX = 4000  # caracteres da resposta do coordenador que vão para o log
DIGEST_BASE = {"development": "d", "staging": "s", "main": "p"}  # a cor do ponto de cada ambiente
DIGEST_ESTADO = {"MERGED": ("m", "merged"), "CLOSED": ("x", "fechado"), "OPEN": ("o", "aberto")}
DIGEST_CSS = """:root{--bg:#f7f7f5;--card:#fff;--ink:#1d1d1f;--mute:#6b6b70;--line:#e4e4e0;--dev:#2f6fdb;--stg:#b7791f;--ok:#2e8b57;--warn:#c2410c;--sec:#b91c1c;--chip:#f0efeb}
@media (prefers-color-scheme:dark){:root{--bg:#141416;--card:#1d1d20;--ink:#ededed;--mute:#9a9aa2;--line:#2c2c31;--chip:#26262b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif}
main{max-width:1000px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:24px;margin:0 0 4px}h2{font-size:17px;margin:28px 0 10px}.sub{color:var(--mute);margin:0 0 8px}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(min(100%,300px),1fr))}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;min-width:0}
.card h3{margin:0 0 6px;font-size:15px}.card p{margin:6px 0 0;color:var(--mute);font-size:13.5px}.card.dec{border-left:4px solid var(--warn)}
.prs{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.pr{display:inline-flex;align-items:center;gap:6px;text-decoration:none;color:var(--ink);background:var(--chip);border-radius:999px;padding:3px 10px;font-size:13px;border:1px solid var(--line)}
.pr em{font-style:normal;font-size:11px;color:var(--mute)}.pr.m{opacity:.6}.pr.m em{color:var(--ok)}
.dot{width:8px;height:8px;border-radius:50%}.d{background:var(--dev)}.s{background:var(--stg)}.p{background:var(--ok)}
.legend{font-size:12.5px;color:var(--mute);display:flex;gap:12px;align-items:center;flex-wrap:wrap}.legend .dot{display:inline-block}
.fila{list-style:none;padding:0;margin:0;display:grid;gap:10px}.fila li{display:flex;gap:12px;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px;min-width:0}
.fila li>div{min-width:0}.fila .num{flex:0 0 28px;height:28px;border-radius:50%;background:var(--ink);color:var(--bg);display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px}
.fila li.feito{opacity:.5}.fila li.feito .num{background:var(--ok)}.fila p{margin:2px 0 0;color:var(--mute);font-size:13.5px}.fila .aviso{color:var(--warn)}
ol.tl{list-style:none;padding:0;margin:0;border-left:2px solid var(--line)}
ol.tl li{position:relative;padding:0 0 14px 18px}ol.tl li::before{content:"";position:absolute;left:-7px;top:6px;width:12px;height:12px;border-radius:50%;background:var(--card);border:2px solid var(--mute)}
ol.tl li.sec::before{border-color:var(--sec)}ol.tl li.fo::before{border-color:var(--dev)}ol.tl li.ok::before{border-color:var(--ok)}ol.tl li.warn::before{border-color:var(--warn)}
ol.tl b{font-weight:600}ol.tl p{margin:2px 0 0;color:var(--mute);font-size:13.5px;overflow-wrap:anywhere}
.run{display:flex;gap:8px;flex-wrap:wrap}.run span{background:var(--chip);border-radius:8px;padding:6px 10px;font-size:13px}
code{font-size:12.5px;background:var(--chip);padding:1px 5px;border-radius:5px;overflow-wrap:anywhere}"""


def _passo_feito(itens):
    """A feature acabou: tem PR, nenhum está aberto e não falta promover (entrou em main, ou só restam PRs fechados)."""
    return bool(itens) and all(i["estado"] != "aberto" for i in itens) and pr_proximo(itens) in (None, "em main")


def ordem_de_merge(grupos, ts):
    """Os grupos de PR (`{task, ligado_em, itens}`, um por feature) na ordem em que devem entrar, pelos `Blocked by` dos tickets.

    A feature do ticket N espera as features dos tickets de que N depende, direta ou por outro ticket (o ticket do meio pode nem ter PR).
    Sem dependência declarada, ou sem ticket, vale a ordem de ligação. Cada grupo volta com `espera` (as tasks de que depende) e, em ciclo
    ou atrás de um, `ciclo`: o resto sai na ordem de ligação em vez de travar a página."""
    por_num = {t["num"]: t for t in ts}
    num_da_task = {t["task"]: t["num"] for t in ts if t.get("task")}

    def ancestrais(num):
        vistos, pilha = set(), list(_dict(por_num.get(num)).get("blocked_by") or [])
        while pilha:
            n = pilha.pop()
            if n not in vistos:
                vistos.add(n)
                pilha += _dict(por_num.get(n)).get("blocked_by") or []
        return vistos

    deps = {}
    for g in grupos:
        acima = ancestrais(num_da_task[g["task"]]) if g["task"] in num_da_task else set()
        deps[g["task"]] = {h["task"] for h in grupos if h["task"] != g["task"] and num_da_task.get(h["task"]) in acima}
    falta = sorted(grupos, key=lambda g: (g.get("ligado_em") or "", g["task"]))
    ordem, postas = [], set()
    while falta:
        g = next((g for g in falta if deps[g["task"]] <= postas), None)
        if g is None:
            ordem += [{**x, "espera": sorted(deps[x["task"]]), "ciclo": True} for x in falta]
            break
        falta.remove(g)
        ordem.append({**g, "espera": sorted(deps[g["task"]]), "ciclo": False})
        postas.add(g["task"])
    return ordem


def _leitura_pr(i, agora=None):
    """O CI e o conflito que o poll guardou de um PR aberto: {marcas: [(ícone, texto)], pronto, velha, idade}. Sem leitura ou com leitura velha
    (mais de LEITURA_VELHA_MIN minutos) o PR não conta como pronto. PR já resolvido: None."""
    if i.get("estado") != "aberto":
        return None
    ci = _dict(i.get("ci"))
    if not ci:
        return {"marcas": [("?", "sem leitura do CI")], "pronto": False, "velha": False, "idade": None}
    agora = time.time() if agora is None else agora
    idade = (agora - (ci.get("lido_em") or 0)) / 60
    marcas = ([("✗", ", ".join(ci["falhas"]))] if ci.get("falhas") else []) + ([("⚠", "conflito")] if ci.get("mergeable") == "CONFLICTING" else []) \
        + ([("⏳", "CI rodando")] if ci.get("rodando") else []) + ([("?", "conflito ainda não calculado")] if ci.get("mergeable") == "UNKNOWN" and not ci.get("falhas") and not ci.get("rodando") else [])
    velha = idade > LEITURA_VELHA_MIN
    return {"marcas": marcas or [("✓", "pronto")], "pronto": not marcas and not velha, "velha": velha, "idade": idade}


def _pr_contrato(i, agora=None):
    """O PR como o contrato do digest (contratos/digest-v1.md) o pede: estado em caixa alta e o título, ou `PR #N` se o gh nunca o deu.
    PR aberto leva também o que o poll leu do CI: mergeable, falhas, rodando, lidoEm, velha e pronto."""
    out = {"numero": i.get("numero"), "url": i["url"], "base": i.get("base"), "estado": ESTADO_GH.get(i["estado"], "OPEN"),
           "titulo": i.get("titulo") or f"PR #{i.get('numero')}"}
    l = _leitura_pr(i, agora)
    if l:
        ci = _dict(i.get("ci"))
        out.update(mergeable=ci.get("mergeable"), falhas=ci.get("falhas") or [], rodando=ci.get("rodando") or [], lidoEm=ci.get("lido_em"), velha=l["velha"], pronto=l["pronto"])
    return out


def _merge_tree(a, b):
    """Os arquivos em conflito ao juntar as branches `a` e `b` (`git merge-tree`, sem tocar em nada), [] se juntam limpo, None se não deu para saber
    (branch fora do clone, git velho). Procura nos repositórios de ORQ_REPOS, pelo `origin/<branch>` e depois pela branch local."""
    for repo in [r for r in os.environ.get("ORQ_REPOS", os.path.expanduser("~/.claude/orq")).split(":") if r]:
        refs = [next((r for r in (f"origin/{h}", h) if _git(repo, "rev-parse", "--verify", "-q", r + "^{commit}")), None) for h in (a, b)]
        if not all(refs):
            continue
        try:
            r = subprocess.run(["git", "-C", repo, "merge-tree", "--write-tree", "--name-only", "--no-messages", *refs], capture_output=True, text=True, timeout=30)
        except (subprocess.TimeoutExpired, OSError):
            return None
        if r.returncode == 0:
            return []
        return [x for x in r.stdout.splitlines()[1:] if x.strip()] if r.returncode == 1 else None
    return None


def _marca_passos(passos, prs, agora=None):
    """Põe em cada passo (formato de `_passos_declarados`) `pronto` (a fazer e todos os PRs abertos prontos) e `avisos` (um PR conflita com um PR de passo
    anterior da fila: `git merge-tree` entre as branches). Devolve o número do próximo passo a mergear: o primeiro a fazer com tudo pronto, ou None."""
    heads = {i.get("numero"): i.get("head") for i in prs.get("itens") or []}
    for n, p in enumerate(passos):
        abertos = [i for i in p["prs"] if i["estado"] == "OPEN"]
        p["pronto"] = not p["feito"] and bool(abertos) and all(i.get("pronto") for i in abertos)
        p["avisos"] = []
        for b in abertos:
            for q in passos[:n]:
                for a in (x for x in q["prs"] if x["estado"] == "OPEN" and heads.get(x["numero"]) and heads.get(b["numero"])):
                    arqs = _merge_tree(heads[a["numero"]], heads[b["numero"]])
                    if arqs:
                        p["avisos"].append(f"#{b['numero']} conflita com #{a['numero']} (passo {q['passo']}): {', '.join(arqs[:5])}" + (f" e mais {len(arqs) - 5}" if len(arqs) > 5 else ""))
    return next((p["passo"] for p in passos if p["pronto"]), None)


def _fila_ro():
    d = _dict(_read_json(_path(FILA)))
    return {"passos": [p for p in d.get("passos") or [] if isinstance(p, dict) and isinstance(p.get("passo"), int)]}


def _mutar_fila(fn):
    """Lê o fila.json, aplica fn(dados) e grava com tmp + rename, sob o fila.lock. Só o coordenador escreve (`orq fila`)."""
    with _trava("fila.lock"):
        d = _fila_ro()
        out = fn(d)
        d["passos"].sort(key=lambda p: p["passo"])
        _write_json(_path(FILA), d, indent=2)
    return out


def fila_add(passo, nome, por, numeros):
    """Declara (ou troca) o passo `passo` da ordem de merge: nome, por quê e os PRs, que já precisam estar ligados (`orq pr ligar`)."""
    if passo < 1 or not numeros:
        raise ValueError("passo pede um número a partir de 1 e ao menos um PR")
    ligados = {i.get("numero") for i in _prs_ro()["itens"]}
    faltam = [n for n in numeros if n not in ligados]
    if faltam:
        raise ValueError(f"PR {', '.join('#' + str(n) for n in faltam)} não está ligado a nenhuma task (orq pr ligar <task> <url>)")

    def grava(d):
        d["passos"] = [p for p in d["passos"] if p["passo"] != passo] + [{"passo": passo, "nome": nome, "por": por, "prs": list(dict.fromkeys(numeros)), "feito": False}]
        append_event({"tipo": "fila", "op": "add", "passo": passo, "nome": nome, "prs": numeros})
        return d["passos"][-1]

    return _mutar_fila(grava)


def fila_marca(passo, op):
    """`feito` marca o passo como feito à mão (vale mesmo com PR aberto); `rm` tira o passo da fila."""
    def muda(d):
        achado = next((p for p in d["passos"] if p["passo"] == passo), None)
        if not achado:
            raise ValueError(f"o passo {passo} não existe na fila (orq fila lista)")
        if op == "rm":
            d["passos"].remove(achado)
        else:
            achado["feito"] = True
        append_event({"tipo": "fila", "op": op, "passo": passo})

    _mutar_fila(muda)


def _txt_pr(i, raw):
    """`#N estado` mais o que o poll leu do CI: ✓, ✗ com o nome dos checks, ⚠ conflito, ⏳ CI rodando, e `(leitura velha, N min)`."""
    l = _leitura_pr(raw.get(i["numero"]) or {})
    if not l:
        return f"#{i['numero']} {i['estado'].lower()}"
    return f"#{i['numero']} {i['estado'].lower()} " + " ".join(f"{ic} {tx}" if ic != "✓" else ic for ic, tx in l["marcas"]) + (f" (leitura velha, {l['idade']:.0f} min)" if l["velha"] else "")


def fila_lista():
    """Linhas de `orq fila lista`: o passo, o nome, se está feito e cada PR com o estado, o CI e o conflito que o poll guardou; os avisos de conflito
    entre passos; e o próximo passo a mergear (o primeiro a fazer com todos os PRs prontos). É por aqui que se responde "posso mergear?"."""
    prs = _prs_ro()
    passos, raw = _passos_declarados(_fila_ro(), prs), {i.get("numero"): i for i in prs["itens"]}
    prox = _marca_passos(passos, prs)
    linhas = []
    for p in passos:
        linhas.append(f"{p['passo']}  {p['nome']}  [{'feito' if p['feito'] else 'a fazer'}]  " + ", ".join(_txt_pr(i, raw) for i in p["prs"]) + (f"  — {p['por']}" if p["por"] else ""))
        linhas += [f"   ⚠ {a}" for a in p["avisos"]]
    if passos:
        alvo = next((p for p in passos if p["passo"] == prox), None)
        linhas.append(f"Próximo a mergear: passo {prox} ({alvo['nome']})" if alvo else "Próximo a mergear: nenhum passo pronto")
    return linhas


def _passos_declarados(fila, prs):
    """Os passos de `orq fila` no formato do contrato, com o estado de cada PR vindo do prs.json. `feito` = marcado à mão, ou PRs que sobraram
    todos MERGED ou CLOSED (PR desligado depois some do passo)."""
    por_num = {i.get("numero"): i for i in prs.get("itens") or []}
    out = []
    for p in fila["passos"]:
        itens = [_pr_contrato(por_num[n]) for n in p.get("prs") or [] if n in por_num]
        out.append({"passo": p["passo"], "nome": p.get("nome") or "", "por": p.get("por") or "", "prs": itens,
                    "feito": bool(p.get("feito")) or (bool(itens) and all(i["estado"] != "OPEN" for i in itens))})
    return out


def _estado_de_gente(a):
    """O estado de um worker vivo em texto de gente: a fase que ele declarou, ou o estado sem o jargão do orq."""
    if a["estado"] == "rodando":
        return a.get("fase") or "rodando"
    return {"travado": "travado", "parado": "parado no prompt", "perguntando": "esperando a sua resposta", "nao_comecou": "não começou",
            "hibernado": f"hibernado desde {_hora_local(a.get('hibernado_desde'))}"}.get(a["estado"], a["estado"])


def _linha_do_log(e, titulo):
    """Uma entrada de `linha` do contrato a partir de um evento do log, ou None se o evento não conta. tipo: ok, sec (falha), info."""
    tipo = e.get("tipo")
    if tipo == "worker_done":
        ok = e.get("outcome") == "succeeded"
        return {"tipo": "ok" if ok else "sec", "titulo": ("Worker entregou" if ok else "Worker falhou") + f": {_cita(e.get('subject'), 100)}",
                "detalhe": titulo.get(e.get("task")) or ""}
    if tipo == "pr" and e.get("op") in ("entrou", "fechou"):
        entrou = e["op"] == "entrou"
        return {"tipo": "ok" if entrou else "sec", "titulo": f"PR #{e.get('numero')} " + (f"entrou em {e.get('base')}" if entrou else "fechado sem merge"),
                "detalhe": titulo.get(e.get("task")) or ""}
    if tipo == "resposta_coordenador" and e.get("texto"):
        return {"tipo": "info", "titulo": _cita(e["texto"].strip().splitlines()[0], 90), "detalhe": e["texto"]}
    if tipo == "resposta_worker" and e.get("texto"):
        return {"tipo": "info", "titulo": "Coordenador respondeu a um worker", "detalhe": e["texto"]}
    if tipo in ("resposta", "resposta_lavish") and e.get("resposta"):
        return {"tipo": "ok", "titulo": f"Decisão: {e.get('header') or e.get('item') or ''}", "detalhe": e["resposta"]}
    if tipo == "pend" and e.get("op") == "done" and e.get("resposta"):
        return {"tipo": "ok", "titulo": f"Pendência {e.get('pend')} fechada", "detalhe": e["resposta"]}
    return None


def tickets_do_painel(ts, aberto):
    """`tickets_orq` do digest: os tickets abertos (não resolved nem wontfix) com grupo (pronto, bloqueado, andamento), os bloqueios ainda
    abertos, a task e o estado do worker vivo dela; mais os 5 resolvidos mais recentes, com a data do arquivo."""
    vivos = {a.get("task"): _estado_de_gente(a) for a in _dict(aberto).get("agentes") or [] if a.get("estado") in ANDA}
    fechado = ("resolved", "wontfix")
    abertos = {t["num"] for t in ts if t["status"] not in fechado}
    saida = []
    for t in ts:
        if t["status"] in fechado:
            continue
        bloqueios = [n for n in t["blocked_by"] if n in abertos]
        grupo = "bloqueado" if bloqueios else "andamento" if t["status"] == "claimed" else "pronto"
        saida.append({"num": t["num"], "titulo": t["titulo"], "status": t["status"], "grupo": grupo, "bloqueios": bloqueios,
                      "task": t["task"], "worker": vivos.get(t["task"]), "arquivo": t["arquivo"]})
    feitos = []
    for t in ts:
        if t["status"] == "resolved":
            with contextlib.suppress(OSError):
                feitos.append({"num": t["num"], "titulo": t["titulo"], "em": datetime.fromtimestamp(os.path.getmtime(t["arquivo"])).strftime("%Y-%m-%d"),
                               "arquivo": t["arquivo"]})
    return {"abertos": saida, "resolvidos": sorted(feitos, key=lambda x: (x["em"], x["num"]), reverse=True)[:5]}


def monta_digest(events, prs, pendencias, aberto, ts, fila, desde, agora, turnos=None, ausente=None, e2e=None, maquina=None):
    """O digest como dados no formato do contrato (contratos/digest-v1.md) mais o que só a página usa. Só arquivos do orq: nada de gh nem de Orca.

    fila = a ordem que o coordenador declarou (`orq fila`); sem nenhum passo declarado, sai da ordem pelos `Blocked by` dos tickets, um passo por
    feature. features = um grupo por task com PR ligado. linha = o que aconteceu desde `desde` (a página a mostra sempre; o arquivo do
    contrato só a leva com o modo ausente ligado)."""
    por_task = {t["task"]: t for t in ts if t.get("task")}
    por_task_prs = {}
    for i in prs.get("itens") or []:
        por_task_prs.setdefault(i["task"], []).append(i)
    grupos = [{"task": task, "ligado_em": min(i.get("ligado_em") or "" for i in itens),
               "itens": sorted(itens, key=lambda i: (AMBIENTES.index(i["base"]) if i.get("base") in AMBIENTES else len(AMBIENTES), i.get("numero") or 0))}
              for task, itens in por_task_prs.items() if not _pr_antigo(itens, agora)]
    titulo = {g["task"]: _dict(por_task.get(g["task"])).get("titulo") or g["task"] for g in grupos}
    feito = {g["task"]: _passo_feito(g["itens"]) for g in grupos}
    ordem = ordem_de_merge(grupos, ts)
    derivada = [{"passo": n, "nome": titulo[g["task"]], "por": (f"Espera: {', '.join(titulo[t] for t in g['espera'] if not feito[t])}." if [t for t in g["espera"] if not feito[t]] else ""),
                 "prs": [_pr_contrato(i) for i in g["itens"]], "feito": feito[g["task"]], "ticket": _dict(por_task.get(g["task"])).get("num"),
                 "proximo": None if feito[g["task"]] else pr_proximo(g["itens"]), "ciclo": g["ciclo"]} for n, g in enumerate(ordem, 1)]
    declarada = _passos_declarados(fila, prs)
    proximo_passo = _marca_passos(declarada or derivada, prs)
    features = []
    for g in ordem:
        ext = [i for i in g["itens"] if i.get("tag") or i.get("nota")]
        features.append({"tag": next((i["tag"] for i in ext if i.get("tag")), None), "nome": titulo[g["task"]],
                         "nota": next((i["nota"] for i in ext if i.get("nota")), ""), "prs": [_pr_contrato(i) for i in g["itens"]]})
    hoje = agora.astimezone().date()
    pend = [{**i, "depois": bool(pend_depois(i, hoje))} for i in _dict(pendencias).get("itens", []) if isinstance(i, dict)]
    rodando = sorted(({"titulo": a.get("titulo") or "worker sem título", "estado": _estado_de_gente(a), "desde": a.get("desde"),
                       **({"prioridade": a["prioridade"]} if a.get("prioridade") else {})}
                      for a in reavalia(_dict(aberto).get("agentes") or [], events, agora, turnos) if a.get("estado") in (*ANDA, "hibernado")), key=lambda r: r.get("prioridade") or 2)  # a mais alta primeiro
    if maquina:  # as vagas e a fila de despacho (ticket 79): uma chave a mais, `rodando` segue só com workers e a fila do E2E
        maquina = {**maquina, "ocupadas": sum(not r["estado"].startswith("hibernado") for r in rodando), "livres": max(maquina["max_workers"] - sum(not r["estado"].startswith("hibernado") for r in rodando), 0)}
    if e2e:  # a fila do E2E é uma linha a mais em `rodando`: `presa` quando não anda
        rodando.append({"titulo": linha_e2e(e2e).split(". PRESA")[0], "estado": "presa" if e2e["presa"] else "rodando", "desde": None})
    linha = [{"ts": e["ts"], **x} for e in events if (e.get("ts") or "") >= desde and (x := _linha_do_log(e, titulo))]
    return {"versao": 1, "geradoEm": agora.strftime("%Y-%m-%dT%H:%M:%SZ"), "ausente": {"ligado": bool(ausente), "desde": _dict(ausente).get("ligada_em")},
            "fila": declarada or derivada, "proximoPasso": proximo_passo, "features": features, "pendencias": pend, "linha": linha[-DIGEST_LINHAS:], "rodando": rodando, "maquina": maquina,
            "tickets_orq": tickets_do_painel(ts, aberto),
            "pagina": {"data": agora.astimezone().strftime("%Y-%m-%d"), "gerado": agora.astimezone().strftime("%H:%M"), "desde": desde, "poll": prs.get("ultimo_poll"),
                       "linha_antes": max(0, len(linha) - DIGEST_LINHAS), "declarada": bool(declarada)}}


def digest_json(d):
    """O que vai para o atual.json: as chaves do contrato, cada passo só com as dele, e `linha` vazia com o modo ausente desligado."""
    return {"versao": d["versao"], "geradoEm": d["geradoEm"], "ausente": d["ausente"],
            "fila": [{k: p[k] for k in ("passo", "nome", "por", "prs", "feito", "pronto", "avisos")} for p in d["fila"]], "proximoPasso": d["proximoPasso"],
            "features": d["features"], "pendencias": d["pendencias"], "linha": d["linha"] if d["ausente"]["ligado"] else [], "rodando": d["rodando"],
            "tickets_orq": d["tickets_orq"],
            **({"retro": d["retro"]} if d.get("retro") else {})}  # aditivo: sem rodada gravada o contrato v1 fica como era


def html_digest(d):
    """A página do digest (um arquivo só, com o CSS dentro e claro/escuro pelo sistema). Todo texto de fora passa por html.escape."""
    e, pg = html.escape, d["pagina"]

    def chip(i):
        cls, nome = DIGEST_ESTADO.get(i["estado"], ("o", i["estado"]))
        return (f'<a class="pr {cls}" href="{e(i["url"])}" title="{e(i.get("base") or "base ainda desconhecida")}">'
                f'<span class="dot {DIGEST_BASE.get(i.get("base"), "d")}"></span>#{e(str(i.get("numero")))}<em>{nome}</em></a>')

    passos = []
    for g in d["fila"]:
        notas = ([f"Ticket {g['ticket']}."] if g.get("ticket") else []) + ([g["por"]] if g["por"] else []) + ([f"Próximo: {g['proximo']}."] if g.get("proximo") else [])
        aviso = '<p class="aviso">Dependência circular nos tickets: a ordem é a de ligação.</p>' if g.get("ciclo") else ""
        passos.append(f'<li class="{"feito" if g["feito"] else ""}"><span class="num">{"✓" if g["feito"] else g["passo"]}</span><div><b>{e(g["nome"])}</b>'
                      f'<p>{e(" ".join(notas))}</p>{aviso}<div class="prs">{"".join(chip(i) for i in g["prs"])}</div></div></li>')
    fila = f'<ol class="fila">{"".join(passos)}</ol>' if passos else '<p class="sub">Nenhum PR ligado: nada para mergear.</p>'
    origem_ = "Ordem declarada com `orq fila`." if pg["declarada"] else "Pelos Blocked by dos tickets, já que nenhum passo foi declarado com `orq fila`."
    pend = "".join(f'<div class="card dec"><h3>{e(str(p.get("titulo") or p.get("id")))}</h3><p>{e(str(p.get("tipo") or ""))} · <code>{e(str(p.get("id") or ""))}</code>'
                   f'{" · Depois" if p["depois"] else ""}</p></div>' for p in d["pendencias"])
    rod = "".join(f'<span>{e(r["titulo"])} [{e(r["estado"])}]</span>' for r in d["rodando"])
    m = d.get("maquina")
    vagas = (f'<p class="sub">Máquina: {m["ocupadas"]}/{m["max_workers"]:g} vagas ocupadas, {m["livres"]:g} livres, {m["fila"]} na fila de despacho.</p>' if m else "")
    linha = "".join(f'<li class="{ {"sec": "sec", "info": "fo"}.get(x["tipo"], "ok") }"><b>{e(x["titulo"])}</b><p>{_hora_local(x["ts"])} {e(x["detalhe"])}</p></li>' for x in d["linha"])
    antes = f'<p class="sub">+{pg["linha_antes"]} antes destes.</p>' if pg["linha_antes"] else ""
    poll = (f"O estado dos PRs é do poll das {datetime.fromtimestamp(pg['poll']).strftime('%H:%M')}; esta página não consulta o GitHub."
            if isinstance(pg.get("poll"), (int, float)) else "O estado dos PRs ainda não foi lido por nenhum poll (`orq pr poll`); esta página não consulta o GitHub.")
    desde = f"Desde as {_hora_local(pg['desde'])}." if pg["desde"] else "Desde o início do log."
    retro = ('<h2>Falhas por rodada do retro</h2><ul class="sub">' + "".join(f'<li>até {e(r["ate"][:10])}: {r["falhas"]} falha(s)</li>' for r in d["retro"]) + "</ul>") if d.get("retro") else ""
    return (f'<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>Digest {e(pg["data"])}</title><style>{DIGEST_CSS}</style></head><body><main>'
            f'<h1>O que espera você</h1><p class="sub">{e(pg["data"])}. {e(desde)} Gerado às {e(pg["gerado"])}. {e(poll)}</p>'
            '<div class="legend"><span><span class="dot d"></span> development</span><span><span class="dot s"></span> staging</span>'
            '<span><span class="dot p"></span> main</span></div>'
            f'<h2>Ordem de merge</h2><p class="sub">{e(origem_)} Dentro de cada passo, development antes de staging.</p>'
            f'{fila}<h2>Com você</h2>{f"<div class=grid>{pend}</div>" if pend else "<p class=sub>Nada esperando por você.</p>"}'
            f'<h2>Rodando agora</h2>{f"<div class=run>{rod}</div>" if rod else "<p class=sub>Nenhum worker rodando: nada vivo.</p>"}{vagas}'
            f'<h1 style="margin-top:36px">O que aconteceu</h1>{antes}'
            f'{f"<ol class=tl>{linha}</ol>" if linha else "<p class=sub>Nada desde então.</p>"}{retro}</main></body></html>')


def digest_gerar(agora=None, desde=None, com_html=False):
    """Monta o digest dos arquivos locais e grava ORQ_HOME/digest/atual.json (o contrato que o painel lê) e, com `com_html`, digest/<data>.html.
    Cada arquivo é trocado de uma vez. Devolve (dados, caminho do json, caminho do html ou None).

    A janela de `linha` e da página: `desde`, senão o momento em que o modo ausente ligou, senão a última mensagem do usuário. Sem rede: o
    estado dos PRs é o que o poll (`orq pr poll`, o painel do gerente) deixou no prs.json."""
    agora = agora or datetime.now(timezone.utc)
    events, ausente = read_events(), _dict(_cursor_ro().get("ausente")) or None
    janela = desde if desde is not None else (ausente or {}).get("ligada_em") or ultima_do_usuario(events, agora)
    d = monta_digest(events, _prs_ro(), _read_json(PEND), _read_json(_path("aberto.json")), tickets(), _fila_ro(), janela, agora, _turnos_ro(), ausente, fila_e2e(),
                    {"max_workers": maquina_cfg()["max_workers"], "fila": len(fila_despacho_itens())})
    d["retro"] = [{"ate": r["ate"], "falhas": r["falhas"], "metricas": r["metricas"]} for r in _retro_rodadas()[-4:]]  # as últimas rodadas do `orq retro --gravar`
    os.makedirs(_path(DIGEST), exist_ok=True)
    _write_json(_path(os.path.join(DIGEST, "atual.json")), digest_json(d), indent=2)
    pagina = None
    if com_html:
        pagina = _path(os.path.join(DIGEST, d["pagina"]["data"] + ".html"))
        _escrever(pagina, html_digest(d))
    return d, _path(os.path.join(DIGEST, "atual.json")), pagina


def digest_abrir(caminho):
    """Abre a página do digest numa aba do Orca (`orca tab create --url file://…`, na worktree do terminal que chama)."""
    orca("create", "--url", pathlib.Path(caminho).as_uri(), area="tab", timeout=10)


def _marcador_away(hora):
    """O marcador `estado/away` que o statusline.sh lê sem Python: a hora local de quando o modo ligou, ou nada (apagado). Falha de disco não derruba o orq."""
    caminho = os.path.join(HOME, "estado", "away")
    try:
        if hora is None:
            os.remove(caminho)
            return
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        with open(caminho, "w") as f:
            f.write(hora)
    except OSError:
        pass


def ausente_ligar():
    """Liga o modo ausente: o Stop do coordenador atualiza o digest a cada resposta (`hook_stop`). Devolve o estado gravado."""
    estado_ = {"ligada_em": now()}
    _cursor_mut(lambda c: c.__setitem__("ausente", estado_))
    _marcador_away(_hora_local(estado_["ligada_em"]))
    append_event({"tipo": "ausente_ligar"})
    return estado_


def ausente_desligar():
    """Desliga o modo ausente; devolve se estava ligado."""
    ligado = bool(_dict(_cursor_ro().get("ausente")))
    _cursor_mut(lambda c: c.pop("ausente", None))
    _marcador_away(None)
    if ligado:
        append_event({"tipo": "ausente_desligar"})
    return ligado


def linhas_ausente(cur):
    """O estado do modo ausente para `orq ausente`: uma linha, mais o aviso de que ninguém roda o poll dos PRs sem o gerente."""
    a = _dict(_dict(cur).get("ausente"))
    if not a:
        return ["modo ausente desligado"]
    return [f"modo ausente ligado desde {_hora_local(a.get('ligada_em'))} (o HUD mostra away desde ... pelo statusline.sh): o Stop de cada resposta atualiza {_path(os.path.join(DIGEST, 'atual.json'))}",
            *([] if _gerente_cfg() else ["aviso: sem agent manager ligado ninguém roda o poll dos PRs; rode `orq pr poll` (o Stop não chama o gh)"])]


PAINEL_URL = "http://localhost:8765/"


def away(op=None):
    """`orq away` / `/away`: liga, desliga ou mostra o modo ausente; sem op alterna. Devolve as linhas para imprimir.
    Ao desligar só mostra o link do painel da 8765 e a contagem; não abre aba."""
    cur = _dict(_cursor_ro().get("ausente"))
    op = {"ligar": "on", "desligar": "off"}.get(op, op) or ("off" if cur else "on")
    if op == "status":
        return linhas_ausente(_cursor_ro())[:1]
    if op == "on":
        if not cur:
            ausente_ligar()
        desde = _hora_local(_dict(_cursor_ro().get("ausente")).get("ligada_em"))
        return [f"away mode ligado desde {desde}; o digest registra cada resposta"]
    n = len(digest_gerar()[0]["linha"]) if cur else 0
    ausente_desligar()
    return [f"away mode desligado; {n} entradas na linha do tempo, veja o painel {PAINEL_URL}"]


def _ultima_resposta(ev):
    """O texto da última resposta do coordenador: `last_assistant_message` do Stop, senão o fim do transcrito (`transcript_path`); "" sem nenhum."""
    texto = ev.get("last_assistant_message")
    if isinstance(texto, str) and texto.strip():
        return texto
    try:
        with open(ev["transcript_path"], "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - TRANSCRITO_FIM))
            linhas = f.read().decode("utf-8", "replace").splitlines()
    except (KeyError, OSError, TypeError):
        return ""
    for linha in reversed(linhas):
        try:
            m = json.loads(linha)
        except ValueError:
            continue
        if isinstance(m, dict) and m.get("type") == "assistant":
            partes = _dict(m.get("message")).get("content")
            textos = [c.get("text", "") for c in partes if isinstance(c, dict) and c.get("type") == "text"] if isinstance(partes, list) else []
            if "".join(textos).strip():
                return "\n".join(textos)
    return ""


def digest_no_stop(ev):
    """No Stop do coordenador, com o modo ausente ligado: grava a resposta como evento `resposta_coordenador` e atualiza o digest. Fail-open: a
    falha vai para o log e o Stop segue."""
    if not _dict(_cursor_ro().get("ausente")):
        return
    try:
        texto = _ultima_resposta(ev)
        if texto:
            append_event({"tipo": "resposta_coordenador", "texto": texto[:RESPOSTA_MAX], "sessao": (ev.get("session_id") or "")[:8]})
        digest_gerar()
    except TimeoutError:
        raise  # o teto de 3 s do hook vale para o hook inteiro
    except Exception as e:  # noqa: BLE001
        log(f"digest: {type(e).__name__}: {e}")


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


# ---- modo noite: motivo de parada por dispatch e cartão da manhã ----

GERENTE_VIVO_S = 300  # a última rodada do absorver até 5 min antes do fim da noite conta como gerente vivo
LACUNA_S = 600  # log sem evento por mais que isso: a máquina pode ter dormido
CARTAO_LINHAS = 40
CARTAO_SESSAO_H = 12  # o SessionStart injeta a primeira linha do cartão até 12 h depois do fim da noite
PARADAS = {"orcamento": "parou: orçamento", "decisao": "parou: decisão pendente", "limite": "parou: limite de uso"}  # orq encerrar --parada


def fim_motivo(dispatch, events, msgs):
    """O estado final nomeado do dispatch que o `liberar` grava: entregue, falhou, parou: <orçamento|decisão pendente|limite de uso> (o `encerrar --parada`),
    sem worker_done. `msgs` é o inbox; None (o Orca não respondeu) dá "motivo desconhecido" em vez de chutar "sem worker_done"."""
    if msgs is None:
        return "motivo desconhecido"
    done = next((_payload(m) for m in msgs if m.get("type") == "worker_done" and _payload(m).get("dispatchId") == dispatch), None)
    if done:
        return "entregue" if done.get("outcome") == "succeeded" else "falhou"
    enc = next((e for e in reversed(events) if e.get("tipo") == "controle" and e.get("acao") == "encerrar" and e.get("dispatch") == dispatch and e.get("parada")), None)
    return PARADAS.get(enc["parada"], "sem worker_done") if enc else "sem worker_done"


def estado_worktree(caminho):
    """{caminho, sujo (arquivos com mudança não commitada), sem_push (commits fora do origin/main)}; o que o git não disser fica de fora."""
    if not caminho or not os.path.isdir(caminho):
        return {"caminho": caminho} if caminho else {}
    sujo, n = _git(caminho, "status", "--porcelain"), _git(caminho, "rev-list", "--count", "origin/main..HEAD")
    return {"caminho": caminho, **({"sujo": len(sujo.splitlines())} if sujo is not None else {}), "sem_push": int(n) if n and n.strip().isdigit() else 0}


def _janela_noite(events):
    """(evento noite_ligar mais recente, ts do noite_desligar seguinte ou None). Sem noite no log: (None, None)."""
    lig = next((e for e in reversed(events) if e.get("tipo") == "noite_ligar"), None)
    if not lig:
        return None, None
    return lig, next((e["ts"] for e in events if e.get("tipo") == "noite_desligar" and (e.get("ts") or "") >= lig["ts"]), None)


def _fim_da_noite(lig, des):
    return _dt(des or lig["ate"])


def cartao_noite(events, cur, pend, agora, vivos=None):
    """O cartão da manhã (até CARTAO_LINHAS linhas): função pura do log da última noite. `vivos` = {dispatch: estado_worktree} dos dispatches ainda sem fim_dispatch."""
    lig, des = _janela_noite(events)
    if not lig:
        return ["Nenhuma noite no log: rode orq noite ligar --ate HH:MM"]
    vivos = vivos or {}
    ini, terminou = lig["ts"], bool(des) or _dt(lig["ate"]) <= agora
    fim = min(_fim_da_noite(lig, des), agora)
    fim_s = fim.strftime("%Y-%m-%dT%H:%M:%SZ")
    na = [e for e in events if (e.get("ts") or "") >= ini]
    fins = {e["dispatch"]: e for e in na if e.get("tipo") == "fim_dispatch"}
    desp = [e for e in na if e.get("tipo") == "despacho"]
    ls = [f"[orq noite] Cartão da manhã: {_hora_local(ini)} a {_hora_local(fim_s)}, {len(desp)} despacho{'s' if len(desp) != 1 else ''}."]
    linha = lambda d: f"{_cita(d.get('titulo') or d['dispatch'], 36)}: {fins[d['dispatch']]['motivo'] if d['dispatch'] in fins else 'rodando'}"
    ls += [f"Despachos ({len(desp)}):", *("  " + x for x in _lim(desp, 8, linha))] if desp else ["Despachos: nenhum"]
    parou = next((e for e in reversed(na) if e.get("tipo") == "noite_parou"), None)
    if parou:
        ls.append(f"Parou de despachar às {_hora_local(parou['ts'])}: {parou['motivo']}.")
    estados = [(d, {**vivos.get(d["dispatch"], {}), **fins.get(d["dispatch"], {})}) for d in desp]
    sujas = [(_cita(d.get("titulo") or d["dispatch"], 30), w) for d, w in estados if (w.get("sujo") or 0) > 0]
    sem_push = [(_cita(d.get("titulo") or d["dispatch"], 30), w) for d, w in estados if (w.get("sem_push") or 0) > 0]
    if sujas:
        ls += [f"Worktrees sujas ({len(sujas)}):", *("  " + x for x in _lim(sujas, 3, lambda t: f"{t[0]}: {t[1]['sujo']} arquivo{'s' if t[1]['sujo'] != 1 else ''} ({t[1].get('caminho') or '?'})"))]
    if sem_push:
        ls += [f"Sem push ({len(sem_push)}):", *("  " + x for x in _lim(sem_push, 3, lambda t: f"{t[0]}: {t[1]['sem_push']} commit{'s' if t[1]['sem_push'] != 1 else ''} ({t[1].get('caminho') or '?'})"))]
    ids = {e["pend"] for e in na if e.get("tipo") == "pend" and e.get("op") == "add"}
    dec = [i for i in (pend or {}).get("itens", []) if i.get("id") in ids and i.get("tipo") == "decisao"]
    if dec:
        ls += [f"Decisões estacionadas ({len(dec)}):", *("  " + x for x in _lim(dec, 3, lambda i: f"{i['id']}  {_cita(i.get('titulo'), 50)}"))]
    volta = (cur or {}).get("gerente_volta")
    ls.append("Gerente: sem rodada do absorver na noite" if not volta or volta < ini else
              f"Gerente: vivo (última rodada {_hora_local(volta)})" if (fim - _dt(volta)).total_seconds() <= GERENTE_VIVO_S else
              f"Gerente: parou às {_hora_local(volta)} (a noite foi até {_hora_local(fim_s)})")
    marcas = [ini, *(e["ts"] for e in na if e.get("ts") and e["ts"] <= fim_s), *([fim_s] if terminou else [])]
    lacunas = sorted(((_dt(b) - _dt(a)).total_seconds(), a, b) for a, b in zip(marcas, marcas[1:]))
    lacunas = [x for x in lacunas if x[0] > LACUNA_S]
    if lacunas:
        seg, a, b = lacunas[-1]
        ls.append(f"Lacuna: sem evento no log de {_hora_local(a)} a {_hora_local(b)} ({int(seg // 60)} min): a máquina pode ter dormido às {_hora_local(a)}."
                  + (f" +{len(lacunas) - 1} lacuna{'s' if len(lacunas) > 2 else ''} menor{'es' if len(lacunas) > 2 else ''}." if len(lacunas) > 1 else ""))
    cmds = [f"git -C {w['caminho']} status --short" for _, w in sujas if w.get("caminho")] + [f"git -C {w['caminho']} log --oneline origin/main..HEAD" for _, w in sem_push if w.get("caminho")]
    if cmds:
        ls += ["Para colar:", *("  " + x for x in _lim(cmds, 6, lambda c: c))]
    return ls[:CARTAO_LINHAS]


def cartao_primeira_linha(events, agora):
    """A linha do cartão que o SessionStart injeta: só com a noite terminada (desligada ou vencida) há menos de CARTAO_SESSAO_H h, senão None."""
    lig, des = _janela_noite(events)
    if not lig:
        return None
    fim = _fim_da_noite(lig, des)
    if fim > agora or agora - fim > timedelta(hours=CARTAO_SESSAO_H):
        return None
    fins = {e["dispatch"]: e["motivo"] for e in events if e.get("tipo") == "fim_dispatch"}
    desp = [e["dispatch"] for e in events if e.get("tipo") == "despacho" and (e.get("ts") or "") >= lig["ts"]]
    cont = {}
    for d in desp:
        m = fins.get(d, "rodando")
        cont[m] = cont.get(m, 0) + 1
    return (f"[orq noite] Cartão da manhã: a noite acabou às {_hora_local(des or lig['ate'])}, {len(desp)} despacho{'s' if len(desp) != 1 else ''}"
            f"{' (' + ', '.join(f'{n} {m}' for m, n in cont.items()) + ')' if cont else ''}; orq resumo --noite tem o resto.")


def cartao_manha(agora=None):
    """`orq resumo --noite`: o cartão do log e, para os dispatches ainda sem fim_dispatch, a worktree vista agora (worker-show; o que falhar fica de fora)."""
    events, agora = read_events(), agora or datetime.now(timezone.utc)
    lig, _ = _janela_noite(events)
    fins = {e.get("dispatch") for e in events if e.get("tipo") == "fim_dispatch"}
    vivos = {}
    for e in [e for e in events if lig and e.get("tipo") == "despacho" and e["ts"] >= lig["ts"] and e.get("dispatch") not in fins][:10]:
        try:
            vivos[e["dispatch"]] = estado_worktree(_checkpoint(e["dispatch"]).get("caminho"))
        except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as x:
            log(f"cartão: worker-show {e['dispatch']}: {x}")
    return "\n".join(cartao_noite(events, _cursor_ro(), _read_json(PEND), agora, vivos))


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
    lembrar_run(sid, run["id"], ev.get("cwd"), ev.get("_harness_orq") or "claude")
    if (g := os.environ.get("ORQ_MATE")) and run["id"] not in (_dict(_mates().get(g)).get("runs") or []):
        _mate_mut(g, run=run["id"])  # o Run do mate: as entradas dele (relatório de worker) ficam no mundo do mate
    return run


# ---------- hooks ----------

def lembrar_run(sid, run_id, cwd=None, harness="claude"):
    """Guarda em cursor.json o último Run visto por session_id (só de coordenador) e a casa dele, o cwd do primeiro prompt (o Codex não tem
    CLAUDE_PROJECT_DIR); grava só quando muda."""
    cur = _cursor_ro()
    if _dict(cur.get("runs")).get(sid) == run_id and (not cwd or _dict(cur.get("casas")).get(sid)) and (harness == "claude" or _dict(cur.get("harnesses")).get(sid)):
        return

    def grava(c):
        _sub(c, "runs")[sid] = run_id
        if cwd:
            _sub(c, "casas").setdefault(sid, cwd)
        if harness != "claude":
            _sub(c, "harnesses")[sid] = harness

    _cursor_mut(grava)


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
                            **({"com_aviso": True} if com_aviso else {}), **({"grupo": os.environ["ORQ_MATE"]} if os.environ.get("ORQ_MATE") else {})}, novo_id=True)
    checar_gerente_bg()
    ctx = estado(entrada)
    if not os.environ.get("ORQ_MATE") and (adiados := avisos_do_contexto()):  # a fila de avisos é do coordenador
        ctx += "\n" + adiados
    refresh_bg()
    return {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit", "additionalContext": ctx}}


def hook_stop(ev, run):
    # modo aviso (fatia 1): nunca bloqueia; a fatia 5 troca o systemMessage por decision=block com stop_hook_active
    if not os.environ.get("ORQ_MATE"):  # o fim de turno do mate não é resposta do coordenador ao usuário ausente
        digest_no_stop(ev)
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
    elif ferr in ("Edit", "Write", "MultiEdit", "NotebookEdit", "apply_patch"):
        arq = ti.get("file_path") or ti.get("notebook_path") or next(iter(PATCH_ARQUIVO.findall(ti.get("command") or "")), "")
        d = os.path.dirname(os.path.join(ev.get("cwd") or os.getcwd(), arq)) if arq else ""
        d = d if os.path.isdir(d) else ev.get("cwd") or os.getcwd()
    else:
        return None
    gd, comum, topo = ((_git(d, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel") or "").split() + ["", "", ""])[:3]
    if not gd:
        return None
    comum = os.path.realpath(os.path.join(d, comum))
    if os.path.realpath(gd) != comum:  # worktree ligada: só é engano se não é onde o coordenador mora
        casa = os.environ.get("CLAUDE_PROJECT_DIR") or _dict(_cursor_ro().get("casas")).get(ev.get("session_id") or "")
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


PR_CREATE = re.compile(r"\bgh(?:-axi)?\s+pr\s+create\b")
PR_HEAD = re.compile(r"(?<!\S)(?:--head[=\s]+|-H[=\s]*)['\"]?([^\s'\"]+)")
PR_CD = re.compile(r"(?<![\w-])cd\s+(\S+)\s*&&")


def _orq_cli(*args):
    """Roda um subcomando do orq fora do hook (o hook só tem HOOK_TIMEOUT s e o Orca é lento): em segundo plano; com ORQ_NO_BG, até o fim."""
    cmd = [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "orq.py"), *args]
    if os.environ.get("ORQ_NO_BG"):
        signal.alarm(0)  # só os testes chegam aqui: com a máquina carregada o alarme do hook cortaria o filho no meio
        subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=True, timeout=30)
    else:
        subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


def _worktrees_por_ramo(cwd):
    """{branch: caminho da worktree} do `git worktree list` de `cwd`."""
    wts = {}
    for bloco in (_git(cwd, "worktree", "list", "--porcelain") or "").split("\n\n"):
        campos = dict(l.split(" ", 1) for l in bloco.splitlines() if " " in l)
        if campos.get("branch", "").startswith("refs/heads/"):
            wts[campos["branch"][len("refs/heads/"):]] = campos.get("worktree")
    return wts


def hook_prligar(ev, run):
    """PostToolUse de Bash, só no coordenador: a saída de `gh pr create` traz a URL do PR; o orq a liga à task dona da branch (`orq pr auto`,
    fora do hook) ou a põe em "PR sem tarefa". A branch é a do `--head` ou a do cwd (com `cd <dir> &&` na frente, a dele). Só lê o prs.json."""
    ti, resp = ev.get("tool_input") or {}, ev.get("tool_response")
    cmd = ti.get("command") or ""
    if ev.get("tool_name") != "Bash" or not PR_CREATE.search(cmd):
        return None
    urls = PR_RE.findall((resp.get("stdout") or "") if isinstance(resp, dict) else str(resp or ""))
    if not urls:
        return None
    d = _prs_ro()
    urls = [u for u in dict.fromkeys(urls) if not any(i["url"] == u for i in d["itens"]) and not any(x.get("url") == u for x in d.get("sem_task") or [])]
    if not urls:
        return None
    cwd = ev.get("cwd") or os.getcwd()
    cd = PR_CD.search(cmd)
    if cd and os.path.isdir(os.path.join(cwd, os.path.expanduser(cd.group(1).strip("'\"")))):
        cwd = os.path.join(cwd, os.path.expanduser(cd.group(1).strip("'\"")))
    heads = [h.split(":")[-1] for h in PR_HEAD.findall(cmd)]
    if not heads and (h := (_git(cwd, "rev-parse", "--abbrev-ref", "HEAD") or "").strip()):
        heads = [h]
    # um --head por URL (laço com branches diferentes) casa na ordem; um --head literal vale para todas; variável ou contagem que não fecha: a branch de cada
    # URL vem do `gh pr view` no `orq pr auto`, fora do hook (None)
    if len(heads) == len(urls) > 1:
        por_url = dict(zip(urls, heads))
    elif len(set(heads)) == 1 and "$" not in heads[0] and "`" not in heads[0]:
        por_url = {u: heads[0] for u in urls}
    else:
        por_url = {u: None for u in urls}
    wts = _worktrees_por_ramo(cwd)
    for url, head in por_url.items():
        wt = wts.get(head)
        _orq_cli("pr", "auto", url, *(["--head", head] if head else ["--cwd", cwd]), *(["--wt", wt] if wt else []))
    quais = ", ".join(f"#{u.rsplit('/', 1)[1]} ({por_url[u] or 'branch desconhecida'})" for u in urls)
    msg = (f"{MARCA} PR {quais}: o orq os liga à task dona da branch; sem dona ele entra em "
           "\"PR sem tarefa\" no `orq status`. Confira com `orq pr lista`.")
    return {"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": msg}}


def guard_worker():
    """PreToolUse de AskUserQuestion num worker: recusa a caixa, que ficaria presa no terminal, e manda escalar pelo Orca."""
    motivo = (f"{MARCA} worker não abre pergunta no terminal: o coordenador não vê esta tela. Escale pelo Orca com os dados do preâmbulo de despacho: "
              "`orca orchestration ask --from <seu terminal> --dispatch-capability <cap> --question \"<pergunta>\" --options \"a,b\"` (espera a resposta) ou "
              "`orca orchestration send ... --type escalation --subject \"Blocked: <motivo>\" --body \"<detalhes>\"`. Sem o preâmbulo, use o handle do "
              "coordenador que veio na mensagem de continuação.")
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": motivo}}


HOOKS = {"prompt": hook_prompt, "stop": hook_stop, "ask": hook_ask, "guard": hook_guard, "session": hook_session, "lugar": hook_lugar, "externas": hook_externas, "prligar": hook_prligar}


def run_hook(kind, harness="claude"):
    """Só no coordenador (Run ligado e terminal que não é de worker). Fail-open: qualquer exceção vira exit 0 e uma linha no log."""
    def estouro(*_):
        raise TimeoutError(f"hook {kind} passou de {HOOK_TIMEOUT}s")
    try:
        signal.signal(signal.SIGALRM, estouro)
        signal.alarm(HOOK_TIMEOUT)
        if not os.environ.get("ORCA_TERMINAL_HANDLE"):
            return 0  # fora do Orca não há Run nem terminal: nem chama o Orca nem enche o log
        ev = json.load(sys.stdin)
        ev["_harness_orq"] = harness  # de que agente veio o hook: o coordenador guarda o dele
        if os.environ.get("ORQ_MATE"):
            if kind in ("prompt", "stop"):
                mate_turno(kind, ev)  # sem Orca: o prazo dos pedidos ao mate conta do fim do turno dele
            elif kind == "guard" and ev.get("tool_name") == "AskUserQuestion":
                print(json.dumps(guard_mate(), ensure_ascii=False))
                return 0
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
        if kind == "prligar":  # a cada Bash: sem Orca, o coordenador é a sessão que já tem Run guardado e não é worker
            sid = ev.get("session_id") or ""
            if _dict(_cursor_ro().get("runs")).get(sid) and _papeis().get(sid) != "worker":
                out = hook_prligar(ev, None)
                if out:
                    print(json.dumps(out, ensure_ascii=False))
            return 0
        run = coordenador(ev)
        if run is None:
            sid = ev.get("session_id") or ""
            if kind in ("prompt", "stop") and _papeis().get(sid) == "worker":
                registra_turno(kind, ev, harness)  # o worker só grava o turno: sem Orca, sem Run
            if kind == "guard" and ev.get("tool_name") == "AskUserQuestion" and _papeis().get(sid) == "worker":
                print(json.dumps(guard_worker(), ensure_ascii=False))  # sem Orca: a caixa abre no terminal, onde só quem olha a vê
                return 0
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


# ---------- grupos e secondmates (ticket 80) ----------
# Desenho: ~/.claude/orquestrador-plan/secondmate-por-grupo.md. Um grupo (ORQ_HOME/groups/<nome>.json) junta os projetos de um domínio; o mate do grupo é
# uma sessão de coordenador com ORQ_MATE=<nome> no ambiente, que `orq mate abrir` sobe num terminal (não pelo worker-start: sem dispatch, não há
# capability para o Orca revogar depois do primeiro worker_done). O canal é o events.jsonl: o coordenador pede (`mate_pedido`, id pN, prazo), o mate
# sobe (`entrada` origem mate, com `corr` quando responde a um pedido), e o gerente cobra o prazo (mate_volta).

GRUPOS_DIR = "groups"
PRAZO_PEDIDO_S = int(os.environ.get("ORQ_PRAZO_PEDIDO_S") or 120)  # o do firstmate, contado do fim do turno que recebeu o pedido
TIPOS_SUBIDA = ("resposta", "decisao", "pr", "bloqueio", "resumo")
TURNOS_MATE = 20  # turnos guardados por mate: o prazo conta do primeiro que começou depois da entrega, não do último
TURNO_ABERTO_TETO_S = 1800  # turno sem fim (Esc, erro da API: o Stop não rodou) conta como acabado no começo depois disto
ENTREGUE = ("enviado", "adiado")  # o avisa_coordenador (ticket 82): adiado já é entrega, sai no contexto do próximo prompt do coordenador
CHARTER_MATE = """Você é o secondmate do grupo {grupo} no orq. O usuário fala só com o coordenador; você coordena os workers deste grupo e não conversa com o usuário.
Projetos do grupo: {projetos}.{regras}
1. Crie o seu Run uma vez: `orca orchestration run-create --objective "{grupo}: secondmate"`. Depois de um resume, o Run já está em `orq grupos`: use `orca orchestration run-use --id <run>`.
2. Despache e acompanhe os workers como o coordenador faz (`orq despachar`, `orq agentes`, `orq steer`, `orq liberar`), com a skill worker-routing.
3. Pedido do coordenador chega digitado como `orq ▸ pedido pN ...`. Responda sempre com `orq mate subir --corr pN --tipo resposta --texto "<resposta>"`: a resposta no chat ninguém lê.
4. Suba ao coordenador, com `orq mate subir --tipo <decisao|pr|bloqueio|resumo> --texto "..."`: decisão que só o usuário toma, PR ou branch pronta, bloqueio, e um resumo quando um ticket fecha. O resto fica com você.
5. Nunca use AskUserQuestion, não faça push nem abra PR: quem faz é o coordenador, depois da subida `pr`."""
MSG_MATE_VOLTA = ("Você é o secondmate do grupo {grupo} e o seu terminal caiu. Confira `orq grupos` (o seu Run), `orq agentes --run <run>` e os pedidos sem resposta "
                  "em `orq mate pedidos`; responda cada um com `orq mate subir --corr pN`.")


def grupos():
    """{nome: cfg} de ORQ_HOME/groups/*.json. Arquivo ilegível, ou com `projetos`/`prefixos` que não são listas, fica de fora com uma linha no log."""
    out = {}
    for arq in sorted(glob.glob(os.path.join(_path(GRUPOS_DIR), "*.json"))):
        cfg = _read_json(arq)
        if not isinstance(cfg, dict) or not all(isinstance(cfg.get(k, []), list) for k in ("projetos", "prefixos")):
            log(f"grupos: {arq} ilegível ou fora do formato, ignorado")
            continue
        out[os.path.basename(arq)[:-len(".json")]] = cfg
    return out


def _dentro(cwd, pasta):
    c, p = os.path.abspath(os.path.expanduser(cwd)), os.path.abspath(os.path.expanduser(pasta))
    return c == p or c.startswith(p.rstrip("/") + "/")


def grupo_de(grupos_, titulo=None, cwd=None, grupo=None):
    """(nome, motivo) do grupo de um pedido; (None, motivo) quando fica com o coordenador. Ordem: `grupo` explícito, prefixo do título (sem caixa),
    cwd dentro de um projeto do grupo. Dois grupos no mesmo critério é ambíguo e não roteia: o coordenador decide ou pergunta."""
    if grupo:
        if grupo not in grupos_:
            raise ValueError(f"grupo {grupo} não existe em {GRUPOS_DIR}/ (há: {', '.join(grupos_) or 'nenhum'})")
        return grupo, "explícito"
    t = (titulo or "").strip().lower()
    criterios = (("título", lambda g: t and any(p and t.startswith(p.lower()) for p in g.get("prefixos") or [])),
                 ("cwd", lambda g: cwd and any(_dentro(cwd, p) for p in g.get("projetos") or [])))
    for nome, casa in criterios:
        achados = [n for n, g in grupos_.items() if casa(g)]
        if len(achados) == 1:
            return achados[0], f"pelo {nome}"
        if achados:
            return None, f"ambíguo pelo {nome} ({', '.join(achados)}): fica com o coordenador"
    return None, "nenhum grupo serve: fica com o coordenador"


def _mates():
    return _dict(_cursor_ro().get("mates"))


def _mate_mut(grupo, **campos):
    def grava(c):
        m = _sub(_sub(c, "mates"), grupo)
        for k, v in campos.items():
            if k == "run":
                m["runs"] = [*[r for r in m.get("runs") or [] if r != v], v]
            else:
                m[k] = v

    _cursor_mut(grava)


def mate_turno(kind, ev):
    """Hooks prompt e stop de uma sessão com ORQ_MATE: o início e o fim do turno do mate (o prazo dos pedidos conta do fim), a sessão e o cwd do resume."""
    if kind == "prompt" and origem(ev.get("prompt")) == "comando":
        return
    g, agora = os.environ["ORQ_MATE"], now()

    def grava(c):
        m = _sub(_sub(c, "mates"), g)
        ts = [t for t in m.get("turnos") or [] if isinstance(t, list) and len(t) == 2]
        if kind == "prompt":
            ts.append([agora, None])
        elif ts and ts[-1][1] is None:
            ts[-1][1] = agora
        m["turnos"] = ts[-TURNOS_MATE:]
        m["terminal"] = os.environ.get("ORCA_TERMINAL_HANDLE")
        if ev.get("session_id"):
            m["sessao"] = ev["session_id"]
        if kind == "prompt" and ev.get("cwd") and not m.get("cwd"):
            m["cwd"] = ev["cwd"]

    _cursor_mut(grava)


def _num_entrada(e):
    m = re.fullmatch(r"e(\d+)", str(e.get("id") or ""))
    return int(m.group(1)) if m else 0


def _grupo_do_run(run):
    """{"grupo": g} quando o Run é de um mate (o hook dele o registrou), para a entrada do Run ficar no mundo do mate; senão {}."""
    return next(({"grupo": g} for g, m in _mates().items() if run in (_dict(m).get("runs") or [])), {})


def mate_pendentes(eventos, mates, agora):
    """Pedidos ao mate sem resposta correlacionada, com o estado: a_entregar, aguardando, reenviar, escalar ou escalado. Pura.

    O prazo conta do fim do primeiro turno do mate que começou depois da entrega (ou da repostagem): turno longo não estoura, e os turnos seguintes (avisos do
    Orca, heartbeats) não empurram o prazo. Sem turno começado depois dela, conta da própria entrega. Turno aberto há mais de TURNO_ABERTO_TETO_S conta do começo. Só uma `entrada` origem mate com o mesmo `corr` resolve. Uma repostagem, uma escalada, e nada mais: nunca em laço."""
    respondidos = {e.get("corr") for e in eventos if e.get("tipo") == "entrada" and e.get("origem") == "mate" and e.get("corr")}
    marcas = {}
    for e in eventos:
        if e.get("tipo") in ("mate_entregue", "mate_reenvio", "mate_escalado"):
            marcas.setdefault(e.get("corr"), {})[e["tipo"]] = _ts(e.get("ts"))
    out = []
    for p in (e for e in eventos if e.get("tipo") == "mate_pedido" and e.get("corr") not in respondidos):
        m, mt = marcas.get(p["corr"], {}), _dict(mates.get(p.get("grupo")))
        base = {"corr": p["corr"], "grupo": p.get("grupo"), "texto": p.get("texto"), "prazo": p.get("prazo"), "ts": p.get("ts")}
        if "mate_entregue" not in m:
            out.append({**base, "estado": "a_entregar"})
            continue
        if not p.get("prazo"):
            continue
        if "mate_escalado" in m:
            out.append({**base, "estado": "escalado"})
            continue
        desde = m.get("mate_reenvio") or m["mate_entregue"]
        turno = next(((_ts(i), _ts(f)) for i, f in (t for t in mt.get("turnos") or [] if isinstance(t, list) and len(t) == 2)
                      if _ts(i) and _ts(i) >= desde), None)
        if turno and not turno[1] and (agora - turno[0]).total_seconds() <= TURNO_ABERTO_TETO_S:
            out.append({**base, "estado": "aguardando"})
            continue
        conta = (turno[1] or turno[0]) if turno else desde
        if (agora - conta).total_seconds() <= p["prazo"]:
            out.append({**base, "estado": "aguardando"})
        else:
            out.append({**base, "estado": "escalar" if "mate_reenvio" in m else "reenviar"})
    return out


def _texto_pedido(corr, texto, prazo, de_novo=False):
    resposta = f" Responda com `orq mate subir --corr {corr} --tipo resposta --texto \"...\"`." if prazo else ""
    texto = " ".join((texto or "").split())  # a quebra de linha submeteria o pedido pela metade; o evento guarda o texto inteiro
    return f"orq ▸ pedido {corr}{' de novo, sem resposta no canal' if de_novo else ''} do coordenador: {texto}{resposta}"


def mate_pedir(grupo, texto, prazo=PRAZO_PEDIDO_S, responde=None):
    """Grava o pedido (`mate_pedido`, corr pN) antes de digitá-lo no terminal do mate; se o mate está no meio do turno, o gerente entrega depois.
    `responde`: a entrada que o mate subiu e este pedido responde (a decisão voltando); ela fecha com o efeito `mate`."""
    if grupo not in grupos():
        raise ValueError(f"grupo {grupo} não existe em {GRUPOS_DIR}/")
    terminal = _dict(_mates().get(grupo)).get("terminal")
    if not terminal:
        raise ValueError(f"o grupo {grupo} não tem mate aberto: orq mate abrir {grupo}")
    if responde:
        eventos = read_events()
        alvo = next((e for e in eventos if e.get("tipo") == "entrada" and e.get("id") == responde), None)
        if not alvo or alvo.get("origem") != "mate" or alvo.get("mate") != grupo:
            raise ValueError(f"--responde {responde}: não é uma entrada que o mate {grupo} subiu; nada foi gravado")
        if any(e.get("tipo") == "intake" and e.get("entrada") == responde for e in eventos):
            raise ValueError(f"--responde {responde}: a entrada já foi fechada; nada foi gravado")
    with _trava("cursor.lock"):
        n = 1 + max((int(e["corr"][1:]) for e in read_events() if e.get("tipo") == "mate_pedido" and re.fullmatch(r"p\d+", str(e.get("corr")))), default=0)
        corr = f"p{n}"
        _grava_evento({"tipo": "mate_pedido", "corr": corr, "grupo": grupo, "texto": texto[:2000], "prazo": prazo, **({"responde": responde} if responde else {})})
    if responde:
        intake(responde, "mate", corr)
    antes = now()  # o hook do mate grava o início do turno antes de o digita voltar: a entrega vale de antes dele
    entrega = digita(terminal, _texto_pedido(corr, texto, prazo))
    if entrega == "enviado":
        append_event({"tipo": "mate_entregue", "corr": corr, "ts": antes})
    return {"corr": corr, "grupo": grupo, "entrega": entrega}


def mate_subir(tipo, texto, corr=None, link=None, grupo=None):
    """O mate sobe ao coordenador: uma entrada origem mate (o coordenador a trata como as outras, com orq intake), com `corr` quando responde a um pedido."""
    g = grupo or os.environ.get("ORQ_MATE")
    if not g:
        raise ValueError("orq mate subir roda no terminal do mate (ORQ_MATE) ou com --grupo")
    if tipo not in TIPOS_SUBIDA:
        raise ValueError(f"--tipo {tipo}: use {'|'.join(TIPOS_SUBIDA)}")
    if corr and not any(e.get("tipo") == "mate_pedido" and e.get("corr") == corr and e.get("grupo") == g for e in read_events()):
        raise ValueError(f"pedido {corr} não existe para o grupo {g}: a resposta não foi gravada")
    if tipo == "resposta" and not corr:
        raise ValueError("--tipo resposta pede --corr <pedido>")
    return append_event({"tipo": "entrada", "origem": "mate", "mate": g, "tipo_mate": tipo, "texto": texto[:2000], "fonte": f"mate {g}",
                         **({"corr": corr} if corr else {}), **({"link": link} if link else {})}, novo_id=True)


def _comando_mate(grupo, cfg, sessao):
    agente, modelo = cfg.get("harness") or "claude", cfg.get("modelo")
    if agente not in HARNESS:
        raise ValueError(f"harness {agente} do grupo {grupo}: o orq só abre {', '.join(HARNESSES)}")
    if sessao:
        cmd = HARNESS[agente]["resume"](sessao, modelo, cfg.get("effort"), MSG_MATE_VOLTA.format(grupo=grupo))
    else:
        regras = f"\nLeia antes as regras do grupo em {cfg['regras']}." if cfg.get("regras") else ""
        charter = CHARTER_MATE.format(grupo=grupo, projetos=", ".join(cfg.get("projetos") or []) or "nenhum", regras=regras)
        cmd = HARNESS[agente]["abrir"](modelo, cfg.get("effort"), charter)
    return shlex.join(["env", f"ORQ_MATE={grupo}", *cmd])


def mate_abrir(grupo):
    """Abre o mate do grupo num terminal novo, ou o retoma (`--resume` da sessão que os hooks dele gravaram) se ele caiu. Um mate vivo por grupo."""
    cfg = grupos().get(grupo)
    if cfg is None:
        raise ValueError(f"grupo {grupo} não existe em {GRUPOS_DIR}/")
    m, vivos = _dict(_mates().get(grupo)), _terminais_vivos()
    if vivos is None:
        raise ValueError("o Orca não listou os terminais: sem saber se o mate está vivo, nada foi aberto")
    if m.get("terminal") in vivos:
        raise ValueError(f"o mate {grupo} já está aberto no terminal {m['terminal']}")
    cwd = m.get("cwd") or cfg.get("cwd") or next(iter(cfg.get("projetos") or []), None)
    cwd = cwd and os.path.expanduser(cwd)
    novo = _terminal_novo(f"mate {grupo}{' (retomado)' if m.get('sessao') else ''}", _comando_mate(grupo, cfg, m.get("sessao")), cwd)
    if m.get("sessao") and not _voltou(novo, cfg.get("harness") or "claude"):
        # sessão que não volta deixa um shell: o gerente digitaria o pedido nele. Fecha, esquece a sessão, e o próximo abrir sobe com o charter
        with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired):
            orca("close", "--terminal", novo, area="terminal")
        _mate_mut(grupo, terminal=None, sessao=None)
        append_event({"tipo": "mate", "op": "abrir", "grupo": grupo, "terminal": novo, "retomado": False, "falhou": "a sessão não voltou"})
        raise ValueError(f"a sessão {m['sessao']} do mate {grupo} não voltou; terminal fechado. Rode orq mate abrir {grupo} de novo para abrir com o charter")
    _mate_mut(grupo, terminal=novo, morto=None, **({"cwd": cwd} if cwd else {}))
    append_event({"tipo": "mate", "op": "abrir", "grupo": grupo, "terminal": novo, "retomado": bool(m.get("sessao")), "anterior": m.get("terminal")})
    return {"grupo": grupo, "terminal": novo, "retomado": bool(m.get("sessao"))}


def mate_volta():
    """Uma volta do gerente pelos mates: entrega o pedido que esperava o mate ficar livre, reenvia uma vez o que estourou o prazo, escala uma vez o que estourou
    de novo, avisa o coordenador de cada subida nova e do mate que caiu (uma vez por terminal). Devolve uma linha por ação."""
    mates, linhas = _mates(), []
    if not mates:
        return linhas
    coord = (_gerente_cfg() or {}).get("coordenador")
    eventos, agora = read_events(), datetime.now(timezone.utc)
    for p in mate_pendentes(eventos, mates, agora):
        terminal, antes = _dict(mates.get(p["grupo"])).get("terminal"), now()
        if p["estado"] == "a_entregar" and terminal and digita(terminal, _texto_pedido(p["corr"], p["texto"], p["prazo"])) == "enviado":
            append_event({"tipo": "mate_entregue", "corr": p["corr"], "ts": antes})
            linhas.append(f"mate {p['grupo']}: {p['corr']} entregue")
        elif p["estado"] == "reenviar" and terminal and digita(terminal, _texto_pedido(p["corr"], p["texto"], p["prazo"], de_novo=True)) == "enviado":
            append_event({"tipo": "mate_reenvio", "corr": p["corr"], "ts": antes})
            linhas.append(f"mate {p['grupo']}: {p['corr']} reenviado")
        elif p["estado"] == "escalar" and coord and avisa_coordenador(coord, f"orq ▸ mate {p['grupo']} não respondeu ao pedido {p['corr']} ({_cita(p['texto'])!r}) "
                                                                               f"nem depois da repostagem. Veja o terminal {terminal}.") in ENTREGUE:
            append_event({"tipo": "mate_escalado", "corr": p["corr"]})
            linhas.append(f"mate {p['grupo']}: {p['corr']} escalado ao coordenador")
    ate = _cursor_ro().get("mate_avisada_ate")
    ate = ate if isinstance(ate, int) else 0  # a marca d'água: o maior eN de subida já avisado (uma lista com teto reavisaria as velhas)
    for e in (x for x in eventos if x.get("tipo") == "entrada" and x.get("origem") == "mate" and _num_entrada(x) > ate):
        if not coord or avisa_coordenador(coord, f"orq ▸ mate {e['mate']} subiu {e['id']} ({e.get('tipo_mate')}): {_cita(e.get('texto'), 200)}. Trate com orq intake {e['id']} "
                                                 f"<efeito>; para responder ao mate, orq mate pedir {e['mate']} --responde {e['id']} --texto \"...\"") not in ENTREGUE:
            break
        _cursor_mut(lambda c, n=_num_entrada(e): c.__setitem__("mate_avisada_ate", n))
        linhas.append(f"mate {e['mate']}: subida {e['id']} avisada ao coordenador")
    vivos = _terminais_vivos()
    for g, m in mates.items():
        t = _dict(m).get("terminal")
        if vivos is None or not t or t in vivos or _dict(m).get("morto") == t:
            continue
        if coord and avisa_coordenador(coord, f"orq ▸ mate {g} caiu (terminal {t} sumiu do Orca). Suba de novo com: orq mate abrir {g}") in ENTREGUE:
            _mate_mut(g, morto=t)
            linhas.append(f"mate {g}: caiu, coordenador avisado")
    return linhas


def texto_grupos(gs, mates, vivos, eventos, agora):
    linhas = []
    for nome, cfg in gs.items():
        m = _dict(mates.get(nome))
        estado = "sem mate" if not m.get("terminal") else "vivo" if vivos is None or m["terminal"] in vivos else "caiu"
        pend = [p for p in mate_pendentes(eventos, mates, agora) if p["grupo"] == nome]
        linhas.append(f"{nome}: {', '.join(cfg.get('projetos') or [])} | prefixos {', '.join(cfg.get('prefixos') or []) or '-'} | mate {estado}"
                      + (f" ({m['terminal']}, Runs {', '.join(m.get('runs') or []) or '-'})" if m.get("terminal") else "")
                      + (f" | pedidos: {', '.join(p['corr'] + ' ' + p['estado'] for p in pend)}" if pend else ""))
    return "\n".join(linhas) or f"nenhum grupo em {_path(GRUPOS_DIR)}"


def guard_mate():
    """PreToolUse de AskUserQuestion num mate: o usuário não olha o terminal dele; a decisão sobe ao coordenador."""
    motivo = (f"{MARCA} o secondmate não pergunta ao usuário: suba a decisão com `orq mate subir --tipo decisao --texto \"<pergunta e opções, com a recomendada>\"`; "
              "a resposta volta como `orq ▸ pedido pN`.")
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": motivo}}


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
    elif efeito == "mate":
        if not any(x.get("tipo") == "mate_pedido" and x.get("corr") == ref for x in eventos):
            raise ValueError(f"mate pede o pedido que respondeu à entrada (pN), e {ref} não existe")
        ev["ref"] = ref
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
    import hashlib
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
AVISO_GAP_S = float(os.environ.get("ORQ_AVISO_GAP_S") or 3)  # entre a leitura da caixa e a que vem logo antes do send: quem começou a digitar nesse meio-tempo barra o aviso (ticket 82)
COORD_OCIOSO_MIN = float(os.environ.get("ORQ_COORD_OCIOSO_MIN") or 10)  # prompt do usuário mais novo que isso: o coordenador tem gente e nenhum aviso é digitado nele (ticket 82)
WAKE_OCIOSO_MIN = float(os.environ.get("ORQ_WAKE_OCIOSO_MIN") or 2)  # o mesmo para o aviso que acorda o coordenador (worker_done): janela curta, a entrega não espera 10 min (ticket 86)
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
    `falhou` (o Orca recusou: nada foi digitado). A caixa é lida duas vezes, com AVISO_GAP_S entre elas (ticket 82: o usuário que começa a digitar
    entre a leitura e o send tinha o aviso por cima do texto). O tui-idle sozinho engana (satisfeito no começo do turno e por uns 20 s de turno, conferido
    no Orca real em 29/09): quem barra de verdade é o `agent_prompt_blocked` do send, que o Orca devolve com o agente no meio do turno. Se o Orca observa a submissão e não viu o turno começar, manda um Enter sozinho (numa caixa
    vazia não faz nada); timeout do send conta como enviado, porque o texto pode ter saído e repetir empilharia."""
    if motivo := terminal_livre(handle):
        return motivo
    time.sleep(AVISO_GAP_S)
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


STEER_AVISO_MAX = 300  # caracteres do ajuste que o aviso digitado no worker ocupado carrega


def _tela_de_turno_sem_rascunho(handle):
    """True com o spinner (`esc to interrupt`, igual nos dois agentes) na tela, sem rascunho na caixa e sem menu esperando resposta humana."""
    try:
        t = orca("read", "--terminal", handle, "--screen", "--limit", str(TELA_LINHAS), area="terminal").get("terminal") or {}
    except (RuntimeError, subprocess.TimeoutExpired):
        return False
    tail = t.get("tail") or []
    return not ((t.get("draft") or "").strip() or any(tela_pergunta(tail, h) for h in HARNESS) or "esc to interrupt" not in "\n".join(map(str, tail[-15:])))


def digita_ocupado(handle, texto):
    """Digita `texto` + Enter num agente no meio do turno, para o Claude Code pôr na fila e injetar no próximo resultado de ferramenta (01/10).

    Só com o spinner na tela, sem rascunho na caixa e sem menu esperando resposta humana, nas duas leituras (AVISO_GAP_S entre elas, ticket 82):
    nos três casos devolve `ocupado` sem digitar. Devolve `ocupado_digitado`, ou `ocupado` se o Orca barrou o send (agent_prompt_blocked) ou falhou."""
    if not _tela_de_turno_sem_rascunho(handle):
        return "ocupado"
    time.sleep(AVISO_GAP_S)
    if not _tela_de_turno_sem_rascunho(handle):
        return "ocupado"
    try:
        orca("send", "--terminal", handle, "--text", texto, "--enter", area="terminal", timeout=TIMEOUT_ORCA)
    except subprocess.TimeoutExpired:
        return "ocupado_digitado"  # o texto pode ter saído: repetir empilharia
    except RuntimeError:
        return "ocupado"
    return "ocupado_digitado"


def coordenador_ativo(agora=None, minutos=None):
    """True se o último prompt do usuário é mais novo que `minutos` (COORD_OCIOSO_MIN): o coordenador tem gente, e digitar nele cai no meio do que ela escreve."""
    minutos = COORD_OCIOSO_MIN if minutos is None else minutos
    agora = agora or datetime.now(timezone.utc)
    ult = next((e["ts"] for e in reversed(read_events()) if e.get("tipo") == "entrada" and e.get("origem") == "usuario" and e.get("ts") and not e.get("grupo")), None)  # a do mate não
    return bool(ult) and (agora - _dt(ult)).total_seconds() < minutos * 60


def _notifica_mac(texto):
    """Notificação nativa do macOS para o aviso que não foi digitado; desligada, só com `"notificar_macos": true` no gerente.json (ticket 82)."""
    if not _gerente_cfg().get("notificar_macos"):
        return
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        subprocess.run([os.environ.get("ORQ_OSASCRIPT") or "osascript", "-e", f"display notification {json.dumps(texto, ensure_ascii=False)} with title \"orq\""],
                       capture_output=True, timeout=3, check=False)


def avisa_coordenador(handle, texto, contexto=True, minutos=None):
    """Leva um aviso ao coordenador sem digitar por cima de quem escreve (ticket 82). Devolve `enviado` (coordenador ocioso: digitado), `adiado`
    (coordenador com gente: o aviso espera na fila `avisos` do cursor, sai no contexto do próximo prompt, `contexto` False para o que o resumo já
    mostra, e `avisos_entregar` o digita se o coordenador ficar ocioso) ou o motivo do `digita` (nada saiu: repita depois). `adiado` já é entrega."""
    if not coordenador_ativo(minutos=minutos):
        return digita(handle, texto)
    _cursor_mut(lambda c: c.setdefault("avisos", []).append({"texto": texto, "ts": now(), "contexto": contexto, **({"minutos": minutos} if minutos is not None else {})}))
    _notifica_mac(texto)
    return "adiado"


def avisos_entregar():
    """Uma volta do painel: com o coordenador ocioso há mais de COORD_OCIOSO_MIN (WAKE_OCIOSO_MIN no aviso de acordar), digita o aviso mais antigo da fila (um por volta; o seguinte
    encontra o coordenador ocupado). Devolve as linhas do painel."""
    g, fila = _gerente_cfg(), _cursor_ro().get("avisos")
    if not g.get("coordenador") or not isinstance(fila, list) or not fila:
        return []
    a = next((x for x in fila if not coordenador_ativo(minutos=x.get("minutos"))), None)  # o aviso de acordar (2 min) não espera atrás de um de 10
    if not a or digita(g["coordenador"], a["texto"]) != "enviado":
        return []
    _cursor_mut(lambda c: c.__setitem__("avisos", [x for x in c.get("avisos") or [] if x != a]))
    return ["aviso adiado digitado no coordenador, ocioso"]


def avisos_do_contexto():
    """Esvazia a fila `avisos` e devolve a linha do contexto do prompt do usuário com os avisos que não foram digitados (vazia sem nenhum)."""
    pegos = []

    def pega(c):
        pegos.extend(c.pop("avisos", None) or [])
    if _cursor_ro().get("avisos"):
        _cursor_mut(pega)
    textos = [re.sub(r"^orq: ", "", a["texto"]) for a in pegos if isinstance(a, dict) and a.get("contexto") and a.get("texto")]
    return "[orq] Avisos que não foram digitados (você estava escrevendo): " + " | ".join(textos) if textos else ""


def _aviso_ajuste(handle, ajuste):
    """O aviso do Orca com o resumo do ajuste, numa linha só (a quebra de linha submeteria o texto antes da hora)."""
    return f"{_aviso_worker(handle)} Ajuste do coordenador: {_cita(ajuste, STEER_AVISO_MAX)}"


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
    t = _dict(_turnos_ro().get(dispatch))
    sid = t.get("sessao")
    arqs = [t["transcrito"]] if t.get("transcrito") else glob.glob(os.path.join(PROJETOS, "*", f"{glob.escape(sid)}.jsonl")) if sid else []
    for arq in arqs:
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
    dispatch = _dispatch_da_msg(linha)
    acordado = acordar(dispatch, f"resposta do coordenador à sua mensagem {msg_id}: {texto}") if dispatch in _hibernados() else None  # o worker hibernado não leria a resposta no inbox
    return append_event({"tipo": "resposta_worker", "msg_id": msg_id, "run": alvo, "dispatch": dispatch, "texto": texto,
                         "resposta_id": (res.get("message") or res).get("id"), **({"acordado": acordado["estado"]} if acordado else {})})


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
    corpo = texto if pedido is None else f"{texto}\n\n{PEDIDO_TITULO} (acréscimo)\n{pedido}"
    if t.get("dispatch_id") in _hibernados():  # sem terminal para receber: o resume leva o ajuste (mesmo a task já entregue, que o worker hibernado ainda pode seguir)
        r = acordar(t["dispatch_id"], f"ajuste do coordenador: {corpo}")
        if r["estado"] == "falhou":
            raise ValueError(f"o worker está hibernado e não acordou: {r['aviso']}")
        ev = append_event({"tipo": "steer", "task": task, "dispatch": t["dispatch_id"], "run": alvo, "texto": texto, "acordado": r["estado"],
                           **({"pedido": pedido} if pedido is not None else {})})
        if entrada:
            intake(entrada, "steer", task, run=alvo)
        return ev
    if t.get("status") != "dispatched" or not t.get("dispatch_id"):
        raise ValueError(f"task {task} está {t.get('status')}, não dispatched: sem worker para receber o ajuste")
    try:
        res = orca("send", "--run", alvo, "--to", f"dispatch:{t['dispatch_id']}", "--subject", "Ajuste",
                   "--body", corpo, "--priority", "high", timeout=10)
    except RuntimeError as e:
        raise ValueError(fenced if "consumer_fenced" in str(e) else str(e))
    msg = res.get("message") or res
    # O Orca digita o próprio aviso (a linha ganha `delivered_at`) no worker que ele alcança, e no que encerrou o turno parado no prompt às vezes não
    # (visto em 29/09): só esse recebe o aviso daqui, e um aviso que o Orca acabou de digitar não se repete.
    time.sleep(STEER_ESPERA_S)
    handle = _terminal_do_dispatch(alvo, t["dispatch_id"])
    entrega = "orca" if _orca_avisou(msg.get("id")) else digita(handle, _aviso_worker(handle)) if handle else "sem_terminal"
    if entrega == "ocupado":
        entrega = digita_ocupado(handle, _aviso_ajuste(handle, texto))
        if entrega == "ocupado_digitado":
            append_event({"tipo": "steer_digitado_ocupado", "task": task, "dispatch": t["dispatch_id"], "run": alvo, "msg_id": msg.get("id")})
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
    import tempfile
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
    cartao = cartao_primeira_linha(read_events(), datetime.now(timezone.utc))
    linhas = estado().splitlines()[:LINHAS_SESSAO - 2 - bool(cartao)]
    if cartao:
        linhas.append(cartao)
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
    novos = {e["dispatch"]: e["terminal"] for e in read_events() if e.get("tipo") == "retomada" and e.get("dispatch") and e.get("terminal")}  # o `orq retomar` subiu outro terminal: o Orca segue apontando o morto
    return [{**w, "agentTerminalHandle": novos[w["dispatchId"]]} if w.get("dispatchId") in novos else w for w in ws]


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


def _morto(handle):
    """O terminal não está no `orca terminal list`. Sem lista (Orca falhou ou cortou) não prova nada: False."""
    vivos = _terminais_vivos()
    return vivos is not None and handle not in vivos


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
                                     "effort": _fundo(res, "worker", "startOptions", "launch", "requested", "effort"),
                                     "agente": _fundo(res, "worker", "startOptions", "agent")}
        except Exception as e:  # noqa: BLE001
            log(f"agentes: worker-show {w.get('dispatchId')}: {type(e).__name__}: {e}")
            return w["dispatchId"], {}

    runs = sorted({w["runId"] for w in ws if w.get("runId")})
    with ThreadPoolExecutor(8) as ex:
        tits = dict(zip(runs, ex.map(titulos, runs)))
        vivos = dict(ex.map(mostra, [w for w in ws if _ativo(w)]))
    return {w["dispatchId"]: {"titulo": tits.get(w.get("runId"), {}).get(w.get("taskId")), **vivos.get(w["dispatchId"], {})} for w in ws}


def tela_pergunta(linhas, agente="claude"):
    """{tipo, texto, opcoes: [[n, rótulo]]} se o fim da tela é um menu do agente esperando resposta humana, senão None (também para agente sem adaptador).

    Três tipos: `trust` (confiar na pasta), `permissao` (Do you want to proceed?…) e `pergunta` (AskUserQuestion). Menu aberto = opções numeradas
    1, 2… seguidas, uma com o cursor `❯`, no fim da tela (no máximo TELA_RODAPE_MAX linhas de rodapé depois); um `❯ 1. …` solto no histórico não conta.
    """
    if agente not in HARNESS:
        return None
    padroes = HARNESS[agente]["tela"]
    tela = [re.sub(r"[│╭╮╰╯─]", " ", str(l)).rstrip() for l in linhas or []]
    ops = [(i, padroes["opcao"].match(l)) for i, l in enumerate(tela)]
    ops = [(i, m) for i, m in ops if m]
    bloco = []
    for i, m in reversed(ops):  # o último bloco de opções consecutivas
        if bloco and (bloco[0][0] - i > 2 or int(m.group(2)) != int(bloco[0][1].group(2)) - 1):
            break
        bloco.insert(0, (i, m))
    if len(bloco) < 2 or int(bloco[0][1].group(2)) != 1 or not any(m.group(1) == padroes["cursor"] for _, m in bloco):
        return None
    if sum(bool(l.strip()) for l in tela[bloco[-1][0] + 1:]) > TELA_RODAPE_MAX:
        return None
    texto = "\n".join(tela)
    tipo = next((t for t, r in padroes["perguntas"] if r.search(texto)), None)
    if not tipo:
        return None
    acima = [l.strip() for l in tela[:bloco[0][0]] if l.strip()][-3:]
    return {"tipo": tipo, "texto": _cita(" ".join(acima), 300), "opcoes": [[int(m.group(2)), m.group(3)] for _, m in bloco]}


def _ler_telas(ws, detalhes):
    """{dispatch: {espera, pergunta}} dos workers do Claude Code rodando cuja tela (fim do `terminal read --screen`) mostra shell/monitor ainda em execução
    (`espera`, o motivo) ou um menu esperando resposta humana (`pergunta`, de tela_pergunta). Só entra quem tem um dos dois.

    Só o refresh, o gerente e o `orq agentes` chamam isto (um read por worker, em paralelo); os hooks de prompt leem o que ficou no cache. Falha de leitura não prova nada.
    """
    def le(w):
        try:
            tail = _fundo(orca("read", "--terminal", w["agentTerminalHandle"], "--screen", "--limit", str(TELA_LINHAS), area="terminal", timeout=10), "terminal", "tail") or []
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as e:
            log(f"agentes: tela de {w['dispatchId']}: {type(e).__name__}: {e}")
            return w["dispatchId"], None
        agente = detalhes[w["dispatchId"]]["agente"]
        espera = HARNESS[agente]["tela"]["espera"]
        m = espera and espera.search("\n".join(map(str, tail[-15:])))
        achado = {"espera": f"{m.group(0).strip()} (tela)" if m else None, "pergunta": tela_pergunta(tail, agente)}
        return w["dispatchId"], achado if m or achado["pergunta"] else None
    alvo = [w for w in ws if w.get("dispatchStatus") == "dispatched" and w.get("agentTerminalHandle") and (detalhes.get(w.get("dispatchId")) or {}).get("agente") in HARNESS]
    with ThreadPoolExecutor(8) as ex:
        return {d: a for d, a in ex.map(le, alvo) if a}


def _telas(ws, detalhes):
    """{dispatch: motivo} da parte `espera` de _ler_telas."""
    return {d: a["espera"] for d, a in _ler_telas(ws, detalhes).items() if a["espera"]}


def agentes(run=None, todos=False, agora=None):
    """O estado de cada dispatch: worker-list de todos os Runs (ou de `run`) mais o inbox. Liberados, sem terminal, retidos pelo Orca e as tasks "Prova r5" só com `todos`."""
    ws, events, vivos, hib = _workers_todos(run), read_events(), _terminais_vivos(), _hibernados()
    if not todos:
        liberados = _liberados(events)
        ws = [w for w in ws if w.get("dispatchId") in hib or not _sem_terminal(w, liberados, vivos)]  # o hibernado fechou o terminal de propósito, e o entregue hibernado ainda não foi liberado; os retidos por motivo do Orca (external_terminal…) e o user_takeover com prompt humano não têm ação possível: só aparecem com --todos
        humanos = _interacao_registrada(events)
        ws = [w for w in ws if w.get("dispatchId") in hib or _ativo(w) and (w.get("dispatchStatus") == "dispatched" or not _retencao(w, humanos))]
    msgs = orca("inbox", "--limit", "200", timeout=20)["messages"]
    agora = agora or datetime.now(timezone.utc)
    det = _detalhes(ws)
    lidas = _ler_telas([w for w in ws if w.get("dispatchId") not in hib], det)  # o terminal do hibernado não existe: nada a ler
    ags = monta_agentes(ws, msgs, events, agora, det, vivos, _turnos_ro(), {d: a["espera"] for d, a in lidas.items() if a["espera"]},
                        {d: a["pergunta"] for d, a in lidas.items() if a["pergunta"]}, hib)
    nao_lidos = {e.get("dispatch") for e in alertas_recentes(events, agora, ags) if e.get("alerta") == "steer_nao_lido"}
    for a in ags:
        if a["dispatch"] in nao_lidos:
            a["alerta"] = "steer não lido"
        a["prioridade"] = prioridade_de(events, a["task"], a["dispatch"], a.get("titulo"))
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
        elif a["estado"] == "hibernado":
            hb = f"hibernado desde {_hora_local(a.get('hibernado_desde'))}" + (f" ({a['motivo_hibernado']})" if a.get("motivo_hibernado") else "")
        if a.get("espera") and a["espera"] not in (a.get("fase") or ""):  # espera vista na tela ou pausa do coordenador: não está na fase do heartbeat
            hb += f" — esperando: {a['espera']}"
        elif a["estado"] == "travado" and a.get("motivo"):
            hb += f" — {a['motivo']}"
        linhas.append(f"{a['estado']:<11} {'P' + str(a['prioridade']) + ' ' if a.get('prioridade') else ''}{a['task']}  {_cita(a.get('titulo') or '?', 36)}  {a.get('modelo') or '?'}  {a['terminal']}  {hb}".rstrip())
        for av in a.get("entrega") or []:
            linhas.append(f"            AVISO: {av}")
        if a.get("controle"):
            linhas.append("            controle: " + "; ".join(_txt_controle(c, a["dispatch"]) for c in a["controle"]))
        if a.get("pergunta"):
            p = a["pergunta"]
            linhas.append(f"            PERGUNTA NA TELA ({p['tipo']}): {p['texto']} [{' | '.join(f'{n}) {r}' for n, r in p['opcoes'])}]")
            linhas.append(f'            -> orq responder-tela {a["task"]} <opção>')
        if a.get("alerta"):
            linhas.append(f"            ALERTA: {a['alerta']} (o worker não leu o ajuste depois de {STEER_TENTATIVAS} avisos; um check sem --ack esconde as mensagens novas)")
        if a["estado"] in ("travado", "nao_comecou", "parado"):
            linhas.append(f'            -> orq steer {a["task"]} "<ajuste>" --run {a["run"]}')
        elif a["estado"] == "entregue":
            linhas.append(f"            -> orq liberar {a['dispatch']}" if not a.get("retido") else f"            retido: {a['retido']} (o orq liberar não fecha)")
        elif a["estado"] == "hibernado":
            linhas.append(f"            -> orq acordar {a['task']} (o steer e o responder também acordam)")
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
    t = _dict(_turnos_ro().get(dispatch))
    if t.get("harness") == "codex" and t.get("transcrito"):
        return _prompts_humanos_codex(t["transcrito"])
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


def _prompts_humanos_codex(arq):
    """Prompts de usuário de um rollout do Codex (`response_item` com `role: user`) além do despacho, do Orca e do contexto que o próprio Codex injeta
    (`<environment_context>`, o AGENTS.md); None se o arquivo não abre."""
    humanos = 0
    try:
        with open(arq, encoding="utf-8", errors="replace") as f:
            for linha in f:
                if '"role":"user"' not in linha and '"role": "user"' not in linha:
                    continue
                try:
                    p = _dict(json.loads(linha).get("payload"))
                except ValueError:
                    continue
                if p.get("type") != "message" or p.get("role") != "user":
                    continue
                texto = "\n".join(c.get("text") or "" for c in p.get("content") or [] if isinstance(c, dict) and c.get("type") == "input_text").strip()
                if texto and not texto.startswith(("<environment_context>", "# AGENTS.md instructions", "<user_instructions>")) and \
                        origem(texto) not in ("despacho", "orca", "notificacao", "resumo"):
                    humanos += 1
    except OSError as e:
        log(f"transcrito {arq}: {type(e).__name__}: {e}")
        return None
    return humanos


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


def _fim_do_dispatch(dispatch, w):
    """Os campos do evento `fim_dispatch`: o motivo (inbox e log) e a worktree como está agora (worker-show; sem ela, só o motivo)."""
    try:
        msgs = orca("inbox", "--limit", "200", timeout=20)["messages"]
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError, KeyError) as e:
        log(f"liberar: inbox falhou ({type(e).__name__}: {e}); fim_dispatch sem motivo")
        msgs = None
    try:
        wt = estado_worktree(_checkpoint(dispatch).get("caminho"))
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:
        log(f"liberar: worker-show {dispatch}: {e}")
        wt = {}
    return {"motivo": fim_motivo(dispatch, read_events(), msgs), **wt}


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
    fim = _fim_do_dispatch(dispatch, w)
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
    if estado != "release_pending":  # a repetição do release_pending grava o fim de novo; vale o último
        append_event({"tipo": "fim_dispatch", "dispatch": dispatch, "task": w.get("taskId"), "run": run_id, **fim})
    if estado != "release_pending":
        _esquece_hibernado(dispatch)  # liberado não volta: o terminal já estava fechado e não há o que acordar
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
    caminho = _caminho_do_worker(res)
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


def responder_tela(task, opcao, run=None):
    """Responde o menu preso na tela do worker da task: lê a tela de novo (o menu tem de estar aberto e a opção existir), digita o número da opção com Enter
    no terminal dele e grava quem respondeu e o quê (evento `controle` responder-tela, com `por` = este terminal). `opcao` é o número ou o começo do rótulo.

    Sem menu aberto, ou opção que não existe, recusa sem digitar nada: um número digitado no prompt comum viraria mensagem ao worker."""
    w = next((w for w in _workers_todos(run) if w.get("taskId") == task and w.get("dispatchStatus") == "dispatched"), None)
    if not w or not w.get("agentTerminalHandle"):
        raise ValueError(f"task {task} não tem worker rodando com terminal: nada a responder")
    handle = w["agentTerminalHandle"]
    agente = _dict(_turnos_ro().get(w["dispatchId"])).get("harness") or "claude"
    p = tela_pergunta(_fundo(orca("read", "--terminal", handle, "--screen", "--limit", str(TELA_LINHAS), area="terminal", timeout=10), "terminal", "tail") or [], agente)
    if not p:
        raise ValueError(f"a tela de {handle} não mostra um menu esperando resposta: nada foi digitado")
    alvo = next((o for o in p["opcoes"] if str(o[0]) == opcao.strip() or o[1].lower().startswith(opcao.strip().lower())), None)
    if not alvo:
        raise ValueError(f"opção {opcao!r} não existe no menu ({' | '.join(f'{n}) {r}' for n, r in p['opcoes'])})")
    try:
        if HARNESS[agente]["tela"].get("enter_separado"):
            orca("send", "--terminal", handle, "--text", str(alvo[0]), area="terminal", timeout=10)
            time.sleep(0.3)
            orca("send", "--terminal", handle, "--enter", area="terminal", timeout=10)
        else:
            orca("send", "--terminal", handle, "--text", str(alvo[0]), "--enter", area="terminal", timeout=10)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _controle("responder-tela", w, "falhou", terminal=handle, erro=str(e), por=os.environ.get("ORCA_TERMINAL_HANDLE"))
        raise
    append_event({"tipo": "pergunta_tela_fim", "dispatch": w["dispatchId"]})
    return _controle("responder-tela", w, "ok", terminal=handle, por=os.environ.get("ORCA_TERMINAL_HANDLE"), opcao=f"{alvo[0]}) {alvo[1]}", pergunta=p["texto"], tipo_pergunta=p["tipo"])


def _parar(acao, w, base):
    """worker-stop do dispatch que ainda roda (o Orca fecha o terminal do agente e nunca apaga a worktree); dispatch que já terminou não leva nada."""
    if w.get("dispatchStatus") != "dispatched":
        return
    try:
        orca("worker-stop", "--dispatch", w["dispatchId"], timeout=30, run=w.get("runId"))
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _controle(acao, w, "falhou", passo="worker-stop", erro=str(e), **base)
        raise
    _esquece_hibernado(w["dispatchId"])


def encerrar(dispatch, motivo, run=None, parada=None):
    """worker-stop (se ainda roda) e depois o `liberar`, com o motivo no log. Parar não volta atrás: se o release falha, o worker fica parado, o
    terminal retido e a worktree onde estava (evento `parcial`), e `orq liberar` termina o serviço."""
    if not (motivo or "").strip():
        raise ValueError("encerrar pede --motivo: ele fica no log e no orq agentes")
    if parada and parada not in PARADAS:
        raise ValueError(f"--parada espera {'|'.join(PARADAS)} (recebi {parada!r})")
    w = _worker_do_dispatch(dispatch, run)
    try:
        cp = _checkpoint(dispatch)
    except RuntimeError as e:
        log(f"encerrar: worker-show {dispatch}: {e}")
        cp = {}
    base = {"motivo": motivo.strip(), "parada": parada, "terminal": w.get("agentTerminalHandle"), "head": cp.get("head"), "sujo": cp.get("sujo")}
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
    ocup = maquina_ocupacao()  # troca um por um: o worker do próprio dispatch sai da conta (é parado antes de subir o novo), então só o modelo caro a mais ou um dispatch já morto estouram o teto
    ocup["vivos"].pop(dispatch, None)
    if motivo := maquina_vaga(pedido[0], ocup):
        raise ValueError(f"{motivo}: nada foi parado. Relance com um modelo barato, espere abrir vaga ou ajuste orq maquina")
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


def _prompt_entrou(dispatch, terminal, titulo):
    """O turno do dispatch começou (hook prompt do worker em turnos.json) ou a tela do terminal já mostra o título do spec fora da caixa de digitação."""
    if _dict(_turnos_ro().get(dispatch)).get("inicio"):
        return True
    if not terminal:
        return False
    try:
        t = orca("read", "--terminal", terminal, "--limit", "40", area="terminal").get("terminal") or {}
    except (RuntimeError, subprocess.TimeoutExpired):
        return False
    return not (t.get("draft") or "").strip() and titulo in json.dumps(t.get("tail") or [], ensure_ascii=False)


def _esperar_prompt(dispatch, terminal, titulo):
    fim = time.monotonic() + INICIO_ESPERA_S
    while True:
        if _prompt_entrou(dispatch, terminal, titulo):
            return True
        if time.monotonic() >= fim:
            return False
        time.sleep(min(0.5, max(fim - time.monotonic(), 0)))


def _conferir_inicio(dispatch, terminal, titulo, out):
    """O spec entrou no worker? Espera o prompt; se não entrou, manda um Enter (numa caixa vazia não faz nada) e espera de novo. `out["enter"]` marca o Enter."""
    if _esperar_prompt(dispatch, terminal, titulo):
        return True
    if not terminal:
        return False
    with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired):
        orca("send", "--terminal", terminal, "--enter", area="terminal", timeout=3 + TIMEOUT_ORCA)
        out["enter"] = True
    return _esperar_prompt(dispatch, terminal, titulo)


def confiar_codex(*caminhos):
    """Marca cada pasta como confiável no Codex (`[projects."<p>"] trust_level = "trusted"` no config.toml) e devolve as que entraram agora. O Codex
    guarda o trust pela raiz do repositório principal, e a worktree herda; o orq grava as duas. Config que não lê como TOML fica como estava."""
    import tomllib  # import tardio: só o despacho de um worker Codex usa
    try:
        txt = open(CODEX_CONFIG, encoding="utf-8").read()
    except FileNotFoundError:
        txt = ""
    try:
        proj = _dict(tomllib.loads(txt).get("projects"))
    except tomllib.TOMLDecodeError as e:
        log(f"confiar_codex: {CODEX_CONFIG} não é TOML ({e}): nada gravado")
        return []
    novos = [p for p in dict.fromkeys(c for c in caminhos if c) if _dict(proj.get(p)).get("trust_level") != "trusted"]
    if not novos:
        return []
    txt += "".join(f'\n[projects.{json.dumps(p, ensure_ascii=False)}]\ntrust_level = "trusted"\n' for p in novos)
    os.makedirs(os.path.dirname(CODEX_CONFIG) or ".", exist_ok=True)
    tmp = f"{CODEX_CONFIG}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(txt)
    os.replace(tmp, CODEX_CONFIG)
    return novos


def _raiz_do_repo(d):
    """A pasta do checkout principal do repositório de `d` (o pai do git common dir), ou None fora de um repositório."""
    comum = (_git(d, "rev-parse", "--path-format=absolute", "--git-common-dir") or "").strip()
    return os.path.realpath(os.path.dirname(comum)) if comum else None


def despachar(run, titulo, spec_arquivo, modelo, effort, worktree=None, name=None, base_branch=None, entrada=None, ticket=None, prioridade=None, agente="claude", _drenando=False):
    """worker-start (com --model e --effort, o que o hook worker-routing-guard exige) + evento `despacho` + intake da entrada.

    Devolve os ids e o comando do waiter; não espera nada. Recusa antes de criar a task o que o Orca recusaria depois.

    Com `ticket` (o número de um `orq ticket novo`) o worker sobe na task que o ticket já criou (`worker-start --task`), sem título nem spec: o
    ticket é o conteúdo. Com o modo noite ligado, recusa depois do horário, do teto de despachos ou das falhas seguidas (noite_checar). Recusa também
    com o uso do plano acima do limiar (uso_checar). `prioridade` (1 alta a 3 baixa) fica no evento; sem ela vale a da frente do título (prioridade_padrao).

    Orçamento da máquina (ticket 79): sem vaga (workers vivos ou caros no teto) ou com a máquina sob pressão, o pedido entra na fila de despacho e a resposta é
    `{estado: "enfileirado", fila, posicao, motivo}` no lugar dos ids do worker; o gerente sobe o item por prioridade quando abrir vaga (`_drenando`: é ele
    quem chama, e sem vaga levanta SemVaga em vez de enfileirar de novo).
    """
    if prioridade is not None and prioridade not in (1, 2, 3):
        raise ValueError("--prioridade espera 1 (alta), 2 ou 3 (baixa)")
    if agente not in HARNESS:
        raise ValueError(f"--agente {agente}: o orq só despacha {', '.join(HARNESSES)}")
    if effort not in HARNESS[agente]["efforts"]:
        raise ValueError(f"--effort {effort} não existe no {agente} ({', '.join(HARNESS[agente]['efforts'])})")
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
    prio = prioridade or prioridade_de(read_events(), tk and tk["task"], None, titulo)
    uso_checar(prio, agente=agente)
    pedido = _texto_da_entrada(entrada)
    _adotar(run)
    if not run_do_coordenador(run):
        raise ValueError(f"o despacho é para o Run {run}, que o coordenador não comanda: {dica_ligar(run)}")
    with _trava("despacho.lock"):  # a vaga conferida e o worker-start formam um passo: despachos paralelos não passam do teto juntos
        motivo = maquina_barra(modelo, run=run)
        if motivo and _drenando:
            raise SemVaga(motivo)
        if motivo:
            return _enfileirar_despacho(motivo, run, titulo, spec, modelo, effort, worktree, name, base_branch, entrada, tk, prio, agente)
        if spec is not None and not spec.lstrip().startswith("#"):
            spec = f"# {titulo}\n\n{spec}"  # o Claude Code tira o nome da aba do começo do prompt
        if spec is not None and pedido is not None:  # o pedido literal fica no topo, separado do que o coordenador escreveu; o review mede contra ele
            cabeca, _, resto = spec.partition("\n")
            spec = (f"{cabeca}\n\n{PEDIDO_TITULO}\n{pedido}\n\nO que o coordenador escreveu abaixo não o substitui: o pronto se confere contra este pedido.\n\n"
                    f"{resto.lstrip(chr(10))}")
        ambiente = noite_ambiente() if noite_ativa(_cursor_ro()) else None  # na noite o worker sobe sem prompt de git (credencial, pinentry)
        confiadas = confiar_codex(_raiz_do_repo(os.getcwd())) if agente == "codex" else []  # antes do worker-start: o Codex pergunta do trust ao subir
        args = ["worker-start", "--run", run, *(["--task", tk["task"]] if tk else ["--spec", spec, "--task-title", titulo]),
                "--agent", agente, "--model", modelo, "--effort", effort]
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
        ev = {"tipo": "despacho", "run": run, "task": task, "dispatch": dispatch, "titulo": titulo, "agente": agente, "modelo": modelo, "effort": effort, "terminal": terminal,
              **({"worktree": worktree} if worktree else {}), **({"nome": name} if name else {}), **({"entrada": entrada} if entrada else {}),
              **({"ticket": tk["num"]} if tk else {}), **({"ambiente": list(ambiente)} if ambiente else {}), **({"prioridade": prioridade} if prioridade else {})}
        append_event(ev)
    out = {"dispatchId": dispatch, "taskId": task, "run": run, "terminal": terminal, "espera": f"python3 ~/.claude/scripts/orca-wait-runs.py {run}"}
    if agente == "codex":
        with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired, KeyError):
            confiadas += confiar_codex(_checkpoint(dispatch)["caminho"])  # a worktree que o Orca criou
        if confiadas:
            out["confiadas"] = confiadas
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
    if not _conferir_inicio(dispatch, terminal, titulo, out):
        append_event({"tipo": "nao_iniciou", "run": run, "task": task, "dispatch": dispatch, "terminal": terminal})
        out["estado"] = "nao_iniciou"
        out["aviso"] = "; ".join(filter(None, [out.get("aviso"), f"o spec não entrou no worker (nem depois do Enter): abra o terminal {terminal}, cole o spec ou relance com orq relancar {dispatch}"]))
    return out


def _sessao_do_coordenador(sessao):
    """Caminho do transcrito: o id dado (ou prefixo dele) ou, sem id, a última sessão de coordenador registrada em cursor.json."""
    if not sessao:
        runs = _dict(_cursor_ro().get("runs"))
        if not runs:
            raise ValueError("nenhuma sessão de coordenador registrada em cursor.json: passe --sessao <id>")
        sessao = list(runs)[-1]
    if _dict(_cursor_ro().get("harnesses")).get(sessao) == "codex":
        raise ValueError(f"a sessão {sessao[:8]} é de um coordenador no Codex, que não tem AskUserQuestion: não há resposta de caixa para auditar")
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

def gerente_ligar(terminal, runs=None, assumir=False):
    """Liga os Runs ao terminal do agent manager (`run-use` com o handle dele) e grava o gerente.json: daí em diante o orq deste coordenador fala
    com o Orca por esse handle, e os avisos do Orca (heartbeat incluído) vão para o terminal do agent manager, não para o coordenador.

    Soma aos Runs que o mesmo gerente já tem (sem duplicar); outro terminal de gerente recomeça a lista. Sem `runs`, entra o Run ligado ao
    coordenador (o do `run-create` que acabou de rodar). O Orca liga um Run por terminal: só o último fica ligado, o painel os reveza.
    `assumir`: este coordenador toma o gerente.json de outro que ainda aparece no Orca (troca de terminal sem o antigo sumir): os Runs dele vêm junto."""
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
    if antes and not ja and (assumir or _morto(antes.get("coordenador")) or _morto(antes.get("gerente"))):
        ja = antes["runs"]  # queda: o coordenador ou o gerente antigo não existem mais, os Runs deles seguem no gerente novo (ticket 48)
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


def gerente_checar():
    """Fora do hook: o terminal do gerente do gerente.json ainda está no `orca terminal list`? Grava o achado em PAINEL_CHECAGEM. Sem lista
    confiável (Orca falhou ou cortou) não prova nada: não está morto."""
    g = _gerente_cfg()
    if not g:
        return None
    vivos = _terminais_vivos()
    ck = {"ts": time.time(), "terminal": g["gerente"], "morto": vivos is not None and g["gerente"] not in vivos}
    _write_json(_path(PAINEL_CHECAGEM), ck)
    return ck


def gerente_subir(forcar=False):
    """O terminal do agent manager sumiu: cria outro com o painel-agent-manager.sh e religa a ele todos os Runs do gerente.json (este coordenador
    assume o arquivo). Recusa com o terminal antigo ainda no Orca, a não ser com `forcar`."""
    g = _gerente_cfg()
    if not g:
        raise ValueError("sem gerente.json: nada a subir (use `orq gerente ligar`)")
    vivos = _terminais_vivos()
    if not forcar:
        if vivos is None:
            raise ValueError("o Orca não listou os terminais: sem prova de que o agent manager morreu, nada foi feito (--forcar sobe assim mesmo)")
        if g["gerente"] in vivos:
            raise ValueError(f"o terminal {g['gerente']} ainda existe no Orca: se o painel parou, reinicie painel-agent-manager.sh nele (--forcar sobe outro)")
    novo = _terminal_novo("agent manager", f"sh {shlex.quote(_path('painel-agent-manager.sh'))}")
    ev = gerente_ligar(novo, g["runs"], assumir=True)
    try:
        os.remove(_path(PAINEL_CHECAGEM))
    except OSError:
        pass
    return {**ev, "terminal": novo, "runs": g["runs"]}


def gerente_desligar(run=None, assumir=False):
    """Devolve ao terminal do coordenador (`run-use` com o handle próprio) o Run `run`, ou todos, e tira do gerente.json.

    Sem `run` o arquivo é apagado. O coordenador segura um Run só (um por terminal): fica com o que o gerente tinha ligado, e os outros
    ficam sem coordenador até um `run-use`. `assumir` desliga também o gerente.json de outro coordenador que ainda aparece no Orca."""
    g = _gerente_cfg()
    meu = os.environ.get("ORCA_TERMINAL_HANDLE")
    if g and g.get("coordenador") != meu and (assumir or _morto(g.get("coordenador"))):
        g = {**g, "coordenador": meu}  # queda: o terminal do coordenador antigo não existe mais, este o substitui (ticket 48)
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


# ---------- orçamento da máquina e fila de despacho (ticket 79) ----------

MAQUINA = "maquina.json"  # por cima de MAQUINA_PADRAO; `orq maquina set <chave> <valor>` grava
MAQUINA_PADRAO = {"max_workers": 4,  # workers vivos ao mesmo tempo (24 GB de RAM, 12 CPUs: cada um pode subir uma stack de E2E)
                  "max_e2e": 1,  # só informativo: a fila global do E2E (scripts/e2e-lock.sh) já serializa as stacks
                  "max_caros": 2, "modelos_caros": ["claude-opus-*", "gpt-6-astra*", "gpt-6-sol*"],  # padrões glob; o modelo caro conta no max_workers também
                  "mem_livre_min_mb": 3072, "livre_pct_min": 15,  # abaixo de qualquer um dos dois a pressão é alta
                  "carga_max": 12,  # loadavg de 1 min acima disto (uma por CPU) é pressão alta
                  "mem_piso_mb": 1024,  # piso de segurança: nem o Run isento sobe com a memória livre abaixo disto
                  "runs_isentos": ["Orquestrador*"],  # padrões glob (id ou objetivo do Run): o trabalho do próprio orq sobe sob pressão e sem o teto de workers; max_caros, max_e2e e mem_piso_mb o seguram
                  "pausar_sob_pressao": False}  # True: sob pressão o gerente pausa sozinho o worker de menor prioridade (orq pausar)
FILA_DESPACHO = "fila-despacho.json"  # {itens: [...]}: o que o `orq despachar` e o `orq retomar` não puderam subir; o gerente sobe por prioridade
FILA_DESPACHO_SPECS = "fila-despacho"  # ORQ_HOME/fila-despacho/<id>.md: cópia do spec de um despacho enfileirado (o arquivo do coordenador pode sumir)
VIVOS_MAQUINA = ("rodando", "travado", "nao_comecou", "parado", "perguntando")
PROCESSOS_PESADOS = ("claude", "codex", "node", "docker")  # a soma de RSS que o `orq maquina` mostra
ESPERA_SEGURADO_S = 300  # item da fila que o uso do plano ou o modo noite segurou só é tentado de novo depois disto
FALHAS_FILA = 3  # tentativas do gerente com erro antes de o item sair da fila


class SemVaga(Exception):
    """O gerente tentou subir um item da fila e a máquina não tem mais vaga (outro despacho tomou a vaga)."""


def _maquina_tipo_ok(padrao, v):
    if isinstance(padrao, bool):
        return isinstance(v, bool)
    if isinstance(padrao, (int, float)):
        return isinstance(v, (int, float)) and not isinstance(v, bool)
    return isinstance(v, list) and all(isinstance(x, str) for x in v)


def maquina_cfg():
    """MAQUINA_PADRAO por cima de maquina.json; chave de tipo errado ou desconhecida vale como ausente."""
    lido = _dict(_read_json(_path(MAQUINA)))
    return {k: lido[k] if k in lido and _maquina_tipo_ok(p, lido[k]) else p for k, p in MAQUINA_PADRAO.items()}


def maquina_definir(chave, valor):
    """`orq maquina set <chave> <valor>`: valor em JSON (4, true, ["claude-opus-*"]); recusa chave desconhecida ou de outro tipo. Devolve a config nova."""
    if chave not in MAQUINA_PADRAO:
        raise ValueError(f"chave {chave!r} desconhecida; as que existem: {', '.join(MAQUINA_PADRAO)}")
    try:
        v = json.loads(valor)
    except ValueError:
        raise ValueError(f"valor {valor!r} não é JSON (use 4, true ou [\"claude-opus-*\"])")
    if not _maquina_tipo_ok(MAQUINA_PADRAO[chave], v):
        raise ValueError(f"valor {valor} não serve para {chave} (espera {type(MAQUINA_PADRAO[chave]).__name__})")
    _write_json(_path(MAQUINA), {**_dict(_read_json(_path(MAQUINA))), chave: v}, indent=2)
    return maquina_cfg()


def maquina_ler():
    """O que a máquina tem agora, com ferramentas nativas do macOS: {mem_livre_mb, livre_pct, carga, ncpu, rss_mb: {claude, codex, node, docker}}.

    mem_livre_mb = (free + inactive + speculative + purgeable) do `vm_stat`; livre_pct = o `System-wide memory free percentage` do `memory_pressure`;
    carga = loadavg de 1 min (o mesmo número do `sysctl vm.loadavg`); rss_mb = soma do RSS dos processos de PROCESSOS_PESADOS no `ps`. Fonte que falha
    fica None/vazia (a pressão que ela mediria vale como desconhecida, nunca como alta). `ORQ_MAQUINA_LEITURA` aponta um JSON no lugar da leitura (testes)."""
    if os.environ.get("ORQ_MAQUINA_LEITURA"):
        simulada = _dict(_read_json(os.environ["ORQ_MAQUINA_LEITURA"]))
        procs = simulada.pop("processos", None)  # a amostra de processos simulada (a lista do _processos); sem ela a origem fica desconhecida
        return {**simulada, "origem": maquina_origem(procs)}

    def roda(*cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=5).stdout
        except (OSError, subprocess.TimeoutExpired):
            return ""
    leitura = {"mem_livre_mb": None, "livre_pct": None, "carga": None, "ncpu": os.cpu_count(), "rss_mb": {}}
    vm = roda("vm_stat")
    pag = re.search(r"page size of (\d+)", vm)
    pags = {n: re.search(rf"^Pages {n}:\s+(\d+)", vm, re.M) for n in ("free", "inactive", "speculative", "purgeable")}
    if pag and all(pags.values()):
        leitura["mem_livre_mb"] = sum(int(m.group(1)) for m in pags.values()) * int(pag.group(1)) // 2**20
    pct = re.search(r"free percentage:\s*(\d+)%", roda("memory_pressure"))
    leitura["livre_pct"] = int(pct.group(1)) if pct else None
    with contextlib.suppress(OSError):
        leitura["carga"] = round(os.getloadavg()[0], 2)
    rss = dict.fromkeys(PROCESSOS_PESADOS, 0)
    for linha in roda("ps", "-axo", "rss=,comm=").splitlines():
        kb, _, comm = linha.strip().partition(" ")
        base = os.path.basename(comm.strip()).lower()
        nome = next((p for p in PROCESSOS_PESADOS if base == p or (p == "docker" and "docker" in comm.lower())), None)
        if nome and kb.isdigit():
            rss[nome] += int(kb) // 1024
    leitura["rss_mb"] = rss
    leitura["origem"] = maquina_origem(_processos(com_cwd=False))
    return leitura


def _nome_processo(args):
    """Como o processo aparece na lista de culpados: o app (`OrbStack` de .../OrbStack.app/...) ou o nome do executável."""
    app = re.search(r"/([^/]+)\.app/", args)
    return app.group(1) if app else os.path.basename((args.split(None, 1) or [""])[0])


def maquina_origem(procs):
    """De quem é a carga, a partir da lista do `_processos`: {orq_cpu, fora_cpu, orq_rss_mb, fora_rss_mb, fora_por_cpu, fora_por_mem} ou None sem a lista.

    São do orq os processos de agente (claude, codex: workers, gerente e coordenador) e tudo o que sobe abaixo deles, mais os do E2E (`e2e` no comando e o que
    sobe abaixo). O resto é de fora. `fora_por_cpu` e `fora_por_mem` são os três maiores de fora, somados por nome ([{nome, valor}], %CPU e MB). Limite: o %CPU do
    `ps` do macOS é uma média que decai em cerca de um minuto, e os containers do OrbStack contam como um processo só, de fora."""
    if not procs:
        return None
    orq = _descendentes(procs, [p["pid"] for p in procs if _agente_do(p) or re.search(r"e2e", p["args"], re.I)])
    dentro, fora = [p for p in procs if p["pid"] in orq], [p for p in procs if p["pid"] not in orq]

    def maiores(chave, escala):
        por_nome = {}
        for p in fora:
            n = _nome_processo(p["args"])
            por_nome[n] = por_nome.get(n, 0) + (p.get(chave) or 0) / escala
        return [{"nome": n, "valor": round(v)} for n, v in sorted(por_nome.items(), key=lambda kv: -kv[1])[:3] if round(v) > 0]
    soma = lambda ps, k, escala=1: round(sum(p.get(k) or 0 for p in ps) / escala)
    return {"orq_cpu": soma(dentro, "cpu"), "fora_cpu": soma(fora, "cpu"), "orq_rss_mb": soma(dentro, "rss", 1024), "fora_rss_mb": soma(fora, "rss", 1024),
            "fora_por_cpu": maiores("cpu", 1), "fora_por_mem": maiores("rss", 1024)}


def maquina_nivel(leitura=None, cfg=None):
    """(`alta` | `ok`, motivo): pressão alta se a memória livre, o percentual livre ou a carga passam do limite de maquina.json. Leitura ausente não conta."""
    l, c = leitura if leitura is not None else maquina_ler(), cfg or maquina_cfg()
    motivos = []
    if isinstance(l.get("mem_livre_mb"), (int, float)) and l["mem_livre_mb"] < c["mem_livre_min_mb"]:
        motivos.append(f"memória livre {l['mem_livre_mb']:g} MB (mínimo {c['mem_livre_min_mb']:g} MB)")
    if isinstance(l.get("livre_pct"), (int, float)) and l["livre_pct"] < c["livre_pct_min"]:
        motivos.append(f"memória livre {l['livre_pct']:g}% (mínimo {c['livre_pct_min']:g}%)")
    if isinstance(l.get("carga"), (int, float)) and l["carga"] > c["carga_max"]:
        motivos.append(f"carga {l['carga']:g} (máximo {c['carga_max']:g})")
    return ("alta", "; ".join(motivos)) if motivos else ("ok", None)


def maquina_causa(leitura=None, cfg=None):
    """(`orq` | `fora` | None, culpados): de quem é a pressão alta. `orq` se a maior parte (metade ou mais) da CPU, no motivo de carga, ou da RSS, no de memória,
    é dos processos do orq; `fora` se não, com os maiores de fora ("mds_stores 146%, OrbStack 77%"). None sem pressão ou sem a amostra de processos
    (origem desconhecida vale como do orq: o aviso segue sugerindo pausar)."""
    l, c = leitura if leitura is not None else maquina_ler(), cfg or maquina_cfg()
    o = l.get("origem")
    if not o:
        return None, None
    cpu_alta = isinstance(l.get("carga"), (int, float)) and l["carga"] > c["carga_max"]
    mem_alta = (isinstance(l.get("mem_livre_mb"), (int, float)) and l["mem_livre_mb"] < c["mem_livre_min_mb"]) or (isinstance(l.get("livre_pct"), (int, float)) and l["livre_pct"] < c["livre_pct_min"])
    if not (cpu_alta or mem_alta):
        return None, None
    if (cpu_alta and o["orq_cpu"] >= o["fora_cpu"]) or (mem_alta and o["orq_rss_mb"] >= o["fora_rss_mb"]):
        return "orq", None
    culpados = [f"{x['nome']} {x['valor']}%" for x in o["fora_por_cpu"]] if cpu_alta else []
    culpados += [f"{x['nome']} {x['valor']} MB" for x in o["fora_por_mem"]] if mem_alta else []
    return "fora", ", ".join(culpados) or "processos de fora do orq"


def maquina_piso(leitura, cfg):
    """O motivo de a memória livre estar abaixo do piso de segurança (`mem_piso_mb`), o limite duro que vale até para o Run isento; ou None."""
    livre = leitura.get("mem_livre_mb")
    if isinstance(livre, (int, float)) and livre < cfg["mem_piso_mb"]:
        return f"memória livre {livre:g} MB abaixo do piso de segurança ({cfg['mem_piso_mb']:g} MB), que vale até para o Run isento"
    return None


def run_isento(run, cfg=None):
    """O Run casa com um padrão de `runs_isentos` (glob, sem diferenciar maiúsculas, contra o id ou o objetivo)? Run que o Orca não mostra não é isento."""
    import fnmatch  # só aqui: fora do topo para não pesar nos hooks
    padroes = (cfg or maquina_cfg())["runs_isentos"]
    if not run or not padroes:
        return False
    try:
        objetivo = (orca("run-show", "--id", run).get("run") or {}).get("objective") or ""
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        objetivo = ""
    return any(fnmatch.fnmatch(x.lower(), p.lower()) for p in padroes for x in (run, objetivo) if x)


def modelo_caro(modelo, cfg=None):
    """O modelo casa com algum padrão glob de `modelos_caros` (sem diferenciar maiúsculas). Modelo desconhecido não é caro."""
    import fnmatch  # só aqui: fora do topo para não pesar nos hooks
    return bool(modelo) and any(fnmatch.fnmatch(str(modelo).lower(), p.lower()) for p in (cfg or maquina_cfg())["modelos_caros"])


def maquina_ocupacao():
    """{vivos: {dispatch: modelo}, dispatched: {dispatch}}: os workers com terminal vivo no Orca (todos os Runs) e os dispatches ainda `dispatched`.

    O modelo vem do evento de despacho ou de retomada e, na falta dele, do worker-show; sem lista de terminais confiável todo `dispatched` conta como vivo."""
    terminais = _terminais_vivos()
    ws = [w for w in _workers_todos() if w.get("dispatchStatus") == "dispatched"]
    vivos = [w for w in ws if terminais is None or w.get("agentTerminalHandle") in terminais]
    modelos = {e["dispatch"]: e["modelo"] for e in read_events() if e.get("tipo") in ("despacho", "retomada") and e.get("dispatch") and e.get("modelo")}
    faltam = [w for w in vivos if w["dispatchId"] not in modelos]
    if faltam:
        modelos |= {d: x["modelo"] for d, x in _detalhes(faltam).items() if x.get("modelo")}
    return {"vivos": {w["dispatchId"]: modelos.get(w["dispatchId"]) for w in vivos}, "dispatched": {w["dispatchId"] for w in ws}}


def maquina_vaga(modelo, ocup=None, cfg=None, isento=False):
    """O motivo de não caber mais um worker deste modelo (workers vivos no teto, ou caros no teto), ou None se cabe. `isento`: Run isento, sem o teto de workers."""
    cfg, ocup = cfg or maquina_cfg(), ocup or maquina_ocupacao()
    n = len(ocup["vivos"])
    if n >= cfg["max_workers"] and not isento:
        return f"{n}/{cfg['max_workers']:g} workers vivos"
    caros = sum(modelo_caro(m, cfg) for m in ocup["vivos"].values())
    if modelo_caro(modelo, cfg) and caros >= cfg["max_caros"]:
        return f"{caros}/{cfg['max_caros']:g} workers caros ({modelo} é caro)"
    return None


def maquina_barra_item(modelo, run, ocup, cfg, pressao, leitura):
    """O motivo de o worker deste Run não subir agora, ou None. A pressão da máquina vem antes das vagas; o Run isento (`runs_isentos`) passa pela pressão e
    pelo teto de workers, e só para no teto de caros (limite de custo), no piso de memória e no max_e2e (a fila global do E2E)."""
    causa = maquina_causa(leitura, cfg) if pressao[0] == "alta" else (None, None)
    motivo = (f"máquina sob pressão: {pressao[1]}" + (f"; vem de fora do orq ({causa[1]})" if causa[0] == "fora" else "")) if pressao[0] == "alta" else maquina_vaga(modelo, ocup, cfg)
    if not motivo or not run_isento(run, cfg):
        return motivo
    return maquina_vaga(modelo, ocup, cfg, isento=True) or maquina_piso(leitura, cfg)


def maquina_barra(modelo, ocup=None, cfg=None, run=None):
    """O motivo de o worker não poder subir agora, ou None. A pressão da máquina vem antes das vagas; Run isento: ver maquina_barra_item."""
    cfg = cfg or maquina_cfg()
    leitura = maquina_ler()
    return maquina_barra_item(modelo, run, ocup, cfg, maquina_nivel(leitura, cfg), leitura)


def _fila_despacho_mut(fn):
    """Lê a fila de despacho, aplica fn(itens) e grava, sob o fila-despacho.lock. Devolve o que fn devolveu."""
    with _trava("fila-despacho.lock"):
        d = _dict(_read_json(_path(FILA_DESPACHO)))
        itens = [i for i in d.get("itens") or [] if isinstance(i, dict)]
        r = fn(itens)
        _write_json(_path(FILA_DESPACHO), {"itens": itens}, indent=2)
        return r


def fila_despacho_itens():
    """Os itens da fila na ordem em que sobem: prioridade (1 antes de 3) e, no empate, o mais antigo."""
    itens = [i for i in _dict(_read_json(_path(FILA_DESPACHO))).get("itens") or [] if isinstance(i, dict)]
    return sorted(itens, key=lambda i: (i.get("prioridade") or 2, i.get("ts") or ""))


def fila_despacho_add(item, motivo):
    """Põe o pedido na fila e devolve (item, posição, já estava). Despacho do mesmo ticket (ou do mesmo título no mesmo Run) não entra duas vezes."""
    import uuid
    novo = {"id": "fd" + uuid.uuid4().hex[:6], "ts": now(), "motivo": motivo, "falhas": 0, **item}

    def poe(itens):
        igual = next((i for i in itens if i.get("tipo") == novo["tipo"] and (i.get("dispatch") == novo.get("dispatch") if novo["tipo"] == "retomada" else
                                                                           (i.get("run"), i.get("ticket") or i.get("titulo")) == (novo["run"], novo.get("ticket") or novo.get("titulo")))), None)
        if igual:
            return igual, True
        itens.append(novo)
        return novo, False
    it, ja = _fila_despacho_mut(poe)
    if not ja:
        append_event({"tipo": "despacho_fila", "op": "entrou", "id": it["id"], "fila_tipo": it["tipo"], "titulo": it.get("titulo"), "prioridade": it.get("prioridade"), "motivo": motivo})
    ordem = [i["id"] for i in fila_despacho_itens()]
    return it, ordem.index(it["id"]) + 1, ja


def fila_despacho_rm(id_, op="removido", **extra):
    """Tira o item da fila (e a cópia do spec). False se o id não está lá."""
    achado = _fila_despacho_mut(lambda itens: next((itens.pop(n) for n, i in enumerate(itens) if i.get("id") == id_), None))
    if achado:
        with contextlib.suppress(OSError):
            os.remove(_path(os.path.join(FILA_DESPACHO_SPECS, id_ + ".md")))
        append_event({"tipo": "despacho_fila", "op": op, "id": id_, "fila_tipo": achado.get("tipo"), "titulo": achado.get("titulo"), **extra})
    return achado


def _enfileirar_despacho(motivo, run, titulo, spec, modelo, effort, worktree, name, base_branch, entrada, tk, prio, agente):
    """O despacho que não coube: guarda o pedido (o spec numa cópia em ORQ_HOME) e devolve a resposta do `orq despachar` no lugar dos ids do worker."""
    import uuid
    id_ = "fd" + uuid.uuid4().hex[:6]
    item = {"id": id_, "tipo": "despacho", "run": run, "titulo": titulo, "modelo": modelo, "effort": effort, "agente": agente, "prioridade": prio,
            **{k: v for k, v in (("worktree", worktree), ("nome", name), ("base_branch", base_branch), ("entrada", entrada), ("ticket", tk and tk["num"])) if v}}
    if os.environ.get("ORQ_MATE"):  # o Run é do mate e só o terminal dele o comanda: o gerente drena com esse handle (ticket 80)
        item.update(coord=os.environ.get("ORCA_TERMINAL_HANDLE"), mate=os.environ["ORQ_MATE"])
    copia = _path(os.path.join(FILA_DESPACHO_SPECS, id_ + ".md"))
    if spec is not None:
        os.makedirs(os.path.dirname(copia), exist_ok=True)
        with open(copia, "w", encoding="utf-8") as f:
            f.write(spec)
        item["spec_arquivo"] = copia
    it, pos, ja = fila_despacho_add(item, motivo)
    if ja and spec is not None:
        os.remove(copia)
    return {"estado": "enfileirado", "fila": it["id"], "posicao": pos, "run": run, "prioridade": prio, "motivo": motivo,
            "aviso": f"{'já estava' if ja else 'entrou'} na fila de despacho (posição {pos}): {motivo}. O gerente sobe quando abrir vaga; veja com orq fila-despacho lista"}


def _ocupar(ocup, dispatch, modelo):
    """Conta na ocupação o worker que acabou de subir, para o próximo item do mesmo lote ver a vaga já tomada."""
    ocup["vivos"][dispatch] = modelo


@contextlib.contextmanager
def _como_coordenador(it=None):
    """O painel roda no terminal do agent manager; o despacho precisa do handle do coordenador (é ele que comanda os Runs, e o orca() troca pelo do gerente).
    O item que um mate enfileirou (`mate`, `coord`) sobe como o mate: o Run é dele."""
    g, antes = _gerente_cfg(), {k: os.environ.get(k) for k in ("ORCA_TERMINAL_HANDLE", "ORQ_MATE")}
    it = it or {}
    if it.get("mate") and it.get("coord"):  # o terminal de agora: o mate pode ter caído e voltado depois de enfileirar
        os.environ.update(ORCA_TERMINAL_HANDLE=_dict(_mates().get(it["mate"])).get("terminal") or it["coord"], ORQ_MATE=it["mate"])
    elif g.get("coordenador"):
        os.environ["ORCA_TERMINAL_HANDLE"] = g["coordenador"]
    try:
        yield
    finally:
        for k, v in antes.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _sobe_da_fila(it):
    """Sobe um item da fila. Devolve a linha do painel; levanta SemVaga, ValueError ou RuntimeError se não subiu (o item volta)."""
    if it["tipo"] == "despacho":
        with _como_coordenador(it):
            r = despachar(it["run"], it.get("titulo") if not it.get("ticket") else None, it.get("spec_arquivo"), it["modelo"], it["effort"], it.get("worktree"), it.get("nome"),
                          it.get("base_branch"), it.get("entrada"), it.get("ticket"), it["prioridade"], it.get("agente") or "claude", _drenando=True)
        return f"fila: {it['titulo']} subiu ({r.get('dispatchId')})"
    d = it["dispatch"]
    cp = _checkpoint(d)
    linha = {k: it.get(k) for k in ("task", "run", "titulo", "agente", "modelo", "effort", "sessao", "cwd", "terminal")} | {"dispatch": d}
    r = _subir_sessao(linha, it["sessao"], it.get("modelo"), cp, f"suba outro worker do spec da task com: orq relancar {d} --nota 'a sessão não pôde ser retomada'",
                      msg_continuar(it.get("coord"), it["task"], d, it["run"]), it.get("agente") or "claude", it.get("effort"))
    return f"fila: {it['titulo']} retomado: {r['estado']}" + (f" ({r['aviso']})" if r.get("aviso") else "")


def despacho_drenar(cfg=None, agora=None, so_isentos=False):
    """Uma volta da fila: sobe, por prioridade, o primeiro item que cabe (um por volta, para a memória mostrar o que o anterior custou antes do próximo).

    Item de modelo caro com o teto de caros cheio fica para trás e o barato de prioridade menor sobe. Retomada cujo dispatch já terminou ou voltou sai da
    fila. O que o uso do plano ou o modo noite seguram espera ESPERA_SEGURADO_S; erro de outra causa conta em `falhas` e, na FALHAS_FILA-ésima, o item sai.
    Item de Run isento (`runs_isentos`) sobe sem o teto de workers, parado só pelo teto de caros e pelo piso de memória; `so_isentos` (pressão alta) tenta só esses."""
    cfg, agora = cfg or maquina_cfg(), agora or time.time()
    itens = fila_despacho_itens()
    if not itens:
        return []
    ocup, linhas = maquina_ocupacao(), []
    for it in itens:
        if it.get("nao_antes", 0) > agora:
            continue
        if it["tipo"] == "retomada" and (it["dispatch"] in ocup["vivos"] or it["dispatch"] not in ocup["dispatched"]):
            fila_despacho_rm(it["id"], "saiu", motivo="o dispatch já terminou ou já voltou")
            linhas.append(f"fila: {it['titulo']} saiu (o dispatch já terminou ou já voltou)")
            continue
        isento = (so_isentos or maquina_vaga(it.get("modelo"), ocup, cfg)) and run_isento(it.get("run"), cfg)  # só pergunta ao Orca quando há o que isentar
        if so_isentos and not isento:
            continue
        if isento and (maquina_vaga(it.get("modelo"), ocup, cfg, isento=True) or maquina_piso(maquina_ler(), cfg)) or not isento and maquina_vaga(it.get("modelo"), ocup, cfg):
            continue
        try:
            linhas.append(_sobe_da_fila(it))
            fila_despacho_rm(it["id"], "subiu")
            return linhas
        except SemVaga:
            continue
        except (ValueError, RuntimeError, subprocess.TimeoutExpired) as e:
            segurado = str(e).startswith(("uso do plano", "modo noite"))
            falhas = it.get("falhas", 0) + (0 if segurado else 1)
            if falhas >= FALHAS_FILA:
                fila_despacho_rm(it["id"], "desistiu", erro=str(e))
                linhas.append(f"fila: {it['titulo']} saiu da fila depois de {falhas} erros ({e}); despache de novo à mão")
                continue
            _fila_despacho_mut(lambda xs, it=it, falhas=falhas, segurado=segurado: [x.update(falhas=falhas, nao_antes=agora + ESPERA_SEGURADO_S if segurado else 0)
                                                                                      for x in xs if x["id"] == it["id"]])
            linhas.append(f"fila: {it['titulo']} segue esperando ({e})")
    return linhas


def _candidato_a_pausar():
    """O worker vivo de menor prioridade (prioridade mais alta em número; no empate o primeiro), fora os poupados por estarem em verificação final; ou None."""
    ags = agentes()
    escolhidos, _ = _lista_pausa(ags, read_events(), set(), 1, _dict(_cursor_ro().get("pausados")))
    return max(escolhidos, key=lambda a: a["prioridade"], default=None)


def _maquina_avisar(motivo, na_fila, cfg, causa=(None, None)):
    """Pressão alta: digita no coordenador, uma vez por episódio, que o gerente parou de subir worker e qual pausar. Com `pausar_sob_pressao` o gerente pausa esse
    worker sozinho (o orq pausar espera até ORQ_PAUSA_ESPERA_S pelo PAUSA.md: a volta do painel demora esse tempo). Coordenador ocupado: a próxima volta tenta.
    Carga que vem de fora do orq (`causa` fora): o aviso lista os maiores de fora, segura o despacho e não sugere nem faz pausa, que não aliviaria nada."""
    g = _gerente_cfg()
    if not g or not g.get("coordenador") or _cursor_ro().get("maquina_aviso"):
        return []
    fora = causa[0] == "fora"
    alvo = None
    with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired, ValueError, KeyError):
        alvo = None if fora else _candidato_a_pausar()
    dica = f" Para abrir folga: orq pausar {alvo['task']} (P{alvo['prioridade']} {alvo.get('titulo') or alvo['task']})." if alvo else ""
    filhos = {}
    with contextlib.suppress(RuntimeError, subprocess.TimeoutExpired, ValueError, KeyError):
        filhos = _filhos_por_task()
    peso = f" Processos em segundo plano: {', '.join(f'{t} {n}' for t, n in filhos.items())}." if filhos else ""
    texto = (f"orq: máquina sob pressão ({motivo}). A carga vem de fora do orq ({causa[1]}): pausar worker não alivia. O gerente segura despacho novo ({na_fila} na fila de despacho)."
             if fora else f"orq: máquina sob pressão ({motivo}). O gerente parou de subir worker ({na_fila} na fila de despacho).{peso}{dica}")
    if avisa_coordenador(g["coordenador"], texto) not in ("enviado", "adiado"):
        return []
    _cursor_mut(lambda c: c.__setitem__("maquina_aviso", {"ts": now(), "motivo": motivo}))
    append_event({"tipo": "maquina_aviso", "motivo": motivo, "na_fila": na_fila, **({"causa": causa[0]} if causa[0] else {}), **({"sugerido": alvo["task"]} if alvo else {}),
                  **({"filhos": filhos} if filhos else {})})
    linhas = [f"máquina sob pressão ({motivo}): coordenador avisado, nada sobe"]
    if cfg["pausar_sob_pressao"] and alvo:
        r = pausar((alvo["task"],))
        linhas += [f"pausa automática: {x['task']} {x['estado']}" for x in r["pausados"]]
    return linhas


def maquina_volta():
    """O trabalho da fila de despacho numa volta do painel do agent manager: pressão alta avisa o coordenador e só o Run isento sobe; senão sobe um item que caiba."""
    cfg = maquina_cfg()
    leitura = maquina_ler()
    nivel, motivo = maquina_nivel(leitura, cfg)
    if nivel == "alta":
        return _maquina_avisar(motivo, len(fila_despacho_itens()), cfg, maquina_causa(leitura, cfg)) + despacho_drenar(cfg, so_isentos=True)
    if _cursor_ro().get("maquina_aviso"):
        _cursor_mut(lambda c: c.pop("maquina_aviso", None))
    return despacho_drenar(cfg)


def texto_maquina(cfg=None, leitura=None, ocup=None):
    """As linhas do `orq maquina`: o que a máquina tem, o que o orq decidiria agora para um despacho barato e para um caro, as vagas e a fila."""
    cfg, leitura = cfg or maquina_cfg(), leitura if leitura is not None else maquina_ler()
    nivel, motivo = maquina_nivel(leitura, cfg)
    rss = _dict(leitura.get("rss_mb"))
    ls = [f"memória livre {leitura.get('mem_livre_mb')} MB ({leitura.get('livre_pct')}% livre), carga {leitura.get('carga')} em {leitura.get('ncpu')} CPUs",
          "RSS: " + ", ".join(f"{k} {rss.get(k, 0)} MB" for k in PROCESSOS_PESADOS)]
    if o := leitura.get("origem"):
        ls.append(f"carga por dono: orq {o['orq_cpu']}% de CPU e {o['orq_rss_mb']} MB, fora do orq {o['fora_cpu']}% e {o['fora_rss_mb']} MB"
                  + (f" (maiores de fora: {', '.join(f'{x['nome']} {x['valor']}%' for x in o['fora_por_cpu'])})" if o["fora_por_cpu"] else ""))
    if ocup is not None:
        caros = sum(modelo_caro(m, cfg) for m in ocup["vivos"].values())
        ls.append(f"vagas: {len(ocup['vivos'])}/{cfg['max_workers']:g} ocupadas, {max(cfg['max_workers'] - len(ocup['vivos']), 0):g} livres; caros {caros}/{cfg['max_caros']:g}; E2E máx {cfg['max_e2e']:g} (a fila do E2E serializa); fila de despacho {len(fila_despacho_itens())}")

    def decide(modelo):
        m = f"pressão alta: {motivo}" if nivel == "alta" else maquina_vaga(modelo, ocup, cfg) if ocup is not None else None
        return f"fila ({m})" if m else "sobe"
    ls.append(f"decisão agora: despacho de modelo barato {decide('barato')}; de modelo caro {decide(next(iter(cfg['modelos_caros']), 'caro').replace('*', 'x'))}")
    return ls


def maquina_painel(agentes_=None):
    """O que o painel e o digest mostram da máquina, só de arquivo: {max_workers, max_caros, ocupadas, livres, caros, fila: [{id, tipo, titulo, prioridade, modelo}]}."""
    cfg = maquina_cfg()
    vivos = [a for a in agentes_ or [] if isinstance(a, dict) and a.get("estado") in VIVOS_MAQUINA]
    return {"max_workers": cfg["max_workers"], "max_caros": cfg["max_caros"], "ocupadas": len(vivos), "livres": max(cfg["max_workers"] - len(vivos), 0),
            "caros": sum(modelo_caro(a.get("modelo"), cfg) for a in vivos),
            "fila": [{k: i.get(k) for k in ("id", "tipo", "titulo", "prioridade", "modelo")} for i in fila_despacho_itens()]}


def linha_maquina():
    """A linha do `orq status`: vagas (do aberto.json, sem falar com o Orca), pressão e fila. Vazia sem nada a dizer (sem worker vivo, sem fila, sem pressão)."""
    p = maquina_painel(_dict(_read_json(_path("aberto.json"))).get("agentes"))
    nivel, motivo = maquina_nivel()
    if not p["ocupadas"] and not p["fila"] and nivel == "ok":
        return ""
    txt = f"Máquina: {p['ocupadas']}/{p['max_workers']:g} workers ({p['caros']}/{p['max_caros']:g} caros), {p['livres']:g} vagas livres"
    txt += f"; PRESSÃO ALTA: {motivo}" if nivel == "alta" else ""
    if p["fila"]:
        txt += f"; {len(p['fila'])} na fila de despacho: " + ", ".join(f"P{i.get('prioridade') or 2} {_cita(i.get('titulo') or '?', 30)}" for i in p["fila"][:3]) + (f" +{len(p['fila']) - 3}" if len(p["fila"]) > 3 else "")
    return txt


# ---------- retomar depois de uma queda (ticket 48) ----------

RETOMAR_ESPERA_S = float(os.environ.get("ORQ_RETOMAR_ESPERA_S") or 20)  # quanto esperar a sessão retomada mostrar atividade antes de dizer que não voltou
MSG_CONTINUE = ("Continue de onde parou. A sessão caiu por uma queda de energia; o terminal e o handle do Orca mudaram. Antes: confira git status e o estado da "
                "sua worktree, e se estava esperando uma suíte, rode-a de novo (a fila do E2E foi liberada). Ao terminar, mande o worker_done como antes; se o "
                "Orca recusar por causa do handle novo, escreva o relatório final num arquivo relatorio-final.md na raiz da sua worktree e mostre o caminho no terminal.")
MSG_ESCALAR = ("Nunca deixe uma pergunta ou confirmação esperando no terminal: o coordenador ({coord}) não vê esta tela e o AskUserQuestion é recusado. Dúvida, permissão ou bloqueio: "
               "`orca orchestration send --from \"$ORCA_TERMINAL_HANDLE\" --to run:{run} --type escalation --subject \"<resumo>\" --body \"<detalhes>\" --task-id {task} --dispatch-id {dispatch}`. "
               "Antes de comandos com `rm` e variável, proteja a variável (`\"${{VAR:?}}\"/*`) para o guard do agente não pedir confirmação.")


def msg_continuar(coord, task, dispatch, run):
    """A mensagem de continuação da sessão retomada: o MSG_CONTINUE e o jeito de escalar (handle do coordenador e comando), já que o contexto de despacho se perdeu."""
    return f"{MSG_CONTINUE} {MSG_ESCALAR.format(coord=coord or '<coordenador>', run=run or '<run>', task=task or '<task>', dispatch=dispatch)}"


def _terminal_novo(titulo, comando, cwd=None):
    """`orca terminal create` (na worktree `cwd`, ou na atual) e o handle do terminal novo."""
    args = ["create", "--title", titulo, "--command", comando, *(["--worktree", f"path:{cwd}"] if cwd else [])]
    return orca(*args, area="terminal", timeout=30)["terminal"]["handle"]


def _voltou(handle, agente="claude"):
    """A sessão do terminal mostra atividade: `esc to interrupt` na tela, ou a tela mudou entre duas leituras; a `falha` da tela do agente é que não voltou."""
    antes, fim = None, time.time() + RETOMAR_ESPERA_S
    while True:
        tela = "\n".join(orca("read", "--terminal", handle, "--screen", area="terminal")["terminal"].get("tail") or [])
        if any(f in tela for f in HARNESS[agente]["tela"]["falha"]):
            return False
        if "esc to interrupt" in tela or (antes is not None and tela != antes):
            return True
        antes = tela
        if time.time() >= fim:
            return False
        time.sleep(1)


def _gerente_a_religar(g, meu, vivos):
    """(terminal do gerente morreu, coordenador do gerente.json mudou) quando o gerente.json é deste coordenador ou de um que morreu; senão None (outro coordenador vivo é dono)."""
    if not g or (g.get("coordenador") != meu and g.get("coordenador") in vivos):
        return None
    morto, trocado = g["gerente"] not in vivos, g.get("coordenador") != meu
    return (morto, trocado) if morto or trocado else None


def sessao_do_dispatch(dispatch, agente):
    """{sessao, cwd, transcrito} da sessão do worker no índice de sessões do Orca (`orca search <dispatch>`): o primeiro hit do agente em que o dispatch
    aparece num prompt de usuário (o preâmbulo), não em saída de ferramenta (o coordenador também o cita). {} sem hit ou com o Orca fora."""
    try:
        hits = orca(dispatch, "--agent", agente, "--scope", "conversation", "--limit", "20", area="search", timeout=15).get("hits") or []
    except (RuntimeError, subprocess.TimeoutExpired, ValueError) as e:
        log(f"sessao_do_dispatch {dispatch}: {type(e).__name__}: {e}")
        return {}
    h = next((h for h in hits if _dict(h.get("evidence")).get("role") == "user" and h.get("sessionId")), None)
    return {"sessao": h["sessionId"], "cwd": h.get("cwd"), "transcrito": _dict(h.get("source")).get("filePath")} if h else {}


def _subir_sessao(linha, sessao, modelo, cp, dica, msg=MSG_CONTINUE, agente="claude", effort=None):
    """O resume do agente (HARNESS) da `sessao` num terminal novo na worktree `linha["cwd"]`, evento `retomada` e conferência da tela. Devolve `linha` com o
    estado (retomado, sem_atividade ou falhou)."""
    comando = shlex.join(HARNESS[agente]["resume"](sessao, modelo, effort, msg))
    try:
        novo = _terminal_novo(f"{linha['titulo']} (retomado)", comando, linha["cwd"])
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        return {**linha, "estado": "falhou", "aviso": f"terminal create falhou ({e}); {dica}"}
    append_event({"tipo": "retomada", "dispatch": linha["dispatch"], "task": linha["task"], "run": linha["run"], "terminal": novo, "anterior": linha["terminal"],
                  "sessao": sessao, "cwd": linha["cwd"], "modelo": modelo, "head": cp["head"], "sujo": cp["sujo"]})
    try:
        voltou = _voltou(novo, agente)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        voltou, linha = False, {**linha, "aviso": f"não consegui ler a tela ({e})"}
    return {**linha, "novo": novo, "estado": "retomado" if voltou else "sem_atividade",
            **({} if voltou else {"aviso": f"{linha.get('aviso') or 'a tela não mostra atividade'}; {dica}"})}


def retomar(dry_run=False, run=None):
    """Depois de uma queda: sobe o agent manager que morreu e retoma, em terminal novo, cada worker que ainda não deu worker_done e perdeu o terminal.

    Worker retomado: `orca terminal create --worktree path:<cwd> --command "claude --resume <sessão> --model <modelo> ... '<continue>'"`, com a sessão e o
    cwd que os hooks do worker gravaram em turnos.json (o cwd cai para a worktree do worker-show). O terminal novo vira o do dispatch (evento `retomada`, que
    o _workers_todos aplica) e a tela dele é conferida. O agent manager sobe `painel-agent-manager.sh` numa aba nova e religa os Runs dele a este coordenador.
    `dry_run` só lista. Sem lista de terminais confiável (Orca falhou ou cortou) recusa: sem prova de quem morreu não se sobe nada."""
    meu = os.environ.get("ORCA_TERMINAL_HANDLE")
    if not meu:
        raise ValueError("fora de um terminal do Orca (sem ORCA_TERMINAL_HANDLE)")
    vivos = _terminais_vivos()
    if vivos is None:
        raise ValueError("o Orca não listou os terminais: sem prova de quem morreu, nada foi retomado")
    g = _gerente_cfg()
    plano = _gerente_a_religar(g, meu, vivos)
    res = {"gerente": None, "workers": []}
    if plano:
        morto, _ = plano
        res["gerente"] = {"terminal": g["gerente"], "runs": g["runs"], "estado": "a_subir" if morto else "a_religar"}
        if not dry_run:
            novo = g["gerente"]
            if morto:
                novo = _terminal_novo("agent manager (retomado)", f"sh {shlex.quote(_path('painel-agent-manager.sh'))}")
            gerente_ligar(novo, g["runs"])
            res["gerente"] = {**res["gerente"], "novo": novo, "estado": "religado"}
    pausados = _dict(_cursor_ro().get("pausados"))  # pausados pelo orçamento de uso voltam com `retomar --pausados`, não aqui
    hib = _hibernados()  # hibernados também não: o terminal fechado foi de propósito, e quem os acorda é o orq acordar (ou o steer, o responder, a pendência, o merge)
    cand = [w for w in _workers_todos(run) if w.get("dispatchStatus") == "dispatched" and w.get("agentTerminalHandle") not in vivos and w.get("dispatchId") not in pausados
            and w.get("dispatchId") not in hib]
    det, turnos, eventos = _detalhes(cand), _turnos_ro(), read_events()
    despachos = {e.get("dispatch"): e for e in eventos if e.get("tipo") == "despacho"}
    por_d, subir = {}, []
    for w in cand:
        d = w["dispatchId"]
        t = _dict(turnos.get(d))
        dd, ev = det.get(d) or {}, despachos.get(d) or {}
        modelo, effort = dd.get("modelo") or ev.get("modelo"), dd.get("effort") or ev.get("effort")
        agente = dd.get("agente") or ev.get("agente") or t.get("harness") or "claude"
        titulo = dd.get("titulo") or ev.get("titulo") or d
        cp = _checkpoint(d)  # do firstmate: a worktree tem de existir, e o head e os arquivos sujos ficam registrados antes de subir o agente
        if not t.get("sessao") and agente in HARNESS:
            t = {**t, **sessao_do_dispatch(d, agente)}  # o hook de turno não viu este worker: o índice de sessões do Orca pode ter visto
        cwd = t.get("cwd") or cp["caminho"]
        linha = {"dispatch": d, "task": w.get("taskId"), "run": w.get("runId"), "titulo": titulo, "agente": agente, "modelo": modelo, "effort": effort,
                 "sessao": t.get("sessao"), "cwd": cwd, "terminal": w.get("agentTerminalHandle")}
        dica = f"suba outro worker do spec da task com: orq relancar {d} --nota 'a sessão não pôde ser retomada'"  # do firstmate: sem sessão, o brief em disco é a instrução durável
        if not linha["sessao"] or not cwd:
            por_d[d] = {**linha, "estado": "sem_sessao", "aviso": f"sem session_id ou cwd gravado (o hook de turno não viu este worker): {dica}"}
        elif not os.path.isdir(cwd):
            por_d[d] = {**linha, "estado": "sem_worktree", "aviso": f"a pasta {cwd} não existe: nada foi subido"}
        else:
            subir.append((prioridade_de(eventos, w.get("taskId"), d, titulo), linha, cp, dica))
    if subir:  # o orçamento da máquina (ticket 79): sobe por prioridade até o teto, o resto vai para a fila de despacho
        cfg, ocup, leitura = maquina_cfg(), maquina_ocupacao(), maquina_ler()
        pressao = maquina_nivel(leitura, cfg)
        for prio, linha, cp, dica in sorted(subir, key=lambda x: x[0]):  # estável: na mesma prioridade vale a ordem do Orca
            d = linha["dispatch"]
            motivo = maquina_barra_item(linha["modelo"], linha["run"], ocup, cfg, pressao, leitura)
            if motivo:
                if not dry_run:
                    fila_despacho_add({"tipo": "retomada", "dispatch": d, "task": linha["task"], "run": linha["run"], "titulo": linha["titulo"], "agente": linha["agente"],
                                       "modelo": linha["modelo"], "effort": linha["effort"], "sessao": linha["sessao"], "cwd": linha["cwd"], "terminal": linha["terminal"],
                                       "prioridade": prio, "coord": meu}, motivo)
                por_d[d] = {**linha, "prioridade": prio, "estado": "a_enfileirar" if dry_run else "enfileirado", "aviso": f"{motivo}; o gerente sobe quando abrir vaga (orq fila-despacho lista)"}
                continue
            _ocupar(ocup, d, linha["modelo"])
            if dry_run:
                por_d[d] = {**linha, "prioridade": prio, "estado": "a_retomar"}
            else:
                por_d[d] = {**_subir_sessao(linha, linha["sessao"], linha["modelo"], cp, dica, msg_continuar(meu, linha["task"], d, linha["run"]), linha["agente"], linha["effort"]), "prioridade": prio}
    res["workers"] = [por_d[w["dispatchId"]] for w in cand]
    for g, m in _mates().items():  # o mate que caiu volta pela sessão dele (ticket 80); o pedido sem resposta continua com o prazo
        if _dict(m).get("terminal") not in vivos and _dict(m).get("sessao") and g in grupos():
            try:
                res.setdefault("mates", []).append({"grupo": g, "estado": "a_retomar"} if dry_run else {**mate_abrir(g), "estado": "retomado"})
            except (ValueError, RuntimeError, subprocess.TimeoutExpired) as e:
                res.setdefault("mates", []).append({"grupo": g, "estado": "falhou", "aviso": str(e)})
    return res


def texto_retomar(res):
    """Uma linha por item, dizendo o que foi (ou seria) feito."""
    g, ls = res["gerente"], []
    if g:
        ls.append(f"gerente {g['terminal']}: {g['estado']}" + (f" -> {g['novo']}" if g.get("novo") else "") + f" ({len(g['runs'])} Run(s))")
    for w in res["workers"]:
        ls.append(f"{w['dispatch']} {w['titulo']}: {w['estado']}" + (f" -> {w['novo']}" if w.get("novo") else "") + (f" ({w['aviso']})" if w.get("aviso") else ""))
    for m in res.get("mates") or []:
        ls.append(f"mate {m['grupo']}: {m['estado']}" + (f" -> {m['terminal']}" if m.get("terminal") else "") + (f" ({m['aviso']})" if m.get("aviso") else ""))
    return "\n".join(ls) or "nada a retomar"


# ---------- orçamento de uso do plano e pausa por prioridade (ticket 51) ----------

# Fonte do uso: o Claude Code passa `rate_limits` ({five_hour, seven_day}: {used_percentage, resets_at}) no stdin do statusline, e o wrapper do HUD do
# OMC (~/.claude/hud/omc-hud-cache.sh) grava esse JSON em hud/cache/stdin.<sessão>.json a cada quadro. É o mesmo número do rodapé (5h:…% wk:…%), o
# mesmo para todas as sessões da conta, e ler o arquivo não chama rede nem o Orca: serve aos hooks.
HUD_CACHE = os.environ.get("ORQ_HUD_CACHE") or os.path.expanduser("~/.claude/hud/cache")
USO_FRESCO_S = 1800  # cache mais velho que isto não diz nada do uso de agora (nenhuma sessão desenhou o rodapé)
USO = "uso.json"  # limiares, por cima de USO_PADRAO: {"semana_avisa": 85, "semana_pausa": 92, "cinco_h": 90}
USO_PADRAO = {"semana_avisa": 85, "semana_pausa": 92, "cinco_h": 90}  # cinco_h: avisa e segura despacho novo até a janela virar
PAUSA_ESPERA_S = float(os.environ.get("ORQ_PAUSA_ESPERA_S") or 300)  # quanto esperar cada worker escrever o PAUSA.md
PAUSA_POLL_S = float(os.environ.get("ORQ_PAUSA_POLL_S") or 2)
MSG_PAUSA = ("Pausa do coordenador (limite de uso do plano). Em até 3 linhas, escreva em PAUSA.md na raiz da sua worktree onde parou e o próximo passo, "
             "pare qualquer E2E que tenha subido e encerre o turno. Não mande worker_done: você volta depois nesta mesma sessão.")
MSG_VOLTA = ("O uso do plano voltou ao normal e o coordenador retomou você. Leia o PAUSA.md na raiz da sua worktree, apague-o e siga do próximo passo. "
             "Ao terminar, mande o worker_done como antes; se o Orca recusar por causa do handle novo, escreva o relatório final num arquivo "
             "relatorio-final.md na raiz da sua worktree e mostre o caminho no terminal.")
_FRENTE_ALTA = re.compile(r"seguran|security|produ[cç][aã]o", re.I)
_FRENTE_BAIXA = re.compile(r"failover|diagn[oó]stic|painel|digest", re.I)
_FASE_FINAL = re.compile(r"review|verif|final", re.I)
_FASE_INICIAL = re.compile(r"investig", re.I)


def _janela_uso(j, agora):
    """(percentual, reset) de uma janela do rate_limits; janela que já virou vale 0; sem número, (None, None)."""
    p, r = _dict(j).get("used_percentage"), _dict(j).get("resets_at")
    if isinstance(p, bool) or not isinstance(p, (int, float)):
        return None, None
    return (0 if isinstance(r, (int, float)) and r <= agora else p), r


def uso_plano(agora=None, agente="claude"):
    """{semana, semana_reset, cinco_h, cinco_h_reset} do plano do `agente` (percentuais 0-100, resets em epoch), ou None sem número.

    Claude: o quadro mais novo do HUD (só lê arquivo, sem rede nem Orca); sem quadro fresco, o `orca account list`. Codex: o `orca account list`,
    que lê a conta de cada harness (as cotas são separadas)."""
    agora = agora or time.time()
    if agente != "claude":
        return _uso_orca(agente, agora)
    return _uso_hud(agora) or _uso_orca(agente, agora)


def _uso_orca(agente, agora):
    """O `rateLimits.<agente>` do `orca account list`: `weekly` é a semana e `session` a janela de 5 h (resetsAt em ms). None sem número ou com o Orca fora."""
    try:
        rl = _dict(_dict(orca("list", area="account", timeout=5).get("rateLimits")).get(agente))
    except (RuntimeError, subprocess.TimeoutExpired, ValueError) as e:
        log(f"uso {agente}: {type(e).__name__}: {e}")
        return None

    def janela(j):
        p, r = _dict(j).get("usedPercent"), _dict(j).get("resetsAt")
        if isinstance(p, bool) or not isinstance(p, (int, float)):
            return None, None
        r = r / 1000 if isinstance(r, (int, float)) else None
        return (0 if r and r <= agora else p), r

    (s, sr), (c, cr) = janela(rl.get("weekly")), janela(rl.get("session"))
    return {"semana": s, "semana_reset": sr, "cinco_h": c, "cinco_h_reset": cr} if s is not None or c is not None else None


def _uso_hud(agora):
    """O `rate_limits` do quadro mais novo do HUD do OMC, ou None sem quadro fresco."""
    try:
        arqs = sorted(glob.glob(os.path.join(HUD_CACHE, "stdin.*.json")), key=os.path.getmtime, reverse=True)
    except OSError:
        return None
    for f in arqs:
        try:
            if agora - os.path.getmtime(f) > USO_FRESCO_S:
                return None  # ordenado: os demais são mais velhos
        except OSError:
            continue
        rl = _dict(_dict(_read_json(f)).get("rate_limits"))
        (s, sr), (c, cr) = _janela_uso(rl.get("seven_day"), agora), _janela_uso(rl.get("five_hour"), agora)
        if s is not None or c is not None:
            return {"semana": s, "semana_reset": sr, "cinco_h": c, "cinco_h_reset": cr}
    return None


def _duracao(s):
    """`1d8h` ou `0h19m`: o que falta para a janela virar."""
    s = max(int(s), 0)
    return f"{s // 86400}d{s % 86400 // 3600}h" if s >= 86400 else f"{s // 3600}h{s % 3600 // 60:02d}m"


def uso_nivel(uso, agora=None):
    """(nivel, motivo, reset): `pausa` (semana acima do limiar de pausa) e `segura` (5 h acima do limiar) recusam despacho, `avisa` só avisa, `ok` ou
    `desconhecido` (sem quadro fresco) não fazem nada. `reset` é o da janela que decidiu, para o aviso valer uma vez por janela."""
    if not uso:
        return "desconhecido", None, None
    agora = agora or time.time()
    cfg = {**USO_PADRAO, **{k: v for k, v in _dict(_read_json(_path(USO))).items() if k in USO_PADRAO and isinstance(v, (int, float))}}

    def txt(nome, limiar):
        r = uso[f"{nome}_reset"]
        return f"{'semana' if nome == 'semana' else 'janela de 5 h'} em {uso[nome]:g}% (limiar {limiar:g}%)" + (f", vira em {_duracao(r - agora)}" if r else "")

    s, c = uso["semana"], uso["cinco_h"]
    if s is not None and s >= cfg["semana_pausa"]:
        return "pausa", txt("semana", cfg["semana_pausa"]), uso["semana_reset"]
    if c is not None and c >= cfg["cinco_h"]:
        return "segura", txt("cinco_h", cfg["cinco_h"]), uso["cinco_h_reset"]
    if s is not None and s >= cfg["semana_avisa"]:
        return "avisa", txt("semana", cfg["semana_avisa"]), uso["semana_reset"]
    return "ok", None, None


def uso_checar(prioridade=2, agora=None, agente="claude"):
    """Recusa o despacho com ValueError se o uso do plano passou do limiar de pausa (semana) ou de segurar (5 h); a prioridade 1 passa pela segura da
    janela de 5 h, mas não pela pausa da semana. Sem quadro fresco não recusa."""
    nivel, motivo, _ = uso_nivel(uso_plano(agora, agente), agora)
    if nivel == "pausa" or (nivel == "segura" and prioridade != 1):
        append_event({"tipo": "uso_parou", "nivel": nivel, "motivo": motivo, **({"agente": agente} if agente != "claude" else {})})
        raise ValueError(f"uso do plano{_do_harness(agente)}: {motivo}; nada foi despachado. "
                         + ("Rode orq pausar para abrir folga." if nivel == "pausa" else "Espere a janela virar, ou ajuste o limiar em uso.json."))


def _do_harness(agente):
    """" do Codex" para o texto do uso de outro harness; o do Claude fica sem sufixo, como antes."""
    return "" if agente == "claude" else f" do {agente.capitalize()}"


def uso_avisar(agora=None, agente="claude"):
    """Digita no coordenador um aviso por (nível, janela): a primeira volta do painel depois de cruzar o limiar, e de novo só se o nível subir ou a
    janela virar. Coordenador ocupado: a próxima volta tenta. Devolve as linhas do painel."""
    g = _gerente_cfg()
    if not g or not g.get("coordenador"):
        return []
    nivel, motivo, reset = uso_nivel(uso_plano(agora, agente), agora)
    k = "uso_aviso" if agente == "claude" else f"uso_aviso_{agente}"
    if nivel in ("ok", "desconhecido"):
        if nivel == "ok" and _cursor_ro().get(k):
            _cursor_mut(lambda c: c.pop(k, None))
        return []
    chave = f"{nivel}:{reset}"
    if _dict(_cursor_ro().get(k)).get("chave") == chave:
        return []
    acao = {"pausa": "orq despachar recusa; rode orq pausar", "segura": "orq despachar recusa até a janela virar", "avisa": "evite despachar o que não for urgente"}[nivel]
    if avisa_coordenador(g["coordenador"], f"orq: uso do plano{_do_harness(agente)}, {motivo}. {acao[0].upper() + acao[1:]}.") not in ("enviado", "adiado"):
        return []
    _cursor_mut(lambda c: c.__setitem__(k, {"chave": chave, "ts": now()}))
    append_event({"tipo": "uso_aviso", "nivel": nivel, "motivo": motivo, **({"agente": agente} if agente != "claude" else {})})
    return [f"uso do plano{_do_harness(agente)}: {nivel} ({motivo}), coordenador avisado"]


def prioridade_padrao(titulo):
    """1 (alta) a 3 (baixa) pela frente do título: segurança e produção altas; failover, diagnóstico e painel baixas; o resto 2."""
    return 1 if _FRENTE_ALTA.search(titulo or "") else 3 if _FRENTE_BAIXA.search(titulo or "") else 2


def prioridade_de(events, task, dispatch, titulo):
    """A prioridade da task: a última de `orq prioridade`, senão a do despacho (--prioridade), senão a da frente do título."""
    for e in reversed(events):
        if e.get("tipo") == "prioridade" and e.get("task") == task:
            return e["valor"]
    for e in reversed(events):
        if e.get("tipo") == "despacho" and e.get("prioridade") and (e.get("dispatch") == dispatch or (task and e.get("task") == task)):
            return e["prioridade"]
    return prioridade_padrao(titulo)


def prioridade_definir(task, valor):
    """`orq prioridade <task> <1-3>`: troca a prioridade de uma task (a do despacho e a do padrão passam a valer menos). Vale antes e depois de a task rodar; o aberto.json é refeito para o digest e o orq agentes verem a troca."""
    if valor not in (1, 2, 3):
        raise ValueError("a prioridade é 1 (alta), 2 ou 3 (baixa)")
    if not re.fullmatch(r"task_\w+", task or ""):
        raise ValueError(f"{task!r} não é um id de task (task_…)")
    ev = append_event({"tipo": "prioridade", "task": task, "valor": valor})
    refresh_bg()
    return ev


def _lista_pausa(ags, events, tasks, ate_prioridade, pausados):
    """(a pausar, preservados): os workers vivos escolhidos e os que o critério poupou, cada um com `prioridade`.

    `tasks` (ids de task ou de dispatch) vale sozinho. Sem ele: prioridade >= `ate_prioridade`; sem o número, a baixa (3) e as que ainda só investigam.
    Em verificação final a fase poupa em qualquer dos dois."""
    vivos = [a for a in ags if a["estado"] in ("rodando", "travado", "nao_comecou", "parado", "perguntando") and a["dispatch"] not in pausados
             and a.get("terminal") != os.environ.get("ORCA_TERMINAL_HANDLE")]
    if tasks:
        perdidas = set(tasks) - {x for a in vivos for x in (a["task"], a["dispatch"])}
        if perdidas:
            raise ValueError(f"sem worker vivo para {', '.join(sorted(perdidas))}")
        return [a for a in vivos if a["task"] in tasks or a["dispatch"] in tasks], []
    escolhidos, poupados = [], []
    for a in vivos:
        fase = a.get("fase") or ""
        if a["prioridade"] >= (ate_prioridade or 3) or (ate_prioridade is None and _FASE_INICIAL.search(fase)):
            (poupados if _FASE_FINAL.search(fase) else escolhidos).append(a)
    return escolhidos, poupados


def pausar(tasks=(), ate_prioridade=None, run=None, dry_run=False):
    """Pausa workers para abrir folga no plano: manda MSG_PAUSA a cada um (steer), espera o PAUSA.md novo na worktree dele, fecha o terminal e grava o
    dispatch em cursor.json `pausados` (sessão, cwd, modelo). O dispatch segue `dispatched` no Orca sem terminal; `orq retomar --pausados` o sobe.

    Worker sem sessão ou cwd gravado não é pausado (não haveria como voltar). Sem PAUSA.md no prazo, o terminal fica aberto. Devolve
    {pausados: [{dispatch, task, titulo, prioridade, estado…}], preservados: […]}."""
    if ate_prioridade is not None and ate_prioridade not in (1, 2, 3):
        raise ValueError("--ate-prioridade espera 1, 2 ou 3")
    alvo, poupados = _lista_pausa(agentes(run), read_events(), set(tasks), ate_prioridade, _dict(_cursor_ro().get("pausados")))
    turnos, saida, espera = _turnos_ro(), [], {}
    for a in alvo:
        t = _dict(turnos.get(a["dispatch"]))
        cwd = t.get("cwd") or _checkpoint(a["dispatch"]).get("caminho")
        linha = {"dispatch": a["dispatch"], "task": a["task"], "run": a["run"], "titulo": a.get("titulo"), "prioridade": a["prioridade"], "fase": a.get("fase"),
                 "agente": a.get("agente") or t.get("harness") or "claude", "modelo": a.get("modelo"), "effort": a.get("effort"), "sessao": t.get("sessao"), "cwd": cwd,
                 "terminal": a["terminal"]}
        if not (linha["sessao"] and cwd and os.path.isdir(cwd) and a["terminal"]):
            saida.append({**linha, "estado": "sem_sessao", "aviso": "sem session_id, worktree ou terminal: sem como voltar, não foi pausado"})
        elif dry_run:
            saida.append({**linha, "estado": "a_pausar"})
        else:
            arq = os.path.join(cwd, "PAUSA.md")
            antes = os.path.getmtime(arq) if os.path.exists(arq) else 0
            try:
                steer(a["task"], MSG_PAUSA, a["run"])
            except (ValueError, RuntimeError, subprocess.TimeoutExpired) as e:
                saida.append({**linha, "estado": "falhou", "aviso": str(e)})
                continue
            espera[a["dispatch"]] = (linha, arq, antes)
    fim = time.time() + PAUSA_ESPERA_S
    while espera:
        for d, (linha, arq, antes) in list(espera.items()):
            if os.path.exists(arq) and os.path.getmtime(arq) > antes:
                espera.pop(d)
                saida.append(_fechar_pausado(linha))
        if espera and time.time() < fim:
            time.sleep(PAUSA_POLL_S)
        elif espera:
            saida += [{**linha, "estado": "sem_pausa_md", "aviso": f"sem PAUSA.md novo em {PAUSA_ESPERA_S:g} s: o terminal segue aberto"} for linha, _, _ in espera.values()]
            break
    return {"pausados": saida, "preservados": [{k: a.get(k) for k in ("dispatch", "task", "titulo", "prioridade", "fase")} for a in poupados]}


def _fechar_pausado(linha):
    """O PAUSA.md chegou: fecha o terminal do worker, guarda o dispatch em `pausados` e grava o evento."""
    encerrados = encerrar_filhos(linha["cwd"], linha["agente"])  # shells e monitores em segundo plano sobrariam órfãos e seguiriam pesando na máquina
    try:
        orca("close", "--terminal", linha["terminal"], area="terminal")
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        return {**linha, "estado": "falhou", "aviso": f"terminal close falhou ({e}); o PAUSA.md está escrito", "encerrados": encerrados}
    guarda = {k: linha[k] for k in ("task", "run", "titulo", "prioridade", "agente", "modelo", "effort", "sessao", "cwd", "terminal")}
    _cursor_mut(lambda c: c.setdefault("pausados", {}).__setitem__(linha["dispatch"], {**guarda, "desde": now()}))
    append_event({"tipo": "pausa_plano", "dispatch": linha["dispatch"], **guarda, "encerrados": encerrados})
    return {**linha, "estado": "pausado", "encerrados": encerrados}


def retomar_pausados(run=None, forcar=False):
    """Sobe de volta (o resume do harness, com MSG_VOLTA) os dispatches de cursor.json `pausados`, os de prioridade mais alta primeiro, e os tira da lista.
    Com o uso do plano do harness ainda acima do limiar, o worker dele fica (`uso_alto`), salvo `forcar`: senão ele voltaria para pausar de novo. Se
    isso vale para todos, recusa."""
    pausados = {d: p for d, p in _dict(_cursor_ro().get("pausados")).items() if not run or p.get("run") == run}
    altos = {}
    for ag in {p.get("agente") or "claude" for p in pausados.values()} if not forcar else ():
        nivel, motivo, _ = uso_nivel(uso_plano(agente=ag))
        if nivel in ("pausa", "segura"):
            altos[ag] = motivo
    if pausados and altos and all((p.get("agente") or "claude") in altos for p in pausados.values()):
        raise ValueError(f"uso do plano ainda alto: {'; '.join(f'{m}{_do_harness(ag)}' for ag, m in altos.items())}. Espere a janela virar ou use --forcar")
    res, cfg = [], maquina_cfg()
    ocup = maquina_ocupacao() if pausados else None
    leitura = maquina_ler()
    pressao = maquina_nivel(leitura, cfg)
    for d, p in sorted(pausados.items(), key=lambda kv: kv[1].get("prioridade") or 2):
        if (p.get("agente") or "claude") not in altos and (motivo := maquina_barra_item(p.get("modelo"), p["run"], ocup, cfg, pressao, leitura)):
            res.append({"dispatch": d, "task": p["task"], "titulo": p["titulo"], "estado": "sem_vaga", "aviso": f"{motivo}; segue pausado, rode orq retomar --pausados quando abrir vaga"})
            continue
        if (p.get("agente") or "claude") in altos:
            res.append({"dispatch": d, "task": p["task"], "titulo": p["titulo"], "estado": "uso_alto", "aviso": altos[p.get("agente") or "claude"]})
            continue
        linha = {"dispatch": d, "task": p["task"], "run": p["run"], "titulo": p["titulo"], "prioridade": p.get("prioridade"), "modelo": p.get("modelo"),
                 "sessao": p["sessao"], "cwd": p["cwd"], "terminal": p["terminal"]}
        if not os.path.isdir(p["cwd"]):
            res.append({**linha, "estado": "sem_worktree", "aviso": f"a pasta {p['cwd']} não existe: nada foi subido"})
            continue
        try:
            cp = _checkpoint(d)
        except (RuntimeError, subprocess.TimeoutExpired):
            cp = {"head": None, "sujo": None}
        r = _subir_sessao(linha, p["sessao"], p.get("modelo"), cp, f"suba outro worker com: orq relancar {d} --nota 'a sessão pausada não pôde ser retomada'", MSG_VOLTA,
                          p.get("agente") or "claude", p.get("effort"))
        if r["estado"] != "falhou":
            _ocupar(ocup, d, p.get("modelo"))
            _cursor_mut(lambda c, d=d: c.get("pausados", {}).pop(d, None))
            append_event({"tipo": "pausa_fim", "dispatch": d, "task": p["task"], "terminal": r.get("novo")})
        res.append(r)
    return res


def texto_pausar(res):
    """Uma linha por worker pausado ou poupado."""
    ls = [f"{w['dispatch']} P{w['prioridade']} {w.get('titulo') or w['task']}: {w['estado']}" + (f" ({w['aviso']})" if w.get("aviso") else "")
          + (f" [{len(w['encerrados'])} processos em segundo plano encerrados]" if w.get("encerrados") else "") for w in res["pausados"]]
    ls += [f"{w['dispatch']} P{w['prioridade']} {w.get('titulo') or w['task']}: preservado (fase {w.get('fase')})" for w in res["preservados"]]
    return "\n".join(ls) or "nenhum worker a pausar"


# ---------- hibernar worker ocioso e acordar quando precisar (ticket 60) ----------

# Cada worker é um `claude` vivo (com os servidores MCP da sessão) que ocupa memória mesmo parado no prompt. Hibernar = guardar a sessão em
# cursor.json `hibernados` e fechar o terminal; a task segue como está no Orca. Acordar = o resume do harness (o mesmo caminho do `orq retomar`),
# com a mensagem que o acordou. O critério é determinístico (tela, hooks do worker, processos, estado do orq), sem LLM. Desenho: docs/design.md.
HIBERNA = "hibernar.json"  # {"min": 15, "externa_min": 2}: os limiares, por cima destes padrões
HIBERNA_MIN = float(os.environ.get("ORQ_HIBERNA_MIN") or 15)  # minutos parado no prompt (ou entregue sem liberar) até hibernar
HIBERNA_EXTERNA_MIN = float(os.environ.get("ORQ_HIBERNA_EXTERNA_MIN") or 2)  # esperando algo externo que o orq conhece: hiberna logo depois deste tempo parado
HIBERNA_VOLTA_S = float(os.environ.get("ORQ_HIBERNA_VOLTA_S") or 60)  # o painel confere os workers a cada tanto, não a cada volta de 10 s
HIBERNA_RSS_ESPERA_S = float(os.environ.get("ORQ_HIBERNA_RSS_ESPERA_S") or 5)  # quanto esperar o processo do terminal fechado sumir do ps antes de medir o RSS depois
TELA_OCUPADA = re.compile(r"esc to interrupt", re.I)  # o spinner do Claude Code e do Codex: o turno corre
MSG_ACORDA = ("Você estava hibernado: o coordenador fechou o terminal por ociosidade para liberar memória e agora o acordou, na mesma sessão. O que chegou: {texto}\n"
              "Antes: confira git status e o estado da sua worktree. Ao terminar, mande o worker_done como antes; se o Orca recusar por causa do handle novo "
              "ou da task já entregue, escreva o relatório final num arquivo relatorio-final.md na raiz da sua worktree e mostre o caminho no terminal.")


def _hibernados():
    """cursor.json `hibernados`: {dispatch: {task, run, titulo, agente, modelo, effort, sessao, cwd, terminal, entregue, desde, motivo, rss_liberado_mb}}."""
    return {d: p for d, p in _dict(_cursor_ro().get("hibernados")).items() if isinstance(p, dict)}


def _esquece_hibernado(dispatch):
    """Tira o dispatch de `hibernados` (acordado, liberado ou parado); sem a entrada não grava nada."""
    if dispatch in _hibernados():
        _cursor_mut(lambda c: c.get("hibernados", {}).pop(dispatch, None))


def _hiberna_cfg():
    """{min, externa_min} em minutos: hibernar.json por cima de ORQ_HIBERNA_MIN, ORQ_HIBERNA_EXTERNA_MIN e dos padrões (15 e 2)."""
    cfg = {"min": HIBERNA_MIN, "externa_min": HIBERNA_EXTERNA_MIN}
    for k, v in _dict(_read_json(_path(HIBERNA))).items():
        if k in cfg and isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0:
            cfg[k] = v
    return cfg


def _agente_do(p):
    """O harness de um processo do `ps` (claude, codex) pelo nome do executável, ou None."""
    nome = os.path.basename((p.get("args") or "").split(" ", 1)[0])
    return nome if nome in HARNESS else None


def _processos(com_cwd=True):
    """[{pid, ppid, rss (KB), cpu (%), args, cwd}] do `ps`, com o cwd (lsof) só dos processos de agente (`com_cwd`); None se o ps falhar. `cwd` None: o lsof não o disse.

    ORQ_PROCESSOS: caminho de um JSON com essa lista, no lugar do ps e do lsof (testes)."""
    if os.environ.get("ORQ_PROCESSOS"):
        return _read_json(os.environ["ORQ_PROCESSOS"])
    try:
        saida = subprocess.run(["ps", "-axo", "pid=,ppid=,rss=,pcpu=,command="], capture_output=True, text=True, timeout=10, check=True).stdout
    except (subprocess.SubprocessError, OSError) as e:
        log(f"hibernar: ps: {type(e).__name__}: {e}")
        return None
    ps = []
    for l in saida.splitlines():
        c = l.split(None, 4)
        if len(c) == 5 and all(x.isdigit() for x in c[:3]) and re.fullmatch(r"\d+(\.\d+)?", c[3]):
            ps.append({"pid": int(c[0]), "ppid": int(c[1]), "rss": int(c[2]), "cpu": float(c[3]), "args": c[4], "cwd": None})
    donos = [p for p in ps if _agente_do(p)] if com_cwd else []
    if donos:
        try:
            lsof = subprocess.run(["lsof", "-a", "-d", "cwd", "-Fpn", "-p", ",".join(str(p["pid"]) for p in donos)], capture_output=True, text=True, timeout=10).stdout
        except (subprocess.SubprocessError, OSError) as e:
            log(f"hibernar: lsof: {type(e).__name__}: {e}")
            return ps
        pid = None
        for l in lsof.splitlines():
            if l[:1] == "p" and l[1:].isdigit():
                pid = int(l[1:])
            elif l[:1] == "n" and pid is not None:
                next(p for p in donos if p["pid"] == pid)["cwd"] = l[1:]
    return ps


def _descendentes(procs, pids):
    """Os pids de `pids` e de todos os processos abaixo deles."""
    vistos, fila = set(), list(pids)
    while fila:
        pid = fila.pop()
        if pid not in vistos:
            vistos.add(pid)
            fila += [p["pid"] for p in procs if p["ppid"] == pid]
    return vistos


def rss_agentes_mb(procs):
    """Soma do RSS, em MB, dos processos de agente (claude, codex) e de tudo o que sobe abaixo deles (os servidores MCP da sessão); None sem a lista."""
    if procs is None:
        return None
    vivos = _descendentes(procs, [p["pid"] for p in procs if _agente_do(p)])
    return round(sum(p["rss"] for p in procs if p["pid"] in vivos) / 1024)


def _processo_do_worker(procs, cwd, agente):
    """(pids, filho): os processos do agente cujo cwd é o do worker e se algum comando do Bash dele ainda roda.

    `filho` True/False; None quando não há prova: sem ps, nenhum processo do agente nesse cwd (o lsof falhou ou o worker `cd`ou) ou harness sem padrão de
    filho (HARNESS[…]["filho"]). Dois agentes no mesmo cwd (o coordenador na mesma worktree) contam juntos: na dúvida, há filho."""
    padrao = HARNESS.get(agente, {}).get("filho")
    pids = [p["pid"] for p in procs or [] if _agente_do(p) == agente and p.get("cwd") == cwd]
    if not padrao or not pids:
        return pids, None
    return pids, any(padrao.search(p["args"]) for p in procs if p["pid"] in _descendentes(procs, pids) - set(pids))


def _protegidos(run):
    """Os terminais que nunca hibernam: o deste processo, o coordenador e o gerente do gerente.json e o coordenador do Run."""
    g = _gerente_cfg() or {}
    try:
        coord = (orca("run-show", "--id", run)["run"] or {}).get("coordinator_handle")
    except (RuntimeError, subprocess.TimeoutExpired, OSError, KeyError):
        coord = None
    return {x for x in (os.environ.get("ORCA_TERMINAL_HANDLE"), g.get("coordenador"), g.get("gerente"), coord) if x}


ENCERRA_ESPERA_S = float(os.environ.get("ORQ_ENCERRA_ESPERA_S") or 5)  # do SIGTERM ao SIGKILL nos filhos do worker pausado


def _filhos_do_worker(procs, cwd, agente):
    """Os processos que o Bash tool do worker deixou vivos: cada comando filho do agente (HARNESS[…]["filho"]) e tudo o que sobe abaixo dele. Fora o agente, os
    servidores MCP da sessão (o close do terminal leva) e os ancestrais deste processo. [] sem lista, sem agente nesse cwd ou harness sem padrão."""
    padrao = HARNESS.get(agente, {}).get("filho")
    pids, _ = _processo_do_worker(procs, cwd, agente)
    if not padrao or not pids:
        return []
    raizes = [p["pid"] for p in procs if p["pid"] in _descendentes(procs, pids) - set(pids) and padrao.search(p["args"])]
    pai = {p["pid"]: p["ppid"] for p in procs}
    nos, eu = set(), os.getpid()
    while eu in pai and eu not in nos:
        nos.add(eu)
        eu = pai[eu]
    return [p for p in procs if p["pid"] in _descendentes(procs, raizes) and p["pid"] not in nos]


def _sinal(pid, sig):
    """Manda `sig` ao pid. ORQ_PROCESSOS (testes): tira o processo do JSON; o SIGTERM só o tira se ele não tem `ignora_term`."""
    if os.environ.get("ORQ_PROCESSOS"):
        arq = os.environ["ORQ_PROCESSOS"]
        ps = _read_json(arq)
        json.dump([p for p in ps if p["pid"] != pid or (sig == signal.SIGTERM and p.get("ignora_term"))], open(arq, "w"))
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.kill(pid, sig)


def encerrar_filhos(cwd, agente):
    """Encerra os processos que o worker deixou em segundo plano (testes, node, sleep/until de monitor): SIGTERM, e depois de ENCERRA_ESPERA_S o SIGKILL nos que
    ficaram. Nada fora da árvore do agente é tocado. Devolve [{pid, args, sinal}] (`sinal`: o último que o matou: TERM ou KILL)."""
    filhos = _filhos_do_worker(_processos(), cwd, agente)
    for p in filhos:
        _sinal(p["pid"], signal.SIGTERM)
    vivos = {p["pid"] for p in filhos}
    fim = time.time() + ENCERRA_ESPERA_S
    while vivos and time.time() < fim:
        vivos &= {p["pid"] for p in _processos() or ()}
        if vivos:
            time.sleep(0.2)
    for pid in vivos:
        _sinal(pid, signal.SIGKILL)
    return [{"pid": p["pid"], "args": p["args"][:200], "sinal": "KILL" if p["pid"] in vivos else "TERM"} for p in filhos]


def _filhos_por_task():
    """{task: n processos em segundo plano} dos workers vivos que têm algum, para o aviso de pressão mostrar quem pesa; {} sem ps."""
    procs, turnos = _processos(), _turnos_ro()
    if not procs:
        return {}
    res = {}
    for a in agentes():
        t = _dict(turnos.get(a["dispatch"]))
        n = len(_filhos_do_worker(procs, t.get("cwd"), a.get("agente") or t.get("harness") or "claude")) if t.get("cwd") else 0
        if n:
            res[a["task"]] = n
    return res


def _tela_ocupada(handle, agente):
    """O motivo se a tela do terminal mostra o turno correndo (spinner), um processo em segundo plano ou um menu esperando resposta; senão None."""
    tail = _fundo(orca("read", "--terminal", handle, "--screen", "--limit", str(TELA_LINHAS), area="terminal", timeout=10), "terminal", "tail") or []
    tela = "\n".join(map(str, tail[-15:]))
    espera = HARNESS[agente]["tela"]["espera"]
    return ("spinner na tela" if TELA_OCUPADA.search(tela) else f"{espera.search(tela).group(0).strip()} (tela)" if espera and espera.search(tela)
            else "menu esperando resposta na tela" if tela_pergunta(tail, agente) else None)


def _acordada_em(events):
    """{dispatch: ts do último `acordar`}: o resume novo adia a próxima hibernação (o turno novo ainda pode não estar no turnos.json)."""
    return {e.get("dispatch"): e.get("ts") for e in events if e.get("tipo") == "acordar"}


def _espera_externa(a, pend, prs, tks):
    """O que o orq já sabe que o worker espera de fora, em texto, ou None: pendência do usuário ligada à task, PR aberto esperando merge, ticket bloqueado."""
    for p in pend:
        if p.get("task") == a["task"]:
            return f"pendência {p.get('id')}"
    for i in prs:
        if i.get("task") == a["task"] and i.get("estado") == "aberto":
            return f"PR #{i.get('numero')} esperando merge"
    status = {t["num"]: t["status"] for t in tks}
    for t in tks:
        if t.get("task") == a["task"]:
            abertos = [n for n in t["blocked_by"] if status.get(n) != STATUS_FECHADO]
            if abertos:
                return f"ticket {t['num']} bloqueado por {', '.join(abertos)}"
    return None


def motivo_hibernar(a, agora, cfg, pend=(), prs=(), tks=(), acordada=None):
    """Pura: por que o worker `a` (linha de agentes()) deve hibernar, ou None. Só olha o estado do orq; a tela e os processos são conferidos depois.

    Só o turno encerrado conta (fim do turno nos hooks do worker, sem heartbeat nem prompt depois): `parado`, o que espera de propósito (`rodando` com
    espera declarada) e o `entregue` sem liberar. Pergunta presa, travado e a tela com shell em segundo plano ficam de fora (essas continuam escalando).
    Três motivos: esperando algo externo que o orq conhece (já depois de cfg.externa_min), entregue e sem liberar, ou parado (os dois depois de cfg.min)."""
    fim, ini = _ts(a.get("turno_fim")), _ts(a.get("turno_inicio"))
    if a["estado"] not in ("parado", "rodando", "entregue") or a.get("retido") or a.get("tela") or not fim or (ini and ini > fim):
        return None
    if a["estado"] != "entregue" and a.get("turno") != "parado":
        return None
    desde = max(fim, _ts(acordada) or fim)
    parado_min = (agora - desde).total_seconds() / 60
    externa = _espera_externa(a, pend, prs, tks) if a["estado"] != "entregue" else None
    if externa and parado_min >= cfg["externa_min"]:
        return f"esperando: {externa}"
    if parado_min >= cfg["min"]:
        return "entregue e sem liberar" if a["estado"] == "entregue" else "ocioso no prompt"
    return None


def _hibernar_agente(a, motivo, procs, forcar=False):
    """Hiberna o worker `a` se nada o impede e devolve a linha com `estado`: hibernado, recusado (com `aviso`) ou falhou. Confere, nesta ordem: terminal
    protegido (coordenador, gerente), worker do orq (sessão e cwd gravados pelos hooks, para o resume), tela (spinner, shell em segundo plano, menu),
    caixa de entrada livre (`terminal_livre`) e processo filho vivo (E2E, teste, build). Sem prova de processo filho só `forcar` passa."""
    t = _dict(_turnos_ro().get(a["dispatch"]))
    agente = a.get("agente") or t.get("harness") or "claude"
    try:
        cwd = t.get("cwd") or _checkpoint(a["dispatch"]).get("caminho")
    except (RuntimeError, subprocess.TimeoutExpired):
        cwd = None
    linha = {"dispatch": a["dispatch"], "task": a["task"], "run": a["run"], "titulo": a.get("titulo"), "agente": agente, "modelo": a.get("modelo"), "effort": a.get("effort"),
             "sessao": t.get("sessao"), "cwd": cwd, "terminal": a.get("terminal"), "entregue": a["estado"] == "entregue", "motivo": motivo}

    def nao(aviso):
        return {**linha, "estado": "recusado", "aviso": aviso}

    if not a.get("terminal") or a["estado"] in ("hibernado", "liberado", "encerrado"):
        return nao(f"worker {a['estado']}: sem terminal a fechar")
    if a["terminal"] in _protegidos(a["run"]):
        return nao("é o coordenador ou o gerente: nunca hibernam")
    if not (linha["sessao"] and cwd and os.path.isdir(cwd)) or agente not in HARNESS:
        return nao("sem session_id e worktree gravados pelos hooks do worker (não é um worker do orq, ou não há como voltar)")
    if a["estado"] == "perguntando":
        return nao("há uma pergunta esperando resposta")
    try:
        ocupada = _tela_ocupada(a["terminal"], agente)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        return nao(f"tela ilegível ({e})")
    if ocupada or (ocupada := terminal_livre(a["terminal"])):
        return nao(f"não está livre: {ocupada}")
    _, filho = _processo_do_worker(procs, cwd, agente)
    if filho:
        return nao("processo filho vivo (E2E, teste ou build)")
    if filho is None and not forcar:
        return nao("sem prova de que não há processo filho (o ps/lsof não achou o processo do agente nessa worktree, ou o harness não tem padrão): use --forcar")
    antes = rss_agentes_mb(procs)
    guarda = {k: linha[k] for k in ("task", "run", "titulo", "agente", "modelo", "effort", "sessao", "cwd", "terminal", "entregue", "motivo")}
    _cursor_mut(lambda c: c.setdefault("hibernados", {}).__setitem__(a["dispatch"], {**guarda, "desde": now()}))  # antes do close: uma queda no meio não deixa o retomar subir o worker
    try:
        orca("close", "--terminal", a["terminal"], area="terminal")
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        _esquece_hibernado(a["dispatch"])
        return {**linha, "estado": "falhou", "aviso": f"terminal close falhou ({e})"}
    depois, fim = antes, time.time() + HIBERNA_RSS_ESPERA_S
    while antes is not None and (depois := rss_agentes_mb(_processos())) is not None and depois >= antes and time.time() < fim:
        time.sleep(0.5)
    liberou = max(0, antes - depois) if antes is not None and depois is not None else None
    _cursor_mut(lambda c: c.get("hibernados", {}).get(a["dispatch"], {}).__setitem__("rss_liberado_mb", liberou))
    append_event({"tipo": "hibernar", "dispatch": a["dispatch"], **guarda, "rss_antes_mb": antes, "rss_depois_mb": depois, "rss_liberado_mb": liberou})
    return {**linha, "estado": "hibernado", "rss_antes_mb": antes, "rss_depois_mb": depois, "rss_liberado_mb": liberou}


def hibernar(alvo, run=None, forcar=False):
    """`orq hibernar <task|dispatch>`: hiberna o worker à mão, com as mesmas recusas do automático (`forcar` só passa a falta de prova de processo filho).
    Levanta ValueError com o motivo se não hibernou."""
    a = next((x for x in agentes(run) if alvo in (x["task"], x["dispatch"])), None)
    if not a:
        raise ValueError(f"sem worker para {alvo} (o orq agentes mostra os dispatches)")
    r = _hibernar_agente(a, "manual", _processos(), forcar)
    if r["estado"] != "hibernado":
        raise ValueError(f"{alvo} não hibernou: {r['aviso']}")
    return r


def hibernar_ociosos(agora=None):
    """Uma volta do gerente: hiberna o que o critério (motivo_hibernar) escolhe e a tela e os processos deixam. No máximo uma vez por HIBERNA_VOLTA_S.
    Devolve as linhas do painel. Recusa na tela ou nos processos é silenciosa: a próxima volta confere de novo."""
    if time.time() - (_cursor_ro().get("hibernar_volta") or 0) < HIBERNA_VOLTA_S:
        return []
    _cursor_mut(lambda c: c.__setitem__("hibernar_volta", time.time()))
    agora, cfg = agora or datetime.now(timezone.utc), _hiberna_cfg()
    acordada, pend, prs, tks = _acordada_em(read_events()), _load_pend()["itens"], _prs_ro()["itens"], tickets()
    cand = [(a, m) for a in agentes() if (m := motivo_hibernar(a, agora, cfg, pend, prs, tks, acordada.get(a["dispatch"])))]
    if not cand:
        return []
    procs, linhas = _processos(), []
    for a, motivo in cand:
        r = _hibernar_agente(a, motivo, procs)
        if r["estado"] == "recusado":
            log(f"hibernar {a['task']}: recusado ({r['aviso']})")
        elif r["estado"] == "falhou":
            linhas.append(f"{a['task']}: não hibernou ({r['aviso']})")
        else:
            linhas.append(f"{a['task']}: hibernado ({motivo}" + (f", ~{r['rss_liberado_mb']} MB" if r.get("rss_liberado_mb") else "") + ")")
    return linhas


def acordar(alvo, texto=None):
    """Sobe de volta o worker hibernado (task ou dispatch): o resume do harness numa terminal novo, pelo mesmo caminho do `orq retomar`, com MSG_ACORDA
    (o `texto` do que chegou) e o jeito de escalar. Tira o dispatch de `hibernados` e grava `acordar`, salvo se o terminal nem subiu (`falhou`: segue
    hibernado, e o gerente tenta de novo). ValueError se `alvo` não está hibernado."""
    d, p = next(((d, p) for d, p in _hibernados().items() if alvo in (d, p.get("task"))), (None, None))
    if not d:
        raise ValueError(f"{alvo} não está hibernado")
    linha = {"dispatch": d, "task": p["task"], "run": p["run"], "titulo": p.get("titulo") or d, "cwd": p["cwd"], "terminal": p["terminal"]}
    if not os.path.isdir(p["cwd"]):
        return {**linha, "estado": "sem_worktree", "aviso": f"a pasta {p['cwd']} não existe: nada foi subido"}
    try:
        cp = _checkpoint(d)
    except (RuntimeError, subprocess.TimeoutExpired):
        cp = {"head": None, "sujo": None}
    coord = (_gerente_cfg() or {}).get("coordenador") or os.environ.get("ORCA_TERMINAL_HANDLE")
    msg = f"{MSG_ACORDA.format(texto=texto or 'acordado à mão (orq acordar).')} " + MSG_ESCALAR.format(coord=coord or "<coordenador>", run=p["run"], task=p["task"], dispatch=d)
    r = _subir_sessao(linha, p["sessao"], p.get("modelo"), cp, f"suba outro worker com: orq relancar {d} --nota 'a sessão hibernada não pôde ser retomada'", msg,
                      p.get("agente") or "claude", p.get("effort"))
    if r["estado"] != "falhou":
        _esquece_hibernado(d)
        append_event({"tipo": "acordar", "dispatch": d, "task": p["task"], "run": p["run"], "terminal": r.get("novo"), "motivo": _cita(texto or "manual", 200)})
    return r


def acordar_gatilhos():
    """Uma volta do gerente: acorda o hibernado (que ainda não entregou) cuja pendência ligada à task foi respondida, ou cujo PR foi mergeado ou fechado,
    depois que ele hibernou. O steer e o responder acordam na hora, pelos próprios comandos. Devolve as linhas do painel.
    ponytail: o `falhou` tenta de novo a cada volta (10 s); com o Orca recusando o terminal isso vira ruído no log, sem limite de tentativas."""
    hib = {d: p for d, p in _hibernados().items() if not p.get("entregue")}
    if not hib:
        return []
    events, linhas = read_events(), []
    for d, p in hib.items():
        e = next((e for e in reversed(events) if e.get("task") == p["task"] and (e.get("ts") or "") >= p["desde"]
                  and (e.get("tipo") == "pend" and e.get("op") == "done" or e.get("tipo") == "pr" and e.get("op") in ("entrou", "fechou"))), None)
        if not e:
            continue
        texto = (f"a pendência {e.get('pend')} foi respondida: {e.get('resposta') or 'fechada sem resposta'}" if e["tipo"] == "pend"
                 else f"o PR #{e.get('numero')} " + (f"entrou em {e.get('base')}" if e["op"] == "entrou" else "foi fechado sem merge"))
        r = acordar(d, texto)
        linhas.append(f"{p['task']}: acordado ({texto[:70]}) -> {r['estado']}")
    return linhas


def linhas_hibernacao():
    """A linha do `orq agentes` com quantos workers estão hibernados e a memória que a hibernação liberou (RSS dos processos de agente, antes e depois)."""
    hib = _hibernados()
    mb = sum(p.get("rss_liberado_mb") or 0 for p in hib.values())
    return [f"Hibernados: {len(hib)}" + (f", ~{mb} MB de RSS liberados (soma dos processos claude e dos MCPs deles, antes e depois do close)" if mb else "")] if hib else []


def texto_hibernado(r):
    """Uma linha para o resultado de `orq hibernar`."""
    return f"{r['dispatch']} {r.get('titulo') or r['task']}: hibernado ({r['motivo']})" + (f", RSS {r['rss_antes_mb']} -> {r['rss_depois_mb']} MB (~{r['rss_liberado_mb']} MB liberados)"
                                                                                       if r.get("rss_liberado_mb") is not None else "")


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
    _cursor_mut(lambda c: c.__setitem__("gerente_volta", now()))  # o cartão da manhã lê daqui se o gerente estava vivo
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
        if avisa_coordenador(g["coordenador"], texto, minutos=WAKE_OCIOSO_MIN) not in ("enviado", "adiado"):  # adiado (coordenador com gente, ticket 86): já é entrega, sai no contexto
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
    try:
        linhas += telas_avisar()
    except Exception as e:  # noqa: BLE001 - a tela ilegível não derruba o painel; a próxima volta tenta
        log(f"telas: {type(e).__name__}: {e}")
    try:
        linhas += [*pr_poll(), *pr_avisar(), *avisa_fila_e2e(), *uso_avisar(), *uso_avisar(agente="codex"), *avisos_entregar()]
    except Exception as e:  # noqa: BLE001 - idem: o gh fora do ar não derruba o painel
        log(f"prs: {type(e).__name__}: {e}")
    try:
        linhas += mate_volta()
    except Exception as e:  # noqa: BLE001 - o canal dos mates não derruba o painel; a próxima volta tenta
        log(f"mates: {type(e).__name__}: {e}")
    try:
        linhas += maquina_volta()
    except Exception as e:  # noqa: BLE001 - a fila de despacho não derruba o painel; a próxima volta tenta
        log(f"fila de despacho: {type(e).__name__}: {e}")
    try:
        linhas += [*acordar_gatilhos(), *hibernar_ociosos()]
    except Exception as e:  # noqa: BLE001 - hibernar é economia, não pode derrubar o painel; a próxima volta tenta
        log(f"hibernar: {type(e).__name__}: {e}")
    return "\n".join(linhas)


# ---------- retro: o coletor de sinais de falha (ticket 78) ----------

ORQ_INSTALL = os.path.realpath(os.environ.get("ORQ_INSTALL") or os.path.expanduser("~/.claude/orq"))  # o checkout que roda (hooks, painel): ninguém trabalha nele
RETRO_DIR = "retro"  # ORQ_HOME/retro/<AAAA-MM-DDTHHMM>.json: as métricas de cada rodada gravada, para comparar semana a semana
RETRO_DIAS = 7
RETRO_CORRECAO_S = 1800  # a reação do usuário a uma entrega vem logo depois dela
RETRO_CORRECAO = re.compile(r"^\W*(n[ãa]o|ops|ajust\w*|errad\w*)\b", re.I)
RETRO_VERMELHO = ("FAILURE", "TIMED_OUT", "ACTION_REQUIRED", "ERROR", "STARTUP_FAILURE")
RETRO_SINAIS = {
    "nao_iniciou": "despacho que não começou",
    "steer_sem_leitura": "steer sem leitura provada",
    "steer_reentregue": "steer reentregue (aviso digitado de novo)",
    "retry": "worker relançado",
    "controle_falhou": "interromper, encerrar ou relançar que falhou",
    "intervencao": "worker interrompido ou encerrado",
    "pergunta_de_worker": "pergunta ou escalation respondida",
    "worker_falhou": "worker_done que não foi succeeded",
    "sem_entrega": "worker liberado sem entrega",
    "liberado_sujo": "worker liberado com árvore suja",
    "liberado_sem_push": "worker liberado com commits fora do origin/main",
    "entrega_com_aviso": "entrega sem prova (commit ausente ou árvore suja)",
    "checkout_em_uso": "trabalho no checkout em uso do orq",
    "entrada_sem_tratamento": "entrada que o Stop achou sem efeito",
    "intake_descartado": "entrada descartada",
    "alerta": "alerta do orq",
    "binding_perdido": "coordenador sem Run ligado",
    "correcao_do_usuario": "correção do usuário logo depois de uma entrega",
    "regra_violada": "regra do usuário violada por um worker (transcrito)",
    "pr_ci_vermelho": "PR com check vermelho (gh)",
    "pr_pediu_mudanca": "PR com mudança pedida no review (gh)",
}
RETRO_POR_MODELO = ("nao_iniciou", "retry", "pergunta_de_worker", "worker_falhou", "sem_entrega", "intervencao", "liberado_sujo")
RETRO_ESCRITA_AGENTS = re.compile(r"(?:>>?|\btee\b|\bsed\s+-i|\bmv\b|\bcp\b|\brm\b|\bln\b)[^\n|;&]*/\.agents/")
RETRO_GIT_NO_CHECKOUT = re.compile(r"\bgit\s+(?:-C\s+(\S+)\s+)?(?:merge|rebase|reset|checkout|switch|pull|cherry-pick|commit)\b")


RETRO_HEREDOC = re.compile(r"<<-?\s*(['\"]?)(\w+)\1[^\n]*\n.*?\n[ \t]*\2[ \t]*(?=\n|$)", re.S)
RETRO_ASPAS = re.compile(r"\"(?:\\.|[^\"\\])*\"|'[^']*'")


def _retro_viola(ferramenta, texto, cwd):
    """Os nomes das regras do usuário que uma chamada de ferramenta do worker quebra. Padrões estreitos e conhecidos; regra nova entra aqui.

    O comando que vale é o que sobra sem os corpos de heredoc e sem o texto entre aspas: um `git push` dentro de um `python3 - <<EOF` ou de um
    `echo` não é push. Só o trailer olha o texto inteiro (ele mora na mensagem do commit), e só quando o `git commit` está no comando de verdade.
    ponytail: regex, sem parser de shell. `bash -c "git push"` passa; `$(git push)` dentro de aspas também."""
    if ferramenta in ("Edit", "Write", "MultiEdit", "NotebookEdit"):
        return (["agents_global"] if "/.agents/" in texto else []) + (["checkout_em_uso_do_orq"] if os.path.realpath(texto).startswith(ORQ_INSTALL + os.sep) else [])
    v = RETRO_ASPAS.sub('""', RETRO_HEREDOC.sub("", texto))
    regras = []
    if re.search(r"\bgit\s+(?:-C\s+\S+\s+)?push\b", v):
        regras.append("push_de_worker")
    if re.search(r"\bgh\s+(?:workflow\s+run|pr\s+merge)\b", v):
        regras.append("producao")
    if ("git commit" in v or "gh pr create" in v) and re.search(r"Co-Authored-By|Generated with", texto, re.I):
        regras.append("trailer")
    if RETRO_ESCRITA_AGENTS.search(v):
        regras.append("agents_global")
    m = RETRO_GIT_NO_CHECKOUT.search(v)
    if m:
        cds = re.findall(r"\bcd\s+(\S+)", v[:m.start()])
        if os.path.realpath(os.path.expanduser(m.group(1) or (cds[-1] if cds else None) or cwd or "/")) == ORQ_INSTALL:
            regras.append("checkout_em_uso_do_orq")
    return regras


def _retro_chamadas(obj):
    """(ferramenta, texto, cwd) de cada chamada de ferramenta de uma linha de transcrito: `tool_use` no Claude Code, `function_call` no Codex."""
    out = []
    conteudo = _dict(obj.get("message")).get("content")
    for b in conteudo if isinstance(conteudo, list) else []:
        if isinstance(b, dict) and b.get("type") == "tool_use":
            i = _dict(b.get("input"))
            out.append((str(b.get("name")), str(i.get("command") or i.get("file_path") or i.get("path") or ""), obj.get("cwd")))
    p = _dict(obj.get("payload"))
    if p.get("type") == "function_call":
        try:
            a = _dict(json.loads(p.get("arguments") or "{}"))
        except ValueError:
            a = {}
        c = a.get("cmd") or a.get("command") or ""
        out.append(("Bash", " ".join(map(str, c)) if isinstance(c, list) else str(c), None))
    return out


def _retro_regras(despachos, turnos, projetos):
    """Casos de regra violada: o transcrito de cada dispatch (turnos.json dá a sessão) lido uma vez, um caso por dispatch e regra.

    ponytail: lê o arquivo inteiro de cada worker da janela; transcrito de dezenas de MB custa alguns segundos. Codex: só o comando, sem cwd."""
    casos = []
    for d in despachos:
        t = _dict(turnos.get(d.get("dispatch")))
        arqs = [t["transcrito"]] if t.get("transcrito") else glob.glob(os.path.join(projetos, "*", f"{glob.escape(t['sessao'])}.jsonl")) if t.get("sessao") else []
        achados = {}
        for arq in arqs:
            try:
                with open(arq, "rb") as f:
                    for n, linha in enumerate(f, 1):
                        if b'"tool_use"' not in linha and b'"function_call"' not in linha:
                            continue
                        try:
                            obj = json.loads(linha)
                        except ValueError:
                            continue
                        for ferr, texto, cwd in _retro_chamadas(_dict(obj)):
                            for regra in _retro_viola(ferr, texto, cwd):
                                a = achados.setdefault(regra, {"n": 0, "ponteiro": f"{arq}:{n}", "ts": _dict(obj).get("timestamp"), "texto": texto})
                                a["n"] += 1
            except OSError as e:
                log(f"retro: transcrito {arq}: {type(e).__name__}: {e}")
        for regra, a in achados.items():
            casos.append({"regra": regra, "ts": a["ts"] or d.get("ts"), "task": d.get("task"), "dispatch": d.get("dispatch"), "titulo": d.get("titulo"), "modelo": d.get("modelo"),
                          "effort": d.get("effort"), "onde": d.get("worktree"), "ponteiro": a["ponteiro"],
                          "detalhe": f"{regra}: {_cita(' '.join(a['texto'].split()), 100)}" + (f" (+{a['n'] - 1} vezes)" if a["n"] > 1 else "")})
    return casos


def _retro_pr_checks(url):
    """{falhos: [nome dos checks vermelhos], revisao} do PR pelo gh, ou None sem gh, sem rede ou sem resposta. É o estado de agora, não o da entrega."""
    try:
        r = subprocess.run([GH, "pr", "view", url, "--json", "statusCheckRollup,reviewDecision"], capture_output=True, text=True, timeout=PR_GH_S)
        d = json.loads(r.stdout) if r.returncode == 0 else None
    except (subprocess.TimeoutExpired, OSError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    return {"falhos": [c.get("name") or c.get("context") or "?" for c in d.get("statusCheckRollup") or []
                       if isinstance(c, dict) and (c.get("conclusion") in RETRO_VERMELHO or c.get("state") in RETRO_VERMELHO)],
            "revisao": d.get("reviewDecision") or ""}


def retro_coleta(events, desde, ate, turnos=None, projetos=None, pr_checks=None, projeto=None):
    """Os sinais de falha de [desde, ate) (carimbos `AAAA-MM-DDTHH:MM:SSZ`), sem LLM: {desde, ate, sinais {nome: {rotulo, n, casos}}, falhas, por_modelo}.

    Cada caso leva título, modelo, effort e um ponteiro (linha do events.jsonl quando o evento traz `_l`, arquivo:linha do transcrito, URL do PR).
    `n` None quer dizer "não consultado": sem `turnos` e `projetos` os transcritos não são lidos, sem `pr_checks` o gh não é chamado, e isso não vira zero.
    `projeto` guarda só os casos cujo caminho, título ou task contém o trecho."""
    ev = [e for e in events if desde <= (e.get("ts") or "") < ate]
    por_disp, por_task = {}, {}
    for d in events:
        if d.get("tipo") == "despacho":
            por_disp[d.get("dispatch")], por_task[d.get("task")] = d, d
    casos = {k: [] for k in RETRO_SINAIS}
    lidos = {e.get("msg_id") for e in events if e.get("tipo") == "steer_fim" and e.get("motivo") == "lido"}
    fins = [(e.get("dispatch"), e["ts"]) for e in events if e.get("tipo") in ("worker_done", "fim_dispatch") and e.get("ts")]
    entregas = sorted(_dt(e["ts"]).timestamp() for e in events if e.get("ts") and (e.get("tipo") == "worker_done" or e.get("origem") == "relatorio_worker"))
    install = re.compile(re.escape(ORQ_INSTALL) + r"(?![\w-])")

    def add(nome, e, detalhe, ponteiro=None):
        d = por_disp.get(e.get("dispatch")) or por_task.get(e.get("task")) or {}
        onde = next((x for x in (e.get("caminho"), e.get("worktree"), d.get("worktree")) if x and x != "current"), None)
        casos[nome].append({"ts": e.get("ts"), "task": e.get("task") or d.get("task"), "dispatch": e.get("dispatch") or d.get("dispatch"), "titulo": d.get("titulo"),
                            "modelo": d.get("modelo"), "effort": d.get("effort"), "onde": onde, "detalhe": detalhe,
                            "ponteiro": ponteiro or (f"events.jsonl:{e['_l']}" if e.get("_l") else f"events.jsonl@{e.get('ts')}")})

    avisos_gate, prs = {}, {}
    for e in ev:
        t = e.get("tipo")
        if t == "nao_iniciou":
            nota = next((x.get("nota") for x in events if x.get("tipo") == "controle" and x.get("acao") == "relancar" and x.get("dispatch") == e.get("dispatch") and x.get("nota")), None)
            add("nao_iniciou", e, nota or "o prompt do spec não entrou, nem depois do Enter")
        elif t == "controle":
            if e.get("resultado") == "falhou":
                add("controle_falhou", e, f"{e.get('acao')}: {_cita(str(e.get('erro') or e.get('passo') or ''), 100)}")
            elif e.get("acao") == "relancar" and e.get("resultado") == "iniciado":
                add("retry", e, e.get("nota") or "relançado sem nota")
            elif e.get("acao") in ("interromper", "encerrar"):
                add("intervencao", e, f"{e.get('acao')}: {_cita(str(e.get('motivo') or e.get('aviso') or ''), 100)}")
        elif t == "steer" and e.get("msg_id") and e["msg_id"] not in lidos:
            depois = any(d == e.get("dispatch") and ts > e["ts"] for d, ts in fins)
            add("steer_sem_leitura", e, ("o worker entregou depois, sem leitura provada do ajuste: " if depois else "sem steer_fim lido: ") + _cita(str(e.get("texto") or ""), 80))
        elif t == "steer_reentrega":
            add("steer_reentregue", e, f"tentativa {e.get('tentativa')}")
        elif t == "resposta_worker":
            add("pergunta_de_worker", e, _cita(str(e.get("texto") or ""), 100))
        elif t == "worker_done" and e.get("outcome") != "succeeded":
            add("worker_falhou", e, f"{e.get('outcome')}: {_cita(str(e.get('subject') or ''), 100)}")
        elif t == "fim_dispatch":
            if e.get("motivo") in ("sem worker_done", "falhou", "motivo desconhecido"):
                add("sem_entrega", e, str(e.get("motivo")))
            if (e.get("sujo") or 0) > 0:
                add("liberado_sujo", e, f"{e['sujo']} arquivo(s) em {e.get('caminho')}")
            if (e.get("sem_push") or 0) > 0:
                add("liberado_sem_push", e, f"{e['sem_push']} commit(s) em {e.get('caminho')} (o ticket pede 'sem push': conferir)")
            if e.get("caminho") and os.path.realpath(e["caminho"]) == ORQ_INSTALL and ((e.get("sujo") or 0) or (e.get("sem_push") or 0)):
                add("checkout_em_uso", e, f"worker liberado em {e['caminho']}")
        elif t == "entrega" and e.get("avisos"):
            add("entrega_com_aviso", e, "; ".join(map(str, e["avisos"])))
            if any(install.search(str(a)) for a in e["avisos"]):
                add("checkout_em_uso", e, "o aviso da entrega cita o checkout em uso")
        elif t == "gate_aviso":
            for i in e.get("abertas") or []:
                avisos_gate.setdefault(i, e)
        elif t == "intake" and e.get("efeito") == "descartado":
            add("intake_descartado", e, f"{e.get('entrada')}: {_cita(str(e.get('nota') or ''), 100)}")
        elif t == "alerta":
            add("alerta", e, str(e.get("alerta")))
        elif t == "binding_perdido":
            add("binding_perdido", e, f"sessão {e.get('sessao')}")
        elif t == "entrada" and e.get("origem") == "usuario" and RETRO_CORRECAO.match(str(e.get("texto") or "")) and entregas:
            agora = _dt(e["ts"]).timestamp()
            antes = [x for x in entregas if x <= agora]
            if antes and agora - antes[-1] <= RETRO_CORRECAO_S:
                add("correcao_do_usuario", e, _cita(" ".join(str(e["texto"]).split()), 100))
        elif t == "pr" and e.get("op") == "ligar" and e.get("url"):
            prs.setdefault(e["url"], e)
    for i, e in avisos_gate.items():
        add("entrada_sem_tratamento", e, f"entrada {i} ficou sem efeito no fim de um turno")
    if turnos is not None and projetos is not None:
        casos["regra_violada"] = _retro_regras([d for d in events if d.get("tipo") == "despacho" and desde <= (d.get("ts") or "") < ate], turnos, projetos)
    else:
        casos["regra_violada"] = None
    if pr_checks is None:
        casos["pr_ci_vermelho"] = casos["pr_pediu_mudanca"] = None
    else:
        with ThreadPoolExecutor(4) as ex:
            for (url, e), c in zip(prs.items(), ex.map(pr_checks, prs)):
                if not c:
                    continue
                if c.get("falhos"):
                    add("pr_ci_vermelho", e, f"PR #{e.get('numero')}: {', '.join(c['falhos'])}", url)
                if c.get("revisao") == "CHANGES_REQUESTED":
                    add("pr_pediu_mudanca", e, f"PR #{e.get('numero')}: review pediu mudança", url)
    if projeto:
        achar = projeto.lower()
        for k, v in casos.items():
            if v is not None:
                casos[k] = [c for c in v if achar in " ".join(str(c.get(x) or "") for x in ("onde", "titulo", "task")).lower()]
    por_modelo = {}
    for e in ev:
        if e.get("tipo") == "despacho":
            por_modelo.setdefault(f"{e.get('modelo')}/{e.get('effort')}", {"despachos": 0, **dict.fromkeys(RETRO_POR_MODELO, 0)})["despachos"] += 1
    for k in RETRO_POR_MODELO:
        for c in casos[k]:
            if c.get("modelo"):
                por_modelo.setdefault(f"{c['modelo']}/{c['effort']}", {"despachos": 0, **dict.fromkeys(RETRO_POR_MODELO, 0)})[k] += 1
    sinais = {k: {"rotulo": RETRO_SINAIS[k], "n": None if v is None else len(v), "casos": v or []} for k, v in casos.items()}
    return {"desde": desde, "ate": ate, "sinais": sinais, "falhas": sum(s["n"] or 0 for s in sinais.values()), "por_modelo": por_modelo}


def _retro_eventos():
    """O events.jsonl com a linha de cada evento em `_l` (o ponteiro de cada caso)."""
    out = []
    try:
        with open(_path("events.jsonl")) as f:
            for n, linha in enumerate(f, 1):
                try:
                    e = json.loads(linha)
                except ValueError:
                    continue
                if isinstance(e, dict):
                    out.append({**e, "_l": n})
    except OSError:
        pass
    return out


def _retro_rodadas():
    """As rodadas gravadas em ORQ_HOME/retro, da mais antiga para a mais nova: {desde, ate, falhas, metricas}."""
    rs = [_read_json(p) for p in sorted(glob.glob(_path(os.path.join(RETRO_DIR, "*.json"))))]
    return sorted((r for r in rs if isinstance(r, dict) and r.get("ate")), key=lambda r: r["ate"])


def retro_gravar(r):
    """Grava as métricas da rodada (só os números, nunca os casos) em ORQ_HOME/retro/<ate>.json. Devolve o caminho."""
    os.makedirs(_path(RETRO_DIR), exist_ok=True)
    caminho = _path(os.path.join(RETRO_DIR, _dt(r["ate"]).strftime("%Y-%m-%dT%H%M") + ".json"))
    _write_json(caminho, {"desde": r["desde"], "ate": r["ate"], "falhas": r["falhas"], "metricas": {k: v["n"] for k, v in r["sinais"].items()}}, indent=2)
    return caminho


def retro_texto(r, anterior=None, por_caso=5):
    """A rodada em texto curto: a tabela de sinais (com a rodada gravada ao lado), os casos com ponteiro e o quadro por modelo e effort."""
    ant = _dict(_dict(anterior).get("metricas"))
    ls = [f"retro {r['desde'][:10]} a {r['ate'][:10]}", f"falhas no período: {r['falhas']}" + (f" (rodada gravada até {anterior['ate'][:10]}: {anterior['falhas']})" if anterior else "")]
    ls.append(f"{'sinal':<24}{'n':>5}" + ("  antes" if anterior else ""))
    for k, s in r["sinais"].items():
        ls.append(f"{k:<24}{'n/d' if s['n'] is None else s['n']:>5}" + (f"  {'n/d' if ant.get(k, '-') is None else ant.get(k, '-')}" if anterior else ""))
    for k, s in r["sinais"].items():
        if s["n"]:
            ls += ["", f"{k} ({s['n']}): {s['rotulo']}"]
            ls += [f"  {(c['ts'] or '')[:16]} {c.get('titulo') or c.get('task') or ''} [{c.get('modelo') or '?'}/{c.get('effort') or '?'}] {c['detalhe']} -> {c['ponteiro']}" for c in s["casos"][:por_caso]]
            ls += [f"  +{s['n'] - por_caso} a mais (--json)"] if s["n"] > por_caso else []
    if r["por_modelo"]:
        ls += ["", f"{'modelo/effort':<28}{'desp':>5}" + "".join(f"{k[:9]:>10}" for k in RETRO_POR_MODELO)]
        ls += [f"{m:<28}{v['despachos']:>5}" + "".join(f"{v[k]:>10}" for k in RETRO_POR_MODELO) for m, v in sorted(r["por_modelo"].items())]
    return "\n".join(ls)


def cmd_retro(a):
    """`orq retro`: coleta a janela (padrão: os últimos 7 dias), imprime e, com --gravar, guarda as métricas para a rodada seguinte comparar."""
    ate = _dt(a.ate).strftime("%Y-%m-%dT%H:%M:%SZ") if a.ate else now()
    desde = _dt(a.desde).strftime("%Y-%m-%dT%H:%M:%SZ") if a.desde else (_dt(ate) - timedelta(days=RETRO_DIAS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    r = retro_coleta(_retro_eventos(), desde, ate, None if a.sem_transcritos else _turnos_ro(), None if a.sem_transcritos else PROJETOS,
                     None if a.sem_gh else _retro_pr_checks, a.projeto)
    anterior = next((x for x in reversed(_retro_rodadas()) if x["ate"] < ate), None)
    if a.gravar:
        retro_gravar(r)
    return json.dumps(r, ensure_ascii=False) if a.json else retro_texto(r, anterior)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="orq")
    sub = ap.add_subparsers(dest="cmd", required=True)
    hk = sub.add_parser("hook")
    hk.add_argument("kind", choices=list(HOOKS))
    hk.add_argument("harness", nargs="?", default="claude", choices=HARNESSES, help="de que agente vem o hook (o padrão é o dos hooks instalados no Claude)")
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
    pr = sub.add_parser("pr", help="PRs de cada feature ligados à task: ligar, lista, desligar, poll (o poll roda fora dos hooks)").add_subparsers(dest="op", required=True)
    pl2 = pr.add_parser("ligar", help="orq pr ligar <task> <url> [--issue N]: registra o PR da feature (development, staging, main ou merge/)")
    pl2.add_argument("task")
    pl2.add_argument("url")
    pl2.add_argument("--issue", type=int, help="número da issue do GitHub, quando houver")
    pa = pr.add_parser("auto", help="orq pr auto <url> [--head B] [--wt DIR]: liga o PR à task dona da branch; sem dona, vai para 'PR sem tarefa' (o hook prligar chama)")
    pa.add_argument("url")
    pa.add_argument("--head")
    pa.add_argument("--wt")
    pa.add_argument("--cwd", help="onde achar a worktree da branch quando o --head não veio (a branch sai do gh pr view)")
    pl2.add_argument("--tag", help="a etiqueta da feature no digest (segurança, failover, …)")
    pl2.add_argument("--nota", help="o que o PR faz, em uma ou duas frases, para o digest")
    pr.add_parser("lista").add_argument("--task")
    pd2 = pr.add_parser("desligar")
    pd2.add_argument("task")
    pd2.add_argument("url")
    pr.add_parser("poll", help="pergunta ao gh pelos PRs abertos; merge ou fechamento vira uma entrada, uma vez").add_argument("--forcar", action="store_true", help="ignora o intervalo mínimo")
    dg = sub.add_parser("digest", help="grava digest/atual.json (o contrato do painel): fila de merge, features, pendências, o que aconteceu e workers vivos")
    dg.add_argument("--desde", help="carimbo ISO (AAAA-MM-DDTHH:MM:SSZ) em vez do momento em que o modo ausente ligou ou da última mensagem do usuário")
    dg.add_argument("--html", action="store_true", help="grava também a página digest/<data>.html")
    dg.add_argument("--abrir", action="store_true", help="grava a página e a abre numa aba do Orca")
    fi = sub.add_parser("fila", help="a ordem de merge que o coordenador declara: add, feito, rm, lista").add_subparsers(dest="op", required=True)
    fa = fi.add_parser("add", help="orq fila add --passo N --nome <nome> --por <por quê> <PR>…: declara (ou troca) o passo; os PRs já precisam estar ligados")
    fa.add_argument("--passo", type=int, required=True)
    fa.add_argument("--nome", required=True)
    fa.add_argument("--por", required=True)
    fa.add_argument("prs", nargs="+", type=int, help="números dos PRs do passo")
    fi.add_parser("feito", help="marca o passo como feito à mão").add_argument("passo", type=int)
    fi.add_parser("rm", help="tira o passo da fila").add_argument("passo", type=int)
    fi.add_parser("lista")
    au = sub.add_parser("ausente", help="modo ausente: orq ausente ligar | desligar | (sem op: estado); o Stop de cada resposta atualiza o digest")
    au.add_argument("op", nargs="?", choices=["ligar", "desligar"])
    aw = sub.add_parser("away", help="alias em inglês do modo ausente: orq away [on|off|status]; sem op alterna; ao desligar mostra o link do painel")
    aw.add_argument("op", nargs="?", choices=["on", "off", "status", "ligar", "desligar"])
    sub.add_parser("steers", help="reentrega o aviso dos ajustes que o worker parado não leu e grava o alerta na terceira falha (o painel do gerente já faz)")
    rp = sub.add_parser("responder", help="responde a pergunta de um worker pelo gerente, ligando o Run da mensagem antes")
    rp.add_argument("msg_id")
    rp.add_argument("texto")
    sub.add_parser("status")
    sub.add_parser("ocupadas", help="os caminhos das worktrees com worker vivo, um por linha (o limpar-mergeados não apaga essas)")
    rs = sub.add_parser("resumo", help="as quatro partes (com você, entrou, anda, vem) e as decisões desde a última mensagem do usuário")
    rs.add_argument("--desde", help="carimbo ISO (AAAA-MM-DDTHH:MM:SSZ) em vez da última mensagem do usuário")
    rs.add_argument("--noite", action="store_true", help="o cartão da manhã da última noite (até 40 linhas)")
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
    rt = sub.add_parser("responder-tela", help="digita a opção de um menu preso na tela do worker (permissão, AskUserQuestion, trust)")
    rt.add_argument("task")
    rt.add_argument("opcao", help="o número da opção ou o começo do rótulo")
    rt.add_argument("--run")
    en = sub.add_parser("encerrar", help="worker-stop (se ainda roda) e release, com o motivo no log")
    en.add_argument("dispatch")
    en.add_argument("--motivo", required=True)
    en.add_argument("--parada", choices=list(PARADAS), help="nomeia a parada no cartão da manhã (parou: orçamento|decisão pendente|limite de uso)")
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
    de.add_argument("--agente", default="claude", help=f"o harness do worker ({', '.join(HARNESSES)}; o padrão é claude)")
    de.add_argument("--modelo", required=True)
    de.add_argument("--effort", required=True)
    de.add_argument("--worktree", choices=["current", "new-top-level"])
    de.add_argument("--name")
    de.add_argument("--base-branch")
    de.add_argument("--entrada")
    de.add_argument("--prioridade", type=int, choices=[1, 2, 3], help="1 alta a 3 baixa; sem ela vale a da frente do título (segurança e produção 1, failover, diagnóstico e painel 3)")
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
    gl.add_argument("--assumir", action="store_true", help="toma o gerente.json de outro coordenador que ainda aparece no Orca, com os Runs dele")
    gd = ge.add_parser("desligar", help="no coordenador: devolve um Run (--run) ou todos a este terminal")
    gd.add_argument("--run")
    gd.add_argument("--assumir", action="store_true", help="desliga também o gerente.json de outro coordenador que ainda aparece no Orca")
    ge.add_parser("checar", help="confere no Orca se o terminal do agent manager ainda existe (o prompt do coordenador chama, em segundo plano)")
    gs = ge.add_parser("subir", help="no coordenador: o terminal do agent manager sumiu; cria outro com o painel e religa todos os Runs do gerente.json")
    gs.add_argument("--forcar", action="store_true", help="sobe mesmo com o terminal antigo ainda no Orca")
    ge.add_parser("absorver", help="no terminal do agent manager: confirma heartbeat e avisa o coordenador do resto, Run por Run")
    rt = sub.add_parser("retomar", help="depois de uma queda: sobe o agent manager e retoma, com claude --resume, os workers sem worker_done que perderam o terminal")
    rt.add_argument("--dry-run", action="store_true", help="só lista")
    rt.add_argument("--run", help="só os dispatches deste Run")
    rt.add_argument("--json", action="store_true")
    rt.add_argument("--pausados", action="store_true", help="sobe os workers que o orq pausar parou (em vez dos que caíram); recusa com o uso ainda alto")
    rt.add_argument("--forcar", action="store_true", help="com --pausados, sobe mesmo com o uso acima do limiar")
    hb = sub.add_parser("hibernar", help="fecha o terminal de um worker ocioso e guarda a sessão (o gerente faz sozinho, com o critério do README); o steer, o responder e o orq acordar o trazem de volta")
    hb.add_argument("alvo", help="id da task ou do dispatch")
    hb.add_argument("--run")
    hb.add_argument("--forcar", action="store_true", help="passa a falta de prova de processo filho (ps/lsof sem achar o agente); nunca passa processo filho vivo, tela ocupada nem coordenador")
    hb.add_argument("--json", action="store_true")
    ac = sub.add_parser("acordar", help="sobe de volta, com o resume do harness, o worker hibernado (task ou dispatch)")
    ac.add_argument("alvo")
    ac.add_argument("--texto", help="o que o worker recebe ao acordar (o padrão é 'acordado à mão')")
    ac.add_argument("--json", action="store_true")
    pz = sub.add_parser("pausar", help="pausa workers para abrir folga no plano: PAUSA.md, fecha o terminal, grava a pausa. Sem argumento: prioridade baixa e os que só investigam")
    pz.add_argument("tasks", nargs="*", help="ids de task ou de dispatch; sem eles vale o critério de prioridade")
    pz.add_argument("--ate-prioridade", type=int, choices=[1, 2, 3], help="pausa as de prioridade N e mais baixas (3 = só as baixas)")
    pz.add_argument("--run")
    pz.add_argument("--dry-run", action="store_true", help="só lista")
    pz.add_argument("--json", action="store_true")
    pr_ = sub.add_parser("prioridade", help="troca a prioridade (1 alta a 3 baixa) de uma task; o orq agentes, o digest, o orq pausar e a recusa por orçamento usam essa ordem")
    pr_.add_argument("task")
    pr_.add_argument("valor", type=int, choices=[1, 2, 3])
    uz = sub.add_parser("uso", help="o uso do plano (semana e janela de 5 h) lido do HUD, o nível e a decisão sobre novos despachos")
    uz.add_argument("--json", action="store_true")
    uz.add_argument("--agente", default="claude", choices=HARNESSES, help="de que plano (as cotas do Claude e do Codex são separadas)")
    mq = sub.add_parser("maquina", help="o orçamento da máquina (ticket 79): o que ela tem agora, o que o orq decide e as vagas. `orq maquina set <chave> <valor>` ajusta o maquina.json")
    mq.add_argument("op", nargs="?", choices=["set"])
    mq.add_argument("chave", nargs="?")
    mq.add_argument("valor", nargs="?", help="em JSON: 4, true, [\"claude-opus-*\"]")
    mq.add_argument("--json", action="store_true")
    fd = sub.add_parser("fila-despacho", help="os despachos e retomadas que esperam vaga na máquina: lista | rm <id>").add_subparsers(dest="op", required=True)
    fd.add_parser("lista").add_argument("--json", action="store_true")
    fd.add_parser("rm").add_argument("id")
    ru = sub.add_parser("runs", help="os Runs com trabalho aberto ou recentes (--todos: o arquivo e os de teste)")
    ru.add_argument("--todos", action="store_true")
    ru.add_argument("--json", action="store_true")
    gp = sub.add_parser("grupos", help="os grupos (ORQ_HOME/groups/*.json) e os mates; com --titulo/--cwd/--grupo diz para qual grupo o pedido vai")
    gp.add_argument("--titulo")
    gp.add_argument("--cwd")
    gp.add_argument("--grupo")
    mt = sub.add_parser("mate", help="o secondmate de um grupo: abrir | pedir | subir | pedidos").add_subparsers(dest="op", required=True)
    mt.add_parser("abrir").add_argument("grupo")
    mp = mt.add_parser("pedir")
    mp.add_argument("grupo")
    mp.add_argument("--texto", required=True)
    mp.add_argument("--prazo", type=int, default=PRAZO_PEDIDO_S, help="segundos do fim do turno do mate até a repostagem; 0: não espera resposta")
    mp.add_argument("--responde", help="a entrada que o mate subiu e este pedido responde: fecha com o efeito mate")
    ms = mt.add_parser("subir")
    ms.add_argument("--tipo", required=True, choices=TIPOS_SUBIDA)
    ms.add_argument("--texto", required=True)
    ms.add_argument("--corr")
    ms.add_argument("--link")
    ms.add_argument("--grupo")
    mt.add_parser("pedidos").add_argument("--grupo")
    sub.add_parser("ingest").add_argument("--refresh", action="store_true", help="depois do ingest, refaz o aberto.json")
    rr = sub.add_parser("retro", help="os sinais de falha dos eventos, transcritos e PRs de uma janela (padrão 7 dias), sem LLM: quantos, quais casos, em que modelo")
    rr.add_argument("--desde", help="início da janela (data ou ISO)")
    rr.add_argument("--ate", help="fim da janela (padrão: agora)")
    rr.add_argument("--projeto", help="só os casos cujo caminho, título ou task contém o trecho")
    rr.add_argument("--json", action="store_true")
    rr.add_argument("--sem-gh", action="store_true", help="não pergunta ao gh pelo CI e pelo review dos PRs")
    rr.add_argument("--sem-transcritos", action="store_true", help="não lê os transcritos dos workers (regras violadas)")
    rr.add_argument("--gravar", action="store_true", help="guarda as métricas em ORQ_HOME/retro para a próxima rodada comparar")
    args = sys.argv[1:] if argv is None else argv
    if args[:1] == ["hook"]:
        try:
            a = ap.parse_args(args)
        except SystemExit as e:  # fail-open: no Codex a saída 2 do argparse bloquearia o prompt do usuário
            if e.code:
                log(f"hook com argumentos inválidos: {args}")
            return 0
    else:
        a = ap.parse_args(args)
    if a.cmd == "hook":
        return run_hook(a.kind, a.harness)
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
        elif a.cmd == "pr":
            if a.op == "ligar":
                print(json.dumps(pr_ligar(a.task, a.url, a.issue, a.tag, a.nota), ensure_ascii=False))
            elif a.op == "auto":
                print(json.dumps(pr_auto(a.url, a.head, a.wt, a.cwd), ensure_ascii=False))
            elif a.op == "desligar":
                pr_desligar(a.task, a.url)
                print(f"PR desligado de {a.task}")
            elif a.op == "lista":
                print("\n".join(pr_lista(a.task)) or "nenhum PR ligado")
            else:
                print("\n".join(pr_poll(forcar=a.forcar)) or "nenhuma mudança nos PRs")
        elif a.cmd == "steer":
            print(json.dumps(steer(a.task, a.texto, a.run, a.entrada), ensure_ascii=False))
        elif a.cmd == "steers":
            print("\n".join(reentrega_steers()) or "nenhum ajuste a reentregar")
        elif a.cmd == "responder":
            print(json.dumps(responder(a.msg_id, a.texto), ensure_ascii=False))
        elif a.cmd == "alerta":
            print(json.dumps(append_event({"tipo": "alerta_visto", "task": a.task}), ensure_ascii=False))
        elif a.cmd == "ocupadas":
            print("\n".join(sorted(worktrees_ocupadas())))
        elif a.cmd == "status":
            print("\n".join([estado(), *linhas_pr(), *linhas_worktrees(), *filter(None, [linha_e2e(fila_e2e()), linha_maquina()])]))
        elif a.cmd == "resumo" and a.noite:
            print(cartao_manha())
        elif a.cmd == "resumo":
            if a.desde:
                _dt(a.desde)  # ValueError vira exit 1
            print(resumo_quatro(read_events(), _read_json(_path("aberto.json")), _read_json(PEND), tickets(), a.desde and _dt(a.desde).strftime("%Y-%m-%dT%H:%M:%SZ"), painel=aviso_painel()))
        elif a.cmd == "agentes":
            ags = agentes(a.run, a.todos)
            parada = [l for l in linhas_noite(_cursor_ro(), read_events()) if "Parou de despachar" in l]
            print(json.dumps(ags, ensure_ascii=False) if a.json else "\n".join([texto_agentes(ags), *linhas_hibernacao(), *parada]))
        elif a.cmd == "runs":
            rs = runs_lista(a.todos)
            print(json.dumps(rs, ensure_ascii=False) if a.json else texto_runs(rs))
        elif a.cmd == "liberar":
            r = liberar(a.dispatch, a.run)
            print(json.dumps(r, ensure_ascii=False))
            if r["aviso"]:
                print(f"aviso: {r['aviso']}", file=sys.stderr)
        elif a.cmd == "responder-tela":
            print(json.dumps(responder_tela(a.task, a.opcao, a.run), ensure_ascii=False))
        elif a.cmd in ("interromper", "encerrar", "relancar"):
            r = interromper(a.dispatch, a.run) if a.cmd == "interromper" else encerrar(a.dispatch, a.motivo, a.run, a.parada) if a.cmd == "encerrar" \
                else relancar(a.dispatch, a.nota, a.modelo, a.effort, a.run)
            print(json.dumps(r, ensure_ascii=False))
            if r.get("aviso"):
                print(f"aviso: {r['aviso']}", file=sys.stderr)
        elif a.cmd == "digest":
            d, arq, pagina = digest_gerar(desde=_dt(a.desde).strftime("%Y-%m-%dT%H:%M:%SZ") if a.desde else None, com_html=a.html or a.abrir)
            print("\n".join(x for x in (arq, pagina) if x))
            print(f"{sum(i['estado'] == 'OPEN' for g in d['fila'] for i in g['prs'])} PR(s) aberto(s), {len(d['pendencias'])} pendência(s), "
                  f"{len(d['rodando'])} rodando, {len(d['linha']) + d['pagina']['linha_antes']} no que aconteceu")
            if a.abrir:
                try:
                    digest_abrir(pagina)
                except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as e:
                    print(f"aviso: a aba não abriu ({e}); abra {pagina}", file=sys.stderr)
        elif a.cmd == "fila":
            if a.op == "add":
                print(json.dumps(fila_add(a.passo, a.nome, a.por, a.prs), ensure_ascii=False))
            elif a.op in ("feito", "rm"):
                fila_marca(a.passo, a.op)
                print(f"passo {a.passo}: {'marcado feito' if a.op == 'feito' else 'tirado da fila'}")
            else:
                print("\n".join(fila_lista()) or "nenhum passo declarado")
        elif a.cmd == "ausente":
            if a.op == "ligar":
                ausente_ligar()
            elif a.op == "desligar":
                ausente_desligar()
            print("\n".join(linhas_ausente(_cursor_ro())))
        elif a.cmd == "away":
            print("\n".join(away(a.op)))
        elif a.cmd == "noite":
            if a.op == "ligar":
                noite_ligar(a.ate, a.max_despachos, a.max_falhas)
            elif a.op == "desligar":
                print("modo noite desligado" if noite_desligar() else "modo noite já estava desligado")
                return 0
            print("\n".join(linhas_noite(_cursor_ro(), read_events())) or "modo noite desligado")
        elif a.cmd == "despachar":
            r = despachar(a.run, a.titulo, a.spec_arquivo, a.modelo, a.effort, a.worktree, a.name, a.base_branch, a.entrada, a.ticket, a.prioridade, a.agente)
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
                print(json.dumps(gerente_ligar(a.terminal, a.run, a.assumir), ensure_ascii=False))
            elif a.op == "desligar":
                print(json.dumps(gerente_desligar(a.run, a.assumir), ensure_ascii=False))
            elif a.op == "checar":
                print(json.dumps(gerente_checar(), ensure_ascii=False))
            elif a.op == "subir":
                print(json.dumps(gerente_subir(a.forcar), ensure_ascii=False))
            else:
                print(gerente_absorver())
        elif a.cmd == "retomar" and a.pausados:
            r = {"gerente": None, "workers": retomar_pausados(a.run, a.forcar)}
            print(json.dumps(r, ensure_ascii=False) if a.json else texto_retomar(r))
        elif a.cmd == "retomar":
            r = retomar(a.dry_run, a.run)
            print(json.dumps(r, ensure_ascii=False) if a.json else texto_retomar(r))
        elif a.cmd == "hibernar":
            r = hibernar(a.alvo, a.run, a.forcar)
            print(json.dumps(r, ensure_ascii=False) if a.json else texto_hibernado(r))
        elif a.cmd == "acordar":
            r = acordar(a.alvo, a.texto)
            print(json.dumps(r, ensure_ascii=False) if a.json else texto_retomar({"gerente": None, "workers": [r]}))
            if r["estado"] in ("falhou", "sem_worktree"):
                return 1
        elif a.cmd == "pausar":
            r = pausar(a.tasks, a.ate_prioridade, a.run, a.dry_run)
            print(json.dumps(r, ensure_ascii=False) if a.json else texto_pausar(r))
        elif a.cmd == "prioridade":
            print(json.dumps(prioridade_definir(a.task, a.valor), ensure_ascii=False))
        elif a.cmd == "grupos" and (a.titulo or a.cwd or a.grupo):
            nome, motivo = grupo_de(grupos(), a.titulo, a.cwd, a.grupo)
            t = nome and _dict(_mates().get(nome)).get("terminal")
            vivos = _terminais_vivos() if t else None
            print(json.dumps({"grupo": nome, "motivo": motivo, "mate": t if t and (vivos is None or t in vivos) else None}, ensure_ascii=False))
        elif a.cmd == "grupos":
            print(texto_grupos(grupos(), _mates(), _terminais_vivos() if _mates() else None, read_events(), datetime.now(timezone.utc)))
        elif a.cmd == "mate" and a.op == "abrir":
            print(json.dumps(mate_abrir(a.grupo), ensure_ascii=False))
        elif a.cmd == "mate" and a.op == "pedir":
            print(json.dumps(mate_pedir(a.grupo, a.texto, a.prazo, a.responde), ensure_ascii=False))
        elif a.cmd == "mate" and a.op == "subir":
            print(json.dumps(mate_subir(a.tipo, a.texto, a.corr, a.link, a.grupo), ensure_ascii=False))
        elif a.cmd == "mate":
            ps = [p for p in mate_pendentes(read_events(), _mates(), datetime.now(timezone.utc)) if not a.grupo or p["grupo"] == a.grupo]
            print("\n".join(f"{p['corr']} {p['grupo']} {p['estado']}: {_cita(p['texto'])}" for p in ps) or "nenhum pedido sem resposta")
        elif a.cmd == "maquina" and a.op == "set":
            if not a.chave or a.valor is None:
                raise ValueError("orq maquina set <chave> <valor>")
            print(json.dumps(maquina_definir(a.chave, a.valor), ensure_ascii=False))
        elif a.cmd == "maquina":
            cfg, leitura = maquina_cfg(), maquina_ler()
            nivel, motivo = maquina_nivel(leitura, cfg)
            ocup = maquina_ocupacao()
            print(json.dumps({"config": cfg, "leitura": leitura, "nivel": nivel, "motivo": motivo, "vivos": ocup["vivos"], "fila": fila_despacho_itens()}, ensure_ascii=False)
                  if a.json else "\n".join(texto_maquina(cfg, leitura, ocup)))
        elif a.cmd == "fila-despacho" and a.op == "rm":
            if not fila_despacho_rm(a.id):
                raise ValueError(f"{a.id} não está na fila de despacho")
            print(f"{a.id} saiu da fila de despacho")
        elif a.cmd == "fila-despacho":
            itens = fila_despacho_itens()
            print(json.dumps(itens, ensure_ascii=False) if a.json else "\n".join(
                f"{n} {i['id']} P{i.get('prioridade') or 2} {i['tipo']} {i.get('titulo')} ({i.get('modelo')}) desde {i.get('ts')}: {i.get('motivo')}" for n, i in enumerate(itens, 1)) or "fila de despacho vazia")
        elif a.cmd == "uso":
            u = uso_plano(agente=a.agente)
            nivel, motivo, _ = uso_nivel(u)
            print(json.dumps({"uso": u, "nivel": nivel, "motivo": motivo}, ensure_ascii=False) if a.json else
                  f"{nivel}" + (f": {motivo}" if motivo else "") + (f" (semana {u['semana']}%, 5 h {u['cinco_h']}%)" if u else " (sem quadro fresco do HUD)"))
        elif a.cmd == "auditar-respostas":
            print(auditar_respostas(a.sessao), end="")
        elif a.cmd == "retro":
            print(cmd_retro(a))
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
