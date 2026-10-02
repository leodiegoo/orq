// Lê os arquivos do ORQ_HOME e monta os blocos da TUI como texto. Só leitura: nada aqui grava nem chama o Orca.
// O serve (orq gerente serve) grava manager-state.json a cada volta com os workers e a máquina; sem ele, os workers vêm do open.json.
// O disco está em inglês (fase 2 da migração); a TUI lê com as chaves pt, como o orq: paraPt troca as chaves e valores pelo mapa do orqlib.py.
import { existsSync, openSync, readFileSync, readSync, readdirSync, statSync, closeSync } from "node:fs"
import { homedir } from "node:os"
import { basename, dirname, join, resolve } from "node:path"
import type { Cor } from "./tema"

type Json = Record<string, any>

/** Um trecho de linha com cor semântica (a paleta do tema dá o hex); `string` solta é texto sem cor. */
export type Trecho = { t: string; c?: Cor; b?: boolean }
export type Linha = string | Trecho[]
export type Bloco = { id: string; titulo: string; linhas: Linha[] }

export const texto = (l: Linha): string => (typeof l === "string" ? l : l.map((x) => x.t).join(""))

// item de backlog.py (a gramática do tasks-axi mora lá; o contrato com o CLI real é testado no test_orq.py), mais `grupo`, `task` (a do `orca:`) e os dois estados derivados
export type Item = {
  id: string
  titulo: string
  estado: "queued" | "in_flight" | "done"
  repo: string | null
  prioridade: number | null
  since: string | null
  closed: string | null
  bloqueios: string[]
  hold: Json | null
  grupo: string
  task: string | null
  bloqueado: boolean
  retido: boolean
}
export type Situacao = "pronto" | "andamento" | "bloqueado" | "retido" | "feito"

export type Estado = {
  agora: number
  gerente: Json | null // manager-state.json
  vivoMs: number | null // mtime do carimbo manager-alive
  servePid: number | null // pid escrito no gerente-serve.pid (a trava só o serve sabe; a TUI confia no carimbo)
  aberto: Json | null
  digest: Json | null
  integrar: Json[]
  despacho: Json[]
  maquinaCfg: Json
  eventos: Json[]
  backlog: Item[] | null // null: sem backlog ligado (backlog.path ou ORQ_BACKLOG)
  backlogErro?: string
}

const VIVO_S = 90 // o PAINEL_LIMITE_MIN_S do orq: carimbo mais velho que isso é gerente parado
const ESTADO_FRESCO_S = 60
const ANDA = new Set(["rodando", "perguntando", "travado", "parado", "nao_comecou", "sem_terminal", "aguardando_integracao", "hibernado"])
const RUIDO = new Set(["intake", "entrada", "heartbeat_absorvido", "gate_aviso"])

type Mapa = { chaves: Record<string, string>; valores: Record<string, Record<string, string>>; arquivos: Record<string, string> }

// o mapa mora só no orqlib.py (KEYS_PT, VALUES_PT, OLD_FILE): lido uma vez, do orq ao lado desta pasta
const ORQ = join(dirname(import.meta.dir), "..")
const MAPA: Mapa = (() => {
  const py = "import json, orqlib as o; print(json.dumps({'chaves': o.KEYS_PT, 'valores': o.VALUES_PT, 'arquivos': o.OLD_FILE}))"
  const r = Bun.spawnSync(["python3", "-c", py], { cwd: ORQ, env: { ...process.env, ORQ_NO_BG: "1" } })
  return JSON.parse(r.stdout.toString())
})()

export const paraPt = (x: any): any => {
  if (Array.isArray(x)) return x.map(paraPt)
  if (x === null || typeof x !== "object") return x
  const out: Json = {}
  for (const [k, v] of Object.entries(x)) {
    const pt = MAPA.chaves[k] ?? k
    const vs = MAPA.valores[pt]
    out[pt] = vs && typeof v === "string" ? (vs[v] ?? v) : paraPt(v)
  }
  return out
}

// o nome novo do arquivo, ou o pt enquanto a migração não rodou
const arq = (home: string, nome: string): string => {
  const antigo = MAPA.arquivos[nome]
  return antigo && !existsSync(join(home, nome)) && existsSync(join(home, antigo)) ? join(home, antigo) : join(home, nome)
}

const lerJson = (p: string): any => {
  try {
    return JSON.parse(readFileSync(p, "utf8"))
  } catch {
    return null
  }
}

const lerEstadoJson = (p: string): any => paraPt(lerJson(p))

const mtime = (p: string): number | null => {
  try {
    return statSync(p).mtimeMs
  } catch {
    return null
  }
}

// só o fim do events.jsonl: o arquivo passa de 700 KB
const fresco = (e: Estado): boolean => !!e.gerente?.ts && (e.agora - Date.parse(e.gerente.ts)) / 1000 <= ESTADO_FRESCO_S

const fimDoLog = (p: string, bytes = 65536): Json[] => {
  if (!existsSync(p)) return []
  const tam = statSync(p).size
  const fd = openSync(p, "r")
  const buf = Buffer.alloc(Math.min(bytes, tam))
  readSync(fd, buf, 0, buf.length, tam - buf.length)
  closeSync(fd)
  const linhas = buf.toString("utf8").split("\n").slice(tam > bytes ? 1 : 0)
  return linhas.flatMap((l) => {
    try {
      return l.trim() ? [paraPt(JSON.parse(l))] : []
    } catch {
      return []
    }
  })
}

// O backlog do orq: ORQ_BACKLOG (vazio desliga) ou a primeira linha de ORQ_HOME/backlog.path, mais o de cada grupo (groups/<nome>.json `backlog`
// ou grupos/<nome>/backlog.md ao lado do backlog da máquina), como o `backlog_group` do orqlib.
function fontesBacklog(home: string, base: string | null | undefined): [string, string][] | null {
  const de = base !== undefined ? base : "ORQ_BACKLOG" in process.env ? process.env.ORQ_BACKLOG || null : (() => {
    try {
      return readFileSync(join(home, "backlog.path"), "utf8").split("\n")[0].trim() || null
    } catch {
      return null
    }
  })()
  if (!de) return null
  const raiz = resolve(home, de.replace(/^~(?=\/)/, homedir()))
  const fontes: [string, string][] = [["geral", raiz]]
  let nomes: string[] = []
  try {
    nomes = readdirSync(join(home, "groups")).filter((f) => f.endsWith(".json")).sort()
  } catch {}
  for (const f of nomes) {
    const nome = basename(f, ".json")
    const cfg = lerJson(join(home, "groups", f)) ?? {}
    const p = cfg.backlog ? resolve(home, String(cfg.backlog).replace(/^~(?=\/)/, homedir())) : join(dirname(raiz), "grupos", nome, "backlog.md")
    if (existsSync(p)) fontes.push([nome, p])
  }
  return fontes
}

const PY_BACKLOG = `
import json, sys, backlog as b
out = []
for g, p in json.loads(sys.argv[1]):
    for i in b.read_value(p):
        meta, _ = b.body_meta(i.pop("corpo"), b.META_TICKET)
        i.update(grupo=g, task=(meta.get("orca") or "").split(" ")[0] or None)
        out.append(i)
by = {i["id"]: i for i in out}
for i in out:
    i["bloqueado"], i["retido"] = b.is_blocked(i, by), b.active_hold(i)
print(json.dumps(out))
`
// o python só roda quando algum backlog.md muda (a TUI relê a cada 3 s)
let cacheBacklog: { sig: string; itens: Item[] } | null = null

function lerBacklog(fontes: [string, string][]): Item[] {
  const sig = fontes.map(([g, p]) => `${g}=${p}@${mtime(p)}`).join("|")
  if (cacheBacklog?.sig === sig) return cacheBacklog.itens
  const r = Bun.spawnSync(["python3", "-c", PY_BACKLOG, JSON.stringify(fontes)], { cwd: ORQ, env: { ...process.env, ORQ_NO_BG: "1" } })
  if (r.exitCode !== 0) throw new Error(r.stderr.toString().trim().split("\n").pop() || "backlog.py falhou")
  cacheBacklog = { sig, itens: JSON.parse(r.stdout.toString()) }
  return cacheBacklog.itens
}

export function lerEstado(home: string, agora = Date.now(), opts: { backlog?: string | null } = {}): Estado {
  const fontes = fontesBacklog(home, opts.backlog)
  let backlog: Item[] | null = null
  let backlogErro: string | undefined
  try {
    backlog = fontes && lerBacklog(fontes)
  } catch (e) {
    backlogErro = e instanceof Error ? e.message : String(e)
  }
  const pid = (() => {
    try {
      return Number(readFileSync(join(home, "gerente-serve.pid"), "utf8").trim()) || null
    } catch {
      return null
    }
  })()
  return {
    agora,
    gerente: lerEstadoJson(arq(home, "manager-state.json")),
    vivoMs: mtime(arq(home, "manager-alive")),
    servePid: pid,
    aberto: lerEstadoJson(arq(home, "open.json")),
    digest: lerJson(join(home, "digest", "atual.json")), // contrato digest-v1: em pt, sem tradução
    integrar: lerEstadoJson(arq(home, "integrate-queue.json"))?.itens ?? [],
    despacho: lerEstadoJson(arq(home, "dispatch-queue.json"))?.itens ?? [],
    maquinaCfg: lerEstadoJson(arq(home, "machine.json")) ?? {},
    eventos: fimDoLog(join(home, "events.jsonl")),
    backlog,
    backlogErro,
  }
}

const idade = (iso: string | null | undefined, agora: number): string => {
  const t = iso ? Date.parse(iso) : NaN
  if (Number.isNaN(t)) return "?"
  const s = Math.max(0, Math.round((agora - t) / 1000))
  return s < 120 ? `${s} s` : s < 7200 ? `${Math.round(s / 60)} min` : `${Math.round(s / 3600)} h`
}

const hora = (iso: string): string => {
  const d = new Date(iso)
  return Number.isNaN(d.getTime()) ? "--:--" : d.toTimeString().slice(0, 5)
}

const corta = (s: string, n: number): string => (s.length > n ? s.slice(0, n - 1) + "…" : s)

const seg = (t: string, c?: Cor, b?: boolean): Trecho => ({ t, c, b })

function blocoGerente(e: Estado): Bloco {
  const linhas: Linha[] = []
  const s = e.vivoMs === null ? null : Math.round((e.agora - e.vivoMs) / 1000)
  if (s === null) linhas.push([seg("PARADO: sem carimbo manager-alive (o gerente não subiu)", "vermelho")])
  else if (s > VIVO_S) linhas.push([seg(`PARADO há ${Math.round(s / 60)} min: nenhum aviso de worker chega`, "vermelho")])
  else linhas.push([seg(`vivo, última volta há ${s} s` + (fresco(e) && e.servePid ? ` (serve, pid ${e.servePid})` : " (painel no terminal)"), "verde")])
  if (e.gerente?.terminal) linhas.push([seg(`terminal do vínculo: ${e.gerente.terminal}`, "secundario")])
  for (const l of (fresco(e) ? e.gerente?.linhas ?? [] : []).slice(0, 4)) linhas.push(corta(String(l), 110))
  return { id: "gerente", titulo: "Gerente", linhas }
}

function workers(e: Estado): { lista: Json[]; fonte: string } {
  if (fresco(e) && Array.isArray(e.gerente!.agentes)) return { lista: e.gerente!.agentes, fonte: "" }
  return { lista: e.aberto?.agentes ?? [], fonte: e.aberto?.ts ? ` (open.json de ${idade(e.aberto.ts, e.agora)} atrás)` : "" }
}

// azul = rodando, roxo = espera o usuário, amarelo = parado ou esperando, vermelho = travado ou sem terminal
const COR_ESTADO: Record<string, Cor> = { rodando: "azul", perguntando: "roxo", travado: "vermelho", sem_terminal: "vermelho", parado: "amarelo", nao_comecou: "amarelo", aguardando_integracao: "amarelo", hibernado: "secundario" }

function blocoWorkers(e: Estado): Bloco {
  const { lista, fonte } = workers(e)
  const vivos = lista.filter((a) => ANDA.has(a.estado))
  const linhas: Linha[] = vivos.map((a) => [
    seg(`${corta(a.titulo ?? a.dispatch ?? "?", 52).padEnd(52)} `),
    seg(`${String(a.modelo ?? "?").padEnd(18)} `, "secundario"),
    seg(`${corta(String(a.fase ?? a.estado) + (a.fase && a.estado !== "rodando" ? ` (${a.estado})` : ""), 36).padEnd(36)} `, COR_ESTADO[a.estado]),
    seg(idade(a.desde, e.agora), "secundario"),
  ])
  return { id: "workers", titulo: `Workers vivos (${vivos.length})${fonte}`, linhas: linhas.length ? linhas : ["nenhum"] }
}

const SITUACOES: Situacao[] = ["pronto", "andamento", "bloqueado", "retido", "feito"]
const ROTULO: Record<Situacao, { simbolo: string; nome: string; cor: Cor }> = {
  pronto: { simbolo: "●", nome: "pronto", cor: "verde" },
  andamento: { simbolo: "▶", nome: "andamento", cor: "azul" },
  bloqueado: { simbolo: "■", nome: "bloqueado", cor: "vermelho" },
  retido: { simbolo: "⏸", nome: "retido", cor: "amarelo" },
  feito: { simbolo: "✓", nome: "feito", cor: "verde" },
}
const COR_PRIORIDADE: Cor[] = ["vermelho", "vermelho", "amarelo", "secundario", "secundario"]
const PARADO_DIAS = 7 // pronto ou bloqueado há mais que isso, sem andar
const SEM_SINAL_AMARELO_S = 600
const SEM_SINAL_VERMELHO_S = 1800

export const situacao = (i: Item): Situacao => (i.estado === "done" ? "feito" : i.bloqueado ? "bloqueado" : i.estado === "in_flight" ? "andamento" : i.retido ? "retido" : "pronto")

/** Estado do backlog na vista: `estado` e `grupo` filtram (null = todos); `offset` é a primeira task da janela. */
export type Vista = { estado: Situacao | null; grupo: string | null; offset: number }
export const VISTA0: Vista = { estado: null, grupo: null, offset: 0 }
export type Tamanho = { largura: number; altura: number }

/** O próximo valor da lista circular de filtros (null = todos). */
export const proximoFiltro = <T>(lista: (T | null)[], atual: T | null): T | null => lista[(lista.indexOf(atual) + 1) % lista.length]
export const FILTROS_ESTADO: (Situacao | null)[] = [null, ...SITUACOES]
export const filtrosGrupo = (e: Estado): (string | null)[] => [null, ...new Set((e.backlog ?? []).map((i) => i.grupo))]

/** As tasks da vista: prioridade primeiro (sem prioridade por último), depois estado; as feitas só com o filtro `feito`. */
export function filtradas(e: Estado, v: Vista): Item[] {
  const ordem: Situacao[] = ["andamento", "pronto", "bloqueado", "retido", "feito"]
  return (e.backlog ?? [])
    .filter((i) => (v.estado ? situacao(i) === v.estado : i.estado !== "done") && (!v.grupo || i.grupo === v.grupo))
    .sort((a, b) => (a.prioridade ?? 9) - (b.prioridade ?? 9) || ordem.indexOf(situacao(a)) - ordem.indexOf(situacao(b)) || a.id.localeCompare(b.id, "en", { numeric: true }))
}

function segundos(s: number): string {
  return s < 120 ? `${s} s` : s < 7200 ? `${Math.round(s / 60)} min` : `${Math.round(s / 3600)} h`
}

// ponytail: casa o worker pela task do `orca:` do ticket; sem ela, pelo título. Dois workers com o mesmo título pegam o primeiro.
const workerDe = (e: Estado, i: Item): Json | undefined => workers(e).lista.find((a) => ANDA.has(a.estado) && (i.task ? a.task === i.task : a.titulo === i.titulo))

function detalhe(e: Estado, i: Item, por_id: Map<string, Item>): Trecho[] {
  const partes: Trecho[][] = []
  if (i.bloqueado) {
    const bs = i.bloqueios.map((id): Trecho[] => {
      const b = por_id.get(id)
      const r = !b ? { nome: "?", cor: "secundario" as Cor } : { feito: { nome: "feito", cor: "verde" as Cor }, andamento: { nome: "em andamento", cor: "azul" as Cor }, bloqueado: { nome: "também bloqueado", cor: "vermelho" as Cor }, retido: { nome: "retido", cor: "amarelo" as Cor }, pronto: { nome: "parado", cor: "amarelo" as Cor } }[situacao(b)]
      return [seg(`${id} `), seg(`(${r.nome})`, r.cor)]
    })
    partes.push([seg("bloqueado por ", "vermelho"), ...bs.flatMap((x, k) => (k ? [seg(", "), ...x] : x))])
  }
  if (i.estado === "in_flight") {
    const w = workerDe(e, i)
    if (!w) partes.push([seg("sem worker no gerente", "amarelo")])
    else {
      const s = w.idade_s ?? (w.desde ? Math.round((e.agora - Date.parse(w.desde)) / 1000) : null)
      const cor: Cor = s === null ? "secundario" : s >= SEM_SINAL_VERMELHO_S ? "vermelho" : s >= SEM_SINAL_AMARELO_S ? "amarelo" : "secundario"
      partes.push([seg(`${w.modelo ?? "?"} · ${w.fase ?? w.estado}`, "azul"), seg(s === null ? "" : ` · ${segundos(s)} sem sinal`, cor)])
    }
  }
  const dias = i.estado !== "done" && i.since ? Math.floor((e.agora - Date.parse(i.since)) / 86400_000) : 0
  if (i.estado === "queued" && dias >= PARADO_DIAS) partes.push([seg(`parado há ${dias} d`, "amarelo")])
  if (i.retido && i.hold?.motivo) partes.push([seg(`retido: ${i.hold.motivo}`, "amarelo")])
  return partes.flatMap((p, k) => (k ? [seg(" · ", "secundario"), ...p] : p))
}

const aperta = (l: Trecho[], n: number): Trecho[] => {
  const out: Trecho[] = []
  let usado = 0
  for (const t of l) {
    if (usado + t.t.length <= n) {
      out.push(t)
      usado += t.t.length
    } else {
      if (n - usado > 0) out.push({ ...t, t: corta(t.t, n - usado) })
      break
    }
  }
  return out
}

function blocoBacklog(e: Estado, v: Vista, util: number, janela: number): Bloco {
  if (!e.backlog) return { id: "backlog", titulo: "Backlog", linhas: [e.backlogErro ? [seg(`erro ao ler o backlog: ${e.backlogErro}`, "vermelho")] : "sem backlog ligado (backlog.path vazio e ORQ_BACKLOG sem valor)"] }
  const todos = e.backlog
  const conta = (s: Situacao) => todos.filter((i) => situacao(i) === s).length
  const cheio = `Backlog (${SITUACOES.map((s) => `${s} ${conta(s)}`).join(" · ")})`
  // o título da caixa some se não cabe: no terminal estreito as contagens descem para a primeira linha, só com símbolo e número
  const estreito = cheio.length + 4 > util
  const titulo = estreito ? "Backlog" : cheio
  const contagens: Trecho[][] = estreito ? [SITUACOES.flatMap((s, k) => [seg(`${k ? "  " : ""}${ROTULO[s].simbolo} ${conta(s)}`, ROTULO[s].cor)])] : []
  const por_id = new Map(todos.map((i) => [i.id, i]))
  const lista = filtradas(e, v)
  const offset = Math.min(Math.max(v.offset, 0), Math.max(lista.length - 1, 0))
  const linhas: Trecho[][] = []
  let mostradas = 0
  for (const i of lista.slice(offset)) {
    const s = situacao(i)
    const r = ROTULO[s]
    const sufixo = `  ${i.repo ?? "-"} · ${i.grupo}`
    const prefixo = [
      seg(`${r.simbolo} ${r.nome}`.padEnd(12) + " ", r.cor),
      seg((i.prioridade === null ? "--" : `P${i.prioridade}`) + " ", i.prioridade === null ? "secundario" : COR_PRIORIDADE[i.prioridade], true),
      seg(i.id.padEnd(5)),
    ]
    const fixo = prefixo.reduce((n, x) => n + x.t.length, 0) + sufixo.length
    const det = detalhe(e, i, por_id)
    if (linhas.length + (det.length ? 2 : 1) > janela && mostradas) break
    linhas.push(aperta([...prefixo, seg(corta(i.titulo, Math.max(12, util - fixo))), seg(sufixo, "secundario")], util))
    if (det.length) linhas.push(aperta([seg("      ↳ ", "secundario"), ...det], util))
    mostradas++
  }
  const filtro = `estado: ${v.estado ?? "todos"} · grupo: ${v.grupo ?? "todos"} · ${lista.length ? `${offset + 1}-${offset + mostradas}` : "0"} de ${lista.length}   j/k rola · e estado · g grupo · 0 limpa`
  return { id: "backlog", titulo, linhas: [...contagens, aperta([seg(filtro, "secundario")], util), ...(linhas.length ? linhas : [[seg("nenhuma task neste filtro", "secundario")]])] }
}

// uma fila comprida vira os 3 primeiros e a conta do resto: a linha não quebra e o backlog fica com a altura
const FILA_MAX = 3

function blocoFilas(e: Estado): Bloco {
  const linhas: Linha[] = []
  const fila = (nome: string, itens: string[]): Linha => [
    seg(`${nome}: `),
    itens.length ? seg(itens.slice(0, FILA_MAX).join(nome === "despacho" ? "; " : ", ") + (itens.length > FILA_MAX ? ` … +${itens.length - FILA_MAX} (${itens.length})` : ""), "amarelo") : seg("vazia", "verde"),
  ]
  linhas.push(fila("integrador", e.integrar.map((i) => `${i.ticket ? i.ticket + " " : ""}${i.branch}`)))
  linhas.push(fila("despacho", e.despacho.map((i) => corta(i.titulo ?? i.id, 40))))
  const passos = (e.digest?.fila ?? []).filter((p: Json) => !p.feito)
  linhas.push(passos.length ? "merge:" : [seg("merge: "), seg("nada pendente", "verde")])
  for (const p of passos.slice(0, 5)) {
    const prs = (p.prs ?? []).filter((x: Json) => x.estado === "OPEN").map((x: Json) => `#${x.numero}→${x.base}`)
    linhas.push([seg(`  ${p.passo}. `, "secundario"), seg(corta(p.nome ?? "", 70)), ...(prs.length ? [seg("  " + prs.join(" "), "azul")] : [])])
  }
  return { id: "filas", titulo: "Filas", linhas }
}

function blocoPendencias(e: Estado): Bloco {
  const pend = (e.digest?.pendencias ?? []).filter((p: Json) => !p.depois)
  const linhas: Linha[] = pend.slice(0, 8).map((p: Json) => [
    seg(`[${p.tipo}] `, p.tipo === "decisao" ? "roxo" : "amarelo"),
    seg(corta(p.titulo ?? p.id, 90)),
    ...(p.desde ? [seg(`  (desde ${p.desde})`, "secundario")] : []),
  ])
  return { id: "pendencias", titulo: `Pendências do usuário (${pend.length})`, linhas: linhas.length ? linhas : [[seg("nenhuma", "verde")]] }
}

function blocoMaquina(e: Estado): Bloco {
  const cfg = { max_workers: 4, carga_max: 12, mem_livre_min_mb: 3072, livre_pct_min: 15, ...e.maquinaCfg, ...(e.gerente?.maquina?.cfg ?? {}) }
  const l = fresco(e) ? e.gerente?.maquina?.leitura : null
  const vagas = e.aberto?.maquina
  const alta = l && ((l.carga ?? 0) > cfg.carga_max || (l.mem_livre_mb ?? Infinity) < cfg.mem_livre_min_mb || (l.livre_pct ?? 100) < cfg.livre_pct_min)
  const linhas: Linha[] = [
    l
      ? [seg(`carga ${l.carga ?? "?"} / ${cfg.carga_max}   memória livre ${l.mem_livre_mb ?? "?"} MB (mín ${cfg.mem_livre_min_mb})   livre ${l.livre_pct ?? "?"}% (mín ${cfg.livre_pct_min}%)`, alta ? "vermelho" : "verde")]
      : [seg("sem leitura da máquina (o serve grava a cada volta; rode orq gerente serve)", "amarelo")],
    `workers ${vagas?.ocupadas ?? workers(e).lista.filter((a) => ANDA.has(a.estado) && a.estado !== "hibernado" && a.estado !== "sem_terminal").length} / ${cfg.max_workers}` +
      (vagas?.max_caros !== undefined ? `   caros ${vagas.caros ?? 0} / ${vagas.max_caros}` : ""),
  ]
  if (alta) linhas.push([seg("PRESSÃO ALTA: o gerente segura despachos novos", "vermelho", true)])
  return { id: "maquina", titulo: "Máquina contra o orçamento", linhas }
}

function blocoEventos(e: Estado): Bloco {
  const ev = e.eventos.filter((x) => !RUIDO.has(x.tipo)).slice(-10).reverse()
  const linhas = ev.map((x) => `${hora(x.ts)} ${String(x.tipo).padEnd(14)} ${corta([x.op, x.titulo ?? x.subject ?? x.run ?? x.task].filter(Boolean).join(" "), 90)}`)
  return { id: "eventos", titulo: "Últimos eventos", linhas: linhas.length ? linhas : ["nenhum"] }
}

const JANELA_MIN = 10 // linhas do backlog que sobram à vista mesmo com os outros blocos cheios (o fim da tela, eventos e máquina, é que cortam)

// as ondas (ticket 342) vêm prontas do serve, uma linha por onda; sem ondas o bloco não aparece
function blocoOndas(e: Estado): Bloco {
  const linhas = (fresco(e) ? e.gerente?.ondas ?? [] : []).map((l: unknown) => corta(String(l), 120))
  return { id: "ondas", titulo: `Ondas (${linhas.length})`, linhas }
}

/** Os sete blocos (e o das ondas, quando o serve manda alguma). O backlog vem logo depois do gerente e ocupa o que sobra da altura (a janela rola com `vista.offset`); cada linha dele é cortada na largura. */
export function montarBlocos(e: Estado, vista: Vista = VISTA0, tela: Tamanho = { largura: 100, altura: 60 }): Bloco[] {
  const ondas = blocoOndas(e)
  const outros = [blocoGerente(e), blocoWorkers(e), ...(ondas.linhas.length ? [ondas] : []), blocoFilas(e), blocoPendencias(e), blocoMaquina(e), blocoEventos(e)]
  const util = tela.largura - 4 // borda e margem da caixa
  const gasto = outros.reduce((n, b) => n + b.linhas.reduce((m, l) => m + Math.max(1, Math.ceil(texto(l).length / util)), 2), 0) // linha comprida quebra
  const janela = Math.max(JANELA_MIN, tela.altura - gasto - 2 - 1 - 1) // borda do backlog, linha de filtro, rodapé
  const [gerente, ...resto] = outros
  return [gerente, blocoBacklog(e, vista, util, janela), ...resto]
}
