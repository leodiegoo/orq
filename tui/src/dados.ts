// Lê os arquivos do ORQ_HOME e monta os blocos da TUI como texto. Só leitura: nada aqui grava nem chama o Orca.
// O serve (orq gerente serve) grava manager-state.json a cada volta com os workers e a máquina; sem ele, os workers vêm do open.json.
// O disco está em inglês (fase 2 da migração); a TUI lê com as chaves pt, como o orq: paraPt troca as chaves e valores pelo mapa do orqlib.py.
import { existsSync, openSync, readFileSync, readSync, statSync, closeSync } from "node:fs"
import { dirname, join } from "node:path"

type Json = Record<string, any>

export type Bloco = { id: string; titulo: string; linhas: string[] }

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
}

const VIVO_S = 90 // o PAINEL_LIMITE_MIN_S do orq: carimbo mais velho que isso é gerente parado
const ESTADO_FRESCO_S = 60
const ANDA = new Set(["rodando", "perguntando", "travado", "parado", "nao_comecou", "sem_terminal", "aguardando_integracao", "hibernado"])
const RUIDO = new Set(["intake", "entrada", "heartbeat_absorvido", "gate_aviso"])

type Mapa = { chaves: Record<string, string>; valores: Record<string, Record<string, string>>; arquivos: Record<string, string> }

// o mapa mora só no orqlib.py (KEYS_PT, VALUES_PT, OLD_FILE): lido uma vez, do orq ao lado desta pasta
const MAPA: Mapa = (() => {
  const orq = join(dirname(import.meta.dir), "..")
  const py = "import json, orqlib as o; print(json.dumps({'chaves': o.KEYS_PT, 'valores': o.VALUES_PT, 'arquivos': o.OLD_FILE}))"
  const r = Bun.spawnSync(["python3", "-c", py], { cwd: orq, env: { ...process.env, ORQ_NO_BG: "1" } })
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

export function lerEstado(home: string, agora = Date.now()): Estado {
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

function blocoGerente(e: Estado): Bloco {
  const linhas: string[] = []
  const s = e.vivoMs === null ? null : Math.round((e.agora - e.vivoMs) / 1000)
  if (s === null) linhas.push("PARADO: sem carimbo manager-alive (o gerente não subiu)")
  else if (s > VIVO_S) linhas.push(`PARADO há ${Math.round(s / 60)} min: nenhum aviso de worker chega`)
  else linhas.push(`vivo, última volta há ${s} s` + (fresco(e) && e.servePid ? ` (serve, pid ${e.servePid})` : " (painel no terminal)"))
  if (e.gerente?.terminal) linhas.push(`terminal do vínculo: ${e.gerente.terminal}`)
  for (const l of (fresco(e) ? e.gerente?.linhas ?? [] : []).slice(0, 4)) linhas.push(corta(String(l), 110))
  return { id: "gerente", titulo: "Gerente", linhas }
}

function workers(e: Estado): { lista: Json[]; fonte: string } {
  if (fresco(e) && Array.isArray(e.gerente!.agentes)) return { lista: e.gerente!.agentes, fonte: "" }
  return { lista: e.aberto?.agentes ?? [], fonte: e.aberto?.ts ? ` (open.json de ${idade(e.aberto.ts, e.agora)} atrás)` : "" }
}

function blocoWorkers(e: Estado): Bloco {
  const { lista, fonte } = workers(e)
  const vivos = lista.filter((a) => ANDA.has(a.estado))
  const linhas = vivos.map(
    (a) => `${corta(a.titulo ?? a.dispatch ?? "?", 52).padEnd(52)} ${String(a.modelo ?? "?").padEnd(18)} ${corta(String(a.fase ?? a.estado), 22).padEnd(22)} ${idade(a.desde, e.agora)}`,
  )
  return { id: "workers", titulo: `Workers vivos (${vivos.length})${fonte}`, linhas: linhas.length ? linhas : ["nenhum"] }
}

function blocoFilas(e: Estado): Bloco {
  const linhas: string[] = []
  linhas.push(`integrador: ${e.integrar.length ? e.integrar.map((i) => `${i.ticket ? i.ticket + " " : ""}${i.branch}`).join(", ") : "vazia"}`)
  linhas.push(`despacho: ${e.despacho.length ? e.despacho.map((i) => corta(i.titulo ?? i.id, 40)).join("; ") : "vazia"}`)
  const passos = (e.digest?.fila ?? []).filter((p: Json) => !p.feito)
  linhas.push(`merge: ${passos.length ? "" : "nada pendente"}`)
  for (const p of passos.slice(0, 5)) {
    const prs = (p.prs ?? []).filter((x: Json) => x.estado === "OPEN").map((x: Json) => `#${x.numero}→${x.base}`)
    linhas.push(`  ${p.passo}. ${corta(p.nome ?? "", 70)}${prs.length ? "  " + prs.join(" ") : ""}`)
  }
  return { id: "filas", titulo: "Filas", linhas }
}

function blocoPendencias(e: Estado): Bloco {
  const pend = (e.digest?.pendencias ?? []).filter((p: Json) => !p.depois)
  const linhas = pend.slice(0, 8).map((p: Json) => `[${p.tipo}] ${corta(p.titulo ?? p.id, 90)}${p.desde ? `  (desde ${p.desde})` : ""}`)
  return { id: "pendencias", titulo: `Pendências do usuário (${pend.length})`, linhas: linhas.length ? linhas : ["nenhuma"] }
}

function blocoMaquina(e: Estado): Bloco {
  const cfg = { max_workers: 4, carga_max: 12, mem_livre_min_mb: 3072, livre_pct_min: 15, ...e.maquinaCfg, ...(e.gerente?.maquina?.cfg ?? {}) }
  const l = fresco(e) ? e.gerente?.maquina?.leitura : null
  const vagas = e.aberto?.maquina
  const linhas = [
    l
      ? `carga ${l.carga ?? "?"} / ${cfg.carga_max}   memória livre ${l.mem_livre_mb ?? "?"} MB (mín ${cfg.mem_livre_min_mb})   livre ${l.livre_pct ?? "?"}% (mín ${cfg.livre_pct_min}%)`
      : "sem leitura da máquina (o serve grava a cada volta; rode orq gerente serve)",
    `workers ${vagas?.ocupadas ?? workers(e).lista.filter((a) => ANDA.has(a.estado) && a.estado !== "hibernado" && a.estado !== "sem_terminal").length} / ${cfg.max_workers}` +
      (vagas?.max_caros !== undefined ? `   caros ${vagas.caros ?? 0} / ${vagas.max_caros}` : ""),
  ]
  const alta = l && ((l.carga ?? 0) > cfg.carga_max || (l.mem_livre_mb ?? Infinity) < cfg.mem_livre_min_mb || (l.livre_pct ?? 100) < cfg.livre_pct_min)
  if (alta) linhas.push("PRESSÃO ALTA: o gerente segura despachos novos")
  return { id: "maquina", titulo: "Máquina contra o orçamento", linhas }
}

function blocoEventos(e: Estado): Bloco {
  const ev = e.eventos.filter((x) => !RUIDO.has(x.tipo)).slice(-10).reverse()
  const linhas = ev.map((x) => `${hora(x.ts)} ${String(x.tipo).padEnd(14)} ${corta([x.op, x.titulo ?? x.subject ?? x.run ?? x.task].filter(Boolean).join(" "), 90)}`)
  return { id: "eventos", titulo: "Últimos eventos", linhas: linhas.length ? linhas : ["nenhum"] }
}

export function montarBlocos(e: Estado): Bloco[] {
  return [blocoGerente(e), blocoWorkers(e), blocoFilas(e), blocoPendencias(e), blocoMaquina(e), blocoEventos(e)]
}
