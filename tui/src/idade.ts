// Idade de cada item de fila (ticket 345): o orq manda `desde`, a TUI calcula a idade e a pinta na escala do orq e do painel.
// A escala entra no modelo de trechos como mais uma cor semântica: neutro (secundario), amarelo, laranja e vermelho da paleta.
import type { Trecho } from "./dados"
import type { Cor } from "./tema"

export type Nivel = "ok" | "warn" | "hot" | "crit"
/** `limites`: os minutos de warn, hot e crit já multiplicados pelo fator do tipo (e pela metade em P1). */
export type Idade = { min: number; limites: number[] }

export const ESCALA_PADRAO = [5, 15, 30]
export const FATORES: Record<string, number> = { fila: 1, integracao: 1, worker: 1, entrega: 3, obrigacao: 3, pendencia: 12 }
const NIVEIS: Nivel[] = ["warn", "hot", "crit"]
const COR_NIVEL: Record<Nivel, Cor> = { ok: "secundario", warn: "amarelo", hot: "laranja", crit: "vermelho" }

/** Minutos de espera desde um instante ISO (ou data AAAA-MM-DD); undefined quando não lê. */
export function minutosDesde(iso: string | null | undefined, agora: number): number | undefined {
  const t = iso ? Date.parse(iso) : NaN
  return Number.isNaN(t) ? undefined : Math.max(0, (agora - t) / 60000)
}

export function limitesDo(escala: unknown, tipo: string, prioridade?: number, fatores: Record<string, number> = FATORES): number[] {
  const base = Array.isArray(escala) && escala.length ? escala.map((x) => Number(Array.isArray(x) ? x[0] : x)).filter(Number.isFinite).sort((a, b) => a - b) : ESCALA_PADRAO
  const k = (fatores[tipo] ?? 1) * (prioridade === 1 ? 0.5 : 1)
  return base.map((m) => m * k)
}

export const nivelDe = ({ min, limites }: Idade): Nivel => {
  const i = limites.filter((l) => min >= l).length
  return i === 0 ? "ok" : NIVEIS[Math.min(i, NIVEIS.length) - 1]
}

/** "há 14 min", "há 2 h 05". */
export const idadeTexto = (min: number): string => {
  const m = Math.floor(min)
  return m < 60 ? `há ${m} min` : `há ${Math.floor(m / 60)} h ${String(m % 60).padStart(2, "0")}`
}

export const corDaIdade = (i: Idade): Cor => COR_NIVEL[nivelDe(i)]

/** O trecho da idade: o texto na cor da escala, e no crítico ▲ e negrito (a cor nunca é o único sinal). */
export const idadeTrecho = (i: Idade, texto = idadeTexto(i.min)): Trecho => {
  const crit = nivelDe(i) === "crit"
  return { t: crit ? `${texto} ▲` : texto, c: corDaIdade(i), b: crit || undefined }
}
