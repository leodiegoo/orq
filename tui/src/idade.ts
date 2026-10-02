// Idade de cada item de fila (ticket 345): o orq manda `desde`, a TUI calcula a idade e pinta na mesma escala do orq e do painel.
// Abaixo do primeiro limite o texto fica neutro; dali em diante a cor sai de amarelo, passa por laranja e chega ao vermelho com interpolação.
import type { Paleta } from "./tema"

export type Nivel = "ok" | "warn" | "hot" | "crit"
/** `limites`: os minutos de warn, hot e crit já multiplicados pelo fator do tipo (e pela metade em P1). */
export type Idade = { min: number; limites: number[] }

export const ESCALA_PADRAO = [5, 15, 30]
export const FATORES: Record<string, number> = { fila: 1, integracao: 1, worker: 1, entrega: 3, obrigacao: 3, pendencia: 12 }
const NIVEIS: Nivel[] = ["warn", "hot", "crit"]

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

const rgb = (hex: string): number[] => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16))
const mistura = (a: string, b: string, f: number): string =>
  "#" + rgb(a).map((c, i) => Math.round(c + (rgb(b)[i] - c) * f).toString(16).padStart(2, "0")).join("")

/** Neutro até o primeiro limite; depois gradiente contínuo warn → hot → crit, crit daí em diante. */
export function corIdade({ min, limites }: Idade, p: Paleta): string {
  const pontos = [p.idade.warn, p.idade.hot, p.idade.crit]
  if (min < limites[0]) return p.secundario
  for (let i = 0; i < pontos.length - 1; i++) {
    if (min < limites[i + 1]) return mistura(pontos[i], pontos[i + 1], (min - limites[i]) / (limites[i + 1] - limites[i]))
  }
  return pontos[pontos.length - 1]
}
