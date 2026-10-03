// Tema da TUI: OpenTUI pinta texto e borda de branco por padrão (ilegível em fundo claro), então cada cor sai da paleta do tema.
// Nenhuma cor de fundo é fixada: o fundo é o do terminal, e a paleta (GitHub Dark/Light) só traz cores com contraste >= 4.5:1 contra o fundo do tema.
import { spawnSync } from "node:child_process"

export type Tema = "light" | "dark"
export type Paleta = { fundo: string; texto: string; secundario: string; borda: string; verde: string; amarelo: string; laranja: string; vermelho: string; azul: string; roxo: string }
/** Cor semântica de um trecho: verde = pronto, em dia ou feito; amarelo = esperando ou parado; laranja = esperando há tempo demais (escala de idade); vermelho = bloqueado, travado ou falhou; azul = em andamento; roxo = decisão com o usuário; secundario = cinza. */
export type Cor = Exclude<keyof Paleta, "fundo" | "borda">

/** O `#rrggbb` de um projeto: não é da paleta, a tela o ajusta ao tema com `textoLegivel`. */
export type CorProjeto = `#${string}`

// GitHub Dark e GitHub Light, chave a chave. `fundo` só serve de referência para o teste de contraste; a TUI não o pinta.
export const PALETAS: Record<Tema, Paleta> = {
  dark: { fundo: "#0d1117", texto: "#c9d1d9", secundario: "#8b949e", borda: "#8b949e", verde: "#3fb950", amarelo: "#d29922", laranja: "#db6d28", vermelho: "#f85149", azul: "#58a6ff", roxo: "#bc8cff" },
  light: { fundo: "#ffffff", texto: "#1f2328", secundario: "#656d76", borda: "#656d76", verde: "#1a7f37", amarelo: "#9a6700", laranja: "#bc4c00", vermelho: "#cf222e", azul: "#0969da", roxo: "#8250df" },
}

const luz = (hex: string): number => {
  const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255).map((c) => (c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4))
  return 0.2126 * r + 0.7152 * g + 0.0722 * b
}

/** Razão de contraste WCAG entre duas cores `#rrggbb`. */
export const contraste = (a: string, b: string): number => {
  const [x, y] = [luz(a), luz(b)].sort((p, q) => q - p)
  return (x + 0.05) / (y + 0.05)
}

const doValor = (v?: string): Tema | null => (v === "light" || v === "dark" ? v : null)

/** `--theme light|dark|auto` (argv) ou ORQ_TUI_THEME; `auto` e ausente dão null. */
export function temaForcado(argv: string[], env: NodeJS.ProcessEnv): Tema | null {
  const i = argv.indexOf("--theme")
  const arg = i >= 0 ? argv[i + 1] : argv.find((a) => a.startsWith("--theme="))?.slice(8)
  return doValor(arg) ?? doValor(env.ORQ_TUI_THEME)
}

/** COLORFGBG ("fg;bg", bg 7 ou 9..15 = claro, 0..6 e 8 = escuro). */
export function temaDoColorFgBg(v?: string): Tema | null {
  const bg = Number(v?.split(";").pop())
  if (!v || !Number.isInteger(bg)) return null
  return bg === 7 || bg >= 9 ? "light" : "dark"
}

/** macOS: `AppleInterfaceStyle` só existe no tema escuro. */
export function temaDoSistema(plataforma = process.platform): Tema | null {
  if (plataforma !== "darwin") return null
  const r = spawnSync("defaults", ["read", "-g", "AppleInterfaceStyle"], { encoding: "utf8" })
  return r.stdout?.trim() === "Dark" ? "dark" : "light"
}

/** Ordem: forçado, OSC 11 (perguntado ao terminal), COLORFGBG, sistema, escuro. */
export async function detectarTema(
  opts: { argv: string[]; env: NodeJS.ProcessEnv; osc?: () => Promise<Tema | null> },
): Promise<Tema> {
  return temaForcado(opts.argv, opts.env) ?? (await opts.osc?.().catch(() => null)) ?? temaDoColorFgBg(opts.env.COLORFGBG) ?? temaDoSistema() ?? "dark"
}

/** A cor `#rrggbb` do projeto, escurecida (tema claro) ou clareada (escuro) o bastante para ler a 4,5:1 sobre o fundo do tema; inalterada se já passa. */
export function textoLegivel(cor: string, tema: Tema): string {
  const fundo = PALETAS[tema].fundo
  const alvo = tema === "light" ? [0, 0, 0] : [255, 255, 255]
  const base = [1, 3, 5].map((i) => parseInt(cor.slice(i, i + 2), 16))
  for (let t = 0; t <= 1.0001; t += 0.02) {
    const c = "#" + base.map((v, i) => Math.round(v + (alvo[i]! - v) * Math.min(t, 1)).toString(16).padStart(2, "0")).join("")
    if (contraste(c, fundo) >= 4.5) return c
  }
  return `#${alvo.map((v) => v.toString(16).padStart(2, "0")).join("")}`
}
