// orq gerente tui: só leitura. Relê os arquivos do ORQ_HOME a cada ORQ_TUI_S segundos (3 por padrão); q ou Ctrl+C sai.
// Tema: --theme light|dark|auto ou ORQ_TUI_THEME; auto pergunta ao terminal (OSC 11), depois COLORFGBG e o tema do macOS.
// Backlog: j/k ou setas rolam, PgUp/PgDn rolam uma página, e troca o filtro de estado, g o de grupo, 0 limpa os dois.
import { createCliRenderer } from "@opentui/core"
import { homedir } from "node:os"
import { join } from "node:path"
import { FILTROS_ESTADO, VISTA0, filtradas, filtrosGrupo, lerEstado, montarBlocos, proximoFiltro, type Estado, type Vista } from "./dados"
import { criarTela } from "./tela"
import { detectarTema } from "./tema"

const home = process.env.ORQ_HOME || join(homedir(), ".claude", "orq")
const intervalo = Number(process.env.ORQ_TUI_S || 3) * 1000

const renderer = await createCliRenderer({ exitOnCtrlC: true })
const tema = await detectarTema({ argv: process.argv, env: process.env, osc: () => renderer.waitForThemeMode(500) })
const atualizar = criarTela(renderer, `orq gerente tui — ${home} — q sai · j/k rola · e estado · g grupo · 0 limpa`, tema)

let vista: Vista = VISTA0
let estado: Estado | null = null
let status = ""

const desenha = () => {
  if (estado) atualizar(montarBlocos(estado, vista, { largura: renderer.width, altura: renderer.height }), status)
}

const volta = () => {
  try {
    estado = lerEstado(home)
    status = `lido às ${new Date().toTimeString().slice(0, 8)}`
    desenha()
  } catch (e) {
    atualizar([], `erro ao ler: ${e instanceof Error ? e.message : String(e)}`)
  }
}

volta()
const timer = setInterval(volta, intervalo)
renderer.on("resize", desenha)
renderer.keyInput.on("keypress", (k: { name?: string }) => {
  if (k.name === "q") {
    clearInterval(timer)
    renderer.destroy()
    process.exit(0)
  }
  if (!estado) return
  const rola = (n: number) => (vista = { ...vista, offset: Math.max(0, Math.min(vista.offset + n, filtradas(estado!, vista).length - 1)) })
  const pagina = Math.max(1, renderer.height - 20)
  if (k.name === "j" || k.name === "down") rola(1)
  else if (k.name === "k" || k.name === "up") rola(-1)
  else if (k.name === "pagedown") rola(pagina)
  else if (k.name === "pageup") rola(-pagina)
  else if (k.name === "e") vista = { ...vista, estado: proximoFiltro(FILTROS_ESTADO, vista.estado), offset: 0 }
  else if (k.name === "g") vista = { ...vista, grupo: proximoFiltro(filtrosGrupo(estado), vista.grupo), offset: 0 }
  else if (k.name === "0") vista = VISTA0
  else return
  desenha()
})
