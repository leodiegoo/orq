// orq gerente tui: só leitura. Relê os arquivos do ORQ_HOME a cada ORQ_TUI_S segundos (3 por padrão); q ou Ctrl+C sai.
// Tema: --theme light|dark|auto ou ORQ_TUI_THEME; auto pergunta ao terminal (OSC 11), depois COLORFGBG e o tema do macOS.
import { createCliRenderer } from "@opentui/core"
import { homedir } from "node:os"
import { join } from "node:path"
import { lerEstado, montarBlocos } from "./dados"
import { criarTela } from "./tela"
import { detectarTema } from "./tema"

const home = process.env.ORQ_HOME || join(homedir(), ".claude", "orq")
const intervalo = Number(process.env.ORQ_TUI_S || 3) * 1000

const renderer = await createCliRenderer({ exitOnCtrlC: true })
const tema = await detectarTema({ argv: process.argv, env: process.env, osc: () => renderer.waitForThemeMode(500) })
const atualizar = criarTela(renderer, `orq gerente tui — ${home} — q sai`, tema)

const volta = () => {
  try {
    atualizar(montarBlocos(lerEstado(home)), `lido às ${new Date().toTimeString().slice(0, 8)}`)
  } catch (e) {
    atualizar([], `erro ao ler: ${e instanceof Error ? e.message : String(e)}`)
  }
}

volta()
const timer = setInterval(volta, intervalo)
renderer.keyInput.on("keypress", (k: { name?: string }) => {
  if (k.name === "q") {
    clearInterval(timer)
    renderer.destroy()
    process.exit(0)
  }
})
