// orq gerente tui: só leitura. Relê os arquivos do ORQ_HOME a cada ORQ_TUI_S segundos (3 por padrão); q ou Ctrl+C sai.
import { createCliRenderer } from "@opentui/core"
import { homedir } from "node:os"
import { join } from "node:path"
import { lerEstado, montarBlocos } from "./dados"
import { criarTela } from "./tela"

const home = process.env.ORQ_HOME || join(homedir(), ".claude", "orq")
const intervalo = Number(process.env.ORQ_TUI_S || 3) * 1000

const renderer = await createCliRenderer({ exitOnCtrlC: true })
const atualizar = criarTela(renderer, `orq gerente tui — ${home} — q sai`)

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
