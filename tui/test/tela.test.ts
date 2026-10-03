// Fumaça: monta a tela a partir de tui/fixtures num renderer de teste e confere os blocos no quadro.
import { expect, test } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import { cpSync, existsSync, mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { VISTA0, lerEstado, montarBlocos, texto, type Trecho } from "../src/dados"
import { criarTela } from "../src/tela"
import { textoLegivel } from "../src/tema"

const fixtures = join(import.meta.dir, "..", "fixtures")
const agora = Date.parse("2026-10-01T12:00:30Z")

test("it should be the seven blocks built from the orq files", async () => {
  const e = { ...lerEstado(fixtures, agora, { backlog: join(fixtures, "backlog.md") }), vivoMs: agora - 5000 } // o mtime do carimbo não vai para o git
  const setup = await createTestRenderer({ width: 140, height: 80 })
  try {
    criarTela(setup.renderer, "orq gerente tui")(montarBlocos(e, VISTA0, { largura: 140, altura: 80 }), "lido")
    await setup.renderOnce()
    const quadro = setup.captureCharFrame()
    for (const t of ["Gerente", "Workers vivos (1)", "Backlog (pronto 3", "Filas", "Pendências do usuário (1)", "Máquina contra o orçamento", "Últimos eventos"]) {
      expect(quadro).toContain(t)
    }
    expect(quadro).toContain("vivo, última volta há 5 s (serve, pid 4242)")
    expect(quadro).toContain("orq: serve do gerente")
    expect(quadro).toContain("claude-opus-5-5")
    expect(quadro).not.toContain("worker já entregue")
    expect(quadro).toContain("128 feat/gerente-serve-e-tui")
    expect(quadro).toContain("#1295→development")
    expect(quadro).not.toContain("coisa para depois")
    expect(quadro).toContain("carga 25.5 / 20")
    expect(quadro).toContain("PRESSÃO ALTA")
    expect(quadro).toContain("125 pronto: orq projeto add")
    expect(quadro).not.toContain("ruido")
  } finally {
    setup.renderer.destroy()
  }
})

test("it should be a stopped manager when the stamp is old and the state file is stale", () => {
  const e = { ...lerEstado(fixtures, agora + 3600_000), vivoMs: agora - 600_000 }
  const blocos = montarBlocos(e)
  const [gerente, workers] = ["gerente", "workers"].map((id) => blocos.find((b) => b.id === id)!)
  expect(texto(gerente.linhas[0])).toStartWith("PARADO há 70 min")
  expect(workers.titulo).toContain("open.json")
  expect(texto(blocos.find((b) => b.id === "maquina")!.linhas[0])).toStartWith("sem leitura da máquina")
})

test("it should be the same blocks from the fixtures migrated to english", () => {
  const tmp = mkdtempSync(join(tmpdir(), "tui-en-"))
  const home = join(tmp, "orq")
  cpSync(fixtures, home, { recursive: true })
  const orca = join(tmp, "orca") // um Orca sem workers vivos: a migração recusa rodar com algum
  writeFileSync(orca, `#!/bin/sh\necho '{"ok": true, "result": {"workers": [], "scope": {"source": "all"}}}'\n`, { mode: 0o755 })
  const r = Bun.spawnSync(["python3", join(import.meta.dir, "..", "..", "scripts", "migrar-ingles.py")], {
    // hermético: nada de ORQ_, ORCA_ ou do harness de quem roda atravessa (ticket 328)
    env: { ...Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^(ORQ_|ORCA_|CLAUDE|CODEX_)/.test(k))), ORQ_HOME: home, ORQ_ORCA: orca, ORQ_NO_BG: "1" },
  })
  expect(r.exitCode).toBe(0)
  expect(existsSync(join(home, "open.json")) && !existsSync(join(home, "aberto.json"))).toBe(true)
  const bl = { backlog: join(fixtures, "backlog.md") }
  const pt = montarBlocos({ ...lerEstado(fixtures, agora, bl), vivoMs: agora - 5000 })
  const en = montarBlocos({ ...lerEstado(home, agora, bl), vivoMs: agora - 5000 })
  expect(en).toEqual(pt)
})

test("it should show the waves block only when the serve brought waves", () => {
  const e = { ...lerEstado(fixtures, agora), vivoMs: agora - 5000 }
  expect(montarBlocos(e).map((b) => b.id)).not.toContain("ondas")
  const linha = "wave 2 Core: waiting, 0/3 integrated; waits for 02; open: 06, 07, 08"
  const bloco = montarBlocos({ ...e, gerente: { ...e.gerente, ondas: [linha] } }).find((b) => b.id === "ondas")
  expect(bloco).toEqual({ id: "ondas", titulo: "Ondas (1)", linhas: [linha] })
})

test("it should badge the workers, the merge steps and the pendências with the project, in the project color", async () => {
  const e = { ...lerEstado(fixtures, agora), vivoMs: agora - 5000 }
  const blocos = montarBlocos(e)
  const linhas = (id: string) => blocos.find((b) => b.id === id)!.linhas as Trecho[][]
  expect(linhas("workers")[0].slice(0, 2)).toEqual([{ t: "orq", c: "#bc8cff", b: undefined }, { t: " ", c: undefined, b: undefined }])
  expect(texto(linhas("workers")[0])).toStartWith("orq orq: serve do gerente")
  expect(linhas("filas").find((l) => texto(l).includes("Failover"))).toContainEqual({ t: "app-web", c: "#e3b341", b: undefined })
  expect(linhas("pendencias")[0][0]).toEqual({ t: "app-web", c: "#e3b341", b: undefined })
  const setup = await createTestRenderer({ width: 140, height: 60 })
  try {
    criarTela(setup.renderer, "orq gerente tui", "dark")(blocos, "lido")
    await setup.renderOnce()
    const spans = setup.captureSpans().lines.flatMap((l) => l.spans)
    const pintado = (t: string) => spans.find((s) => s.text.trim() === t)!
    const hex = (c: { r: number; g: number; b: number }) => "#" + [c.r, c.g, c.b].map((v) => Math.round(v * 255).toString(16).padStart(2, "0")).join("")
    expect(hex(pintado("orq").fg)).toBe("#bc8cff")
    expect(hex(pintado("app-web").fg)).toBe(textoLegivel("#e3b341", "dark"))
  } finally {
    setup.renderer.destroy()
  }
})

test("it should leave the badge in the theme text when the project has no valid color", () => {
  const e = lerEstado(fixtures, agora)
  const sem = { ...e, digest: { ...e.digest, projetos: [{ nome: "orq", cor: "azul" }], grupos: [] } }
  const workers = montarBlocos(sem).find((b) => b.id === "workers")!
  expect((workers.linhas[0] as Trecho[])[0]).toEqual({ t: "orq", c: undefined, b: undefined })
})

test("it should list the slots per project and the starvation warning in the machine block", () => {
  const e = { ...lerEstado(fixtures, agora), vivoMs: agora - 5000 }
  const maquina = {
    ...e.aberto?.maquina,
    projetos: [
      { projeto: "product-app", vivos: 0, fila: 1, posicao: 1, aviso: "product-app: 1 item waiting for 14 min, 0 of 6 slots" },
      { projeto: "orq", vivos: 6, fila: 3, posicao: 2, aviso: null },
    ],
  }
  const linhas = montarBlocos({ ...e, aberto: { ...e.aberto, maquina } }).find((b) => b.id === "maquina")!.linhas
  expect(linhas).toContain("projeto product-app: 0 vivos, 1 na fila (1º na posição 1)")
  expect(linhas).toContain("  AVISO: product-app: 1 item waiting for 14 min, 0 of 6 slots")
  expect(linhas).toContain("projeto orq: 6 vivos, 3 na fila (1º na posição 2)")
})
