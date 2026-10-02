// Fumaça: monta a tela a partir de tui/fixtures num renderer de teste e confere os blocos no quadro.
import { expect, test } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import { cpSync, existsSync, mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { lerEstado, montarBlocos } from "../src/dados"
import { criarTela } from "../src/tela"

const fixtures = join(import.meta.dir, "..", "fixtures")
const agora = Date.parse("2026-10-01T12:00:30Z")

test("it should be the six blocks built from the orq files", async () => {
  const e = { ...lerEstado(fixtures, agora), vivoMs: agora - 5000 } // o mtime do carimbo não vai para o git
  const setup = await createTestRenderer({ width: 140, height: 60 })
  try {
    criarTela(setup.renderer, "orq gerente tui")(montarBlocos(e), "lido")
    await setup.renderOnce()
    const quadro = setup.captureCharFrame()
    for (const t of ["Gerente", "Workers vivos (1)", "Filas", "Pendências do usuário (1)", "Máquina contra o orçamento", "Últimos eventos"]) {
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
  const [gerente, workers] = montarBlocos(e)
  expect(gerente.linhas[0]).toStartWith("PARADO há 70 min")
  expect(workers.titulo).toContain("open.json")
  expect(montarBlocos(e)[4].linhas[0]).toStartWith("sem leitura da máquina")
})

test("it should be the same blocks from the fixtures migrated to english", () => {
  const tmp = mkdtempSync(join(tmpdir(), "tui-en-"))
  const home = join(tmp, "orq")
  cpSync(fixtures, home, { recursive: true })
  const orca = join(tmp, "orca") // um Orca sem workers vivos: a migração recusa rodar com algum
  writeFileSync(orca, `#!/bin/sh\necho '{"ok": true, "result": {"workers": [], "scope": {"source": "all"}}}'\n`, { mode: 0o755 })
  const r = Bun.spawnSync(["python3", join(import.meta.dir, "..", "..", "scripts", "migrar-ingles.py")], {
    env: { ...process.env, ORQ_HOME: home, ORQ_ORCA: orca, ORQ_NO_BG: "1" },
  })
  expect(r.exitCode).toBe(0)
  expect(existsSync(join(home, "open.json")) && !existsSync(join(home, "aberto.json"))).toBe(true)
  const pt = montarBlocos({ ...lerEstado(fixtures, agora), vivoMs: agora - 5000 })
  const en = montarBlocos({ ...lerEstado(home, agora), vivoMs: agora - 5000 })
  expect(en).toEqual(pt)
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
  const linhas = montarBlocos({ ...e, aberto: { ...e.aberto, maquina } })[4].linhas
  expect(linhas).toContain("projeto product-app: 0 vivos, 1 na fila (1º na posição 1)")
  expect(linhas).toContain("  AVISO: product-app: 1 item waiting for 14 min, 0 of 6 slots")
  expect(linhas).toContain("projeto orq: 6 vivos, 3 na fila (1º na posição 2)")
})
