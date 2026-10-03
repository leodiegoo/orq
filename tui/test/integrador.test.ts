// Fila do integrador na TUI (ticket 355): o ciclo em andamento, a fila em ordem, a previsão e o alerta de fila parada.
import { expect, test } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { lerEstado, montarBlocos, texto } from "../src/dados"
import { criarTela } from "../src/tela"
import { PALETAS } from "../src/tema"

const fixtures = join(import.meta.dir, "..", "fixtures")
const agora = Date.parse("2026-10-02T10:00:00Z")
const fila = [0, 1, 2].map((i) => ({ ticket: `0${i}`, branch: `feat/b${i}`, desde: new Date(agora - (20 - i) * 60000).toISOString(), in_cycle: i < 2 }))

function quadro(integrator: unknown) {
  const tmp = mkdtempSync(join(tmpdir(), "tui-integrador-"))
  cpSync(fixtures, tmp, { recursive: true })
  const atual = join(tmp, "digest", "atual.json")
  writeFileSync(atual, JSON.stringify({ ...JSON.parse(readFileSync(atual, "utf8")), integrator }))
  return montarBlocos({ ...lerEstado(tmp, agora), vivoMs: agora - 5000 }).find((b) => b.id === "integrador")
}

test("it should be the running cycle, the queue in order with the wait of each item and the end forecast", () => {
  const b = quadro({
    cycle: { branches: ["feat/b0", "feat/b1"], desde: "2026-10-02T09:55:00Z", stage: "replay", running_s: 300 },
    queue: fila, stalled: null, estimate: { seconds: 1100, eta: "2026-10-02T10:18:20Z", cycles_left: 2.33, return_rate: 0.33, no_history: false },
  })!
  const t = b.linhas.map(texto)
  expect(b.titulo).toBe("Integrador")
  expect(t[0]).toBe("ciclo: 2 branches, roda há 5 min, etapa replay")
  expect(t.slice(1, 4).map((l) => l.replace(/ há .*/, ""))).toEqual(["  1. 00 feat/b0", "  2. 01 feat/b1", "  3. 02 feat/b2"])
  expect(t[1]).toContain("há 20 min")
  expect(t[4]).toMatch(/^termina por volta de \d\d:\d\d \(~18 min\)$/)
})

test("it should be red with 'integrador parado desde HH:MM' when the queue waits and no cycle starts", async () => {
  const b = quadro({
    cycle: null, queue: fila.map((i) => ({ ...i, in_cycle: false })), stalled: { desde: "2026-10-02T09:49:00Z" },
    estimate: { seconds: 360, eta: "2026-10-02T10:06:00Z", cycles_left: 1, return_rate: 0, no_history: true },
  })!
  expect(b.titulo).toBe("Integrador PARADO")
  expect(texto(b.linhas[0])).toMatch(/^integrador parado desde \d\d:\d\d \(3 na fila, nenhum ciclo começou\)$/)
  expect(texto(b.linhas.at(-1)!)).toContain("estimado sem histórico")
  const setup = await createTestRenderer({ width: 120, height: 20 })
  try {
    criarTela(setup.renderer, "orq gerente tui", "dark")([b], "lido")
    await setup.renderOnce()
    const spans = setup.captureSpans().lines.flatMap((l) => l.spans)
    const hex = (c: { r: number; g: number; b: number }) => "#" + [c.r, c.g, c.b].map((v) => Math.round(v * 255).toString(16).padStart(2, "0")).join("")
    expect(spans.some((s) => s.text.includes("integrador parado desde") && hex(s.fg) === PALETAS.dark.vermelho)).toBe(true)
  } finally {
    setup.renderer.destroy()
  }
})

test("it should be no block when the digest has nothing from the integrator", () => {
  expect(quadro({ cycle: null, queue: [], stalled: null, estimate: { seconds: 0, eta: "2026-10-02T10:00:00Z", cycles_left: 0, return_rate: 0, no_history: true } })).toBeUndefined()
})
