// Idade da fila (ticket 345): escala, texto, cor interpolada, contraste nos dois temas e a tela com relógio simulado.
import { expect, test } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import { cpSync, mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { lerEstado, montarBlocos } from "../src/dados"
import { corDaIdade, idadeTexto, idadeTrecho, limitesDo, nivelDe } from "../src/idade"
import { criarTela } from "../src/tela"
import { PALETAS, contraste, type Tema } from "../src/tema"

const fixtures = join(import.meta.dir, "..", "fixtures")
const escala = [[5, "warn"], [15, "hot"], [30, "crit"]]
const nivel = (min: number, tipo = "fila", prio?: number) => nivelDe({ min, limites: limitesDo(escala, tipo, prio) })

test("it should be neutral at 3 min, hot at 20, crit at 40 and crit for P1 at 15", () => {
  expect([3, 5, 20, 40].map((m) => nivel(m))).toEqual(["ok", "warn", "hot", "crit"])
  expect(nivel(15, "fila", 1)).toBe("crit")
  expect(nivel(14, "fila", 1)).toBe("hot")
  expect([40, 50, 90].map((m) => nivel(m, "entrega"))).toEqual(["warn", "hot", "crit"])
  expect(nivel(120, "pendencia")).toBe("warn")
})

test("it should be the age text in minutes and in hours", () => {
  expect([0, 14.9, 59, 125].map(idadeTexto)).toEqual(["há 0 min", "há 14 min", "há 59 min", "há 2 h 05"])
})

test("it should be the scale's color as a trecho: neutral, yellow, orange, red, and the ▲ only when critical", () => {
  const cor = (min: number) => corDaIdade({ min, limites: limitesDo(escala, "fila") })
  expect([3, 5, 20, 40].map(cor)).toEqual(["secundario", "amarelo", "laranja", "vermelho"])
  expect(idadeTrecho({ min: 20, limites: [5, 15, 30] })).toEqual({ t: "há 20 min", c: "laranja", b: undefined })
  expect(idadeTrecho({ min: 40, limites: [5, 15, 30] })).toEqual({ t: "há 40 min ▲", c: "vermelho", b: true })
})

for (const tema of ["light", "dark"] as Tema[]) {
  test(`it should be the four age colors with contrast of 4.5:1 or more on the ${tema} background`, () => {
    const p = PALETAS[tema]
    for (const c of ["secundario", "amarelo", "laranja", "vermelho"] as const) expect(contraste(p[c], p.fundo)).toBeGreaterThanOrEqual(4.5)
  })
}

test("it should be the oldest age in the header and the colored lines on the screen, advancing with the clock", async () => {
  const tmp = mkdtempSync(join(tmpdir(), "tui-idade-"))
  cpSync(fixtures, tmp, { recursive: true })
  writeFileSync(join(tmp, "fila-despacho.json"), JSON.stringify({ itens: [{ id: "d1", titulo: "conserto do cache do E2E", prioridade: 2, ts: "2026-10-01T11:13:30Z" }] }))
  const quadro = async (agora: number, tema: Tema) => {
    const setup = await createTestRenderer({ width: 140, height: 60 })
    try {
      criarTela(setup.renderer, "orq gerente tui", tema)(montarBlocos({ ...lerEstado(tmp, agora), vivoMs: agora - 5000 }), "lido")
      await setup.renderOnce()
      const linhas = setup.captureSpans().lines.flatMap((l) => l.spans)
      return { texto: setup.captureCharFrame(), spans: linhas }
    } finally {
      setup.renderer.destroy()
    }
  }
  const hex = (c: { r: number; g: number; b: number }) => "#" + [c.r, c.g, c.b].map((v) => Math.round(v * 255).toString(16).padStart(2, "0")).join("")
  const t = await quadro(Date.parse("2026-10-01T12:00:30Z"), "dark") // 47 min no despacho, 2 min no integrador
  expect(t.texto).toContain("fila: 2 itens, o mais antigo há 47 min ▲")
  expect(t.texto).toContain("despacho: conserto do cache do E2E há 47 min")
  expect(t.spans.some((s) => s.text.includes("há 47 min") && hex(s.fg) === PALETAS.dark.vermelho)).toBe(true)
  const antes = await quadro(Date.parse("2026-10-01T11:16:30Z"), "light") // 3 min: neutro, sem símbolo
  expect(antes.texto).toContain("o mais antigo há 3 min")
  expect(antes.texto).not.toContain("o mais antigo há 3 min ▲") // a pendência da fixture, de 59 h, é que está crítica
  expect(antes.spans.some((s) => s.text.includes("há 3 min") && hex(s.fg) === PALETAS.light.secundario)).toBe(true)
})
