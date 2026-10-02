// Contraste: monta a tela nos dois temas e confere cada texto visível contra o fundo do tema.
import { expect, test } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import { join } from "node:path"
import { lerEstado, montarBlocos } from "../src/dados"
import { criarTela } from "../src/tela"
import { PALETAS, contraste, temaDoColorFgBg, temaForcado, type Tema } from "../src/tema"

const fixtures = join(import.meta.dir, "..", "fixtures")
const agora = Date.parse("2026-10-01T12:00:30Z")
const hex = (c: { r: number; g: number; b: number }) => "#" + [c.r, c.g, c.b].map((v) => Math.round(v * 255).toString(16).padStart(2, "0")).join("")

for (const tema of ["light", "dark"] as Tema[]) {
  test(`it should be text with contrast of 4.5:1 or more on the ${tema} background`, async () => {
    const setup = await createTestRenderer({ width: 140, height: 60 })
    try {
      const e = { ...lerEstado(fixtures, agora), vivoMs: agora - 5000 }
      criarTela(setup.renderer, "orq gerente tui", tema)(montarBlocos(e), "lido")
      await setup.renderOnce()
      const visiveis = setup.captureSpans().lines.flatMap((l) => l.spans).filter((s) => s.text.trim())
      expect(visiveis.length).toBeGreaterThan(20)
      for (const s of visiveis) expect(contraste(hex(s.fg), PALETAS[tema].fundo)).toBeGreaterThanOrEqual(4.5)
    } finally {
      setup.renderer.destroy()
    }
  })
}

test("it should be the forced theme from the flag, then the variable, and light from COLORFGBG", () => {
  expect(temaForcado(["--theme", "light"], { ORQ_TUI_THEME: "dark" })).toBe("light")
  expect(temaForcado(["--theme=dark"], {})).toBe("dark")
  expect(temaForcado(["--theme", "auto"], { ORQ_TUI_THEME: "light" })).toBe("light")
  expect(temaForcado([], {})).toBeNull()
  expect(temaDoColorFgBg("0;15")).toBe("light")
  expect(temaDoColorFgBg("15;0")).toBe("dark")
  expect(temaDoColorFgBg("garbage")).toBeNull()
})
