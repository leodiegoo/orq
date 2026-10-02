// Contraste: monta a tela nos dois temas e confere cada texto visível contra o fundo do tema.
import { expect, test } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import { join } from "node:path"
import { VISTA0, lerEstado, montarBlocos } from "../src/dados"
import { criarTela } from "../src/tela"
import { PALETAS, contraste, temaDoColorFgBg, temaForcado, type Tema } from "../src/tema"

const fixtures = join(import.meta.dir, "..", "fixtures")
const agora = Date.parse("2026-10-01T12:00:30Z")
const hex = (c: { r: number; g: number; b: number }) => "#" + [c.r, c.g, c.b].map((v) => Math.round(v * 255).toString(16).padStart(2, "0")).join("")

for (const tema of ["light", "dark"] as Tema[]) {
  test(`it should be text with contrast of 4.5:1 or more on the ${tema} background`, async () => {
    const setup = await createTestRenderer({ width: 140, height: 80 })
    try {
      const e = { ...lerEstado(fixtures, agora, { backlog: join(fixtures, "backlog.md") }), vivoMs: agora - 5000 }
      criarTela(setup.renderer, "orq gerente tui", tema)(montarBlocos(e, VISTA0, { largura: 140, altura: 80 }), "lido")
      await setup.renderOnce()
      const visiveis = setup.captureSpans().lines.flatMap((l) => l.spans).filter((s) => s.text.trim())
      expect(visiveis.length).toBeGreaterThan(20)
      for (const s of visiveis) expect(contraste(hex(s.fg), PALETAS[tema].fundo)).toBeGreaterThanOrEqual(4.5)
    } finally {
      setup.renderer.destroy()
    }
  })
}

const GITHUB = {
  dark: { fundo: "#0d1117", texto: "#c9d1d9", verde: "#3fb950", amarelo: "#d29922", vermelho: "#f85149", azul: "#58a6ff", roxo: "#bc8cff", secundario: "#8b949e" },
  light: { fundo: "#ffffff", texto: "#1f2328", verde: "#1a7f37", amarelo: "#9a6700", vermelho: "#cf222e", azul: "#0969da", roxo: "#8250df", secundario: "#656d76" },
}

for (const tema of ["light", "dark"] as Tema[]) {
  test(`it should be the GitHub ${tema} palette, every color with contrast of 4.5:1 or more on its background`, () => {
    expect(PALETAS[tema]).toMatchObject(GITHUB[tema])
    for (const [nome, c] of Object.entries(PALETAS[tema])) if (nome !== "fundo") expect([nome, contraste(c, PALETAS[tema].fundo) >= 4.5]).toEqual([nome, true])
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
