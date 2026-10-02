// Bloco de backlog: contagens, bloqueios, worker cruzado com o gerente, ordem, filtros, rolagem, terminal estreito e cores do GitHub.
import { expect, test } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import { join } from "node:path"
import { VISTA0, lerEstado, montarBlocos, proximoFiltro, texto, type Bloco, type Vista } from "../src/dados"
import { criarTela } from "../src/tela"
import { PALETAS, type Tema } from "../src/tema"

const fixtures = join(import.meta.dir, "..", "fixtures")
const agora = Date.parse("2026-10-01T12:00:30Z")
const estado = () => ({ ...lerEstado(fixtures, agora, { backlog: join(fixtures, "backlog.md") }), vivoMs: agora - 5000 })
const backlog = (v: Partial<Vista> = {}, tela = { largura: 120, altura: 80 }): Bloco => montarBlocos(estado(), { ...VISTA0, ...v }, tela).find((b) => b.id === "backlog")!
const linhas = (b: Bloco) => b.linhas.map(texto)
const hex = (c: { r: number; g: number; b: number }) => "#" + [c.r, c.g, c.b].map((v) => Math.round(v * 255).toString(16).padStart(2, "0")).join("")

test("it should be the counts by state in the header, across the machine backlog and the group backlog", () => {
  const b = backlog()
  expect(b.titulo).toBe("Backlog (pronto 3 · andamento 2 · bloqueado 3 · retido 1 · feito 1)")
})

test("it should be each blocked task with who blocks it and whether the blocker is moving or stopped", () => {
  const l = linhas(backlog({ estado: "bloqueado" })).join("\n")
  expect(l).toContain("t13")
  expect(l).toContain("bloqueado por t10 (em andamento)")
  expect(l).toContain("bloqueado por t12 (parado)")
  expect(l).toContain("bloqueado por t10 (em andamento)") // f2, do grupo, espera o t10 do backlog da máquina
  expect(l).not.toContain("t16")
})

test("it should be the worker of an in-flight task from orq agents, and a warning when there is none", () => {
  const l = linhas(backlog({ estado: "andamento" }))
  expect(l.join("\n")).toContain("claude-opus-5-5 · implementing · 4 min sem sinal")
  expect(l.join("\n")).toContain("sem worker no gerente")
})

test("it should be the ready tasks marked, and the one stopped for long marked as stopped", () => {
  const l = linhas(backlog({ estado: "pronto" }))
  expect(l.join("\n")).toContain("● pronto")
  expect(l[l.findIndex((x) => /\bt12 {2}/.test(x)) + 1]).toContain("parado há 11 d")
  expect(l.find((x) => x.includes("t16"))).not.toContain("parado há")
  expect(l.join("\n")).not.toContain("t15") // retido
})

test("it should be priority first, then state, with the done ones out of the default view", () => {
  const ids = linhas(backlog()).filter((x) => /\bP[0-4]|--/.test(x) && /\b[tf]\d+\b/.test(x)).map((x) => x.match(/\b([tf]\d+)\b/)![1])
  expect(ids).toEqual(["t10", "t12", "f2", "t14", "t11", "f1", "t13", "t15", "t16"])
})

test("it should be the state filter and the group filter, with done shown only on request", () => {
  expect(linhas(backlog({ estado: "feito" })).join("\n")).toContain("t09")
  const financeiro = linhas(backlog({ grupo: "financeiro" })).join("\n")
  expect(financeiro).toContain("f1")
  expect(financeiro).toContain("f2")
  expect(financeiro).not.toContain("t12")
  expect(proximoFiltro([null, "pronto", "feito"], null)).toBe("pronto")
  expect(proximoFiltro([null, "pronto", "feito"], "feito")).toBeNull()
})

test("it should be a scroll that moves the window and a window that fits the height", () => {
  const tela = { largura: 120, altura: 44 } // sobra pouco para o backlog
  const topo = linhas(backlog({ offset: 0 }, tela))
  const rolado = linhas(backlog({ offset: 5 }, tela))
  expect(topo.some((x) => /\bt10 {2}/.test(x))).toBe(true)
  expect(rolado.some((x) => /\bt10 {2}/.test(x))).toBe(false)
  const outros = montarBlocos(estado(), VISTA0, tela).filter((b) => b.id !== "backlog").reduce((n, b) => n + b.linhas.length + 2, 0) // as linhas do fixture não quebram
  expect(topo.length + 2 + outros).toBeLessThanOrEqual(44)
})

test("it should be lines that fit a narrow terminal", () => {
  const b = backlog({}, { largura: 60, altura: 80 })
  for (const l of linhas(b)) expect(l.length).toBeLessThanOrEqual(56) // borda e margem da caixa
  expect([b.titulo, linhas(b)[0]]).toEqual(["Backlog", "● 3  ▶ 2  ■ 3  ⏸ 1  ✓ 1"]) // o título cheio não cabe: as contagens descem
})

test("it should be off when no backlog is bound", () => {
  const e = { ...lerEstado(fixtures, agora, { backlog: null }), vivoMs: agora - 5000 }
  const b = montarBlocos(e, VISTA0, { largura: 120, altura: 80 }).find((x) => x.id === "backlog")!
  expect(b.titulo).toBe("Backlog")
  expect(b.linhas.map(texto)[0]).toContain("sem backlog")
})

for (const tema of ["dark", "light"] as Tema[]) {
  test(`it should be the GitHub ${tema} colors on state and priority, each with a symbol or text`, async () => {
    const setup = await createTestRenderer({ width: 140, height: 80 })
    try {
      const p = PALETAS[tema]
      criarTela(setup.renderer, "orq gerente tui", tema)(montarBlocos(estado(), VISTA0, { largura: 140, altura: 80 }), "lido")
      await setup.renderOnce()
      const spans = setup.captureSpans().lines.flatMap((l) => l.spans)
      const cor = (t: string) => hex(spans.find((s) => s.text.includes(t))!.fg)
      expect(cor("● pronto")).toBe(p.verde)
      expect(cor("▶ andamento")).toBe(p.azul)
      expect(cor("■ bloqueado")).toBe(p.vermelho)
      expect(cor("⏸ retido")).toBe(p.amarelo)
      expect(cor("P1")).toBe(p.vermelho)
      expect(cor("P2")).toBe(p.amarelo)
      expect(cor("P3")).toBe(p.secundario)
      expect(cor("em andamento)")).toBe(p.azul)
      expect(cor("(parado)")).toBe(p.amarelo)
    } finally {
      setup.renderer.destroy()
    }
  })
}
