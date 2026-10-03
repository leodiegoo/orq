// Idade de cada ticket no bloco de backlog (ticket 365): o `desde` do digest vira "há N min" na cor da escala e a ordem "mais antigos primeiro".
import { expect, test } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import { cpSync, mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { VISTA0, lerEstado, montarBlocos, texto } from "../src/dados"
import { criarTela } from "../src/tela"
import { PALETAS } from "../src/tema"

const fixtures = join(import.meta.dir, "..", "fixtures")
const agora = Date.parse("2026-10-01T12:00:30Z")
const tmp = mkdtempSync(join(tmpdir(), "tui-idade-ticket-"))
cpSync(fixtures, tmp, { recursive: true })
const tk = (num: string, grupo: string, desde: string) => ({ num, titulo: "x", status: "x", grupo, bloqueios: [], task: null, worker: null, arquivo: "x", desde })
writeFileSync(join(tmp, "digest", "atual.json"), JSON.stringify({ versao: 1, fila: [], pendencias: [], tickets_orq: { abertos: [
  tk("10", "andamento", "2026-10-01T11:13:30Z"), // 47 min, P1: crítico
  tk("12", "pronto", "2026-10-01T11:58:30Z"), //  2 min, P1: neutro
  tk("13", "bloqueado", "2026-10-01T11:40:30Z"), // 20 min, P2: laranja
], resolvidos: [] } }))
const estado = () => ({ ...lerEstado(tmp, agora, { backlog: join(fixtures, "backlog.md") }), vivoMs: agora - 5000 })
const backlog = (v = {}) => montarBlocos(estado(), { ...VISTA0, ...v }, { largura: 140, altura: 80 }).find((b) => b.id === "backlog")!
const linha = (id: string) => backlog().linhas.map(texto).find((l) => new RegExp(`\\b${id} `).test(l))!

test("it should be the age of each ticket on its row, and nothing for a ticket the digest does not know", () => {
  expect(linha("t10")).toContain("há 47 min ▲")
  expect(linha("t12")).toContain("há 2 min")
  expect(linha("t13")).toContain("há 20 min")
  expect(linha("t13")).not.toContain("▲")
  expect(linha("t16")).not.toContain("há ")
})

test("it should be the oldest first when asked, and the age colors on the screen", async () => {
  const ids = backlog({ antigos: true }).linhas.map(texto).flatMap((l) => l.match(/(?:P\d|--) (t\d+)/)?.slice(1) ?? [])
  expect(ids.slice(0, 3)).toEqual(["t10", "t13", "t12"])
  const setup = await createTestRenderer({ width: 140, height: 80 })
  try {
    criarTela(setup.renderer, "orq gerente tui", "dark")(montarBlocos(estado(), VISTA0, { largura: 140, altura: 80 }), "lido")
    await setup.renderOnce()
    const spans = setup.captureSpans().lines.flatMap((l) => l.spans)
    const hex = (c: { r: number; g: number; b: number }) => "#" + [c.r, c.g, c.b].map((v) => Math.round(v * 255).toString(16).padStart(2, "0")).join("")
    const cor = (t: string) => hex(spans.find((s) => s.text.includes(t))!.fg)
    expect([cor("há 47 min ▲"), cor("há 20 min"), cor("há 2 min")]).toEqual([PALETAS.dark.vermelho, PALETAS.dark.laranja, PALETAS.dark.secundario])
  } finally {
    setup.renderer.destroy()
  }
})
