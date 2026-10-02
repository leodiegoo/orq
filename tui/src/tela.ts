// Um BoxRenderable com borda e título por bloco, empilhados; `atualizar` troca só o texto, a árvore fica.
// As linhas dos blocos trazem trechos com cor semântica (verde, amarelo...): aqui cada um vira o hex da paleta do tema.
import { BoxRenderable, StyledText, TextRenderable, bold, fg, type CliRenderer } from "@opentui/core"
import { texto, type Bloco } from "./dados"
import { PALETAS, type Tema } from "./tema"

export function criarTela(renderer: CliRenderer, rodape: string, tema: Tema = "dark") {
  const cor = PALETAS[tema]
  const caixas = new Map<string, { box: BoxRenderable; texto: TextRenderable }>()
  const raiz = new BoxRenderable(renderer, { id: "raiz", flexDirection: "column", width: "100%", height: "100%" })
  const pe = new TextRenderable(renderer, { id: "rodape", content: rodape, fg: cor.secundario })
  renderer.root.add(raiz)

  const estilizado = (b: Bloco) =>
    new StyledText(
      b.linhas.flatMap((l, n) => [
        ...(n ? [fg(cor.texto)("\n")] : []),
        ...(typeof l === "string" ? [l] : l).map((t) => {
          const c = fg(typeof t === "string" ? cor.texto : cor[t.c ?? "texto"])(typeof t === "string" ? t : t.t)
          return typeof t !== "string" && t.b ? bold(c) : c
        }),
      ]),
    )

  return function atualizar(blocos: Bloco[], status: string) {
    for (const b of blocos) {
      let c = caixas.get(b.id)
      if (!c) {
        const box = new BoxRenderable(renderer, { id: b.id, border: true, title: b.titulo, borderColor: cor.borda, paddingX: 1, flexShrink: 0 })
        const texto = new TextRenderable(renderer, { id: `${b.id}-texto`, content: "", fg: cor.texto })
        box.add(texto)
        raiz.add(box)
        c = { box, texto }
        caixas.set(b.id, c)
      }
      c.box.title = b.titulo
      c.texto.content = estilizado(b)
    }
    if (!pe.parent) raiz.add(pe)
    pe.content = `${rodape}   ${status}`
  }
}
