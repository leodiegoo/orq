// Um BoxRenderable com borda e título por bloco, empilhados; `atualizar` troca só o texto, a árvore fica.
import { BoxRenderable, TextRenderable, type CliRenderer } from "@opentui/core"
import type { Bloco } from "./dados"

export function criarTela(renderer: CliRenderer, rodape: string) {
  const caixas = new Map<string, { box: BoxRenderable; texto: TextRenderable }>()
  const raiz = new BoxRenderable(renderer, { id: "raiz", flexDirection: "column", width: "100%", height: "100%" })
  const pe = new TextRenderable(renderer, { id: "rodape", content: rodape })
  renderer.root.add(raiz)

  return function atualizar(blocos: Bloco[], status: string) {
    for (const b of blocos) {
      let c = caixas.get(b.id)
      if (!c) {
        const box = new BoxRenderable(renderer, { id: b.id, border: true, title: b.titulo, paddingX: 1, flexShrink: 0 })
        const texto = new TextRenderable(renderer, { id: `${b.id}-texto`, content: "" })
        box.add(texto)
        raiz.add(box)
        c = { box, texto }
        caixas.set(b.id, c)
      }
      c.box.title = b.titulo
      c.texto.content = b.linhas.join("\n")
    }
    if (!pe.parent) raiz.add(pe)
    pe.content = `${rodape}   ${status}`
  }
}
