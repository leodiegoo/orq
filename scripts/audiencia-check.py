#!/usr/bin/env python3
"""Procura termos proibidos nos arquivos versionados do repositório público.

A lista fica fora do repositório (ORQ_TERMOS, padrão ~/.claude/orquestrador-plan/termos-proibidos.txt): um termo por linha,
sem diferenciar maiúsculas; `re:` no começo da linha vira expressão regular; `#` comenta. Sem a lista, avisa e sai 0.
Falha (1) com arquivo:linha. Uso: audiencia-check.py [raiz-do-repositório]"""
import os
import re
import subprocess
import sys


def termos(caminho):
    padroes = []
    for linha in open(caminho, encoding="utf-8"):
        linha = linha.strip()
        if not linha or linha.startswith("#"):
            continue
        padroes.append(re.compile(linha[3:] if linha.startswith("re:") else re.escape(linha), re.I))
    return padroes


def achados(raiz, padroes):
    arquivos = subprocess.run(["git", "-C", raiz, "ls-files", "-z"], capture_output=True, text=True, check=True).stdout.split("\0")
    for nome in filter(None, arquivos):
        try:
            texto = open(os.path.join(raiz, nome), encoding="utf-8").read()
        except (OSError, UnicodeDecodeError):
            continue  # binário (png) ou apagado no índice
        for n, linha in enumerate(texto.splitlines(), 1):
            for p in padroes:
                if p.search(linha):
                    yield f"{nome}:{n}: termo proibido /{p.pattern}/"


def main():
    raiz = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    lista = os.environ.get("ORQ_TERMOS") or os.path.expanduser("~/.claude/orquestrador-plan/termos-proibidos.txt")
    if not os.path.exists(lista):
        print(f"audiencia-check: sem {lista}, checagem pulada", file=sys.stderr)
        return 0
    falhas = list(achados(raiz, termos(lista)))
    print("\n".join(falhas))
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
