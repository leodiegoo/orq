#!/usr/bin/env python3
"""Ponto de entrada do orq. O código mora em orqlib.py: como módulo ele fica em __pycache__,
e como script o Python recompilaria 290 KB a cada hook (ticket 49: o teto de 100 ms)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import orqlib  # noqa: E402

if __name__ == "__main__":
    sys.exit(orqlib.main())
sys.modules[__name__] = orqlib  # `import orq` devolve o próprio orqlib: testes e scripts enxergam as mesmas globais
