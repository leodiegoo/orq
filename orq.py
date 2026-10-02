#!/usr/bin/env python3
"""Ponto de entrada do orq. O código mora em orqlib.py: como módulo ele fica em __pycache__,
e como script o Python recompilaria 290 KB a cada hook (ticket 49: o teto de 100 ms)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
try:
    import orqlib  # noqa: E402
except Exception as e:  # noqa: BLE001 - orqlib quebrado (conflito aberto, edição pela metade): o hook sai mudo, o comando mostra a causa
    if sys.argv[1:2] != ["hook"]:
        raise
    import fail_safe
    fail_safe.bail_out("orq.py hook", e)

if __name__ == "__main__":
    sys.exit(orqlib.main())
sys.modules[__name__] = orqlib  # `import orq` devolve o próprio orqlib: testes e scripts enxergam as mesmas globais
