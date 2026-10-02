#!/usr/bin/env python3
"""orq entry point. The code lives in orqlib.py: as a module it stays in __pycache__,
and as a script Python would recompile 290 KB on every hook (ticket 49: the 100 ms ceiling)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
try:
    import orqlib  # noqa: E402
except Exception as e:  # noqa: BLE001 - broken orqlib (open conflict, half-done edit): the hook exits silent, the command shows the cause
    if sys.argv[1:2] != ["hook"]:
        raise
    import fail_safe
    fail_safe.bail_out("orq.py hook", e)

if __name__ == "__main__":
    sys.exit(orqlib.main())
sys.modules[__name__] = orqlib  # `import orq` returns orqlib itself: tests and scripts see the same globals
