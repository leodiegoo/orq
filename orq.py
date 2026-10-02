#!/usr/bin/env python3
"""orq entry point. The code lives in orqlib.py: as a module it stays in __pycache__,
and as a script Python would recompile 290 KB on every hook (ticket 49: the 100 ms ceiling)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import fail_safe  # noqa: E402 - stdlib only and Python 3.9 syntax: it has to load where orqlib does not
if sys.version_info < fail_safe.MIN_PYTHON:  # before orqlib: its f-strings are a SyntaxError under 3.9, and a hook that fails to compile is a hook that went silent
    old = fail_safe.OldPython()
    if __name__ == "__main__" and sys.argv[1:2] != ["hook"]:
        sys.exit(str(old))
    fail_safe.bail_out("orq.py hook", old)
try:
    import orqlib  # noqa: E402
except Exception as e:  # noqa: BLE001 - broken orqlib (open conflict, half-done edit): the hook exits silent, the command shows the cause
    if sys.argv[1:2] != ["hook"]:
        raise
    fail_safe.bail_out("orq.py hook", e)
else:
    if sys.argv[1:2] == ["hook"]:
        fail_safe.recovered("import")  # the interpreter that failed to import loads it again: the marker goes away

if __name__ == "__main__":
    sys.exit(orqlib.main())
sys.modules[__name__] = orqlib  # `import orq` returns orqlib itself: tests and scripts see the same globals
