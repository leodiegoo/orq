"""Exit path of the hook entry points when the orqlib import fails (conflict marker, SyntaxError, half-written file).

A hook that breaks takes down every worker's turn; a silent hook just stops helping. Hence: exit 0, nothing on stdout/stderr and the cause in ORQ_LOG.
This file is minimal and does not import orq: it has to stay up when the rest is not."""
import os
import sys
from datetime import datetime, timezone


def bail_out(origin_name, exc):
    try:
        log = os.environ.get("ORQ_LOG") or os.path.expanduser("~/.claude/logs/orq.log")
        os.makedirs(os.path.dirname(log), exist_ok=True)
        with open(log, "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {origin_name}: import failed, hook ignored: {type(exc).__name__}: {exc}\n")
    except OSError:
        pass
    sys.exit(0)
