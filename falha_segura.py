"""Saída dos pontos de entrada de hook quando o import do orqlib falha (marcador de conflito, SyntaxError, arquivo pela metade).

Um hook que quebra derruba o turno de todo worker; um hook mudo só deixa de ajudar. Por isso: exit 0, nada em stdout/stderr e a causa em ORQ_LOG.
Este arquivo é mínimo e não importa o orq: precisa continuar de pé quando o resto não está."""
import os
import sys
from datetime import datetime, timezone


def sair(origem, exc):
    try:
        log = os.environ.get("ORQ_LOG") or os.path.expanduser("~/.claude/logs/orq.log")
        os.makedirs(os.path.dirname(log), exist_ok=True)
        with open(log, "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {origem}: import failed, hook ignored: {type(exc).__name__}: {exc}\n")
    except OSError:
        pass
    sys.exit(0)
