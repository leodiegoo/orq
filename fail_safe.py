"""Exit path of the hook entry points when the orqlib import fails (conflict marker, SyntaxError, half-written file, Python too old), and the
warning every hook failure leaves behind.

A hook that breaks takes down every worker's turn; a silent hook just stops helping, and nobody notices for hours (ticket 228). Hence: exit 0, the cause in
ORQ_LOG, a marker in ORQ_HOME (hook-failed.json: `orq status` and the manager read it) and, at most once every QUIET_S, a systemMessage on stdout plus a macOS
notification. Writing the marker or the notification failing never breaks the hook.
This file is minimal, stdlib only and written for Python 3.9: it does not import orq, it has to stay up when the rest is not."""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

MIN_PYTHON = (3, 12)  # orqlib uses PEP 701 f-strings (nested same-kind quotes), a SyntaxError before 3.12
QUIET_S = 600  # a new warning only after this long since the last one
MARKER = "hook-failed.json"
HERE = os.path.dirname(os.path.realpath(__file__))


class OldPython(RuntimeError):
    """The interpreter is older than MIN_PYTHON: orqlib would not even compile."""

    def __init__(self):
        RuntimeError.__init__(self, "orq needs Python %d.%d+; this is %d.%d.%d (%s)" % (*MIN_PYTHON, *sys.version_info[:3], sys.executable))


def marker_path():
    return os.path.join(os.environ.get("ORQ_HOME") or HERE, MARKER)  # the clone that runs the hooks; no import, this file has to load alone


def _clock():
    """Epoch seconds; ORQ_AGORA (orq's simulated clock, ISO) wins so the tests can move time."""
    simulated = os.environ.get("ORQ_AGORA")
    if simulated:
        try:
            return datetime.fromisoformat(simulated.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    return time.time()


def _iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hhmm(iso):
    """Local HH:MM of an ISO UTC stamp."""
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone().strftime("%H:%M")
    except (ValueError, AttributeError):
        return "?"


def _where(exc):
    """`file:line` of the failure: the SyntaxError's own position, otherwise the last frame inside orq's folder (the deeper ones are stdlib), otherwise the last frame."""
    if isinstance(exc, SyntaxError) and exc.filename:
        return "%s:%s" % (os.path.basename(exc.filename), exc.lineno)
    import traceback  # late: only the failure path pays for it
    frames = traceback.extract_tb(exc.__traceback__)
    mine = [f for f in frames if os.path.dirname(os.path.realpath(f.filename)) == HERE]
    frame = (mine or frames or [None])[-1]
    return "%s:%s" % (os.path.basename(frame.filename), frame.lineno) if frame else "?"


def read_marker():
    try:
        with open(marker_path(), encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def write_marker(data):
    path = marker_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = "%s.%d.tmp" % (path, os.getpid())
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def _notify(text):
    """macOS notification (text by argv, never spliced into the script); ORQ_ALARME=off turns it off."""
    if os.environ.get("ORQ_ALARME") == "off":
        return
    try:
        subprocess.run([os.environ.get("ORQ_OSASCRIPT") or "osascript", "-e", "on run argv", "-e", 'display notification (item 1 of argv) with title "orq"', "-e", "end run", "--", text],
                       capture_output=True, timeout=3, check=False)
    except (OSError, subprocess.SubprocessError):
        pass


def warn(kind, exc):
    """Records the failure in the marker and returns the systemMessage to show, or None when one went out less than QUIET_S ago (or the marker cannot be written).
    `kind` is `import` for an orqlib that does not load, `run:<hook>` for a hook that raised or timed out. Also fires the macOS notification, at the same pace."""
    try:
        now = _clock()
        old = read_marker()
        data = {**old, "kind": kind, "count": int(old.get("count") or 0) + 1, "first": old.get("first") or _iso(now), "last": _iso(now),
                "exception": "%s: %s" % (type(exc).__name__, exc), "where": _where(exc), "executable": sys.executable,
                "python": "%d.%d.%d" % tuple(sys.version_info[:3])}
        quiet = now - float(old.get("alerted_at") or 0) < QUIET_S
        if not quiet:
            data["alerted_at"] = now
        write_marker(data)
        if quiet:
            return None
        head = str(exc) if isinstance(exc, OldPython) else "hooks broken (%s in %s)" % (type(exc).__name__, data["where"])
        text = "orq: %s, %d failure%s since %s. See ~/.claude/logs/orq.log" % (head, data["count"], "" if data["count"] == 1 else "s", _hhmm(data["first"]))
        _notify(text)
        return text
    except Exception:  # noqa: BLE001 - the warning never breaks the hook: it goes silent as it did before
        return None


def recovered(kind):
    """Removes the marker when it is of this `kind` and was left by this same interpreter: the hook that failed works again. Another interpreter's marker stays."""
    path = marker_path()
    if not os.path.exists(path):
        return
    try:
        old = read_marker()
        if old.get("kind") == kind and old.get("executable") == sys.executable:
            os.remove(path)
    except OSError:
        pass


def broken_line():
    """The line `orq status` and the manager show, or '' while no hook is failing."""
    old = read_marker()
    if not old.get("first"):
        return ""
    return "orq hooks broken since %s (Python %s): %s" % (_hhmm(old["first"]), old.get("python") or "?", old.get("exception") or "?")


def bail_out(origin_name, exc):
    """Exit of an entry point that could not import orqlib: log, marker, warning on stdout (at most one every QUIET_S) and exit 0."""
    try:
        log = os.environ.get("ORQ_LOG") or os.path.expanduser("~/.claude/logs/orq.log")
        os.makedirs(os.path.dirname(log), exist_ok=True)
        with open(log, "a") as f:
            f.write("%s %s: import failed, hook ignored: %s: %s\n" % (datetime.now(timezone.utc).isoformat(timespec="seconds"), origin_name, type(exc).__name__, exc))
    except OSError:
        pass
    text = warn("import", exc)
    if text:
        try:
            print(json.dumps({"systemMessage": text}, ensure_ascii=False))
        except (OSError, ValueError):
            pass
    sys.exit(0)
