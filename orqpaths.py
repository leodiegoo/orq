"""Where orq finds itself (ticket 124): the code is the clone this file sits in, and the state, the plan and the integration worktrees live inside it,
all gitignored. ORQ_HOME, ORQ_PLAN and ORQ_WT override each one. Stdlib only and Python 3.9 syntax, like fail_safe: the hooks' fallback path imports it.

Run from a linked worktree, the code answers the main checkout of the repository, so a worker that runs `python3 orq.py` in its worktree reads the live
state, as it did when the default was a fixed path."""
import os

HERE = os.path.dirname(os.path.realpath(__file__))
# legacy: the install path before ticket 124. While it is still a link to the clone, the old plan and worktree folders are read if the new ones do not exist yet.
LEGACY_LINK = os.path.expanduser("~/.claude/orq")  # legacy
LEGACY_PLAN = os.path.expanduser("~/.claude/orquestrador-plan")  # legacy
LEGACY_WT = os.path.expanduser("~/.claude/orq-wt")  # legacy


def main_checkout(path):
    """The main checkout of the repository at `path`. A linked worktree has a `.git` file (`gitdir: <repo>/.git/worktrees/<name>`) and answers `<repo>`; anything else answers `path`."""
    try:
        with open(os.path.join(path, ".git"), encoding="utf-8") as f:
            gitdir = f.read().split("gitdir:", 1)[1].strip()
    except (OSError, IndexError):
        return path
    common = os.path.dirname(os.path.dirname(os.path.join(path, gitdir)))
    return os.path.dirname(common) if os.path.basename(common) == ".git" else path


def _new_or_legacy(new, old):
    """`new`, unless it does not exist yet while the legacy link still stands and `old` exists: read compatibility until the migration."""
    return old if not os.path.exists(new) and os.path.islink(LEGACY_LINK) and os.path.exists(old) else new


CODE = main_checkout(HERE)
HOME = os.environ.get("ORQ_HOME") or CODE
PLAN = os.environ.get("ORQ_PLAN") or _new_or_legacy(os.path.join(CODE, "plan"), LEGACY_PLAN)
WT = os.environ.get("ORQ_WT") or os.environ.get("ORQ_WT_ROOT") or _new_or_legacy(os.path.join(CODE, ".worktrees"), LEGACY_WT)
