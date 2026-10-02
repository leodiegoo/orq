# orq

This clone is orq's code and, by default, its state (`ORQ_HOME`), its plan (`plan/`) and its integration worktrees (`.worktrees/`), all gitignored. The README is the reference: [Install](README.md#install), [Projects](README.md#projects), [Where things live](README.md#where-things-live).

## First use

Run this when the user asks to set orq up, or when `orq` is not on `PATH` in a session opened in this clone. A dispatched worker skips it.

1. Check the tools: `orca`, `git`, `gh`, and a Python 3.12+ (`python3 --version`; orq also finds Homebrew, mise, uv or `python3.12`). Optional: `tasks-axi` (backlog), `lavish-axi` (decision pages), `engram`, Bun (`orq manager tui`). Done when you have one line per tool: found with its version, or missing.
2. List what is missing and ask the user before installing any of it. Install only what they approve.
3. Run `python3 orq.py install` from the clone and show its output. Done when a second run prints no `linked:`, `added` or `repointed` line. When it names Codex hooks to trust, tell the user to review them once in `/hooks`.
4. Turn this session into the coordinator: `orq start --objective "<what the user is working on>"`. It needs an Orca terminal; outside one, tell the user to reopen the harness in an Orca terminal inside this clone.
5. Add the user's projects as they name them: `orq project add <path|url> --dry-run`, show the `orca.yaml` and the `ambientes` it proposes, then run it without `--dry-run` once the user agrees.

## Changing orq

The live clone runs the hooks and the manager panel, so work happens in a worktree of your own (`git worktree add -b <type>/<description> .worktrees/<ticket> origin/main`) and the live `main` moves only through `scripts/integrar.py`. Done when `python3 test_orq.py && python3 test_precompact.py` is green in that worktree.
