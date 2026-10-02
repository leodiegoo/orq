<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/banner-dark.svg">
  <img alt="orq: a task register and noise filter for coding agents in Orca" src="assets/banner.svg">
</picture>

# orq

A task register and noise filter for a Claude Code or Codex session that coordinates coding agents in [Orca](https://www.onorca.dev).

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)
![Python 3, stdlib only](https://img.shields.io/badge/python-3%20stdlib%20only-3776AB.svg)

## The problem

Orca runs several coding agents side by side: one coordinator session creates Runs and tasks with `orca orchestration`, and workers run in their own terminals. Two things go wrong once more than a couple of workers are busy.

The coordinator drowns in noise. Orca types "You have N orchestration messages" into the coordinator's terminal for every worker message, heartbeats included, and each notice is a new prompt that wakes the model and eats context.

Requests get dropped. The user asks for five things across a long session, the context gets compacted, and nothing records that the third request never became a task. Orca stores tasks, but it has no link between "the user asked for X at 14:02" and what that message turned into.

orq handles both. Hooks record every user prompt as an entry that stays open until the coordinator records what it became. A separate manager terminal soaks up heartbeats so the coordinator only wakes up for results, questions and escalations.

## How it works

```mermaid
flowchart LR
    U([User]) -->|prompt| C["Coordinator<br/>Claude Code session"]
    C -->|"orq despachar / steer / interromper / relancar / liberar"| O[("Orca orchestration")]
    O -->|dispatch| W1[Worker]
    O -->|dispatch| W2[Worker]
    W1 -->|"heartbeat, worker_done, question"| O
    W2 -->|"heartbeat, worker_done, question"| O
    O -->|"notices go to the bound terminal"| M["Agent manager<br/>plain shell, no LLM"]
    M -->|"ack heartbeats"| O
    M -->|"one notice per new batch"| C
    C -.->|hooks| H{{"orq hook prompt / stop / ask / guard / session"}}
    H -->|append| E[("events.jsonl")]
```

The Runs are bound to the agent manager's terminal instead of the coordinator's, so Orca sends every notice there. The manager loop (`painel-agent-manager.sh`, which runs `orq gerente absorver` every 10 s) acknowledges batches that contain only heartbeats and types a single notice into the coordinator when something else arrives.

Intake runs through Claude Code hooks:

```mermaid
sequenceDiagram
    participant U as User
    participant CC as Coordinator
    participant H as orq hooks
    participant L as events.jsonl
    U->>CC: "add a login endpoint"
    CC->>H: UserPromptSubmit
    H->>L: append entrada e12
    H-->>CC: context: open entries, Orca backlog, live workers
    CC->>L: orq intake e12 tarefa task_abc123
    CC->>H: Stop
    H->>L: read entries without an effect
    H-->>U: warning listing any entry still open
```

Every state change is a line appended to `events.jsonl`. Open entries, live workers, pending gates and suspicious answers are all computed from that log by pure functions; nothing reads "the last line" as current state.

## Features

- Deterministic intake. `orq hook prompt` classifies each prompt by its origin (user, Orca notice, task notification, slash command, compaction summary, worker dispatch preamble) and records only user prompts as entries, then injects a status summary of at most five lines.
- Effects. `orq intake <entry> <effect> [ref]` closes an entry as `tarefa` (task), `steer`, `pend`, `decisao` (decision), `conversa` (nothing to do) or `descartado` (discarded, with a note). An entry that still has obligations (see "Obrigações dos avisos") refuses `conversa` and `descartado`. It refuses a task or pending id that does not exist.
- Stop reminder. `orq hook stop` lists the entries still without an effect. It only warns for now; blocking the end of the turn is planned but not built.
- Report ingestion. `orq ingest` turns completed Orca automation runs and `worker_done` messages carrying a `reportPath` into entries, one per numbered action item in the report. A worker whose `worker_done` Orca refused (changed handle) and who left `relatorio-final.md` in its worktree root counts as delivered: with the turn closed and the file newer than the turn start, ingest records a fallback `worker_done` (`origem: relatorio-final`, outcome `succeeded`) and an entry pointing at the file. A real `worker_done` for the same dispatch wins and is never duplicated.
- Heartbeat absorption. Orca notices whose mailbox holds only heartbeats are acknowledged and blocked before they reach the model, both in the prompt hook and in the manager loop.
- Agent manager. `orq gerente ligar|desligar|subir|checar|absorver` binds Runs to a separate terminal and rotates through several Runs, because Orca binds one Run per terminal. A round touches the `gerente-vivo` stamp at its start, after each Run and at its end, and records its duration in `gerente.json` (`voltas_s`, the last 10). The coordinator reports "painel parado" only when the stamp is older than the larger of 90 s and 3× the average round; a stamp between 60 s and that limit reads "painel lento (N s por volta)", because the process is alive and under load.
- `orq iniciar [--objetivo "<front>" | --run <r>] [--agente claude|codex] [--assumir]` turns the Claude or Codex you already opened into the coordinator, and never opens another one. It finds the harness from the processes above it (the inherited environment can be stale), refuses if a hook of the orq is missing from that harness's hooks file (`~/.claude/settings.json`, `~/.codex/hooks.json`), warns when the Codex hooks are installed but not yet trusted, binds the Run (`--run`, a new one from `--objetivo`, or the one the coordinator already commands), raises the agent manager or reuses the live one, and prints `orq status`. A manager that belongs to another live coordinator needs `--assumir`. Running it twice changes nothing.
- `orq despachar` starts a worker with an explicit model and effort, renames its tab, records the dispatch and links the entry.
- Night mode. `orq noite ligar --ate HH:MM [--max-despachos N] [--max-falhas 3]` gives the coordinator a budget: the hooks tell it the rules for an unattended night, and `orq despachar` refuses past the end time, at the dispatch ceiling or after N failures in a row (`orq noite desligar` frees it). See `docs/design.md`, "Night mode". With night mode on, `orq hook externas` also denies push, PR merge, deploy, `--no-verify` and a few destructive commands (see "External-action guard"), and workers start without git prompts.
- Morning card. `orq resumo --noite` prints, in at most 40 lines, how each dispatch of the last night ended (`entregue`, `falhou`, `parou: orçamento|decisão pendente|limite de uso`, `sem worker_done`), dirty worktrees, unpushed commits, parked decisions, whether the manager stayed alive, log gaps that may be sleep, and the commands to paste. `orq liberar` records the ending as `fim_dispatch`; `orq encerrar --parada orcamento|decisao|limite` names a stop. SessionStart adds the first line for 12 hours after the night ends.
- `orq agentes` shows every dispatch across all Runs as running, stuck (no heartbeat for 15 min), asking, delivered or released. Two more states mean "not stuck, on purpose". `aguardando_integracao`: the worker's ticket is on the integrator queue (`orq integrar fila add <branch> <ticket>`, `rm <ticket>`, `lista`), so a worker that would show as parked or stuck waits for the main branch with no steer suggestion, and hibernation counts it as a known wait. `servico`: a dispatch started with `orq despachar --servico` (integrator, secondmate) stays alive after its first `worker_done`, when Orca revokes its capability; it never shows as "delivered, not released", and the worker reports each cycle with `orq ciclo feito --dispatch <id> --hash <commit> [--nota …]`, which does not call Orca. The row reads "serviço, último ciclo HH:MM".
- `orq resumo [--desde <ts>]` prints, on demand and since the last user message, what waits on you, what came in (and what each entry became), what is running, what comes next (ready and blocked tickets) and the decisions made. It is a command, not part of the per-prompt summary.
- Delivery proof: when a `worker_done` cites commit shas, ingest checks them with `git` (worker worktree, then `ORQ_REPOS`, default this repo) and, for a cited PR URL, with `gh` when available. A missing commit or a dirty tree logs an `entrega` event, shown as "entrega sem commit" in `orq resumo` and `orq agentes`. It never blocks the worker.
- `orq liberar` acknowledges a finished worker's messages, releases it and closes its terminal when that is safe. `orq pausar` on a dispatch that already sent its `worker_done` stops at once and says to use `orq liberar`; it no longer waits for a `PAUSA.md` that will not come.
- Worker control. `orq interromper <dispatch>` sends Orca's interrupt to a running worker's terminal. `orq encerrar <dispatch> --motivo <why>` stops it (`worker-stop`) and releases it. `orq relancar <dispatch> --nota <what changed>` stops it and starts another in the same worktree and task (`worker-start --task --retry-of`), keeping the old model and effort unless `--modelo` and `--effort` say otherwise. Each step is an event, and `orq agentes` prints the control history under the dispatch. If the model you asked for does not start, the old one does; if nothing starts, the worktree stays and the error prints the command to repeat.
- `orq passar <dispatch> --para codex|claude` continues a worker on the other harness, in the same worktree and task. It refuses before stopping anything (same harness, worktree gone, Run not bound, no equivalent model, the other harness over its quota limit, no machine slot), then stops the old worker, writes `PASSAGEM.md` in the worktree root (git state, the task's steers, open decisions, the end of the transcript fenced as history, how to search the rest), runs `worker-start --retry-of --agent <other>` with the equivalent model and effort (`--modelo` and `--effort` override), steers the new worker to read the file, releases the old terminal and logs a `passagem` event. If the new worker does not start, the old one stays stopped and retained, the file stays, and the error prints the command to repeat.
- `orq passagem <dispatch> [--para codex|claude]` writes `PASSAGEM.md` in the worktree of a worker that has no turn left (it hit its plan limit), from facts only and within 20 seconds, without stopping or starting anything. Sections in order: next step, open questions (unanswered `question`/`escalation` of the worker plus pending decisions), decisions already made, git state (dirty paths, commits, PR), partial report (last visible answer), end of the transcript fenced as history, where the rest is (`claude --resume`/`codex resume`, `orca search`), how to act (including whether the worker holds the E2E queue). A source that fails or is slow shows as `não li <fonte>`. `orq passar` writes the same file. `orq status` lists passages (`orq passar`) open for more than 15 minutes: the new worker has registered no turn and the event was not accepted.
- `orq retomar [--dry-run]` after a crash: reopens the agent manager and resumes, with `claude --resume` or `codex resume`, each worker that has no `worker_done` and lost its terminal (session id and cwd come from the worker's hooks).
- Machine budget (ticket 79). The machine has 24 GB and 12 CPUs, and fourteen workers starting at once froze it. `orq despachar` now checks `~/.claude/orq/maquina.json` (defaults when absent: `max_workers` 4, `max_caros` 2, `modelos_caros` `claude-opus-*`, `gpt-6-astra*`, `gpt-6-sol*`, `mem_livre_min_mb` 3072, `livre_pct_min` 15, `carga_max` 12, `mem_piso_mb` 1024, `runs_isentos` `["Orquestrador*"]`, `pausar_sob_pressao` false, `max_e2e` 1 for the record only: the E2E queue already serializes stacks) and the machine itself (`vm_stat`, `memory_pressure`, load average, RSS of `claude`/`codex`/`node`/`docker` from `ps`). With no free slot, with the expensive-model ceiling reached, or under memory/load pressure, the request goes to a priority queue (`fila-despacho.json`, answer `{estado: "enfileirado", fila, posicao, motivo}`) with the same model and effort, never a cheaper model; a cheap model still starts while there is a general slot. The manager loop starts one queued item per lap, P1 before P2, oldest first on a tie, and under pressure it stops, tells the coordinator once per episode which worker to `orq pausar` (with `pausar_sob_pressao` true it pauses that worker itself). Pressure is split by owner (ticket 85): `ps` sums the CPU and RSS of the orq's processes (the `claude`/`codex` trees of workers, manager and coordinator, plus anything with `e2e` in the command) against everything else. When most of the load comes from outside (Spotlight, OrbStack, a browser), the notice names the biggest outsiders ("mds_stores 146%, OrbStack 77%"), the manager still holds new dispatches, and it neither suggests nor performs a pause, because pausing a worker would only delay the work. The pause suggestion appears only when the orq's own processes are the cause. The notice also says how many background processes (test runs, monitors) each live worker has, so the coordinator sees who weighs. `orq pausar` ends those processes (SIGTERM, then SIGKILL after `ORQ_ENCERRA_ESPERA_S`, default 5 s) before it closes the terminal and lists them in the `pausa_plano` event; processes outside the worker's tree are left alone (ticket 84). A Run whose id or objective matches `runs_isentos` (the orq's own Run by default) is never queued for lack of resources: it starts under pressure and past `max_workers`, and only the hard limits hold it: `max_caros` (a cost limit, an expensive model still waits for a slot), `mem_piso_mb` of free memory and `max_e2e` (kept by the E2E queue). `orq retomar`, `orq retomar --pausados` and `orq relancar` count against the same budget. `orq maquina [--json]` prints the readings and what the orq would decide now; `orq maquina set <key> <json value>` adjusts it; `orq fila-despacho lista|rm <id>` manages the queue. `orq status`, the 8765 panel (`aberto.json` key `maquina`) and the digest page show slots taken, free slots and the queue.
- Secondmates by group (ticket 80). You talk to one coordinator; the coordinator can hand a whole domain to a secondmate ("mate"), which coordinates that domain's workers in its own Run. A group is `~/.claude/orq/groups/<name>.json` (gitignored): `projetos` (folders), `prefixos` (title prefixes such as `orq:`), and optional `harness`, `modelo`, `effort`, `regras` (a rules file the mate reads first) and `cwd`. `orq grupos` lists groups and mates, and `orq status` adds one line per group that has a mate (`mate orq: vivo (term_x) | pedidos: p1 aguardando`, or `caiu`; a group with no mate gets none); `orq grupos --titulo T [--cwd D] [--grupo G]` says where a request goes: explicit group, then title prefix, then the cwd inside a project folder (compared by file identity, so a symlink or `/tmp` against `/private/tmp` still matches); two groups matching, or none, keeps it with the coordinator. `orq mate abrir <group>` opens the mate in an Orca terminal (`cd <folder>; ORQ_MATE=<group> claude --model <modelo>`, then types the charter once the Claude box shows), or resumes its session if it died; it is not a dispatch, so there is no capability for Orca to revoke after the first `worker_done`. The channel is `events.jsonl`: `orq mate pedir <group> --texto T [--prazo 120] [--responde eN]` records request `pN` and types `orq ▸ pedido pN ...` into the mate; the mate answers with `orq mate subir --corr pN --tipo resposta --texto ...` and raises decisions, ready branches, blockers and summaries with `orq mate subir --tipo decisao|pr|bloqueio|resumo`, each an entry (origin `mate`) the coordinator closes with `orq intake` (effect `mate` when `orq mate pedir --responde eN` answered it). Only a reply with the same `corr` resolves a request. The deadline counts from the end of the mate's turn that received it; the manager loop reposts once, then tells the coordinator once, never in a loop (`orq mate pedidos` lists the open ones). The manager also announces each new entry from a mate and a mate whose terminal vanished (following the deferral of "Notices never land in the middle of what you type"), and `orq retomar` resumes a fallen mate. Entries typed into a mate carry `grupo` and never show in the coordinator's summary. A mate never opens AskUserQuestion (the guard hook refuses it) and never pushes: the coordinator does, after a `pr` entry. See `docs/design.md`, "Secondmates by group".
- `orq steer` sends a correction to a running worker and, if Orca did not notify it and it sits idle at its prompt, types the notice into its terminal. A worker in the middle of a turn gets the notice too, with up to 300 characters of the correction, when its screen shows the spinner (`esc to interrupt`), the input box is empty and no menu is waiting for an answer; Claude Code queues the text and injects it at the next tool result (`steer_digitado_ocupado` event). The manager loop (or `orq steers`) then checks that the worker read it: with no read after 90 s and the worker idle it types the notice again, up to 3 times, and then records a "steer não lido" alert in the summary and in `orq agentes`. A worker with an open question gets neither the retype nor the alert. A read is the message's `read` flag in Orca's inbox or its id in the worker's transcript, because Orca only sets `read` on `check --ack`.
- Hibernation (ticket 60). Every worker is a live `claude` with its MCP servers, and it holds memory while it sits at the prompt. `orq hibernar <task|dispatch>` stores the session (id, cwd, model, effort, task) in `cursor.json` `hibernados` and closes the terminal; the task does not change in Orca. The manager loop does it by itself, at most once a minute, with no LLM, when the worker's turn has ended (hooks) and the screen, the processes and orq's own state agree. The criterion, with N = 15 min (`ORQ_HIBERNA_MIN`, or `{"min": N}` in `ORQ_HOME/hibernar.json`):
  - parked at the prompt for more than N min, or delivered (`worker_done`) and not released for more than N min; or
  - waiting for something outside that orq already knows (a pending item linked to the task, an open PR linked to the task, a ticket blocked by an unresolved one), after only 2 min parked (`ORQ_HIBERNA_EXTERNA_MIN`, or `"externa_min"`).
  - Never: the coordinator, the manager, the Run's coordinator, a terminal with no session and cwd from the worker hooks (not an orq worker), a worker with a question or permission stuck on screen (those keep escalating), a spinner (`esc to interrupt`), `N shell still running`, a draft in the input box, or a live child process (`/shell-snapshots/` shell under the `claude` of that worktree: E2E, test, build). With no proof about child processes (no `claude` found with that cwd, or Codex, which has no pattern) the automatic mode skips the worker; `orq hibernar --forcar` passes only that.
  - It wakes up (`orq acordar <task|dispatch> [--texto …]`, same `claude --resume` path as `orq retomar`, message with the escalation command) when `orq steer` or `orq responder` needs the worker (the message travels in the resume), or, in the manager loop, when a pending item linked to its task is answered or its PR is merged or closed. A worker hibernated after delivering is not woken by those two. `orq retomar` skips hibernated workers; `orq liberar` and `orq encerrar` forget them.
  - `orq agentes` and the digest show "hibernado desde HH:MM", and `orq agentes` ends with the memory freed: the sum of the RSS of the `claude`/`codex` processes and everything under them, measured before and after each close (also in the `hibernar` event). See `docs/design.md`, "Hibernating idle workers".
- `orq responder <msg_id> "<text>"` answers a worker's question through the manager's handle, binding the manager to the message's Run first (a bare `orca orchestration reply` fails with `consumer_fenced` from another Run).
- Pause warning proposes the handover. When a harness reaches `semana_pausa` and the other is at `ok`, the manager's line lists `orq passar <dispatch> --para <other>` for each worker `orq pausar` would pause, so the coordinator can move them instead of parking them.
- Notices never land in the middle of what you type. The manager types into the coordinator only with away mode on (`orq away on`); otherwise every notice waits for your next prompt (ticket 107). With away mode on, it types its notices (PR merged or closed, stuck E2E queue, plan usage, a worker menu on screen, machine pressure) into the coordinator only when your last prompt is older than 10 minutes (`ORQ_COORD_OCIOSO_MIN`); the line that wakes it for a worker's message waits 2 minutes instead (`ORQ_WAKE_OCIOSO_MIN`); before that they wait in `cursor.json` and show up once in the context of your next prompt, or get typed if you stay quiet. Every typing reads the input box twice, 3 seconds apart (`ORQ_AVISO_GAP_S`), and a draft in either read cancels it, for workers too. `"notificar_macos": true` in `gerente.json` adds a macOS notification for each deferred notice (off by default). Orca has its own notice (`You have N orchestration messages` without `--terminal`) that it types into the Run's `coordinator_handle`; it has no off switch, and orq keeps the coordinator from being that handle. See `docs/design.md`, "Notices to the coordinator".
- Pull requests linked to a task. The question "can I merge?" is answered by `orq fila lista`: each step shows ✓ ready, ✗ the red check by name, ⚠ conflict (also between steps of the queue) or ⏳ CI running, plus the next step to merge (GitHub ties checks to the commit, and one branch opens a PR per environment, so a red check from another environment's deploy workflow, such as Web Deploy Staging on the PR to main, does not count: it shows as `ℹ falha em outro ambiente`); the poll keeps this from one `gh pr list` per repository. `orq pr ligar <task> <url> [--issue N]` registers the requests of a feature (development, staging, main). A light poll outside the hooks (`orq pr poll`, and every manager lap, at most every 2 minutes) wakes the coordinator only when a linked request is merged or closed, with a line such as "PR #1216 entrou em development", and `orq status` shows where each feature stands and what comes next. The next request is only suggested, never opened. The `orq hook prligar` PostToolUse hook links the request by itself when the coordinator runs `gh pr create` (by the branch's worktree or dispatch name); a branch with no known task is listed as "PR sem tarefa" in `orq status`, and a linked request also joins the merge queue by itself (ticket 111): it enters the open step of its task (same group, main or development/staging; a `merge/<feature>-<env>` branch counts as its feature), otherwise it opens a new step named after the dispatch title (or the PR title), with the first paragraph of the PR body (120 characters) as the reason; a main request opens "<name> para main" and, when development and staging already went in, says "development e staging já entraram (#a, #b)". Edit or reorder with `orq fila`; a request with no task never enters the queue. and the wake-up says when development and staging both went in and only main is left. See `docs/design.md`, "Pull requests linked to a task".
- Digest and away mode. `orq digest [--desde <ts>] [--html] [--abrir]` writes `~/.claude/orq/digest/atual.json`, the file the dashboard reads: the merge queue, features, pending items, what happened, and live workers. The queue is what the coordinator declares with `orq fila add|feito|rm|lista` (each step: name, why, PR numbers) or, when none is declared, the order from the tickets' `Blocked by`. `--html` adds a page, `--abrir` opens it in an Orca tab. While away mode is on, `statusline.sh` adds `away desde HH:MM` (yellow) to the first HUD line; see `docs/design.md`. `orq ausente ligar|desligar` (alias `orq away [on|off|status]`, no Claude Code `/away`; bare `/away` toggles and, when turning off, prints the panel link and the timeline count without opening a tab) makes the coordinator's Stop hook log each reply and refresh the file. It reads only orq's own files: no `gh`, no Orca call, so PR state is as fresh as the last `orq pr poll`. See `docs/design.md`, "Digest and away mode". The digest also carries `tickets_orq`: the open tickets from `issues/` (grouped as in progress, ready or blocked, with the open blockers and the live worker state) plus the 5 latest resolved ones. Away mode also changes what the coordinator does, enforced by hooks (ticket 126): the `AskUserQuestion` guard denies the box and points to `orq pend add --tipo decisao` (park the decision, keep going on what does not depend on it; a worker stuck on it gets `--task <id>` so its task blocks); `orq perguntar` still builds and opens the Lavish page but saves the link on the pending item and returns at once, without polling; a hibernated worker does not take a machine slot; and the Stop hook blocks the end of the turn (`decision: block`, the reason names the next step) while there is work that needs no user: a delivery not yet in the integrator queue, an integrator cycle whose commits have no push, or a ready P1/P2 ticket with `Modelo:`/`Effort:` and a free slot. The same reason blocks at most 3 times in 30 minutes, then the Stop lets go. `orq away off` lists the items opened during the absence, decisions first, each with its Lavish link.
- Retro. `orq retro [--desde D] [--ate D] [--projeto TRECHO] [--json] [--sem-gh] [--sem-transcritos] [--gravar]` counts, with no LLM, the failure signals of a window (default 7 days): dispatches that never started, steers with no proof of reading, retries and interventions, workers released dirty, deliveries with no commit, user rules broken in worker transcripts (push, trailer, writes to `~/.agents`, deploys, work in the live checkout), PRs with a red check, and the user's corrections right after a delivery. Each case has a pointer (`events.jsonl:<line>`, `transcript:<line>`, PR URL); a window with no failures says `falhas no período: 0`, and `n/d` marks what was not consulted. `--gravar` keeps the numbers so the next run and `orq digest` show the trend. The `orq-retro` skill (a Sonnet worker, weekly Friday 16:00 as a proposed Orca automation, or on demand) reads the collector and proposes at most five changes as a Lavish page, each a check, a text or a `worker-routing` calibration. Nothing is applied without your ok. See `docs/design.md`, "Retro".
- Tickets as markdown files (`orq ticket novo|fechar|lista`), each backed by an Orca task with dependencies. Closing a ticket frees what waited on it: `orq ticket fechar` removes the number from the `Blocked by:` line of every dependent, moves the dependent's Orca task from `blocked` to `ready`, and reports the ones left with no blocker as `liberados` (also a line in `orq status` — `liberados: 88, 91 (P1, P2)` — until the ticket is dispatched). A freed ticket of priority 1 or 2 whose header declares `Modelo:` and `Effort:` goes into the dispatch queue (ticket 79) and the manager starts it when a slot opens; without them the command only warns, and priority 3 never starts by itself.
- `orq doctor tasks [--dry-run] [--json]` crosses the open Orca tasks of every Run with the tickets: a `blocked` or `pending` task whose ticket is resolved becomes `completed` with `supersededBy`, and one with no ticket is listed for the coordinator to decide.
- No manual `run-use`. `orq ticket novo`, `steer`, `liberar` (and `ticket fechar`, `doctor tasks`) bind the Run of the task themselves and, when they finish, bind back the Run that was bound before. With the agent manager linked nothing is switched (that would take the coordinator out of the Run the manager holds); the refusal still names `orq gerente ligar`.
- A pending list for the user (`orq pend add|done`), stored as JSON for a dashboard to read. A decision can hold an Orca gate on a task until it is answered.
- AskUserQuestion guard. While any worker is running, the question widget is refused and the decision goes through a separate page (see [design](docs/design.md#askuserquestion-guard)). It is the `python3 ~/.claude/orq/orq.py hook ask` PreToolUse hook (matcher `AskUserQuestion`), already in `settings.hooks.example.json`; with away mode on the same hook denies the box even with no worker running (ticket 126), so there is nothing new to register in `settings.json`.
- Compaction handoff. `precompact.py` snapshots the coordinator's state on PreCompact and injects it back after `/compact`; `orq hook session` injects status and open tickets on every session start.
- `worker-routing` skill plus a PreToolUse guard that refuses `orca orchestration worker-start`, `orq despachar` or an Agent call without an explicit model and effort.

## Requirements

- macOS or Linux (`fcntl` locks and `SIGALRM`), Python 3 with the standard library only (developed and tested on 3.14)
- [Claude Code](https://docs.claude.com/en/docs/claude-code) or [Codex CLI](https://developers.openai.com/codex) for the coordinator and workers (either one, or both)
- [Orca](https://www.onorca.dev) with the `orca` CLI on `PATH`
- git

Optional: `gh` (open PRs in the handoff, branch cleanup), `engram` (the handoff is also saved there), and `lavish-axi` if you want to use the decision page the guard points to. `orq perguntar` runs `lavish-axi` itself; the rest only parses its `poll` output in `orq lavish-resposta`.

## Install

```sh
git clone https://github.com/leodiegoo/orq.git ~/.claude/orq
mkdir -p ~/.claude/hooks ~/.claude/scripts ~/.claude/skills ~/.local/bin
mkdir -p ~/.claude/orquestrador-plan/issues

for f in worker-routing-guard.py limpar-mergeados-hook.py; do
  ln -s ~/.claude/orq/hooks/$f ~/.claude/hooks/$f
done
for f in limpar-mergeados.py orca-wait-runs.py trust-cwd.py; do
  ln -s ~/.claude/orq/scripts/$f ~/.claude/scripts/$f
done
ln -s ~/.claude/orq/skills/worker-routing ~/.claude/skills/worker-routing
ln -s ~/.claude/orq/skills/orq-retro ~/.claude/skills/orq-retro   # the weekly retro analysis
mkdir -p ~/.claude/commands
ln -s ~/.claude/orq/commands/away.md ~/.claude/commands/away.md   # /away slash command
ln -s ~/.claude/orq/orq.py ~/.local/bin/orq   # the manager loop calls `orq`
```

Then merge the `hooks` section of [`settings.hooks.example.json`](settings.hooks.example.json) into `~/.claude/settings.json`, next to any hooks you already have.

For Codex, merge [`codex.hooks.example.json`](codex.hooks.example.json) into `~/.codex/hooks.json`, adding each group at the end of its event: Codex records hook trust by position (`hooks.json:<event>:<group>:<hook>`), so inserting a group in the middle unsets the trust of the ones after it. `orq hooks-codex` does that merge for you: it appends the missing groups at the end of each event and never reorders or removes anything. Review the new hooks once in `/hooks`, or start Codex with `--dangerously-bypass-hook-trust`. Until they are trusted the orq does not see that terminal, so `orq status`, `orq agentes` and the coordinator's session preamble start with "⚠ hooks do orq não confiados no Codex: rode /hooks" (position check of the `hooks.json` hooks against `[hooks.state]` in `~/.codex/config.toml`; it cannot see a hook edited after it was trusted, because the hash is not documented). Then link the skills where Codex reads them:

```sh
mkdir -p ~/.agents/skills
ln -s ~/.claude/orq/skills/worker-routing ~/.agents/skills/worker-routing
ln -s ~/.claude/orq/skills/away ~/.agents/skills/away   # $away, the Codex side of /away (Codex has no user slash commands, so `$away on|off|status` is the form)
``` The orq hooks exit at once outside an Orca terminal and in worker sessions, so they are safe to install globally. The worker-routing guard is the exception: it checks dispatch commands in every session.

The branch cleanup reads branch patterns to keep from `~/.claude/scripts/limpar-mergeados.keep` (one glob per line; a missing file means none). Write your own. It treats `main`, `development` and `staging` as protected branches (override with `ORQ_PROTECTED_BRANCHES`, comma-separated) and counts a branch as finished only when a PR into `main` merges it (`ORQ_FINAL_BASE`). Untracked files the orq itself asked the worker for (`PAUSA.md`, `PASSAGEM.md`, `relatorio*.md` in the root, `.scratch/*/relatorio-final.md`) do not count as changes: they are copied to `~/.claude/orquestrador-plan/relatorios/<worktree>-<file>` (`ORQ_RELATORIOS`) before the worktree goes, and the summary lists them under `guardados`. Any other untracked or modified file still blocks, with no `--force`. Besides the "merged" prompt hook, `orq pr poll` starts the cleanup in the background (same 20 s delay, `ORQ_LIMPAR_ATRASO_S`) for the branch of each PR it sees merged into `main`, with `--branch` and `--task`; the repo comes from the dispatch's project, else the cwd. The result lands in the log as a `pr`/`limpou` event (removed, kept, skipped), after the `pr`/`limpeza` event that marks the start.

Closed PRs without a merge (ticket 104). When every PR linked to a task is closed without merge and none is open, `orq pr poll` records a `fechada` event. After `ORQ_FECHADO_DIAS` days (default 1), or right away with `orq limpar --fechados [--dry-run]`, the orq copies the worktree's `relatorio-final.md` to `~/.claude/orquestrador-plan/relatorios/` (`ORQ_RELATORIOS`), removes the worktree with `orca worktree rm --run-hooks`, and deletes the local and the remote branch (`limpou_fechado` event). GitHub restores the remote branch from the closed PR (Restore branch button); the event says so. It never touches an environment branch, a branch that is the head or base of an open PR, or a task with any open or merged PR. Until the first real `orq limpar --fechados` (it writes `limpar-fechados.json`), the automatic run only prints a `limparia …` preview, once per task. If the worktree cannot be removed or the report cannot be saved, the branches stay.

### Where things live

| Path | Contents | Override |
|---|---|---|
| `~/.claude/orq/` | runtime state: `events.jsonl`, `cursor.json`, `aberto.json`, `gerente.json`, `gerente-vivo` (the manager panel's heartbeat), locks, `handoff/` (all gitignored) | `ORQ_HOME` |
| `~/.claude/orquestrador-plan/issues/` | tickets, `NN-<slug>.md` | `ORQ_ISSUES` |
| `~/.claude/orquestrador-plan/desenho.md` | your own design notes; orq only prints this path at session start and in the handoff | `ORQ_MAPA`, `ORQ_DESENHO` (precompact) |
| `~/.claude/dashboard/data/pendencias.json` | the user's pending list | `ORQ_PENDENCIAS` |
| `~/.claude/logs/orq.log` | errors from hooks that failed open | `ORQ_LOG` |
| `~/.claude/projects/` | Claude Code transcripts, read by `orq liberar` and `orq retro` | `ORQ_PROJETOS` |
| `~/.claude/orq/groups/` | one JSON per group of projects with a secondmate (ticket 80) | `ORQ_HOME` |
| `~/.claude/orq/retro/` | the numbers of each saved `orq retro --gravar` round (no cases) | `ORQ_HOME` |
| `~/.codex/config.toml` | `orq despachar --agente codex` adds `trust_level = "trusted"` for the repository root and the new worktree | `ORQ_CODEX_CONFIG` (or `CODEX_HOME`) |

`orq auditar-respostas` reads the coordinator's transcripts from `ORQ_TRANSCRITOS`. By default that is the Claude Code project folder for the current directory (`~/.claude/projects/` plus the cwd with every character outside `[A-Za-z0-9]` turned into `-`), so run it from the coordinator's working directory or set the variable. Other knobs: `ORQ_ORCA` (path to the Orca binary), `ORQ_ORCA_TIMEOUT` (seconds per Orca call, default 2.5), `ORQ_NO_BG=1` (no background refresh), `ORQ_GERENTE_PRESO_S`, `ORQ_OCIOSO_MS`, `ORQ_STEER_ESPERA_S`, `ORQ_WAIT_POLL`, `ORQ_WAIT_MAX`.

## Usage

Start the manager in a plain shell terminal inside Orca, then bind your Run to it from the coordinator:

```sh
# manager terminal
echo $ORCA_TERMINAL_HANDLE          # term_manager
~/.claude/orq/painel-agent-manager.sh

# coordinator, one command (creates the Run, raises the manager, binds the Run to it)
orq iniciar --objetivo "Auth work"

# or by hand, after `orca orchestration run-create --objective "Auth work"`
orq gerente ligar --terminal term_manager
orq gerente desligar --run run_demo  # hand one Run back; without --run, all of them
```

Record what each entry became:

```sh
$ orq intake e12 tarefa task_abc123
{"tipo": "intake", "entrada": "e12", "efeito": "tarefa", "ref": "task_abc123", "run": "run_demo", "origem": "usuario", "texto": "add a login endpoint with rate limiting"}
$ orq intake e13 conversa
$ orq intake e14 descartado --nota "duplicate of e12"
```

Tickets and dispatch:

```sh
$ orq ticket novo --titulo "Add login endpoint" --spec-arquivo spec.md --blocked-by 01
{"ticket": "02", "arquivo": "~/.claude/orquestrador-plan/issues/02-add-login-endpoint.md", "task": "task_abc123", "run": "run_demo"}
$ orq ticket lista
01 Add session store (claimed)
02 Add login endpoint (ready-for-agent; Blocked by: 01)
$ orq despachar --run run_demo --ticket 02 --modelo <model-id> --effort medium \
    --worktree new-top-level --name add-login-endpoint --entrada e12
{"dispatchId": "ctx_abc123", "taskId": "task_abc123", "run": "run_demo", "terminal": "term_abc123", "espera": "python3 ~/.claude/scripts/orca-wait-runs.py run_demo", "entrada": "e12"}
$ orq ticket fechar 02 --answer notes/login-done.md
```

Without a ticket, `orq despachar --run r --titulo "..." --spec-arquivo f --modelo m --effort e` creates the task itself. The spec for `ticket novo` must contain a `## Acceptance criteria` section.

Workers:

```sh
$ orq agentes
travado     task_abc123  Add login endpoint  <model-id>  term_abc123  implement 13:40 (há 22 min)
            -> orq steer task_abc123 "<ajuste>" --run run_demo
rodando     task_def456  Fix flaky test  <model-id>  term_def456  test 14:01 (há 1 min)
entregue    task_789abc  Update the README  <model-id>  term_789abc
            -> orq liberar ctx_789abc
$ orq steer task_abc123 "Use the existing rate limiter instead of writing one" --run run_demo
$ orq liberar ctx_789abc
```

`orq agentes` also takes `--json`, `--run r` and `--todos` (includes released workers and terminals Orca retained).

The user's pending list:

```sh
$ orq pend add --id pick-db --tipo decisao --titulo "Postgres or SQLite for sessions?" --task task_abc123
$ orq pend add --id rotate-key --tipo acao --titulo "Rotate the staging API key" --espera "ops team"
$ orq pend done pick-db --resposta "Postgres"
```

`--tipo` is `acao`, `decisao` or `avisar`. A decision id is at most 12 characters because it doubles as the AskUserQuestion `header`, and answering that question closes it. `--task` creates an Orca gate that holds the task until the decision closes.

Status at any time (the same summary the prompt hook injects):

```sh
$ orq status
[orq] Sem efeito: e9 ('Fix flaky test in the auth suite').
Aberto (cache de 14:02): backlog 3 (task_abc1… 'Add session store', 4 d), rodando 2, bloqueado 0, gates 0. Vivos: task_def4… test 14:01.
Com você: 2 (1 decisões).
Efeito: orq intake <e> tarefa <task>|steer <task>|pend <id>|decisao <id>|conversa|descartado --nota <motivo>
```

Ask the user a decision on a page, the same way on Claude Code and Codex: `orq perguntar --id <pend> --pergunta "..." --opcao "A" --opcao "B" [--recomendada N] [--espera-min M]`. It creates the decision if the id is new, builds a Lavish page (one radio per option, the recommended one only labelled, never preselected; free text, "decide later" and "let's talk"), opens it in Orca's browser, waits on `lavish-axi poll` and records the answer like `orq lavish-resposta`: only an explicit choice closes the decision. No answer (timeout, session ended, empty choice) leaves it open with a warning. It blocks until the answer, so run it as the harness's background job.

Also available: `orq ingest [--refresh]`, `orq alerta visto <task>`, `orq lavish-resposta <file|->` and `orq auditar-respostas [--sessao id]`.

## Obrigações dos avisos

A notice often implies work that has nothing to do with the notice being "seen". A merged `main` request means checking the production deploy, updating the issue comment and cleaning the branch. orq writes that work down when the notice arrives, so it does not depend on the coordinator remembering it (ticket 114). The same hooks run it on Claude Code and Codex (`UserPromptSubmit` and `Stop`).

| Notice | Obligations (key: text) |
|---|---|
| request merged into `main` | `deploy`: check the production deploy (quave-one); `comentario`: update the comment on the linked issue; `limpeza`: check that the branch and worktree are gone; `ticket`: close the task's ticket |
| request merged into `development` or `staging` | `deploy`: check that environment's deploy; `proximo`: open the next request, or postpone with the reason to hold |
| `worker_done`, a worker question, an automation report | none created: the worker list, `orq responder` and the existing report triage already hold them |
| machine pressure from outside | none |

An obligation that needs a value it does not have is not created. The issue comes from `orq pr ligar --issue N` or from an `issue: #N` line in the header of the task's ticket; with neither, there is no `comentario`. With no open ticket for the task there is no `ticket`, and with no next environment suggested (another request of the feature still open) there is no `proximo`. The table is `OBRIGACOES` in `orqlib.py`.

The obligations are tied to the notice's entry (`eN`) and show in the prompt's extra line, also on the line the manager types for the merge, until each is closed:

```text
A fazer por você: e484 → deploy (conferir o deploy de produção (quave-one)), comentario (atualizar o comentário da #2045), limpeza (…).
```

```sh
orq feito e484 comentario --prova "https://github.com/<org>/<repo>/issues/2045#issuecomment-1"
orq adiar e484 deploy --motivo "the deploy runs tomorrow"   # creates an "a fazer depois" ticket with the reason
```

While an entry has an open obligation, `orq intake eN conversa` and `descartado` are refused. Closing the last obligation closes the entry too. Whatever orq can confirm by itself it closes and only records: `orq ticket fechar NN` closes the `ticket` obligation. The coordinator's Stop hook warns once about each obligation open for more than 10 minutes (`ORQ_OBRIGACAO_MIN`); it never blocks.

## Tests

```sh
python3 test_orq.py              # fake Orca, temporary ORQ_HOME
python3 test_precompact.py
python3 scripts/limpar-mergeados.py --self-test
```

Editing orq: hooks and the manager panel execute `~/.claude/orq/orq.py` while it runs, so a half-edited file stops them (the panel once died ten laps in a row on a `NameError`). Work in a separate worktree (`git worktree add ../orq-<topic>`), run the tests there, and move the live copy only through `scripts/integrar.py <branch>...` (never `git merge`, `git pull` or `git checkout` inside `~/.claude/orq`): it merges in `~/.claude/orq-wt/integra-<branches>`, runs the tests there and advances `main` by fast-forward only when they pass; on a conflict you resolve in that worktree, commit, and run `integrar.py --avancar <worktree>`. If `orqlib.py` still fails to import, the hooks (`orq.py hook`, `precompact.py`, `limpar-mergeados-hook.py`) exit 0 with no output and log `import falhou` to `~/.claude/logs/orq.log` instead of breaking the worker's turn. See `docs/design.md`, "Integrating branches outside the live checkout". The panel writes `gerente-vivo` on every lap from its own shell; `orq status`, `orq resumo` and the prompt hook warn when it is older than 60 s.

Audience check: `git config core.hooksPath githooks` runs `scripts/audiencia-check.py` before each commit. It scans tracked files for the terms in a private list outside the repo (`ORQ_TERMOS`, default `~/.claude/orquestrador-plan/termos-proibidos.txt`: one term per line, `re:` prefix for a regex) and prints `file:line`. Without the list it skips with a warning.

## Projects

One file per project in `~/.claude/orq/projects/<name>.json` (under `ORQ_HOME`, gitignored), read on every call, no cache and no daemon:

```json
{"repo": "path:/Users/me/code/my-app", "harness": "codex", "grupo": "work",
 "ambientes": [{"branch": "development"}, {"branch": "staging"}, {"branch": "main", "producao": true}], "fluxo": "promocao"}
```

- `repo` is the only required key: the Orca repository selector (`path:`, `id:` or `name:`). `harness` is the default of `orq despachar` for that project's workers (`claude` when absent). `grupo` is free text that only groups the listing. `orca` holds the `orq projeto add` overrides (below). Other keys are ignored for now.
- `ambientes` is the ordered list of the project's environments (branches); the one marked `producao` is production, the last one when none is marked. `fluxo: "promocao"` means the same feature branch opens one PR into each environment, in order; `fluxo: "direto"` means one PR, into production. With no `ambientes` block the project has one environment, the remote's default branch (`origin/HEAD` of the repo), and the direct flow. A block that is not a list of `{"branch"}`, repeats a branch, marks two productions or names another `fluxo` makes the file `inválido`. Nothing in orq names `development`, `staging` or `main` any more: `orq pr` (`pronto para <next environment>`, `em <production>`), `orq fila`, the digest legend and the dots on its PR chips, and the base of new worktrees all read this block. The project of a feature is the one of the Run that dispatched it, else the one containing the cwd, else the default above. A project that declares development, staging and main gets the texts orq always had.
- `orq projeto add <path|url> [--nome N] [--harness claude|codex] [--grupo G] [--orca-yaml FILE] [--substituir-orca-yaml] [--dry-run] [--json]` adds a project in one step (ticket 125). A URL is cloned into `--destino` (default `~/Developer/<name>`). It writes `projects/<name>.json` (kept as is when it exists, but refused when it points at another repository), registers the repository in Orca when `orca repo list` does not have it (`orca repo add`, then `orca repo set-base-ref origin/<production of the project>`; a repository Orca already knows keeps its base) and writes `orca.yaml` at the repository root. Everything is validated before anything is touched.
  - **`orca.yaml`.** The fixed part is always there: trust the folder in the project's harness (`python3 ~/.claude/scripts/trust-cwd.py` for Claude, `orq projeto confiar` for Codex) and the `.scratch` sync in (`setup`) and back (`archive`). The rest is a block per marker the repository has, never a default: `install` (a lockfile: `pnpm-lock.yaml`, `yarn.lock`, `package-lock.json`, `bun.lock`, `uv.lock`), `setup_script` (`scripts/setup-worktree.sh`), `graphify` (`graphify-out/`), `meteor` (a `.meteor/` at the root or one folder down, cleans its `local` and `_build`) and `e2e` (`scripts/e2e-infra.sh` with a `destroy)` case, else `docker-compose.e2e.yml`). A repository with only `package-lock.json` gets the trust, the `.scratch` sync and `npm ci`, nothing else.
  - **The agent proposes.** The blocks do not have to come from the detection: when the agent adds a project, it reads the repository (lockfile, setup scripts, E2E, Meteor, graphify, Docker…), writes the `setup` and `archive` blocks that make sense for it into a file, shows `orq projeto add … --dry-run` to the user, and passes the file with `--orca-yaml`. The proposal replaces the detected blocks; the fixed part is added around it (do not write it in the proposal; a line equal to a fixed one is dropped). A real project's `orca.yaml` is an example, not a rule: a project that is not Meteor gets no Meteor block. orq only validates: it reads the subset Orca understands (top-level keys `scripts`, `setupAgentStartupPolicy`, `issueCommand`, `defaultTabs`, `environmentRecipes`, `worktree`; under `scripts` only `setup` and `archive`, as a `|` block or a plain one-liner; spaces, no tabs) and refuses anything else with the line, before writing. It also reads the file it generated back and refuses a result that differs.
  - **Overrides in the project file**, for when the detection is wrong: `"orca": {"blocos": {"meteor": false, "graphify": true}, "setup_extra": ["make bootstrap"], "archive_extra": ["rm -rf .cache"]}`. `false` removes a detected block, `true` adds one the detection missed (with a generic command), the `*_extra` lists are appended last. They apply to the detected blocks and to the extras of a proposal alike; `blocos` only to the detection.
  - **An `orca.yaml` already in the repository is never overwritten.** The command prints the unified diff of what it would write, leaves the file and says so; ask the user and, if they accept, run it again with `--substituir-orca-yaml`.
  - `--dry-run` prints the `orca.yaml` and the diff and touches nothing (no clone, no Orca call that writes).
- `orq projetos [--json]` lists them. A file that is not JSON, has no `repo` or names a harness orq does not dispatch shows as `inválido: <why>` and is never picked on its own.
- `orq run projeto <name> [--run <id>]` ties a Run to a project: it writes a `run_projeto` event (the last one for the Run wins; a name with no file, or an invalid file, is refused and nothing is written). Orca keeps only the Run's `--objective`, so the link lives in orq's log.
- `orq despachar` resolves the project as `--projeto <name>`, then the Run's project, then the project whose `repo: path:` contains the current directory (or its main checkout), the longest path winning. A name that no longer exists or is invalid refuses before any task is created, also when it comes from the Run: falling back to the cwd would start the worker in the wrong repository. The harness is `--agente`, then the project's, then `claude`.
- With a project the dispatch calls `worker-start --repo <selector> --worktree new-top-level`, writes `projeto` in the `despacho` event, and for a Codex worker trusts the project's repository root (a `path:` selector is used as is; `id:` and `name:` are looked up with `orca repo list`; an unknown one falls back to the cwd root as before). `--worktree current` is refused when the project came from `--projeto` or the Run (it would stay in the cwd); when only the cwd matched, `current` still works and no `--repo` is sent. A dispatch queued for lack of a machine slot (ticket 79) keeps the project, so the manager starts it in the right repository from any directory.
- When the project declares `ambientes` and `--base-branch` is absent, the dispatch passes `--base-branch origin/<production>`: the work branch is born from production. Without the block Orca keeps its own default.
- No environment name is fixed in the code. The merge obligations (ticket 114) ask for the production deploy when the PR's base is the project's production and for the environment deploy plus the next PR when it is another environment; the check filter of a PR (ticket 103) matches the environment named in the workflow and treats "production" as the project's production branch; the merge queue (ticket 111) joins a PR into the step of its group (production, or the other environments) by the same flow; and `scripts/limpar-mergeados.py` takes the final base and the protected branches from `orq fluxo --repo <path> [--json]` (production, environments, flow of the project that contains the repository, or the remote's default branch with none). `ORQ_FINAL_BASE` and `ORQ_PROTECTED_BRANCHES` still force them.
- **The mate opens inside the project in Orca** (ticket 125). `orq mate abrir <group>` creates the mate's terminal with `--worktree path:<root of the group's project>` when `orca repo list` has that repository, so the terminal shows up grouped with the project. The project is the group's `projeto_mate` (a folder) or, without it, the first of its `projetos`. A project Orca does not know yet keeps the old behavior: the terminal opens in the current checkout and the command `cd`s into the folder.
- With no project file the behavior is the old one. There is still one coordinator for all projects.

Not done yet: E2E queue and transcripts, the quota policy, a project column in `orq agentes` and `orq iniciar`. See `docs/design.md`, "Projects".

## Claude Code and Codex

The coordinator can be a Claude Code or a Codex session, and each worker can be either: `orq despachar --agente codex --modelo gpt-6-sol --effort low ...` (the default is `claude`, or the harness of the project, below). What changes per agent sits in one table, `HARNESS` in `orqlib.py`: the resume command, the screen patterns and the accepted efforts. Orca builds the launch command itself from `worker-start --agent`. The rest goes through Orca for both:

- `orq retomar` finds a session the hooks never recorded through Orca's session index (`orca search <dispatch id>`);
- `orq uso [--agente codex]` reads each plan from `orca account list`, because the quotas are separate (Claude reads the HUD frame first);
- `orq despachar` refuses a dispatch when the quota of the chosen harness is over the limit;
- `orq passar <dispatch> --para codex|claude` moves a worker that hit its plan limit to the other harness (see Features).

The `worker-routing` skill maps the Claude roles to Codex models (Luna for clear, repeatable work, Sol for ambiguous or hard work, Astra low and medium only in place of Opus xhigh and max, no Terra). OpenAI publishes no equivalence with Claude models; the table follows OpenAI's own model guidance (<https://learn.chatgpt.com/docs/models>) and independent benchmarks cited in the skill.

Weaker on Codex: there is no AskUserQuestion, so decisions go through `orq perguntar` (Codex's own `request_user_input` only works in Plan mode and is refused in Default mode, codex-cli 0.159.3); `/away` becomes the `away` skill; and an untrusted Codex hook does not run, which leaves that session invisible to orq's turn tracking. The details are in [`docs/design.md`](docs/design.md#harnesses-claude-code-and-codex-ticket-73).

## Portability and lock-in

orq is a personal tool tuned for Claude Code or Codex plus Orca. It works, it has a large test suite, and it is shaped by one person's workflow.

Tied to the harness: the hook events and their JSON formats (UserPromptSubmit, PreToolUse, PostToolUse, PreCompact, SessionStart, Stop), which Claude Code and Codex share almost field for field; the AskUserQuestion tool and its `questions`/`answers`/`annotations` payload (Claude Code only); the transcript formats (`~/.claude/projects`, Codex rollouts); the skill file format.

Tied to Orca: the `orca orchestration` CLI and its JSON (Runs, tasks, gates, dispatches, the mailbox with `check --peek`/`--ack`, the inbox), `orca terminal` (send, wait, read, rename, close), `orca search`, `orca account list`, `orca automations runs`, the notice text Orca types into terminals, and the dispatch preamble that identifies a worker session.

Generic: the Python core, the append-only `events.jsonl` and the functions that derive state from it, the markdown tickets, and the entry/effect state machine.

A third agent would mean one more `HARNESS` entry (resume command, screen patterns, efforts), its hook config, and a transcript reader for `orq liberar`.

## Language

Code comments, docstrings, CLI names and messages are in Brazilian Portuguese, the author's language. A short glossary:

| pt-BR | English |
|---|---|
| entrada / efeito | entry / effect |
| tarefa, decisao, conversa, descartado | task, decision, conversation, discarded |
| pend, pendência | pending item for the user |
| acao, avisar, espera | action, notify, waiting on a third party |
| despachar / liberar | dispatch / release |
| grupo, mate, pedir, subir, pedido | group, secondmate, request, raise, request record |
| gerente, ligar / desligar, absorver | manager, bind / unbind, absorb |
| agentes: rodando, travado, perguntando, entregue, liberado | agents: running, stuck, asking, delivered, released |
| ticket novo / fechar / lista | ticket new / close / list |
| sem efeito, com você, aberto | without effect, with you, open |

## License

MIT. See [LICENSE](LICENSE). Copyright Leonardo Diego Barbosa.
