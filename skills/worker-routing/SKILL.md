---
name: worker-routing
description: Picks the Run, the model and the effort before dispatching a worker. Use when calling `orca orchestration worker-start`, when launching a subagent through the Agent tool, or when assembling the agents of a Workflow.
---

# Worker model and effort

Applies when you dispatch a worker (Orca `worker-start --model <id> --effort <level>`, Agent tool, workflow) and must choose model and effort. The two choices are independent:

- **model** follows **ambiguity**: how much doubt there is about *which* solution to follow;
- **effort** follows how much reasoning *this* execution deserves.

Diff size and file count are secondary signals. Swapping an API across 40 files can be Sonnet medium; a 30-line function that sometimes duplicates a payment can call for Opus high.

## Roles

- **Haiku, scout**: the path is already determined. Search, locating a symbol or usage, module summary, rename, imports, lint, simple config. It has no effort control.
- **Sonnet, default executor** (about 70% of tasks):
  - **low**: obvious fix, DTO, validation, new field, tests for one function;
  - **medium**: normal feature, endpoint, refactor following patterns, failing test;
  - **high**: multi-file debugging, feature that crosses database, API and front end, behavior-preserving refactor.
- **Opus, specialist and escalation**: real doubt about the solution.
  - **high**: architecture, boundaries and data model; obscure bug of unknown cause; review of security, concurrency or invariants; auth, payment, destructive migration; plan for a feature that crosses the system;
  - **xhigh**: a previous worker already failed, or several hypotheses are still open;
  - **max**: last resort, only after Opus xhigh failed.

Trivial planning ("`phone` field with migration, endpoint and form") is Sonnet medium, not Opus.

## Fixed overrides

- Pure search or mechanical change: Haiku.
- Security, payment or architecture decision: Opus high.
- Worker failed twice on the same task: Opus xhigh.

## Worker on Codex

`orq dispatch --agent codex --model <slug> --effort <level>` starts the worker on Codex (Orca launches it with `worker-start --agent codex`). Without `--agent`, the worker is Claude. The role still comes from ambiguity. To reach the Codex model, start from the role you would give on Claude:

| On Claude | On Codex |
|---|---|
| Haiku | no equivalent: simple search and mechanical change stay on Claude Haiku |
| Sonnet low | `gpt-6-luna` low |
| Sonnet medium | `gpt-6-luna` medium or high |
| Sonnet high | `gpt-6-luna` xhigh or max, or `gpt-6-sol` low, which is cheap for what it delivers |
| Opus high | `gpt-6-sol` medium or high |
| Opus xhigh | `gpt-6-astra` low |
| Opus max | `gpt-6-astra` medium; if it fails, move up to Claude Opus max |

- Luna's `max` sits above high and below Sol.
- To escalate on Codex, go up one step at a time: Luna low → medium → high → xhigh → max → Sol low → medium → high → xhigh → max → Astra low → Astra medium → Claude Opus max.
- Astra only at low and medium, in place of Opus xhigh and max (user decision, 01/10). Do not use `gpt-5.6-terra`.
- Do not use `ultra` on a worker: it delegates to subagents on its own.

Where the table comes from:
- OpenAI does not publish an equivalence with Claude. It warns that efforts do not correspond even between generations of its own models ("Reasoning efforts don't map exactly between model generations", https://learn.chatgpt.com/docs/models).
- The Luna and Sol split follows the roles that same page gives:
  - Luna for clear, repeatable tasks where you know what a good result looks like;
  - Sol for ambiguous, hard or high-value tasks.
- Sol sits at Opus's step, but below it, because of two results:
  - on the Artificial Analysis Intelligence Index, GPT-6 Sol at max scores 47.5, against 51.2 for Opus 5.5 at medium and 57.6 at max (https://kingy.ai/blog/gpt-6-sol-vs-claude-opus-5-5/);
  - Anthropic shows Sonnet 5.5 at high tying the best GPT-6 Sol on FrontierCode (https://www.anthropic.com/claude-sonnet-5-5).

## Where to dispatch

Every user task becomes a task in the Orca Run of its **stream** (the subject). The Run's objective is the card title on the tablet panel, which only reads Orca.

- **New stream:** `orca orchestration run-create --objective "<subject>" --json` before the first dispatch; with the agent manager on, follow with `orq manager bind --terminal <manager>`.
- **Existing stream:** dispatch with `--run <stream's id>`.
- **A new task is born as a ticket:** `orq ticket new --title "..." --spec-file <f> [--blocked-by NN,NN]` writes `~/.claude/orquestrador-plan/issues/NN-<slug>.md` (the ticket is the only source of the content; the spec carries `## Acceptance criteria`) and creates the task in Orca. `orq dispatch --ticket <NN> --model <m> --effort <e>` starts the worker on that task, and `orq ticket close <NN> --answer <file|text>` writes the Answer and completes the task (`docs/design.md`).
- **Dispatch:** `orq dispatch --run <stream> --title "..." --spec-file <f> --model <m> --effort <e> [--worktree current|new-top-level --name ... --base-branch ...] [--entry <e>]`. It runs `worker-start` with model and effort, writes the `dispatch` event, links the entry (`--entry`) and returns `dispatchId`, `taskId` and the ready-made waiter command (`orca-wait-runs.py`, `waiting` in the JSON). The title becomes the first line of the spec and the tab name. The coordinator must be bound to the Run (`run-use --id`).
- **Agent tool:** the subagent does not create a task. Before launching, run `task-create --run <stream> --task-title ... --spec ...` and then `task-update --status dispatched`. When it returns, mark `completed` or `failed`.
- **Agent manager:** Orca notices ("You have N orchestration message", heartbeat included) go only to the terminal bound to the Run, and a terminal with no agent receives no notice. That is why the Runs are bound to the agent manager's terminal, a shell running `~/.claude/orq/painel-agent-manager.sh`, and the coordinator only wakes for what matters (`docs/design.md`).
  - **Bind:** `orq manager bind --terminal <manager> [--run r]...` in the coordinator; it adds to the Runs the manager already has, and without `--run` the Run of the last `run-create`/`run-use` is added. `orq dispatch` on a new Run binds it on its own. `orq manager unbind [--run r]` gives one Run or all of them back to the coordinator (which holds only one).
  - **One Run per terminal:** Orca binds the manager to one Run at a time; the panel rotates them, and every `orq` command with `--run` (`dispatch`, `release`, `steer`, `ticket`, the waiter, the hook) rebinds the manager to the Run first, under the lock. A heartbeat from a Run unbound from the coordinator notifies nobody.
  - **Raw Orca command on the Run:** prefix it with `env ORCA_TERMINAL_HANDLE=<manager>` (`check`, `worker-start`, `worker-stop`, `task-update`…; for `reply` use `orq reply`). Orca answers `consumer_fenced` if the manager is on another Run: run `orq manager bind --terminal <manager> --run <r>` again or wait for the panel to come back, which stays on the Run of the notice.
  - **What reaches the coordinator:** the panel acknowledges heartbeats every 10 s and types into the coordinator `You have N orchestration message. Run orca orchestration check --run <r> --terminal <manager>.` once per batch with `worker_done`, `question` or `escalation`, from any Run of the manager. Run that `check` as is, process it and confirm with `--ack`. The waiter also wakes with the `worker_done`.
- **Coordinator binding:** it consumes the mailbox of one Run only, the last from `run-create` or `run-use --id`. On another Run, `check` returns `consumer_fenced`, and `worker-start`, `task-create` and `task-update` fail or do not save. Before touching a Run, do `run-use --id` on it and check the `ok` in the result. With the agent manager on, `orq` commands with `--run` pick the Run's owner on their own: the rebound manager, or the coordinator itself if it holds the Run outside the manager. Without `--run` and with the manager on more than one Run, `orq` asks for `--run`; a Run nobody holds asks for `orq manager bind --terminal <manager> --run <r>`. `orq pend add --task <t> --run <r>` creates the gate in the task's Run.
- **The Run belongs to the coordinator:** the worker spec tells the worker to use only the Run it received at dispatch; the worker does not run `run-create` or `run-use`. A test that needs a Run uses fake `ORQ_HOME` and `ORQ_ORCA`. `orq` decides the worker role from the dispatch preamble (the worker's first prompt), before any bound Run, so a Run created by mistake does not change the role but dirties the panel. `worker-list` cannot be used as a signal: without `--run` it only lists the Run bound to the terminal.
- **Several active streams:** wait with `~/.claude/scripts/orca-wait-runs.py <run>...`. It follows the `task-list` of every Run and the mailbox of the bound Run (with the agent manager, that of each of its Runs); on the other Runs in the list it wakes on question, escalation and worker_done read from the inbox, without consuming. To read and ack a message from another Run, do `run-use --id <run>` and switch back afterwards.
- **Heartbeat does not wake the coordinator:** the `orq hook prompt` hook absorbs the Orca notice when the bound Run's mailbox only has heartbeats (`docs/design.md`), and the waiter does the same. The last phase and the time of each running dispatch show under `Alive:` in the `orq` summary and under "Running" in the panel. A heartbeat notice from another Run is blocked the same way, reading the `inbox` and confirming nothing: the messages stay in that Run's mailbox until `run-use --id`. Any other message type, from any Run, wakes it.
- **Adjusting a running task:** `orq steer <task> "<text>" [--run r] [--entry e]`. It finds the dispatch, sends the `send` and records it; the coordinator must command the worker's Run (manager bound to it, or `run-use --id`). If Orca did not notify the worker (the inbox line has no `delivered_at`) and it is free, `orq` types the notice; if Orca already notified it, it does not repeat. The manager panel checks on every round (or `orq steers`): no read 90 s later and with the worker idle at the prompt, it retypes the notice, up to 3 times; after that it records the "unread steer" alert in the summary and in `orq agents` (`orq alert seen <task>` handles it). A read is the inbox `read` or the message id in the worker's transcript: the `read` only becomes 1 with `check --ack`. The worker spec tells the worker to confirm with `check --terminal <it> --ack <deliveryId>` after reading, because without the ack `check` repeats the same delivery and hides new messages. A busy worker receives nothing.
- **Answering a worker's question:** `orq reply <msg_id> "<text>"` finds the message's Run in the inbox, binds the manager to it and answers through the manager's handle. The raw `orca orchestration reply` gives `consumer_fenced` when the manager is on another Run.
- **Question or permission stuck in the worker's terminal:** nothing that asks for a human answer stays on the worker's screen. The `orq` hook refuses AskUserQuestion in a worker session and tells it to escalate (`orca orchestration ask` or `send --type escalation`). What Claude Code asks on its own (a permission prompt such as "Dangerous rm operation… Do you want to proceed?", the "trust this folder") the panel recognizes on screen: the worker becomes `asking` in `orq agents`, the manager types the question and the options once into the coordinator, and `orq answer-screen <task> <option>` (number or start of the label) types the answer into its terminal, with who answered in the log. The worker spec asks for commands that do not trigger the guard: `rm -rf "${S:?}"/*.exit`, never `rm -rf $S/*.exit`, and the same for any `rm`, `mv` or `cp` with a variable in the path. A session resumed by `orq resume` gets, in the continuation message, the coordinator's handle and the escalation command, because the dispatch preamble was lost.
- **User decision with an active worker:** goes through a Lavish page in the Orca browser, and `orq lavish-answer <file>` records the answer (`docs/design.md`). Pass the command the raw output of `lavish-axi poll`, without extracting the JSON. The page sends `disposicao: "escolha"` (or `manter`/`trocar`) with the written answer to close the decision; `livre`, `adiar` and `conversar` leave it open. AskUserQuestion is valid only with no active dispatch: an `orq` hook refuses the box while there is any, in any Run. A worker's doubt goes through `orca orchestration ask` to the coordinator (the Orca preamble already says so), never through the box.
- **Finished worker:** `orq release <dispatch> [--run r]` confirms the dispatch's pending `worker_done`, runs `worker-release` and, if the state comes back `retained` with no retention reason, runs `orca terminal close` on its terminal. A terminal Orca retained for a reason (`user_takeover`, `user_requested`, `external_terminal`…) and the coordinator's stay open, with the notice in the output. Read the output afterwards with `worker-read` (https://www.onorca.dev/docs/cli/orchestration). If the release comes back `release_pending`, repeat it later; `terminal close` by hand is no substitute.
- **Worker control:** `orq interrupt <dispatch>` sends the interrupt to the terminal (the worker stays alive). `orq end <dispatch> --reason "…"` runs `worker-stop` and `orq release`, with the reason in the log. `orq relaunch <dispatch> --note "what changed" [--model m --effort e]` stops the worker and starts another in the same worktree and task (`--retry-of`), with the old one's model and effort if you do not change them; the note arrives as the first adjustment. If the requested profile does not start, the old one starts; if nothing starts, the worktree stays and the message carries the command to repeat. The history shows in `orq agents`. `orq switch <dispatch> --to codex|claude [--model m --effort e]` continues the worker on the other harness (plan limit) in the same worktree and task: it writes `HANDOFF.md` (git, the task's decisions, the end of the transcript), starts `worker-start --retry-of --agent <other>` with the equivalent model from the table above and tells the new one to read the file; it refuses before stopping if the other harness is also above the threshold.
- **Who is alive:** `orq agents [--json] [--run r] [--all]` gives the state of each dispatch of all Runs: running, stuck (no heartbeat for more than 15 min, with the suggested `orq steer`), asking, and delivered (terminal open, ready for `orq release`). The injected summary and the panel show the same (`docs/design.md`).
- **Declared wait:** before a long blocking command (E2E queue, CI, deploy), the spec tells the worker to send a heartbeat with `--phase "waiting: <reason> until HH:MM"` (local time; without the `until`, it counts for 60 min). Until the deadline the dispatch counts as running, not stuck; once past, it becomes stuck with "wait expired". Without the heartbeat, a worker parked on the command shows as stuck at 15 min.
- **Checkpoint:** the worker spec tells it to write `orca worktree set --comment` on phase transitions, in the format of https://www.onorca.dev/docs/cli/worktree-checkpoints (the first line is the action; read first with `orca worktree current --json`).
- **Worker report:** the spec tells the worker to write the report (the `reportPath` and the body of `worker_done`) with the `writing-for-agents` skill: verdict first, one source per fact, a checkable done criterion, the rest behind a pointer. A research report follows the `research` skill.
- **Worker spec that edits `orq`:** tells it to create a worktree of its own (`git worktree add ../orq-<ticket>` from `~/.claude/orq`), run the tests there and commit there; the live `~/.claude/orq` only moves with `git pull` after the green commit, because the hooks and the panel run its `orq.py`. The panel writes `manager-alive` on every round and `orq status`, `orq summary` and the prompt hook warn "agent manager panel stopped" after 60 s.
- **Worker spec at night:** with `orq night` on, the spec tells the worker not to push, merge a PR, deploy or `git commit --no-verify` (the `orq hook external` hook denies it), to park the decision with `orq pend add` and, if the commit fails in pre-commit, to repair what the hook pointed at instead of bypassing it. `orq dispatch` already starts the worker with `GIT_TERMINAL_PROMPT=0` and `commit.gpgsign=false`.
- **User request in the spec:** `orq dispatch --entry eNNN` puts the entry's literal text in a `## User request` section at the top of the spec, separate from what the coordinator wrote, and `orq steer <task> "<text>" --entry eNNN` appends the new request to it (`## User request (addition)`). The spec tells the worker's review to check done against that section, not against the coordinator's summary. Without `--entry`, the spec comes out as it was; with `--ticket` the spec is the ticket file and does not get the section.
- **Worker spec that opens a PR:** tells it to cite only versioned files (`docs/research`, `docs/adr`, `docs/features`) in the PR body and to push the research in the same PR when the PR depends on it; `.scratch/` stays out.

## Escalation

Go up one step at a time when the worker fails or reports uncertainty: Haiku → Sonnet low → medium → high → Opus high → xhigh → max. Ask in the spec for the worker to return `needs_escalation`, with the reason and the suggested step, instead of insisting. That way Opus is not spent as a precaution.

The retry spec cites the old task's id. When the retry finishes well, close the old one: `orca orchestration task-update --id <old> --status completed --result '{"supersededBy":"<new>"}'`. Without it, it stays as a failure in the panel and in `task-list`.

## Calibrate

The rough target is 15% Haiku, 70% Sonnet and 15% Opus, without becoming a rule. The traces do the calibrating: tasks finished without retry, escalations, tests breaking after "done" and fixes in review. `orq retro` counts this per model and effort (the `por_modelo` table), and the `orq-retro` skill proposes the change to this table, which only applies with the user's ok.
