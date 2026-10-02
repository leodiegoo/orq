---
name: orq-retro
description: Analyzes an `orq retro` round and proposes changes to the orq environment (checks, text, worker-routing calibration). Use in the weekly orq retro or when the user asks to learn from worker failures.
---

# orq retro

The collector (`orq retro`) counts failure signals without an LLM. This skill reads the round and proposes changes to the **environment**, never to the product code. The analysis source is the collector; the steps for classifying and presenting are those of the `retro` skill (read it first).

## Steps

1. Run `orq retro --json` (the weekly trigger adds `--save`, which also feeds the gap ledger; on demand it does not save). The default window is 7 days; `--since YYYY-MM-DD` changes it. Then run `orq retro gaps --json`: the ledger remembers the gaps of earlier weeks.
2. For each signal with `n > 0`, open the pointer of at least one case before claiming anything: `sed -n <line>p <orq clone>/events.jsonl`, the transcript line, the PR. A signal only becomes a proposal once the cause is read in the pointer.
3. Separate signal from **noise**: a dirty tree with the same count on every `liberado_sujo` of the main checkout is dirt that was already there, not the worker's; a high `entrada_sem_tratamento` measures the coordinator's habit, not a bug; a steer from before the first `steer_end` in the log predates the read proof. Noise cases go in a single line.
4. Classify each proposal into exactly one class:
   - **check**: mechanical error (fixed pattern, forbidden command, wrong place). It becomes a hook, test or guard in orq. A new `_retro_violations` pattern goes here.
   - **text**: judgment error. It becomes a line in AGENTS.md, in the worker's default spec or in a memory. Say which file and the exact line.
   - **calibration**: the model or effort missed by enough. It becomes a change to the `worker-routing` table, with the `por_modelo` table as proof.
5. Make the short list: at most 5 proposals, each a gap with `estado` `proposta` in `gaps --json` (2 or more distinct sessions, in any weeks), or 1 case that lost work or broke orq. A `rejeitada` gap is not proposed again: it comes back by itself once it has more sessions than at the rejection. Order by severity. When a proposal is about a gap the ledger already has, cite its `id`; do not invent another.
6. Write the page in Lavish (`.lavish/retro-YYYY-MM-DD.html`, playbook `input`): per proposal, the evidence with pointer, the class, the concrete change and the collector metric that should drop the following week. Each proposal gets approve, reject or adjust. The page also lists the open gaps still under 2 sessions, in one line each.
7. Save the summary to `<orq clone>/plan/relatorios/retro-YYYY-MM-DD.md` (ORQ_PLAN) and finish. The round only ends with the page open and the report written.

## After the ok

Nothing is applied without the user's ok on the page. When the user rejects a proposal on the page, run `orq retro reject <id> --reason "<their reason>"` right away. When they approve one and the ticket is created, run `orq retro accept <id> --ticket <n>` so the gap turns covered when the ticket closes. An approved **check** or **calibration** proposal becomes a ticket (`orq ticket new`); a **text** one, an edit to the document, in a worker.
