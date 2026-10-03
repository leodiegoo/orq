---
name: away
description: Turns the orq away mode on or off, or shows it. Use when the user asks for `$away on|off|status` (with `on` taking `--text`, `--until`, `--max-dispatches`, `--max-failures`), or for away mode in a coordinator on Codex.
---

Run `orq away <on|off|status>` with the arguments the user gave (`status` if none), including the flags of `on`: `--text "<the user's words, verbatim>"` (the mandate: pass the free text the user gave after `on` as given, without rewording), `--until HH:MM` (default 08:00, the expected return), `--max-dispatches N`, `--max-failures 3` and repeat the output to the user, in full (when turning it off it carries the away report). The output is the whole answer.
